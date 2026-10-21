package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.ValidationRowCursor;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.tapstate.core.validation.ValidationCheckEvent.Participation;
import static io.tapstate.core.validation.ValidationRootCheck.*;

/**
 * Complete keyed root reads with independent source participation and unfiltered target cardinality.
 * Query coverage, physical/logical key bindings, resource reservations and deadlines are admitted by
 * the caller. In particular, a scan's one retained duplicate sample cannot supply this classification.
 * This produces a local observation only; it neither confirms nor publishes a difference.
 */
public final class ValidationRootReader {
    public record Query(String readId, String contractVersion, Supplier<ValidationRowCursor> open) {
        public Query {
            new ValidationCheckEvent.ReadBoundary(readId, contractVersion, Instant.EPOCH, Instant.EPOCH);
            Objects.requireNonNull(open, "open");
        }
    }

    /** Known source data issues explain uncertain participation; execution failures must be thrown. */
    public record Source(ValidationRelationReader.Evaluation row, List<Finding> uncertainty) {
        public Source {
            Objects.requireNonNull(row, "row");
            if (uncertainty == null || uncertainty.size() > 256
                    || (row.disposition() == ValidationRelationRows.Disposition.INDETERMINATE) != !uncertainty.isEmpty()
                    || uncertainty.stream().anyMatch(cause -> cause == null || !cause.kind().sourceProblem())) {
                throw invalid("rootRead.sourceClassification");
            }
            uncertainty = List.copyOf(uncertainty);
        }
    }

    /** Every target row participates. Projection cannot hide a returned duplicate. */
    public record Target(ValidationValues.Row working, ValidationPhysicalIdentity physical) {
        public Target {
            Objects.requireNonNull(working, "working");
            Objects.requireNonNull(physical, "physical");
        }
    }

    public record Pair(ValidationRelationRows.Sample source, ValidationCheckEvent.ReadBoundary sourceRead,
                       ValidationRelationRows.Sample target, ValidationCheckEvent.ReadBoundary targetRead) {
        public Optional<SampledSide> sourceSample() { return sample(source, sourceRead); }
        public Optional<SampledSide> targetSample() { return sample(target, targetRead); }
    }

    @FunctionalInterface
    public interface ContentCheck {
        /** Complete every admitted content/descendant obligation, or record its explicit blocking scope. */
        void check(Pair pair, Builder root);
    }

    private ValidationRootReader() {}

    /** Opens each cursor only when needed and authorized; the relation reader closes it on every exit. */
    public static ValidationRootCheck read(ValidationObservation observation, ValidationMethod method,
            Query sourceQuery, Query targetQuery, ValidationRelationRows.Limits readLimits, Limits resultLimits,
            Clock clock, Runnable requireAuthorized, Function<ValidationValues.Row, Source> sourceRule,
            Function<ValidationValues.Row, Target> targetRule, ContentCheck content) {
        Objects.requireNonNull(sourceQuery, "sourceQuery");
        Objects.requireNonNull(targetQuery, "targetQuery");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(requireAuthorized, "requireAuthorized");
        Objects.requireNonNull(sourceRule, "sourceRule");
        Objects.requireNonNull(targetRule, "targetRule");
        Objects.requireNonNull(content, "content");
        try (var root = ValidationRootCheck.pending(observation, method, resultLimits)) {
            var whole = new Location(observation.root(), List.of());
            // References to already-budgeted finding metadata; bounded by the result's finding cap.
            var uncertainty = new LinkedHashSet<Finding>();
            requireAuthorized.run();
            var source = ValidationRelationReader.read(observation, sourceQuery.readId(), sourceQuery.contractVersion(),
                    sourceQuery.open().get(), readLimits, clock, requireAuthorized, original -> {
                        var classified = Objects.requireNonNull(sourceRule.apply(original), "source classification");
                        var diagnostic = classified.row().physical().map(identity -> new SampledSide(identity, original, sourceQuery.readId()));
                        for (Finding cause : classified.uncertainty()) {
                            root.sourceFinding(cause, diagnostic);
                            blockContent(root, whole, cause);
                            uncertainty.add(cause);
                        }
                        return classified.row();
                    });
            root.read(Side.SOURCE, source.read());
            if (source.atLeastTwo()) {
                var cause = new Finding(Kind.SOURCE_DUPLICATE, whole,
                        Optional.of(new Multiplicity(source.included(), source.participationComplete())));
                root.finding(cause, sample(source.includedSamples().getFirst(), source.read()), Optional.empty())
                        .notCompared(Check.IDENTITY, whole, cause);
                blockContent(root, whole, cause);
                uncertainty.add(cause);
            }
            if (!uncertainty.isEmpty()) {
                root.bindOutcome(Participation.UNKNOWN, TargetPresence.NOT_READ);
                for (Finding cause : uncertainty) root.notCompared(Check.PARTICIPATION, whole, cause);
                root.checked(Check.IDENTITY);
                requireAuthorized.run();
                return root.finish();
            }
            requireAuthorized.run();
            var target = ValidationRelationReader.read(observation, targetQuery.readId(), targetQuery.contractVersion(),
                    targetQuery.open().get(), readLimits, clock, requireAuthorized, original -> {
                        var classified = Objects.requireNonNull(targetRule.apply(original), "target classification");
                        return new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.INCLUDED,
                                classified.working(), Optional.of(classified.physical()));
                    });
            boolean shouldExist = source.unique().isPresent();
            boolean exists = !target.definitelyEmpty();
            root.bindOutcome(shouldExist ? Participation.SHOULD_EXIST : Participation.SHOULD_NOT_EXIST,
                    exists ? TargetPresence.PRESENT : TargetPresence.ABSENT).read(Side.TARGET, target.read());
            var sourceSample = source.unique().flatMap(row -> sample(row, source.read()));
            var targetSample = exists ? sample(target.includedSamples().getFirst(), target.read()) : Optional.<SampledSide>empty();
            if (target.atLeastTwo()) {
                var cause = new Finding(Kind.TARGET_DUPLICATE, whole, Optional.of(new Multiplicity(target.included(), true)));
                root.finding(cause, sourceSample, targetSample);
                if (shouldExist) {
                    root.notApplicable(Check.STRUCTURE, whole, cause);
                    if (method == ValidationMethod.FULL_COLUMN) root.notApplicable(Check.VALUES, whole, cause);
                }
            }
            if (shouldExist != exists) root.finding(new Finding(shouldExist ? Kind.TARGET_MISSING : Kind.TARGET_EXTRA, whole), sourceSample, targetSample);
            root.checked(Check.IDENTITY).checked(Check.PARTICIPATION);
            if (shouldExist && exists) {
                if (!target.atLeastTwo()) {
                    requireAuthorized.run();
                    content.check(new Pair(source.unique().orElseThrow(), source.read(), target.unique().orElseThrow(), target.read()), root);
                }
                root.checked(Check.STRUCTURE);
                if (method == ValidationMethod.FULL_COLUMN) root.checked(Check.VALUES);
            } else {
                root.notApplicable(Check.STRUCTURE);
                if (method == ValidationMethod.FULL_COLUMN) root.notApplicable(Check.VALUES);
            }
            requireAuthorized.run();
            return root.finish();
        }
    }

    private static void blockContent(Builder root, Location location, Finding cause) {
        root.notCompared(Check.STRUCTURE, location, cause);
        if (root.method() == ValidationMethod.FULL_COLUMN) root.notCompared(Check.VALUES, location, cause);
    }
    private static Optional<SampledSide> sample(ValidationRelationRows.Sample row, ValidationCheckEvent.ReadBoundary read) {
        return row.physical().map(identity -> new SampledSide(identity, row.original(), read.readId()));
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
}
