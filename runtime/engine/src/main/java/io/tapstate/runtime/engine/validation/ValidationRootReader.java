package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.ValidationRowCursor;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.ArrayList;
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

    /**
     * A known source problem and the final content scopes it prevents comparing. An empty scope list
     * retains the diagnosis after explicit rules remove/replace that contribution; it hides no target field.
     */
    public record SourceIssue(Finding finding, Optional<SampledSide> sample, List<Location> affected) {
        public SourceIssue {
            Objects.requireNonNull(finding, "finding"); Objects.requireNonNull(sample, "sample");
            if (!finding.kind().sourceProblem() || affected == null || affected.size() > 256)
                throw invalid("rootRead.sourceIssue");
            affected = List.copyOf(affected);
        }
    }

    /** Only completed independent source reads; partial query results and execution failures never enter here. */
    public record SourceEvidence(ValidationObservation observation, List<ValidationCheckEvent.ReadBoundary> reads, List<SourceIssue> issues) {
        public SourceEvidence {
            if (reads == null || reads.size() > 256 || issues == null || issues.size() > 256
                    || observation == null && (!reads.isEmpty() || !issues.isEmpty()))
                throw invalid("rootRead.sourceEvidence");
            reads = List.copyOf(reads); issues = List.copyOf(issues);
        }
        public static SourceEvidence empty() { return new SourceEvidence(null, List.of(), List.of()); }
        /** Imports one completed Nest relation's facts without pretending its enclosing parent read ended. */
        public static SourceEvidence fromSingleSource(ValidationObservation root, ValidationNestSingleSource.Result source) {
            Objects.requireNonNull(root, "root"); Objects.requireNonNull(source, "source");
            requireNestedScope(root, source.observation());
            return new SourceEvidence(root, List.of(source.read()), source.issues().stream()
                    .map(issue -> new SourceIssue(issue.finding(), issue.sample(), issue.affected())).toList());
        }

        /** Records each conflict cause once, retaining all affected literal/embedded locations. */
        public static SourceEvidence fromFieldConflicts(ValidationObservation root, ValidationObservation parent,
                ValidationNestFieldCheck.Result fields, Optional<SampledSide> originalParent) {
            Objects.requireNonNull(root, "root"); Objects.requireNonNull(parent, "parent");
            Objects.requireNonNull(fields, "fields"); Objects.requireNonNull(originalParent, "originalParent");
            requireNestedScope(root, parent);
            var affected = new LinkedHashMap<Finding, LinkedHashSet<Location>>();
            for (var conflict : fields.conflicts()) {
                var at = new Location(parent.root(), new ArrayList<>(conflict.contributor().path()));
                var cause = new Finding(Kind.SOURCE_FIELD_CONFLICT, at);
                var paths = affected.computeIfAbsent(cause, ignored -> new LinkedHashSet<>());
                paths.add(at);
                paths.add(new Location(parent.root(), new ArrayList<>(conflict.occupied().path())));
            }
            return new SourceEvidence(root, List.of(), affected.entrySet().stream()
                    .map(entry -> new SourceIssue(entry.getKey(), originalParent, new ArrayList<>(entry.getValue()))).toList());
        }
        private static void requireNestedScope(ValidationObservation root, ValidationObservation actual) {
            if (!root.run().equals(actual.run()) || root.revision() != actual.revision()
                    || !root.root().mappingId().equals(actual.root().mappingId())
                    || !root.root().outputId().equals(actual.root().outputId())
                    || !root.root().rootKey().equals(actual.root().rootKey())
                    || actual.root().ancestors().size() < root.root().ancestors().size()
                    || !actual.root().ancestors().subList(0, root.root().ancestors().size()).equals(root.root().ancestors())) {
                throw invalid("rootRead.nestObservation");
            }
        }
    }

    /** Known source data issues may survive a definite predicate; uncertainty specifically prevents participation. */
    public record Source(ValidationRelationReader.Evaluation row, List<Finding> uncertainty, SourceEvidence evidence) {
        public Source(ValidationRelationReader.Evaluation row, List<Finding> uncertainty) {
            this(row, uncertainty, SourceEvidence.empty());
        }
        public Source {
            Objects.requireNonNull(row, "row");
            Objects.requireNonNull(evidence, "evidence");
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

    /** Content evidence retains deduplicated metadata only; raw diagnostic samples belong to the bounded root report. */
    public record Pair(ValidationRelationRows.Sample source, ValidationCheckEvent.ReadBoundary sourceRead,
                       ValidationRelationRows.Sample target, ValidationCheckEvent.ReadBoundary targetRead,
                       SourceEvidence sourceEvidence) {
        public Optional<SampledSide> sourceSample() { return sample(source, sourceRead); }
        public Optional<SampledSide> targetSample() { return sample(target, targetRead); }
    }

    @FunctionalInterface
    public interface ContentCheck {
        /**
         * Complete every admitted content/descendant obligation, or record its blocking scope.
         * Source evidence's affected scopes must not be compared using invented partial values.
         */
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
            var retainedSource = new SourceEvidence[] { null };
            var contentBlocked = new boolean[] { false };
            requireAuthorized.run();
            var source = ValidationRelationReader.read(observation, sourceQuery.readId(), sourceQuery.contractVersion(),
                    sourceQuery.open().get(), readLimits, clock, requireAuthorized, original -> {
                        var classified = Objects.requireNonNull(sourceRule.apply(original), "source classification");
                        var diagnostic = classified.row().physical().map(identity -> new SampledSide(identity, original, sourceQuery.readId()));
                        if (classified.evidence().observation() != null && !observation.equals(classified.evidence().observation()))
                            throw invalid("rootRead.sourceEvidenceObservation");
                        var recordedHere = new LinkedHashSet<Finding>();
                        for (var read : classified.evidence().reads()) root.read(Side.SOURCE, read);
                        for (var issue : classified.evidence().issues()) {
                            root.sourceFinding(issue.finding(), issue.sample());
                            for (var affected : issue.affected()) blockContent(root, affected, issue.finding());
                            recordedHere.add(issue.finding()); contentBlocked[0] |= !issue.affected().isEmpty();
                        }
                        if (classified.row().disposition() == ValidationRelationRows.Disposition.INCLUDED && retainedSource[0] == null)
                            retainedSource[0] = contentEvidence(classified.evidence());
                        for (Finding cause : classified.uncertainty()) {
                            if (!recordedHere.contains(cause)) root.sourceFinding(cause, diagnostic);
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
                    content.check(new Pair(source.unique().orElseThrow(), source.read(), target.unique().orElseThrow(), target.read(),
                            Objects.requireNonNull(retainedSource[0], "unique source evidence")), root);
                }
                root.checked(Check.STRUCTURE);
                if (method == ValidationMethod.FULL_COLUMN) root.checked(Check.VALUES);
            } else if (!contentBlocked[0]) {
                root.notApplicable(Check.STRUCTURE);
                if (method == ValidationMethod.FULL_COLUMN) root.notApplicable(Check.VALUES);
            }
            requireAuthorized.run();
            return root.finish();
        }
    }

    private static SourceEvidence contentEvidence(SourceEvidence source) {
        // Import into RootCheck first: its hard finding/scope/read limits bound this metadata. Never
        // retain the caller's diagnostic rows here, including samples the root report has dropped.
        var byFinding = new LinkedHashMap<Finding, LinkedHashSet<Location>>();
        for (var issue : source.issues()) byFinding.computeIfAbsent(issue.finding(), ignored -> new LinkedHashSet<>()).addAll(issue.affected());
        var compact = new ArrayList<SourceIssue>();
        for (var entry : byFinding.entrySet()) {
            var affected = new ArrayList<>(entry.getValue());
            if (affected.isEmpty()) compact.add(new SourceIssue(entry.getKey(), Optional.empty(), List.of()));
            for (int from = 0; from < affected.size(); from += 256)
                compact.add(new SourceIssue(entry.getKey(), Optional.empty(), affected.subList(from, Math.min(from + 256, affected.size()))));
        }
        return new SourceEvidence(source.observation(), new ArrayList<>(new LinkedHashSet<>(source.reads())), compact);
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
