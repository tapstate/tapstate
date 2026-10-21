package io.tapstate.core.validation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static io.tapstate.core.validation.ValidationRootCheck.*;

/**
 * Compares a fully defined current field view and records every independent mismatch in its root.
 * The compiled caller checks delegated Nest scopes separately and never supplies unresolved source
 * fields as missing values. This helper does not complete root obligations or authorize publication.
 */
public final class ValidationFieldCheck {
    /** Fixed-model eligibility for the physical output document only; never a general ignore-field rule. */
    public static final class GeneratedId {
        private final ValidationRunIdentity run;
        private final String mappingId;
        private final String outputId;
        private final Predicate<Object> requirement;
        private GeneratedId(ValidationRunIdentity run, String mappingId, String outputId, Predicate<Object> requirement) {
            this.run = run; this.mappingId = mappingId; this.outputId = outputId; this.requirement = requirement;
        }
    }
    public record Result(long differences, boolean generatedIdValueExempt) {}
    private ValidationFieldCheck() {}

    /**
     * The caller proves these flags from the fixed writer/model, final source projection and all key
     * and rule dependencies. The pure bounded requirement checks every explicit target type/null
     * obligation on the actual owned value; it may not guess from an identifier's printed appearance.
     */
    public static Optional<GeneratedId> generatedId(ValidationRunIdentity run, String mappingId, String outputId, boolean targetGeneratesId,
            boolean sourceMapsId, boolean identityOrRuleDependency, Predicate<Object> requirement) {
        Objects.requireNonNull(run, "run");
        Objects.requireNonNull(requirement, "requirement");
        mappingId = ValidationError.identifier(mappingId, "generatedId.mappingId");
        outputId = ValidationError.identifier(outputId, "generatedId.outputId");
        return targetGeneratesId && !sourceMapsId && !identityOrRuleDependency
                ? Optional.of(new GeneratedId(run, mappingId, outputId, requirement)) : Optional.empty();
    }

    /**
     * Reserve three extra working-row allocations when applying the generated-id rule, in addition
     * to original reads, rules and retained diagnostics. Local limits bound copies, not total heap.
     */
    public static Result check(ValidationObservation parent, List<Object> path, ValidationValues.Row source,
            ValidationValues.Row target, Optional<SampledSide> sourceSample, Optional<SampledSide> targetSample,
            Optional<GeneratedId> generatedId, BiPredicate<String, String> compatible,
            ValidationValueComparison.TraversalLimits limits, Builder root) {
        Objects.requireNonNull(root, "root");
        try {
            Objects.requireNonNull(parent, "parent");
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(target, "target");
            Objects.requireNonNull(sourceSample, "sourceSample");
            Objects.requireNonNull(targetSample, "targetSample");
            Objects.requireNonNull(generatedId, "generatedId");
            Objects.requireNonNull(compatible, "compatible");
            Objects.requireNonNull(limits, "limits");
            Location location = new Location(parent.root(), path);
            List<Object> fieldPath = location.fieldPath();
            requireScope(parent, root);
            sourceSample.ifPresent(sample -> root.requireSampleRead(Side.SOURCE, sample));
            targetSample.ifPresent(sample -> root.requireSampleRead(Side.TARGET, sample));
            bounded(source, limits.row()); bounded(target, limits.row());
            if (generatedId.isPresent() && (!location.wholeRoot() || !generatedId.orElseThrow().run.equals(parent.run())
                    || !generatedId.orElseThrow().mappingId.equals(parent.root().mappingId())
                    || !generatedId.orElseThrow().outputId.equals(parent.root().outputId()))) {
                throw ValidationError.invalid("fieldCheck.generatedIdScope");
            }
            boolean exempt = generatedId.isPresent() && !source.fields().containsKey("_id");
            long requiredDifferences = 0;
            ValidationValues.Row comparedTarget = target;
            if (exempt) {
                var idPath = new ArrayList<Object>(fieldPath); idPath.add("_id");
                Location id = new Location(parent.root(), idPath);
                if (!target.fields().containsKey("_id")) {
                    record(root, Kind.TARGET_MISSING, id, sourceSample, targetSample);
                    requiredDifferences++;
                } else {
                    if (!generatedId.orElseThrow().requirement.test(target.fields().get("_id"))) {
                        record(root, Kind.TARGET_TYPE, id, sourceSample, targetSample);
                        requiredDifferences++;
                    }
                    var fields = new LinkedHashMap<>(target.fields()); fields.remove("_id");
                    comparedTarget = ValidationValues.ownRow(fields, limits.row());
                }
            }
            long differences = ValidationValueComparison.visitDifferences(source, comparedTarget, compatible, limits, difference -> {
                var field = new ArrayList<Object>(fieldPath); field.addAll(difference.path());
                Kind kind = switch (difference.kind()) {
                    case VALUE -> Kind.TARGET_VALUE;
                    case LOGICAL_TYPE -> Kind.TARGET_TYPE;
                    case MISSING_SOURCE -> Kind.TARGET_EXTRA;
                    case MISSING_TARGET -> Kind.TARGET_MISSING;
                };
                record(root, kind, new Location(parent.root(), field), sourceSample, targetSample);
            });
            return new Result(requiredDifferences + differences, exempt);
        } catch (RuntimeException | Error failure) {
            root.close();
            throw failure;
        }
    }

    private static void record(Builder root, Kind kind, Location location, Optional<SampledSide> source, Optional<SampledSide> target) {
        Finding finding = new Finding(kind, location);
        root.finding(finding, source, target);
        if (kind != Kind.TARGET_VALUE) {
            root.notApplicable(Check.STRUCTURE, location, finding).notApplicable(Check.VALUES, location, finding);
        }
    }
    private static void bounded(ValidationValues.Row row, ValidationValues.Limits limits) {
        if (row.accountedBytes() > limits.bytes() || row.nodes() > limits.nodes() || row.depth() > limits.depth()) {
            throw new io.tapstate.core.common.TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                    java.util.Map.of("limit", "fieldCheck.row"), null);
        }
    }
    private static void requireScope(ValidationObservation actual, Builder root) {
        var expected = root.observation();
        if (!actual.run().equals(expected.run()) || actual.revision() != expected.revision()
                || !actual.root().mappingId().equals(expected.root().mappingId()) || !actual.root().outputId().equals(expected.root().outputId())
                || !actual.root().rootKey().equals(expected.root().rootKey()) || root.method() != ValidationMethod.FULL_COLUMN
                || root.participation() != ValidationCheckEvent.Participation.SHOULD_EXIST || root.targetPresence() != TargetPresence.PRESENT) {
            throw ValidationError.invalid("fieldCheck.observation");
        }
    }
}
