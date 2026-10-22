package io.tapstate.core.validation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static io.tapstate.core.validation.ValidationRootCheck.*;

/**
 * Compares current field views and records every independent mismatch in their root. Partial views
 * require recorded source blocks for every unavailable path. The compiled caller checks delegated
 * Nest scopes separately and never supplies unresolved source fields as known missing values. This helper does not complete root obligations or authorize publication.
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
        return checkView(parent, path, source, target, sourceSample, targetSample, generatedId, compatible, limits, List.of(), Set.of(), List.of(), root);
    }

    /**
     * Paths are relative to this source working row, including ordinary-array ordinals. Each needs
     * an existing source block for both structure and values at the exact enclosing observation and
     * field path. Known parent shape and siblings remain comparable. Reserve one extra row-sized
     * mask-metadata allocation; masks and their traversal checks have hard byte/operation limits.
     */
    public static Result checkPartial(ValidationObservation parent, List<Object> path, ValidationValues.Row source,
            ValidationValues.Row target, Optional<SampledSide> sourceSample, Optional<SampledSide> targetSample,
            Optional<GeneratedId> generatedId, BiPredicate<String, String> compatible,
            ValidationValueComparison.TraversalLimits limits, List<List<Object>> unavailablePaths, Builder root) {
        return checkView(parent, path, source, target, sourceSample, targetSample, generatedId, compatible, limits, unavailablePaths, Set.of(), List.of(), root);
    }

    /** Defined fields override only the whole-row unknown marker, as established by explicit static rules. */
    public static Result checkPartial(ValidationObservation parent, List<Object> path, ValidationValues.Row source,
            ValidationValues.Row target, Optional<SampledSide> sourceSample, Optional<SampledSide> targetSample,
            Optional<GeneratedId> generatedId, BiPredicate<String, String> compatible,
            ValidationValueComparison.TraversalLimits limits, List<List<Object>> unavailablePaths, Set<String> definedFields, Builder root) {
        return checkView(parent, path, source, target, sourceSample, targetSample, generatedId, compatible, limits, unavailablePaths, definedFields, List.of(), root);
    }

    /**
     * Combines independently checked Nest arrays with ordinary fields in the same complete parent
     * view. Evidence is valid only for these exact rows, observation, and builder; no arbitrary
     * ignore paths are accepted. Unknown source paths remain separate and require source blocks.
     * Nested members use their full ancestor observation and their own complete working views.
     */
    public static Result checkComposed(ValidationObservation parent, ValidationValues.Row source,
            ValidationValues.Row target, Optional<SampledSide> sourceSample, Optional<SampledSide> targetSample,
            Optional<GeneratedId> generatedId, BiPredicate<String, String> compatible,
            ValidationValueComparison.TraversalLimits limits, List<List<Object>> unavailablePaths, Set<String> definedFields,
            List<ValidationNestArrayFieldCheck.Checked> arrays, Builder root) {
        return checkView(parent, List.of(), source, target, sourceSample, targetSample, generatedId, compatible, limits,
                unavailablePaths, definedFields, arrays, root);
    }

    private static Result checkView(ValidationObservation parent, List<Object> path, ValidationValues.Row source,
            ValidationValues.Row target, Optional<SampledSide> sourceSample, Optional<SampledSide> targetSample,
            Optional<GeneratedId> generatedId, BiPredicate<String, String> compatible,
            ValidationValueComparison.TraversalLimits limits, List<List<Object>> unavailablePaths, Set<String> definedFields,
            List<ValidationNestArrayFieldCheck.Checked> arrays, Builder root) {
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
            var masks = unavailable(parent, fieldPath, unavailablePaths, definedFields, limits.row().bytes(), root);
            var composition = composed(parent, source, target, masks, arrays, limits.row().bytes(), limits.operations(), root);
            var unavailable = composition.paths();
            var defined = masks.definedFields();
            sourceSample.ifPresent(sample -> root.requireSampleRead(Side.SOURCE, sample));
            targetSample.ifPresent(sample -> root.requireSampleRead(Side.TARGET, sample));
            bounded(source, limits.row()); bounded(target, limits.row());
            if (generatedId.isPresent() && (!location.wholeRoot() || !generatedId.orElseThrow().run.equals(parent.run())
                    || !generatedId.orElseThrow().mappingId.equals(parent.root().mappingId())
                    || !generatedId.orElseThrow().outputId.equals(parent.root().outputId()))) {
                throw ValidationError.invalid("fieldCheck.generatedIdScope");
            }
            boolean exempt = generatedId.isPresent() && !source.fields().containsKey("_id")
                    && unavailable.stream().noneMatch(mask -> mask.isEmpty() && !defined.contains("_id") || mask.equals(List.of("_id")));
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
            long differences = ValidationValueComparison.visitKnownDifferences(source, comparedTarget, compatible,
                    new ValidationValueComparison.TraversalLimits(limits.row(), composition.remainingOperations()), unavailable, defined, difference -> {
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

    private record Masks(List<List<Object>> paths, Set<String> definedFields, long bytes) {}
    private static Masks unavailable(ValidationObservation parent, List<Object> base, List<List<Object>> paths,
            Set<String> definedFields, long maxBytes, Builder root) {
        if (paths == null || paths.size() > 256 || definedFields == null || definedFields.size() > 256)
            throw ValidationError.invalid("fieldCheck.unavailablePaths");
        long bytes = 64;
        for (String field : definedFields) {
            ValidationError.fieldName(field, "fieldCheck.definedField");
            bytes += 48L + 2L * field.length();
            if (bytes > maxBytes) throw new io.tapstate.core.common.TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                    java.util.Map.of("limit", "fieldCheck.unavailablePaths"), null);
        }
        var masks = new ArrayList<List<Object>>(paths.size());
        for (var path : paths) {
            if (path == null || path.size() > 64 - base.size() || !path.isEmpty() && !(path.getFirst() instanceof String))
                throw ValidationError.invalid("fieldCheck.unavailablePaths");
            bytes += 64L + 16L * path.size();
            for (Object segment : path) bytes += segment instanceof String text ? 48L + 2L * text.length() : 32L;
            if (bytes > maxBytes) throw new io.tapstate.core.common.TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                    java.util.Map.of("limit", "fieldCheck.unavailablePaths"), null);
            var complete = new ArrayList<Object>(base); complete.addAll(path);
            var location = new Location(parent.root(), complete);
            root.requireBlocked(Check.STRUCTURE, location).requireBlocked(Check.VALUES, location);
            masks.add(List.copyOf(path));
        }
        return new Masks(List.copyOf(masks), Set.copyOf(definedFields), bytes);
    }

    private record Composition(List<List<Object>> paths, long remainingOperations) {}
    private static Composition composed(ValidationObservation parent, ValidationValues.Row source, ValidationValues.Row target,
            Masks masks, List<ValidationNestArrayFieldCheck.Checked> arrays, long maxBytes, long operations, Builder root) {
        if (arrays == null || arrays.size() > 256 - masks.paths().size()) throw ValidationError.invalid("fieldCheck.arrays");
        if (arrays.isEmpty()) return new Composition(masks.paths(), operations);
        var work = new MaskWork(operations);
        long bytes = masks.bytes();
        var paths = new ArrayList<>(masks.paths());
        for (var array : arrays) {
            if (array == null) throw ValidationError.invalid("fieldCheck.arrays");
            work.step(1);
            List<String> declared = array.require(parent, source, target, root);
            bytes += 96L + 16L * declared.size();
            for (String segment : declared) { work.step(1L + segment.length()); bytes += 48L + 2L * segment.length(); }
            if (bytes > maxBytes) throw new io.tapstate.core.common.TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                    java.util.Map.of("limit", "fieldCheck.arrays"), null);
            for (var path : paths) {
                work.step(1L + declared.getFirst().length());
                if (path.isEmpty() && masks.definedFields().contains(declared.getFirst())) continue;
                if (prefix(path, declared, work) || prefix(declared, path, work)) throw ValidationError.invalid("fieldCheck.arrayOverlap");
            }
            paths.add(new ArrayList<>(declared));
        }
        return new Composition(List.copyOf(paths), work.remaining);
    }

    private static boolean prefix(List<?> left, List<?> right, MaskWork work) {
        work.step(1);
        if (left.size() > right.size()) return false;
        for (int i = 0; i < left.size(); i++) {
            Object a = left.get(i); Object b = right.get(i);
            work.step(1L + (a instanceof String text && b instanceof String other ? Math.min(text.length(), other.length()) : 0));
            if (!a.equals(b)) return false;
        }
        return true;
    }

    private static final class MaskWork {
        private long remaining;
        private MaskWork(long operations) { remaining = operations; }
        private void step(long amount) {
            // Leave at least the root traversal step to the value walker.
            if (amount >= remaining) throw new io.tapstate.core.common.TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                    java.util.Map.of("limit", "fieldCheck.arrayOperations"), null);
            remaining -= amount;
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
