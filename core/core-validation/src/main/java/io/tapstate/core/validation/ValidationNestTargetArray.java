package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static io.tapstate.core.validation.ValidationRootCheck.*;

/**
 * Reads one actual target array into a parent-scoped membership frame. Element diagnostics retain
 * the physical document read, not an invented table row identified by the logical element key.
 * Invalid structure or identity closes the frame: dropping an unkeyed element could falsely prove
 * another member missing or unique. Complete source membership is required before target extraction;
 * its independently established problems survive a malformed target. Other root branches may continue.
 */
public final class ValidationNestTargetArray {
    public enum State { COMPLETE, INVALID }
    public record Limits(ValidationValues.Limits row, int elements, long operations) {
        public Limits {
            Objects.requireNonNull(row, "row");
            if (elements < 1 || elements > 65536 || operations < 1 || operations > 64L * 1024 * 1024) {
                throw ValidationError.invalid("targetArray.limits");
            }
        }
    }
    private ValidationNestTargetArray() {}

    /** A missing row-key binding means identity hidden by the fixed projection, never a missing field. */
    public static State read(ValidationObservation parent, List<String> arrayPath, ValidationCheckEvent.Side physicalParent,
            ValidationValues.Row workingParent, ValidationKeyBinding elementKey, Optional<ValidationKeyBinding> rowKey,
            Set<String> containerTypes, Limits limits, ValidationNestArrayMembers members, Builder root) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(members, "members");
        try {
            Objects.requireNonNull(parent, "parent");
            Objects.requireNonNull(physicalParent, "physicalParent");
            Objects.requireNonNull(workingParent, "workingParent");
            Objects.requireNonNull(elementKey, "elementKey");
            Objects.requireNonNull(rowKey, "rowKey");
            Objects.requireNonNull(limits, "limits");
            if (arrayPath == null || arrayPath.isEmpty() || arrayPath.size() > 32
                    || containerTypes == null || containerTypes.size() > 32) throw ValidationError.invalid("targetArray.path");
            for (String path : arrayPath) ValidationError.identifier(path, "targetArray.path");
            for (String type : containerTypes) ValidationError.identifier(type, "targetArray.containerTypes");
            arrayPath = List.copyOf(arrayPath);
            containerTypes = Set.copyOf(containerTypes);
            requireScope(parent, root);
            members.requireRequest(parent, arrayPath);
            members.requireSourceComplete();
            if (physicalParent.groupStatus() != ValidationCheckEvent.GroupStatus.COMPLETE || physicalParent.observedOccurrences() != 1) {
                throw ValidationError.invalid("targetArray.parent");
            }
            checkPathBudget(arrayPath, elementKey);
            if (rowKey.isPresent()) checkPathBudget(arrayPath, rowKey.orElseThrow());
            bounded(physicalParent.row(), limits.row()); bounded(workingParent, limits.row());
            root.read(Side.TARGET, physicalParent.boundary());
            var sample = new SampledSide(physicalParent.identity(), physicalParent.row(), physicalParent.boundary().readId());
            var meter = new Meter(limits.operations());
            Object array = workingParent.fields();
            for (String segment : arrayPath) {
                meter.step(1);
                array = unwrap(array, containerTypes, meter);
                if (!(array instanceof Map<?, ?> map) || !map.containsKey(segment)) {
                    return invalid(root, members, parent, arrayPath, arrayPath, Kind.TARGET_STRUCTURE, sample);
                }
                array = map.get(segment);
            }
            array = unwrap(array, containerTypes, meter);
            if (!(array instanceof List<?> values)) {
                return invalid(root, members, parent, arrayPath, arrayPath, Kind.TARGET_STRUCTURE, sample);
            }
            if (values.size() > limits.elements()) throw limit("elements");
            boolean valid = true;
            for (int ordinal = 0; ordinal < values.size(); ordinal++) {
                meter.step(1);
                var position = new ArrayList<Object>(arrayPath); position.add(ordinal);
                Object value = unwrap(values.get(ordinal), containerTypes, meter);
                if (!(value instanceof Map<?, ?> object)) {
                    finding(root, parent, arrayPath, position, Kind.TARGET_STRUCTURE, sample);
                    valid = false;
                    continue;
                }
                // The owned parent has already guaranteed String keys and bounded portable contents.
                @SuppressWarnings("unchecked") Map<String, Object> fields = (Map<String, Object>) object;
                var row = ValidationValues.ownRow(fields, limits.row());
                meter.step(row.nodes());
                var element = elementKey.read(row);
                var declared = rowKey.isPresent() ? rowKey.orElseThrow().read(row) : null;
                Set<List<String>> reported = new HashSet<>();
                boolean keyed = recordProblems(element, position, arrayPath, parent, sample, root, reported)
                        & (declared == null || recordProblems(declared, position, arrayPath, parent, sample, root, reported));
                if (!keyed) { valid = false; continue; }
                if (valid) members.add(ValidationNestArrayMembers.Side.TARGET, parent,
                        new ValidationNestArrayMembers.Member(declared == null ? Optional.empty() : Optional.of(((ValidationKeyBinding.Bound) declared).key()),
                                ((ValidationKeyBinding.Bound) element).key(), physicalParent.identity(), physicalParent.row(), row));
            }
            if (!valid) return discardInvalid(members, root);
            members.complete(ValidationNestArrayMembers.Side.TARGET, parent, physicalParent.boundary());
            return State.COMPLETE;
        } catch (RuntimeException | Error failure) {
            members.close(); root.close();
            throw failure;
        }
    }

    private static boolean recordProblems(ValidationKeyBinding.Result key, List<Object> position, List<String> arrayPath,
            ValidationObservation parent, SampledSide sample, Builder root, Set<List<String>> reported) {
        if (key instanceof ValidationKeyBinding.Bound) return true;
        for (var problem : ((ValidationKeyBinding.Invalid) key).problems()) {
            if (!reported.add(problem.path())) continue;
            var path = new ArrayList<Object>(position); path.addAll(problem.path());
            finding(root, parent, arrayPath, path, Kind.TARGET_KEY_INVALID, sample);
        }
        return false;
    }
    private static State invalid(Builder root, ValidationNestArrayMembers members, ValidationObservation parent,
            List<String> arrayPath, List<?> position, Kind kind, SampledSide sample) {
        finding(root, parent, arrayPath, position, kind, sample);
        return discardInvalid(members, root);
    }
    private static State discardInvalid(ValidationNestArrayMembers members, Builder root) {
        ValidationNestArrayCheck.retainSourceProblems(members, root);
        members.close();
        return State.INVALID;
    }
    private static void finding(Builder root, ValidationObservation parent, List<String> arrayPath, List<?> position,
            Kind kind, SampledSide sample) {
        Finding cause = new Finding(kind, new Location(parent.root(), new ArrayList<>(position)));
        Location scope = new Location(parent.root(), new ArrayList<>(arrayPath));
        root.finding(cause, Optional.empty(), Optional.of(sample));
        // Missing element identity prevents membership matching, even when its payload is readable.
        root.notApplicable(Check.STRUCTURE, scope, cause);
        if (root.method() == ValidationMethod.FULL_COLUMN) root.notApplicable(Check.VALUES, scope, cause);
    }
    private static Object unwrap(Object value, Set<String> containerTypes, Meter meter) {
        while (value instanceof ValidationValues.Typed typed) {
            meter.step(1);
            if (!containerTypes.contains(typed.logicalType())) return value;
            value = typed.value();
        }
        return value;
    }
    private static void checkPathBudget(List<String> arrayPath, ValidationKeyBinding key) {
        if (key.components().stream().anyMatch(component -> arrayPath.size() + 1 + component.path().size() > 64)) {
            throw ValidationError.invalid("targetArray.keyPath");
        }
    }
    private static void bounded(ValidationValues.Row row, ValidationValues.Limits limits) {
        if (row.accountedBytes() > limits.bytes() || row.nodes() > limits.nodes() || row.depth() > limits.depth()) throw limit("row");
    }
    private static void requireScope(ValidationObservation actual, Builder root) {
        var expected = root.observation();
        if (!actual.run().equals(expected.run()) || actual.revision() != expected.revision()
                || !actual.root().mappingId().equals(expected.root().mappingId()) || !actual.root().outputId().equals(expected.root().outputId())
                || !actual.root().rootKey().equals(expected.root().rootKey())
                || root.participation() != ValidationCheckEvent.Participation.SHOULD_EXIST || root.targetPresence() != TargetPresence.PRESENT) {
            throw ValidationError.invalid("targetArray.observation");
        }
    }
    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "targetArray." + dimension), null);
    }
    private static final class Meter {
        private final long limit;
        private long used;
        private Meter(long limit) { this.limit = limit; }
        private void step(long amount) { if (amount > limit - used) throw limit("operations"); used += amount; }
    }
}
