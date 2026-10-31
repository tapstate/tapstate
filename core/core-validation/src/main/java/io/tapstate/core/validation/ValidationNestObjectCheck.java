package io.tapstate.core.validation;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

import static io.tapstate.core.validation.ValidationRootCheck.*;

/**
 * Composes complete single-source cardinality with the actual target object's shape. The matched
 * callback must check admitted observable identity, projected payload and descendants before the
 * caller completes the root. No historical winner or expected business document is manufactured.
 */
public final class ValidationNestObjectCheck {
    public record Target(ValidationCheckEvent.Side physical, ValidationValues.Row workingParent) {
        public Target {
            Objects.requireNonNull(physical, "physical");
            Objects.requireNonNull(workingParent, "workingParent");
        }
    }
    private record Slot(boolean present, Object value, List<String> invalidPrefix) {}
    private ValidationNestObjectCheck() {}

    /** Source facts are tied to this exact parent and output location; blocked facts never select a target. */
    public static void check(ValidationObservation parent, List<String> path, ValidationNestSingleSource.Result source,
            Optional<Target> target, Set<String> containerTypes, ValidationValues.Limits limits, Builder root,
            BiConsumer<ValidationRelationRows.Sample, ValidationValues.Row> matched) {
        Objects.requireNonNull(root, "root");
        try { check(new ValidationNestScope(parent), path, source, target, containerTypes, limits, root, matched); }
        catch (RuntimeException | Error failure) { root.close(); throw failure; }
    }

    /** Local target traversal and complete diagnostic coordinates are distinct for mixed object/array trees. */
    public static void check(ValidationNestScope scope, List<String> path, ValidationNestSingleSource.Result source,
            Optional<Target> target, Set<String> containerTypes, ValidationValues.Limits limits, Builder root,
            BiConsumer<ValidationRelationRows.Sample, ValidationValues.Row> matched) {
        Objects.requireNonNull(root, "root");
        try {
            Objects.requireNonNull(scope, "scope");
            var parent = scope.observation();
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(target, "target");
            Objects.requireNonNull(limits, "limits");
            Objects.requireNonNull(matched, "matched");
            if (path == null || path.isEmpty() || path.size() > 32 || containerTypes == null || containerTypes.size() > 32) {
                throw ValidationError.invalid("objectCheck.path");
            }
            for (String segment : path) ValidationError.identifier(segment, "objectCheck.path");
            for (String type : containerTypes) ValidationError.identifier(type, "objectCheck.containerTypes");
            path = List.copyOf(path); containerTypes = Set.copyOf(containerTypes);
            requireScope(parent, root.observation());
            Location location = scope.location(path);
            if (!source.observation().equals(parent) || !source.location().equals(location)) throw ValidationError.invalid("objectCheck.source");
            root.read(Side.SOURCE, source.read());
            if (source.state() == ValidationNestSingleSource.State.BLOCKED) {
                root.requireBlocked(Check.STRUCTURE, location);
                if (root.method() == ValidationMethod.FULL_COLUMN) root.requireBlocked(Check.VALUES, location);
                return;
            }
            if (root.participation() != ValidationCheckEvent.Participation.SHOULD_EXIST || root.targetPresence() != TargetPresence.PRESENT || target.isEmpty()) {
                throw ValidationError.invalid("objectCheck.targetParent");
            }
            Target actual = target.orElseThrow();
            if (actual.physical().groupStatus() != ValidationCheckEvent.GroupStatus.COMPLETE || actual.physical().observedOccurrences() != 1) {
                throw ValidationError.invalid("objectCheck.targetParent");
            }
            bounded(actual.physical().row(), limits); bounded(actual.workingParent(), limits);
            root.read(Side.TARGET, actual.physical().boundary());
            var targetSample = new SampledSide(actual.physical().identity(), actual.physical().row(), actual.physical().boundary().readId());
            Slot slot = slot(actual.workingParent(), path, containerTypes);
            if (source.state() == ValidationNestSingleSource.State.EMPTY) {
                if (slot.present()) difference(root, Kind.TARGET_STRUCTURE, location, location, Optional.empty(), targetSample);
                return;
            }
            var sourceRow = source.unique().orElseThrow();
            bounded(sourceRow.original(), limits); bounded(sourceRow.working(), limits);
            var sourceSample = sourceRow.physical().map(identity -> new SampledSide(identity, sourceRow.original(), source.read().readId()));
            if (slot.invalidPrefix() != null) {
                difference(root, Kind.TARGET_STRUCTURE, scope.location(slot.invalidPrefix()), location, sourceSample, targetSample);
            } else if (!slot.present()) {
                difference(root, Kind.TARGET_MISSING, location, location, sourceSample, targetSample);
            } else {
                Object object = unwrap(slot.value(), containerTypes);
                if (!(object instanceof Map<?, ?> fields)) difference(root, Kind.TARGET_STRUCTURE, location, location, sourceSample, targetSample);
                else {
                    // The bounded owned parent has already checked every map key and portable value.
                    @SuppressWarnings("unchecked") Map<String, Object> body = (Map<String, Object>) fields;
                    matched.accept(sourceRow, ValidationValues.ownRow(body, limits));
                }
            }
        } catch (RuntimeException | Error failure) {
            root.close();
            throw failure;
        }
    }

    private static Slot slot(ValidationValues.Row row, List<String> path, Set<String> containerTypes) {
        Object value = row.fields();
        for (int i = 0; i < path.size(); i++) {
            value = unwrap(value, containerTypes);
            if (!(value instanceof Map<?, ?> fields)) return new Slot(false, null, path.subList(0, i));
            if (!fields.containsKey(path.get(i))) return new Slot(false, null, null);
            value = fields.get(path.get(i));
        }
        return new Slot(true, value, null);
    }
    private static Object unwrap(Object value, Set<String> containerTypes) {
        while (value instanceof ValidationValues.Typed typed) {
            if (!containerTypes.contains(typed.logicalType())) return value;
            value = typed.value();
        }
        return value;
    }
    private static void difference(Builder root, Kind kind, Location at, Location dependent,
            Optional<SampledSide> source, SampledSide target) {
        Finding finding = new Finding(kind, at);
        root.finding(finding, source, Optional.of(target)).notApplicable(Check.STRUCTURE, dependent, finding);
        if (root.method() == ValidationMethod.FULL_COLUMN) root.notApplicable(Check.VALUES, dependent, finding);
    }
    private static void bounded(ValidationValues.Row row, ValidationValues.Limits limits) {
        if (row.accountedBytes() > limits.bytes() || row.nodes() > limits.nodes() || row.depth() > limits.depth()) {
            throw new io.tapstate.core.common.TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "objectCheck.row"), null);
        }
    }
    private static void requireScope(ValidationObservation actual, ValidationObservation expected) {
        if (!actual.run().equals(expected.run()) || actual.revision() != expected.revision()
                || !actual.root().mappingId().equals(expected.root().mappingId()) || !actual.root().outputId().equals(expected.root().outputId())
                || !actual.root().rootKey().equals(expected.root().rootKey())) throw ValidationError.invalid("objectCheck.observation");
    }
}
