package io.tapstate.core.validation;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static io.tapstate.core.validation.ValidationRootCheck.Builder;

/**
 * Checks a declared array before handing its exact subtree to the surrounding field comparison.
 * Complete independent source membership supplies the array facts; target content never supplies
 * expected values. The compiled caller checks every unique pair's values and descendants. This is
 * synchronous local composition, not a root completion, publication permit or reusable cache entry.
 */
public final class ValidationNestArrayFieldCheck {
    private ValidationNestArrayFieldCheck() {}

    /** Opaque evidence tied to this builder, observation, and the same immutable working row objects. */
    public static final class Checked implements ValidationNestCheckedSubtree {
        private final ValidationNestScope scope;
        private final ValidationValues.Row source;
        private final ValidationValues.Row target;
        private final Builder root;
        private final List<String> path;

        private Checked(ValidationNestScope scope, ValidationValues.Row source, ValidationValues.Row target,
                Builder root, List<String> path) {
            this.scope = scope; this.source = source; this.target = target; this.root = root;
            this.path = List.copyOf(path);
        }

        @Override public List<String> require(ValidationObservation parent, List<Object> prefix, ValidationValues.Row source, ValidationValues.Row target, Builder root) {
            if (!scope.observation().equals(parent) || !scope.objectPath().equals(prefix) || this.source != source || this.target != target || this.root != root) {
                throw ValidationError.invalid("fieldCheck.arrayEvidenceScope");
            }
            return path;
        }
    }

    /**
     * Consumes the source-complete membership frame on success or failure. A malformed target is
     * diagnosed before returning evidence; it is never converted into an empty membership. Retain
     * the existing row/member budgets plus one bounded path per evidence object until comparison.
     * Source conflicts at the parent field must be handled before calling this method. Intermediate
     * source prefixes must already be present admitted objects; only the array leaf may be absent
     * from the surrounding view because its facts are carried separately by membership.
     */
    public static Checked check(ValidationObservation parent, List<String> arrayPath, ValidationValues.Row sourceView,
            ValidationCheckEvent.Side physicalTarget, ValidationValues.Row targetView, ValidationKeyBinding elementKey,
            Optional<ValidationKeyBinding> rowKey, Set<String> containerTypes, ValidationNestTargetArray.Limits limits,
            ValidationNestArrayMembers members, Builder root, Consumer<ValidationNestArrayMembers.Group> uniquePair) {
        Objects.requireNonNull(root, "root"); Objects.requireNonNull(members, "members");
        try { return check(new ValidationNestScope(parent), arrayPath, sourceView, physicalTarget, targetView, elementKey, rowKey,
                containerTypes, limits, members, root, uniquePair); }
        catch (RuntimeException | Error failure) { members.close(); root.close(); throw failure; }
    }

    public static Checked check(ValidationNestScope scope, List<String> arrayPath, ValidationValues.Row sourceView,
            ValidationCheckEvent.Side physicalTarget, ValidationValues.Row targetView, ValidationKeyBinding elementKey,
            Optional<ValidationKeyBinding> rowKey, Set<String> containerTypes, ValidationNestTargetArray.Limits limits,
            ValidationNestArrayMembers members, Builder root, Consumer<ValidationNestArrayMembers.Group> uniquePair) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(members, "members");
        try {
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(sourceView, "sourceView");
            Objects.requireNonNull(uniquePair, "uniquePair");
            Objects.requireNonNull(limits, "limits");
            if (root.method() != ValidationMethod.FULL_COLUMN) throw ValidationError.invalid("fieldCheck.arrayMethod");
            if (sourceView.accountedBytes() > limits.row().bytes() || sourceView.nodes() > limits.row().nodes()
                    || sourceView.depth() > limits.row().depth()) {
                throw new io.tapstate.core.common.TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                        java.util.Map.of("limit", "fieldCheck.arrayRow"), null);
            }
            if (arrayPath == null || arrayPath.isEmpty() || arrayPath.size() > 32 || containerTypes == null || containerTypes.size() > 32)
                throw ValidationError.invalid("fieldCheck.arrayPath");
            for (String segment : arrayPath) ValidationError.identifier(segment, "fieldCheck.arrayPath");
            for (String type : containerTypes) ValidationError.identifier(type, "fieldCheck.arrayContainerType");
            arrayPath = List.copyOf(arrayPath); containerTypes = Set.copyOf(containerTypes);
            var remaining = sourcePrefixes(sourceView, arrayPath, containerTypes, limits.operations());
            var state = ValidationNestTargetArray.read(scope, arrayPath, physicalTarget, targetView, elementKey,
                    rowKey, containerTypes, new ValidationNestTargetArray.Limits(limits.row(), limits.elements(), remaining), members, root);
            if (state == ValidationNestTargetArray.State.COMPLETE) ValidationNestArrayCheck.check(members.finish(), root, uniquePair);
            return new Checked(scope, sourceView, targetView, root, arrayPath);
        } catch (RuntimeException | Error failure) {
            members.close(); root.close();
            throw failure;
        }
    }

    static long sourcePrefixes(ValidationValues.Row source, List<String> path, Set<String> containers, long operations) {
        Object current = source.fields();
        for (int i = 0; i < path.size() - 1; i++) {
            operations = work(operations, 1L + path.get(i).length());
            if (!(current instanceof Map<?, ?> object) || !object.containsKey(path.get(i))) throw ValidationError.invalid("fieldCheck.arraySourcePrefix");
            current = object.get(path.get(i));
            while (current instanceof ValidationValues.Typed typed) {
                operations = work(operations, 1L + typed.logicalType().length());
                if (!containers.contains(typed.logicalType())) throw ValidationError.invalid("fieldCheck.arraySourcePrefix");
                current = typed.value();
            }
            if (!(current instanceof Map<?, ?>)) throw ValidationError.invalid("fieldCheck.arraySourcePrefix");
        }
        return operations;
    }

    private static long work(long remaining, long amount) {
        if (amount >= remaining) throw new io.tapstate.core.common.TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                Map.of("limit", "fieldCheck.arrayPrefixOperations"), null);
        return remaining - amount;
    }
}
