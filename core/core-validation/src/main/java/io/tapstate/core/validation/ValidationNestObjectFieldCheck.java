package io.tapstate.core.validation;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

/** Delegates a complete object subtree without making the parent recompare nested arrays in order. */
public final class ValidationNestObjectFieldCheck {
    private ValidationNestObjectFieldCheck() {}

    public static final class Checked implements ValidationNestCheckedSubtree {
        private final ValidationNestScope scope;
        private final ValidationValues.Row source;
        private final ValidationValues.Row target;
        private final ValidationRootCheck.Builder root;
        private final List<String> path;
        private Checked(ValidationNestScope scope, ValidationValues.Row source, ValidationValues.Row target,
                        ValidationRootCheck.Builder root, List<String> path) {
            this.scope = scope; this.source = source; this.target = target; this.root = root; this.path = List.copyOf(path);
        }
        @Override public List<String> require(ValidationObservation parent, List<Object> prefix,
                ValidationValues.Row source, ValidationValues.Row target, ValidationRootCheck.Builder root) {
            if (!scope.observation().equals(parent) || !scope.objectPath().equals(prefix)
                    || this.source != source || this.target != target || this.root != root) {
                throw ValidationError.invalid("fieldCheck.objectEvidenceScope");
            }
            return path;
        }
    }

    /**
     * All matched content and descendants must complete in the synchronous callback. Missing, extra
     * or malformed target objects are diagnosed by the object checker. A source-blocked object stays
     * an unavailable source path and cannot issue this proof. Parent conflicts must already be
     * checked. Intermediate source prefixes are admitted objects; the object leaf may be absent.
     */
    public static Checked check(ValidationNestScope scope, List<String> path, ValidationValues.Row sourceView,
            ValidationNestSingleSource.Result source, ValidationCheckEvent.Side physicalTarget,
            ValidationValues.Row targetView, Set<String> containerTypes, ValidationValueComparison.TraversalLimits limits,
            ValidationRootCheck.Builder root, BiConsumer<ValidationRelationRows.Sample, ValidationValues.Row> matched) {
        Objects.requireNonNull(root, "root");
        try {
            Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(sourceView, "sourceView");
            Objects.requireNonNull(source, "source"); Objects.requireNonNull(limits, "limits");
            Objects.requireNonNull(matched, "matched");
            if (source.state() == ValidationNestSingleSource.State.BLOCKED || root.method() != ValidationMethod.FULL_COLUMN
                    || path == null || path.isEmpty() || path.size() > 32 || containerTypes == null || containerTypes.size() > 32) {
                throw ValidationError.invalid("fieldCheck.objectSource");
            }
            for (String segment : path) ValidationError.identifier(segment, "fieldCheck.objectPath");
            for (String type : containerTypes) ValidationError.identifier(type, "fieldCheck.objectContainerType");
            if (sourceView.accountedBytes() > limits.row().bytes() || sourceView.nodes() > limits.row().nodes()
                    || sourceView.depth() > limits.row().depth()) {
                throw new io.tapstate.core.common.TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                        Map.of("limit", "fieldCheck.objectRow"), null);
            }
            path = List.copyOf(path); containerTypes = Set.copyOf(containerTypes);
            ValidationNestArrayFieldCheck.sourcePrefixes(sourceView, path, containerTypes, limits.operations());
            ValidationNestObjectCheck.check(scope, path, source,
                    Optional.of(new ValidationNestObjectCheck.Target(physicalTarget, targetView)), containerTypes, limits.row(), root, matched);
            return new Checked(scope, sourceView, targetView, root, path);
        } catch (RuntimeException | Error error) { root.close(); throw error; }
    }
}
