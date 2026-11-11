package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationKeyBinding;
import io.tapstate.core.validation.ValidationReferenceFields;
import io.tapstate.core.validation.ValidationValues;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Binds a compiled relation's working-parent fields to an explicitly confirmed physical lookup key.
 * No lineage or inverse projection is inferred. The caller must admit that the physical predicate
 * reads exactly the declared current relationship, including any prior static projections. Native
 * encoding, model versions, indexing and complete-query/resource admission remain separate.
 */
public final class ValidationNestLookupBinding {
    public sealed interface Result permits Ready, NoReference, Invalid, Unresolved {}
    public record Ready(ValidationKey key) implements Result {
        public Ready { Objects.requireNonNull(key, "key"); }
    }
    public enum NoReference implements Result { INSTANCE }
    public record Invalid(List<ValidationKeyBinding.Problem> problems) implements Result {
        public Invalid {
            if (problems == null || problems.isEmpty() || problems.size() > 32) throw invalid("nestLookup.problems");
            problems = List.copyOf(problems);
        }
    }

    /**
     * Unknown projected components are not missing/NULL keys. Preserve independently proven presence
     * problems, but do not build a query. The caller retains the original source causes and masks.
     */
    public record Unresolved(List<String> fields, List<ValidationKeyBinding.Problem> knownProblems) implements Result {
        public Unresolved {
            if (fields == null || fields.isEmpty() || fields.size() > 32 || knownProblems == null || knownProblems.size() > 32)
                throw invalid("nestLookup.unresolved");
            for (String field : fields) ValidationError.fieldName(field, "nestLookup.unresolvedField");
            fields = List.copyOf(fields); knownProblems = List.copyOf(knownProblems);
        }
    }

    private final ValidationNestRelations.Relation relation;
    private final ValidationKeyBinding physical;
    private final List<ValidationKeyBinding> parentComponents;
    private final ValidationReferenceFields reference;
    private final ValidationValues.Limits rowLimits;

    public ValidationNestLookupBinding(String node, ValidationNestRelations.Relation relation,
            List<ValidationKeyBinding.Component> parentComponents, ValidationKeyBinding physical, ValidationValues.Limits rowLimits) {
        this.relation = Objects.requireNonNull(relation, "relation");
        this.physical = Objects.requireNonNull(physical, "physical");
        Objects.requireNonNull(parentComponents, "parentComponents");
        this.rowLimits = Objects.requireNonNull(rowLimits, "rowLimits");
        if (relation.join().isEmpty() || relation.join().size() > 32 || parentComponents.size() != relation.join().size()
                || physical.components().size() != relation.join().size()) throw invalid("nestLookup.arity");
        var bindings = new ArrayList<ValidationKeyBinding>();
        var referenceFields = new LinkedHashSet<String>();
        for (int i = 0; i < relation.join().size(); i++) {
            var component = Objects.requireNonNull(parentComponents.get(i), "parent component");
            if (!component.path().equals(List.of(relation.join().get(i).parent()))
                    || component.domain() != physical.components().get(i).domain()
                    || physical.components().get(i).path().size() != 1) throw invalid("nestLookup.component");
            // A reference may use the same parent field for distinct components of a child identity.
            bindings.add(new ValidationKeyBinding(node, List.of(component), Set.of(), rowLimits));
            referenceFields.add(relation.join().get(i).parent());
        }
        this.parentComponents = List.copyOf(bindings);
        reference = new ValidationReferenceFields(List.copyOf(referenceFields), rowLimits);
    }

    public ValidationKeyBinding physical() { return physical; }
    public ValidationNestRelations.Relation relation() { return relation; }

    /**
     * Uses the projection's knowledge before inspecting reference values. An explicit static assignment
     * can establish a field within a whole-unknown row; a specific unknown descendant still blocks a
     * scalar lookup component. Literal dotted column names remain distinct from nested paths.
     */
    public Result prepare(ValidationStaticMap.Projection parent) {
        Objects.requireNonNull(parent, "parent");
        var row = parent.row();
        if (row.accountedBytes() > rowLimits.bytes() || row.nodes() > rowLimits.nodes() || row.depth() > rowLimits.depth()) {
            throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "nestLookup.row"), null);
        }
        var unavailable = new LinkedHashSet<String>();
        for (var join : relation.join()) {
            String field = join.parent();
            for (var path : parent.unavailablePaths()) {
                if (path.isEmpty() ? !parent.definedFields().contains(field) : path.getFirst().equals(field)) {
                    unavailable.add(field); break;
                }
            }
        }
        if (unavailable.isEmpty()) return prepare(row);
        var problems = new ArrayList<ValidationKeyBinding.Problem>();
        long keyBytes = 32;
        for (int i = 0; i < parentComponents.size(); i++) {
            String field = relation.join().get(i).parent();
            if (unavailable.contains(field)) continue;
            if (relation.direction() == ValidationNestRelations.Direction.REFERENCE) {
                // A missing component is definite even if another component later proves NULL. A
                // type error is not: an unresolved component may instead make this no-reference.
                if (!row.fields().containsKey(field)) problems.add(new ValidationKeyBinding.Problem(i,
                        List.of(field), ValidationKeyBinding.ProblemKind.MISSING_FIELD));
            } else {
                var bound = parentComponents.get(i).read(row);
                if (bound instanceof ValidationKeyBinding.Invalid bad) {
                    for (var problem : bad.problems()) problems.add(new ValidationKeyBinding.Problem(i, problem.path(), problem.kind()));
                } else {
                    keyBytes += ((ValidationKeyBinding.Bound) bound).key().parts().getFirst().retainedBytes();
                    if (keyBytes > ValidationKey.MAX_KEY_BYTES) throw new TapstateException(ValidationError.KEY_LIMIT_EXCEEDED,
                            Map.of("limit", "keyBytes"), null);
                }
            }
        }
        return new Unresolved(new ArrayList<>(unavailable), problems);
    }

    /** Missing takes precedence over NULL for references; gathering never issues a NULL key query. */
    public Result prepare(ValidationValues.Row workingParent) {
        if (relation.direction() == ValidationNestRelations.Direction.REFERENCE) {
            var presence = reference.read(workingParent);
            if (presence instanceof ValidationReferenceFields.NoReference) return NoReference.INSTANCE;
            if (presence instanceof ValidationReferenceFields.Missing missing) {
                var problems = new ArrayList<ValidationKeyBinding.Problem>();
                for (int i = 0; i < relation.join().size(); i++) {
                    String field = relation.join().get(i).parent();
                    if (missing.fields().contains(field)) problems.add(new ValidationKeyBinding.Problem(i, List.of(field), ValidationKeyBinding.ProblemKind.MISSING_FIELD));
                }
                return new Invalid(problems);
            }
        }
        var parts = new ArrayList<ValidationKey.Part>();
        long keyBytes = 32;
        var problems = new ArrayList<ValidationKeyBinding.Problem>();
        for (int i = 0; i < parentComponents.size(); i++) {
            var bound = parentComponents.get(i).read(workingParent);
            if (bound instanceof ValidationKeyBinding.Bound value) {
                var part = value.key().parts().getFirst();
                keyBytes += part.retainedBytes();
                if (keyBytes > ValidationKey.MAX_KEY_BYTES) {
                    throw new TapstateException(ValidationError.KEY_LIMIT_EXCEEDED, Map.of("limit", "keyBytes"), null);
                }
                parts.add(part);
            } else for (var problem : ((ValidationKeyBinding.Invalid) bound).problems()) {
                problems.add(new ValidationKeyBinding.Problem(i, problem.path(), problem.kind()));
            }
        }
        return problems.isEmpty() ? new Ready(new ValidationKey(parts)) : new Invalid(problems);
    }

    /** The parent uses its own admitted row budget, independently of any child reader's limit. */
    ValidationReferenceFields.Complete requireReferenceFields(ValidationValues.Row workingParent) {
        if (relation.direction() != ValidationNestRelations.Direction.REFERENCE) throw invalid("nestLookup.referenceDirection");
        if (reference.read(workingParent) instanceof ValidationReferenceFields.Complete complete) return complete;
        throw invalid("nestLookup.referenceNotReady");
    }

    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
}
