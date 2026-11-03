package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.validation.*;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static io.tapstate.runtime.engine.validation.ValidationNestLookupBinding.*;
import static org.assertj.core.api.Assertions.*;

class ValidationNestLookupBindingTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 100, 16);

    @Test void compoundParentValuesFollowCompiledDirectionAndExplicitPhysicalNamesWithoutLoss() {
        var on = new LinkedHashMap<String, String>(); on.put("child_id", "customer"); on.put("tenant", "tenant");
        var relation = relation(on, List.of("tenant", "child_id"), List.of("order_id"));
        var physical = physical(List.of("tenant_db", "customer_db"));
        var binding = new ValidationNestLookupBinding("nest", relation, List.of(component("tenant"), component("customer")), physical, ROW);
        BigInteger exact = new BigInteger("9007199254740993123456789");
        var raw = ValidationValues.ownRow(Map.of("tenant", 7, "customer", exact), ROW);
        var ready = (Ready) binding.prepare(raw);
        assertThat(ready.key()).isEqualTo(new ValidationKey(List.of(ValidationKey.Part.number(7), ValidationKey.Part.number(exact))));
        assertThat(binding.physical()).isSameAs(physical);
        assertThat(raw.fields()).containsEntry("customer", exact).doesNotContainKey("customer_db");
    }

    @Test void referencesTreatNullAsNoReferenceButMissingStillWins() {
        var relation = relation(Map.of("id", "customer", "tenant", "tenant"), List.of("tenant", "id"), List.of("order_id"));
        var binding = new ValidationNestLookupBinding("nest", relation, List.of(component("tenant"), component("customer")), physical(List.of("tenant_db", "id_db")), ROW);
        var fields = new HashMap<String, Object>(); fields.put("tenant", 7); fields.put("customer", null);
        assertThat(binding.prepare(ValidationValues.ownRow(fields, ROW))).isEqualTo(NoReference.INSTANCE);
        fields.remove("tenant");
        var missing = (Invalid) binding.prepare(ValidationValues.ownRow(fields, ROW));
        assertThat(missing.problems()).containsExactly(new ValidationKeyBinding.Problem(0, List.of("tenant"), ValidationKeyBinding.ProblemKind.MISSING_FIELD));
    }

    @Test void aGatheringNullIdentityIsADataIssueRatherThanAnEmptyRelationOrNullQuery() {
        var relation = relation(Map.of("order_id", "id"), List.of("child_id"), List.of("id"));
        var binding = new ValidationNestLookupBinding("nest", relation, List.of(component("id")), physical(List.of("order_db")), ROW);
        var fields = new HashMap<String, Object>(); fields.put("id", null);
        var result = (Invalid) binding.prepare(ValidationValues.ownRow(fields, ROW));
        assertThat(result.problems()).containsExactly(new ValidationKeyBinding.Problem(0, List.of("id"), ValidationKeyBinding.ProblemKind.NULL_COMPONENT));
    }

    @Test void repeatedParentFieldsCanSupplyDistinctChildIdentityComponents() {
        var on = new LinkedHashMap<String, String>(); on.put("a", "tenant"); on.put("b", "tenant");
        var relation = relation(on, List.of("a", "b"), List.of("id"));
        var binding = new ValidationNestLookupBinding("nest", relation, List.of(component("tenant"), component("tenant")), physical(List.of("a_db", "b_db")), ROW);
        assertThat(((Ready) binding.prepare(ValidationValues.ownRow(Map.of("tenant", 7), ROW))).key())
                .isEqualTo(new ValidationKey(List.of(ValidationKey.Part.number(7), ValidationKey.Part.number(7))));
    }

    @Test void incompatibleBindingsAreRefusedBeforeAnyParentIsRead() {
        var relation = relation(Map.of("id", "customer"), List.of("id"), List.of("order_id"));
        assertThatThrownBy(() -> new ValidationNestLookupBinding("nest", relation, List.of(component("wrong")), physical(List.of("customer_db")), ROW))
                .isInstanceOf(TapstateException.class);
        var text = new ValidationKeyBinding("physical", List.of(new ValidationKeyBinding.Component(List.of("customer_db"), ValidationKey.Kind.TEXT, Set.of("text"))), Set.of(), ROW);
        assertThatThrownBy(() -> new ValidationNestLookupBinding("nest", relation, List.of(component("customer")), text, ROW))
                .isInstanceOf(TapstateException.class);
    }

    @Test void aDataProblemDoesNotHideAnUnknownLaterWrapperOrAHardRowLimit() {
        var relation = relation(Map.of("a", "a", "b", "b"), List.of("a", "b"), List.of("a", "b"));
        var binding = new ValidationNestLookupBinding("nest", relation, List.of(component("a"), component("b")), physical(List.of("a_db", "b_db")), ROW);
        assertThatThrownBy(() -> binding.prepare(ValidationValues.ownRow(Map.of("b", new ValidationValues.Typed("unadmitted", null, "codec", 1)), ROW)))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED));
        var bounded = new ValidationNestLookupBinding("nest", relation, List.of(component("a"), component("b")), physical(List.of("a_db", "b_db")),
                new ValidationValues.Limits(1, 1, 1));
        assertThatThrownBy(() -> bounded.prepare(ValidationValues.ownRow(Map.of("a", 1, "b", 2), ROW)))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
    }

    @Test void anInvalidComponentCannotHideAggregateKeyBudgetExhaustion() {
        var limits = new ValidationValues.Limits(256 * 1024, 100, 16);
        var on = new LinkedHashMap<String, String>();
        var parent = new java.util.ArrayList<ValidationKeyBinding.Component>();
        var physical = new java.util.ArrayList<ValidationKeyBinding.Component>();
        var values = new LinkedHashMap<String, Object>();
        for (int i = 0; i < 6; i++) {
            on.put("child" + i, "parent" + i);
            var domain = i == 0 ? ValidationKey.Kind.EXACT_NUMBER : ValidationKey.Kind.TEXT;
            var types = Set.of(i == 0 ? "exact-number" : "text");
            parent.add(new ValidationKeyBinding.Component(List.of("parent" + i), domain, types));
            physical.add(new ValidationKeyBinding.Component(List.of("physical" + i), domain, types));
            values.put("parent" + i, i == 0 ? "invalid-number" : "x".repeat(7000));
        }
        var relation = relation(on, List.copyOf(on.keySet()), List.of("id"));
        var binding = new ValidationNestLookupBinding("nest", relation, parent,
                new ValidationKeyBinding("physical", physical, Set.of(), limits), limits);
        assertThatThrownBy(() -> binding.prepare(ValidationValues.ownRow(values, limits)))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.KEY_LIMIT_EXCEEDED));
    }

    @Test void unknownProjectedReferenceDoesNotBecomeMissingNullOrAQueryKey() {
        var relation = relation(Map.of("id", "customer", "tenant", "tenant"), List.of("tenant", "id"), List.of("order_id"));
        var binding = new ValidationNestLookupBinding("nest", relation, List.of(component("tenant"), component("customer")),
                physical(List.of("tenant_db", "id_db")), ROW);
        var identity = ValidationStaticMap.compile("identity", new TransformBody.MapProjection(Map.of()), ROW);
        var nullReference = new HashMap<String, Object>(); nullReference.put("tenant", 7); nullReference.put("customer", null);
        for (Map<String, Object> fields : List.<Map<String, Object>>of(Map.of("tenant", 7), Map.of("tenant", 7, "customer", 2), nullReference)) {
            var partial = identity.apply(ValidationValues.ownRow(fields, ROW), List.of(List.of("customer")));
            assertThat(binding.prepare(partial)).isEqualTo(new Unresolved(List.of("customer"), List.of()));
        }
        var knownNull = new HashMap<String, Object>(); knownNull.put("tenant", null);
        assertThat(binding.prepare(identity.apply(ValidationValues.ownRow(knownNull, ROW), List.of(List.of("customer")))))
                .isEqualTo(new Unresolved(List.of("customer"), List.of()));
        var missing = (Unresolved) binding.prepare(identity.apply(ValidationValues.ownRow(Map.of(), ROW), List.of(List.of("customer"))));
        assertThat(missing.knownProblems()).containsExactly(new ValidationKeyBinding.Problem(0, List.of("tenant"), ValidationKeyBinding.ProblemKind.MISSING_FIELD));
    }

    @Test void staticOverridesCanEstablishALookupFieldWithoutResolvingUnrelatedUnknowns() {
        var relation = relation(Map.of("id", "customer"), List.of("id"), List.of("order_id"));
        var binding = new ValidationNestLookupBinding("nest", relation, List.of(component("customer")), physical(List.of("customer_db")), ROW);
        var literal = ValidationStaticMap.compile("literal", new TransformBody.MapProjection(Map.of("customer", new FieldRule.Literal(7))), ROW);
        var ready = (Ready) binding.prepare(literal.apply(ValidationValues.ownRow(Map.of(), ROW), List.of(List.of())));
        assertThat(ready.key()).isEqualTo(new ValidationKey(List.of(ValidationKey.Part.number(7))));
        var drop = ValidationStaticMap.compile("drop", new TransformBody.MapProjection(Map.of("customer", new FieldRule.Drop())), ROW);
        assertThat(binding.prepare(drop.apply(ValidationValues.ownRow(Map.of(), ROW), List.of(List.of())))).isInstanceOf(Invalid.class);
        var identity = ValidationStaticMap.compile("identity", new TransformBody.MapProjection(Map.of()), ROW);
        assertThat(binding.prepare(identity.apply(ValidationValues.ownRow(Map.of("customer", 7), ROW), List.of(List.of("sibling")))))
                .isEqualTo(ready);
    }

    @Test void unknownGatheringComponentRetainsKnownNullProblemAndNeverBypassesRowBudget() {
        var relation = relation(Map.of("order", "id", "tenant", "tenant"), List.of("child_id"), List.of("id", "tenant"));
        var binding = new ValidationNestLookupBinding("nest", relation, List.of(component("id"), component("tenant")),
                physical(List.of("order_db", "tenant_db")), ROW);
        var fields = new HashMap<String, Object>(); fields.put("id", null);
        var identity = ValidationStaticMap.compile("identity", new TransformBody.MapProjection(Map.of()), ROW);
        var partial = identity.apply(ValidationValues.ownRow(fields, ROW), List.of(List.of("tenant")));
        var result = (Unresolved) binding.prepare(partial);
        assertThat(result.knownProblems()).containsExactly(new ValidationKeyBinding.Problem(0, List.of("id"), ValidationKeyBinding.ProblemKind.NULL_COMPONENT));
        var tiny = new ValidationNestLookupBinding("nest", relation, List.of(component("id"), component("tenant")),
                physical(List.of("order_db", "tenant_db")), new ValidationValues.Limits(1, 1, 1));
        assertThatThrownBy(() -> tiny.prepare(partial)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
    }

    @Test void literalDottedLookupColumnsAreNotNestedKnowledgePaths() {
        var relation = relation(Map.of("id", "a.b"), List.of("id"), List.of("order_id"));
        var binding = new ValidationNestLookupBinding("nest", relation, List.of(component("a.b")), physical(List.of("id_db")), ROW);
        var identity = ValidationStaticMap.compile("identity", new TransformBody.MapProjection(Map.of()), ROW);
        var row = ValidationValues.ownRow(Map.of("a.b", 7), ROW);
        assertThat(binding.prepare(identity.apply(row, List.of(List.of("a", "b"))))).isInstanceOf(Ready.class);
        assertThat(binding.prepare(identity.apply(row, List.of(List.of("a.b")))))
                .isEqualTo(new Unresolved(List.of("a.b"), List.of()));
    }

    @Test void partialGatheringCannotHideUnsupportedKnownValuesOrAggregateKeyExhaustion() {
        var relation = relation(Map.of("a", "a", "b", "b", "c", "c"), List.of("child_id"), List.of("a", "b", "c"));
        var binding = new ValidationNestLookupBinding("nest", relation, List.of(component("a"), component("b"), component("c")),
                physical(List.of("a_db", "b_db", "c_db")), ROW);
        var identity = ValidationStaticMap.compile("identity", new TransformBody.MapProjection(Map.of()), ROW);
        var partial = identity.apply(ValidationValues.ownRow(Map.of("c", new ValidationValues.Typed("unadmitted", null, "codec", 1)), ROW),
                List.of(List.of("a")));
        assertThatThrownBy(() -> binding.prepare(partial)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED));
        var large = new ValidationValues.Limits(256 * 1024, 100, 16);
        var on = new LinkedHashMap<String, String>(); var values = new LinkedHashMap<String, Object>();
        var components = new java.util.ArrayList<ValidationKeyBinding.Component>();
        for (int i = 0; i < 7; i++) {
            String field = "key" + i; on.put(field, field); values.put(field, "x".repeat(7000));
            components.add(new ValidationKeyBinding.Component(List.of(field), ValidationKey.Kind.TEXT, Set.of("text")));
        }
        var gathering = relation(on, List.of("child_id"), List.copyOf(on.keySet()));
        var bounded = new ValidationNestLookupBinding("nest", gathering, components,
                new ValidationKeyBinding("physical", components, Set.of(), large), large);
        var view = ValidationStaticMap.compile("identity", new TransformBody.MapProjection(Map.of()), large)
                .apply(ValidationValues.ownRow(values, large), List.of(List.of("key0")));
        assertThatThrownBy(() -> bounded.prepare(view)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(ValidationError.KEY_LIMIT_EXCEEDED));
    }

    private static ValidationNestRelations.Relation relation(Map<String, String> on, List<String> childKey, List<String> parentKey) {
        var embed = new Embed("child", on, EmbedAs.OBJECT, "child", childKey, null, null, null, List.of());
        return ValidationNestRelations.compile("nest", new NestRoot("parent", parentKey, null, null, List.of(embed)), Set.of("parent", "child"),
                Map.of(), new ValidationNestRelations.Limits(10, 10, 65536)).children().getFirst();
    }
    private static ValidationKeyBinding.Component component(String field) {
        return new ValidationKeyBinding.Component(List.of(field), ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number"));
    }
    private static ValidationKeyBinding physical(List<String> fields) {
        return new ValidationKeyBinding("physical", fields.stream().map(ValidationNestLookupBindingTest::component).toList(), Set.of(), ROW);
    }
}
