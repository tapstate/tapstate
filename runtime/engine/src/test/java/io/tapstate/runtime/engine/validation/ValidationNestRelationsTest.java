package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.validation.ValidationError;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static io.tapstate.runtime.engine.validation.ValidationNestRelations.*;
import static org.assertj.core.api.Assertions.*;

class ValidationNestRelationsTest {
    private static final Limits LIMITS = new Limits(100, 16, 1024 * 1024);

    @Test void compositeGatheringUsesParentKeyOrderRatherThanOnMapOrderOrArrayIdentity() {
        var on = new LinkedHashMap<String, String>(); on.put("order_id", "id"); on.put("tenant_fk", "tenant");
        var child = embed("items", on, EmbedAs.ARRAY, "items", List.of("row_id"), List.of("sku"), List.of());
        var plan = compile("nest", root(List.of("tenant", "id"), List.of(child)), Set.of("orders", "items"), Map.of(), LIMITS);
        var relation = plan.children().getFirst();
        assertThat(relation.direction()).isEqualTo(Direction.GATHER);
        assertThat(relation.join()).containsExactly(new Pair("tenant_fk", "tenant"), new Pair("order_id", "id"));
        assertThat(relation.rowKey()).contains(List.of("row_id"));
        assertThat(relation.arrayKey()).contains(List.of("sku"));
        assertThat(relation.treePath()).containsExactly("items");
        on.put("extra", "extra");
        assertThat(relation.join()).hasSize(2);
    }

    @Test void nestedReferencesUseTheChildIdentityOrderAndKeepEveryTreeSegment() {
        var on = new LinkedHashMap<String, String>(); on.put("customer_id", "customer_fk"); on.put("tenant", "tenant_fk");
        var owner = embed("customers", on, EmbedAs.OBJECT, "owner", List.of("tenant", "customer_id"), null, List.of());
        var items = embed("items", Map.of("order_id", "id"), EmbedAs.ARRAY, "items", List.of("line_id"), List.of("position"), List.of(owner));
        var plan = compile("nest", root(List.of("id"), List.of(items)), Set.of("orders", "items", "customers"), Map.of(), LIMITS);
        var reference = plan.children().getFirst().children().getFirst();
        assertThat(reference.direction()).isEqualTo(Direction.REFERENCE);
        assertThat(reference.join()).containsExactly(new Pair("tenant", "tenant_fk"), new Pair("customer_id", "customer_fk"));
        assertThat(reference.treePath()).containsExactly("items", "owner");
        assertThat(reference.parentAlias()).isEqualTo("items");
        assertThat(reference.arrayKey()).isEmpty();
    }

    @Test void aOneToOneKeyOnBothSidesKeepsGatheringForEveryShape() {
        for (EmbedAs shape : EmbedAs.values()) {
            var child = embed("profiles", Map.of("id", "id"), shape, shape == EmbedAs.FLAT ? null : "profile", List.of("id"), null, List.of());
            var relation = compile("nest", root(List.of("id"), List.of(child)), Set.of("orders", "profiles"), Map.of(), LIMITS).children().getFirst();
            assertThat(relation.direction()).isEqualTo(Direction.GATHER);
            assertThat(relation.outputPath().isPresent()).isEqualTo(shape != EmbedAs.FLAT);
            if (shape == EmbedAs.FLAT) assertThat(relation.treePath()).containsExactly("$flat[profiles]");
        }
    }

    @Test void unavailableParentIdentityKeepsGatheringEvenWhenTheChildSideIsItsOwnKey() {
        var child = embed("customers", Map.of("id", "customer_fk"), EmbedAs.OBJECT, "owner", List.of("id"), null, List.of());
        var plan = compile("nest", root(null, List.of(child)), Set.of("orders", "customers"), Map.of(), LIMITS);
        assertThat(plan.rootKey()).isEmpty();
        assertThat(plan.children().getFirst().direction()).isEqualTo(Direction.GATHER);
        assertThat(plan.children().getFirst().join()).containsExactly(new Pair("id", "customer_fk"));
        // Relation direction alone does not supply the missing root validation identity or admission.
    }

    @Test void explicitKeysWinThenPrimaryKeysThenOneUniqueIndexWithoutPickingAmongCandidates() {
        var child = embed("items", Map.of("order_id", "business_id"), EmbedAs.ARRAY, "items", null, null, List.of());
        var metadata = Map.of("orders", new Metadata(List.of("physical_id"), List.of(List.of("unique_id"))),
                "items", new Metadata(List.of(), List.of(List.of("line_id"))));
        var plan = compile("nest", root(List.of("business_id"), List.of(child)), Set.of("orders", "items"), metadata, LIMITS);
        assertThat(plan.rootKey()).contains(List.of("business_id"));
        assertThat(plan.children().getFirst().rowKey()).contains(List.of("line_id"));
        assertThat(plan.children().getFirst().arrayKey()).contains(List.of("line_id"));
        assertThat(compile("nest", root(null, List.of()), Set.of("orders"), Map.of("orders", metadata.get("orders")), LIMITS).rootKey())
                .contains(List.of("physical_id"));
        var ambiguous = new Metadata(List.of(), List.of(List.of("a"), List.of("b")));
        assertThat(compile("nest", root(null, List.of()), Set.of("orders"), Map.of("orders", ambiguous), LIMITS).rootKey()).isEmpty();
    }

    @Test void unknownAliasesEmptyJoinsAndKnownNeitherKeyDirectionsAreRejected() {
        for (var child : List.of(embed("missing", Map.of("order_id", "id"), EmbedAs.ARRAY, "items", List.of("row_id"), null, List.of()),
                embed("items", Map.of(), EmbedAs.ARRAY, "items", List.of("row_id"), null, List.of()),
                embed("items", Map.of("other_child", "other_parent"), EmbedAs.ARRAY, "items", List.of("row_id"), null, List.of()))) {
            assertThatThrownBy(() -> compile("nest", root(List.of("id"), List.of(child)), Set.of("orders", "items"), Map.of(), LIMITS))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED));
        }
        var missing = embed("items", Map.of("row_id", "foreign"), EmbedAs.OBJECT, "item", null, null, List.of());
        assertThatThrownBy(() -> compile("nest", root(List.of("id"), List.of(missing)), Set.of("orders", "items"), Map.of(), LIMITS))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("rule", "nest.rowKeyUnavailable"));
    }

    @Test void repeatedPathsAndPrefixOverlapsAreConfigProblemsButFlatCardinalityIsNotGuessed() {
        var a = embed("items", Map.of("order_id", "id"), EmbedAs.OBJECT, "address", List.of("row_id"), null, List.of());
        var b = embed("profiles", Map.of("order_id", "id"), EmbedAs.ARRAY, "address.city", List.of("profile_id"), null, List.of());
        assertThatThrownBy(() -> compile("nest", root(List.of("id"), List.of(a, b)), Set.of("orders", "items", "profiles"), Map.of(), LIMITS))
                .isInstanceOf(TapstateException.class);
        var flat = embed("profiles", Map.of("order_id", "id"), EmbedAs.FLAT, null, List.of("profile_id"), null, List.of());
        assertThat(compile("nest", root(List.of("id"), List.of(a, flat)), Set.of("orders", "items", "profiles"), Map.of(), LIMITS).children())
                .hasSize(2);
        assertThatThrownBy(() -> compile("nest", root(List.of("id"), List.of(flat, flat)), Set.of("orders", "profiles"), Map.of(), LIMITS))
                .isInstanceOf(TapstateException.class);
    }

    @Test void nodeDepthAndRetainedMetadataBudgetsAreHardBounds() {
        var nested = embed("details", Map.of("parent_id", "row_id"), EmbedAs.ARRAY, "details", List.of("detail_id"), null, List.of());
        var child = embed("items", Map.of("order_id", "id"), EmbedAs.ARRAY, "items", List.of("row_id"), null, List.of(nested));
        for (var limits : List.of(new Limits(1, 16, 10000), new Limits(10, 1, 10000), new Limits(10, 16, 1))) {
            assertThatThrownBy(() -> compile("nest", root(List.of("id"), List.of(child)), Set.of("orders", "items", "details"), Map.of(), limits))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
        }
    }

    private static NestRoot root(List<String> key, List<Embed> children) { return new NestRoot("orders", key, null, null, children); }
    private static Embed embed(String alias, Map<String, String> on, EmbedAs shape, String path, List<String> key, List<String> arrayKey, List<Embed> children) {
        return new Embed(alias, on, shape, path, key, arrayKey, true, true, children);
    }
}
