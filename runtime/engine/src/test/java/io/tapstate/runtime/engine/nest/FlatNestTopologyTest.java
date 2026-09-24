package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class FlatNestTopologyTest {

    @Test
    void oneToOneAndManyToOneFlatRelationsBothCompile() {
        NestTopology oneToOne = compile(
                flat("profile", "customer_id", "id"),
                tables(
                        table("customer", "customers", List.of("id"), "id", "name"),
                        table("profile", "profiles", List.of("profile_id"), "profile_id", "customer_id", "tier")));
        NestTopology manyToOne = compile(
                flat("region", "region_id", "region_ref"),
                tables(
                        table("customer", "customers", List.of("id"), "id", "region_ref"),
                        table("region", "regions", List.of("region_id"), "region_id", "name")));

        assertThat(oneToOne.slots().getFirst().isReference()).isFalse();
        assertThat(manyToOne.slots().getFirst().isReference()).isTrue();
        assertThat(oneToOne.slots().getFirst().path()).isNull();
        assertThat(manyToOne.slots().getFirst().path()).isNull();
    }

    @Test
    void aPotentialOneToManyFlatRelationIsNotRejectedBeforeRowsArrive() {
        NestTopology topology = compile(
                flat("event", "customer_id", "id"),
                tables(
                        table("customer", "customers", List.of("id"), "id"),
                        table("event", "events", List.of("event_id"), "event_id", "customer_id", "kind")));

        assertThat(topology.slots()).hasSize(1);
        assertThat(topology.slots().getFirst().isReference()).isFalse();
    }

    @Test
    void discoveredModelsRefuseAFlatFieldAlreadyOwnedByTheParent() {
        assertThatThrownBy(() -> compile(
                flat("profile", "customer_id", "id"),
                tables(
                        table("customer", "customers", List.of("id"), "id", "name"),
                        table("profile", "profiles", List.of("profile_id"), "profile_id", "name"))))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(NestError.FLAT_FIELD_CONFLICT);
                    assertThat(error.args()).containsEntry("fields", "name");
                    assertThat(error.args()).containsEntry("occupiedBy", "$root row");
                });
    }

    @Test
    void discoveredModelsRefuseConflictsAcrossFlatSiblingsAndNestedPaths() {
        Embed address = new Embed(
                "address",
                Map.of("customer_id", "id"),
                EmbedAs.OBJECT,
                "contact.address",
                null,
                null,
                null,
                null);
        Function<String, NestTable> models = tables(
                table("customer", "customers", List.of("id"), "id"),
                table("profile", "profiles", List.of("profile_id"), "profile_id", "contact"),
                table("preference", "preferences", List.of("preference_id"), "preference_id", "contact"),
                table("address", "addresses", List.of("address_id"), "address_id"));

        assertThatThrownBy(() -> compile(models,
                flat("profile", "customer_id", "id"),
                flat("preference", "customer_id", "id")))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(NestError.FLAT_FIELD_CONFLICT));
        assertThatThrownBy(() -> compile(models, flat("profile", "customer_id", "id"), address))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(NestError.FLAT_FIELD_CONFLICT));
    }

    @Test
    void absentModelsDeferFieldCollisionChecksToRuntime() {
        NestTopology topology = compile(
                flat("profile", "customer_id", "id"),
                tables(
                        new NamedNestTable("customer", new NestTable("customers", List.of("id"))),
                        new NamedNestTable("profile", new NestTable("profiles", List.of("profile_id")))));

        assertThat(topology.slots()).hasSize(1);
    }

    @Test
    void pathlessFlatSiblingsNeedDistinctAliasesForDurableIdentity() {
        assertThatThrownBy(() -> compile(tables(
                        table("customer", "customers", List.of("id"), "id"),
                        table("profile", "profiles", List.of("profile_id"), "profile_id", "a", "b")),
                flat("profile", "customer_id", "id"),
                flat("profile", "customer_id", "id")))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(NestError.FLAT_EMBED_ALIAS_CONFLICT);
                    assertThat(error.args()).containsEntry("alias", "profile");
                });
    }

    private static NestTopology compile(Embed embed, Function<String, NestTable> tables) {
        return compile(tables, embed);
    }

    private static NestTopology compile(Function<String, NestTable> tables, Embed... embeds) {
        TransformBody.Nest nest = new TransformBody.Nest(
                null, null, new NestRoot("customer", List.of("id"), null, null, List.of(embeds)));
        return NestTopology.compile("p", "document", nest, tables);
    }

    private static Embed flat(String alias, String childField, String parentField) {
        return new Embed(
                alias,
                Map.of(childField, parentField),
                EmbedAs.FLAT,
                null,
                null,
                null,
                null,
                null);
    }

    private static NamedNestTable table(String alias, String table, List<String> key, String... fields) {
        return new NamedNestTable(alias, new NestTable(table, key, List.of(), List.of(fields)));
    }

    private static Function<String, NestTable> tables(NamedNestTable... tables) {
        Map<String, NestTable> byAlias = new LinkedHashMap<>();
        for (NamedNestTable named : tables) {
            byAlias.put(named.alias(), named.table());
        }
        return byAlias::get;
    }

    /** Test-only carrier that lets the varargs table fixture retain its alias. */
    private record NamedNestTable(String alias, NestTable table) {}
}
