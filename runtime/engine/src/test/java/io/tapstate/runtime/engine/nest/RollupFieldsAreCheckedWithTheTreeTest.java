package io.tapstate.runtime.engine.nest;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.aggregate.RollupError;
import io.tapstate.core.dsl.aggregate.ValueType;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.RollupField;
import io.tapstate.core.model.RollupFunction;
import io.tapstate.core.model.RollupPick;
import io.tapstate.core.model.TransformBody;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What compiling a nest tree settles about its rollup fields before a row arrives: where each lands, what
 * it may read - the element's columns and the rows its references resolve to - and the types it computes in.
 */
class RollupFieldsAreCheckedWithTheTreeTest {

    static final Map<String, NestTable> TABLES = Map.of(
            "t", new NestTable("transactions", List.of("id"), List.of(),
                    List.of("id", "player_id"), Map.of("id", ValueType.INT64, "player_id", ValueType.INT64)),
            "i", new NestTable("items", List.of("id"), List.of(),
                    List.of("id", "transaction_id", "prize_definition_id", "amount", "price"),
                    Map.of("id", ValueType.INT64, "transaction_id", ValueType.INT64,
                            "prize_definition_id", ValueType.INT64, "amount", ValueType.INT64,
                            "price", ValueType.decimal(2))),
            "pd", new NestTable("prize_definitions", List.of("id"), List.of(),
                    List.of("id", "prize_code", "prize_value"),
                    Map.of("id", ValueType.INT64, "prize_code", ValueType.of(TapstateType.STRING),
                            "prize_value", ValueType.INT64)),
            "r", new NestTable("ratings", List.of("trans_id", "rating_type"), List.of(),
                    List.of("trans_id", "rating_type", "model_date", "score"),
                    Map.of("trans_id", ValueType.INT64, "rating_type", ValueType.of(TapstateType.STRING),
                            "model_date", ValueType.of(TapstateType.DATE), "score", ValueType.INT64)));

    static Embed prize() {
        return new Embed("pd", Map.of("id", "prize_definition_id"), EmbedAs.OBJECT, "prize_definition", null,
                null, null, null);
    }

    static Map<String, RollupField> itemRollups() {
        Map<String, RollupField> fields = new LinkedHashMap<>();
        fields.put("qty", RollupField.of(RollupFunction.SUM, "$amount", null, null, 0));
        fields.put("amount", RollupField.of(RollupFunction.SUM,
                "=after.amount * after.prize_definition.prize_value", null, null, 0));
        fields.put("reward_qty", RollupField.of(RollupFunction.SUM, "$amount",
                "=after.prize_definition.prize_code == 'reward-points'", null, null));
        return fields;
    }

    static Embed items(Map<String, RollupField> rollups) {
        return new Embed("i", Map.of("transaction_id", "id"), EmbedAs.ARRAY, "item_movement", null,
                List.of("id"), null, null, List.of(prize()), rollups);
    }

    static Embed ratings() {
        return new Embed("r", Map.of("trans_id", "id"), EmbedAs.ROLLUP, null, null,
                List.of("trans_id", "rating_type"), null, null, null,
                Map.of("latest_score", RollupField.of(RollupFunction.LAST,
                        new RollupPick("$score", List.of("$model_date", "$rating_type")), null, null, null)));
    }

    static NestTopology compile(Embed... embeds) {
        return compile(TABLES::get, embeds);
    }

    static NestTopology compile(Function<String, NestTable> tables, Embed... embeds) {
        return NestTopology.compile("p", "doc",
                new TransformBody.Nest(null, null, new NestRoot("t", List.of("id"), null, null, List.of(embeds))),
                tables);
    }

    @Test
    void aTreeWithRollupsCompilesAndEachSlotCarriesItsTypedFields() {
        NestTopology topology = compile(items(itemRollups()), ratings());
        EmbedSlot items = topology.slots().get(0);
        EmbedSlot ratings = topology.slots().get(1);
        assertThat(items.rollups()).extracting(spec -> spec.field()).containsExactly("qty", "amount", "reward_qty");
        assertThat(items.rollups().get(1).references()).containsExactly("prize_definition");
        assertThat(items.rollups().get(0).references()).isEmpty();
        assertThat(ratings.as()).isEqualTo(EmbedAs.ROLLUP);
        assertThat(ratings.path()).isNull();
        assertThat(ratings.stateField()).isEqualTo("$rollup[r]");
        assertThat(ratings.rollups().getFirst().resultType()).isEqualTo(ValueType.INT64);
    }

    @Test
    void aRollupOnAnEmbedTheLevelPointsAtIsRefused() {
        Embed prizeWithRollup = new Embed("pd", Map.of("id", "prize_definition_id"), EmbedAs.ARRAY, "prizes",
                null, null, null, null, null, Map.of("n", RollupField.of(RollupFunction.COUNT, true, null, null, null)));
        Embed items = new Embed("i", Map.of("transaction_id", "id"), EmbedAs.ARRAY, "item_movement", null,
                List.of("id"), null, null, List.of(prizeWithRollup), null);
        assertThatThrownBy(() -> compile(items)).isInstanceOfSatisfying(TapstateException.class, e -> {
            assertThat(e.code()).isEqualTo(RollupError.ON_REFERENCE_EMBED);
            assertThat(e.args()).containsEntry("embedPath", "item_movement.prizes");
        });
    }

    @Test
    void aRollupFieldMayNotTakeANameAColumnAPathOrAnotherRollupAlreadyHas() {
        assertConflict(items(Map.of("player_id", RollupField.of(RollupFunction.COUNT, true, null, null, null))),
                "player_id", "$root row");
        assertConflict(items(Map.of("item_movement", RollupField.of(RollupFunction.COUNT, true, null, null, null))),
                "item_movement", "item_movement");
        Embed second = new Embed("r", Map.of("trans_id", "id"), EmbedAs.ROLLUP, null, null,
                List.of("trans_id", "rating_type"), null, null, null,
                Map.of("qty", RollupField.of(RollupFunction.COUNT, true, null, null, null)));
        assertThatThrownBy(() -> compile(items(itemRollups()), second))
                .isInstanceOfSatisfying(TapstateException.class, e -> {
                    assertThat(e.code()).isEqualTo(RollupError.FIELD_CONFLICT);
                    assertThat(e.args()).containsEntry("field", "qty").containsEntry("level", "$root")
                            .containsEntry("other", "item_movement rollup field");
                });
    }

    @Test
    void anArgumentIsCheckedAgainstTheReferencedRowsModelSoATypoIsRefused() {
        Map<String, RollupField> typo = Map.of("amount", RollupField.of(RollupFunction.SUM,
                "=after.amount * after.prize_definition.prize_valeu", null, null, 0));
        assertThatThrownBy(() -> compile(items(typo))).isInstanceOfSatisfying(TapstateException.class, e -> {
            assertThat(e.code()).isEqualTo(RollupError.EXPRESSION_INVALID);
            assertThat(e.args()).containsEntry("field", "amount").containsEntry("part", "argument");
            assertThat((String) e.args().get("reason")).contains("prize_valeu");
        });
    }

    @Test
    void aSumOverADecimalTimesADecimalIsRefusedRatherThanComputedThroughDouble() {
        Map<String, RollupField> decimalProduct = Map.of("x", RollupField.of(RollupFunction.SUM,
                "=after.price * after.price", null, null, null));
        assertThatThrownBy(() -> compile(items(decimalProduct))).isInstanceOfSatisfying(TapstateException.class,
                e -> assertThat(e.code()).isEqualTo(RollupError.EXPRESSION_INVALID));
    }

    @Test
    void aSumWithNoDiscoveredModelHasNoTypeToComputeInAndIsRefused() {
        Function<String, NestTable> undiscovered = alias -> new NestTable(TABLES.get(alias).name(),
                TABLES.get(alias).primaryKey());
        assertThatThrownBy(() -> compile(undiscovered, items(Map.of("qty",
                RollupField.of(RollupFunction.SUM, "$amount", null, null, null)))))
                .isInstanceOfSatisfying(TapstateException.class, e -> {
                    assertThat(e.code()).isEqualTo(RollupError.TYPE_UNKNOWN);
                    assertThat(e.args()).containsEntry("field", "qty").containsEntry("expression", "$amount");
                });
        // a count needs no type, so the same tree with only a count compiles
        assertThat(compile(undiscovered, items(Map.of("n",
                RollupField.of(RollupFunction.COUNT, true, null, null, null)))).slots()).hasSize(1);
    }

    @Test
    void aRollupMayReadItsChildLevelsRollupsBecauseThoseAreComputedFirst() {
        Embed lines = new Embed("pd", Map.of("item_id", "id"), EmbedAs.ARRAY, "lines", null, List.of("id"),
                null, null, null, Map.of("line_total", RollupField.of(RollupFunction.SUM, "$prize_value", null,
                null, 0)));
        Function<String, NestTable> tables = alias -> alias.equals("pd")
                ? new NestTable("lines", List.of("id"), List.of(), List.of("id", "item_id", "prize_value"),
                        Map.of("id", ValueType.INT64, "item_id", ValueType.INT64, "prize_value", ValueType.INT64))
                : TABLES.get(alias);
        Embed items = new Embed("i", Map.of("transaction_id", "id"), EmbedAs.ARRAY, "item_movement", null,
                List.of("id"), null, null, List.of(lines),
                Map.of("total", RollupField.of(RollupFunction.SUM, "$line_total", null, null, 0)));
        NestTopology topology = compile(tables, items);
        assertThat(topology.slots().getFirst().rollups().getFirst().argumentType()).isEqualTo(ValueType.INT64);
    }

    private static void assertConflict(Embed embed, String field, String other) {
        assertThatThrownBy(() -> compile(embed)).isInstanceOfSatisfying(TapstateException.class, e -> {
            assertThat(e.code()).isEqualTo(RollupError.FIELD_CONFLICT);
            assertThat(e.args()).containsEntry("field", field).containsEntry("other", other);
        });
    }
}
