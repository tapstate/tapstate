package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.aggregate.AggregateError;
import io.tapstate.core.dsl.aggregate.AggregateStepSpec;
import io.tapstate.core.dsl.aggregate.Contribution;
import io.tapstate.core.dsl.aggregate.ValueType;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** One input row evaluated against a step as written: membership, group and what each field is given. */
class AggregateRowTest {

    private static final Map<String, ValueType> ORDERS = columns();

    private static Map<String, ValueType> columns() {
        Map<String, ValueType> columns = new LinkedHashMap<>();
        columns.put("id", ValueType.INT64);
        columns.put("region", ValueType.of(TapstateType.STRING));
        columns.put("status", ValueType.of(TapstateType.STRING));
        columns.put("amount", ValueType.decimal(2));
        columns.put("qty", ValueType.INT64);
        return columns;
    }

    private static AggregateStepSpec spec(String payload) {
        PipelineResource pipeline = (PipelineResource) new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: sales
                source: src_orders
                transforms:
                  - id: by_region
                    from: [ orders ]
                    type: aggregate
                """ + payload + """
                serve:
                  from: by_region
                  sync: [ { id: dw, source: tgt, write_mode: upsert } ]
                """);
        TransformBody.Aggregate step = (TransformBody.Aggregate) ((Step.Inline) pipeline.transforms().getFirst()).body();
        return AggregateStepRules.compile("by_region", step, ORDERS, true, List.of("id"), "orders");
    }

    private static final AggregateStepSpec SALES = spec("""
                where: "=after.status != 'cancelled'"
                groupBy:
                  region: $region
                  big: "=after.qty > 10"
                fields:
                  orders: { count: true }
                  revenue: { sum: $amount }
                  big_orders: { count: true, filter: "=after.qty >= 10" }
                  most: { max: $amount }
                  latest: { last: { value: $status, by: $qty } }
            """);

    private static Map<String, Object> row(Object id, String region, String status, String amount, Long qty) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("region", region);
        row.put("status", status);
        row.put("amount", amount == null ? null : new BigDecimal(amount));
        row.put("qty", qty);
        return row;
    }

    @Test
    void aMemberRowGivesItsGroupAndOneInputPerField() {
        AggregateRow.Evaluated evaluated = AggregateRow.of(SALES, row(7L, "EU", "paid", "1200.50", 12L)).orElseThrow();

        assertThat(AggregateRow.key(SALES, row(7L, "EU", "paid", "1200.50", 12L))).containsExactly(7L);
        assertThat(evaluated.group()).containsExactly("EU", true);
        List<Contribution> inputs = evaluated.inputs();
        assertThat(inputs).hasSize(5);
        assertThat(inputs.get(0)).isNotNull();
        assertThat(inputs.get(1).value()).isEqualTo(new BigDecimal("1200.50"));
        assertThat(inputs.get(2)).as("the field filter holds").isNotNull();
        assertThat(inputs.get(3).value()).isEqualTo(new BigDecimal("1200.50"));
        assertThat(inputs.get(4).value()).isEqualTo("paid");
        assertThat(inputs.get(4).by()).containsExactly(12L);
    }

    @Test
    void aRowThatFailsTheConditionIsNoMember() {
        assertThat(AggregateRow.of(SALES, row(7L, "EU", "cancelled", "5.00", 1L))).isEmpty();
    }

    @Test
    void aFieldFilterThatFailsGivesOnlyThatFieldNothing() {
        AggregateRow.Evaluated evaluated = AggregateRow.of(SALES, row(7L, "EU", "paid", "5.00", 1L)).orElseThrow();

        assertThat(evaluated.group()).containsExactly("EU", false);
        assertThat(evaluated.inputs().get(0)).isNotNull();
        assertThat(evaluated.inputs().get(2)).isNull();
    }

    @Test
    void aNullArgumentGivesAValueFunctionNothingAndANullGroupValueIsAGroup() {
        AggregateRow.Evaluated evaluated = AggregateRow.of(SALES, row(7L, null, "paid", null, 3L)).orElseThrow();

        assertThat(evaluated.group().get(0)).isNull();
        assertThat(evaluated.inputs().get(0)).as("the row is still counted").isNotNull();
        assertThat(evaluated.inputs().get(1)).isNull();
        assertThat(evaluated.inputs().get(3)).isNull();
    }

    @Test
    void aNullOrderingKeepsTheRowOutOfLastAndANullValueDoesNot() {
        assertThat(AggregateRow.of(SALES, row(7L, "EU", "paid", "5.00", null)).orElseThrow().inputs().get(4)).isNull();

        AggregateStepSpec noCondition = spec("""
                    fields:
                      latest: { last: { value: $status, by: $qty } }
                """);
        Contribution taken = AggregateRow.of(noCondition, row(7L, "EU", null, "5.00", 2L)).orElseThrow().inputs().get(0);
        assertThat(taken).isNotNull();
        assertThat(taken.value()).isNull();
    }

    @Test
    void anExpressionThatFailsOnARowIsACodedError() {
        AggregateStepSpec dividing = spec("""
                    groupBy: { share: "=after.qty / (after.qty - after.qty)" }
                    fields: { orders: { count: true } }
                """);

        assertThatThrownBy(() -> AggregateRow.of(dividing, row(7L, "EU", "paid", "5.00", 2L)))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(AggregateError.EVALUATION_FAILED);
                    assertThat(error.args()).containsEntry("step", "by_region").containsKey("reason");
                });
    }
}
