package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.RollupField;
import io.tapstate.core.model.RollupFunction;
import io.tapstate.core.model.RollupPick;
import io.tapstate.core.model.RollupResultType;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The editor face of an aggregate step: the structured payload reads its key, filter, groups and rollup
 * fields, writes them back, and refuses a field that does not name exactly one usable rollup function.
 */
class PipelineRepresentationAggregateTest {

    private final PipelineRepresentation representation = new PipelineRepresentation();

    private PipelineResource model(Map<String, Object> step) {
        return representation.toModel(new PipelineInput(
                "p", null, List.of("src_a"), List.of(step), null,
                Map.of("from", "by_region", "sync", List.of(Map.of("source", "src_a"))),
                null, null), null);
    }

    private static Map<String, Object> aggregateStep(Map<String, Object> payload) {
        Map<String, Object> step = new LinkedHashMap<>();
        step.put("id", "by_region");
        step.put("from", "orders");
        step.putAll(payload);
        return step;
    }

    private static Map<String, Object> fields() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("orders", Map.of("count", true));
        Map<String, Object> total = new LinkedHashMap<>();
        total.put("sum", "after.amount");
        total.put("filter", "after.status == 'paid'");
        total.put("type", "decimal");
        total.put("default", 0);
        fields.put("paid_total", total);
        fields.put("first_sku", Map.of("first", Map.of("value", "after.sku", "by", "after.created_at")));
        fields.put("last_sku", Map.of("last", Map.of("value", "after.sku", "by", List.of("after.day", "after.seq"))));
        return fields;
    }

    private static TransformBody.Aggregate aggregateIn(PipelineResource pipeline) {
        return (TransformBody.Aggregate) ((Step.Inline) pipeline.transforms().get(0)).body();
    }

    @Test
    @DisplayName("a body with groupBy is read as an aggregate with its key, filter, groups and rollup fields")
    void groupByMakesAnAggregate() {
        TransformBody.Aggregate body = aggregateIn(model(aggregateStep(Map.of("body", Map.of(
                "key", List.of("id"), "where", "after.amount > 0",
                "groupBy", Map.of("region", "$region"), "fields", fields())))));

        assertThat(body.key()).containsExactly("id");
        assertThat(body.where()).isEqualTo("after.amount > 0");
        assertThat(body.groupBy()).containsEntry("region", "$region");
        assertThat(body.fields()).containsOnlyKeys("orders", "paid_total", "first_sku", "last_sku");
        RollupField total = body.fields().get("paid_total");
        assertThat(total.function()).isEqualTo(RollupFunction.SUM);
        assertThat(total.filter()).isEqualTo("after.status == 'paid'");
        assertThat(total.type()).isEqualTo(RollupResultType.DECIMAL);
        assertThat(total.defaultValue()).isEqualTo(0);
        assertThat(body.fields().get("first_sku").argument())
                .isEqualTo(new RollupPick("after.sku", List.of("after.created_at")));
        assertThat(body.fields().get("last_sku").argument())
                .isEqualTo(new RollupPick("after.sku", List.of("after.day", "after.seq")));
    }

    @Test
    @DisplayName("the view writes the aggregate back, and reads back to the same model")
    void viewRoundTrips() {
        PipelineResource pipeline = model(aggregateStep(Map.of(
                "type", "aggregate", "where", "after.amount > 0",
                "groupBy", Map.of("region", "$region"), "fields", fields())));

        PipelineView view = representation.toView(
                pipeline, "e".repeat(64), List.of(new PipelineSourceSummary("src_a", null, "mysql")));
        Map<String, Object> step = view.transforms().get(0);

        assertThat(step).containsEntry("type", "aggregate");
        Map<String, Object> echoed = new LinkedHashMap<>(step);
        echoed.keySet().retainAll(List.of("id", "type", "from", "body", "key", "where", "groupBy", "fields"));
        assertThat(aggregateIn(model(echoed))).isEqualTo(aggregateIn(pipeline));
    }

    @Test
    @DisplayName("a field naming no function, two functions or an unknown one is refused")
    void aFieldNamesExactlyOneKnownFunction() {
        assertRefused(Map.of("n", Map.of("filter", "true")), "must name exactly one rollup function");
        assertRefused(Map.of("n", Map.of("sum", "a", "max", "a")), "must name exactly one rollup function");
        assertRefused(Map.of("n", Map.of("median", "a")), "is not a rollup function");
    }

    @Test
    @DisplayName("first and last need an ordering, and the other functions an expression")
    void argumentsAreChecked() {
        assertRefused(Map.of("n", Map.of("first", Map.of("value", "after.sku"))), "first.by is required");
        assertRefused(Map.of("n", Map.of("sum", 3)), "sum must be an expression");
        assertRefused(Map.of("n", Map.of("count", false)), "count must be an expression or true");
    }

    private void assertRefused(Map<String, Object> fields, String message) {
        Map<String, Object> step = aggregateStep(Map.of("type", "aggregate", "groupBy", Map.of(), "fields", fields));

        assertThatThrownBy(() -> model(step))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining(message);
    }
}
