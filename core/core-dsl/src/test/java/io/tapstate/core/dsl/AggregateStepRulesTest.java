package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.aggregate.AggregateError;
import io.tapstate.core.dsl.aggregate.AggregateStepSpec;
import io.tapstate.core.dsl.aggregate.ValueType;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AggregateStepRulesTest {

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_orders
            connector: mysql
            mode: cdc
            tables: [ orders, refunds ]
            """;
    private static final String TARGET = """
            version: tapstate/v1
            kind: source
            id: tgt
            connector: postgres
            """;

    private static final Map<String, ValueType> ORDERS = columns();

    private static Map<String, ValueType> columns() {
        Map<String, ValueType> columns = new LinkedHashMap<>();
        columns.put("id", ValueType.INT64);
        columns.put("region", ValueType.of(TapstateType.STRING));
        columns.put("status", ValueType.of(TapstateType.STRING));
        columns.put("amount", ValueType.decimal(2));
        columns.put("qty", ValueType.INT64);
        columns.put("updated_at", ValueType.of(TapstateType.DATETIME));
        return columns;
    }

    private static String pipeline(String transforms) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: sales
                source: src_orders
                transforms:
                %s
                serve:
                  from: by_region
                  sync: [ { id: dw, source: tgt, write_mode: upsert } ]
                """.formatted(transforms.stripTrailing());
    }

    private static List<Resource> batch(String transforms) {
        DslParser parser = new DslParser();
        return List.of(parser.parse(SOURCE), parser.parse(TARGET), parser.parse(pipeline(transforms)));
    }

    private static TransformBody.Aggregate step(String payload) {
        PipelineResource pipeline = (PipelineResource) new DslParser().parse(pipeline("""
                  - id: by_region
                    from: [ orders ]
                    type: aggregate
                """ + payload));
        return (TransformBody.Aggregate) ((Step.Inline) pipeline.transforms().getFirst()).body();
    }

    private static void refused(Runnable action, AggregateError code, Map<String, Object> args) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class, error -> {
            assertThat(error.code()).isEqualTo(code);
            assertThat(error.args()).containsOnlyKeys(code.placeholders()).containsAllEntriesOf(args);
        });
    }

    @Test
    @DisplayName("a filter above the step is refused, naming the filter and pointing at where")
    void aFilterAboveTheStepIsRefused() {
        refused(() -> Workspace.of(batch("""
                  - { id: open, from: [ orders ], type: filter, expr: "after.status == 'open'" }
                  - { id: slim, from: [ open ], type: map, fields: { note: false } }
                  - id: by_region
                    from: [ slim ]
                    type: aggregate
                    key: [ id ]
                    fields: { orders: { count: true } }
                """)), AggregateError.UPSTREAM_FILTER_UNSAFE,
                Map.of("step", "by_region", "filter", "open", "source", "src_orders"));
    }

    @Test
    @DisplayName("a source table, and a map or union over source tables, are inputs the step accepts")
    void statelessInputsAreAccepted() {
        assertThatCode(() -> Workspace.of(batch("""
                  - { id: both, from: [ orders, refunds ], type: union }
                  - { id: slim, from: [ both ], type: map, fields: { note: false } }
                  - id: by_region
                    from: [ slim ]
                    type: aggregate
                    key: [ id ]
                    fields: { orders: { count: true } }
                """))).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("an expansion anywhere between the tables and the step is refused, naming the step and its kind")
    void aStepThatChangesRowCountIsRefusedAsInput() {
        refused(() -> Workspace.of(batch("""
                  - { id: lines, from: [ orders ], type: unwind, path: items, element_key: sku }
                  - { id: kept, from: [ lines ], type: map, fields: { note: false } }
                  - id: by_region
                    from: [ kept ]
                    type: aggregate
                    key: [ id ]
                    fields: { orders: { count: true } }
                """)), AggregateError.INPUT_NOT_SUPPORTED,
                Map.of("step", "by_region", "from", "lines", "kind", "unwind"));
    }

    @Test
    @DisplayName("two inputs are refused: the rows have to be merged by a union the author wrote")
    void twoInputsAreRefused() {
        refused(() -> Workspace.of(batch("""
                  - id: by_region
                    from: [ orders, refunds ]
                    type: aggregate
                    key: [ id ]
                    fields: { orders: { count: true } }
                """)), AggregateError.INPUT_NOT_SUPPORTED, Map.of("step", "by_region"));
    }

    @Test
    @DisplayName("a name used for a group and for a field is refused")
    void aNameUsedTwiceIsRefused() {
        refused(() -> Workspace.of(batch("""
                  - id: by_region
                    from: [ orders ]
                    type: aggregate
                    groupBy: { region: $region }
                    fields: { region: { count: true } }
                """)), AggregateError.NAME_CONFLICT, Map.of("step", "by_region", "name", "region"));
    }

    @Test
    @DisplayName("the output is the group names in order as the key, then the fields with their result types")
    void theOutputShapeIsGroupsThenTypedFields() {
        AggregateStepSpec spec = AggregateStepRules.compile("by_region", step("""
                    where: "=after.status != 'cancelled'"
                    groupBy:
                      region: $region
                      big: "=after.qty > 10"
                    fields:
                      orders: { count: true }
                      revenue: { sum: $amount }
                      units: { sum: $qty }
                      latest: { last: { value: $status, by: $updated_at } }
                """), ORDERS, true, List.of("id"), "orders");

        assertThat(spec.key()).containsExactly("id");
        assertThat(spec.where()).isEqualTo("after.status != 'cancelled'");
        assertThat(spec.outputKey()).containsExactly("region", "big");
        assertThat(spec.outputColumns()).containsExactly(
                Map.entry("region", ValueType.of(TapstateType.STRING)),
                Map.entry("big", ValueType.of(TapstateType.BOOLEAN)),
                Map.entry("orders", ValueType.INT64),
                Map.entry("revenue", ValueType.decimal(2)),
                Map.entry("units", ValueType.INT64),
                Map.entry("latest", ValueType.of(TapstateType.STRING)));
    }

    @Test
    @DisplayName("with no group names the output key is empty: one row over every member")
    void noGroupNamesIsOneGroup() {
        AggregateStepSpec spec = AggregateStepRules.compile("totals",
                step("    fields: { orders: { count: true } }\n"), ORDERS, true, List.of("id"), "orders");

        assertThat(spec.outputKey()).isEmpty();
        assertThat(spec.outputColumns()).containsOnlyKeys("orders");
    }

    @Test
    @DisplayName("the step's own key wins over the input's, and an input without a key needs one")
    void theKeyIsTheStepsOrTheInputs() {
        TransformBody.Aggregate keyed = step("""
                    key: [ id, region ]
                    fields: { orders: { count: true } }
                """);
        TransformBody.Aggregate unkeyed = step("    fields: { orders: { count: true } }\n");

        assertThat(AggregateStepRules.compile("s", keyed, ORDERS, true, List.of("id"), "orders").key())
                .containsExactly("id", "region");
        refused(() -> AggregateStepRules.compile("s", unkeyed, ORDERS, true, List.of(), "orders"),
                AggregateError.KEY_REQUIRED, Map.of("step", "s", "from", "orders"));
    }

    @Test
    @DisplayName("a key column the input does not have is refused when the input's columns are known")
    void aKeyColumnTheInputLacksIsRefused() {
        TransformBody.Aggregate body = step("""
                    key: [ order_id ]
                    fields: { orders: { count: true } }
                """);

        refused(() -> AggregateStepRules.compile("s", body, ORDERS, true, List.of("id"), "orders"),
                AggregateError.EXPRESSION_INVALID, Map.of("step", "s", "field", "key", "part", "key"));
        assertThatCode(() -> AggregateStepRules.compile("s", body, Map.of(), false, List.of(), "orders"))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("a group expression that does not check against the input is refused under the group's name")
    void aGroupExpressionThatDoesNotCheckIsRefused() {
        TransformBody.Aggregate body = step("""
                    groupBy: { bucket: "=after.region + 1" }
                    fields: { orders: { count: true } }
                """);

        refused(() -> AggregateStepRules.compile("s", body, ORDERS, true, List.of("id"), "orders"),
                AggregateError.EXPRESSION_INVALID, Map.of("step", "s", "field", "bucket", "part", "groupBy"));
    }

    @Test
    @DisplayName("a field's refusal is restated under the step: a sum over a string, and a sum of unknown type")
    void aFieldRefusalNamesTheStep() {
        TransformBody.Aggregate overString = step("    fields: { total: { sum: $region } }\n");
        TransformBody.Aggregate overUnknown = step("    fields: { total: { sum: $mystery } }\n");

        refused(() -> AggregateStepRules.compile("s", overString, ORDERS, true, List.of("id"), "orders"),
                AggregateError.EXPRESSION_INVALID, Map.of("step", "s", "field", "total"));
        refused(() -> AggregateStepRules.compile("s", overUnknown, Map.of(), false, List.of("id"), "orders"),
                AggregateError.TYPE_UNKNOWN, Map.of("step", "s", "field", "total", "expression", "$mystery"));
    }

    @Test
    @DisplayName("the filter steps above the step are found through maps and unions, nearest first")
    void filtersUpstreamAreFound() {
        List<Resource> batch = batch("""
                  - { id: open, from: [ orders ], type: filter, expr: "after.status == 'open'" }
                  - { id: slim, from: [ open ], type: map, fields: { note: false } }
                  - { id: paid, from: [ slim ], type: filter, expr: "after.qty > 0" }
                  - id: by_region
                    from: [ paid ]
                    type: aggregate
                    key: [ id ]
                    fields: { orders: { count: true } }
                """);
        Map<String, Resource> byId = new LinkedHashMap<>();
        batch.forEach(resource -> byId.put(resource.id(), resource));

        assertThat(AggregateStepRules.filtersUpstream("by_region", (PipelineResource) byId.get("sales"), byId))
                .containsExactly("paid", "open");
    }
}
