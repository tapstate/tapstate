package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.model.Resource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Judging a batch's row expressions against the columns their sources were discovered to hold. This
 * runs where a source model exists — never in the offline check, which has none — and refuses an
 * expression that cannot be evaluated on what the columns actually are, rather than letting it reach
 * the runtime and fail there.
 *
 * <p>These cases are all about the type verdict, so every source they read has been discovered. What
 * happens when one has not is the discovery obligation's own concern.
 */
class RowExpressionTypeRulesTest {

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_orders
            connector: mysql
            config: { host: 10.10.0.5, username: u, password: p }
            mode: cdc
            tables: [ orders ]
            """;

    private static List<Resource> batch(String... documents) {
        DslParser parser = new DslParser();
        List<Resource> resources = new ArrayList<>();
        resources.add(parser.parse(SOURCE));
        for (String document : documents) {
            resources.add(parser.parse(document));
        }
        return resources;
    }

    /** A pipeline whose one filter step carries {@code expr}. */
    private static String filterPipeline(String expr) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: orders_out
                source: src_orders
                transforms:
                  - { id: keep, from: [orders], type: filter, expr: "%s" }
                serve:
                  from: keep
                  sync: [ { id: out, source: src_orders, write_mode: upsert } ]
                """.formatted(expr);
    }

    /** The source's one discovered table, {@code orders}, holding the given columns. */
    private static Map<String, List<DiscoveredTable>> model(Object... columnsAndTypes) {
        return Map.of("src_orders", List.of(new DiscoveredTable("orders", columns(columnsAndTypes))));
    }

    private static Map<String, TapstateType> columns(Object... columnsAndTypes) {
        Map<String, TapstateType> columns = new LinkedHashMap<>();
        for (int i = 0; i < columnsAndTypes.length; i += 2) {
            columns.put((String) columnsAndTypes[i], (TapstateType) columnsAndTypes[i + 1]);
        }
        return columns;
    }

    // ---- a column whose type cannot survive the expression ------------------------------

    @Test
    @DisplayName("computing on an exact fixed-point column is refused, naming the column and the type")
    void decimalComputationIsRefused() {
        DslException thrown = catchThrowableOfType(DslException.class, () -> RowExpressionTypeRules.validate(
                batch(filterPipeline("after.amount * 2 > 0")), model("amount", TapstateType.DECIMAL)));

        assertThat(thrown.code())
                .isEqualTo(DslError.ROW_EXPRESSION_TYPE_UNSUPPORTED);
        assertThat(thrown.args())
                .containsEntry("column", "amount")
                .containsEntry("type", "DECIMAL");
        assertThat(thrown.path()).isEqualTo("transforms[0].expr");
    }

    @Test
    @DisplayName("a lossless numeric column passes, so the gate is not refusing numbers as a class")
    void losslessNumericPasses() {
        assertThatCode(() -> RowExpressionTypeRules.validate(
                batch(filterPipeline("after.amount * 2 > 0")), model("amount", TapstateType.INT64)))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("carrying a fixed-point column through unchanged is not a loss and passes")
    void decimalCarriedThroughPasses() {
        String pipeline = """
                version: tapstate/v1
                kind: pipeline
                id: orders_out
                source: src_orders
                transforms:
                  - id: project
                    from: [orders]
                    type: map
                    fields: { total: "=after.amount" }
                serve:
                  from: project
                  sync: [ { id: out, source: src_orders, write_mode: upsert } ]
                """;

        assertThatCode(() -> RowExpressionTypeRules.validate(
                batch(pipeline), model("amount", TapstateType.DECIMAL))).doesNotThrowAnyException();
    }

    // ---- a column whose type nothing resolved -------------------------------------------

    @Test
    @DisplayName("any reference to a column with no resolved type is refused, not only computing on it")
    void unresolvedColumnTypeIsRefusedOnAnyReference() {
        DslException thrown = catchThrowableOfType(DslException.class, () -> RowExpressionTypeRules.validate(
                batch(filterPipeline("has(after.amount)")), model("amount", TapstateType.UNKNOWN)));

        assertThat(thrown.code())
                .isEqualTo(DslError.ROW_EXPRESSION_TYPE_UNKNOWN);
        assertThat(thrown.args()).containsEntry("column", "amount").containsEntry("table", "orders");
    }

    @Test
    @DisplayName("a column two upstreams type differently is refused for the upstream it is wrong on")
    void anUpstreamTheExpressionIsWrongAboutIsRefused() {
        // The expression genuinely runs on both tables, so it has to hold on both. Judging them apart
        // rather than pooling their columns is what makes the refusal say which one it failed on and
        // why: pooled, the only thing that could be said is that the column has no resolved type,
        // which is not true of either table and tells the author nothing to act on.
        String secondSource = """
                version: tapstate/v1
                kind: source
                id: src_archive
                connector: mysql
                config: { host: 10.10.0.6, username: u, password: p }
                mode: cdc
                tables: [ archive ]
                """;
        String pipeline = """
                version: tapstate/v1
                kind: pipeline
                id: orders_out
                source: [ src_orders, src_archive ]
                transforms:
                  - { id: keep, from: [orders, archive], type: filter, expr: "after.amount > 0" }
                serve:
                  from: keep
                  sync: [ { id: out, source: src_orders, write_mode: upsert } ]
                """;
        Map<String, List<DiscoveredTable>> tables = new LinkedHashMap<>();
        tables.put("src_orders", List.of(new DiscoveredTable("orders", columns("amount", TapstateType.INT64))));
        tables.put("src_archive", List.of(new DiscoveredTable("archive", columns("amount", TapstateType.STRING))));

        DslException thrown = catchThrowableOfType(DslException.class,
                () -> RowExpressionTypeRules.validate(batch(secondSource, pipeline), tables));

        // The archive table types the column as text, and comparing text to a number is an ordinary
        // wrong expression - the offline layer would say the same thing if it had the types.
        assertThat(thrown.code()).isEqualTo(DslError.ILLEGAL_EXPRESSION);
    }

    @Test
    @DisplayName("two tables of one source naming a column differently do not pool into an unresolved one")
    void columnsAreNotPooledAcrossTheTablesOfOneSource() {
        // One database naming a column id in two unrelated tables, typed differently, is the ordinary
        // shape of a database. Pooled, the column would be unresolved and every expression reading it
        // refused - on most real databases, for most expressions.
        String source = """
                version: tapstate/v1
                kind: source
                id: src_shop
                connector: mysql
                config: { host: 10.10.0.7, username: u, password: p }
                mode: cdc
                tables: [ orders, customers ]
                """;
        Map<String, List<DiscoveredTable>> tables = Map.of("src_shop", List.of(
                new DiscoveredTable("orders", columns("id", TapstateType.INT64)),
                new DiscoveredTable("customers", columns("id", TapstateType.STRING))));

        assertThatCode(() -> RowExpressionTypeRules.validate(
                batch(source, shopPipeline("customers", "after.id != ''")), tables))
                .as("the step reads customers, where id is text and the comparison holds")
                .doesNotThrowAnyException();

        // The same expression against the other table is still refused, so the narrowing is not the
        // gate quietly standing down.
        DslException thrown = catchThrowableOfType(DslException.class, () -> RowExpressionTypeRules.validate(
                batch(source, shopPipeline("orders", "after.id != ''")), tables));
        assertThat(thrown.code()).isEqualTo(DslError.ILLEGAL_EXPRESSION);
    }

    /** A pipeline over {@code src_shop} whose filter reads one named table. */
    private static String shopPipeline(String table, String expr) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: shop_out
                source: src_shop
                transforms:
                  - { id: keep, from: [%s], type: filter, expr: "%s" }
                serve:
                  from: keep
                  sync: [ { id: out, source: src_shop, write_mode: upsert } ]
                """.formatted(table, expr);
    }

    // ---- what the gate must leave alone --------------------------------------------------

    @Test
    @DisplayName("a column the model does not carry stays dyn, since a sampled model may simply miss it")
    void columnAbsentFromTheModelPasses() {
        assertThatCode(() -> RowExpressionTypeRules.validate(
                batch(filterPipeline("after.absent * 2 > 0")), model("amount", TapstateType.DECIMAL)))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("an expression reading no row field is judged without any source model at all")
    void expressionWithoutRowAccessNeedsNoModel() {
        assertThatCode(() -> RowExpressionTypeRules.validate(
                batch(filterPipeline("op == 'i'")), Map.of())).doesNotThrowAnyException();
    }

    // ---- every place an expression can hide ----------------------------------------------

    @Test
    @DisplayName("a computed field of a map projection is judged")
    void mapProjectionFieldIsJudged() {
        String pipeline = """
                version: tapstate/v1
                kind: pipeline
                id: orders_out
                source: src_orders
                transforms:
                  - id: project
                    from: [orders]
                    type: map
                    fields: { doubled: "=after.amount * 2" }
                serve:
                  from: project
                  sync: [ { id: out, source: src_orders, write_mode: upsert } ]
                """;

        DslException thrown = catchThrowableOfType(DslException.class, () -> RowExpressionTypeRules.validate(
                batch(pipeline), model("amount", TapstateType.DECIMAL)));

        assertThat(thrown.code())
                .isEqualTo(DslError.ROW_EXPRESSION_TYPE_UNSUPPORTED);
        assertThat(thrown.path()).isEqualTo("transforms[0].fields.doubled");
    }

    @Test
    @DisplayName("a push element's whole-payload projection is judged")
    void pushFormatIsJudged() {
        String pipeline = """
                version: tapstate/v1
                kind: pipeline
                id: orders_out
                source: src_orders
                transforms:
                  - { id: keep, from: [orders], type: filter, expr: "op == 'i'" }
                serve:
                  from: keep
                  push: [ { id: topic_out, source: src_orders, topic: t, format: "=after.amount * 2" } ]
                """;

        DslException thrown = catchThrowableOfType(DslException.class, () -> RowExpressionTypeRules.validate(
                batch(pipeline), model("amount", TapstateType.DECIMAL)));

        assertThat(thrown.code())
                .isEqualTo(DslError.ROW_EXPRESSION_TYPE_UNSUPPORTED);
        assertThat(thrown.path()).isEqualTo("serve.push[0].format");
    }

    @Test
    void aDecimalChoiceIsRefusedInBothPushFormats() {
        String expression = "has(after.a) ? after.a : after.b";
        Map<String, String> formats = Map.of(
                "\"=" + expression + "\"", "serve.push[0].format",
                "{ chosen: \"=" + expression + "\" }", "serve.push[0].format.chosen");
        for (var format : formats.entrySet()) {
            String pipeline = """
                    version: tapstate/v1
                    kind: pipeline
                    id: orders_out
                    source: src_orders
                    transforms:
                      - { id: keep, from: [orders], type: filter, expr: "op == 'i'" }
                    serve:
                      from: keep
                      push: [ { id: topic_out, source: src_orders, topic: t, format: %s } ]
                    """.formatted(format.getKey());

            DslException thrown = catchThrowableOfType(DslException.class,
                    () -> RowExpressionTypeRules.validate(batch(pipeline),
                            model("a", TapstateType.DECIMAL, "b", TapstateType.DECIMAL)));

            assertThat(thrown).isNotNull();
            assertThat(thrown.code()).isEqualTo(DslError.ROW_EXPRESSION_TYPE_UNSUPPORTED);
            assertThat(thrown.args()).containsEntry("expr", expression)
                    .containsEntry("column", "a").containsEntry("type", "DECIMAL")
                    .containsEntry("table", "orders");
            assertThat(thrown.path()).isEqualTo(format.getValue());
        }
    }

    @Test
    @DisplayName("an expression inside a reused transform definition is judged where the pipeline uses it")
    void reusedTransformDefinitionIsJudged() {
        String definition = """
                version: tapstate/v1
                kind: transform
                id: keep_big
                type: filter
                expr: "after.amount > 0"
                """;
        String pipeline = """
                version: tapstate/v1
                kind: pipeline
                id: orders_out
                source: src_orders
                transforms:
                  - { id: keep, from: [orders], use: keep_big }
                serve:
                  from: keep
                  sync: [ { id: out, source: src_orders, write_mode: upsert } ]
                """;

        DslException thrown = catchThrowableOfType(DslException.class, () -> RowExpressionTypeRules.validate(
                batch(definition, pipeline), model("amount", TapstateType.DECIMAL)));

        assertThat(thrown.code())
                .isEqualTo(DslError.ROW_EXPRESSION_TYPE_UNSUPPORTED);
    }

    @Test
    @DisplayName("a transform definition no pipeline uses has no source to judge it against, and is left alone")
    void unusedTransformDefinitionIsLeftAlone() {
        String definition = """
                version: tapstate/v1
                kind: transform
                id: keep_big
                type: filter
                expr: "after.amount * 2 > 0"
                """;

        assertThatCode(() -> RowExpressionTypeRules.validate(
                batch(definition), model("amount", TapstateType.DECIMAL))).doesNotThrowAnyException();
    }
}
