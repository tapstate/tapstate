package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.hazelcast.function.SupplierEx;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.Workspace;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A join whose SQL cannot be resolved when the pipeline is assembled is refused with a code, so the
 * converger records the pipeline as failed with the reason instead of retrying the start on every pass.
 *
 * <p>Assembly used to let the SQL front end's own exception escape. It carries no code, so the
 * converger treated it as a defect of the process rather than a condition of the pipeline: the pass
 * threw, the pipeline stayed {@code new}, and the only trace was a climbing failure counter. Every case
 * here asserts the code, because "something was thrown" is exactly what the old behaviour satisfied.
 */
class AJoinThatCannotResolveItsInputsIsRefusedWithACodeTest {

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: orders_src
            connector: mysql
            config: { host: h }
            mode: cdc
            tables: [ orders ]
            """;

    private static final String ITEMS_SOURCE = """
            version: tapstate/v1
            kind: source
            id: items_src
            connector: mysql
            config: { host: h }
            mode: cdc
            tables: [ order_items ]
            """;

    private static final String TARGET = """
            version: tapstate/v1
            kind: source
            id: orders_dest
            connector: mongodb
            config: { uri: u }
            """;

    private static String joinPipeline(String select) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: wide
                source: [ orders_src, items_src ]
                transforms:
                  - id: widen
                    type: join
                    from: { o: orders, i: order_items }
                    engine: builtin
                    sql: |
                      %s
                      FROM o JOIN i ON i.id = o.id
                serve:
                  from: widen
                  sync: [ { id: sync_1, source: orders_dest } ]
                """.formatted(select);
    }

    /**
     * The driving side is a {@code js} step that passes rows through unchanged. Validation now refuses
     * this shape, so it reaches assembly only as a pipeline stored before it did; it is stored here
     * without being validated for that reason.
     */
    private static final String JOIN_OVER_A_STEP = """
            version: tapstate/v1
            kind: pipeline
            id: wide
            source: [ orders_src, items_src ]
            transforms:
              - id: renamed
                from: [ orders ]
                type: js
                script: |
                  function process(record, ctx) { return record; }
              - id: widen
                type: join
                from: { o: renamed, i: order_items }
                engine: builtin
                sql: |
                  SELECT o.id AS order_id, i.id AS item_id
                  FROM o JOIN i ON i.id = o.id
            serve:
              from: widen
              sync: [ { id: sync_1, source: orders_dest } ]
            """;

    @Test
    void aJoinWhoseColumnsResolveBuilds() {
        InMemoryStorePort store = discovered(stored(true,
                joinPipeline("SELECT o.id AS order_id, i.id AS item_id")));

        assertThatCode(() -> new StoreBackedDagSource(store, discardingBinder()).dagFor("wide"))
                .doesNotThrowAnyException();
    }

    @Test
    void aJoinReadingAStepIsRefusedWithACodeNamingTheStep() {
        InMemoryStorePort store = discovered(stored(false, JOIN_OVER_A_STEP));

        Throwable thrown = catchThrowable(() -> new StoreBackedDagSource(store, discardingBinder()).dagFor("wide"));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException refused = (TapstateException) thrown;
        assertThat(refused.code().code()).isEqualTo("actuation.join-input-not-a-table");
        assertThat(refused.args())
                .containsEntry("step", "widen")
                .containsEntry("alias", "o")
                .containsEntry("ref", "renamed");
    }

    /**
     * Validation accepts this one - it cannot know the columns, which only discovery supplies - so it is
     * the shape any author can reach, and the one a source dropping a column produces as well.
     */
    @Test
    void sqlNamingAColumnNoInputHasIsRefusedWithACodeCarryingTheDiagnosis() {
        InMemoryStorePort store = discovered(stored(true,
                joinPipeline("SELECT o.id AS order_id, o.no_such_column AS gone, i.id AS item_id")));

        Throwable thrown = catchThrowable(() -> new StoreBackedDagSource(store, discardingBinder()).dagFor("wide"));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException refused = (TapstateException) thrown;
        assertThat(refused.code().code()).isEqualTo("actuation.join-sql-invalid");
        assertThat(refused.args()).containsEntry("step", "widen");
        assertThat(String.valueOf(refused.args().get("detail")))
                .containsIgnoringCase("no_such_column")
                // The diagnosis is the validator's own words, not the exception classes that carried it.
                .doesNotContain("Exception");
    }

    // ---- fixtures ----------------------------------------------------------------------

    /** Parses and stores the pipeline with its sources, through the product's own gate when asked to. */
    private static InMemoryStorePort stored(boolean validate, String pipeline) {
        DslParser parser = new DslParser();
        List<Resource> resources = new ArrayList<>();
        for (String document : List.of(SOURCE, ITEMS_SOURCE, TARGET, pipeline)) {
            resources.add(parser.parse(document));
        }
        if (validate) {
            Workspace.of(resources);
        }
        InMemoryStorePort store = new InMemoryStorePort();
        resources.forEach(store.artifacts()::save);
        OpenRingGenerations.forSources(store, "orders_src");
        return store;
    }

    private static InMemoryStorePort discovered(InMemoryStorePort store) {
        discovered(store, "orders_src", "orders");
        discovered(store, "items_src", "order_items");
        return store;
    }

    private static void discovered(InMemoryStorePort store, String connectionId, String table) {
        store.schemas().save(new DiscoveredSourceModel(connectionId, "mysql", 0L,
                new SourceModel(List.of(new SourceTable(table,
                        List.of(new SourceField("id", "text")), List.of("id"), null)))));
    }

    private static StoreBackedDagSource.SinkWriterBinder discardingBinder() {
        return (connectorId, settings, writeMode, ddl, target, node) -> (SupplierEx<SinkWriter>) () -> null;
    }
}
