package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.Workspace;
import io.tapstate.core.common.TapstateException;
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
 * The seam between the two halves of a pipeline's life: every pipeline the product's own validator
 * accepts, the builder must be able to build.
 *
 * <p>Nothing else checks this. Both halves are covered, and both hand-build their pipelines - differently.
 * The validator's own corpus addresses a source by the table it reads ({@code from: [orders]}); every DAG
 * builder test addresses it by its source id ({@code FromRef.literal("orders_src")}) and injects the
 * reference-to-vertex map besides, which defines away the very mapping production has to get right. Two
 * green suites describing two different products, and no test where they meet.
 *
 * <p>So this one parses with the product's parser, validates with the product's rules, and hands what
 * comes out to the builder. A disagreement between them has nowhere left to hide.
 */
class ValidatedPipelineBuildsTest {

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: orders_src
            connector: mysql
            config: { host: h }
            mode: cdc
            tables: [ orders ]
            """;

    private static final String TARGET = """
            version: tapstate/v1
            kind: source
            id: orders_dest
            connector: mongodb
            config: { uri: u }
            """;

    /**
     * The shape the product's own valid corpus writes: a transform addressing the table its source reads,
     * a serve addressing the transform. Addressing the source by its id instead is what every builder test
     * does, and the validator rejects it.
     */
    private static final String PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: p
            source: orders_src
            transforms:
              - { id: keep_even, from: [orders], type: filter, expr: "after.id % 2 == 0" }
            serve:
              from: keep_even
              sync: [ { id: sync_1, source: orders_dest } ]
            """;

    /** A source with no transform between it and the sink: the serve addresses the source's table. */
    private static final String DIRECT_PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: direct
            source: orders_src
            serve:
              from: orders
              sync: [ { id: sync_1, source: orders_dest } ]
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

    /**
     * A nest over two single-table sources, with {@code arrayKey} deliberately left out: the element
     * identity then has to come from the embedded table's own key, which only the assembly root can
     * resolve. It is the one part of a nest the engine refuses to work out for itself.
     */
    private static final String NEST_PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: nested
            source: [ orders_src, items_src ]
            transforms:
              - id: doc
                type: nest
                from: { o: orders, i: order_items }
                root:
                  from: o
                  key: [ id ]
                  embed:
                    - { from: i, on: { order_id: id }, as: array, path: items }
            serve:
              from: doc
              sync: [ { id: sync_1, source: orders_dest } ]
            """;

    @Test
    void aValidatedNestPipelineBuildsIntoADag() {
        InMemoryStorePort store = validated(SOURCE, ITEMS_SOURCE, TARGET, NEST_PIPELINE);
        discovered(store, "items_src", "order_items", List.of("id"));

        DAG dag = new StoreBackedDagSource(store, discardingBinder()).dagFor("nested");

        // A nest draws its own vertices: one assembler for the root, and no resolver here because every
        // embed is a leaf. Building at all is the point - the builder refuses a nest step outright when the
        // assembly root supplies no nest binding, which is exactly what it had until now.
        assertThat(vertexNames(dag)).contains("orders_src", "items_src", "serve.sync_1");
        assertThat(vertexNames(dag)).anyMatch(name -> name.startsWith("nest:"));
    }

    @Test
    void aNestEmbedWhoseTableDeclaresNoKeyTellsTheAuthorToDeclareOne() {
        InMemoryStorePort store = validated(SOURCE, ITEMS_SOURCE, TARGET, NEST_PIPELINE);
        discovered(store, "items_src", "order_items", List.of());

        // The embed declares no arrayKey and its table declares no key either, so there is nothing to
        // identify an element by. That is the author's to fix and carries a code saying so - not a crash,
        // and not a silently append-only array.
        assertThatThrownBy(() -> new StoreBackedDagSource(store, discardingBinder()).dagFor("nested"))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> assertThat(((TapstateException) thrown).code().code())
                        .isEqualTo("nest.array-key-unresolvable"));
    }

    /** Two nest steps that both call an alias {@code o}, over different tables. */
    private static final String CLASHING_ALIAS_PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: clash
            source: [ orders_src, items_src ]
            transforms:
              - id: doc_a
                type: nest
                from: { o: orders, i: order_items }
                root:
                  from: o
                  key: [ id ]
                  embed:
                    - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
              - id: doc_b
                type: nest
                from: { o: order_items, i: orders }
                root:
                  from: o
                  key: [ id ]
                  embed:
                    - { from: i, on: { id: order_id }, as: array, path: parents, arrayKey: [ id ] }
            serve:
              from: doc_a
              sync: [ { id: sync_1, source: orders_dest } ]
            """;

    @Test
    void refusesOneAliasNamingTwoDifferentTablesAcrossNestSteps() {
        InMemoryStorePort store = validated(SOURCE, ITEMS_SOURCE, TARGET, CLASHING_ALIAS_PIPELINE);

        // Aliases are declared per step but the nest binding answers per alias, so the two declarations
        // cannot both be honoured. Answering one of them silently would compile a tree whose element
        // identities come from the wrong table -- correct-looking documents built on the wrong key.
        assertThatThrownBy(() -> new StoreBackedDagSource(store, discardingBinder()).dagFor("clash"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("alias 'o'");
    }

    @Test
    void aValidatedPipelineWithATransformBuildsIntoADag() {
        InMemoryStorePort store = validated(SOURCE, TARGET, PIPELINE);

        DAG dag = new StoreBackedDagSource(store, discardingBinder()).dagFor("p");

        assertThat(vertexNames(dag)).containsExactlyInAnyOrder("orders_src", "keep_even", "serve.sync_1");
    }

    /**
     * The serve reaches the source directly, so its reference names a table and must still find the source's
     * vertex. The vertex is keyed by the source id, and nothing but this mapping bridges the two.
     */
    @Test
    void aValidatedPipelineServingItsSourceDirectlyBuildsIntoADag() {
        InMemoryStorePort store = validated(SOURCE, TARGET, DIRECT_PIPELINE);

        DAG dag = new StoreBackedDagSource(store, discardingBinder()).dagFor("direct");

        assertThat(vertexNames(dag)).containsExactlyInAnyOrder("orders_src", "serve.sync_1");
    }

    // ---- fixtures ----------------------------------------------------------------------

    /** Parses and validates through the product's own gate, then stores what it accepted. */
    private static InMemoryStorePort validated(String... documents) {
        DslParser parser = new DslParser();
        List<Resource> resources = new ArrayList<>();
        for (String document : documents) {
            resources.add(parser.parse(document));
        }
        // The acceptance gate an apply runs. A fixture this throws on is a fixture no author could write.
        Workspace.of(resources);
        InMemoryStorePort store = new InMemoryStorePort();
        resources.forEach(store.artifacts()::save);
        OpenRingGenerations.forSources(store, "orders_src");
        return store;
    }

    /** Persists a discovery model for one connection carrying one table and the key it declares. */
    private static void discovered(InMemoryStorePort store, String connectionId, String table, List<String> key) {
        store.schemas().save(new DiscoveredSourceModel(connectionId, "mysql", 0L,
                new SourceModel(List.of(new SourceTable(table, List.of(new SourceField("id", "int")), key, null)))));
    }

    private static StoreBackedDagSource.SinkWriterBinder discardingBinder() {
        return (connectorId, settings, writeMode, ddl, target) -> (SupplierEx<SinkWriter>) () -> null;
    }

    private static List<String> vertexNames(DAG dag) {
        List<String> names = new ArrayList<>();
        for (Vertex vertex : dag) {
            names.add(vertex.getName());
        }
        return names;
    }
}
