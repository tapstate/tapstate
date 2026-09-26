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
import io.tapstate.core.lifecycle.ParallelismBudget;
import io.tapstate.runtime.srs.SourcePlacement;
import io.tapstate.runtime.engine.nest.NestSettings;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    private static final String FLAT_NEST_PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: flat
            source: [ orders_src, items_src ]
            transforms:
              - id: doc
                type: nest
                from: { o: orders, i: order_items }
                root:
                  from: o
                  key: [ id ]
                  embed:
                    - from: i
                      on: { order_id: id }
                      as: flat
                      key: [ detail_id ]
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

    /**
     * The threads a nest's vertices hold count against what a member may run of them, counted from the tree the
     * nest compiles to: here one vertex, so two wide it holds two threads - past a member allowed one, and within
     * one allowed two.
     */
    @Test
    void aWideNestIsRefusedWhereItsVerticesWouldHoldMoreThreadsThanAMemberMay() {
        InMemoryStorePort store = validated(SOURCE, ITEMS_SOURCE, TARGET, NEST_PIPELINE.replace(
                "    type: nest\n", "    type: nest\n    execution: { parallelism: 2 }\n"));
        discovered(store, "items_src", "order_items", List.of("id"));

        assertThatThrownBy(() -> sourceAllowing(store, 1).dagFor("nested"))
                .isInstanceOfSatisfying(TapstateException.class, refused -> {
                    assertThat(refused.code()).isEqualTo(ActuationError.NO_SAFE_PARALLELISM);
                    assertThat(refused.args()).containsEntry("node", "doc").containsEntry("candidates",
                            "2 per member breaks " + ParallelismBudget.MAX_BLOCKING_PROCESSORS_PER_MEMBER);
                });
        assertThat(sourceAllowing(store, 2).dagFor("nested")).isNotNull();
    }

    /** The assembled source on one member, allowed {@code threads} threads for vertices that hold one each. */
    private static StoreBackedDagSource sourceAllowing(InMemoryStorePort store, int threads) {
        return new StoreBackedDagSource(store, NestSettings.defaults(), StoreReachability.assumingReachable(),
                SourcePlacement.anyMember(), () -> 1, new ParallelismBudget(16, 8, 262144, threads));
    }

    @Test
    void discoveredModelsRefuseAFlatFieldCollisionBeforeTheJobStarts() {
        InMemoryStorePort store = validated(SOURCE, ITEMS_SOURCE, TARGET, FLAT_NEST_PIPELINE);
        discovered(store, "orders_src", "orders", List.of("id"), "id", "name");
        discovered(store, "items_src", "order_items", List.of("detail_id"),
                "detail_id", "order_id", "name");

        assertThatThrownBy(() -> new StoreBackedDagSource(store, discardingBinder()).dagFor("flat"))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code().code()).isEqualTo("nest.flat-field-conflict");
                    assertThat(error.args()).containsEntry("fields", "name");
                });
    }

    /**
     * A join step, all the way from the artifact to the graph. Everything the engine will not work out
     * comes from here: the SQL compiled into a plan (by a library the runtime ring cannot see), the key
     * the driving rows are filed under (from the discovered schema, which that ring may not read), and
     * where the state lives. Nothing else joins those three up, so nothing else would notice any of
     * them going missing.
     */
    @Test
    void aValidatedJoinPipelineBuildsIntoADag() {
        InMemoryStorePort store = validated(SOURCE, ITEMS_SOURCE, TARGET, JOIN_PIPELINE);
        discovered(store, "orders_src", "orders", List.of("id"));
        discovered(store, "items_src", "order_items", List.of("id"));

        DAG dag = new StoreBackedDagSource(store, discardingBinder()).dagFor("wide");

        assertThat(vertexNames(dag)).contains("orders_src", "items_src", "widen", "serve.sync_1");
    }

    @Test
    void aCompiledJoinCarriesEachDimensionSourcesOwnRowKeyUnderItsAlias() {
        InMemoryStorePort store = validated(SOURCE, ITEMS_SOURCE, TARGET, JOIN_PIPELINE);
        discovered(store, "orders_src", "orders", List.of("id"));
        discovered(store, "items_src", "order_items", List.of("id"));

        StoreBackedDagSource.CompiledJoin compiled =
                new StoreBackedDagSource(store, discardingBinder()).compiledJoinsOf("wide").get("widen");

        assertThat(compiled.factKeyColumns()).containsExactly("id");
        assertThat(compiled.dimensionRowKeyColumns())
                .containsExactly(Map.entry("i", List.of("id")));
    }

    /**
     * The driving source declares no key. Every row a join mirrors and every entry in its reverse index
     * is filed under that key, so without one two different rows share an entry - which is not an error
     * anywhere, it simply builds the wide row out of whichever of them was written last. The author is
     * told, with a code, instead.
     */
    @Test
    void aJoinWhoseDrivingTableDeclaresNoKeyTellsTheAuthorToDeclareOne() {
        InMemoryStorePort store = validated(SOURCE, ITEMS_SOURCE, TARGET, JOIN_PIPELINE);
        discovered(store, "orders_src", "orders", List.of());
        discovered(store, "items_src", "order_items", List.of("id"));

        assertThatThrownBy(() -> new StoreBackedDagSource(store, discardingBinder()).dagFor("wide"))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("actuation.join-source-key-missing");
    }

    /**
     * A join over two single-table sources, addressed the way an author writes it. It matches on the
     * one column the discovery fixture declares, because what this is about is the wiring rather than
     * the join being a sensible one.
     */
    private static final String JOIN_PIPELINE = """
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
                  SELECT o.id AS order_id, i.id AS item_id
                  FROM o JOIN i ON i.id = o.id
            serve:
              from: widen
              sync: [ { id: sync_1, source: orders_dest } ]
            """;

    /**
     * The same join written with the table names the aliases stand for. The SQL is legal and derives -
     * both spellings are registered as sources - but only an alias reaches the topology.
     */
    private static final String BARE_TABLE_JOIN_PIPELINE = """
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
                  SELECT orders.id AS order_id, order_items.id AS item_id
                  FROM orders JOIN order_items ON order_items.id = orders.id
            serve:
              from: widen
              sync: [ { id: sync_1, source: orders_dest } ]
            """;

    /**
     * A source named by its table where the step declared an alias for it. The vertex wiring looks each
     * source up in the step's declared from-map, so a name that is not one of its keys has nowhere to be
     * wired from - and the author never wrote the concept the failure would otherwise name.
     */
    @Test
    void aJoinNamingTheTableRatherThanItsAliasIsRefusedWithACode() {
        InMemoryStorePort store =
                validated(SOURCE, ITEMS_SOURCE, TARGET, BARE_TABLE_JOIN_PIPELINE);
        discovered(store, "orders_src", "orders", List.of("id"));
        discovered(store, "items_src", "order_items", List.of("id"));

        assertThatThrownBy(() -> new StoreBackedDagSource(store, discardingBinder()).dagFor("wide"))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> assertThat(((TapstateException) thrown).code().code())
                        .isEqualTo("actuation.join-source-not-declared"));
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
        discovered(store, "orders_src", "orders", List.of("id"));

        DAG dag = new StoreBackedDagSource(store, discardingBinder()).dagFor("p");

        // A sink its author wrote no width for runs four writers, and reads a keyed table through a router.
        assertThat(vertexNames(dag))
                .containsExactlyInAnyOrder("orders_src", "keep_even", "route.serve.sync_1", "serve.sync_1");
        assertThat(dag.getVertex("serve.sync_1").getLocalParallelism()).isEqualTo(4);
    }

    /** A source asked to be read by four processors is still read by one: its reader stays pinned to one. */
    @Test
    void aSourceAskedForMoreThanOneReaderIsStillReadByOne() {
        InMemoryStorePort store = validated(SOURCE.replace("tables: [ orders ]\n",
                "tables: [ orders ]\nexecution: { parallelism: 4 }\n"), TARGET, PIPELINE);
        discovered(store, "orders_src", "orders", List.of("id"));

        DAG dag = new StoreBackedDagSource(store, discardingBinder()).dagFor("p");

        assertThat(dag.getVertex("orders_src").getMetaSupplier().preferredLocalParallelism())
                .as("one reader for the whole cluster").isEqualTo(1);
    }

    /**
     * A sync element whose author asked for one writer runs as one writer for the whole cluster, with nothing
     * in front of it to route by - the width written in the element, carried from the text to the graph.
     */
    @Test
    void aSyncElementAskingForOneWriterRunsItWithNoRouter() {
        InMemoryStorePort store = validated(SOURCE, TARGET, PIPELINE.replace(
                "sync: [ { id: sync_1, source: orders_dest } ]",
                "sync: [ { id: sync_1, source: orders_dest, execution: { parallelism: 1 } } ]"));
        discovered(store, "orders_src", "orders", List.of("id"));

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
        discovered(store, "orders_src", "orders", List.of("id"));

        DAG dag = new StoreBackedDagSource(store, discardingBinder()).dagFor("direct");

        assertThat(vertexNames(dag)).containsExactlyInAnyOrder("orders_src", "route.serve.sync_1", "serve.sync_1");
    }

    // ---- fixtures ----------------------------------------------------------------------

    /** Parses and validates through the product's own gate, then stores what it accepted. */
    /**
     * A pipeline whose join SQL this release cannot run. The refusal happens while the artifact is
     * read, which is the whole point of it: the shapes it names run silently wrong rather than
     * failing, so the check has to sit in the offline path.
     *
     * <p><b>What this case is really about is that the check runs at all, here.</b> It is written in
     * one core module against a SQL library, and whether that library reaches the assembly root is a
     * property of the dependency graph rather than of any code. Measured: one exclusion on the path
     * this module reaches that library by took every one of its artifacts off this classpath, and the
     * whole build stayed green - because nothing else parses a join step from here. A missing library
     * makes this case fail to link rather than refuse, which is a different failure and a loud one.
     */
    @Test
    void aJoinShapeThisReleaseCannotRunIsRefusedWhileTheArtifactIsRead() {
        assertThatThrownBy(() -> new DslParser().parse(UNRUNNABLE_JOIN_PIPELINE))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("FULL OUTER JOIN");
    }

    /** The same shape the validator's own corpus refuses, addressed the way an author writes it. */
    private static final String UNRUNNABLE_JOIN_PIPELINE = """
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
                  SELECT o.id AS order_id, i.id AS item_id
                  FROM o FULL OUTER JOIN i ON i.order_id = o.id
            serve:
              from: widen
              sync: [ { id: sync_1, source: orders_dest } ]
            """;

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
        discovered(store, connectionId, table, key, "id");
    }

    /** The same fixture with the complete discovered field names used by flat preflight checks. */
    private static void discovered(InMemoryStorePort store, String connectionId, String table, List<String> key,
            String... fields) {
        store.schemas().save(new DiscoveredSourceModel(connectionId, "mysql", 0L,
                new SourceModel(List.of(new SourceTable(table,
                        java.util.Arrays.stream(fields).map(field -> new SourceField(field, "text")).toList(),
                        key,
                        null)))));
    }

    private static StoreBackedDagSource.SinkWriterBinder discardingBinder() {
        return (connectorId, settings, writeMode, ddl, target, node) -> (SupplierEx<SinkWriter>) () -> null;
    }

    private static List<String> vertexNames(DAG dag) {
        List<String> names = new ArrayList<>();
        for (Vertex vertex : dag) {
            names.add(vertex.getName());
        }
        return names;
    }
}
