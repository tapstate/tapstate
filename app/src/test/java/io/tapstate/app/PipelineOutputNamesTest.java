package io.tapstate.app;

import io.tapstate.control.core.*;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.spi.store.DataBrowserTableInfo;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineOutputNamesTest {
    @Test
    void undiscoveredUnwindListsOnlyItsDeclaredOutputWhileStartStillRequiresDiscovery() {
        var store = sources();
        var pipeline = pipeline(store, """
                transforms:
                  - id: expanded
                    type: unwind
                    from: orders
                    path: items
                    element_key: sku
                serve:
                  from: expanded
                  sync: [{id: sink, source: target, rename: {prefix: unwind_}}]
                """);
        var resolver = new StoreBackedDagSource(store);
        assertThat(resolver.outputNames(pipeline)).singleElement().satisfies(output -> {
            assertThat(output.table()).isEqualTo("unwind_orders");
            assertThat(output.identity()).isEqualTo("sink:orders");
        });
        assertThat(catalog(store).list()).singleElement().satisfies(row -> {
            assertThat(row.id()).isEqualTo("p:sink:orders");
            assertThat(row.location()).isEqualTo(new ViewCatalogItem.Location("target", "dest", "unwind_orders"));
            assertThat(row.documents()).isNull();
            assertThat(row.status().state()).isEqualTo("not-loaded");
        });
        assertThatThrownBy(() -> new StoreBackedPipelineWriteTargets(store).of(pipeline))
                .hasMessageContaining("actuation.source-schema-not-discovered");
    }

    @Test
    void nestAndJoinUseTheRootAndFactRatherThanTheirStepIds() {
        for (String transform : List.of("""
                transforms:
                  - id: assembled
                    type: nest
                    from: {o: orders, c: customers}
                    root:
                      from: o
                      key: [id]
                      embed: [{from: c, on: {id: customer_id}, as: object, path: customer}]
                """, """
                transforms:
                  - id: assembled
                    type: join
                    from: {o: orders, c: customers}
                    engine: builtin
                    sql: SELECT o.id FROM o LEFT JOIN c ON o.customer_id = c.id
                """)) {
            var store = sources();
            var pipeline = pipeline(store, transform + """
                    serve:
                      from: assembled
                      sync: [{id: sink, source: target, rename: {prefix: shaped_}}]
                    """);
            assertThat(new StoreBackedDagSource(store).outputNames(pipeline)).singleElement()
                    .satisfies(output -> assertThat(output.table()).isEqualTo("shaped_orders"));
        }
    }

    @Test
    void partialMapDoesNotFilterOutputsAndEachSinkUsesItsOwnRename() {
        var store = sources();
        var pipeline = pipeline(store, """
                serve:
                  from: [orders, customers]
                  sync:
                    - {id: one, source: target, rename: {map: {orders: chosen}, prefix: pre_}}
                    - {id: two, source: target, rename: {case: upper, suffix: _COPY}}
                """);
        assertThat(new StoreBackedDagSource(store).outputNames(pipeline))
                .extracting(PipelineOutputNames.Output::table)
                .containsExactly("chosen", "pre_customers", "ORDERS_COPY", "CUSTOMERS_COPY");
    }

    @Test
    void unavailableDiscoveryIsReportedWithoutDroppingHealthyPipelines() {
        var store = sources();
        pipeline(store, """
                serve:
                  from: orders
                  sync: [{id: sink, source: target}]
                """);
        store.artifacts().save(new DslParser().parse("""
                version: tapstate/v1
                kind: source
                id: unknown
                connector: mongodb
                config: {uri: "mongodb://localhost/input"}
                tables: ["/order.*/"]
                """));
        store.artifacts().save(new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: unresolved
                source: unknown
                serve:
                  from: unknown
                  sync: [{id: sink, source: target}]
                """));
        var result = catalog(store).catalog();
        assertThat(result.items()).singleElement().satisfies(row -> assertThat(row.pipelineId()).isEqualTo("p"));
        assertThat(result.unresolved()).singleElement().satisfies(problem -> {
            assertThat(problem.pipelineId()).isEqualTo("unresolved");
            assertThat(problem.code()).isEqualTo("actuation.source-schema-not-discovered");
            assertThat(problem.params()).containsEntry("source", "unknown");
        });
    }

    @Test
    void knownManagedViewSurvivesAnUnresolvedSyncInTheSamePipeline() {
        var store = sources();
        store.artifacts().save(new DslParser().parse("""
                version: tapstate/v1
                kind: source
                id: unknown
                connector: mongodb
                tables: ["/order.*/"]
                """));
        var pipeline = (PipelineResource) new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: mixed
                source: [src, unknown]
                view: {id: managed, from: src.orders, primary_key: _id, storage: {warm: {collection: known}}}
                serve: {from: unknown, sync: [{id: sink, source: target}]}
                """);
        store.artifacts().save(pipeline);
        var result = catalog(store).catalog();
        assertThat(result.items()).singleElement().satisfies(row -> assertThat(row.location().collection()).isEqualTo("known"));
        assertThat(result.unresolved()).hasSize(1);
    }

    @Test
    void qualifiedJoinPreservesTheDrivingSourceWhenPhysicalNamesRepeat() {
        assertQualifiedJoin(", rename: {map: {left.orders: wrong, right.orders: correct}}", "correct");
    }

    @Test
    void qualifiedJoinKeepsItsExistingDefaultWriteAddress() {
        assertQualifiedJoin("", "right.orders");
    }

    @Test
    void qualifiedNestUsesTheConfiguredRootSourcesRename() {
        assertQualifiedOutput("""
                  - id: shaped
                    type: nest
                    from: {o: right.orders, c: left.orders}
                    root:
                      from: o
                      key: [id]
                      embed: [{from: c, on: {id: id}, as: object, path: related}]
                """, ", rename: {map: {left.orders: wrong, right.orders: correct}}", "correct");
    }

    private static void assertQualifiedJoin(String rename, String expected) {
        assertQualifiedOutput("""
                  - id: shaped
                    type: join
                    from: {o: right.orders, c: left.orders}
                    engine: builtin
                    sql: SELECT o.id FROM o LEFT JOIN c ON o.id = c.id
                """, rename, expected);
    }

    private static void assertQualifiedOutput(String transform, String rename, String expected) {
        var store = sources();
        var parser = new DslParser();
        for (String id : List.of("left", "right")) {
            store.artifacts().save(parser.parse("version: tapstate/v1\nkind: source\nid: " + id
                    + "\nconnector: mongodb\ntables: [orders]\n"));
        }
        var pipeline = (PipelineResource) parser.parse("""
                version: tapstate/v1
                kind: pipeline
                id: joined
                source: [left, right]
                transforms:
                %sserve:
                  from: shaped
                  sync: [{id: sink, source: target%s}]
                """.formatted(transform, rename));
        assertThat(new StoreBackedDagSource(store).outputNames(pipeline)).singleElement()
                .satisfies(output -> assertThat(output.table()).isEqualTo(expected));
        store.artifacts().save(pipeline);
        for (String source : List.of("left", "right")) {
            store.schemas().save(new io.tapstate.spi.store.DiscoveredSourceModel(source, "mongodb", 0L,
                    new io.tapstate.spi.store.SourceModel(List.of(new io.tapstate.spi.store.SourceTable("orders",
                            List.of(new io.tapstate.spi.store.SourceField("id", "INT", io.tapstate.core.common.TapstateType.INT64, null)),
                            List.of("id"), List.of())))));
        }
        assertThat(new StoreBackedPipelineWriteTargets(store).of(pipeline)).singleElement()
                .satisfies(target -> assertThat(target.table()).isEqualTo(expected));
        OpenRingGenerations.forSources(store, "left", "right");
        Map<String, io.tapstate.spi.sink.TargetTable> bound = new java.util.LinkedHashMap<>();
        var binder = new StoreBackedDagSource.SinkWriterBinder() {
            @Override
            public com.hazelcast.function.SupplierEx<? extends io.tapstate.spi.sink.SinkWriter> bind(
                    String connector, Map<String, Object> config, io.tapstate.spi.sink.WriteMode mode,
                    io.tapstate.spi.sink.DdlPolicy ddl, io.tapstate.spi.sink.TargetTable target,
                    io.tapstate.core.model.PipelineNode node) {
                throw new AssertionError("Expected a stream-keyed binding");
            }

            @Override
            public com.hazelcast.function.SupplierEx<? extends io.tapstate.spi.sink.SinkWriter> bind(
                    String connector, Map<String, Object> config, io.tapstate.spi.sink.WriteMode mode,
                    io.tapstate.spi.sink.DdlPolicy ddl, Map<String, io.tapstate.spi.sink.TargetTable> targets,
                    io.tapstate.core.model.PipelineNode node) {
                bound.putAll(targets);
                return () -> null;
            }
        };
        new StoreBackedDagSource(store, binder).dagFor("joined");
        assertThat(bound).containsOnlyKeys("shaped");
        assertThat(bound.get("shaped").name()).isEqualTo(expected);
    }

    @Test
    void reusableServeAndChainedTransformsKeepTheExplicitTargetAndStableIdentity() {
        var store = sources();
        store.artifacts().save(new DslParser().parse("""
                version: tapstate/v1
                kind: serve
                id: publish
                sync: [{id: sink, source: target, rename: {map: {orders: exact_name}, prefix: ignored_}}]
                """));
        var pipeline = pipeline(store, """
                transforms:
                  - {id: first, type: filter, from: src.orders, expr: "true"}
                  - {id: second, type: unwind, from: first, path: items, element_key: sku}
                serve: {use: publish, from: second}
                """);
        assertThat(new StoreBackedDagSource(store).outputNames(pipeline)).singleElement().satisfies(output -> {
            assertThat(output.table()).isEqualTo("exact_name");
            assertThat(output.identity()).isEqualTo("sink:orders");
        });
    }

    private static InMemoryStorePort sources() {
        var store = new InMemoryStorePort();
        var parser = new DslParser();
        store.artifacts().save(parser.parse("""
                version: tapstate/v1
                kind: source
                id: src
                connector: mongodb
                config: {uri: "mongodb://localhost/input"}
                tables: [orders, customers]
                """));
        store.artifacts().save(parser.parse("""
                version: tapstate/v1
                kind: source
                id: target
                connector: mongodb
                config: {uri: "mongodb://localhost/dest"}
                """));
        return store;
    }

    private static PipelineResource pipeline(InMemoryStorePort store, String body) {
        var pipeline = (PipelineResource) new DslParser().parse(
                "version: tapstate/v1\nkind: pipeline\nid: p\nsource: src\n" + body);
        store.artifacts().save(pipeline);
        return pipeline;
    }

    private static ViewCatalogService catalog(InMemoryStorePort store) {
        var artifacts = new ArtifactQueryService(store.artifacts());
        var browser = new DataBrowserService(store.artifacts(), store.schemas(), config -> List.of(),
                (config, collection) -> new DataBrowserTableInfo(7L, 42L, 6L),
                (config, query) -> { throw new AssertionError("must not read rows"); },
                (config, request, listener) -> { throw new AssertionError("must not follow rows"); });
        return new ViewCatalogService(artifacts, new PipelineObservationQueryService(artifacts, store.observations()),
                browser, new StoreBackedPipelineOutputNames(store, false));
    }
}
