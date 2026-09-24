package io.tapstate.app;

import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.ArtifactDraft;
import io.tapstate.control.core.ArtifactOutcome;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.DerivedSchemas;
import io.tapstate.control.core.PlanAdvisories;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.spi.store.DerivedSchema;
import io.tapstate.spi.store.DerivedSchemaStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.StorePort;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SchemaRefreshAllNodesTest {

    @Test
    void aRealMidRefreshStoreFailureIsVisibleAndAnUnchangedApplyRepairsIt() {
        InMemoryStorePort backing = new InMemoryStorePort();
        StorePort store = mock(StorePort.class, delegatesTo(backing));
        DerivedSchemaStore models = mock(DerivedSchemaStore.class, delegatesTo(backing.derivedSchemas()));
        when(store.derivedSchemas()).thenReturn(models);
        AuditGate audit = new AuditGate(record -> {}, Clock.systemUTC());
        StoreBackedDerivedSchemas schemas = new StoreBackedDerivedSchemas(store, audit);
        ApplyService apply = new ApplyService(TapstateCatalog::load, store.artifacts(), audit,
                store.schemas(), PlanAdvisories.none(), schemas);
        discover(backing, false);
        apply.apply("alice", DRAFTS);
        OpenRingGenerations.forSources(backing, "orders_src");
        new StoreBackedDagSource(backing).dagFor("orders_pipeline");
        Map<String, DerivedSchema> original = records(backing);
        discover(backing, true);
        boolean[] fail = {true};
        doAnswer(call -> {
            if (fail[0] && call.getArgument(1).equals("trimmed")) {
                fail[0] = false;
                throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "test outage"), null);
            }
            backing.derivedSchemas().record(call.getArgument(0), call.getArgument(1), call.getArgument(2),
                    call.getArgument(3), call.getArgument(4), call.getArgument(5));
            return null;
        }).when(models).record(anyString(), anyString(), any(), anyString(), anyString(), anyString());

        var partial = apply.apply("alice", DRAFTS);

        assertThat(partial.outcomes()).hasSize(DRAFTS.size()).allSatisfy(outcome ->
                assertThat(outcome.change()).isEqualTo(ArtifactOutcome.Change.UNCHANGED));
        assertThat(partial.warnings()).singleElement().satisfies(warning -> {
            assertThat(warning.code()).isEqualTo("control.schema-derivation-incomplete");
            assertThat(warning.params()).containsEntry("pipeline", "orders_pipeline")
                    .containsEntry("causeCode", "io.store-unavailable");
        });
        assertThat(records(backing).get("snapshot_rows").schema()).containsKey("region");
        assertThat(records(backing).get("trimmed")).isEqualTo(original.get("trimmed"));
        assertThat(schemas.compare("orders_pipeline")).hasSize(NODES.size())
                .filteredOn(step -> step.step().equals("trimmed")).singleElement()
                .satisfies(step -> assertThat(step.columns()).anyMatch(DerivedSchemas.ColumnReport::drifted));

        var repaired = apply.apply("alice", DRAFTS);

        assertThat(repaired.warnings()).isEmpty();
        assertThat(records(backing)).hasSize(NODES.size());
        records(backing).forEach((node, record) -> {
            assertThat(record.schema()).containsKey("region");
            assertThat(backing.derivedSchemas().pinned("orders_pipeline", node)).contains(original.get(node));
        });
        assertThat(schemas.compare("orders_pipeline")).hasSize(NODES.size()).allSatisfy(step ->
                assertThat(step.columns()).isNotEmpty().noneMatch(DerivedSchemas.ColumnReport::drifted));
    }

    @ParameterizedTest
    @MethodSource("liveStates")
    void applyingWhileARunExistsWarnsWithoutMovingItsModelsOrBlockingOtherArtifacts(PipelineState state) {
        InMemoryStorePort store = new InMemoryStorePort();
        AuditGate audit = new AuditGate(record -> {}, Clock.systemUTC());
        StoreBackedDerivedSchemas schemas = new StoreBackedDerivedSchemas(store, audit);
        ApplyService apply = new ApplyService(TapstateCatalog::load, store.artifacts(), audit,
                store.schemas(), PlanAdvisories.none(), schemas);
        discover(store, false);
        apply.apply("alice", DRAFTS);
        OpenRingGenerations.forSources(store, "orders_src");
        new StoreBackedDagSource(store).dagFor("orders_pipeline");
        Map<String, DerivedSchema> original = records(store);
        store.state().create("orders_pipeline", StateJson.of(state), Instant.EPOCH);
        store.desired().save(new DesiredState("orders_pipeline", state, "revision"));
        discover(store, true);
        List<ArtifactDraft> changed = new ArrayList<>(DRAFTS);
        changed.set(1, new ArtifactDraft("views", DRAFTS.get(1).content().replace("uri: u", "uri: updated")));

        var result = apply.apply("alice", changed);

        assertThat(result.outcomes()).hasSize(3);
        assertThat(result.outcomes().get(1).change()).isEqualTo(ArtifactOutcome.Change.UPDATED);
        assertThat(((SourceResource) store.artifacts().get("views").orElseThrow()).config())
                .containsEntry("uri", "updated");
        assertThat(result.warnings()).singleElement().satisfies(warning -> {
            assertThat(warning.code()).isEqualTo("control.schema-derivation-incomplete");
            assertThat(warning.params()).containsEntry("pipeline", "orders_pipeline")
                    .containsEntry("causeCode", "actuation.schema-sync-while-running");
        });
        assertThat(records(store)).isEqualTo(original);
        original.forEach((node, record) ->
                assertThat(store.derivedSchemas().pinned("orders_pipeline", node)).contains(record));
    }

    private static Stream<PipelineState> liveStates() {
        return Stream.of(PipelineState.RUNNING, PipelineState.PAUSED);
    }

    @Test
    void apply_refreshes_a_joins_downstream_nodes_without_accepting_its_drift_baseline() {
        InMemoryStorePort store = new InMemoryStorePort();
        DslParser parser = new DslParser();
        DRAFTS.subList(0, 2).forEach(draft -> store.artifacts().save(parser.parse(draft.content())));
        store.artifacts().save(parser.parse("""
                version: tapstate/v1
                kind: source
                id: customers_src
                connector: mysql
                config: { host: h }
                mode: cdc
                tables: [customers]
                """));
        store.artifacts().save(parser.parse("""
                version: tapstate/v1
                kind: pipeline
                id: joined
                source: [orders_src, customers_src]
                transforms:
                  - id: customer_orders
                    from: { o: orders, c: customers }
                    type: join
                    engine: builtin
                    sql: SELECT o.id AS order_id, c.name AS customer_name FROM o LEFT JOIN c ON o.id = c.id
                  - { id: positive_orders, from: customer_orders, type: filter, expr: "after.order_id > 0" }
                view:
                  id: joined_view
                  from: positive_orders
                  primary_key: order_id
                  storage: { warm: { collection: joined_view } }
                """));
        discover(store, false);
        store.schemas().save(new DiscoveredSourceModel("customers_src", "mysql", 1L,
                new SourceModel(List.of(new SourceTable("customers", List.of(
                        new SourceField("id", "bigint", TapstateType.INT64),
                        new SourceField("name", "varchar", TapstateType.STRING)), List.of("id"), List.of())))));
        OpenRingGenerations.forSources(store, "orders_src", "customers_src");
        new StoreBackedDagSource(store).dagFor("joined");
        DerivedSchema baseline = store.derivedSchemas().latest("joined", "customer_orders").orElseThrow();
        store.schemas().save(new DiscoveredSourceModel("orders_src", "mysql", 2L,
                new SourceModel(List.of(new SourceTable("orders", List.of(
                        new SourceField("id", "decimal", TapstateType.DECIMAL)), List.of("id"), List.of())))));
        StoreBackedDerivedSchemas schemas = new StoreBackedDerivedSchemas(store,
                new AuditGate(record -> {}, Clock.systemUTC()));

        schemas.derive("joined");

        assertThat(store.derivedSchemas().latest("joined", "customer_orders")).contains(baseline);
        for (String node : List.of("positive_orders", "joined_view")) {
            assertThat(store.derivedSchemas().latest("joined", node)).get()
                    .satisfies(recorded -> assertThat(recorded.schema()).containsEntry("order_id", "DECIMAL NULL"));
            assertThat(store.derivedSchemas().pinned("joined", node)).get()
                    .satisfies(recorded -> assertThat(recorded.schema()).containsEntry("order_id", "INT64 NULL"));
        }
        schemas.accept("alice", "joined");

        assertThat(store.derivedSchemas().latest("joined", "customer_orders")).get()
                .satisfies(recorded -> assertThat(recorded.schema()).containsEntry("order_id", "DECIMAL NULL"));
        assertThat(store.derivedSchemas().pinned("joined", "customer_orders")).contains(baseline);
        assertThat(schemas.compare("joined")).hasSize(5).allSatisfy(step ->
                assertThat(step.columns()).isNotEmpty().noneMatch(DerivedSchemas.ColumnReport::drifted));
    }

    @ParameterizedTest
    @MethodSource("undiscoveredSelections")
    void an_undiscovered_sibling_does_not_block_refresh_of_the_known_branch(List<TableRef> selection) {
        InMemoryStorePort store = new InMemoryStorePort();
        DslParser parser = new DslParser();
        DRAFTS.forEach(draft -> store.artifacts().save(parser.parse(draft.content()
                .replace("source: [orders_src]", "source: [orders_src, pending]")
                .replace("transforms:\n", "transforms:\n"
                        + "  - { id: pending_rows, from: pending, type: filter, expr: true }\n"
                        + "  - { id: all_tables, from: /.*orders/, type: union }\n"))));
        store.artifacts().save(new SourceResource("pending", null, "mongodb", Map.of("uri", "u"),
                SourceMode.CDC, selection, null, null));
        StoreBackedDerivedSchemas schemas = new StoreBackedDerivedSchemas(store,
                new AuditGate(record -> {}, Clock.systemUTC()));
        discover(store, false);
        schemas.derive("orders_pipeline");
        discover(store, true);

        schemas.derive("orders_pipeline");

        assertThat(records(store)).hasSize(NODES.size()).allSatisfy((node, recorded) ->
                assertThat(recorded.schema()).as("known branch %s", node).containsKey("region"));
        assertThat(store.derivedSchemas().latest("orders_pipeline", "pending_rows")).isEmpty();
        // The regex already matches the known table, but could also match an undiscovered one.
        // Recording that partial union would claim its input model was complete.
        assertThat(store.derivedSchemas().latest("orders_pipeline", "all_tables")).isEmpty();

        store.schemas().save(new DiscoveredSourceModel("pending", "mongodb", 1L,
                new SourceModel(List.of(new SourceTable("pending_orders", List.of(
                        new SourceField("pending_value", "varchar", TapstateType.STRING)), List.of(), List.of())))));
        schemas.derive("orders_pipeline");

        assertThat(store.derivedSchemas().latest("orders_pipeline", "pending_rows")).get()
                .satisfies(recorded -> assertThat(recorded.schema()).containsKey("pending_value"));
        assertThat(store.derivedSchemas().latest("orders_pipeline", "all_tables")).get()
                .satisfies(recorded -> assertThat(recorded.schema()).containsKeys("region", "pending_value"));
    }

    private static Stream<List<TableRef>> undiscoveredSelections() {
        return Stream.of(null, List.of(TableRef.literal("pending_orders")), List.of(TableRef.regex(".*")));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void explicit_refresh_records_every_nodes_new_shape_without_moving_run_pins(boolean accept) {
        InMemoryStorePort store = new InMemoryStorePort();
        AuditGate audit = new AuditGate(record -> {}, Clock.systemUTC());
        StoreBackedDerivedSchemas schemas = new StoreBackedDerivedSchemas(store, audit);
        ApplyService apply = new ApplyService(TapstateCatalog::load, store.artifacts(), audit,
                store.schemas(), PlanAdvisories.none(), schemas);
        discover(store, false);
        apply.apply("alice", DRAFTS);
        OpenRingGenerations.forSources(store, "orders_src");
        new StoreBackedDagSource(store).dagFor("orders_pipeline");
        Map<String, DerivedSchema> original = records(store);

        discover(store, true);
        assertThat(records(store)).isEqualTo(original);
        refresh(accept, schemas, apply);

        Map<String, DerivedSchema> refreshed = records(store);
        original.forEach((node, previous) -> {
            DerivedSchema current = refreshed.get(node);
            assertThat(current.schema()).as("new columns at %s", node)
                    .containsEntry("region", "STRING NULL");
            assertThat(current.version()).as("one new version at %s", node)
                    .isEqualTo(previous.version() + 1);
            assertThat(current.statement()).isEqualTo(previous.statement());
            assertThat(current.derivedFrom()).isNotEqualTo(previous.derivedFrom());
            assertThat(store.derivedSchemas().pinned("orders_pipeline", node)).contains(previous);
        });
        assertThat(refreshed.get("orders_src.orders").schema()).containsKey("secret");
        assertThat(refreshed.get("snapshot_rows").schema()).containsKey("secret");
        for (String node : List.of("trimmed", "positive_id", "order_state")) {
            assertThat(refreshed.get(node).schema()).containsExactly(
                    Map.entry("id", "INT64 NULL"), Map.entry("region", "STRING NULL"));
        }
        assertThat(schemas.compare("orders_pipeline")).hasSize(NODES.size())
                .allSatisfy(step -> assertThat(step.columns()).isNotEmpty()
                        .noneMatch(DerivedSchemas.ColumnReport::drifted));

        refresh(accept, schemas, apply);
        assertThat(records(store)).isEqualTo(refreshed);
    }

    private static void refresh(boolean accept, StoreBackedDerivedSchemas schemas, ApplyService apply) {
        if (accept) {
            schemas.accept("alice", "orders_pipeline");
        } else {
            assertThat(apply.apply("alice", DRAFTS).outcomes())
                    .hasSize(DRAFTS.size())
                    .allSatisfy(outcome -> assertThat(outcome.change()).isEqualTo(ArtifactOutcome.Change.UNCHANGED));
        }
    }

    private static Map<String, DerivedSchema> records(InMemoryStorePort store) {
        Map<String, DerivedSchema> records = new LinkedHashMap<>();
        NODES.forEach(node -> records.put(node,
                store.derivedSchemas().latest("orders_pipeline", node).orElseThrow()));
        return records;
    }

    private static void discover(InMemoryStorePort store, boolean region) {
        List<SourceField> fields = new ArrayList<>(List.of(
                new SourceField("id", "bigint", TapstateType.INT64),
                new SourceField("secret", "varchar", TapstateType.STRING)));
        if (region) {
            fields.add(new SourceField("region", "varchar", TapstateType.STRING));
        }
        store.schemas().save(new DiscoveredSourceModel("orders_src", "mysql", region ? 2L : 1L,
                new SourceModel(List.of(new SourceTable("orders", fields, List.of("id"), List.of())))));
    }

    private static final List<String> NODES =
            List.of("orders_src.orders", "snapshot_rows", "trimmed", "positive_id", "order_state");

    private static final List<ArtifactDraft> DRAFTS = List.of(
            new ArtifactDraft("source", """
                    version: tapstate/v1
                    kind: source
                    id: orders_src
                    connector: mysql
                    config: { host: h }
                    mode: cdc
                    tables: [orders]
                    """),
            new ArtifactDraft("views", """
                    version: tapstate/v1
                    kind: source
                    id: views
                    connector: mongodb
                    config: { uri: u }
                    """),
            new ArtifactDraft("pipeline", """
                    version: tapstate/v1
                    kind: pipeline
                    id: orders_pipeline
                    source: [orders_src]
                    transforms:
                      - { id: snapshot_rows, from: orders, type: filter, expr: "op == 'r'" }
                      - { id: trimmed, from: snapshot_rows, type: map, fields: { secret: false } }
                      - { id: positive_id, from: trimmed, type: filter, expr: "after.id > 0" }
                    view:
                      id: order_state
                      from: positive_id
                      primary_key: id
                      storage: { warm: { collection: order_state } }
                    """));
}
