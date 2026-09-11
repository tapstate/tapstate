package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Coverage for resolving a sink's write-side target model from the discovered source model: the pure mapping
 * of one discovered {@link SourceTable} onto a {@link TargetTable}, where the table-level ordered primary-key
 * column list becomes per-field key flags whose order the sink keys an upsert on.
 */
class TargetModelResolverTest {

    @Test
    void numericDescriptorSurvivesTableRenamingAndRekeying() throws Exception {
        var number = new io.tapstate.core.common.NumericType(128, true, false, true, new java.math.BigDecimal("-99999999999999.9999"), new java.math.BigDecimal("99999999999999.9999"), 18, 4);
        var source = new SourceTable("orders", List.of(new SourceField("amount", "decimal(18,4)",
                io.tapstate.core.common.TapstateType.DECIMAL, null, number)), List.of(), List.of());
        var result = TargetModelResolver.keyedOn(TargetModelResolver.rename(TargetModelResolver.toTargetTable(source),
                new io.tapstate.core.model.RenameSpec(null, null, "archive_", null)), List.of("amount"));
        assertThat(result.fields().getFirst().numericType()).isEqualTo(number);
        var bytes = new java.io.ByteArrayOutputStream();
        try (var out = new java.io.ObjectOutputStream(bytes)) { out.writeObject(result); }
        try (var in = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray()))) {
            assertThat(in.readObject()).isEqualTo(result);
        }
    }

    @Test
    void carriesInferredTypesThroughRenamesAndKeyChanges() {
        SourceTable source = new SourceTable("orders", List.of(
                new SourceField("id", "SOURCE_NUMBER", io.tapstate.core.common.TapstateType.INT64, null),
                new SourceField("email", "SOURCE_TEXT", io.tapstate.core.common.TapstateType.STRING, null)),
                List.of("id"), List.of());
        TargetTable target = TargetModelResolver.keyedOn(TargetModelResolver.rename(
                TargetModelResolver.toTargetTable(source),
                new io.tapstate.core.model.RenameSpec(null, null, "archive_", null)), List.of("email"));
        assertThat(target.fields()).extracting(TargetField::inferredType).containsExactly(
                io.tapstate.core.common.TapstateType.STRING, io.tapstate.core.common.TapstateType.INT64);
    }

    @Test
    void maps_a_discovered_table_to_a_target_table_flagging_the_primary_key() {
        SourceTable orders = new SourceTable(
                "orders",
                List.of(new SourceField("id", "INT"), new SourceField("amount", "DECIMAL")),
                List.of("id"),
                List.of());

        TargetTable target = TargetModelResolver.toTargetTable(orders);

        assertThat(target.name()).isEqualTo("orders");
        assertThat(target.fields()).containsExactly(
                new TargetField("id", "INT", true, io.tapstate.core.common.TapstateType.UNKNOWN),
                new TargetField("amount", "DECIMAL", false, io.tapstate.core.common.TapstateType.UNKNOWN));
    }

    @Test
    void orders_primary_key_fields_by_key_order_so_the_upsert_key_matches_the_source() {
        SourceTable line = new SourceTable(
                "line",
                List.of(new SourceField("a", "INT"), new SourceField("b", "INT"), new SourceField("c", "INT")),
                List.of("c", "a"),
                List.of());

        TargetTable target = TargetModelResolver.toTargetTable(line);

        // The sink keys an upsert in target-field order, so the key columns must lead in key order (c, a);
        // the non-key fields follow in source order.
        assertThat(target.fields()).containsExactly(
                new TargetField("c", "INT", true, io.tapstate.core.common.TapstateType.UNKNOWN),
                new TargetField("a", "INT", true, io.tapstate.core.common.TapstateType.UNKNOWN),
                new TargetField("b", "INT", false, io.tapstate.core.common.TapstateType.UNKNOWN));
    }

    @Test
    void maps_a_table_with_no_primary_key_to_all_non_key_fields() {
        SourceTable logs = new SourceTable(
                "logs", List.of(new SourceField("msg", "TEXT")), List.of(), List.of());

        TargetTable target = TargetModelResolver.toTargetTable(logs);

        assertThat(target.fields()).containsExactly(new TargetField("msg", "TEXT", false, io.tapstate.core.common.TapstateType.UNKNOWN));
    }

    @Test
    void resolves_the_target_from_the_discovered_model_of_the_named_source() {
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(cdcSource("src_mysql", "orders"));
        store.artifacts().save(pipeline("p", "src_mysql"));
        store.schemas().save(discovered("src_mysql", "mysql", new SourceTable(
                "orders",
                List.of(new SourceField("id", "INT"), new SourceField("amount", "DECIMAL")),
                List.of("id"),
                List.of())));

        TargetModelResolver.ResolvedTarget target = new TargetModelResolver(store).resolve("src_mysql");

        assertThat(target).isEqualTo(new TargetModelResolver.ResolvedTarget("orders", new TargetTable("orders", List.of(
                new TargetField("id", "INT", true, io.tapstate.core.common.TapstateType.UNKNOWN),
                new TargetField("amount", "DECIMAL", false, io.tapstate.core.common.TapstateType.UNKNOWN)), List.of(new io.tapstate.spi.sink.TargetIndex(List.of("id"), true)))));
    }

    @Test
    void resolves_the_table_with_no_model_when_the_source_schema_was_never_discovered() {
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(cdcSource("src_mysql", "orders"));
        store.artifacts().save(pipeline("p", "src_mysql"));

        TargetModelResolver.ResolvedTarget target = new TargetModelResolver(store).resolve("src_mysql");

        assertThat(target).isEqualTo(new TargetModelResolver.ResolvedTarget("orders", null));
    }

    @Test
    void ignores_a_discovered_model_from_a_different_connector() {
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(cdcSource("src_mysql", "orders"));
        store.artifacts().save(pipeline("p", "src_mysql"));
        store.schemas().save(discovered("src_mysql", "postgres", new SourceTable(
                "orders", List.of(new SourceField("id", "BIGINT")), List.of("id"), List.of())));

        TargetModelResolver.ResolvedTarget target = new TargetModelResolver(store).resolve("src_mysql");

        assertThat(target).isEqualTo(new TargetModelResolver.ResolvedTarget("orders", null));
    }

    @Test
    void resolves_every_table_of_a_source_from_one_read_of_its_discovery() {
        // The discovery is stored per connection and holds every table of it, so resolving a source's
        // tables is one read and a walk. Reading it again per table is the same answer at N times the
        // cost, and it is invisible in every case that seeds one or two tables - the sizes this is
        // written against are the ones a real connection has.
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(multiTableSource("src_mysql", "orders", "customers", "shipments"));
        PipelineResource pipeline = pipeline("p", "src_mysql");
        store.artifacts().save(pipeline);
        store.schemas().save(new DiscoveredSourceModel("src_mysql", "mysql", 0L, new SourceModel(
                List.of(oneColumnTable("orders"), oneColumnTable("customers"),
                        oneColumnTable("shipments")))));
        int before = store.schemaStore().reads();

        Map<String, TargetTable> targets = new TargetModelResolver(store).resolveAll(pipeline);

        // It really did resolve all three: a resolution that answered nothing would also read nothing,
        // and the count below would be the count this is looking for.
        assertThat(targets.keySet()).containsExactly("orders", "customers", "shipments");
        assertThat(store.schemaStore().reads() - before).isEqualTo(1);
    }

    @Test
    void preservesIndexesThroughRenameAndAddsTheChosenUpsertKey() {
        SourceTable source = new SourceTable("orders", List.of(
                new SourceField("id", "INT"), new SourceField("email", "TEXT")),
                List.of("id"), List.of(new io.tapstate.spi.store.SourceIndex(
                        "email_lookup", List.of("email"), false)));
        TargetTable resolved = TargetModelResolver.toTargetTable(source);
        assertThat(resolved.indexes()).containsExactly(
                new io.tapstate.spi.sink.TargetIndex(List.of("id"), true),
                new io.tapstate.spi.sink.TargetIndex(List.of("email"), false));
        TargetTable renamed = TargetModelResolver.rename(resolved,
                new io.tapstate.core.model.RenameSpec(null, null, "archive_", null));
        assertThat(renamed.indexes()).isEqualTo(resolved.indexes());
        assertThat(TargetModelResolver.keyedOn(renamed, List.of("email")).indexes())
                .containsAll(resolved.indexes())
                .contains(new io.tapstate.spi.sink.TargetIndex(List.of("email"), true));
    }

    // ---- fixtures ----------------------------------------------------------------------

    private static SourceResource multiTableSource(String id, String... tables) {
        return new SourceResource(id, null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                java.util.Arrays.stream(tables).map(t -> (TableRef) TableRef.literal(t)).toList(),
                null, null);
    }

    private static SourceTable oneColumnTable(String name) {
        return new SourceTable(name, List.of(new SourceField("id", "INT")), List.of("id"), List.of());
    }

    private static SourceResource cdcSource(String id, String table) {
        return new SourceResource(id, null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(table)), null, null);
    }

    private static PipelineResource pipeline(String id, String sourceId) {
        return new PipelineResource(id, null, List.of(SourceRef.spec(sourceId, true)), null, null, null, null, null);
    }

    private static DiscoveredSourceModel discovered(String connectionId, String connectorId, SourceTable table) {
        return new DiscoveredSourceModel(connectionId, connectorId, 0L, new SourceModel(List.of(table)));
    }
}
