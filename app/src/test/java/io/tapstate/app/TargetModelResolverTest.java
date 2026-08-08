package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.RenameCase;
import io.tapstate.core.model.RenameSpec;
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
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Coverage for resolving a sink's write-side target model from the discovered source model: the pure mapping
 * of one discovered {@link SourceTable} onto a {@link TargetTable}, where the table-level ordered primary-key
 * column list becomes per-field key flags whose order the sink keys an upsert on.
 */
class TargetModelResolverTest {

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
                new TargetField("id", "INT", true),
                new TargetField("amount", "DECIMAL", false));
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
                new TargetField("c", "INT", true),
                new TargetField("a", "INT", true),
                new TargetField("b", "INT", false));
    }

    @Test
    void maps_a_table_with_no_primary_key_to_all_non_key_fields() {
        SourceTable logs = new SourceTable(
                "logs", List.of(new SourceField("msg", "TEXT")), List.of(), List.of());

        TargetTable target = TargetModelResolver.toTargetTable(logs);

        assertThat(target.fields()).containsExactly(new TargetField("msg", "TEXT", false));
    }

    @Test
    void explicit_table_map_takes_precedence_over_bulk_rename_rules() {
        SourceTable address = new SourceTable(
                "PlayerAddress", List.of(new SourceField("id", "INT")), List.of("id"), List.of());

        TargetTable target = TargetModelResolver.toTargetTable(address,
                new RenameSpec(Map.of("PlayerAddress", "player_address"), RenameCase.UPPER, "ods_", "_v1"));

        assertThat(target.name()).isEqualTo("player_address");
    }

    @Test
    void bulk_table_rename_applies_case_before_prefix_and_suffix() {
        SourceTable address = new SourceTable(
                "PLAYER_ADDRESS", List.of(new SourceField("id", "INT")), List.of("id"), List.of());

        TargetTable target = TargetModelResolver.toTargetTable(address,
                new RenameSpec(null, RenameCase.CAMEL, "ods_", "_v1"));

        assertThat(target.name()).isEqualTo("ods_playerAddress_v1");
    }

    @Test
    void bulk_table_rename_supports_pascal_case() {
        SourceTable address = new SourceTable(
                "player_address", List.of(new SourceField("id", "INT")), List.of("id"), List.of());

        TargetTable target = TargetModelResolver.toTargetTable(address,
                new RenameSpec(null, RenameCase.PASCAL, null, null));

        assertThat(target.name()).isEqualTo("PlayerAddress");
    }

    @Test
    void bulk_table_rename_preserves_acronym_and_digit_boundaries() {
        SourceTable server = new SourceTable(
                "HTTP2ServerV1", List.of(new SourceField("id", "INT")), List.of("id"), List.of());

        TargetTable target = TargetModelResolver.toTargetTable(server,
                new RenameSpec(null, RenameCase.PASCAL, null, null));

        assertThat(target.name()).isEqualTo("Http2ServerV1");
    }

    @Test
    void bulk_table_rename_supports_upper_case() {
        SourceTable address = new SourceTable(
                "PlayerAddress", List.of(new SourceField("id", "INT")), List.of("id"), List.of());

        TargetTable target = TargetModelResolver.toTargetTable(address,
                new RenameSpec(null, RenameCase.UPPER, null, null));

        assertThat(target.name()).isEqualTo("PLAYERADDRESS");
    }

    @Test
    void resolves_the_target_from_the_discovered_model_of_the_pipelines_source() {
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(cdcSource("src_mysql", "orders"));
        store.artifacts().save(pipeline("p", "src_mysql"));
        store.schemas().save(discovered("src_mysql", "mysql", new SourceTable(
                "orders",
                List.of(new SourceField("id", "INT"), new SourceField("amount", "DECIMAL")),
                List.of("id"),
                List.of())));

        Optional<TargetModelResolver.ResolvedTarget> target =
                new TargetModelResolver(store).resolve(pipelineArtifact(store, "p"));

        assertThat(target).contains(new TargetModelResolver.ResolvedTarget("orders", new TargetTable("orders", List.of(
                new TargetField("id", "INT", true),
                new TargetField("amount", "DECIMAL", false)))));
    }

    @Test
    void resolves_to_empty_when_the_source_schema_was_never_discovered() {
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(cdcSource("src_mysql", "orders"));
        store.artifacts().save(pipeline("p", "src_mysql"));

        Optional<TargetModelResolver.ResolvedTarget> target =
                new TargetModelResolver(store).resolve(pipelineArtifact(store, "p"));

        assertThat(target).isEmpty();
    }

    // ---- fixtures ----------------------------------------------------------------------

    private static SourceResource cdcSource(String id, String table) {
        return new SourceResource(id, null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(table)), null, null, null);
    }

    private static PipelineResource pipeline(String id, String sourceId) {
        return new PipelineResource(id, null, List.of(sourceId), null, null, null, null, null);
    }

    private static PipelineResource pipelineArtifact(InMemoryStorePort store, String id) {
        return (PipelineResource) store.artifacts().get(id).orElseThrow();
    }

    private static DiscoveredSourceModel discovered(String connectionId, String connectorId, SourceTable table) {
        return new DiscoveredSourceModel(connectionId, connectorId, 0L, new SourceModel(List.of(table)));
    }
}
