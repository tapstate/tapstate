package io.tapstate.control.core;

import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SourceSchemaQueryServiceTest {

    @Test
    void sourceSchemaProjectsOnlyTheTablesTheSourceSelectsWhileConnectionSchemaRemainsComplete() {
        SourceResource source = source(List.of(
                TableRef.literal("orders"),
                TableRef.regex("audit_.*"),
                TableRef.spec("customers", "active == true", List.of("id"), Map.of())));
        DiscoveredSourceModel discovered = discovery("orders", "orders", "payments", "audit_log", "customers");
        SchemaStore schemas = schemaStore(discovered);

        Optional<SchemaReport> sourceSchema = new SourceSchemaQueryService(artifactStore(source), schemas).find("orders");
        Optional<SchemaReport> connectionSchema = new SchemaQueryService(schemas).find("orders");

        assertThat(sourceSchema).isPresent();
        assertThat(sourceSchema.orElseThrow().tables()).extracting(SchemaReport.Table::name)
                .containsExactly("orders", "audit_log", "customers");
        assertThat(connectionSchema).isPresent();
        assertThat(connectionSchema.orElseThrow().tables()).extracting(SchemaReport.Table::name)
                .containsExactly("orders", "payments", "audit_log", "customers");
    }

    @Test
    void aSourceSelectionThatMatchesNoStoredTableProducesAnEmptySchema() {
        SourceResource source = source(List.of(TableRef.literal("orders")));
        SchemaStore schemas = schemaStore(discovery("orders", "legacy_orders"));

        Optional<SchemaReport> report = new SourceSchemaQueryService(artifactStore(source), schemas).find("orders");

        assertThat(report).isPresent();
        assertThat(report.orElseThrow().tables()).isEmpty();
    }

    private static SourceResource source(List<TableRef> tables) {
        return new SourceResource("orders", null, "mysql", Map.of(), null, tables, null, null, null);
    }

    private static DiscoveredSourceModel discovery(String connectionId, String... names) {
        return new DiscoveredSourceModel(
                connectionId,
                "mysql",
                1_700_000_000_000L,
                new SourceModel(List.of(names).stream()
                        .map(name -> new SourceTable(
                                name, List.of(new SourceField("id", "bigint")), List.of("id"), List.of()))
                        .toList()));
    }

    private static ArtifactStore artifactStore(SourceResource source) {
        return new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
                throw new UnsupportedOperationException("read-only in this test");
            }

            @Override
            public Optional<Resource> get(String id) {
                return source.id().equals(id) ? Optional.of(source) : Optional.empty();
            }

            @Override
            public List<Resource> list() {
                return List.of(source);
            }
        };
    }

    private static SchemaStore schemaStore(DiscoveredSourceModel stored) {
        return new SchemaStore() {
            @Override
            public void save(DiscoveredSourceModel discovered) {
                throw new UnsupportedOperationException("read-only in this test");
            }

            @Override
            public Optional<DiscoveredSourceModel> get(String connectionId) {
                return stored.connectionId().equals(connectionId) ? Optional.of(stored) : Optional.empty();
            }
        };
    }
}
