package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.dsl.DslError;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.SchemaStore;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Apply is where a write target is judged. Which connectors may be written to is a property of the
 * deployment being applied to — it accepts the supported set plus whatever it named for itself — so
 * the document stays as valid offline as the grammar says it is, and the server is what refuses to
 * install it.
 *
 * <p>The refusal has to land before anything is written, for the same reason as the other apply-path
 * gates: an apply that refused the pipeline and stored it anyway would leave behind exactly the
 * artifact the refusal exists to keep out.
 */
class ApplyServiceTargetConnectorTest {

    private static final Clock FIXED_CLOCK =
            Clock.fixed(Instant.parse("2026-09-15T10:15:30Z"), ZoneOffset.UTC);

    private static final String READ_SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_orders
            connector: mysql
            config: { host: 10.10.0.5, username: u, password: p }
            mode: cdc
            tables: [ orders ]
            """;

    private static String target(String id, String connector, String config) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: %s
                """.formatted(id, connector, config);
    }

    private static String pipelineWritingTo(String targetId) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: orders_out
                source: src_orders
                serve:
                  from: orders
                  sync: [ { id: out, source: %s, write_mode: upsert } ]
                """.formatted(targetId);
    }

    private final InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
    private final InMemorySchemaStore schemas = new InMemorySchemaStore();
    private final ApplyService service = new ApplyService(
            TapstateCatalog::load, artifacts, new AuditGate(record -> { }, FIXED_CLOCK), schemas,
            PlanAdvisories.none(), SchemaDerivation.none());

    private List<ArtifactDraft> batch(String targetId, String targetDocument) {
        return List.of(new ArtifactDraft("src_orders.tap.yml", READ_SOURCE),
                new ArtifactDraft(targetId + ".tap.yml", targetDocument),
                new ArtifactDraft("orders_out.tap.yml", pipelineWritingTo(targetId)));
    }

    @Test
    @DisplayName("apply refuses a sync writing through a supported connector that is not the write target")
    void applyRefusesAnotherSupportedConnector() {
        DslException thrown = catchThrowableOfType(DslException.class, () -> service.apply("tester",
                batch("tgt_pg", target("tgt_pg", "postgres",
                        "{ host: 10.30.0.6, database: dw, username: w, password: p }"))));

        assertThat(thrown.code()).isEqualTo(DslError.UNSUPPORTED_TARGET_CONNECTOR);
        assertThat(thrown.args())
                .containsEntry("connector", "postgres")
                .containsEntry("source", "tgt_pg");
    }

    @Test
    @DisplayName("a refused apply writes nothing, so the refused pipeline is not left stored")
    void aRefusedApplyStoresNothing() {
        catchThrowableOfType(DslException.class, () -> service.apply("tester",
                batch("tgt_pg", target("tgt_pg", "postgres",
                        "{ host: 10.30.0.6, database: dw, username: w, password: p }"))));

        assertThat(artifacts.get("orders_out")).isEmpty();
        assertThat(artifacts.get("tgt_pg")).isEmpty();
        assertThat(artifacts.saved).isEmpty();
    }

    @Test
    @DisplayName("the same batch applies once the sync writes through the supported connector")
    void applyAcceptsTheSupportedConnector() {
        // Same reader, same pipeline, same everything but the connector under the target — so what
        // the case above refused is the connector and not the shape of the batch.
        assertThatCode(() -> service.apply("tester",
                batch("tgt_mg", target("tgt_mg", "mongodb", "{ uri: \"mongodb://10.30.0.11:27017/ods\" }"))))
                .doesNotThrowAnyException();
        assertThat(artifacts.get("orders_out")).isPresent();
    }

    // ---- doubles -------------------------------------------------------------------------

    private static final class InMemoryArtifactStore implements ArtifactStore {
        final Map<String, Resource> byId = new LinkedHashMap<>();
        final List<Resource> saved = new ArrayList<>();

        @Override
        public void saveAll(List<Resource> artifacts) {
            saved.addAll(artifacts);
            artifacts.forEach(r -> byId.put(r.id(), r));
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.ofNullable(byId.get(id));
        }

        @Override
        public List<Resource> list() {
            return List.copyOf(byId.values());
        }
    }

    private static final class InMemorySchemaStore implements SchemaStore {
        private final Map<String, DiscoveredSourceModel> byConnection = new HashMap<>();

        @Override
        public void save(DiscoveredSourceModel discovered) {
            byConnection.put(discovered.connectionId(), discovered);
        }

        @Override
        public Optional<DiscoveredSourceModel> get(String connectionId) {
            return Optional.ofNullable(byConnection.get(connectionId));
        }
    }
}
