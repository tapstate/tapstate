package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.dsl.DslError;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
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
 *
 * <p>These cases are the wiring, so they run against a store rather than against a batch: what a
 * deployment holds and what an author submitted are different sets here, and the two directions the
 * rule has to get right are only visible once they differ. A write target is filed once and referred
 * to afterwards, so it is normally stored rather than resubmitted; a stored artifact an earlier
 * release filed is not the author's to answer for, and on the typed path the resource set reaching
 * validation reaches it anyway.
 */
class ApplyServiceTargetConnectorTest {

    private static final Clock FIXED_CLOCK =
            Clock.fixed(Instant.parse("2026-09-15T10:15:30Z"), ZoneOffset.UTC);

    private static final String READ_SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_orders
            connector: mysql
            config: { host: 10.10.0.5, database: ods, username: u, password: p }
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

    private static final String SERVE_DEFINITION = """
            version: tapstate/v1
            kind: serve
            id: out
            sync: [ { id: s, source: tgt_pg, write_mode: upsert } ]
            """;

    private static final String PIPELINE_USING_THE_DEFINITION = """
            version: tapstate/v1
            kind: pipeline
            id: orders_out
            source: src_orders
            transforms:
              - { id: keep, from: [orders], type: filter, expr: "op != 'd'" }
            serve: out
            """;

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

    // ---- what a deployment already holds ---------------------------------------------------

    @Test
    @DisplayName("a typed pipeline write is refused for a definition and a target it only references")
    void aTypedPipelineWriteIsRefusedForADefinitionAndTargetItOnlyReferences() {
        // The typed face submits one document. Everything the pipeline writes through is stored, so a
        // rule resolving ids within the submitted set has nothing to judge and installs the write.
        DslParser parser = new DslParser();
        artifacts.landDirectly(parser.parse(READ_SOURCE));
        artifacts.landDirectly(parser.parse(target("tgt_pg", "postgres",
                "{ host: 10.30.0.6, database: dw, username: w, password: p }")));
        artifacts.landDirectly(parser.parse(SERVE_DEFINITION));

        DslException thrown = catchThrowableOfType(DslException.class, () ->
                service.create("tester", parser.parse(PIPELINE_USING_THE_DEFINITION)));

        assertThat(thrown.code()).isEqualTo(DslError.UNSUPPORTED_TARGET_CONNECTOR);
        assertThat(thrown.args()).containsEntry("connector", "postgres").containsEntry("resource", "out");
        assertThat(artifacts.get("orders_out")).isEmpty();
    }

    @Test
    @DisplayName("apply refuses a sync written in a serve definition, and stores nothing")
    void applyRefusesASyncCarriedByAServeDefinition() {
        // Nothing existence-checks a standalone definition's sinks — the reference closure visits an
        // inline serve only — so a definition is the one place a sync can name a target the batch
        // never carried. A rule resolving ids within the submitted set finds nothing here.
        service.apply("tester", List.of(new ArtifactDraft("tgt_pg.tap.yml",
                target("tgt_pg", "postgres", "{ host: 10.30.0.6, database: dw, username: w, password: p }"))));

        DslException thrown = catchThrowableOfType(DslException.class, () -> service.apply("tester",
                List.of(new ArtifactDraft("src_orders.tap.yml", READ_SOURCE),
                        new ArtifactDraft("out.tap.yml", SERVE_DEFINITION),
                        new ArtifactDraft("orders_out.tap.yml", PIPELINE_USING_THE_DEFINITION))));

        assertThat(thrown.code()).isEqualTo(DslError.UNSUPPORTED_TARGET_CONNECTOR);
        assertThat(thrown.args()).containsEntry("resource", "out");
        assertThat(thrown.path()).isEqualTo("sync[0].source");
        assertThat(artifacts.get("orders_out")).isEmpty();
        assertThat(artifacts.get("out")).isEmpty();
    }

    @Test
    @DisplayName("a typed source edit is not refused for a stored pipeline the closure only pulled in")
    void aTypedSourceEditIsNotRefusedForAPipelineItOnlyPulledIn() {
        // The state an upgrade inherits: a pipeline filed while a relational target was still
        // installable. The typed write path validates the submitted source's referrer closure, which
        // reaches that pipeline and, through it, the connection it writes to.
        DslParser parser = new DslParser();
        artifacts.landDirectly(parser.parse(READ_SOURCE));
        artifacts.landDirectly(parser.parse(target("tgt_pg", "postgres",
                "{ host: 10.30.0.6, database: dw, username: w, password: p }")));
        artifacts.landDirectly(parser.parse(pipelineWritingTo("tgt_pg")));
        Resource rotated = parser.parse(READ_SOURCE.replace("password: p", "password: rotated"));

        assertThatCode(() -> service.replace("tester", rotated,
                CanonicalHash.of(artifacts.get("src_orders").orElseThrow())))
                .doesNotThrowAnyException();
        assertThat(artifacts.get("src_orders").orElseThrow()).isEqualTo(rotated);
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

        /**
         * The conditional batch the typed write path uses: every named id must still store the hash
         * declared against it, or the batch writes nothing and names the id that moved on.
         */
        @Override
        public Optional<String> saveAll(List<Resource> artifacts, Map<String, String> expectedContentHashes) {
            for (Map.Entry<String, String> precondition : expectedContentHashes.entrySet()) {
                Resource stored = byId.get(precondition.getKey());
                if (stored == null || !CanonicalHash.of(stored).equals(precondition.getValue())) {
                    return Optional.of(precondition.getKey());
                }
            }
            saveAll(artifacts);
            return Optional.empty();
        }

        /** Puts a resource in the store without going through apply, the way an earlier release left it. */
        void landDirectly(Resource resource) {
            byId.put(resource.id(), resource);
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
