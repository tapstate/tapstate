package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.migration.MigrationRunner;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.event.Op;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.ArtifactBatchWrite;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactWrite;
import io.tapstate.spi.store.RegistrationSource;
import io.tapstate.spi.store.SrsLogRecord;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Gated real-Atlas metadata-store witness, independent of the Atlas PDK connector. */
class ManagedAtlasMetadataIT {

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: metadata_probe
            connector: mongodb
            config: { host: example.invalid }
            """;

    @Test
    void atlasMetadataMigratesPersistsAndReopensAcrossSeparateDatabases() {
        String baseUri = System.getenv("TAPSTATE_ATLAS_TEST_URI");
        assumeTrue(baseUri != null && !baseUri.isBlank(),
                "a controlled Atlas URI is required for this live witness");

        String suffix = UUID.randomUUID().toString().substring(0, 12);
        String metadataDatabase = "ts_plan_metadata_" + suffix;
        String operatorDatabase = "ts_plan_operator_" + suffix;
        String metadataUri = inDatabase(baseUri, metadataDatabase);
        byte[] connectorBytes = "plan-only-connector-bytes".getBytes(StandardCharsets.UTF_8);
        Instant sampleAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        RateSample sample = new RateSample("metadata_probe_pipeline", sampleAt,
                Map.of("records.out", 2L), Map.of("probe", 0L), sampleAt.minusSeconds(60));
        String cleanupUri = System.getenv("TAPSTATE_ATLAS_CLEANUP_URI");
        if (cleanupUri == null || cleanupUri.isBlank()) {
            cleanupUri = baseUri;
        }
        MongoConnectionSettings settings = new MongoConnectionSettings(
                metadataUri, null, Duration.ofSeconds(20));

        try (MongoClient cleanup = MongoClients.create(cleanupUri)) {
            try {
                try (MongoConnection connection = new MongoConnection(settings)) {
                    connection.verify();
                    assertMigrationCurrent(connection);
                    MongoStorePort port = new MongoStorePort(connection, operatorDatabase, Duration.ofHours(1));
                    DslParser parser = new DslParser();
                    var first = parser.parse(SOURCE);
                    var second = parser.parse(SOURCE.replace("metadata_probe", "metadata_probe_second"));
                    var rolledBack = parser.parse(SOURCE.replace("metadata_probe", "metadata_probe_rolled_back"));
                    port.artifacts().saveAll(List.of(first, second));
                    assertThat(port.artifacts().writeAll(List.of(
                            ArtifactWrite.createOnly(rolledBack), ArtifactWrite.createOnly(first))))
                            .isEqualTo(ArtifactBatchWrite.refused("metadata_probe", ArtifactMutation.ALREADY_EXISTS));
                    assertThat(port.artifacts().get("metadata_probe_rolled_back")).isEmpty();
                    port.state().create("metadata_probe_pipeline", "{\"phase\":\"snapshot\"}", Instant.now());
                    port.keyedState().save("nest.metadata_probe_pipeline", "key",
                            "cold-state".getBytes(StandardCharsets.UTF_8));
                    port.connectors().register("mongodb-atlas", "2.0.5-SNAPSHOT", RegistrationSource.SEED,
                            connectorBytes);
                    port.rateHistory().append(sample);
                    port.srsLog().store("metadata-probe-ring", 1L,
                            new SrsLogRecord("resume-token", Op.INSERT, 1L, null,
                                    Map.of("id", 1), 0L));
                    assertThat(expirySeconds(connection.database())).isEqualTo(3_600L);
                }

                try (MongoConnection reopened = new MongoConnection(settings)) {
                    reopened.verify();
                    assertMigrationCurrent(reopened);
                    MongoStorePort port = new MongoStorePort(reopened, operatorDatabase, Duration.ofMinutes(30));
                    assertThat(port.artifacts().get("metadata_probe")).isPresent();
                    assertThat(port.artifacts().get("metadata_probe_second")).isPresent();
                    assertThat(port.artifacts().get("metadata_probe_rolled_back")).isEmpty();
                    assertThat(port.state().read("metadata_probe_pipeline")).isPresent();
                    assertThat(port.keyedState().load("nest.metadata_probe_pipeline", "key"))
                            .hasValueSatisfying(bytes -> assertThat(bytes)
                                    .isEqualTo("cold-state".getBytes(StandardCharsets.UTF_8)));
                    assertThat(port.connectors().list()).hasSize(1);
                    assertThat(port.connectors().artifact(port.connectors().list().get(0).contentHash()))
                            .hasValueSatisfying(bytes -> assertThat(bytes).isEqualTo(connectorBytes));
                    assertThat(port.rateHistory().readPage("metadata_probe_pipeline",
                            sampleAt.minusSeconds(1), sampleAt.plusSeconds(1), null, 2).entries())
                            .extracting(entry -> entry.sample()).containsExactly(sample);
                    assertThat(port.srsLog().load("metadata-probe-ring", 1L))
                            .hasValueSatisfying(record -> assertThat(record.srcToken()).isEqualTo("resume-token"));
                    assertThat(expirySeconds(reopened.database())).isEqualTo(1_800L);

                    MongoDatabase metadata = reopened.database();
                    assertThat(metadata.getCollection(MongoStorePort.ARTIFACTS).countDocuments()).isEqualTo(2);
                    assertThat(metadata.getCollection(MongoStorePort.SRS_LOG).countDocuments()).isEqualTo(1);
                    assertThat(metadata.getCollection(MongoStorePort.PIPELINE_RATE_HISTORY).countDocuments())
                            .isEqualTo(1);
                    assertThat(metadata.getCollection(MongoStorePort.CONNECTOR_ARTIFACTS + ".files")
                            .countDocuments()).isEqualTo(1);
                    assertThat(metadata.getCollection(MongoStorePort.OPERATOR_STATE).countDocuments()).isZero();
                    assertThat(reopened.client().getDatabase(operatorDatabase)
                            .getCollection(MongoStorePort.OPERATOR_STATE).countDocuments()).isEqualTo(1);
                }
            } finally {
                cleanup.getDatabase(metadataDatabase).drop();
                cleanup.getDatabase(operatorDatabase).drop();
            }
        }
    }

    private static void assertMigrationCurrent(MongoConnection connection) {
        MigrationRunner.Status status = connection.systemDataStatus();
        assertThat(status.installed()).isEqualTo(status.supported())
                .isEqualTo(MigrationRunner.SUPPORTED_VERSION);
        assertThat(status.pending()).isEmpty();
    }

    private static long expirySeconds(MongoDatabase database) {
        MongoCollection<Document> history = database.getCollection(MongoStorePort.PIPELINE_RATE_HISTORY);
        List<Document> indexes = history.listIndexes().into(new java.util.ArrayList<>());
        Document expiring = indexes.stream()
                .filter(index -> index.containsKey("expireAfterSeconds"))
                .findFirst().orElseThrow();
        return expiring.get("expireAfterSeconds", Number.class).longValue();
    }

    private static String inDatabase(String uri, String database) {
        int schemeEnd = uri.indexOf("://");
        int slash = schemeEnd < 0 ? -1 : uri.indexOf('/', schemeEnd + 3);
        if (slash < 0) {
            throw new IllegalArgumentException("Atlas test URI must include a database path");
        }
        int options = uri.indexOf('?', slash);
        return uri.substring(0, slash + 1) + database
                + (options < 0 ? "" : uri.substring(options));
    }
}
