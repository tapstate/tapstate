package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.dsl.Interpolator;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/** A rejected first admission cannot leave a checkpoint, claim, intent, or fabricated FAILED observation. */
@RequiresDocker
class PatternSourceAdmissionIT {
    private static final String PIPELINE = "pattern_admission";

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void missingDiscoveryRefusesBeforeAnyPipelineRuntimeHistoryIsCreated(Tiers tier, @TempDir Path directory) throws Exception {
        Path source = Files.createDirectories(directory.resolve("source"));
        Path target = Files.createDirectories(directory.resolve("target"));
        Files.writeString(source.resolve("orders.csv"), "id,seq\n1,1\n");
        try (var storage = PreparedNativeStore.create(); var raw = MongoClients.create(storage.configurationUri());
                ServerHandle server = tier.launch(storage)) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            String sourceYaml = Files.readString(Path.of("examples/a-view-whose-source-matches-tables-by-pattern-is-refused-without-discovery/src_file.tap.yml"));
            control.apply(Map.of("src_file.tap.yml", Interpolator.interpolate(sourceYaml, key -> key.equals("SRC_DIR") ? source.toString() : null),
                    "tgt_file.tap.yml", Workspaces.targetYaml("tgt_file", target),
                    "pipeline.tap.yml", Workspaces.pipelineYaml(PIPELINE, "src_file", "tgt_file", "orders")));
            var configuration = raw.getDatabase(storage.configurationDatabase());
            var cache = raw.getDatabase(storage.cacheDatabase());
            var mapping = SystemCollections.OPERATOR_GENERATIONS.on(configuration).find(new Document("_id", PIPELINE)).first();
            var intent = SystemCollections.PIPELINE_INTENTS.on(configuration).find(new Document("_id", PIPELINE)).first();
            assertThat(mapping).isNotNull();
            assertThat(mapping.get("cachePreparation", Document.class).getString("phase")).isEqualTo("UNASSIGNED");
            requireNoRuntime(cache);

            var refusal = control.startExpectingRefusal(PIPELINE);

            assertThat(refusal.status()).isEqualTo(409);
            assertThat(refusal.code()).isEqualTo("actuation.source-schema-not-discovered");
            assertThat(refusal.params()).containsEntry("source", "src_file");
            requireNoRuntime(cache);
            assertThat(SystemCollections.OPERATOR_GENERATIONS.on(configuration).find(new Document("_id", PIPELINE)).first()).isEqualTo(mapping);
            assertThat(SystemCollections.PIPELINE_INTENTS.on(configuration).find(new Document("_id", PIPELINE)).first()).isEqualTo(intent);
        }
    }

    private static void requireNoRuntime(MongoDatabase cache) {
        assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(cache).find(new Document("_id", PIPELINE)).first()).isNull();
        assertThat(SystemCollections.CACHE_PIPELINE_OBSERVATION.on(cache).find(new Document("_id", PIPELINE)).first()).isNull();
        assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cache).countDocuments(new Document("_id.resourceId", PIPELINE))).isZero();
        assertThat(SystemCollections.SRS_CONSUMERS.on(cache).countDocuments(new Document("pipelineId", PIPELINE))).isZero();
    }
}
