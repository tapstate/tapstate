package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A real clustered pipeline binds durable sink progress to its submitted run, then rebinds it on restart.
 * The specification vocabulary cannot inspect the run identity stored beside a consumer checkpoint.
 * The separate store race witness covers a late acknowledgement; this case covers the production wiring
 * from the HTTP start verb through the distributed sink to that durable fence, without calling app seams.
 */
class ASinkCheckpointBelongsToTheRunThatWroteItIT {

    private static final String PIPELINE = "fenced_sink_pipe";
    private static final String SOURCE = "fenced_sink_src";
    private static final String TARGET = "fenced_sink_tgt";
    private static final String TABLE = "orders";
    private static final Duration DELIVERY = Duration.ofMinutes(2);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void restartingAPipelineRebindsItsDurableCheckpointToTheReplacementRun(@TempDir Path directory)
            throws Exception {
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        String storeUri = SharedMongo.replicaSetUrl("e2e_sink_checkpoint_fence");
        ConnectionString connection = new ConnectionString(storeUri);

        try (FileEndpoints files = new FileEndpoints();
                MongoClient reader = MongoClients.create(connection);
                TwoMemberCluster cluster = TwoMemberCluster.start(storeUri, "e2e-sink-checkpoint-fence")) {
            MongoDatabase database = reader.getDatabase(connection.getDatabase());
            EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
            EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
            files.seed(sourceAddress, TABLE, SeedRows.generated(3));
            cluster.awaitBothMembers();
            ControlPlane control = cluster.first();
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put(SOURCE + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE, source));
            resources.put(TARGET + ".tap.yml", Workspaces.targetYaml(TARGET, target));
            resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE, SOURCE, TARGET, TABLE));
            control.apply(resources);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            awaitState(control, PipelineState.RUNNING);
            Await.until("the first run's rows and checkpoint to land", DELIVERY,
                    () -> files.count(targetAddress, TABLE) == 3 && hasCheckpoint(database),
                    () -> "target rows=" + files.count(targetAddress, TABLE) + ", consumer=" + consumer(database));
            Document firstFence = assertFenceMatchesCurrentRun(database, control.clusterId());

            control.stop(PIPELINE, false);
            awaitState(control, PipelineState.STOPPED);
            files.insert(sourceAddress, TABLE, SeedRows.generated(4).subList(3, 4));
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            awaitState(control, PipelineState.RUNNING);
            Await.until("the replacement run's change and checkpoint to land", DELIVERY,
                    () -> files.count(targetAddress, TABLE) == 4
                            && consumer(database) != null
                            && !firstFence.equals(consumer(database).get("sinkAckFence")),
                    () -> "target rows=" + files.count(targetAddress, TABLE) + ", consumer=" + consumer(database));
            Document replacementFence = assertFenceMatchesCurrentRun(database, control.clusterId());
            assertThat(replacementFence.getLong("executionGeneration"))
                    .as("a replacement execution cannot retain the old run's durable sink fence")
                    .isGreaterThan(firstFence.getLong("executionGeneration"));
        }
    }

    private static void awaitState(ControlPlane control, PipelineState state) {
        Await.until(PIPELINE + " to reach " + state, DELIVERY,
                () -> control.state(PIPELINE).filter(state::equals).isPresent(),
                () -> String.valueOf(control.state(PIPELINE)));
    }

    private static boolean hasCheckpoint(MongoDatabase database) {
        Document consumer = consumer(database);
        return consumer != null && consumer.containsKey("sinkAckedEpoch") && consumer.containsKey("sinkAckedSeq");
    }

    private static Document consumer(MongoDatabase database) {
        return database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS)
                .find(new Document("pipelineId", SrsConsumerId.of(PIPELINE, SOURCE).value())).first();
    }

    private static Document assertFenceMatchesCurrentRun(MongoDatabase database, String clusterId) {
        Document consumer = consumer(database);
        assertThat(consumer).isNotNull();
        Document fence = consumer.get("sinkAckFence", Document.class);
        assertThat(fence).as("the sink's actual durable checkpoint carries its admitted run identity").isNotNull();
        Document claim = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                .find(new Document("_id.clusterId", clusterId)
                        .append("_id.resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                        .append("_id.resourceId", PIPELINE)).first();
        assertThat(claim).isNotNull();
        assertThat(fence).containsEntry("clusterId", clusterId)
                .containsEntry("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                .containsEntry("resourceId", PIPELINE);
        for (String field : new String[] {"ownerNodeId", "ownerBootId", "claimGeneration",
                "executionGeneration", "topologyRevision"}) {
            assertThat(fence).containsEntry(field, claim.get(field));
        }
        return fence;
    }
}
