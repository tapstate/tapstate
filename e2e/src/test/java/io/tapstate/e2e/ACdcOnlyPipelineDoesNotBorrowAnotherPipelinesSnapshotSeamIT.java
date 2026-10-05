package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A CDC-only pipeline starts at its own present on a cleared source and at the shared checkpoint on a held source.
 *
 * <p>Clearing the only consumer removes the actual mining-chain record. Its successor must carry only
 * the row written after it starts. Keeping the consumer retains the chain: the newcomer carries a row
 * written while it was stopped, but excludes an older row already behind the observed checkpoint. The
 * original consumer is then started again and must deliver its own stopped-period change.
 *
 * <p>The two sources deliberately have different artifact ids and table selections but identical physical
 * Mongo coordinates. Mining-chain identity excludes the table subset, so they meet in the one persisted
 * chain whose source-qualified consumer progress is under test. The observed real connector places the
 * writes on known sides of stream teardown and startup; the result is read independently from the target.
 */
class ACdcOnlyPipelineDoesNotBorrowAnotherPipelinesSnapshotSeamIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final String FIRST_SOURCE = "seam_owner_source";
    private static final String SECOND_SOURCE = "cdc_only_source";
    private static final String FIRST_TARGET = "seam_owner_target";
    private static final String SECOND_TARGET = "cdc_only_target";
    private static final String FIRST_PIPELINE = "seam_owner_pipeline";
    private static final String SECOND_PIPELINE = "cdc_only_pipeline";
    private static final String FIRST_COLLECTION = "seam_owner_orders";
    private static final String SECOND_COLLECTION = "cdc_only_orders";
    private static final String PASSED_BY_THE_CHAIN = "written-before-the-chain-moved-on";
    private static final String MOVES_THE_CHAIN_ON = "moves-the-chain-on";
    private static final String WHILE_STOPPED = "written-while-the-seam-owner-was-stopped";
    private static final String BEFORE_START = "written-before-cdc-only-start";
    private static final String AFTER_START = "written-after-cdc-only-start";

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aCdcOnlyPipelineDoesNotReplayAChangeFromBeforeItsOwnStart(
            Tiers tier, @TempDir Path temporary) throws Exception {
        run(tier, temporary, true);
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aCdcOnlyPipelineOnAHeldSourceCarriesOnFromTheCheckpointAndNotTheSeam(
            Tiers tier, @TempDir Path temporary) throws Exception {
        run(tier, temporary, false);
    }

    private static void run(Tiers tier, Path temporary, boolean clearState) throws Exception {
        String suffix = (clearState ? "cleared_" : "held_") + tier.name().toLowerCase(Locale.ROOT);
        String sourceDatabase = "cdc_own_start_source_" + suffix;
        String sourceUri = SharedMongo.replicaSetUrl(sourceDatabase);
        String targetUri = SharedMongo.replicaSetUrl("cdc_own_start_target_" + suffix);
        Path witness = temporary.resolve("mongo-writes");
        Path tail = temporary.resolve("mongo-writes.tail");
        byte[] connector = ObservedMongoConnectorJar.build(ConnectorJars.bytesFor("mongodb"), witness);
        String storeUri = SharedMongo.replicaSetUrl("cdc_own_start_store_" + suffix);

        try (MongoClient source = MongoClients.create(sourceUri);
                MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = tier.launch(storeUri);
                StoreDocuments documents = StoreDocuments.at(storeUri)) {
            seed(source, sourceDatabase, FIRST_COLLECTION, "snapshot-row");
            ControlPlane control = connected(server, connector);

            control.apply(workspace(
                    FIRST_SOURCE, FIRST_TARGET, FIRST_PIPELINE, FIRST_COLLECTION,
                    "snapshot_and_cdc", sourceUri, targetUri));
            control.discoverSchema(
                    FIRST_SOURCE, "mongodb", Map.of("uri", sourceUri, "database", sourceDatabase));
            control.lifecycle(FIRST_PIPELINE, LifecycleVerb.START);

            EndpointAddress target = EndpointAddress.uri(targetUri);
            Await.until("the seam-owning pipeline to land its snapshot", TIMEOUT,
                    () -> names(mongo, target, FIRST_COLLECTION).contains("snapshot-row"),
                    () -> names(mongo, target, FIRST_COLLECTION).toString());
            Await.until("the seam-owning pipeline's tail to open", TIMEOUT,
                    () -> tailLines(tail).size() == 1 && tailLines(tail).getFirst().startsWith("START "),
                    () -> tailLines(tail).toString());

            String chain = Await.answered("the seam owner's source-qualified mining chain", TIMEOUT, () -> {
                List<String> candidates = documents.miningChainIds().stream()
                        .filter(id -> ownerConsumer(documents, id) != null).toList();
                return candidates.size() == 1 ? Optional.of(candidates.getFirst()) : Optional.empty();
            });
            if (!clearState) {
                seed(source, sourceDatabase, SECOND_COLLECTION, PASSED_BY_THE_CHAIN);
                Checkpoint beforeChange = Await.answered("the durable capture checkpoint before the owner's change",
                        TIMEOUT, () -> checkpoint(documents.chain(chain)));
                insert(source, sourceDatabase, FIRST_COLLECTION, 2, MOVES_THE_CHAIN_ON);
                Await.until("the seam owner to land its change after the older newcomer row", TIMEOUT,
                        () -> names(mongo, target, FIRST_COLLECTION).contains(MOVES_THE_CHAIN_ON),
                        () -> names(mongo, target, FIRST_COLLECTION).toString());
                Await.until("the seam owner's source-qualified table confirmation", TIMEOUT,
                        () -> hasOwnedCdcConfirmation(documents, chain),
                        () -> String.valueOf(ownerConsumer(documents, chain)));
                // Compare this capture's batch checkpoints, not the table confirmation's ring sequence.
                // The confirmed change can itself advance the checkpoint; no later heartbeat is required.
                Await.until("the delivered owner's change to have an advanced capture checkpoint", TIMEOUT,
                        () -> checkpoint(documents.chain(chain))
                                .filter(next -> next.epoch() == beforeChange.epoch()
                                        && next.sequence() > beforeChange.sequence()).isPresent(),
                        () -> String.valueOf(documents.chain(chain)));
            }

            control.stop(FIRST_PIPELINE, clearState);
            awaitState(control, FIRST_PIPELINE, PipelineState.STOPPED);
            Await.until("the seam-owning pipeline's tail to return", TIMEOUT,
                    () -> tailLines(tail).size() == 2 && tailLines(tail).getLast().equals("END"),
                    () -> tailLines(tail).toString());

            if (clearState) {
                Await.until("clearing the sole consumer to remove its actual mining-chain record", TIMEOUT,
                        () -> documents.chain(chain) == null,
                        () -> String.valueOf(documents.chain(chain)));
                seed(source, sourceDatabase, SECOND_COLLECTION, BEFORE_START);
            } else {
                assertThat(documents.chain(chain)).as("keeping state retains the shared checkpoint").isNotNull();
                assertThat(ownerConsumer(documents, chain)).as("keeping state retains the source's own consumer")
                        .isNotNull();
                insert(source, sourceDatabase, SECOND_COLLECTION, 2, BEFORE_START);
                insert(source, sourceDatabase, FIRST_COLLECTION, 3, WHILE_STOPPED);
            }
            control.apply(workspace(
                    SECOND_SOURCE, SECOND_TARGET, SECOND_PIPELINE, SECOND_COLLECTION,
                    "cdc_only", sourceUri, targetUri));
            control.discoverSchema(
                    SECOND_SOURCE, "mongodb", Map.of("uri", sourceUri, "database", sourceDatabase));
            control.lifecycle(SECOND_PIPELINE, LifecycleVerb.START);
            awaitState(control, SECOND_PIPELINE, PipelineState.RUNNING);
            Await.until("the CDC-only pipeline's tail to open", TIMEOUT,
                    () -> tailLines(tail).size() == 3 && tailLines(tail).getLast().startsWith("START "),
                    () -> tailLines(tail).toString());

            insert(source, sourceDatabase, SECOND_COLLECTION, clearState ? 2 : 3, AFTER_START);
            Await.until("the change written after the CDC-only start to reach the target", TIMEOUT,
                    () -> names(mongo, target, SECOND_COLLECTION).contains(AFTER_START),
                    () -> "rows=" + names(mongo, target, SECOND_COLLECTION)
                            + ", state=" + control.state(SECOND_PIPELINE)
                            + ", logs=" + control.logs(SECOND_PIPELINE));

            if (clearState) {
                assertThat(names(mongo, target, SECOND_COLLECTION))
                        .as("the live CDC-only tail on a cleared chain carries only its post-start row")
                        .containsExactly(AFTER_START);
            } else {
                assertThat(names(mongo, target, SECOND_COLLECTION))
                        .as("the held chain replays from its checkpoint, past the older snapshot-seam row")
                        .containsExactly(AFTER_START, BEFORE_START);
                control.lifecycle(FIRST_PIPELINE, LifecycleVerb.START);
                awaitState(control, FIRST_PIPELINE, PipelineState.RUNNING);
                Await.until("the retained consumer to deliver its stopped-period change", TIMEOUT,
                        () -> names(mongo, target, FIRST_COLLECTION).contains(WHILE_STOPPED),
                        () -> "rows=" + names(mongo, target, FIRST_COLLECTION)
                                + ", state=" + control.state(FIRST_PIPELINE)
                                + ", logs=" + control.logs(FIRST_PIPELINE));
                assertThat(names(mongo, target, FIRST_COLLECTION))
                        .containsExactly(MOVES_THE_CHAIN_ON, "snapshot-row", WHILE_STOPPED);
                assertThat(control.errorCount(FIRST_PIPELINE)).contains(0L);
            }
            assertThat(control.errorCount(SECOND_PIPELINE)).contains(0L);
        }
    }

    private record Checkpoint(String token, long epoch, long sequence) { }

    private static Optional<Checkpoint> checkpoint(Document chain) {
        if (chain == null || !Boolean.TRUE.equals(chain.get("sourceReadDurable"))
                || !(chain.get("sourceReadOffset") instanceof String token) || token.isBlank()
                || !(chain.get("sourceReadEpoch") instanceof Number epoch) || epoch.longValue() < 1L
                || !(chain.get("epoch") instanceof Number active) || active.longValue() != epoch.longValue()
                || !(chain.get("sourceReadSeq") instanceof Number sequence)) {
            return Optional.empty();
        }
        return Optional.of(new Checkpoint(token, epoch.longValue(), sequence.longValue()));
    }

    private static Document ownerConsumer(StoreDocuments documents, String chain) {
        Document consumer = documents.consumerOffset(chain, SrsConsumerId.of(FIRST_PIPELINE, FIRST_SOURCE).value());
        if (consumer == null || !FIRST_PIPELINE.equals(consumer.getString("ownerPipelineId"))
                || !FIRST_SOURCE.equals(consumer.getString("sourceNodeId"))) {
            return null;
        }
        return consumer;
    }

    private static boolean hasOwnedCdcConfirmation(StoreDocuments documents, String chain) {
        Document consumer = ownerConsumer(documents, chain);
        if (consumer == null || !(consumer.get("sinkAckedByTable") instanceof Document tables)
                || !(tables.get(FIRST_COLLECTION) instanceof Document confirmed)) {
            return false;
        }
        return confirmed.get("sinkAckedEpoch") instanceof Number epoch && epoch.longValue() > 0L
                && confirmed.get("sinkAckedSeq") instanceof Number sequence && sequence.longValue() >= 0L
                && confirmed.get("sinkAckedSrcpos") instanceof String token && !token.isBlank();
    }

    private static ControlPlane connected(ServerHandle server, byte[] connector) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin("e2e", "e2e-password");
        control.registerConnector("mongodb", connector);
        return control;
    }

    private static Map<String, String> workspace(
            String sourceId,
            String targetId,
            String pipelineId,
            String collection,
            String readMode,
            String sourceUri,
            String targetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(sourceId + ".tap.yml", sourceYaml(sourceId, collection, sourceUri));
        resources.put(targetId + ".tap.yml", targetYaml(targetId, targetUri));
        resources.put(pipelineId + ".tap.yml",
                pipelineYaml(pipelineId, sourceId, targetId, collection, readMode));
        return resources;
    }

    private static String sourceYaml(String sourceId, String collection, String sourceUri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(sourceId, sourceUri, collection);
    }

    private static String targetYaml(String targetId, String targetUri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(targetId, targetUri);
    }

    private static String pipelineYaml(
            String pipelineId, String sourceId, String targetId, String collection, String readMode) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: %s }
                transforms:
                  - { id: all_rows, from: [%s], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: %s
                """.formatted(pipelineId, sourceId, readMode, collection, targetId);
    }

    private static void seed(
            MongoClient source, String database, String collection, String name) {
        source.getDatabase(database).getCollection(collection).drop();
        insert(source, database, collection, 1, name);
    }

    private static void insert(
            MongoClient source, String database, String collection, int id, String name) {
        source.getDatabase(database).getCollection(collection)
                .insertOne(new Document("_id", id).append("oid", id).append("name", name));
    }

    private static List<String> names(
            MongoEndpoints mongo, EndpointAddress target, String collection) {
        return mongo.documents(target, collection).stream()
                .map(document -> document.getString("name"))
                .sorted()
                .toList();
    }

    private static List<String> tailLines(Path tail) {
        try {
            return Files.exists(tail) ? Files.readAllLines(tail) : List.of();
        } catch (java.io.IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }

    private static void awaitState(
            ControlPlane control, String pipelineId, PipelineState expected) {
        Await.until(pipelineId + " to reach " + expected, TIMEOUT,
                () -> control.state(pipelineId).filter(expected::equals).isPresent(),
                () -> String.valueOf(control.state(pipelineId)));
    }
}
