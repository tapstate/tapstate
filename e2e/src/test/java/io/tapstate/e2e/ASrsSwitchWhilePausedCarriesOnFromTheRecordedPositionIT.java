package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoDesiredStore;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStateStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.srs.CaptureError;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.testsupport.DockerGate;

import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A held shared source cannot reinterpret its retained recovery state as an independent direct channel.
 *
 * <p>The two modes keep different recovery coordinates. A shared capture checkpoint can advance while
 * this consumer is paused, so it cannot stand in for that consumer's confirmed direct-source boundary.
 * Transparent migration with retained state is unsupported: the edited resume must report the precise
 * coded refusal, preserve the user's RUNNING intent and keep the original source-qualified recovery
 * document. It must neither clear the record nor silently open an independent channel from it.
 *
 * <p>The real shared snapshot, a delivered CDC update, PAUSE and a paused-period delete and insert all
 * execute before the switch. After refusal, the complete target documents must equal their paused
 * baseline: the deleted source row remains at the held target and the new source row is absent there.
 * Both source changes are verified directly, so a no-op fixture cannot satisfy that result.
 *
 * <p>A clearing stop leaves the target intact. A new full snapshot therefore cannot express the held
 * delete without a separate target reload policy, and this case makes no reset-as-migration promise.
 * Mongo is on both ends; only the source reference's buffering flag changes in the reapplied workspace.
 *
 * <p>Gated on Docker and on a directory of real connector jars
 * ({@code -Dtapstate.e2e.connectors-dir}); the real-process tier additionally needs the app module
 * packaged. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=ASrsSwitchWhilePausedCarriesOnFromTheRecordedPositionIT -Dtest=NoSuchUnitTestOnPurpose
 * </pre>
 */
class ASrsSwitchWhilePausedCarriesOnFromTheRecordedPositionIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    private static final long SEEDED_ROWS = 5;
    private static final String COLLECTION = "orders";
    private static final String SOURCE_ID = "src_mongo";
    private static final String TARGET_ID = "tgt_mongo";
    private static final String BEFORE_PAUSE = "changed-while-buffering";
    private static final int CHANGED_BEFORE = 1;
    private static final int DELETED_WHILE_PAUSED = 2;
    private static final int ADDED_WHILE_PAUSED = 6;
    private static final String ADDED_NAME = "added-while-paused";

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aRetainedSharedSourceCannotBeTransparentlyResumedAsAnIndependentDirectChannel(Tiers tier) {
        String suffix = "srs_switch_paused_" + tier.name().toLowerCase(Locale.ROOT);
        String database = suffix + "_src";
        String sourceUri = SharedMongo.replicaSetUrl(database);
        String targetUri = SharedMongo.replicaSetUrl(suffix + "_tgt");
        String storeUri = SharedMongo.replicaSetUrl(suffix + "_state");
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (MongoClient source = MongoClients.create(new ConnectionString(sourceUri));
                MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = tier.launch(storeUri);
                MongoClient stored = MongoClients.create(new ConnectionString(storeUri))) {

            String stateName = new ConnectionString(storeUri).getDatabase();
            assertThat(stateName).isNotBlank();
            MongoDatabase state = stored.getDatabase(stateName);
            MongoDesiredStore desired = new MongoDesiredStore(state.getCollection(MongoStorePort.PIPELINE_DESIRED));
            MongoStateStore actual = new MongoStateStore(state.getCollection(MongoStorePort.PIPELINE_STATE));
            MongoObservationStore latest = new MongoObservationStore(stored,
                    state.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                    state.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
            seed(source, database);
            ControlPlane control = start(server, suffix, sourceUri, targetUri);

            awaitCount(mongo, target, "the full load of the seeded documents");
            // A change while it is running and buffering, so the run ends holding a position it confirmed
            // rather than only a finished load: carrying on has to have something to carry on from.
            rename(source, database, CHANGED_BEFORE, BEFORE_PAUSE);
            awaitName(mongo, target, CHANGED_BEFORE, BEFORE_PAUSE, "the change the buffered tail captured");

            String sharedChain = sourceChain(control, suffix);
            Await.until("the source-qualified consumer to confirm its snapshot and CDC update", TIMEOUT,
                    () -> settledRecovery(consumer(state, sharedChain, suffix)),
                    () -> String.valueOf(consumer(state, sharedChain, suffix)));
            control.lifecycle(suffix, LifecycleVerb.PAUSE);
            awaitState(control, suffix, PipelineState.PAUSED);
            var paused = latest.readStored(suffix).orElseThrow();
            assertThat(paused.observation().pipelineId()).isEqualTo(suffix);
            assertThat(paused.observation().state()).isEqualTo(PipelineState.PAUSED);
            assertThat(paused.observation().failure()).isNull();
            assertThat(paused.observation().observedAt()).isNotNull();
            var scope = paused.scope().orElseThrow();
            long originalExecution = executionGeneration(state, suffix);
            assertThat(originalExecution).isPositive().isEqualTo(scope.executionGeneration());
            var pausedActual = actual.read(suffix).orElseThrow();
            assertThat(StateJson.parse(pausedActual.stateJson())).isEqualTo(PipelineState.PAUSED);
            Document savedRecovery = consumer(state, sharedChain, suffix);
            assertThat(savedRecovery).isNotNull();
            assertThat(settledRecovery(savedRecovery)).isTrue();
            List<Document> pausedTarget = mongo.documents(target, COLLECTION);
            assertThat(pausedTarget).hasSize((int) SEEDED_ROWS);
            assertThat(namesOf(mongo, target, DELETED_WHILE_PAUSED)).containsExactly("order-" + DELETED_WHILE_PAUSED);

            // The logical consumer is held. Physical shared capture may still log both source changes;
            // its checkpoint is not this paused consumer's direct-channel recovery boundary.
            delete(source, database, DELETED_WHILE_PAUSED);
            insert(source, database, ADDED_WHILE_PAUSED, ADDED_NAME);

            assertThat(source.getDatabase(database).getCollection(COLLECTION)
                    .find(new Document("_id", DELETED_WHILE_PAUSED)).first()).isNull();
            assertThat(source.getDatabase(database).getCollection(COLLECTION)
                    .find(new Document("_id", ADDED_WHILE_PAUSED)).first())
                    .isNotNull().satisfies(row -> assertThat(row.get("name")).isEqualTo(ADDED_NAME));

            // Changing this one word selects an independent recovery record; it does not authorize
            // interpreting the held shared consumer's progress as that record.
            // The whole workspace goes back, not just the pipeline: an apply replaces what is there rather
            // than patching it, so sending the pipeline alone leaves its source and target referring to
            // nothing and is refused outright.
            control.apply(workspace(suffix, sourceUri, targetUri, false));

            String directChain = sourceChain(control, suffix);
            assertThat(directChain).isNotEqualTo(sharedChain);
            assertThat(state.getCollection(MongoStorePort.SRS_META)
                    .find(new Document("_id", directChain)).first()).isNull();
            assertThat(consumer(state, directChain, suffix)).isNull();
            control.lifecycle(suffix, LifecycleVerb.RESUME);
            var resumedIntent = desired.read(suffix).orElseThrow();
            assertThat(resumedIntent.pipelineId()).isEqualTo(suffix);
            assertThat(resumedIntent.targetState()).isEqualTo(PipelineState.RUNNING);
            assertThat(resumedIntent.purgeState()).isFalse();

            var refused = Await.answered("the current pre-execution FAILED diagnostic for the refused mode switch", TIMEOUT,
                    () -> latest.readStored(suffix).filter(value -> value.scope().isEmpty()
                            && value.refusal().filter(owner -> owner.pipelineId().equals(suffix)
                                    && owner.pipelineIncarnationId().equals(scope.pipelineIncarnationId())
                                    && owner.generationFrontier().equals(OptionalLong.of(originalExecution))).isPresent()
                            && value.observation().state() == PipelineState.FAILED
                            && value.observation().failure() != null
                            && value.observation().observedAt() != null
                            && value.observation().observedAt().isAfter(paused.observation().observedAt())
                            && actual.read(suffix).map(checkpoint -> StateJson.parse(checkpoint.stateJson()))
                                    .filter(PipelineState.FAILED::equals).isPresent()
                            && control.state(suffix).filter(PipelineState.FAILED::equals).isPresent()));
            assertThat(refused.scope()).as("a refused replacement has no new execution owner").isEmpty();
            var refusalOwner = refused.refusal().orElseThrow();
            assertThat(refusalOwner.generationFrontier()).isEqualTo(OptionalLong.of(originalExecution));
            assertThat(refusalOwner.pipelineIncarnationId()).isEqualTo(scope.pipelineIncarnationId());
            assertThat(refusalOwner.checkpointEpoch()).isEqualTo(actual.read(suffix).orElseThrow().epoch());
            assertThat(executionGeneration(state, suffix))
                    .as("the retained mode switch is refused before admitting another execution")
                    .isEqualTo(originalExecution);
            assertThat(refused.observation().metrics()).isEmpty();
            assertThat(refused.observation().snapshot()).isEmpty();
            assertThat(refused.observation().positions()).isEmpty();
            assertThat(refused.observation().facts()).isEmpty();
            assertThat(refused.observation().failure().code())
                    .isEqualTo(CaptureError.RECOVERY_PROGRESS_UNPROVEN.code());
            assertThat(refused.observation().failure().params()).containsEntry("pipeline", suffix)
                    .containsEntry("source", SOURCE_ID);
            assertThat(control.failureCode(suffix)).contains(CaptureError.RECOVERY_PROGRESS_UNPROVEN.code());
            String currentLogs = control.logs(suffix);
            assertThat(currentLogs).as("current logs must be read successfully").startsWith("200 ");
            Object parsedLogs = JsonReader.parse(currentLogs.substring(4));
            assertThat(parsedLogs).isInstanceOf(Map.class);
            Map<?, ?> logBody = (Map<?, ?>) parsedLogs;
            assertThat(logBody.get("pipelineId")).isEqualTo(suffix);
            assertThat(logBody.get("lines")).isInstanceOf(List.class);
            assertThat((List<?>) logBody.get("lines")).allSatisfy(line -> {
                assertThat(line).isInstanceOf(Map.class);
                Map<?, ?> logLine = (Map<?, ?>) line;
                assertThat(logLine.get("timestampMillis")).isInstanceOf(Number.class);
                assertThat(logLine.get("level")).isInstanceOf(String.class);
                assertThat(logLine.get("message")).isInstanceOf(String.class);
                assertThat((String) logLine.get("message"))
                        .as("a refused replacement must not borrow the previous execution's log scope")
                        .doesNotContain(CaptureError.RECOVERY_PROGRESS_UNPROVEN.code());
            });
            assertThat(actual.read(suffix).orElseThrow().epoch()).isGreaterThan(pausedActual.epoch());
            assertThat(desired.read(suffix)).contains(resumedIntent);
            assertThat(consumer(state, sharedChain, suffix))
                    .as("the refusal preserves the exact source-qualified consumer recovery document")
                    .isEqualTo(savedRecovery);
            assertThat(state.getCollection(MongoStorePort.SRS_META)
                    .find(new Document("_id", sharedChain)).first()).isNotNull();
            assertThat(state.getCollection(MongoStorePort.SRS_META)
                    .find(new Document("_id", directChain)).first()).isNull();
            assertThat(consumer(state, directChain, suffix)).isNull();
            assertThat(mongo.documents(target, COLLECTION))
                    .as("a refused switch must not resnapshot, replay or clear the held target")
                    .containsExactlyInAnyOrderElementsOf(pausedTarget);
            assertThat(namesOf(mongo, target, DELETED_WHILE_PAUSED)).containsExactly("order-" + DELETED_WHILE_PAUSED);
            assertThat(namesOf(mongo, target, ADDED_WHILE_PAUSED)).isEmpty();
            assertThat(namesOf(mongo, target, CHANGED_BEFORE)).containsExactly(BEFORE_PAUSE);
        }
    }

    /** Reads the same durable execution authority used by the runtime, without assigning an execution. */
    private static long executionGeneration(MongoDatabase state, String pipelineId) {
        Document claim = SystemCollections.WORKLOAD_CLAIMS.on(state)
                .find(new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", pipelineId))
                .first();
        assertThat(claim).as("the pipeline's actual durable execution authority").isNotNull();
        Object generation = claim.get("executionGeneration");
        assertThat(generation instanceof Integer || generation instanceof Long)
                .as("the persisted execution generation is an exact integer").isTrue();
        return ((Number) generation).longValue();
    }

    /** Everything up to and including the start. The pipeline begins with the buffer on. */
    private static ControlPlane start(
            ServerHandle server, String pipelineId, String sourceUri, String targetUri) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin("e2e", "e2e-password");
        control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

        control.apply(workspace(pipelineId, sourceUri, targetUri, true));
        control.discoverSchema(SOURCE_ID, "mongodb",
                Map.of("uri", sourceUri, "database", pipelineId + "_src"));
        control.lifecycle(pipelineId, LifecycleVerb.START);
        return control;
    }

    /**
     * The whole workspace, with the buffering switch set as asked.
     *
     * <p>Built as a whole both times it is sent. An apply is a replacement of what the workspace holds,
     * so the second one has to carry the source and the target it does not change, or they stop existing
     * and the pipeline that refers to them is refused.
     */
    private static Map<String, String> workspace(
            String pipelineId, String sourceUri, String targetUri, boolean srs) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE_ID + ".tap.yml", sourceYaml(sourceUri, pipelineId + "_src"));
        resources.put(TARGET_ID + ".tap.yml", targetYaml(targetUri, pipelineId + "_tgt"));
        resources.put("pipeline.tap.yml", pipelineYaml(pipelineId, srs));
        return resources;
    }

    /** The same pipeline either way; {@code srs} is the only thing that differs between the two runs. */
    private static String pipelineYaml(String pipelineId, boolean srs) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source:
                  - { id: %s, srs: %s }
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [%s], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: %s
                """
                .formatted(pipelineId, SOURCE_ID, srs, COLLECTION, TARGET_ID);
    }

    private static String sourceYaml(String uri, String database) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s", database: %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(SOURCE_ID, uri, database, COLLECTION);
    }

    private static String targetYaml(String uri, String database) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s", database: %s }
                """
                .formatted(TARGET_ID, uri, database);
    }

    /** Written by a driver of the database rather than through any face of the product. */
    private static void seed(MongoClient client, String database) {
        client.getDatabase(database).getCollection(COLLECTION).drop();
        for (int id = 1; id <= SEEDED_ROWS; id++) {
            insert(client, database, id, "order-" + id);
        }
    }

    private static void insert(MongoClient client, String database, int id, String name) {
        client.getDatabase(database).getCollection(COLLECTION)
                .insertOne(new Document("_id", id).append("oid", id).append("name", name));
    }

    private static void rename(MongoClient client, String database, int id, String name) {
        client.getDatabase(database).getCollection(COLLECTION)
                .updateOne(new Document("_id", id), new Document("$set", new Document("name", name)));
    }

    /** Checked, because a delete that matched nothing would leave the target agreeing for no reason. */
    private static void delete(MongoClient client, String database, int id) {
        assertThat(client.getDatabase(database).getCollection(COLLECTION)
                        .deleteOne(new Document("_id", id)).getDeletedCount())
                .as("the source to have deleted document %d", id)
                .isEqualTo(1L);
    }

    private static void awaitState(ControlPlane control, String pipelineId, PipelineState expected) {
        Await.until("%s to reach %s".formatted(pipelineId, expected), TIMEOUT,
                () -> control.state(pipelineId).filter(expected::equals).isPresent(),
                () -> control.state(pipelineId) + ", failure " + control.failureCode(pipelineId));
    }

    private static void awaitCount(MongoEndpoints mongo, EndpointAddress target, String what) {
        Await.until("the target to hold %d documents after %s".formatted(SEEDED_ROWS, what), TIMEOUT,
                () -> mongo.count(target, COLLECTION) == SEEDED_ROWS,
                () -> "%d documents".formatted(mongo.count(target, COLLECTION)));
    }

    private static void awaitName(
            MongoEndpoints mongo, EndpointAddress target, int id, String expected, String what) {
        Await.until("%s, read back from the target".formatted(what), TIMEOUT,
                () -> namesOf(mongo, target, id).contains(expected),
                () -> namesOf(mongo, target, id).toString());
    }

    /** Freezes the actual source/table chain named by the pipeline's ordinary position response. */
    private static String sourceChain(ControlPlane control, String pipelineId) {
        var positions = control.positionRead(pipelineId);
        assertThat(positions.pipelineId()).isEqualTo(pipelineId);
        var selected = positions.chains().stream().filter(chain -> SOURCE_ID.equals(chain.sourceId())
                && chain.tables().contains(COLLECTION)).toList();
        assertThat(selected).hasSize(1);
        assertThat(selected.getFirst().tables()).containsExactly(COLLECTION);
        return selected.getFirst().chainId();
    }

    /** One exact composite key; a different pipeline or source cannot provide this recovery state. */
    private static Document consumer(MongoDatabase state, String chainId, String pipelineId) {
        String consumerId = SrsConsumerId.of(pipelineId, SOURCE_ID).value();
        Document record = state.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS)
                .find(new Document("_id", new Document("chain", chainId).append("pipeline", consumerId))).first();
        if (record != null) {
            assertThat(record.getString("miningChainId")).isEqualTo(chainId);
            assertThat(record.getString("pipelineId")).isEqualTo(consumerId);
            assertThat(record.getString("ownerPipelineId")).isEqualTo(pipelineId);
            assertThat(record.getString("sourceNodeId")).isEqualTo(SOURCE_ID);
        }
        return record;
    }

    /** Waits for actual completion and a settled table cursor; absent progress never means zero. */
    private static boolean settledRecovery(Document record) {
        if (record == null || !(record.get("snapshotCompletedTables") instanceof List<?> completed)
                || !completed.contains(COLLECTION)
                || !(record.get("cdcStartPosition") instanceof String seam) || seam.isBlank()
                || !(record.get("snapshotEpoch") instanceof Number epoch) || epoch.longValue() < 1
                || !(record.get("perTableSeq") instanceof Document read)
                || !(read.get(COLLECTION) instanceof Number readSequence) || readSequence.longValue() < 0
                || !(record.get("sinkAckedByTable") instanceof Document confirmed)
                || !(confirmed.get(COLLECTION) instanceof Document table)
                || !(table.get("sinkAckedEpoch") instanceof Number ackEpoch) || ackEpoch.longValue() < 1
                || !(table.get("sinkAckedSeq") instanceof Number ackSequence)) {
            return false;
        }
        return ackSequence.longValue() == readSequence.longValue();
    }

    private static List<String> namesOf(MongoEndpoints mongo, EndpointAddress target, int id) {
        return mongo.documents(target, COLLECTION).stream()
                .filter(document -> document.get("oid") instanceof Number found && found.intValue() == id)
                .map(document -> String.valueOf(document.get("name")))
                .toList();
    }
}
