package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoArtifactStore;
import io.tapstate.adapters.mongostore.MongoSrsMetaStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.adapters.mongostore.SourceConfigKeyringStore;
import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.control.core.ClusterPipelineRecoveryView;
import io.tapstate.control.core.ClusterRecoveryItemView;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.spi.store.CaptureResumeWitness;
import io.tapstate.spi.store.ClusterRecoveryPosition;
import io.tapstate.spi.store.ClusterRecoveryStatus;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A genuine source-log rollover is refused through the unchanged Mongo connector and packaged
 * cluster. An independent later queue item still recovers. These local processes do not substitute
 * for an acceptance run on independent hosts.
 */
class AnExpiredSourcePositionNeverFallsForwardSilentlyIT {
    private static final String CONNECTOR = "mongodb";
    private static final String EXPIRED = "a_expired_position";
    private static final String FOLLOWING = "z_following_source";
    private static final String EXPIRED_SOURCE = "expired_source";
    private static final String LIVE_SOURCE = "live_source";
    private static final String EXPIRED_TABLE = "expired_orders";
    private static final String LIVE_TABLE = "live_orders";
    private static final List<String> NODES = List.of("expiry-a", "expiry-b", "expiry-c");
    private static final Duration BOUND = Duration.ofMinutes(3);
    private static final long MAX_CHURN_BYTES = 64L * 1024 * 1024;

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require(CONNECTOR);
    }

    @Test
    void aRealExpiredResumeIsTerminalWithoutBlockingTheFollowingSource(@TempDir Path temporary) throws Exception {
        String storeUri = SharedMongo.replicaSetUrl("e2e_expired_position_state");
        String targetUri = SharedMongo.replicaSetUrl("e2e_expired_position_target");
        String liveUri = SharedMongo.replicaSetUrl("e2e_expired_position_live");
        Path readWitness = temporary.resolve("mongo-reader-events");
        Path writeWitness = temporary.resolve("mongo-writes");
        byte[] connector = ObservedMongoConnectorJar.build(ConnectorJars.bytesFor(CONNECTOR),
                writeWitness, readWitness);
        // Only this source has a deliberately bounded log. The shared state/target/source replica
        // set retains its original configuration for every other specification in the JVM.
        try (MongoDBContainer sourceSet = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--oplogSize", "1",
                        "--oplogMinRetentionHours", "0", "--syncdelay", "1")) {
            sourceSet.start();
            String expiredUri = sourceSet.getReplicaSetUrl("e2e_expired_position_source");
            try (MongoClient source = MongoClients.create(expiredUri);
                    MongoClient live = MongoClients.create(liveUri);
                    MongoClient targets = MongoClients.create(targetUri);
                    MongoClient state = MongoClients.create(storeUri)) {
                MongoDatabase sourceDb = database(source, expiredUri);
                MongoDatabase liveDb = database(live, liveUri);
                MongoDatabase targetDb = database(targets, targetUri);
                MongoDatabase store = database(state, storeUri);
                seed(sourceDb, EXPIRED_TABLE);
                seed(liveDb, LIVE_TABLE);
                try (PartitionableCluster cluster = PartitionableCluster.start(storeUri,
                        "real-expired-position", NODES, Duration.ofSeconds(9))) {
                    NODES.forEach(node -> cluster.awaitExactly(node, NODES.size(), BOUND));
                    ControlPlane first = cluster.member(NODES.getFirst());
                    first.registerConnector(CONNECTOR, connector);
                    first.apply(resources(expiredUri, liveUri, targetUri));
                    first.discoverSchema(EXPIRED_SOURCE, CONNECTOR, Map.of("uri", expiredUri));
                    first.discoverSchema(LIVE_SOURCE, CONNECTOR, Map.of("uri", liveUri));
                    first.lifecycle(EXPIRED, LifecycleVerb.START);
                    first.lifecycle(FOLLOWING, LifecycleVerb.START);
                    awaitState(first, EXPIRED, PipelineState.RUNNING);
                    awaitState(first, FOLLOWING, PipelineState.RUNNING);
                    awaitValue(targetDb, EXPIRED_TABLE, "seed");
                    awaitValue(targetDb, LIVE_TABLE, "seed");
                    change(sourceDb, EXPIRED_TABLE, "confirmed-before-expiry");
                    change(liveDb, LIVE_TABLE, "confirmed-before-restart");
                    awaitValue(targetDb, EXPIRED_TABLE, "confirmed-before-expiry");
                    awaitValue(targetDb, LIVE_TABLE, "confirmed-before-restart");
                    Await.until("the expired source's confirmed native token to be durable", BOUND,
                            () -> {
                                Retained position = confirmed(store);
                                return position != null && delivery(readWitness, position.token()).isPresent();
                            },
                            () -> String.valueOf(consumer(store)) + "; " + lines(readWitness));
                    long originalGeneration = first.executionGenerationOf(EXPIRED).orElseThrow();
                    assertThat(first.captureOwnersOf(EXPIRED)).hasSize(1);
                    String originalCapture = first.captureOwnersOf(EXPIRED).keySet().iterator().next();
                    cluster.killAll();
                    assertThat(NODES).allSatisfy(node -> assertThat(cluster.isAlive(node)).isFalse());
                    Retained retained = confirmed(store);
                    assertThat(retained).isNotNull();
                    int beforeRestart = lines(readWitness).size();
                    Map<String, Long> oldWriteCalls = writerInvocations(writeWitness);
                    assertThat(oldWriteCalls).containsKeys(EXPIRED_TABLE, LIVE_TABLE);
                    String expiredWriterTable = oldWriteCalls.keySet().stream().filter(EXPIRED_TABLE::equals).findFirst().orElseThrow();
                    String followingWriterTable = oldWriteCalls.keySet().stream().filter(LIVE_TABLE::equals).findFirst().orElseThrow();
                    Document originalFacts = originalFacts(store, cluster.clusterId())
                            .append("tableAcknowledgment", new Document("token", retained.token())
                                    .append("epoch", retained.epoch()).append("sequence", retained.sequence()));
                    // No old reader process is alive when this server-time upper bound is taken.
                    BsonTimestamp upper = source.getDatabase("admin").runCommand(new Document("hello", 1))
                            .get("operationTime", BsonTimestamp.class);
                    assertThat(upper).isNotNull();
                    rollPast(source, sourceDb, upper);
                    assertThat(oldest(source).compareTo(upper)).isPositive();
                    Await.until("the old profile authorization horizon to expire on Mongo time", BOUND,
                            () -> store.getCollection(MongoStorePort.CLUSTER_EXECUTION_PROFILES).find(
                                    new Document("_id", cluster.clusterId()).append("$expr",
                                            new Document("$lte", List.of("$authorizationUntil", "$$NOW"))))
                                    .first() != null,
                            () -> String.valueOf(store.getCollection(MongoStorePort.CLUSTER_EXECUTION_PROFILES)
                                    .find(new Document("_id", cluster.clusterId())).first()));
                    // Archive the final old diagnostic truth after every old authorization horizon,
                    // before any replacement member can prepare or open a new physical reader.
                    ArchivedSource archived = archiveDiagnostic(state, store, originalCapture);
                    assertThat(archived.witness().srsEnabled()).isFalse();
                    assertThat(archived.witness().progressKind()).isEqualTo(io.tapstate.spi.store.ConsumerProgressKind.DIRECT_SOURCE);
                    assertThat(archived.witness().snapshotOwed()).as("the original load is settled before testing an expired CDC request").isFalse();
                    ClusterRecoveryPosition request = archived.diagnosticPosition();
                    String requestedToken = request.position().token();
                    String nativeToken = nativeOffset(readWitness, requestedToken).orElseThrow();
                    BsonDocument resumeToken = BsonDocument.parse(nativeToken);
                    assertThat(resumeToken).containsKey("_data");
                    assertThatThrownBy(() -> {
                        try (var cursor = sourceDb.watch().startAfter(resumeToken).cursor()) {
                            cursor.tryNext();
                        }
                    }).isInstanceOf(MongoCommandException.class)
                            .satisfies(failure -> assertThat(((MongoCommandException) failure).getErrorCode()).isEqualTo(286));
                    originalFacts.append("diagnosticArchive", positionDocument(archived.diagnosticPosition()))
                            .append("diagnosticWitness", archived.witness().toString())
                            .append("originalPhysicalStartRequest", archived.physicalStartRequest());
                    cluster.relaunchAll();
                    ControlPlane resumed = cluster.member(NODES.getFirst());
                    Await.until("the real connector to retry the exact expired request", BOUND,
                            () -> after(readWitness, beforeRestart).stream()
                                    .anyMatch(line -> line.startsWith("START\t" + EXPIRED_TABLE + "\t")),
                            () -> after(readWitness, beforeRestart).toString());
                    change(sourceDb, EXPIRED_TABLE, "after-expiry-sentinel");
                    change(liveDb, LIVE_TABLE, "after-restart-sentinel");
                    ClusterRecoveryItemView failed = Await.answered("the expired queue item to be terminal", BOUND,
                            () -> item(resumed, EXPIRED)
                                    .filter(item -> ClusterRecoveryStatus.REBUILD_FAILED.name().equals(item.persistedStatus())));
                    AtomicReference<ClusterPipelineRecoveryView> lastFollowing = new AtomicReference<>();
                    ClusterRecoveryItemView recovered;
                    try {
                        recovered = Await.answered("the following source to recover", BOUND, () -> {
                            // Save the same response this poll already requested, before filtering its status.
                            ClusterPipelineRecoveryView reading = resumed.pipelineRecovery(FOLLOWING);
                            lastFollowing.set(reading);
                            return reading.items().stream()
                                    .max(java.util.Comparator.comparingLong(ClusterRecoveryItemView::enqueueSequence))
                                    .filter(item -> "RECOVERED".equals(item.persistedStatus()));
                        });
                    } catch (AssertionError failure) {
                        try {
                            Path scene = retainFollowingDiagnostics(store, targetDb, cluster.clusterId(), originalFacts,
                                    request, failed, lastFollowing.get(), beforeRestart, readWitness, writeWitness);
                            throw new AssertionError(failure.getMessage() + "; recovery diagnostics retained at " + scene, failure);
                        } catch (IOException | RuntimeException unavailable) {
                            failure.addSuppressed(unavailable);
                            throw failure;
                        }
                    }
                    assertThat(failed.originalExecutionGeneration()).isEqualTo(originalGeneration);
                    assertThat(failed.attempt()).isEqualTo(1);
                    assertThat(failed.diagnostic()).isNotNull();
                    assertThat(failed.diagnostic().code()).isEqualTo(ConnectorError.RESUME_POSITION_REJECTED.code());
                    assertThat(failed.diagnostic().params()).containsEntry("pdkCode", "10003")
                            .containsEntry("requested", requestedToken);
                    assertThat(failed.originalPositions().get(EXPIRED_SOURCE)).satisfies(position -> {
                        ClusterRecoveryPosition diagnostic = archived.diagnosticPosition();
                        assertThat(position.connectorId()).isEqualTo(diagnostic.connectorId());
                        assertThat(position.captureId()).isEqualTo(originalCapture).isEqualTo(diagnostic.captureId());
                        assertThat(position.kind()).isEqualTo(diagnostic.kind().name());
                        assertThat(position.token()).isEqualTo(requestedToken).isEqualTo(diagnostic.position().token());
                        assertThat(position.epoch()).isEqualTo(diagnostic.position().order() == null
                                ? null : diagnostic.position().order().epoch());
                        assertThat(position.sequence()).isEqualTo(diagnostic.position().order() == null
                                ? null : diagnostic.position().order().seq());
                        assertThat(position.provenance()).isEqualTo(diagnostic.provenance());
                        assertThat(position.reference()).isEqualTo(diagnostic.durableStateReference());
                    });
                    ClusterRecoveryItemView.Successor successor = failed.successor();
                    assertThat(successor).isNotNull();
                    assertThat(successor.sourceRequirementsRecorded()).isTrue();
                    assertThat(successor.requiredSourceIds()).containsExactly(EXPIRED_SOURCE);
                    ClusterRecoveryItemView.FailureNote note = successor.failureNote();
                    assertThat(note).isNotNull();
                    assertThat(note.pipelineClaim()).isEqualTo(successor.pipelineClaim());
                    assertThat(note.pipelineClaim().clusterId()).isEqualTo(cluster.clusterId());
                    assertThat(note.pipelineClaim().type()).isEqualTo(io.tapstate.spi.store.WorkloadClaimType.PIPELINE_ACTUATION.name());
                    assertThat(note.pipelineClaim().resourceId()).isEqualTo(EXPIRED);
                    assertThat(note.pipelineClaim().executionGeneration()).isEqualTo(originalGeneration + 1);
                    assertThat(note.pipelineClaim().profileGeneration()).isEqualTo(successor.profile().generation());
                    assertThat(note.stage()).isEqualTo(io.tapstate.spi.store.ClusterRecoveryStore.FailureStage.SOURCE_POSITION_REJECTION.name());
                    assertThat(note.recordedAt()).isNotNull().isAfterOrEqualTo(successor.allocatedAt()).isBeforeOrEqualTo(failed.updatedAt());
                    assertThat(note.diagnostic()).isEqualTo(failed.diagnostic());
                    assertThat(note.diagnostic().reason()).isEqualTo(io.tapstate.spi.store.ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED.name());
                    assertThat(note.diagnostic().code()).isEqualTo(ConnectorError.RESUME_POSITION_REJECTED.code());
                    assertThat(note.diagnostic().disposition()).as("the coded source rejection retains its actionable hint").isNotBlank();
                    assertThat(note.diagnostic().params()).containsEntry("pdkCode", "10003").containsEntry("requested", requestedToken);
                    assertThat(note.diagnostic().positions()).containsOnlyKeys(EXPIRED_SOURCE);
                    ClusterRecoveryItemView.Position expectedRequest = new ClusterRecoveryItemView.Position(request.connectorId(),
                            request.captureId(), request.kind().name(), request.position().order() == null ? null : request.position().order().epoch(),
                            request.position().order() == null ? null : request.position().order().seq(), request.position().token(),
                            request.provenance(), request.durableStateReference());
                    assertThat(note.diagnostic().positions().get(EXPIRED_SOURCE)).isEqualTo(expectedRequest);
                    // A rejected request is proven by the qualified failure fact, without a successful startup receipt.
                    if (!successor.requestedPositions().isEmpty()) {
                        assertThat(successor.requestedPositions()).containsOnlyKeys(EXPIRED_SOURCE);
                        assertThat(successor.requestedPositions().get(EXPIRED_SOURCE)).isEqualTo(expectedRequest);
                    }
                    assertThat(successor.acceptedPositions()).isEmpty();
                    assertThat(successor.sourcesAcceptedAt()).isNull();
                    List<String> observed = after(readWitness, beforeRestart);
                    assertThat(observed.stream().filter(line -> line.startsWith("START\t" + EXPIRED_TABLE + "\t")))
                            .allSatisfy(line -> assertThat(line.split("\t", 4)[2]).isEqualTo(requestedToken));
                    assertThat(observed).anyMatch(line -> line.startsWith("REFUSED\t" + EXPIRED_TABLE + "\t")
                            && line.split("\t", 6)[4].equals("10003"));
                    assertThat(observed).noneMatch(line -> line.startsWith("DELIVERY\t" + EXPIRED_TABLE + "\t"));
                    assertThat(targetDb.getCollection(EXPIRED_TABLE).find(new Document("_id", 1)).first().getString("value"))
                            .isEqualTo("confirmed-before-expiry");
                    assertThat(recovered.enqueueSequence()).isGreaterThan(failed.enqueueSequence());
                    awaitValue(targetDb, LIVE_TABLE, "after-restart-sentinel");
                    awaitState(resumed, FOLLOWING, PipelineState.RUNNING);
                    Map<String, Long> resumedWriteCalls = writerInvocations(writeWitness);
                    assertThat(resumedWriteCalls.getOrDefault(expiredWriterTable, 0L))
                            .as("the expired source causes no target invocation after the old processes stop")
                            .isEqualTo(oldWriteCalls.get(expiredWriterTable));
                    assertThat(resumedWriteCalls.getOrDefault(followingWriterTable, 0L))
                            .as("the following source still invokes the real target writer after recovery")
                            .isGreaterThan(oldWriteCalls.get(followingWriterTable));
                }
            }
        }
    }

    private static ArchivedSource archiveDiagnostic(MongoClient state, MongoDatabase store, String originalCapture) {
        MongoArtifactStore artifacts = new MongoArtifactStore(state, store.getCollection(MongoStorePort.ARTIFACTS),
                new SourceConfigKeyringStore(store).loadExistingCipher());
        PipelineResource pipeline = (PipelineResource) artifacts.get(EXPIRED).orElseThrow();
        SourceRef.Spec reference = (SourceRef.Spec) pipeline.sources().stream()
                .filter(ref -> EXPIRED_SOURCE.equals(ref.id())).findFirst().orElseThrow();
        ReadMode mode = pipeline.settings() == null || pipeline.settings().readMode() == null
                ? ReadMode.SNAPSHOT_AND_CDC : pipeline.settings().readMode();
        Document consumer = consumer(store);
        assertThat(consumer).isNotNull();
        Document prepared = consumer.get("preparedSourceStart", Document.class);
        assertThat(prepared).isNotNull();
        Document initialWitness = prepared.get("witness", Document.class);
        assertThat(initialWitness).isNotNull();
        assertThat(initialWitness.getString("sourceId")).isEqualTo(EXPIRED_SOURCE);
        assertThat(initialWitness.getString("connectorId")).isEqualTo(CONNECTOR);
        assertThat(initialWitness.getString("miningChainId")).isEqualTo(consumer.getString("miningChainId"));
        assertThat(initialWitness.getString("consumerId")).isEqualTo(SrsConsumerId.of(EXPIRED, EXPIRED_SOURCE).value());
        assertThat(initialWitness.getBoolean("srsEnabled")).isEqualTo(reference.srs());
        assertThat(initialWitness.getString("readMode")).isEqualTo(mode.name());
        assertThat(initialWitness.getList("tables", String.class)).containsExactly(EXPIRED_TABLE);
        Document physicalStart = prepared.get("requestedPosition", Document.class);
        assertThat(physicalStart).isNotNull();
        assertThat(physicalStart.getString("captureId")).isEqualTo(originalCapture);
        MongoSrsMetaStore meta = new MongoSrsMetaStore(state, store.getCollection(MongoStorePort.SRS_META),
                store.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS), store.getCollection(MongoStorePort.WORKLOAD_CLAIMS));
        CaptureResumeWitness witness = meta.resumeWitness(EXPIRED_SOURCE, CONNECTOR, consumer.getString("miningChainId"),
                SrsConsumerId.of(EXPIRED, EXPIRED_SOURCE).value(), mode, reference.srs(), List.of(EXPIRED_TABLE));
        ClusterRecoveryPosition diagnostic = witness.requestedPosition(originalCapture).orElseThrow();
        assertThat(diagnostic.kind()).isEqualTo(ClusterRecoveryPosition.Kind.DURABLE_POSITION);
        assertThat(diagnostic.position().token()).isNotBlank();
        return new ArchivedSource(witness, diagnostic, physicalStart);
    }

    private static Document positionDocument(ClusterRecoveryPosition position) {
        Document point = new Document("token", position.position().token());
        if (position.position().order() != null) {
            point.append("epoch", position.position().order().epoch()).append("sequence", position.position().order().seq());
        }
        return new Document("sourceId", position.sourceId()).append("connectorId", position.connectorId())
                .append("captureId", position.captureId()).append("kind", position.kind().name())
                .append("position", point).append("provenance", position.provenance())
                .append("reference", position.durableStateReference());
    }

    private static Document originalFacts(MongoDatabase store, String cluster) {
        MongoDatabase bounded = store.withTimeout(2, TimeUnit.SECONDS);
        Document scope = new Document("clusterId", cluster);
        return new Document("clusterId", cluster).append("pipelines", List.of(EXPIRED, FOLLOWING))
                .append("sourceIds", List.of(EXPIRED_SOURCE, LIVE_SOURCE)).append("tables", List.of(EXPIRED_TABLE, LIVE_TABLE))
                .append("claims", readRows(bounded, MongoStorePort.WORKLOAD_CLAIMS, scope, null))
                .append("profile", readRows(bounded, MongoStorePort.CLUSTER_EXECUTION_PROFILES, new Document("_id", cluster), null))
                .append("consumers", readRows(bounded, MongoStorePort.SRS_CONSUMER_OFFSETS, consumerScope(), null))
                .append("meta", readRows(bounded, MongoStorePort.SRS_META, new Document(), null))
                .append("desired", readRows(bounded, MongoStorePort.PIPELINE_DESIRED, pipelineScope(), null))
                .append("artifacts", readRows(bounded, MongoStorePort.ARTIFACTS, pipelineScope(), artifactFields()));
    }

    private static Path retainFollowingDiagnostics(MongoDatabase store, MongoDatabase target, String cluster,
            Document original, ClusterRecoveryPosition requested, ClusterRecoveryItemView failed, ClusterPipelineRecoveryView following,
            int readerBaseline, Path readerWitness, Path writerWitness) throws IOException {
        Path directory = Path.of(System.getProperty("tapstate.e2e.build-directory", "target"))
                .resolve("failure-scenes").resolve("expired-following").resolve(cluster);
        Files.createDirectories(directory);
        writeScrubbed(directory.resolve("predicate-inputs.json"), new Document("clusterId", cluster)
                .append("expiredPipeline", EXPIRED).append("followingPipeline", FOLLOWING)
                .append("sources", List.of(EXPIRED_SOURCE, LIVE_SOURCE)).append("tables", List.of(EXPIRED_TABLE, LIVE_TABLE))
                .append("readerBaseline", readerBaseline).append("expiredRequested", positionDocument(requested))
                .append("failedReading", String.valueOf(failed)).append("lastUnfilteredFollowingReading", String.valueOf(following)).toJson());
        writeScrubbed(directory.resolve("original-facts.json"), original.toJson());
        retainJournal(directory, "reader-journal.tsv", readerWitness);
        retainJournal(directory, "writer-journal.tsv", writerWitness);
        MongoDatabase bounded = store.withTimeout(2, TimeUnit.SECONDS);
        Document scope = new Document("clusterId", cluster);
        Map<String, Document> captured = new LinkedHashMap<>();
        captured.put("recovery", readRows(bounded, MongoStorePort.CLUSTER_RECOVERY_QUEUE,
                new Document(scope).append("pipelineId", new Document("$in", List.of(EXPIRED, FOLLOWING))), null));
        captured.put("claims", readRows(bounded, MongoStorePort.WORKLOAD_CLAIMS, scope, null));
        captured.put("profile", readRows(bounded, MongoStorePort.CLUSTER_EXECUTION_PROFILES, new Document("_id", cluster), null));
        captured.put("members", readRows(bounded, MongoStorePort.CLUSTER_NODE_REGISTRY, scope, null));
        captured.put("membership", readRows(bounded, MongoStorePort.CLUSTER_MEMBERSHIP, new Document("_id", cluster), null));
        captured.put("capacity", readRows(bounded, MongoStorePort.CLUSTER_CAPACITY_OCCUPANCY,
                new Document(scope).append("pipelineId", new Document("$in", List.of(EXPIRED, FOLLOWING))), null));
        captured.put("consumers", readRows(bounded, MongoStorePort.SRS_CONSUMER_OFFSETS, consumerScope(), null));
        // This isolated fixture contains only the two declared source chains, with their actual stored IDs.
        captured.put("meta", readRows(bounded, MongoStorePort.SRS_META, new Document(), null));
        captured.put("desired", readRows(bounded, MongoStorePort.PIPELINE_DESIRED, pipelineScope(), null));
        captured.put("state", readRows(bounded, MongoStorePort.PIPELINE_STATE, pipelineScope(), null));
        captured.put("observations", readRows(bounded, MongoStorePort.PIPELINE_OBSERVATION, pipelineScope(), null));
        captured.put("artifacts", readRows(bounded, MongoStorePort.ARTIFACTS, pipelineScope(), artifactFields()));
        MongoDatabase boundedTarget = target.withTimeout(2, TimeUnit.SECONDS);
        captured.put("expired-target", readRows(boundedTarget, EXPIRED_TABLE, new Document("_id", 1), null));
        captured.put("following-target", readRows(boundedTarget, LIVE_TABLE, new Document("_id", 1), null));
        for (Map.Entry<String, Document> entry : captured.entrySet()) {
            writeScrubbed(directory.resolve(entry.getKey() + ".json"), entry.getValue().toJson());
        }
        return directory;
    }

    private static Document readRows(MongoDatabase database, String collection, Document scope, Document fields) {
        Document result = new Document("collection", collection).append("scope", scope);
        try {
            List<Document> stages = new ArrayList<>();
            stages.add(new Document("$match", scope));
            if (fields != null) { stages.add(new Document("$project", fields)); }
            stages.add(new Document("$addFields", new Document("diagnosticObservedAt", "$$NOW")));
            result.append("documents", database.getCollection(collection).aggregate(stages)
                    .maxTime(2, TimeUnit.SECONDS).into(new ArrayList<>()));
        } catch (RuntimeException unavailable) {
            result.append("readUnavailable", unavailable.getClass().getSimpleName() + ": " + unavailable.getMessage());
        }
        return result;
    }

    private static Document pipelineScope() {
        return new Document("_id", new Document("$in", List.of(EXPIRED, FOLLOWING)));
    }

    private static Document consumerScope() {
        return new Document("pipelineId", new Document("$in", List.of(
                SrsConsumerId.of(EXPIRED, EXPIRED_SOURCE).value(), SrsConsumerId.of(FOLLOWING, LIVE_SOURCE).value())));
    }

    private static Document artifactFields() {
        // Keep identity and recorded source selection, excluding connector configuration and addresses.
        return new Document("_id", 1).append("kind", 1).append("incarnation", 1).append("contentHash", 1)
                .append("body.source", 1).append("body.settings", 1);
    }

    private static void retainJournal(Path directory, String name, Path witness) throws IOException {
        if (Files.exists(witness)) { writeScrubbed(directory.resolve(name), Files.readString(witness)); }
        else { Files.writeString(directory.resolve(name + "-unavailable.txt"), "The owned SDK journal was absent at failure.\n"); }
    }

    private static void writeScrubbed(Path file, String text) throws IOException {
        Files.writeString(file, text.replaceAll("[a-zA-Z][a-zA-Z0-9+.-]*://[^\\s\"']+", "<address elided>"));
    }

    private static void rollPast(MongoClient source, MongoDatabase database, BsonTimestamp upper) {
        String payload = "x".repeat(64 * 1024);
        AtomicLong bytes = new AtomicLong();
        AtomicLong ids = new AtomicLong();
        Await.until("the owned source oplog to move beyond the old reader's upper bound", BOUND, () -> {
            if (oldest(source).compareTo(upper) > 0) return true;
            if (bytes.get() < MAX_CHURN_BYTES) {
                List<Document> batch = new ArrayList<>();
                for (int count = 0; count < 16; count++) {
                    batch.add(new Document("_id", ids.incrementAndGet()).append("payload", payload));
                }
                database.getCollection("owned_oplog_churn").insertMany(batch);
                bytes.addAndGet((long) payload.length() * batch.size());
            }
            return false;
        }, () -> "oldest=" + oldest(source) + ", upper=" + upper + ", churnBytes=" + bytes.get());
    }

    private static BsonTimestamp oldest(MongoClient source) {
        Document entry = source.getDatabase("local").getCollection("oplog.rs")
                .find().sort(new Document("$natural", 1)).first();
        if (entry == null) throw new AssertionError("the owned replica set has no oplog entry");
        return entry.get("ts", BsonTimestamp.class);
    }

    private static Document consumer(MongoDatabase store) {
        return store.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS)
                .find(new Document("pipelineId", SrsConsumerId.of(EXPIRED, EXPIRED_SOURCE).value())).first();
    }

    private static Retained confirmed(MongoDatabase store) {
        Document consumer = consumer(store);
        if (consumer == null || !(consumer.get("sinkAckedByTable") instanceof Document tables)
                || !(tables.get(EXPIRED_TABLE) instanceof Document point)
                || point.getString("sinkAckedSrcpos") == null
                || !(point.get("sinkAckedEpoch") instanceof Number epoch)
                || !(point.get("sinkAckedSeq") instanceof Number sequence)) return null;
        return new Retained(point.getString("sinkAckedSrcpos"), epoch.longValue(), sequence.longValue());
    }

    private static Map<String, Long> writerInvocations(Path path) {
        assertThat(Files.exists(path)).as("the unchanged real target writer produces its invocation journal").isTrue();
        Map<String, Long> counts = new LinkedHashMap<>();
        for (String line : lines(path)) {
            String[] fields = line.split("\t", -1);
            assertThat(fields).as("the real writer's table, row count and columns").hasSize(3);
            assertThat(fields[0]).isNotBlank();
            assertThat(Long.parseLong(fields[1])).isNotNegative();
            counts.merge(fields[0], 1L, Long::sum);
        }
        return Map.copyOf(counts);
    }

    /** Exact SDK argument bytes locate a native token; independent server refusal proves expiry. */
    private static Optional<String> nativeOffset(Path path, String token) {
        return lines(path).stream().filter(line -> line.startsWith("START\t" + EXPIRED_TABLE + "\t")
                        || line.startsWith("DELIVERY\t" + EXPIRED_TABLE + "\t"))
                .map(line -> line.split("\t", 4)).filter(fields -> fields.length == 4 && fields[2].equals(token))
                .map(fields -> fields[3]).findFirst();
    }

    private static Optional<String> delivery(Path path, String token) {
        return lines(path).stream().filter(line -> line.startsWith("DELIVERY\t" + EXPIRED_TABLE + "\t"))
                .map(line -> line.split("\t", 4)).filter(fields -> fields.length == 4 && fields[2].equals(token))
                .map(fields -> fields[3]).findFirst();
    }

    private static List<String> lines(Path path) {
        try { return Files.exists(path) ? Files.readAllLines(path) : List.of(); }
        catch (IOException failure) { throw new UncheckedIOException(failure); }
    }

    private static List<String> after(Path path, int before) {
        List<String> all = lines(path);
        return all.subList(Math.min(before, all.size()), all.size());
    }

    private static Optional<ClusterRecoveryItemView> item(ControlPlane control, String pipeline) {
        return control.pipelineRecovery(pipeline).items().stream()
                .max(java.util.Comparator.comparingLong(ClusterRecoveryItemView::enqueueSequence));
    }

    private static MongoDatabase database(MongoClient client, String uri) {
        return client.getDatabase(new ConnectionString(uri).getDatabase());
    }

    private static void seed(MongoDatabase database, String table) {
        database.getCollection(table).insertOne(new Document("_id", 1).append("value", "seed"));
    }

    private static void change(MongoDatabase database, String table, String value) {
        database.getCollection(table).updateOne(new Document("_id", 1),
                new Document("$set", new Document("value", value)));
    }

    private static void awaitValue(MongoDatabase database, String table, String value) {
        Await.until(table + " to hold " + value, BOUND,
                () -> database.getCollection(table).find(new Document("_id", 1).append("value", value)).first() != null,
                () -> String.valueOf(database.getCollection(table).find(new Document("_id", 1)).first()));
    }

    private static void awaitState(ControlPlane control, String pipeline, PipelineState expected) {
        Await.until(pipeline + " to reach " + expected, BOUND,
                () -> control.state(pipeline).filter(expected::equals).isPresent(),
                () -> String.valueOf(control.state(pipeline)));
    }

    private static Map<String, String> resources(String expired, String live, String target) {
        Map<String, String> resources = new LinkedHashMap<>();
        add(resources, EXPIRED, EXPIRED_SOURCE, EXPIRED_TABLE, expired, target);
        add(resources, FOLLOWING, LIVE_SOURCE, LIVE_TABLE, live, target);
        return resources;
    }

    private static void add(Map<String, String> resources, String pipeline, String source, String table,
            String uri, String targetUri) {
        String target = source + "_target";
        resources.put(source + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(source, uri, table));
        resources.put(target + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(target, targetUri));
        resources.put(pipeline + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source:
                  - { id: %s, srs: false }
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: rows_through, from: [%s], type: filter, expr: "true" }
                serve:
                  from: rows_through
                  sync:
                    - source: %s
                """.formatted(pipeline, source, table, target));
    }

    private record Retained(String token, long epoch, long sequence) {}

    private record ArchivedSource(CaptureResumeWitness witness, ClusterRecoveryPosition diagnosticPosition,
            Document physicalStartRequest) {}
}
