package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.ClusterClaimView;
import io.tapstate.control.core.ClusterMemberState;
import io.tapstate.control.core.ClusterRecoveryItemView;
import io.tapstate.control.core.ClusterRecoveryView;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** The unchanged Mongo source and target witness a permanently oversized head yielding to two recoverable runs. */
class ARebuildFailureDoesNotBlockEveryLaterPipelineIT {
    private static final String CONNECTOR = "mongodb";
    private static final List<String> NODES = List.of("node-a", "node-b", "node-c");
    private static final List<Lane> LANES = List.of(
            new Lane("poison_a_head", "poison_source_a", "poison_target_a", "poison_orders_a", 18),
            new Lane("poison_b_follow", "poison_source_b", "poison_target_b", "poison_orders_b", 3),
            new Lane("poison_c_follow", "poison_source_c", "poison_target_c", "poison_orders_c", 3));
    private static final Lane HEAD = LANES.getFirst();
    private static final Set<String> IDS = LANES.stream().map(Lane::pipeline).collect(Collectors.toSet());
    private static final Duration BOUND = Duration.ofMinutes(4);

    @BeforeAll
    static void requireTheRealConnectorAndDocker() {
        DockerGate.require();
        RealConnectorGate.require(CONNECTOR);
    }

    @Test
    void anOversizedHeadExhaustsItsBudgetWithoutOpeningAReaderAndBothFollowersRecover(@TempDir Path directory) throws Exception {
        Map<String, String> sourceUris = new LinkedHashMap<>();
        for (Lane lane : LANES) sourceUris.put(lane.source(), SharedMongo.replicaSetUrl("e2e_poison_" + lane.source()));
        String targetUri = SharedMongo.replicaSetUrl("e2e_poison_target");
        String storeUri = SharedMongo.replicaSetUrl("e2e_poison_state");
        Path writes = directory.resolve("mongo-writes");
        Path reads = directory.resolve("mongo-reads");
        byte[] observed = ObservedMongoConnectorJar.build(ConnectorJars.bytesFor(CONNECTOR), writes, reads);
        try (MongoClient sources = MongoClients.create(sourceUris.values().iterator().next());
                MongoClient targets = MongoClients.create(targetUri); MongoClient states = MongoClients.create(storeUri)) {
            MongoDatabase target = database(targets, targetUri);
            MongoDatabase store = database(states, storeUri);
            for (Lane lane : LANES) database(sources, sourceUris.get(lane.source())).getCollection(lane.table()).insertOne(row(1, "seed"));
            try (PartitionableCluster cluster = PartitionableCluster.start(storeUri, "real-poison-head", NODES,
                    Duration.ofSeconds(30), Map.of("tapstate.execution.cluster-capacity.writers", "8"))) {
                ControlPlane control = cluster.member(NODES.getFirst());
                awaitActive(control, NODES);
                control.registerConnector(CONNECTOR, observed);
                control.apply(resources(sourceUris, targetUri));
                Map<String, ClusterClaimView> originals = new LinkedHashMap<>();
                for (Lane lane : LANES) {
                    control.discoverSchema(lane.source(), CONNECTOR, Map.of("uri", sourceUris.get(lane.source())));
                    control.lifecycle(lane.pipeline(), LifecycleVerb.START);
                    Await.until(lane.pipeline() + " to run over the original three members", BOUND,
                            () -> control.state(lane.pipeline()).filter(PipelineState.RUNNING::equals).isPresent()
                                    && control.membersCarryingPartOf(lane.pipeline()).equals(Set.copyOf(NODES)),
                            () -> control.failure(lane.pipeline()) + "; placement=" + control.membersCarryingPartOf(lane.pipeline()));
                    awaitValue(target, lane, 1, "seed");
                    Await.until("the natural snapshot to finish before confirming CDC for " + lane.pipeline(), BOUND,
                            () -> control.snapshotTables(lane.pipeline()).containsKey(lane.table())
                                    && control.snapshotTables(lane.pipeline()).get(lane.table()).landed(),
                            () -> String.valueOf(control.snapshotTables(lane.pipeline())));
                    var plan = Await.answered("the actual original compiled writer width", BOUND, () -> control.executionPlan(lane.pipeline()));
                    assertThat(plan.members()).containsExactlyInAnyOrderElementsOf(NODES);
                    assertThat(plan.nodeRequested(lane.writers()).computedLocal()).isEqualTo(lane == HEAD ? 6 : 1);
                    Set<String> priorReaderPoints = readerPoints(reads, lane.table(), Set.of("START", "DELIVERY"));
                    assertThat(priorReaderPoints).isNotEmpty();
                    database(sources, sourceUris.get(lane.source())).getCollection(lane.table()).insertOne(row(2, "confirmed-before-loss"));
                    awaitValue(target, lane, 2, "confirmed-before-loss");
                    Await.until("the actual sink checkpoint to confirm the baseline for " + lane.pipeline(), BOUND,
                            () -> control.durablePosition(lane.pipeline(), lane.table())
                                    .filter(point -> !priorReaderPoints.contains(point)
                                            && readerPoints(reads, lane.table(), Set.of("DELIVERY")).contains(point)).isPresent(),
                            () -> "position=" + control.durablePosition(lane.pipeline(), lane.table()));
                    originals.put(lane.pipeline(), claim(control, lane));
                }
                ClusterRecoveryView baseline = control.clusterRecovery();
                assertReadable(baseline);
                assertThat(baseline.items()).isEmpty();
                assertThat(baseline.capacity().configuredLimits().writers()).isEqualTo(8);
                assertThat(baseline.capacity().occupiedByNode().keySet()).containsExactlyInAnyOrderElementsOf(NODES);
                baseline.capacity().occupiedByNode().values().forEach(counts -> assertThat(counts.writers()).isEqualTo(8));
                long headStarts = readerStarts(reads, HEAD.table());
                assertThat(headStarts).isPositive();
                Map<String, Long> writtenBefore = writtenRows(writes);
                String victim = control.captureOwnersOf(HEAD.pipeline()).values().iterator().next();
                List<String> survivors = NODES.stream().filter(node -> !node.equals(victim)).toList();
                ControlPlane survivor = cluster.member(survivors.getFirst());
                Trace trace = new Trace();
                try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> changes = SystemCollections.CLUSTER_RECOVERY_QUEUE.on(store)
                        .watch().maxAwaitTime(100, TimeUnit.MILLISECONDS).cursor()) {
                    trace.drain(changes);
                    cluster.kill(victim);
                    Await.until("all three actual member-loss queue entries", BOUND, () -> {
                        trace.drain(changes);
                        return trace.sequences.size() == LANES.size();
                    }, trace::description);
                    assertThat(trace.sequences.get(HEAD.pipeline())).isLessThan(trace.sequences.get(LANES.get(1).pipeline()));
                    assertThat(trace.sequences.get(HEAD.pipeline())).isLessThan(trace.sequences.get(LANES.get(2).pipeline()));
                    awaitOriginalBounds(store, cluster.clusterId(), originals);
                    for (Lane lane : LANES) database(sources, sourceUris.get(lane.source())).getCollection(lane.table())
                            .insertOne(row(3, "after-loss-sentinel"));
                    Await.until("the permanent head refusal and both later real recoveries", BOUND, () -> {
                        trace.drain(changes);
                        ClusterRecoveryView current = survivor.clusterRecovery();
                        assertReadable(current);
                        if (current.items().size() != LANES.size()) return false;
                        return current.items().stream().allMatch(item -> item.pipelineId().equals(HEAD.pipeline())
                                ? "REBUILD_FAILED".equals(item.persistedStatus()) : "RECOVERED".equals(item.persistedStatus()))
                                && LANES.stream().skip(1).allMatch(lane -> hasValue(target, lane, 3, "after-loss-sentinel"));
                    }, () -> trace.description() + "; reading=" + survivor.clusterRecovery());
                    trace.drain(changes);
                    trace.assertRefusals();
                    ClusterRecoveryView completed = survivor.clusterRecovery();
                    ClusterRecoveryItemView poison = item(completed, HEAD);
                    assertThat(poison.attempt()).isEqualTo(poison.maxAttempts()).isEqualTo(3);
                    assertThat(poison.executionFrontier()).isEqualTo(originals.get(HEAD.pipeline()).executionGeneration());
                    assertThat(poison.successor()).isNull();
                    assertThat(poison.permit()).isNull();
                    assertThat(poison.diagnostic().code()).isEqualTo(LifecycleError.CLUSTER_CAPACITY_REFUSED.code());
                    assertThat(poison.diagnostic().params().get("resource")).isEqualTo("writers");
                    assertThat(((Number) poison.diagnostic().params().get("requested")).longValue()).isEqualTo(9);
                    assertThat(((Number) poison.diagnostic().params().get("limit")).longValue()).isEqualTo(8);
                    assertThat(poison.diagnostic().params().get("node")).isIn(survivors.toArray());
                    assertThat(survivor.executionGenerationOf(HEAD.pipeline()).orElseThrow())
                            .isEqualTo(originals.get(HEAD.pipeline()).executionGeneration());
                    assertThat(readerStarts(reads, HEAD.table())).isEqualTo(headStarts);
                    assertThat(hasValue(target, HEAD, 3, "after-loss-sentinel")).isFalse();
                    for (Lane lane : LANES.subList(1, LANES.size())) {
                        ClusterRecoveryItemView recovered = item(completed, lane);
                        assertThat(recovered.attempt()).isEqualTo(1);
                        assertThat(recovered.executionFrontier()).isEqualTo(originals.get(lane.pipeline()).executionGeneration() + 1);
                        assertThat(recovered.successor().executionNodeIds()).containsExactlyInAnyOrderElementsOf(survivors);
                        assertThat(recovered.successor().nativeInitializedAt()).isNotNull();
                        assertThat(recovered.successor().sourcesAcceptedAt()).isNotNull();
                        assertThat(recovered.successor().requiredSourceIds()).containsExactly(lane.source());
                        assertThat(recovered.successor().acceptedPositions()).isEqualTo(recovered.successor().requestedPositions());
                        awaitValue(target, lane, 3, "after-loss-sentinel");
                    }
                    for (Lane lane : LANES) {
                        survivor.stop(lane.pipeline(), false);
                        Await.until(lane.pipeline() + " to stop its real writers", BOUND,
                                () -> survivor.state(lane.pipeline()).filter(PipelineState.STOPPED::equals).isPresent(),
                                () -> String.valueOf(survivor.state(lane.pipeline())));
                    }
                    Map<String, Long> writtenAfter = writtenRows(writes);
                    assertThat(writtenAfter.getOrDefault(HEAD.table(), 0L) - writtenBefore.getOrDefault(HEAD.table(), 0L)).isZero();
                    for (Lane lane : LANES.subList(1, LANES.size())) {
                        assertThat(writtenAfter.getOrDefault(lane.table(), 0L) - writtenBefore.getOrDefault(lane.table(), 0L))
                                .as("one real physical sentinel write, even when upserts would hide a replay").isEqualTo(1);
                        assertThat(target.getCollection(lane.table()).countDocuments()).isEqualTo(3);
                    }
                }
            }
        }
    }

    private static final class Trace {
        private final Map<String, Document> latest = new LinkedHashMap<>();
        private final Map<String, Long> sequences = new LinkedHashMap<>();
        private final Map<Integer, Document> refusals = new LinkedHashMap<>();

        private void drain(MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor) {
            for (ChangeStreamDocument<Document> change; (change = cursor.tryNext()) != null;) {
                Document row = change.getFullDocument();
                if (row == null || !IDS.contains(row.getString("pipelineId"))) continue;
                assertThat(change.getOperationType()).isIn(OperationType.INSERT, OperationType.REPLACE);
                String pipeline = row.getString("pipelineId");
                long sequence = row.get("enqueueSequence", Number.class).longValue();
                if (change.getOperationType() == OperationType.INSERT) {
                    assertThat(sequences.putIfAbsent(pipeline, sequence)).isNull();
                } else {
                    assertThat(sequences.get(pipeline)).isEqualTo(sequence);
                }
                Document previous = latest.put(pipeline, row);
                if (previous != null) {
                    assertThat(row.get("_id")).isEqualTo(previous.get("_id"));
                    assertThat(row.get("itemRevision", Number.class).longValue())
                            .isGreaterThan(previous.get("itemRevision", Number.class).longValue());
                }
                assertThat(latest.values().stream().filter(value -> value.get("permit") != null).count()).isLessThanOrEqualTo(1);
                if (HEAD.pipeline().equals(pipeline) && row.get("attempt", Number.class).intValue() > 0) {
                    Document diagnostic = row.get("diagnostic", Document.class);
                    assertThat(diagnostic).isNotNull();
                    assertThat(diagnostic.getString("code")).isEqualTo(LifecycleError.CLUSTER_CAPACITY_REFUSED.code());
                    int attempt = row.get("attempt", Number.class).intValue();
                    if (!refusals.containsKey(attempt)) {
                        assertThat(attempt).isEqualTo(refusals.size() + 1);
                        Document earlier = refusals.get(attempt - 1);
                        if (earlier != null) assertThat(row.getDate("updatedAt")).isAfterOrEqualTo(earlier.getDate("nextEligibleAt"));
                        assertThat(row.getString("status")).isEqualTo(attempt < 3 ? "RETRY_BACKOFF" : "REBUILD_FAILED");
                        refusals.put(attempt, row);
                    }
                    assertThat(row.get("successor")).isNull();
                    assertThat(row.get("permit")).isNull();
                }
            }
        }

        private void assertRefusals() {
            assertThat(refusals.keySet()).containsExactly(1, 2, 3);
            assertThat(refusals.get(1).getDate("nextEligibleAt")).isAfter(refusals.get(1).getDate("updatedAt"));
            assertThat(refusals.get(2).getDate("nextEligibleAt")).isAfter(refusals.get(2).getDate("updatedAt"));
            assertThat(refusals.get(3).get("nextEligibleAt")).isNull();
            assertThat(refusals.get(3).getList("executionAliases", Number.class)).hasSize(1);
        }

        private String description() { return "sequences=" + sequences + "; refusal attempts=" + refusals.keySet() + "; latest=" + latest; }
    }

    private record Lane(String pipeline, String source, String target, String table, int writers) {}

    private static void awaitOriginalBounds(MongoDatabase store, String clusterId, Map<String, ClusterClaimView> originals) {
        Await.until("Mongo server time to pass every original pipeline promise", BOUND,
                () -> originals.entrySet().stream().allMatch(entry -> {
                    ClusterClaimView original = entry.getValue();
                    Document exactLiveAuthority = new Document("ownerNodeId", original.ownerNodeId())
                            .append("ownerBootId", original.ownerBootId()).append("claimGeneration", original.claimGeneration())
                            .append("executionGeneration", original.executionGeneration()).append("profileGeneration", original.profileGeneration())
                            .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")));
                    return SystemCollections.WORKLOAD_CLAIMS.on(store).find(new Document("_id",
                            new Document("clusterId", clusterId).append("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                                    .append("resourceId", entry.getKey()))
                            .append("$nor", List.of(exactLiveAuthority)).append("retiredAuthorizationUntil", new Document("$type", "date"))
                            .append("$expr", new Document("$lte", List.of("$retiredAuthorizationUntil", "$$NOW")))).first() != null;
                }), () -> "original authority promises remain active: " + originals.keySet());
    }

    private static ClusterRecoveryItemView item(ClusterRecoveryView reading, Lane lane) {
        return reading.items().stream().filter(value -> value.pipelineId().equals(lane.pipeline())).findFirst().orElseThrow();
    }

    private static ClusterClaimView claim(ControlPlane control, Lane lane) {
        return control.clusterStatus().pipelines().stream().filter(value -> value.pipelineId().equals(lane.pipeline()))
                .findFirst().orElseThrow().controllerClaim();
    }

    private static void assertReadable(ClusterRecoveryView reading) {
        assertThat(reading.queueUnavailable()).isNull();
        assertThat(reading.profileUnavailable()).isNull();
        assertThat(reading.claimUnavailable()).isNull();
        assertThat(reading.quorumReady()).isTrue();
    }

    private static void awaitActive(ControlPlane control, List<String> nodes) {
        Await.until("the exact live initial native cohort", BOUND,
                () -> control.clusterStatus().members().stream().filter(member -> member.state() == ClusterMemberState.ACTIVE
                        && Boolean.TRUE.equals(member.live())).map(member -> member.nodeId()).collect(Collectors.toSet()).equals(Set.copyOf(nodes)),
                () -> String.valueOf(control.clusterStatus().members()));
    }

    private static void awaitValue(MongoDatabase target, Lane lane, int id, String value) {
        Await.until(lane.table() + " row " + id + " to contain " + value, BOUND,
                () -> hasValue(target, lane, id, value), () -> String.valueOf(target.getCollection(lane.table()).find(new Document("_id", id)).first()));
    }

    private static boolean hasValue(MongoDatabase target, Lane lane, int id, String value) {
        return target.getCollection(lane.table()).find(new Document("_id", id).append("value", value)).first() != null;
    }

    private static long readerStarts(Path reads, String table) {
        return lines(reads).stream().map(line -> line.split("\t", 4))
                .filter(fields -> fields.length == 4 && "START".equals(fields[0]) && Set.of(fields[1].split(",")).equals(Set.of(table))).count();
    }

    private static Set<String> readerPoints(Path reads, String table, Set<String> events) {
        return lines(reads).stream().map(line -> line.split("\t", 4))
                .filter(fields -> fields.length == 4 && events.contains(fields[0]) && Set.of(fields[1].split(",")).equals(Set.of(table)))
                .map(fields -> fields[2]).collect(Collectors.toSet());
    }

    private static Map<String, Long> writtenRows(Path writes) {
        Map<String, Long> rows = new LinkedHashMap<>();
        for (String line : lines(writes)) {
            String[] cells = line.split("\t", -1);
            assertThat(cells).hasSize(3);
            rows.merge(cells[0], Long.parseLong(cells[1]), Long::sum);
        }
        return rows;
    }

    private static List<String> lines(Path path) {
        try { return Files.exists(path) ? Files.readAllLines(path) : List.of(); }
        catch (IOException failure) { throw new UncheckedIOException(failure); }
    }

    private static MongoDatabase database(MongoClient client, String uri) { return client.getDatabase(new ConnectionString(uri).getDatabase()); }
    private static Document row(int id, String value) { return new Document("_id", id).append("id", id).append("value", value); }

    private static Map<String, String> resources(Map<String, String> sourceUris, String targetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        for (Lane lane : LANES) {
            resources.put(lane.source() + ".tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: %s
                    connector: mongodb
                    config: { uri: "%s" }
                    mode: cdc
                    tables: [ %s ]
                    """.formatted(lane.source(), sourceUris.get(lane.source()), lane.table()));
            resources.put(lane.target() + ".tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: %s
                    connector: mongodb
                    config: { uri: "%s" }
                    """.formatted(lane.target(), targetUri));
            resources.put(lane.pipeline() + ".tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: %s
                    settings: { read_mode: snapshot_and_cdc }
                    transforms:
                      - { id: rows_through, from: [ %s ], type: filter, expr: "op != 'x'" }
                    serve:
                      from: rows_through
                      sync:
                        - source: %s
                          execution: { parallelism: %d }
                    """.formatted(lane.pipeline(), lane.source(), lane.table(), lane.target(), lane.writers()));
        }
        return resources;
    }
}
