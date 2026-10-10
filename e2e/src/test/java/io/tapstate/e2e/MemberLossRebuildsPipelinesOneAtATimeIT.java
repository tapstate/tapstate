package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

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
import io.tapstate.control.core.ClusterPipelineView;
import io.tapstate.control.core.ClusterRecoveryItemView;
import io.tapstate.control.core.ClusterRecoveryView;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Three real Mongo CDC runs lose an actual participant and receive one serialized replacement each.
 *
 * <p>Mongo's queue change stream carries every committed insert and replacement post-image, so a
 * permit overlap shorter than a status-poll interval is still visible. The third source has two
 * tables whose sink frontiers are checked independently. An unchanged connector artifact supplies
 * the actual native offset objects and physical writer calls through the existing observer jar.
 */
class MemberLossRebuildsPipelinesOneAtATimeIT {

    private static final String CONNECTOR = "mongodb";
    private static final List<String> NODES = List.of("node-a", "node-b", "node-c");
    private static final List<Lane> LANES = List.of(
            new Lane("serial_recovery_a", "serial_source_a", "serial_target_a", List.of("serial_orders_a")),
            new Lane("serial_recovery_b", "serial_source_b", "serial_target_b", List.of("serial_orders_b")),
            new Lane("serial_recovery_c", "serial_source_c", "serial_target_c",
                    List.of("serial_orders_c", "serial_customers_c")));
    private static final Set<String> PIPELINES = LANES.stream().map(Lane::pipeline).collect(Collectors.toSet());
    private static final Duration BOUND = Duration.ofMinutes(4);

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require(CONNECTOR);
    }

    @Test
    void eachLostRunUsesOneQueueItemOnePermitAndOneNewExecution(@TempDir Path temporary) throws Exception {
        Map<String, String> sourceUris = new LinkedHashMap<>();
        for (Lane lane : LANES) {
            sourceUris.put(lane.source(), SharedMongo.replicaSetUrl("e2e_serial_recovery_" + lane.source()));
        }
        String targetUri = SharedMongo.replicaSetUrl("e2e_serial_recovery_target");
        String storeUri = SharedMongo.replicaSetUrl("e2e_serial_recovery_state");
        Path writes = temporary.resolve("mongo-writes");
        Path reads = temporary.resolve("mongo-reader-events");
        byte[] connector = ObservedMongoConnectorJar.build(ConnectorJars.bytesFor(CONNECTOR), writes, reads);

        try (MongoClient sourceClient = MongoClients.create(sourceUris.values().iterator().next());
                MongoClient targetClient = MongoClients.create(targetUri);
                MongoClient storeClient = MongoClients.create(storeUri)) {
            MongoDatabase target = database(targetClient, targetUri);
            MongoDatabase store = database(storeClient, storeUri);
            for (Lane lane : LANES) {
                MongoDatabase source = database(sourceClient, sourceUris.get(lane.source()));
                for (String table : lane.tables()) {
                    source.getCollection(table).insertOne(row(1, "seed"));
                }
            }

            try (PartitionableCluster cluster = PartitionableCluster.start(storeUri, "real-serial-recovery", NODES)) {
                ControlPlane first = cluster.member(NODES.getFirst());
                awaitActive(first, NODES);
                first.registerConnector(CONNECTOR, connector);
                first.apply(resources(sourceUris, targetUri));
                Map<String, ClusterClaimView> originalClaims = new LinkedHashMap<>();
                Map<String, Map<String, Confirmed>> originalFloors = new LinkedHashMap<>();
                Map<String, String> originalCaptures = new LinkedHashMap<>();
                for (Lane lane : LANES) {
                    first.discoverSchema(lane.source(), CONNECTOR, Map.of("uri", sourceUris.get(lane.source())));
                    first.lifecycle(lane.pipeline(), LifecycleVerb.START);
                    awaitState(first, lane, PipelineState.RUNNING);
                    awaitSnapshot(first, lane, target);
                    awaitParticipants(first, lane, NODES);
                    for (String table : lane.tables()) {
                        database(sourceClient, sourceUris.get(lane.source())).getCollection(table)
                                .insertOne(row(2, "confirmed-before-loss"));
                        awaitValue(target, table, 2, "confirmed-before-loss");
                    }
                    Map<String, Confirmed> floors = awaitFloors(first, store, lane, reads);
                    assertCaptureCovers(store, lane, floors, reads);
                    originalClaims.put(lane.pipeline(), claim(first, lane));
                    originalFloors.put(lane.pipeline(), floors);
                    Map<String, String> owners = first.captureOwnersOf(lane.pipeline());
                    assertThat(owners).hasSize(1);
                    originalCaptures.put(lane.pipeline(), owners.keySet().iterator().next());
                }
                assertThat(first.clusterRecovery().items()).isEmpty();
                assertThat(queueDocuments(store, cluster.clusterId())).isEmpty();

                Lane multipleTables = LANES.getLast();
                Map<String, String> captureOwners = first.captureOwnersOf(multipleTables.pipeline());
                assertThat(captureOwners).as("the two-table source has one physical capture owner").hasSize(1);
                String victim = captureOwners.values().iterator().next();
                for (Lane lane : LANES) {
                    assertThat(first.membersCarryingPartOf(lane.pipeline())).contains(victim);
                    assertThat(originalClaims.get(lane.pipeline()).executionMembers().stream()
                            .map(ClusterClaimView.Member::nodeId)).contains(victim);
                }
                List<String> survivors = NODES.stream().filter(node -> !node.equals(victim)).toList();
                ControlPlane surviving = cluster.member(survivors.getFirst());
                Map<String, Long> writtenBefore = writtenRows(writes);
                int readsBeforeLoss = lines(reads).size();
                QueueTrace trace = new QueueTrace();

                try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> changes =
                        SystemCollections.CLUSTER_RECOVERY_QUEUE.on(store).watch()
                                .maxAwaitTime(100, TimeUnit.MILLISECONDS).cursor()) {
                    // Establish the real stream before the member can disappear; initial queue truth is empty.
                    trace.drain(changes);
                    cluster.kill(victim);
                    assertThat(cluster.isAlive(victim)).isFalse();
                    Await.until("three durable recovery items for the actual lost participant", BOUND,
                            () -> {
                                trace.drain(changes);
                                return trace.inserted.size() == LANES.size();
                            }, () -> trace.description() + "; queue=" + surviving.clusterRecovery());
                    assertThat(queueDocuments(store, cluster.clusterId())).hasSize(LANES.size());

                    // These rows are new after the failure; no lifecycle command asks a pipeline to recover.
                    for (Lane lane : LANES) {
                        for (String table : lane.tables()) {
                            database(sourceClient, sourceUris.get(lane.source())).getCollection(table)
                                    .insertOne(row(3, "after-loss-sentinel"));
                        }
                    }
                    Await.until("all three queue steps and all table sentinels to recover", BOUND, () -> {
                        trace.drain(changes);
                        ClusterRecoveryView queue = surviving.clusterRecovery();
                        assertReadable(queue);
                        return queue.items().size() == LANES.size()
                                && queue.items().stream().allMatch(item -> "RECOVERED".equals(item.persistedStatus()))
                                && LANES.stream().allMatch(lane -> lane.tables().stream()
                                        .allMatch(table -> hasValue(target, table, 3, "after-loss-sentinel")));
                    }, () -> trace.description() + "; queue=" + surviving.clusterRecovery());
                    trace.drain(changes);
                    trace.assertComplete();
                    awaitActive(surviving, survivors);

                    ClusterRecoveryView recovered = surviving.clusterRecovery();
                    assertReadable(recovered);
                    assertThat(recovered.items()).hasSize(LANES.size());
                    assertThat(recovered.items().stream().map(ClusterRecoveryItemView::enqueueSequence)).doesNotHaveDuplicates();
                    for (Lane lane : LANES) {
                        awaitState(surviving, lane, PipelineState.RUNNING);
                        awaitParticipants(surviving, lane, survivors);
                        ClusterClaimView current = claim(surviving, lane);
                        ClusterClaimView original = originalClaims.get(lane.pipeline());
                        assertThat(current.executionGeneration()).isEqualTo(original.executionGeneration() + 1);
                        ClusterRecoveryItemView item = recovered.items().stream()
                                .filter(value -> value.pipelineId().equals(lane.pipeline())).findFirst().orElseThrow();
                        assertRecovered(item, original, current, lane, reads, originalFloors.get(lane.pipeline()),
                                originalCaptures.get(lane.pipeline()));
                        if (lane.equals(multipleTables)) {
                            String requested = item.successor().requestedPositions().get(lane.source()).token();
                            assertThat(observed(reads, lane, "START", requested, readsBeforeLoss))
                                    .as("the lost physical reader restarts the unchanged SDK at its frozen native token").isTrue();
                        }
                        assertPreparedTables(trace.latest.get(lane.pipeline()), lane);
                        Map<String, Confirmed> after = awaitFloors(surviving, store, lane, reads);
                        assertCaptureCovers(store, lane, after, reads);
                        for (String table : lane.tables()) {
                            Confirmed old = originalFloors.get(lane.pipeline()).get(table);
                            Confirmed next = after.get(table);
                            assertThat(next.chain()).isEqualTo(old.chain());
                            assertThat(next.token()).isNotEqualTo(old.token());
                            assertThat(target.getCollection(table).countDocuments()).isEqualTo(3);
                        }
                        assertSinkFence(store, cluster.clusterId(), lane, current);
                        Await.until("the replacement to publish only its new table sentinels", BOUND,
                                () -> surviving.recordsOut(lane.pipeline()).filter(rows -> rows == lane.tables().size()).isPresent(),
                                () -> "confirmed rows=" + surviving.recordsOut(lane.pipeline()));
                        assertThat(executions(surviving, lane)).hasSize(1);
                        assertThat(SystemCollections.CLUSTER_CAPACITY_OCCUPANCY.on(store)
                                .find(new Document("clusterId", cluster.clusterId()).append("pipelineId", lane.pipeline()))
                                .into(new ArrayList<>())).singleElement().satisfies(occupancy -> {
                                    assertThat(occupancy.getString("nativeJobId")).isEqualTo(item.successor().nativeJobId());
                                    assertThat(occupancy.get("executionGeneration", Number.class).longValue())
                                            .isEqualTo(current.executionGeneration());
                                });
                    }

                    // Closing the real writers makes their journal include all attempts, not only the first arrival.
                    for (Lane lane : LANES) {
                        surviving.stop(lane.pipeline(), false);
                        awaitState(surviving, lane, PipelineState.STOPPED);
                    }
                    trace.drain(changes);
                    trace.assertComplete();
                    Map<String, Long> writtenAfter = writtenRows(writes);
                    for (Lane lane : LANES) {
                        for (String table : lane.tables()) {
                            assertThat(writtenAfter.getOrDefault(table, 0L) - writtenBefore.getOrDefault(table, 0L))
                                    .as("one physical sentinel write for %s; an upsert cannot hide a replay", table).isEqualTo(1);
                        }
                    }
                }
            }
        }
    }

    /** Reconstructs committed queue truth from every original post-image, without update-lookups or polling gaps. */
    private static final class QueueTrace {
        private final Map<String, Document> latest = new LinkedHashMap<>();
        private final Map<String, Long> inserted = new LinkedHashMap<>();
        private final Map<String, Set<String>> reservations = new LinkedHashMap<>();
        private int peakPermits;
        private int peakRebuilding;

        private void drain(MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor) {
            for (ChangeStreamDocument<Document> event; (event = cursor.tryNext()) != null;) {
                Document document = event.getFullDocument();
                String pipeline = document == null ? null : document.getString("pipelineId");
                if (pipeline == null && event.getDocumentKey() != null
                        && event.getDocumentKey().get("_id") instanceof org.bson.BsonDocument key
                        && key.containsKey("pipelineId")) {
                    pipeline = key.getString("pipelineId").getValue();
                }
                if (!PIPELINES.contains(pipeline)) continue;
                assertThat(event.getOperationType()).isIn(OperationType.INSERT, OperationType.REPLACE);
                assertThat(document).as("a queue mutation carries its original full post-image").isNotNull();
                long sequence = document.get("enqueueSequence", Number.class).longValue();
                if (event.getOperationType() == OperationType.INSERT) {
                    assertThat(inserted.putIfAbsent(pipeline, sequence)).as("one durable queue insertion for %s", pipeline).isNull();
                } else {
                    assertThat(inserted).containsKey(pipeline);
                    assertThat(sequence).isEqualTo(inserted.get(pipeline));
                }
                Document previous = latest.put(pipeline, document);
                if (previous != null) {
                    assertThat(document.get("itemRevision", Number.class).longValue())
                            .isGreaterThan(previous.get("itemRevision", Number.class).longValue());
                }
                Document permit = document.get("permit", Document.class);
                if (permit != null) {
                    reservations.computeIfAbsent(pipeline, ignored -> new LinkedHashSet<>()).add(permit.getString("reservationId"));
                }
                long permits = latest.values().stream().filter(row -> row.get("permit") != null).count();
                long rebuilding = latest.values().stream().filter(row -> "REBUILDING".equals(row.getString("status"))).count();
                peakPermits = Math.max(peakPermits, Math.toIntExact(permits));
                peakRebuilding = Math.max(peakRebuilding, Math.toIntExact(rebuilding));
                assertThat(permits).as("committed recovery reservation peak").isLessThanOrEqualTo(1);
                assertThat(rebuilding).as("committed REBUILDING peak").isLessThanOrEqualTo(1);
                assertThat(permits).isEqualTo(rebuilding);
            }
        }

        private void assertComplete() {
            assertThat(inserted.keySet()).containsExactlyInAnyOrderElementsOf(PIPELINES);
            assertThat(latest.keySet()).containsExactlyInAnyOrderElementsOf(PIPELINES);
            assertThat(peakPermits).isEqualTo(1);
            assertThat(peakRebuilding).isEqualTo(1);
            latest.forEach((pipeline, document) -> {
                assertThat(document.getString("status")).isEqualTo("RECOVERED");
                assertThat(document.get("attempt", Number.class).intValue()).isEqualTo(1);
                assertThat(document.get("permit")).isNull();
                assertThat(reservations.get(pipeline)).hasSize(1);
                long original = document.get("event", Document.class).get("originalExecutionGeneration", Number.class).longValue();
                assertThat(document.getList("executionAliases", Number.class).stream().map(Number::longValue))
                        .containsExactlyInAnyOrder(original, original + 1);
            });
        }

        private String description() {
            return "inserted=" + inserted + ", permit peak=" + peakPermits + ", rebuilding peak=" + peakRebuilding
                    + ", steps=" + latest.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                            entry -> entry.getValue().getString("status")));
        }
    }

    private record Lane(String pipeline, String source, String target, List<String> tables) {}
    private record Confirmed(String chain, String token, SourceOrder order) {}

    private static Map<String, Confirmed> awaitFloors(ControlPlane control, MongoDatabase store, Lane lane, Path reads) {
        return Await.answered("each table's confirmed native SDK token for " + lane.pipeline(), BOUND, () -> {
            Map<String, Confirmed> floors = new LinkedHashMap<>();
            for (String table : lane.tables()) {
                Confirmed point = confirmed(store, lane, table);
                if (point == null || !delivered(reads, lane, point.token())
                        || !control.durablePosition(lane.pipeline(), table).filter(point.token()::equals).isPresent()) {
                    return Optional.empty();
                }
                floors.put(table, point);
            }
            return Optional.of(floors);
        });
    }

    private static Confirmed confirmed(MongoDatabase store, Lane lane, String table) {
        Document consumer = consumer(store, lane);
        if (consumer == null || !(consumer.get("sinkAckedByTable") instanceof Document tables)
                || !(tables.get(table) instanceof Document point) || point.getString("sinkAckedSrcpos") == null
                || !(point.get("sinkAckedEpoch") instanceof Number epoch) || !(point.get("sinkAckedSeq") instanceof Number sequence)) {
            return null;
        }
        return new Confirmed(consumer.getString("miningChainId"), point.getString("sinkAckedSrcpos"),
                new SourceOrder(epoch.longValue(), sequence.longValue()));
    }

    private static void assertCaptureCovers(MongoDatabase store, Lane lane, Map<String, Confirmed> floors, Path reads) {
        String chain = floors.values().iterator().next().chain();
        Document captured = Await.answered("the durable source checkpoint covering every confirmed table", BOUND, () -> {
            Document root = SystemCollections.SRS_META.on(store).find(new Document("_id", chain)).first();
            if (root == null || !Boolean.TRUE.equals(root.get("sourceReadDurable"))
                    || !(root.get("sourceReadEpoch") instanceof Number epoch) || !(root.get("sourceReadSeq") instanceof Number sequence)
                    || !(root.get("sourceReadOffset") instanceof String token) || !delivered(reads, lane, token)) {
                return Optional.empty();
            }
            SourceOrder order = new SourceOrder(epoch.longValue(), sequence.longValue());
            return floors.values().stream().allMatch(floor -> chain.equals(floor.chain())
                    && order.epoch() == floor.order().epoch() && order.seq() >= floor.order().seq())
                    ? Optional.of(root) : Optional.empty();
        });
        assertThat(captured.getString("sourceReadOffset")).isNotBlank();
    }

    private static void assertRecovered(ClusterRecoveryItemView item, ClusterClaimView original,
            ClusterClaimView current, Lane lane, Path reads, Map<String, Confirmed> floors, String captureId) {
        assertThat(item.cause()).isEqualTo("MEMBER_LOSS");
        assertThat(item.persistedStatus()).isEqualTo("RECOVERED");
        assertThat(item.attempt()).isEqualTo(1);
        assertThat(item.originalExecutionGeneration()).isEqualTo(original.executionGeneration());
        assertThat(item.executionFrontier()).isEqualTo(original.executionGeneration() + 1);
        assertThat(item.successor()).isNotNull();
        assertThat(item.successor().sourceRequirementsRecorded()).isTrue();
        assertThat(item.successor().requiredSourceIds()).containsExactly(lane.source());
        assertThat(item.successor().pipelineClaim().executionGeneration()).isEqualTo(current.executionGeneration());
        assertThat(item.successor().pipelineClaim().claimGeneration()).isEqualTo(current.claimGeneration());
        assertThat(item.successor().pipelineClaim().ownerNodeId()).isEqualTo(current.ownerNodeId());
        assertThat(item.successor().pipelineClaim().ownerBootId()).isEqualTo(current.ownerBootId());
        assertThat(item.successor().executionNodeIds()).containsExactlyInAnyOrderElementsOf(
                current.executionMembers().stream().map(ClusterClaimView.Member::nodeId).toList());
        assertThat(item.successor().profile()).isEqualTo(item.targetProfile());
        assertThat(item.successor().nativeJobId()).isNotBlank();
        assertThat(item.successor().submittedAt()).isNotNull();
        assertThat(item.successor().nativeInitializedAt()).isNotNull();
        assertThat(item.successor().sourcesAcceptedAt()).isNotNull();
        assertThat(item.successor().executionCompleted()).isFalse();
        assertThat(item.successor().failureNote()).isNull();
        assertThat(item.originalPositions()).containsOnlyKeys(lane.source());
        assertThat(item.successor().requestedPositions()).containsOnlyKeys(lane.source());
        assertThat(item.successor().acceptedPositions()).containsOnlyKeys(lane.source());
        assertThat(item.successor().acceptedPositions()).isEqualTo(item.successor().requestedPositions());
        for (ClusterRecoveryItemView.Position point : List.of(item.originalPositions().get(lane.source()),
                item.successor().requestedPositions().get(lane.source()), item.successor().acceptedPositions().get(lane.source()))) {
            assertThat(point.connectorId()).isEqualTo(CONNECTOR);
            assertThat(point.captureId()).isEqualTo(captureId);
            assertThat(point.kind()).isEqualTo("DURABLE_POSITION");
            assertThat(point.token()).isNotBlank();
            assertThat(point.epoch()).isNotNull();
            assertThat(point.sequence()).isNotNull();
            assertThat(delivered(reads, lane, point.token()))
                    .as("the archived or accepted token is an actual completed SDK delivery, never a fabricated point").isTrue();
            SourceOrder order = new SourceOrder(point.epoch(), point.sequence());
            floors.forEach((table, floor) -> {
                // A higher ring epoch cannot establish source-log continuity. The exact native resume
                // and qualified receipt carry that proof when the physical reader was replaced.
                if (order.epoch() == floor.order().epoch()) {
                    assertThat(order.seq()).as("source checkpoint covers confirmed table %s in its actual epoch", table)
                            .isGreaterThanOrEqualTo(floor.order().seq());
                }
            });
        }
    }

    private static void assertPreparedTables(Document queue, Lane lane) {
        Document receipt = queue.get("successor", Document.class).get("startupReceipt", Document.class);
        assertThat(receipt).isNotNull();
        assertThat(receipt.getList("preparedWitnesses", Document.class)).singleElement().satisfies(witness -> {
            assertThat(witness.getString("sourceId")).isEqualTo(lane.source());
            assertThat(witness.getString("consumerId")).isEqualTo(SrsConsumerId.of(lane.pipeline(), lane.source()).value());
            assertThat(witness.getList("tables", String.class)).containsExactlyInAnyOrderElementsOf(lane.tables());
            assertThat(witness.getList("snapshotCompletedTables", String.class)).containsAll(lane.tables());
            assertThat(witness.get("sinkAckedByTable", Document.class).keySet()).containsAll(lane.tables());
        });
    }

    private static void assertSinkFence(MongoDatabase store, String clusterId, Lane lane, ClusterClaimView current) {
        Document fence = consumer(store, lane).get("sinkAckFence", Document.class);
        assertThat(fence).isNotNull();
        assertThat(fence).containsEntry("clusterId", clusterId)
                .containsEntry("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                .containsEntry("resourceId", lane.pipeline())
                .containsEntry("ownerNodeId", current.ownerNodeId()).containsEntry("ownerBootId", current.ownerBootId());
        assertThat(fence.get("claimGeneration", Number.class).longValue()).isEqualTo(current.claimGeneration());
        assertThat(fence.get("executionGeneration", Number.class).longValue()).isEqualTo(current.executionGeneration());
    }

    private static Document consumer(MongoDatabase store, Lane lane) {
        return SystemCollections.SRS_CONSUMER_OFFSETS.on(store)
                .find(new Document("pipelineId", SrsConsumerId.of(lane.pipeline(), lane.source()).value())).first();
    }

    private static boolean delivered(Path reads, Lane lane, String token) {
        return observed(reads, lane, "DELIVERY", token, 0);
    }

    private static boolean observed(Path reads, Lane lane, String event, String token, int from) {
        return lines(reads).stream().skip(from).filter(line -> line.startsWith(event + "\t")).map(line -> line.split("\t", 4))
                .anyMatch(fields -> fields.length == 4 && Set.of(fields[1].split(",")).equals(Set.copyOf(lane.tables()))
                        && fields[2].equals(token));
    }

    private static Map<String, Long> writtenRows(Path writes) {
        Map<String, Long> rows = new LinkedHashMap<>();
        for (String line : lines(writes)) {
            String[] fields = line.split("\t", -1);
            assertThat(fields).as("the unchanged real writer's invocation journal").hasSize(3);
            rows.merge(fields[0], Long.parseLong(fields[1]), Long::sum);
        }
        return rows;
    }

    private static List<String> lines(Path path) {
        try { return Files.exists(path) ? Files.readAllLines(path) : List.of(); }
        catch (IOException failure) { throw new UncheckedIOException("reading the real connector journal", failure); }
    }

    private static void assertReadable(ClusterRecoveryView recovery) {
        assertThat(recovery.queueUnavailable()).isNull();
        assertThat(recovery.claimUnavailable()).isNull();
        assertThat(recovery.profileUnavailable()).isNull();
        assertThat(recovery.quorumReady()).isTrue();
    }

    private static void awaitActive(ControlPlane control, List<String> nodes) {
        Await.until("the exact live active cohort " + nodes, BOUND,
                () -> control.clusterStatus().members().stream().filter(member -> member.state() == ClusterMemberState.ACTIVE
                        && Boolean.TRUE.equals(member.live())).map(member -> member.nodeId()).collect(Collectors.toSet())
                        .equals(Set.copyOf(nodes)),
                () -> "members=" + control.clusterStatus().members());
    }

    private static void awaitParticipants(ControlPlane control, Lane lane, List<String> nodes) {
        Await.until("actual working processors for every planned member of " + lane.pipeline(), BOUND,
                () -> control.membersCarryingPartOf(lane.pipeline()).equals(Set.copyOf(nodes))
                        && executions(control, lane).size() == 1,
                () -> "carrying=" + control.membersCarryingPartOf(lane.pipeline()) + ", claim=" + claim(control, lane));
    }

    private static Set<String> executions(ControlPlane control, Lane lane) {
        return control.placedVertices(lane.pipeline()).stream().map(ControlPlane.PlacedVertex::executionId)
                .filter(java.util.Objects::nonNull).collect(Collectors.toCollection(TreeSet::new));
    }

    private static ClusterClaimView claim(ControlPlane control, Lane lane) {
        List<ClusterPipelineView> found = control.clusterStatus().pipelines().stream()
                .filter(pipeline -> pipeline.pipelineId().equals(lane.pipeline())).toList();
        assertThat(found).hasSize(1);
        assertThat(found.getFirst().controllerClaim()).isNotNull();
        assertThat(found.getFirst().controllerClaim().leased()).isTrue();
        return found.getFirst().controllerClaim();
    }

    private static void awaitState(ControlPlane control, Lane lane, PipelineState state) {
        Await.until(lane.pipeline() + " to reach " + state, BOUND,
                () -> control.state(lane.pipeline()).filter(state::equals).isPresent(),
                () -> "state=" + control.state(lane.pipeline()) + ", failure=" + control.failure(lane.pipeline()));
    }

    private static void awaitSnapshot(ControlPlane control, Lane lane, MongoDatabase target) {
        Await.until("every natural load to settle for " + lane.pipeline(), BOUND,
                () -> lane.tables().stream().allMatch(table -> control.snapshotTables(lane.pipeline()).containsKey(table)
                        && control.snapshotTables(lane.pipeline()).get(table).landed() && hasValue(target, table, 1, "seed")),
                () -> "snapshot=" + control.snapshotTables(lane.pipeline()));
    }

    private static void awaitValue(MongoDatabase target, String table, int id, String value) {
        Await.until(table + " row " + id + " to hold " + value, BOUND,
                () -> hasValue(target, table, id, value), () -> String.valueOf(target.getCollection(table).find(new Document("_id", id)).first()));
    }

    private static boolean hasValue(MongoDatabase target, String table, int id, String value) {
        return target.getCollection(table).find(new Document("_id", id).append("value", value)).first() != null;
    }

    private static List<Document> queueDocuments(MongoDatabase store, String clusterId) {
        return SystemCollections.CLUSTER_RECOVERY_QUEUE.on(store).find(new Document("clusterId", clusterId)).into(new ArrayList<>());
    }

    private static MongoDatabase database(MongoClient client, String uri) {
        return client.getDatabase(new ConnectionString(uri).getDatabase());
    }

    private static Document row(int id, String value) {
        return new Document("_id", id).append("id", id).append("value", value);
    }

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
                    """.formatted(lane.source(), sourceUris.get(lane.source()), String.join(", ", lane.tables())));
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
                          execution: { parallelism: 3 }
                    """.formatted(lane.pipeline(), lane.source(), String.join(", ", lane.tables()), lane.target()));
        }
        return resources;
    }
}
