package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoSrsMetaStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.control.core.ClusterClaimView;
import io.tapstate.control.core.ClusterRecoveryItemView;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.ReadMode;
import io.tapstate.spi.store.ClusterRecoveryPosition;
import io.tapstate.spi.store.ClusterRecoveryStatus;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** A stopped cluster restores three different unfinished/continuous workloads from its durable records. */
class AFullClusterRestartRebuildsDesiredPipelinesFromMongoTruthIT {
    private static final Duration BOUND = Duration.ofMinutes(6);
    private static final Duration NODE_SESSION = Duration.ofSeconds(9);
    private static final List<String> NODES = List.of("full-restart-a", "full-restart-b", "full-restart-c");
    private static final int LARGE_ROWS = 100_000;
    private static final int HELD_ROW = 100;
    private static final String SNAPSHOT = "a_full_restart_snapshot";
    private static final String CDC = "b_full_restart_cdc";
    private static final String NEST = "c_full_restart_nest";
    private static final String LARGE_TABLE = "large_orders";
    private static final String CDC_TABLE = "single_orders";
    private static final String PARENTS = "orders";
    private static final String CHILDREN = "order_items";
    private static final Source SNAPSHOT_SOURCE = new Source("full_snapshot_source", "mongodb", LARGE_TABLE, false);
    private static final Source CDC_SOURCE = new Source("full_cdc_source", "mongodb", CDC_TABLE, false);
    private static final Source PARENT_SOURCE = new Source("full_parent_source", "mysql", PARENTS, true);
    private static final Source CHILD_SOURCE = new Source("full_child_source", "mysql", CHILDREN, true);
    private static final Map<String, List<Source>> SOURCES = Map.of(
            SNAPSHOT, List.of(SNAPSHOT_SOURCE), CDC, List.of(CDC_SOURCE), NEST, List.of(PARENT_SOURCE, CHILD_SOURCE));
    private static final List<String> PIPELINES = List.of(SNAPSHOT, CDC, NEST);

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mongodb", "mysql");
    }

    @Test
    void compatibleBootsRestoreMongoIntentWithoutAnyNewLifecycleCommand(@TempDir Path temporary) throws Exception {
        String storeUri = SharedMongo.replicaSetUrl("e2e_full_restart_state");
        String snapshotUri = SharedMongo.replicaSetUrl("e2e_full_restart_snapshot_source");
        String cdcUri = SharedMongo.replicaSetUrl("e2e_full_restart_cdc_source");
        String targetUri = SharedMongo.replicaSetUrl("e2e_full_restart_target");
        Map<String, Object> mysql = SharedMySql.settings("e2e_full_restart_nest_source");
        Path fileTarget = Files.createDirectories(temporary.resolve("snapshot-target"));
        Path reads = temporary.resolve("actual-mongo-readers");
        Holds holds = new Holds(temporary.resolve("snapshot-holds"));
        holds.after(LARGE_TABLE, HELD_ROW);
        try (MongoClient client = MongoClients.create(storeUri);
                MongoClient snapshots = MongoClients.create(snapshotUri);
                MongoClient singles = MongoClients.create(cdcUri);
                MongoClient targets = MongoClients.create(targetUri);
                Connection sql = SharedMySql.connect(mysql);
                FileEndpoints files = new FileEndpoints()) {
            MongoDatabase store = database(client, storeUri);
            MongoDatabase snapshot = database(snapshots, snapshotUri);
            MongoDatabase single = database(singles, cdcUri);
            MongoDatabase target = database(targets, targetUri);
            seedRows(snapshot, LARGE_TABLE, LARGE_ROWS);
            seedRows(single, CDC_TABLE, 3);
            seedNest(sql);
            MongoSrsMetaStore meta = new MongoSrsMetaStore(client, store.getCollection(MongoStorePort.SRS_META),
                    store.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS), store.getCollection(MongoStorePort.WORKLOAD_CLAIMS));
            try (PartitionableCluster cluster = PartitionableCluster.start(storeUri, "full-mongo-truth", NODES, NODE_SESSION)) {
                NODES.forEach(node -> cluster.awaitExactly(node, NODES.size(), BOUND));
                ControlPlane initial = cluster.member(NODES.getFirst());
                initial.registerConnector("mongodb", ObservedMongoConnectorJar.build(ConnectorJars.bytesFor("mongodb"),
                        temporary.resolve("mongo-writes"), reads));
                initial.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                initial.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                        Files.readAllBytes(E2eConnectorJar.buildInto(temporary)));
                initial.apply(resources(snapshotUri, cdcUri, targetUri, fileTarget, holds, mysql));
                initial.discoverSchema(SNAPSHOT_SOURCE.id(), "mongodb", Map.of("uri", snapshotUri));
                initial.discoverSchema(CDC_SOURCE.id(), "mongodb", Map.of("uri", cdcUri));
                initial.discoverSchema(PARENT_SOURCE.id(), "mysql", mysql);
                initial.discoverSchema(CHILD_SOURCE.id(), "mysql", mysql);
                // These are the only lifecycle requests. The new boots restore the retained RUNNING intent.
                initial.lifecycle(CDC, LifecycleVerb.START);
                initial.lifecycle(NEST, LifecycleVerb.START);
                awaitState(initial, CDC);
                awaitState(initial, NEST);
                awaitMongo(target, CDC_TABLE, Map.of(1L, 1L, 2L, 2L, 3L, 3L));
                awaitNest(target, expectedNest("parent-1", "child-11", "child-12", "parent-2"));
                Await.until("both Nest table loads to land", BOUND,
                        () -> landed(initial, NEST, PARENTS) && landed(initial, NEST, CHILDREN),
                        () -> String.valueOf(initial.snapshotTables(NEST)));
                update(single, CDC_TABLE, 1, -1);
                execute(sql, "UPDATE order_items SET sku='confirmed-child-11' WHERE id=11");
                awaitMongo(target, CDC_TABLE, Map.of(1L, -1L, 2L, 2L, 3L, 3L));
                awaitNest(target, expectedNest("parent-1", "confirmed-child-11", "child-12", "parent-2"));
                initial.lifecycle(SNAPSHOT, LifecycleVerb.START);
                awaitState(initial, SNAPSHOT);
                Await.answered("an actual file writer holding a written snapshot batch before acknowledgement", BOUND,
                        () -> holds.holderAfter(LARGE_TABLE, HELD_ROW));
                Await.until("a genuine incomplete large source snapshot with its own durable tail seam", BOUND, () -> {
                    Document row = consumer(store, SNAPSHOT, SNAPSHOT_SOURCE);
                    long count = files.countRows(EndpointAddress.uri(fileTarget.toString()), LARGE_TABLE);
                    return count > 0 && count < LARGE_ROWS && !landed(initial, SNAPSHOT, LARGE_TABLE)
                            && row != null && row.getString("cdcStartPosition") != null
                            && row.get("snapshotEpoch") instanceof Number epoch && epoch.longValue() > 0
                            && !completed(row, LARGE_TABLE);
                }, () -> "targetRows=" + files.countRows(EndpointAddress.uri(fileTarget.toString()), LARGE_TABLE)
                        + ", snapshot=" + initial.snapshotTables(SNAPSHOT) + ", consumer="
                        + consumer(store, SNAPSHOT, SNAPSHOT_SOURCE));
                assertThat(csv(fileTarget, LARGE_TABLE)).containsEntry(11L, 11L);
                Map<String, ClusterClaimView> previous = new LinkedHashMap<>();
                for (String pipeline : PIPELINES) {
                    awaitCohort(initial, pipeline);
                    assertThat(initial.state(pipeline)).as("each original run is healthy before the full stop")
                            .contains(PipelineState.RUNNING);
                    previous.put(pipeline, claim(initial, pipeline));
                }
                Map<String, ClusterMemberFacts> oldBoots = initial.clusterMembers().stream()
                        .collect(Collectors.toMap(ClusterMemberFacts::nodeId, member -> member));
                ClusterRecoveryItemView.Profile oldProfile = initial.clusterProfile();
                assertThat(oldProfile).isNotNull();
                Map<String, String> revisions = PIPELINES.stream().collect(Collectors.toMap(pipeline -> pipeline,
                        pipeline -> desired(store, pipeline).getString("revision")));
                cluster.killAll();
                assertThat(NODES).allSatisfy(node -> assertThat(cluster.isAlive(node)).isFalse());
                // Read after every JVM is dead. No old producer can advance these archived source facts.
                Map<String, ClusterRecoveryPosition> archived = new LinkedHashMap<>();
                for (String pipeline : PIPELINES) {
                    for (Source source : SOURCES.get(pipeline)) {
                        Document row = consumer(store, pipeline, source);
                        assertThat(row).isNotNull();
                        Document prepared = row.get("preparedSourceStart", Document.class);
                        assertThat(prepared).isNotNull();
                        String capture = prepared.get("requestedPosition", Document.class).getString("captureId");
                        archived.put(source.id(), meta.resumeWitness(source.id(), source.connector(),
                                row.getString("miningChainId"), SrsConsumerId.of(pipeline, source.id()).value(),
                                ReadMode.SNAPSHOT_AND_CDC, source.buffered(), List.of(source.table()))
                                .requestedPosition(capture).orElseThrow());
                    }
                }
                String snapshotSeam = consumer(store, SNAPSHOT, SNAPSHOT_SOURCE).getString("cdcStartPosition");
                assertThat(archived.get(SNAPSHOT_SOURCE.id()).position()).isNotNull();
                assertThat(archived.get(SNAPSHOT_SOURCE.id()).position().token()).isEqualTo(snapshotSeam);
                int oldReaderEvents = lines(reads).size();
                update(snapshot, LARGE_TABLE, 7, -7);
                snapshot.getCollection(LARGE_TABLE).deleteOne(new Document("_id", 11));
                snapshot.getCollection(LARGE_TABLE).insertOne(row(LARGE_ROWS + 1, LARGE_ROWS + 1));
                update(single, CDC_TABLE, 1, -101);
                single.getCollection(CDC_TABLE).deleteOne(new Document("_id", 2));
                single.getCollection(CDC_TABLE).insertOne(row(4, 404));
                execute(sql, "UPDATE orders SET name='parent-after-stop' WHERE id=1",
                        "UPDATE order_items SET sku='child-after-stop' WHERE id=12",
                        "DELETE FROM order_items WHERE id=11", "DELETE FROM orders WHERE id=2",
                        "INSERT INTO orders VALUES(3,'new-parent')", "INSERT INTO order_items VALUES(31,3,'new-child')");
                awaitOldAuthorizationExpired(store, cluster.clusterId(), oldBoots);
                assertIntents(store, revisions);
                cluster.relaunchAll();
                ControlPlane resumed = cluster.member(NODES.getFirst());
                NODES.forEach(node -> cluster.awaitExactly(node, NODES.size(), BOUND));
                Await.until("one new compatible profile generation", BOUND,
                        () -> resumed.clusterProfile() != null && resumed.clusterProfile().generation() == oldProfile.generation() + 1,
                        () -> String.valueOf(resumed.clusterProfile()));
                assertThat(resumed.clusterProfile().hash()).isEqualTo(oldProfile.hash());
                for (ClusterMemberFacts member : resumed.clusterMembers()) {
                    assertThat(oldBoots).containsKey(member.nodeId());
                    assertThat(member.bootId()).isNotEqualTo(oldBoots.get(member.nodeId()).bootId());
                    assertThat(member.memberUuid()).isNotEqualTo(oldBoots.get(member.nodeId()).memberUuid());
                }
                Set<Long> newPids = NODES.stream().map(node -> cluster.processCarrying(node).pid()).collect(Collectors.toSet());
                Await.until("a new writer reaches the retained snapshot hold", BOUND,
                        () -> heldBy(holds.directory(), newPids), () -> "newPids=" + newPids + ", holds=" + paths(holds.directory()));
                ClusterRecoveryItemView loading = Await.answered("the snapshot's one unfinished rebuild", BOUND,
                        () -> item(resumed, SNAPSHOT).filter(item -> item.successor() != null
                                && item.persistedStatus().equals(ClusterRecoveryStatus.REBUILDING.name())));
                assertThat(loading.successor().sourcesAcceptedAt()).isNull();
                assertThat(loading.successor().pipelineClaim().executionGeneration()).isEqualTo(previous.get(SNAPSHOT).executionGeneration() + 1);
                assertThat(consumer(store, SNAPSHOT, SNAPSHOT_SOURCE).getString("cdcStartPosition")).isEqualTo(snapshotSeam);
                assertThat(completed(consumer(store, SNAPSHOT, SNAPSHOT_SOURCE), LARGE_TABLE)).isFalse();
                holds.releaseAfter(LARGE_TABLE, HELD_ROW);
                Map<String, ClusterRecoveryItemView> recovered = new LinkedHashMap<>();
                for (String pipeline : PIPELINES) {
                    ClusterRecoveryItemView item = Await.answered(pipeline + " to recover from Mongo truth", BOUND,
                            () -> item(resumed, pipeline).filter(value -> value.persistedStatus().equals(ClusterRecoveryStatus.RECOVERED.name())));
                    assertRecovered(store, resumed, pipeline, item, previous.get(pipeline), oldProfile, archived);
                    recovered.put(pipeline, item);
                }
                assertThat(recovered.values().stream().map(ClusterRecoveryItemView::enqueueSequence).distinct()).hasSize(3);
                assertIntents(store, revisions);
                assertThat(consumer(store, SNAPSHOT, SNAPSHOT_SOURCE).getString("cdcStartPosition")).isEqualTo(snapshotSeam);
                assertThat(completed(consumer(store, SNAPSHOT, SNAPSHOT_SOURCE), LARGE_TABLE)).isTrue();
                assertReaderStarted(reads, oldReaderEvents, LARGE_TABLE, snapshotSeam);
                assertReaderStarted(reads, oldReaderEvents, CDC_TABLE, archived.get(CDC_SOURCE.id()).position().token());
                assertThat(after(reads, oldReaderEvents)).anyMatch(line -> line.startsWith("DELIVERY\t" + LARGE_TABLE + "\t"));
                awaitCsv(snapshot, fileTarget, LARGE_TABLE);
                awaitMongo(target, CDC_TABLE, Map.of(1L, -101L, 3L, 3L, 4L, 404L));
                awaitNest(target, Map.of(1L, "parent-after-stop|12:child-after-stop", 3L, "new-parent|31:new-child"));
                // These new events happen after the restarted snapshot and source startup have completed.
                update(snapshot, LARGE_TABLE, 8, -8);
                snapshot.getCollection(LARGE_TABLE).deleteOne(new Document("_id", 13));
                snapshot.getCollection(LARGE_TABLE).insertOne(row(LARGE_ROWS + 2, LARGE_ROWS + 2));
                update(single, CDC_TABLE, 3, -303);
                single.getCollection(CDC_TABLE).deleteOne(new Document("_id", 4));
                single.getCollection(CDC_TABLE).insertOne(row(5, 505));
                execute(sql, "UPDATE order_items SET sku='post-recovery-child' WHERE id=12",
                        "DELETE FROM order_items WHERE id=31", "INSERT INTO order_items VALUES(32,3,'post-recovery-insert')");
                awaitCsv(snapshot, fileTarget, LARGE_TABLE);
                awaitMongo(target, CDC_TABLE, mongoRows(single, CDC_TABLE));
                awaitNest(target, sqlNest(sql));
                assertThat(csv(fileTarget, LARGE_TABLE)).containsEntry(7L, -7L).containsEntry(8L, -8L)
                        .doesNotContainKeys(11L, 13L).containsKeys((long) LARGE_ROWS + 1, (long) LARGE_ROWS + 2);
                assertThat(target.getCollection(CDC_TABLE).countDocuments()).isEqualTo(single.getCollection(CDC_TABLE).countDocuments());
                assertThat(checksum(mongoRows(target, CDC_TABLE))).isEqualTo(checksum(mongoRows(single, CDC_TABLE)));
                assertThat(checksum(nestRows(target))).isEqualTo(checksum(sqlNest(sql)));
                assertIntents(store, revisions);
                for (String pipeline : PIPELINES) {
                    assertThat(claim(resumed, pipeline).executionGeneration()).isEqualTo(previous.get(pipeline).executionGeneration() + 1);
                    assertThat(queueRows(store, cluster.clusterId(), pipeline)).hasSize(1);
                }
            }
        }
    }

    private static void assertRecovered(MongoDatabase store, ControlPlane control, String pipeline,
            ClusterRecoveryItemView item, ClusterClaimView previous, ClusterRecoveryItemView.Profile oldProfile,
            Map<String, ClusterRecoveryPosition> archived) {
        assertThat(item.cause()).isEqualTo("FULL_CLUSTER_RESTART");
        assertThat(item.attempt()).isEqualTo(1);
        assertThat(item.originalExecutionGeneration()).isEqualTo(previous.executionGeneration());
        assertThat(item.originalProfile().generation()).isEqualTo(oldProfile.generation());
        assertThat(item.targetProfile().generation()).isEqualTo(oldProfile.generation() + 1);
        assertThat(item.successor().executionNodeIds()).containsExactlyInAnyOrderElementsOf(NODES);
        assertThat(item.successor().requiredSourceIds()).containsExactlyInAnyOrderElementsOf(
                SOURCES.get(pipeline).stream().map(Source::id).toList());
        assertThat(item.successor().sourceRequirementsRecorded()).isTrue();
        assertThat(item.successor().nativeJobId()).isNotBlank();
        assertThat(item.successor().nativeInitializedAt()).isNotNull();
        assertThat(item.successor().sourcesAcceptedAt()).isNotNull();
        assertThat(control.placedVertices(pipeline).stream().map(ControlPlane.PlacedVertex::executionId)
                .filter(java.util.Objects::nonNull).collect(Collectors.toSet())).hasSize(1);
        assertThat(item.successor().acceptedPositions()).isEqualTo(item.successor().requestedPositions());
        assertThat(claim(control, pipeline).executionGeneration()).isEqualTo(previous.executionGeneration() + 1);
        assertThat(claim(control, pipeline).executionMembers().stream().map(ClusterClaimView.Member::nodeId))
                .containsExactlyInAnyOrderElementsOf(NODES);
        assertThat(queueRows(store, control.clusterId(), pipeline)).hasSize(1);
        Document successor = queueRows(store, control.clusterId(), pipeline).getFirst().get("successor", Document.class);
        Document receipt = successor.get("startupReceipt", Document.class);
        assertThat(receipt).isNotNull();
        List<Document> witnesses = receipt.getList("preparedWitnesses", Document.class);
        assertThat(witnesses).hasSize(SOURCES.get(pipeline).size());
        for (Source source : SOURCES.get(pipeline)) {
            ClusterRecoveryPosition original = archived.get(source.id());
            ClusterRecoveryItemView.Position actual = item.originalPositions().get(source.id());
            assertThat(actual.connectorId()).isEqualTo(source.connector());
            assertThat(actual.captureId()).isEqualTo(original.captureId());
            assertThat(actual.kind()).isEqualTo(original.kind().name());
            assertThat(actual.token()).isEqualTo(original.position() == null ? null : original.position().token());
            assertThat(actual.epoch()).isEqualTo(original.position() == null || original.position().order() == null
                    ? null : original.position().order().epoch());
            assertThat(actual.sequence()).isEqualTo(original.position() == null || original.position().order() == null
                    ? null : original.position().order().seq());
            Document witness = witnesses.stream().filter(value -> source.id().equals(value.getString("sourceId"))).findFirst().orElseThrow();
            assertThat(witness.getList("tables", String.class)).containsExactly(source.table());
            assertThat(witness.getString("consumerId")).isEqualTo(SrsConsumerId.of(pipeline, source.id()).value());
            Document root = store.getCollection(MongoStorePort.SRS_META).find(new Document("_id", witness.getString("miningChainId"))).first();
            Document reader = root.get("captureReadAttempt", Document.class);
            assertThat(reader).isNotNull();
            assertThat(reader.getDate("firstDeliveredAt")).isNotNull();
            assertThat(reader.getString("resolvedAnchor")).isNotBlank();
            assertThat(reader.getBoolean("failed")).isFalse();
            assertThat(reader.get("captureClaim", Document.class).get("profileGeneration", Number.class).longValue())
                    .isEqualTo(item.targetProfile().generation());
            Document prepared = consumer(store, pipeline, source).get("preparedSourceStart", Document.class);
            assertThat(prepared.get("readerClaim")).isEqualTo(reader.get("captureClaim"));
            assertThat(prepared.get("readerTables")).isEqualTo(reader.get("tables"));
            assertThat(prepared.get("readerVersion")).isEqualTo(reader.get("version"));
            assertThat(prepared.get("readerEpoch")).isEqualTo(reader.get("chainEpoch"));
            // Epoch changes fence ring generations; they are not proof of native-token advancement.
            assertThat(item.successor().requestedPositions().get(source.id()).token()).isNotBlank();
        }
    }

    private static void awaitOldAuthorizationExpired(MongoDatabase store, String cluster, Map<String, ClusterMemberFacts> boots) {
        Document nodes = new Document("clusterId", cluster).append("resourceType", WorkloadClaimType.NODE_SESSION.name());
        assertThat(store.getCollection(MongoStorePort.WORKLOAD_CLAIMS).find(nodes).into(new ArrayList<>())).hasSize(NODES.size())
                .allSatisfy(row -> assertThat(row.getString("ownerBootId")).isEqualTo(boots.get(row.getString("ownerNodeId")).bootId()));
        Await.until("all exact old node sessions and their conservative horizon to expire on Mongo time", BOUND,
                () -> store.getCollection(MongoStorePort.WORKLOAD_CLAIMS).countDocuments(new Document(nodes)
                        .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")))) == 0
                        && store.getCollection(MongoStorePort.CLUSTER_EXECUTION_PROFILES).find(new Document("_id", cluster)
                                .append("$expr", new Document("$lte", List.of("$authorizationUntil", "$$NOW")))).first() != null,
                () -> "sessions=" + store.getCollection(MongoStorePort.WORKLOAD_CLAIMS).find(nodes).into(new ArrayList<>())
                        + ", profile=" + store.getCollection(MongoStorePort.CLUSTER_EXECUTION_PROFILES).find(new Document("_id", cluster)).first());
    }

    private static ClusterClaimView claim(ControlPlane control, String pipeline) {
        return control.clusterStatus().pipelines().stream().filter(value -> value.pipelineId().equals(pipeline))
                .findFirst().orElseThrow().controllerClaim();
    }

    private static void awaitCohort(ControlPlane control, String pipeline) {
        Await.until(pipeline + " to use every actual active member", BOUND,
                () -> control.membersCarryingPartOf(pipeline).equals(Set.copyOf(NODES)) && claim(control, pipeline) != null
                        && claim(control, pipeline).leased(),
                () -> "participants=" + control.membersCarryingPartOf(pipeline) + ", claim=" + claim(control, pipeline));
    }

    private static Optional<ClusterRecoveryItemView> item(ControlPlane control, String pipeline) {
        return control.pipelineRecovery(pipeline).items().stream().max(java.util.Comparator.comparingLong(ClusterRecoveryItemView::enqueueSequence));
    }

    private static List<Document> queueRows(MongoDatabase store, String cluster, String pipeline) {
        return store.getCollection(MongoStorePort.CLUSTER_RECOVERY_QUEUE)
                .find(new Document("clusterId", cluster).append("pipelineId", pipeline)).into(new ArrayList<>());
    }

    private static Document desired(MongoDatabase store, String pipeline) {
        return store.getCollection(MongoStorePort.PIPELINE_DESIRED).find(new Document("_id", pipeline)).first();
    }

    private static void assertIntents(MongoDatabase store, Map<String, String> revisions) {
        revisions.forEach((pipeline, revision) -> assertThat(desired(store, pipeline))
                .containsEntry("targetState", PipelineState.RUNNING.name()).containsEntry("revision", revision));
    }

    private static Document consumer(MongoDatabase store, String pipeline, Source source) {
        return store.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS)
                .find(new Document("pipelineId", SrsConsumerId.of(pipeline, source.id()).value())).first();
    }

    private static boolean completed(Document consumer, String table) {
        return consumer != null && consumer.get("snapshotCompletedTables") instanceof List<?> completed && completed.contains(table);
    }

    private static boolean landed(ControlPlane control, String pipeline, String table) {
        return control.snapshotTables(pipeline).containsKey(table) && control.snapshotTables(pipeline).get(table).landed();
    }

    private static boolean heldBy(Path directory, Set<Long> pids) {
        String prefix = "held-after-" + LARGE_TABLE + "-" + HELD_ROW + "-";
        return paths(directory).stream().filter(name -> name.startsWith(prefix))
                .map(name -> Long.parseLong(name.substring(prefix.length()))).anyMatch(pids::contains);
    }

    private static List<String> paths(Path directory) {
        try (var files = Files.list(directory)) { return files.map(path -> path.getFileName().toString()).toList(); }
        catch (IOException failure) { throw new UncheckedIOException(failure); }
    }

    private static List<String> lines(Path path) {
        try { return Files.exists(path) ? Files.readAllLines(path) : List.of(); }
        catch (IOException failure) { throw new UncheckedIOException(failure); }
    }

    private static List<String> after(Path path, int count) {
        List<String> all = lines(path);
        return all.subList(Math.min(count, all.size()), all.size());
    }

    private static void assertReaderStarted(Path path, int count, String table, String token) {
        assertThat(after(path, count)).anyMatch(line -> line.startsWith("START\t" + table + "\t")
                && line.split("\t", 4)[2].equals(token));
    }

    private static MongoDatabase database(MongoClient client, String uri) {
        return client.getDatabase(new ConnectionString(uri).getDatabase());
    }

    private static Document row(long id, long sequence) {
        return new Document("_id", id).append("id", id).append("seq", sequence);
    }

    private static void seedRows(MongoDatabase database, String table, int rows) {
        for (int begin = 1; begin <= rows; begin += 1000) {
            List<Document> batch = new ArrayList<>();
            for (int id = begin; id <= rows && id < begin + 1000; id++) batch.add(row(id, id));
            database.getCollection(table).insertMany(batch);
        }
    }

    private static void update(MongoDatabase database, String table, long id, long sequence) {
        database.getCollection(table).updateOne(new Document("_id", id), new Document("$set", new Document("seq", sequence)));
    }

    private static void seedNest(Connection sql) throws Exception {
        execute(sql, "CREATE TABLE orders(id INT PRIMARY KEY,name VARCHAR(64))",
                "CREATE TABLE order_items(id INT PRIMARY KEY,order_id INT,sku VARCHAR(64))",
                "INSERT INTO orders VALUES(1,'parent-1'),(2,'parent-2')",
                "INSERT INTO order_items VALUES(11,1,'child-11'),(12,1,'child-12'),(21,2,'child-21')");
    }

    private static void execute(Connection sql, String... statements) throws Exception {
        try (Statement command = sql.createStatement()) { for (String statement : statements) command.execute(statement); }
    }

    private static Map<Long, Long> mongoRows(MongoDatabase database, String table) {
        Map<Long, Long> rows = new TreeMap<>();
        for (Document row : database.getCollection(table).find()) {
            long id = ((Number) row.get("id")).longValue();
            assertThat(rows.put(id, ((Number) row.get("seq")).longValue())).isNull();
        }
        return rows;
    }

    private static Map<Long, Long> csv(Path directory, String table) {
        List<String> lines = lines(directory.resolve(table + ".csv"));
        if (lines.isEmpty()) return Map.of();
        List<String> header = List.of(lines.getFirst().split(",", -1));
        int id = header.indexOf("id");
        int sequence = header.indexOf("seq");
        assertThat(id).isNotNegative(); assertThat(sequence).isNotNegative();
        Map<Long, Long> rows = new TreeMap<>();
        for (String line : lines.subList(1, lines.size())) {
            if (line.isBlank()) continue;
            String[] values = line.split(",", -1);
            assertThat(rows.put(Long.parseLong(values[id]), Long.parseLong(values[sequence]))).isNull();
        }
        return rows;
    }

    private static void awaitCsv(MongoDatabase source, Path target, String table) {
        Await.until("the entire restarted snapshot/tail checksum to equal its source", BOUND,
                () -> checksum(csv(target, table)).equals(checksum(mongoRows(source, table))),
                () -> "sourceCount=" + source.getCollection(table).countDocuments() + ", targetCount=" + csv(target, table).size());
    }

    private static void awaitMongo(MongoDatabase target, String table, Map<Long, Long> expected) {
        Await.until(table + " to converge to its complete source checksum", BOUND,
                () -> mongoRows(target, table).equals(expected), () -> "target=" + mongoRows(target, table));
    }

    private static Map<Long, String> expectedNest(String parent1, String child11, String child12, String parent2) {
        return Map.of(1L, parent1 + "|11:" + child11 + ",12:" + child12, 2L, parent2 + "|21:child-21");
    }

    private static Map<Long, String> nestRows(MongoDatabase target) {
        Map<Long, String> rows = new TreeMap<>();
        for (Document document : target.getCollection(PARENTS).find()) {
            Map<Long, String> children = new TreeMap<>();
            List<Document> embedded = document.getList("items", Document.class);
            if (embedded != null) {
                for (Document child : embedded) {
                    assertThat(children.put(((Number) child.get("id")).longValue(), child.getString("sku"))).isNull();
                }
            }
            assertThat(rows.put(((Number) document.get("id")).longValue(), document.getString("name") + "|"
                    + children.entrySet().stream().map(entry -> entry.getKey() + ":" + entry.getValue()).collect(Collectors.joining(","))))
                    .isNull();
        }
        return rows;
    }

    private static Map<Long, String> sqlNest(Connection sql) throws Exception {
        Map<Long, String> rows = new TreeMap<>();
        try (Statement query = sql.createStatement(); var parents = query.executeQuery("SELECT id,name FROM orders ORDER BY id")) {
            while (parents.next()) {
                int id = parents.getInt(1);
                List<String> items = new ArrayList<>();
                try (Statement childQuery = sql.createStatement(); var children = childQuery.executeQuery(
                        "SELECT id,sku FROM order_items WHERE order_id=" + id + " ORDER BY id")) {
                    while (children.next()) items.add(children.getInt(1) + ":" + children.getString(2));
                }
                rows.put((long) id, parents.getString(2) + "|" + String.join(",", items));
            }
        }
        return rows;
    }

    private static void awaitNest(MongoDatabase target, Map<Long, String> expected) {
        Await.until("the whole Nest to match the actual parent/child tables", BOUND,
                () -> nestRows(target).equals(expected), () -> "target=" + nestRows(target));
    }

    private static String checksum(Map<?, ?> rows) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            rows.forEach((key, value) -> digest.update((key + "=" + value + "\n").getBytes(StandardCharsets.UTF_8)));
            return HexFormat.of().formatHex(digest.digest());
        } catch (java.security.NoSuchAlgorithmException unavailable) { throw new AssertionError(unavailable); }
    }

    private static void awaitState(ControlPlane control, String pipeline) {
        Await.until(pipeline + " to have a running observed execution", BOUND,
                () -> control.state(pipeline).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> control.state(pipeline) + ", failure=" + control.failure(pipeline));
    }

    private static Map<String, String> resources(String snapshot, String cdc, String target, Path file,
            Holds holds, Map<String, Object> mysql) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SNAPSHOT_SOURCE.id() + ".tap.yml", mongoSource(SNAPSHOT_SOURCE, snapshot));
        resources.put(CDC_SOURCE.id() + ".tap.yml", mongoSource(CDC_SOURCE, cdc));
        for (Source source : List.of(PARENT_SOURCE, CHILD_SOURCE)) {
            resources.put(source.id() + ".tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: %s
                    connector: mysql
                    config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                    mode: cdc
                    tables: [ %s ]
                    """.formatted(source.id(), mysql.get("host"), mysql.get("port"), mysql.get("database"),
                    mysql.get("username"), mysql.get("password"), source.table()));
        }
        resources.put("snapshot_file.tap.yml", """
                version: tapstate/v1
                kind: source
                id: snapshot_file
                connector: e2e_file
                config: { uri: "%s", hold: "%s" }
                """.formatted(file, holds.directory()));
        resources.put("cold_mongo_target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: cold_mongo_target
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(target));
        resources.put(SNAPSHOT + ".tap.yml", plainPipeline(SNAPSHOT, SNAPSHOT_SOURCE, "snapshot_file"));
        resources.put(CDC + ".tap.yml", plainPipeline(CDC, CDC_SOURCE, "cold_mongo_target"));
        resources.put(NEST + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ %s, %s ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: cold_order_doc
                    type: nest
                    from: { o: orders, i: order_items }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                    execution: { parallelism: 3 }
                serve:
                  from: cold_order_doc
                  sync:
                    - source: cold_mongo_target
                """.formatted(NEST, PARENT_SOURCE.id(), CHILD_SOURCE.id()));
        return resources;
    }

    private static String mongoSource(Source source, String uri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(source.id(), uri, source.table());
    }

    private static String plainPipeline(String id, Source source, String target) {
        return """
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
                """.formatted(id, source.id(), source.table(), target);
    }

    private record Source(String id, String connector, String table, boolean buffered) {}
}
