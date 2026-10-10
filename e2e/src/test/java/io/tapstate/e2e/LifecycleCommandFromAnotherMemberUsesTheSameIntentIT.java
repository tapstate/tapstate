package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoDesiredStore;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.ClusterCapacityView;
import io.tapstate.control.core.ClusterClaimView;
import io.tapstate.control.core.ClusterMemberState;
import io.tapstate.control.core.ClusterMemberView;
import io.tapstate.control.core.ClusterPipelineView;
import io.tapstate.control.core.ClusterTopologyView;
import io.tapstate.core.lifecycle.DesiredState;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A late member's lifecycle commands reach the same durable intent and the existing claim holder.
 *
 * <p>All five members are packaged server processes. Two leave before the first run is admitted.
 * The fourth returns before resume and the fifth before stop, keeping each issuer outside the
 * native cohort present when that command is sent.
 * The file connector's read and write witnesses distinguish a paused or duplicated job from a
 * successful HTTP response; this case makes no claim about a connector resume token.
 */
class LifecycleCommandFromAnotherMemberUsesTheSameIntentIT {

    private static final String DATABASE = "e2e_lifecycle_other_member";
    private static final String PIPELINE = "other_member_pipe";
    private static final String SOURCE = "other_member_src";
    private static final String TARGET = "other_member_tgt";
    private static final String TABLE = "orders";
    private static final String JOINING = "node-d";
    private static final String STOP_ISSUER = "node-e";
    private static final List<String> ORIGINAL = List.of("node-a", "node-b", "node-c");
    private static final List<String> ALL = List.of("node-a", "node-b", "node-c", JOINING);
    private static final List<String> FLEET = List.of("node-a", "node-b", "node-c", JOINING, STOP_ISSUER);
    private static final long SEEDED_ROWS = 3;
    private static final Duration BOUND = Duration.ofMinutes(3);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aMemberOutsideTheRunCannotCreateAnotherJobBySendingItsLifecycleCommands(@TempDir Path directory)
            throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));
        Path reads = Files.createDirectories(directory.resolve("reads"));
        Holds holds = new Holds(directory.resolve("holds"));
        WriteWitness writes = new WriteWitness(directory.resolve("writes"));
        EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
        EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
        String store = SharedMongo.replicaSetUrl(DATABASE);

        try (FileEndpoints files = new FileEndpoints();
                MongoClient mongo = MongoClients.create(store);
                PartitionableCluster cluster = PartitionableCluster.start(store, "e2e-other-member", FLEET)) {
            MongoDatabase database = mongo.getDatabase(DATABASE);
            MongoDesiredStore desired = new MongoDesiredStore(SystemCollections.PIPELINE_DESIRED.on(database));
            MongoObservationStore observations = new MongoObservationStore(SystemCollections.PIPELINE_OBSERVATION.on(database));
            ControlPlane control = cluster.member(ORIGINAL.getFirst());
            awaitActive(control, FLEET);
            cluster.stop(JOINING);
            cluster.stop(STOP_ISSUER);
            awaitActive(control, ORIGINAL);
            Await.until("the fourth boot's node session to retire before admission", BOUND,
                    () -> List.of(JOINING, STOP_ISSUER).stream().allMatch(node -> control.clusterStatus().members().stream()
                            .anyMatch(member -> node.equals(member.nodeId()) && Boolean.FALSE.equals(member.live())
                                    && Boolean.FALSE.equals(member.sessionLeased()))),
                    () -> "members = " + control.clusterStatus().members());

            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
            files.seed(sourceAddress, TABLE, SeedRows.generated(SEEDED_ROWS));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put(SOURCE + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE, source, List.of(TABLE),
                    Map.of("read_witness", reads.toString())));
            resources.put(TARGET + ".tap.yml", Workspaces.targetYaml(TARGET, target,
                    Map.of("hold", holds.directory().toString(), "write_witness", writes.directory().toString())));
            resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE, SOURCE, TARGET, TABLE, 3));
            control.apply(resources);
            holds.after(TABLE, SEEDED_ROWS);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            awaitState(control, PipelineState.RUNNING);
            Await.answered("a writer to hold the snapshot's final acknowledgment", BOUND,
                    () -> holds.holderAfter(TABLE, SEEDED_ROWS));
            Await.until("the held load's progress to be published", BOUND,
                    () -> control.snapshotTables(PIPELINE).containsKey(TABLE),
                    () -> "load = " + control.snapshotTables(PIPELINE));
            assertThat(control.snapshotTables(PIPELINE).get(TABLE).landed())
                    .as("having written the rows has not yet settled their load").isFalse();
            holds.releaseAfter(TABLE, SEEDED_ROWS);
            awaitLanded(control, files, targetAddress, SEEDED_ROWS);
            awaitMeasured(control, ORIGINAL);
            ClusterClaimView originalClaim = claim(control);
            assertThat(originalClaim.ownerNodeId()).isIn(ORIGINAL);
            Submitted originalJob = awaitOneSubmitted(database, cluster.clusterId());
            assertThat(originalClaim.executionMembers().stream().map(ClusterClaimView.Member::nodeId))
                    .containsExactlyInAnyOrderElementsOf(ORIGINAL);
            assertThat(originalJob.generation()).isEqualTo(originalClaim.executionGeneration());
            Set<String> originalExecution = executions(control);
            assertThat(originalExecution).hasSize(1);

            ControlPlane fourth = cluster.relaunch(JOINING);
            awaitActive(control, ALL);
            Await.until("the late member to be named awaiting rebalance", BOUND,
                    () -> control.awaitingRebalance(PIPELINE).equals(List.of(JOINING))
                            && fourth.clusterAwaitingRebalance(PIPELINE).equals(List.of(JOINING)),
                    () -> "awaiting = " + control.awaitingRebalance(PIPELINE));
            assertOutsideRun(fourth);
            assertSameDriver(originalClaim, claim(fourth));
            assertThat(executions(fourth)).isEqualTo(originalExecution);
            assertThat(oneSubmitted(database, cluster.clusterId())).isEqualTo(originalJob);

            DesiredState running = desired.read(PIPELINE).orElseThrow();
            assertThat(fourth.startUnlessRunning(PIPELINE))
                    .as("START from RUNNING is the existing coded refusal, not another submission").isFalse();
            assertThat(desired.read(PIPELINE)).contains(running);
            assertSameRun(originalClaim, claim(fourth), originalJob, database, cluster.clusterId());
            files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);
            awaitRow(files, targetAddress, SEEDED_ROWS + 1);
            assertWrittenOnce(writes, SEEDED_ROWS + 1);

            assertOutsideRun(fourth);
            fourth.lifecycle(PIPELINE, LifecycleVerb.PAUSE);
            awaitIntent(desired, PipelineState.PAUSED, running.revision());
            awaitState(control, PipelineState.PAUSED);
            assertSameRun(originalClaim, claim(fourth), originalJob, database, cluster.clusterId());
            DesiredState paused = desired.read(PIPELINE).orElseThrow();
            assertStartRefused(fourth, PipelineState.PAUSED);
            assertThat(desired.read(PIPELINE)).contains(paused);
            assertSameRun(originalClaim, claim(fourth), originalJob, database, cluster.clusterId());

            long capturedBefore = tailRows(reads);
            files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);
            Await.until("the physical capture to read the change while the job is paused", BOUND,
                    () -> tailRows(reads) > capturedBefore, () -> "tail rows = " + tailRows(reads));
            Instant captureObserved = Instant.now();
            Await.until("a fresh paused observation after the capture's change", BOUND,
                    () -> observations.read(PIPELINE).filter(observation -> observation.state() == PipelineState.PAUSED
                            && observation.observedAt() != null && observation.observedAt().isAfter(captureObserved)).isPresent(),
                    () -> "observation = " + observations.read(PIPELINE));
            assertThat(files.fetch(targetAddress, TABLE, Map.of("id", SEEDED_ROWS + 2))).isEmpty();
            assertThat(writes.rowsOf(TABLE).stream().filter(row -> row.id().equals(String.valueOf(SEEDED_ROWS + 2))))
                    .isEmpty();

            assertOutsideRun(fourth);
            fourth.lifecycle(PIPELINE, LifecycleVerb.RESUME);
            awaitIntent(desired, PipelineState.RUNNING, running.revision());
            awaitState(control, PipelineState.RUNNING);
            awaitRow(files, targetAddress, SEEDED_ROWS + 2);
            assertWrittenOnce(writes, SEEDED_ROWS + 2);
            awaitMeasured(control, ALL);
            ClusterClaimView resumed = claim(fourth);
            assertSameDriver(claim(control), resumed);
            assertThat(resumed.claimGeneration()).isGreaterThan(originalClaim.claimGeneration());
            assertThat(resumed.executionGeneration()).isEqualTo(originalClaim.executionGeneration() + 1);
            assertThat(resumed.executionMembers().stream().map(ClusterClaimView.Member::nodeId))
                    .containsExactlyInAnyOrderElementsOf(ALL);
            Submitted resumedJob = awaitOneSubmitted(database, cluster.clusterId());
            assertThat(resumedJob.generation()).isEqualTo(resumed.executionGeneration());
            assertThat(resumedJob.jobId()).isNotEqualTo(originalJob.jobId());
            assertThat(executions(fourth)).hasSize(1);

            ControlPlane fifth = cluster.relaunch(STOP_ISSUER);
            awaitActive(control, FLEET);
            Await.until("the fifth member to remain outside the already resumed job", BOUND,
                    () -> fifth.awaitingRebalance(PIPELINE).equals(List.of(STOP_ISSUER)),
                    () -> "awaiting = " + fifth.awaitingRebalance(PIPELINE));
            assertOutsideRun(fifth, STOP_ISSUER);
            assertSameRun(resumed, claim(fifth), resumedJob, database, cluster.clusterId());
            fifth.stop(PIPELINE, false);
            awaitIntent(desired, PipelineState.STOPPED, running.revision());
            awaitState(control, PipelineState.STOPPED);
            assertThat(desired.read(PIPELINE).orElseThrow().purgeState()).isFalse();
            // Retired receipts may remain stored; the live native and capacity views must be empty.
            Await.until("the stopped native execution and its cached authority to retire", BOUND,
                    () -> stoppedExecutionRetired(database, cluster.clusterId(), resumed, resumedJob)
                            && stoppedView(control, resumed) && stoppedView(fifth, resumed),
                    () -> "cluster = " + control.clusterStatus() + "; submissions = "
                            + submissions(database, cluster.clusterId()));
            long stoppedGeneration = resumed.executionGeneration();

            // With no run left, a legal START from this endpoint creates one new run using all active members.
            fifth.lifecycle(PIPELINE, LifecycleVerb.START);
            awaitIntent(desired, PipelineState.RUNNING, running.revision());
            awaitState(control, PipelineState.RUNNING);
            awaitLanded(control, files, targetAddress, SEEDED_ROWS + 2);
            awaitMeasured(control, FLEET);
            ClusterClaimView restarted = claim(fifth);
            assertSameDriver(claim(control), restarted);
            assertThat(restarted.executionGeneration()).isEqualTo(stoppedGeneration + 1);
            Submitted nextJob = awaitOneSubmitted(database, cluster.clusterId());
            assertThat(nextJob.generation()).isEqualTo(restarted.executionGeneration());
            assertThat(nextJob.jobId()).isNotEqualTo(resumedJob.jobId());
            assertThat(restarted.executionMembers().stream().map(ClusterClaimView.Member::nodeId))
                    .containsExactlyInAnyOrderElementsOf(FLEET);
            assertThat(executions(fifth)).hasSize(1);
            assertThat(desired.pipelineIds()).containsExactly(PIPELINE);
            files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);
            awaitRow(files, targetAddress, SEEDED_ROWS + 3);
            assertWrittenOnce(writes, SEEDED_ROWS + 3);
            control.stop(PIPELINE, false);
            awaitState(control, PipelineState.STOPPED);
        }
    }

    private static void awaitActive(ControlPlane control, List<String> expected) {
        Await.until("the exact active cohort " + expected, BOUND,
                () -> active(control).equals(new TreeSet<>(expected)),
                () -> "members = " + control.clusterStatus().members());
    }

    private static Set<String> active(ControlPlane control) {
        return control.clusterStatus().members().stream()
                .filter(member -> member.state() == ClusterMemberState.ACTIVE && Boolean.TRUE.equals(member.live()))
                .map(ClusterMemberView::nodeId).collect(Collectors.toCollection(TreeSet::new));
    }

    private static void awaitState(ControlPlane control, PipelineState state) {
        Await.until(PIPELINE + " to reach " + state, BOUND,
                () -> control.state(PIPELINE).filter(state::equals).isPresent(),
                () -> control.state(PIPELINE) + ", failure = " + control.failure(PIPELINE));
    }

    private static void awaitIntent(MongoDesiredStore desired, PipelineState state, String revision) {
        Await.until("the single durable intent to become " + state, BOUND,
                () -> desired.read(PIPELINE).filter(intent -> intent.targetState() == state
                        && intent.revision().equals(revision)).isPresent(),
                () -> "intent = " + desired.read(PIPELINE));
        assertThat(desired.pipelineIds()).containsExactly(PIPELINE);
    }

    private static void awaitLanded(ControlPlane control, FileEndpoints files, EndpointAddress target, long rows) {
        Await.until("the natural snapshot load to settle before lifecycle changes", BOUND,
                () -> control.snapshotTables(PIPELINE).containsKey(TABLE)
                        && control.snapshotTables(PIPELINE).get(TABLE).landed() && files.count(target, TABLE) == rows,
                () -> "load = " + control.snapshotTables(PIPELINE) + ", target rows = " + files.count(target, TABLE));
    }

    private static void awaitMeasured(ControlPlane control, List<String> cohort) {
        Await.until("each planned member to report its actual processors", BOUND, () -> {
            List<String> uuids = control.clusterStatus().members().stream().filter(member -> cohort.contains(member.nodeId()))
                    .map(ClusterMemberView::memberUuid).toList();
            return uuids.size() == cohort.size() && control.membersMeasuring(PIPELINE).containsAll(uuids)
                    && control.membersCarryingPartOf(PIPELINE).containsAll(cohort) && executions(control).size() == 1;
        }, () -> "measured = " + control.membersMeasuring(PIPELINE) + ", carrying = "
                + control.membersCarryingPartOf(PIPELINE));
    }

    private static ClusterClaimView claim(ControlPlane control) {
        List<ClusterPipelineView> pipelines = control.clusterStatus().pipelines().stream()
                .filter(pipeline -> PIPELINE.equals(pipeline.pipelineId())).toList();
        assertThat(pipelines).hasSize(1);
        ClusterClaimView claim = pipelines.getFirst().controllerClaim();
        assertThat(claim).isNotNull();
        assertThat(claim.leased()).isTrue();
        return claim;
    }

    private static void assertOutsideRun(ControlPlane control) {
        assertOutsideRun(control, JOINING);
    }

    private static void assertOutsideRun(ControlPlane control, String nodeId) {
        assertThat(control.awaitingRebalance(PIPELINE)).containsExactly(nodeId);
        assertThat(control.membersCarryingPartOf(PIPELINE)).doesNotContain(nodeId);
        assertThat(claim(control).executionMembers().stream().map(ClusterClaimView.Member::nodeId)).doesNotContain(nodeId);
    }

    private static void assertSameDriver(ClusterClaimView expected, ClusterClaimView actual) {
        assertThat(actual.ownerNodeId()).isEqualTo(expected.ownerNodeId());
        assertThat(actual.ownerBootId()).isEqualTo(expected.ownerBootId());
        assertThat(actual.claimGeneration()).isEqualTo(expected.claimGeneration());
    }

    private static void assertSameRun(ClusterClaimView expected, ClusterClaimView actual, Submitted submitted,
            MongoDatabase database, String clusterId) {
        assertSameDriver(expected, actual);
        assertThat(actual.executionGeneration()).isEqualTo(expected.executionGeneration());
        assertThat(oneSubmitted(database, clusterId)).isEqualTo(submitted);
    }

    private static void assertStartRefused(ControlPlane control, PipelineState from) {
        Throwable refusal = catchThrowable(() -> control.lifecycle(PIPELINE, LifecycleVerb.START));
        assertThat(refusal).isInstanceOf(AssertionError.class);
        String prefix = "could not start " + PIPELINE + ": expected HTTP 200, got 409 - ";
        assertThat(refusal.getMessage()).startsWith(prefix);
        ControlPlane.Refusal decoded = ControlPlane.interpretRefusal(409,
                refusal.getMessage().substring(prefix.length()), "starting " + PIPELINE);
        assertThat(decoded.code()).isEqualTo(LifecycleError.ILLEGAL_TRANSITION.code());
        assertThat(decoded.params()).containsEntry("from", from.name()).containsEntry("verb", LifecycleVerb.START.id());
    }

    private static Set<String> executions(ControlPlane control) {
        return control.placedVertices(PIPELINE).stream().map(ControlPlane.PlacedVertex::executionId)
                .filter(Objects::nonNull).collect(Collectors.toCollection(TreeSet::new));
    }

    private record Submitted(String jobId, long generation) {}

    private static boolean stoppedView(ControlPlane control, ClusterClaimView expected) {
        ClusterTopologyView view = control.clusterStatus();
        ClusterPipelineView pipeline = view.pipelines().stream()
                .filter(candidate -> PIPELINE.equals(candidate.pipelineId())).findFirst().orElse(null);
        ClusterCapacityView capacity = view.recovery() == null ? null : view.recovery().capacity();
        return pipeline != null && pipeline.vertices().isEmpty() && pipeline.controllerClaim() != null
                && pipeline.controllerClaim().executionGeneration() == expected.executionGeneration()
                && capacity != null && "AVAILABLE".equals(capacity.availability())
                && "mongo-capacity-snapshot".equals(capacity.provenance()) && capacity.profile() != null
                && Objects.equals(capacity.profile().hash(), expected.executionProfileHash())
                && Objects.equals(capacity.profile().generation(), expected.executionProfileGeneration())
                && capacity.occupiedByNode() != null && capacity.occupiedByNode().isEmpty();
    }

    private static boolean stoppedExecutionRetired(MongoDatabase database, String clusterId,
            ClusterClaimView expected, Submitted submitted) {
        Document key = new Document("clusterId", clusterId)
                .append("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name()).append("resourceId", PIPELINE);
        Document oldLiveAuthority = new Document("_id", key).append("ownerNodeId", expected.ownerNodeId())
                .append("ownerBootId", expected.ownerBootId()).append("claimGeneration", expected.claimGeneration())
                .append("executionGeneration", expected.executionGeneration()).append("profileGeneration", expected.profileGeneration())
                .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")));
        Document retired = new Document("_id", key).append("retiredAuthorizationUntil", new Document("$type", "date"))
                .append("$nor", List.of(oldLiveAuthority))
                .append("$expr", new Document("$lte", List.of("$retiredAuthorizationUntil", "$$NOW")));
        if (SystemCollections.WORKLOAD_CLAIMS.on(database).find(retired).first() == null) { return false; }
        if (SystemCollections.PIPELINE_STATE.on(database).find(new Document("_id", PIPELINE)
                .append("stateJson", PipelineState.STOPPED.name())
                .append("pendingPipelineResume", new Document("$exists", false))).first() == null) { return false; }

        Document receipt = new Document("clusterId", clusterId).append("pipelineId", PIPELINE)
                .append("nativeJobId", submitted.jobId()).append("executionGeneration", submitted.generation());
        var occupancy = SystemCollections.CLUSTER_CAPACITY_OCCUPANCY.on(database);
        if (occupancy.find(receipt).first() == null) { return true; }
        Document retiredReceipt = new Document(receipt)
                .append("pipelineClaim.ownerNodeId", expected.ownerNodeId())
                .append("pipelineClaim.ownerBootId", expected.ownerBootId())
                .append("pipelineClaim.claimGeneration", expected.claimGeneration())
                .append("pipelineClaim.executionGeneration", expected.executionGeneration())
                .append("pipelineClaim.profileGeneration", expected.profileGeneration())
                .append("authorityUntil", new Document("$type", "date")).append("deadline", new Document("$type", "date"))
                .append("$expr", new Document("$and", List.of(
                        new Document("$lte", List.of("$authorityUntil", "$$NOW")),
                        new Document("$lte", List.of("$deadline", "$$NOW")))));
        return occupancy.find(retiredReceipt).first() != null;
    }

    private static List<Document> submissions(MongoDatabase database, String clusterId) {
        return SystemCollections.CLUSTER_CAPACITY_OCCUPANCY.on(database)
                .find(new Document("clusterId", clusterId).append("pipelineId", PIPELINE))
                .into(new ArrayList<>());
    }

    private static Submitted awaitOneSubmitted(MongoDatabase database, String clusterId) {
        return Await.answered("one actual native-job submission receipt", BOUND, () -> {
            List<Document> submissions = submissions(database, clusterId);
            if (submissions.size() != 1 || !(submissions.getFirst().get("nativeJobId") instanceof String jobId)
                    || !(submissions.getFirst().get("executionGeneration") instanceof Number generation)) {
                return java.util.Optional.empty();
            }
            return java.util.Optional.of(new Submitted(jobId, generation.longValue()));
        });
    }

    private static Submitted oneSubmitted(MongoDatabase database, String clusterId) {
        List<Document> submissions = submissions(database, clusterId);
        assertThat(submissions).as("one submission for this resource, not a job local to each endpoint").hasSize(1);
        Document submitted = submissions.getFirst();
        assertThat(submitted.getString("nativeJobId")).isNotBlank();
        return new Submitted(submitted.getString("nativeJobId"), submitted.get("executionGeneration", Number.class).longValue());
    }

    private static void awaitRow(FileEndpoints files, EndpointAddress target, long id) {
        Await.until("CDC row " + id + " at the target", BOUND,
                () -> files.fetch(target, TABLE, Map.of("id", id)).isPresent(),
                () -> "row = " + files.fetch(target, TABLE, Map.of("id", id)));
    }

    private static void assertWrittenOnce(WriteWitness writes, long id) {
        Await.until("the target connector to record its physical write of " + id, BOUND,
                () -> writes.rowsOf(TABLE).stream().anyMatch(row -> row.id().equals(String.valueOf(id))),
                () -> "writes = " + writes.rowsOf(TABLE));
        assertThat(writes.rowsOf(TABLE).stream().filter(row -> row.id().equals(String.valueOf(id))))
                .as("one physical write of the new CDC row, even if the target's key could hide a second job").hasSize(1);
    }

    private static long tailRows(Path directory) {
        try (Stream<Path> files = Files.list(directory)) {
            long rows = 0;
            for (Path file : files.filter(path -> path.getFileName().toString().startsWith("reads-")).toList()) {
                for (String line : Files.readAllLines(file)) {
                    String[] cells = line.split("\t", -1);
                    if (cells.length == 4 && "tail".equals(cells[1]) && TABLE.equals(cells[2])) {
                        rows += Long.parseLong(cells[3]);
                    }
                }
            }
            return rows;
        } catch (IOException failure) {
            throw new UncheckedIOException("reading the physical capture witness", failure);
        }
    }
}
