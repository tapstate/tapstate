package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.SrsConsumerId;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A fourth member is visible without changing a running three-member execution. A later pipeline
 * uses all four members, and both executions are reconciled against their actual submitted plans.
 *
 * <p>Eight requested writers distinguish the shapes: the original three-member plan has nine,
 * while the later four-member plan has eight. The native callback facts and published compiler
 * plans provide the widths; this case does not calculate them from the membership size.
 *
 * <p>The existing harness retains its process-failure-only profile and two-member bootstrap.
 * Its three/four local processes do not establish production-profile or independent-host acceptance.
 */
class AMemberJoiningARunningPipelineIsNamedAwaitingRebalanceIT {
    private static final String TABLE = "orders";
    private static final String PIPELINE = "joining_member_pipe";
    private static final String NEW_PIPELINE = "joining_member_new_pipe";
    private static final String SOURCE = "joining_member_src";
    private static final String TARGET = "joining_member_tgt";
    private static final String NEW_TARGET = "joining_member_new_tgt";
    private static final String INITIAL_THIRD = "node-c";
    private static final String JOINING = "node-d";
    private static final List<String> ORIGINAL = List.of(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B, INITIAL_THIRD);
    private static final List<String> CURRENT = List.of(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B, INITIAL_THIRD, JOINING);
    private static final int WRITERS = 8;
    private static final long SEEDED_ROWS = 20;

    /** Twice the engine's default scale-up delay, preserving the existing spontaneous-restart discriminator. */
    private static final Duration WELL_PAST_THE_SCALE_UP_DELAY = Duration.ofSeconds(20);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aRunningRunKeepsItsExecutionAndWidthAndNamesTheMemberItIsNotUsing(@TempDir Path directory)
            throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));
        Path laterTarget = Files.createDirectories(directory.resolve("tgt_later"));
        EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
        EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
        EndpointAddress laterTargetAddress = EndpointAddress.uri(laterTarget.toString());
        String storeUri = SharedMongo.replicaSetUrl("e2e_joining_member_cluster");

        try (FileEndpoints files = new FileEndpoints(); MongoClient mongo = MongoClients.create(storeUri);
                TwoMemberCluster cluster = TwoMemberCluster.start(storeUri, "e2e-joining-member")) {
            var store = mongo.getDatabase(new ConnectionString(storeUri).getDatabase());
            cluster.awaitBothMembers();
            ControlPlane control = cluster.first();
            try (RealProcessServer third = cluster.launching(INITIAL_THIRD)) {
                cluster.awaitMembers(3);
                awaitActive(control, ORIGINAL);
                String clusterId = control.clusterId();
                var originalCommitted = ClusterProjectionAssertions.committedFacts(store, clusterId);
                assertThat(originalCommitted.nodeIds()).containsExactlyInAnyOrderElementsOf(ORIGINAL);
                List<NativeMemberWitness.MemberFacts> oldCohort;
                try (NativeMemberWitness nativeReader = NativeMemberWitness.connect(
                        clusterId, cluster.processCarrying(TwoMemberCluster.NODE_A))) {
                    oldCohort = nativeReader.members();
                }
                assertThat(oldCohort.stream().map(NativeMemberWitness.MemberFacts::nodeId))
                        .containsExactlyInAnyOrderElementsOf(ORIGINAL);

                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
                files.seed(sourceAddress, TABLE, SeedRows.generated(SEEDED_ROWS));
                control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put(SOURCE + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE, source));
                resources.put(TARGET + ".tap.yml", Workspaces.targetYaml(TARGET, target));
                resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE, SOURCE, TARGET, TABLE, WRITERS));
                control.apply(resources);
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                awaitRunning(control, PIPELINE);
                Await.until("the seeded rows to cross", Duration.ofMinutes(1),
                        () -> files.count(targetAddress, TABLE) == SEEDED_ROWS,
                        () -> "rows at the target = " + files.count(targetAddress, TABLE));
                awaitCompleteContexts(control, PIPELINE, oldCohort);

                ControlPlane.Plan before = Await.answered("the status to carry the run's plan",
                        () -> control.executionPlan(PIPELINE));
                assertThat(before.members()).containsExactlyInAnyOrderElementsOf(ORIGINAL);
                assertThat(before.nodeRequested(WRITERS).effective()).as("the actual original three-member plan").isEqualTo(9);
                long generation = control.executionGenerationOf(PIPELINE).orElseThrow();
                Set<String> executionBefore = executionIds(control, PIPELINE);
                assertThat(executionBefore).hasSize(1);
                assertThat(control.awaitingRebalance(PIPELINE)).isEmpty();
                ExecutionPlan frozenPlan;
                try (NativeMemberWitness nativeReader = NativeMemberWitness.connect(
                        clusterId, cluster.processCarrying(TwoMemberCluster.NODE_A))) {
                    frozenPlan = nativeReader.publishedPlan(PIPELINE);
                }
                ClusterProjectionAssertions.assertProjectionFacts(cluster, store, Set.of(PIPELINE), ORIGINAL,
                        originalCommitted, Map.of(INITIAL_THIRD, third), Map.of(PIPELINE, oldCohort));

                try (RealProcessServer fourth = cluster.launching(JOINING)) {
                    cluster.awaitMembers(4);
                    awaitActive(control, CURRENT);
                    Instant joined = Instant.now();
                    var expandedCommitted = ClusterProjectionAssertions.committedFacts(store, clusterId);
                    assertThat(expandedCommitted.nodeIds()).containsExactlyInAnyOrderElementsOf(CURRENT);
                    assertThat(expandedCommitted.revision()).isGreaterThan(originalCommitted.revision());
                    assertThat(expandedCommitted.profileGeneration()).isEqualTo(originalCommitted.profileGeneration());
                    Await.until("both projections to name the late member awaiting rebalance", Duration.ofMinutes(1),
                            () -> control.awaitingRebalance(PIPELINE).equals(List.of(JOINING))
                                    && control.clusterAwaitingRebalance(PIPELINE).equals(List.of(JOINING)),
                            () -> "awaiting = " + control.awaitingRebalance(PIPELINE));
                    Await.until("a reading after the engine's scale-up delay", Duration.ofMinutes(2),
                            () -> control.measuredAt(PIPELINE)
                                    .filter(taken -> taken.isAfter(joined.plus(WELL_PAST_THE_SCALE_UP_DELAY))).isPresent(),
                            () -> "the latest reading was taken at " + control.measuredAt(PIPELINE));

                    Map<String, RealProcessServer> additional = Map.of(INITIAL_THIRD, third, JOINING, fourth);
                    assertThat(executionIds(control, PIPELINE)).isEqualTo(executionBefore);
                    assertThat(control.executionGenerationOf(PIPELINE)).contains(generation);
                    assertThat(control.membersCarryingPartOf(PIPELINE)).containsExactlyInAnyOrderElementsOf(ORIGINAL);
                    assertThat(control.executionPlan(PIPELINE)).contains(before);
                    try (NativeMemberWitness nativeReader = NativeMemberWitness.connect(
                            clusterId, cluster.processCarrying(TwoMemberCluster.NODE_A))) {
                        assertThat(nativeReader.publishedPlan(PIPELINE)).isEqualTo(frozenPlan);
                    }
                    ClusterProjectionAssertions.assertProjectionFacts(cluster, store, Set.of(PIPELINE), CURRENT,
                            expandedCommitted, additional, Map.of(PIPELINE, oldCohort));
                    files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);
                    try {
                        Await.until("a change after the join to cross the old run", Duration.ofMinutes(1),
                                () -> files.count(targetAddress, TABLE) == SEEDED_ROWS + 1,
                                () -> "rows at the old target = " + files.count(targetAddress, TABLE));
                    } catch (AssertionError failure) {
                        var boundedStore = store.withTimeout(2, TimeUnit.SECONDS);
                        StringBuilder facts = new StringBuilder("member-add failure before owned-process cleanup")
                                .append("\noriginal cohort: ").append(oldCohort)
                                .append("\noriginal committed: ").append(originalCommitted)
                                .append("\nexpanded committed: ").append(expandedCommitted)
                                .append("\noriginal published plan: ").append(frozenPlan)
                                .append("\noriginal execution generation: ").append(generation)
                                .append("\noriginal runtime execution ids: ").append(executionBefore);
                        failureRead(facts, "current cluster projection", () -> control.clusterStatus(Duration.ofSeconds(2)));
                        failureRead(facts, "current pipeline state", () -> SystemCollections.PIPELINE_STATE.on(boundedStore)
                                .find(new Document("_id", PIPELINE)).maxTime(2, TimeUnit.SECONDS).first());
                        failureRead(facts, "current desired intent", () -> SystemCollections.PIPELINE_DESIRED.on(boundedStore)
                                .find(new Document("_id", PIPELINE)).maxTime(2, TimeUnit.SECONDS).first());
                        failureRead(facts, "current source definition", () -> SystemCollections.ARTIFACTS.on(boundedStore)
                                .find(new Document("_id", SOURCE)).maxTime(2, TimeUnit.SECONDS).first());
                        failureRead(facts, "claims with Mongo read time and remaining lease", () ->
                                SystemCollections.WORKLOAD_CLAIMS.on(boundedStore).aggregate(List.of(
                                        new Document("$match", new Document("clusterId", clusterId)),
                                        new Document("$addFields", new Document("mongoNow", "$$NOW")
                                                .append("leaseRemainingMillis", new Document("$subtract", List.of("$leaseUntil", "$$NOW"))))))
                                        .maxTime(2, TimeUnit.SECONDS).into(new ArrayList<>()));
                        failureRead(facts, "source consumer offsets", () -> sourceConsumers(boundedStore));
                        failureRead(facts, "source mining metadata", () -> sourceConsumers(boundedStore).stream().map(consumer -> {
                            String chain = consumer.getString("miningChainId");
                            if (chain == null) { throw new AssertionError("the source consumer has no mining chain identity"); }
                            return SystemCollections.SRS_META.on(boundedStore).find(new Document("_id", chain)).maxTime(2, TimeUnit.SECONDS).first();
                        }).toList());
                        failureRead(facts, "source row count", () -> files.count(sourceAddress, TABLE));
                        failureRead(facts, "target row count", () -> files.count(targetAddress, TABLE));
                        failureRead(facts, "source CSV", () -> csvContents(source));
                        failureRead(facts, "target CSV", () -> csvContents(target));
                        failure.addSuppressed(new AssertionError(facts.toString()));
                        throw failure;
                    }
                    assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);

                    List<NativeMemberWitness.MemberFacts> newCohort;
                    try (NativeMemberWitness nativeReader = NativeMemberWitness.connect(
                            clusterId, cluster.processCarrying(TwoMemberCluster.NODE_A))) {
                        newCohort = nativeReader.members();
                    }
                    assertThat(newCohort.stream().map(NativeMemberWitness.MemberFacts::nodeId))
                            .containsExactlyInAnyOrderElementsOf(CURRENT);
                    control.apply(Map.of(SOURCE + ".tap.yml", resources.get(SOURCE + ".tap.yml"),
                            NEW_TARGET + ".tap.yml", Workspaces.targetYaml(NEW_TARGET, laterTarget),
                            NEW_PIPELINE + ".tap.yml", Workspaces.pipelineYaml(NEW_PIPELINE, SOURCE, NEW_TARGET, TABLE, WRITERS)));
                    control.lifecycle(NEW_PIPELINE, LifecycleVerb.START);
                    awaitRunning(control, NEW_PIPELINE);
                    Await.until("the later run's own load to reach its target", Duration.ofMinutes(1),
                            () -> files.count(laterTargetAddress, TABLE) == SEEDED_ROWS + 1,
                            () -> "rows at the later target = " + files.count(laterTargetAddress, TABLE));
                    awaitCompleteContexts(control, NEW_PIPELINE, newCohort);
                    var laterPlan = control.executionPlan(NEW_PIPELINE).orElseThrow();
                    assertThat(laterPlan.members()).containsExactlyInAnyOrderElementsOf(CURRENT);
                    assertThat(laterPlan.nodeRequested(WRITERS).effective()).as("the actual later four-member plan").isEqualTo(8);
                    assertThat(control.awaitingRebalance(NEW_PIPELINE)).isEmpty();
                    ClusterProjectionAssertions.assertProjectionFacts(cluster, store, Set.of(PIPELINE, NEW_PIPELINE), CURRENT,
                            expandedCommitted, additional, Map.of(PIPELINE, oldCohort, NEW_PIPELINE, newCohort));
                    assertThat(executionIds(control, PIPELINE)).isEqualTo(executionBefore);
                    assertThat(control.executionGenerationOf(PIPELINE)).contains(generation);
                    assertThat(control.awaitingRebalance(PIPELINE)).containsExactly(JOINING);
                }
            }
        }
    }

    private static List<Document> sourceConsumers(com.mongodb.client.MongoDatabase boundedStore) {
        return SystemCollections.SRS_CONSUMER_OFFSETS.on(boundedStore)
                .find(new Document("pipelineId", SrsConsumerId.of(PIPELINE, SOURCE).value()))
                .maxTime(2, TimeUnit.SECONDS).into(new ArrayList<>());
    }

    private static String csvContents(Path directory) {
        try {
            return Files.readString(directory.resolve(TABLE + ".csv"));
        } catch (IOException unknown) {
            throw new UncheckedIOException(unknown);
        }
    }

    private static void failureRead(StringBuilder facts, String label, Supplier<?> reading) {
        facts.append("\n").append(label).append(": ");
        try {
            facts.append(reading.get());
        } catch (RuntimeException | AssertionError unknown) {
            facts.append("unreadable (").append(unknown.getClass().getName()).append("): ").append(unknown.getMessage());
        }
    }

    private static void awaitRunning(ControlPlane control, String pipeline) {
        Await.until(pipeline + " to reach RUNNING", Duration.ofMinutes(1),
                () -> control.state(pipeline).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> control.state(pipeline) + ", failure " + control.failure(pipeline));
    }

    private static void awaitActive(ControlPlane control, List<String> nodes) {
        Await.until("the actual native cohort to be committed", Duration.ofMinutes(2),
                () -> control.clusterStatus().members().stream().filter(member -> Boolean.TRUE.equals(member.live())
                                && member.state() == io.tapstate.control.core.ClusterMemberState.ACTIVE)
                        .map(member -> member.nodeId()).collect(Collectors.toSet()).equals(Set.copyOf(nodes)),
                () -> "members = " + control.clusterStatus().members());
    }

    private static void awaitCompleteContexts(ControlPlane control, String pipelineId,
            List<NativeMemberWitness.MemberFacts> cohort) {
        Await.until("complete actual processor contexts for " + pipelineId, Duration.ofMinutes(1), () -> {
            var pipeline = control.clusterStatus().pipelines().stream()
                    .filter(row -> row.pipelineId().equals(pipelineId)).findFirst().orElse(null);
            return pipeline != null && pipeline.measuredFrom().containsAll(cohort.stream().map(NativeMemberWitness.MemberFacts::uuid).toList())
                    && !pipeline.vertices().isEmpty() && pipeline.vertices().stream().allMatch(vertex -> !vertex.processors().isEmpty()
                    && vertex.processors().stream().allMatch(processor -> processor.context() != null)
                    && (vertex.computedLocal() == null || vertex.processors().stream()
                            .allMatch(processor -> processor.context().totalParallelism() == vertex.processors().size())));
        }, () -> "measured from " + control.membersMeasuring(pipelineId) + "; vertices=" + control.placedVertices(pipelineId));
    }

    private static Set<String> executionIds(ControlPlane control, String pipeline) {
        return control.placedVertices(pipeline).stream().map(ControlPlane.PlacedVertex::executionId)
                .filter(Objects::nonNull).collect(Collectors.toCollection(TreeSet::new));
    }
}
