package io.tapstate.app;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoArtifactStore;
import io.tapstate.adapters.mongostore.MongoClusterMembershipStore;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStateStore;
import io.tapstate.adapters.mongostore.MongoWorkloadClaimStore;
import io.tapstate.control.core.PipelineExplanation.PendingReason;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.CurrentObservationReader;
import io.tapstate.control.core.MonitorError;
import io.tapstate.control.core.PipelineExplainService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.StorePort;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real durable ownership and binding, with a controlled data actuator outside the pending-label boundary. */
@RequiresDocker
class ClaimedTerminalNoopPendingMongoIT {
    private static final String CLUSTER = "claimed-pending-cluster";
    private static final String PIPELINE = "orders";
    private static final String SLOW = "slow";
    private static final WorkloadOwner CLIENT = new WorkloadOwner("node-a", "pending-fixture-client-a");
    private static final Set<String> MEMBERS = Set.of("node-a", "node-b");
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration WAIT = Duration.ofSeconds(5);

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    private static Stream<Object[]> terminalCases() {
        return Stream.of(new Object[] { PipelineState.FAILED, false }, new Object[] { PipelineState.FAILED, true },
                new Object[] { PipelineState.COMPLETED, false }, new Object[] { PipelineState.COMPLETED, true });
    }

    @ParameterizedTest(name = "terminal={0}, full queue={1}")
    @MethodSource("terminalCases")
    void aClaimedTerminalDecisionDoesNotBecomeAQueuedOrCapacityStart(PipelineState terminal, boolean fillQueue)
            throws Exception {
        try (Fixture fixture = new Fixture(terminal)) {
            fixture.observeActualTerminalDecision();
            WorkloadClaim before = fixture.currentClaim();
            CheckpointDoc checkpoint = fixture.state.read(PIPELINE).orElseThrow();
            fixture.occupyWorker(fillQueue);

            fixture.driver.reconcile();

            PendingReason raw = fillQueue ? PendingReason.START_CAPACITY : PendingReason.START_PENDING;
            assertThat(fixture.pending.pending(PIPELINE).orElseThrow().reason()).isEqualTo(raw);
            assertThat(fixture.pending.capacityCount()).isEqualTo(fillQueue ? 1 : 0);
            assertThat(fixture.explain.explain(PIPELINE).pending())
                    .as("the public projection of the same actual claimed terminal NOOP is not a requested start")
                    .isNull();
            assertThat(fixture.pending.pending(PIPELINE).orElseThrow().reason()).isEqualTo(raw);
            assertThat(fixture.pending.capacityCount()).as("a read does not erase refused local work").isEqualTo(fillQueue ? 1 : 0);
            assertThat(fixture.state.read(PIPELINE)).contains(checkpoint);
            assertThat(fixture.desired.read(PIPELINE)).contains(fixture.intent);
            assertThat(WorkloadClaimFence.from(fixture.currentClaim())).isEqualTo(WorkloadClaimFence.from(before));
            assertThat(fixture.submissions).hasValue(1);
            assertThat(fixture.work.activeCount()).isEqualTo(2);
            assertThat(fixture.work.health().activeSlots()).isEqualTo(1);
        }
    }

    @Test
    void aNewFullIntentStillKeepsItsQueuedPendingAfterAClaimedTerminalDecision() throws Exception {
        verifyChangedBoundary(Change.INTENT);
    }

    @Test
    void aRebuildStampStillKeepsItsQueuedPendingAfterAClaimedTerminalDecision() throws Exception {
        verifyChangedBoundary(Change.REBUILD);
    }

    @Test
    void aReacquiredActualClaimStillKeepsItsQueuedPendingAfterAClaimedTerminalDecision() throws Exception {
        verifyChangedBoundary(Change.CLAIM);
    }

    @Test
    void anUnknownBindingStillKeepsItsQueuedPendingAfterAClaimedTerminalDecision() throws Exception {
        try (Fixture fixture = new Fixture(PipelineState.FAILED)) {
            fixture.scopes.discard(PIPELINE, fixture.scopes.current(PIPELINE).orElseThrow());
            fixture.observeActualTerminalDecision(false);
            WorkloadClaim before = fixture.currentClaim();
            CheckpointDoc checkpoint = fixture.state.read(PIPELINE).orElseThrow();
            fixture.occupyWorker(false);

            fixture.driver.reconcile();

            assertThat(fixture.pending.pending(PIPELINE).orElseThrow().reason()).isEqualTo(PendingReason.START_PENDING);
            assertThat(fixture.scopes.bindingIdentity(PIPELINE).known()).isFalse();
            assertThatThrownBy(() -> fixture.explain.explain(PIPELINE)).isInstanceOfSatisfying(TapstateException.class,
                    refusal -> assertThat(refusal.code()).isEqualTo(MonitorError.NO_OBSERVATION));
            assertThat(fixture.state.read(PIPELINE)).contains(checkpoint);
            assertThat(fixture.desired.read(PIPELINE)).contains(fixture.intent);
            assertThat(WorkloadClaimFence.from(fixture.currentClaim())).isEqualTo(WorkloadClaimFence.from(before));
            assertThat(fixture.submissions).hasValue(1);
        }
    }

    @Test
    void aRemotelyRecreatedArtifactStillKeepsItsQueuedPendingBeforeLocalInvalidation() throws Exception {
        try (Fixture fixture = new Fixture(PipelineState.FAILED)) {
            fixture.observeActualTerminalDecision();
            WorkloadClaim before = fixture.currentClaim();
            var oldScope = fixture.scopes.current(PIPELINE).orElseThrow();
            var oldBinding = fixture.scopes.bindingIdentity(PIPELINE);
            fixture.occupyWorker(false);
            var resource = fixture.artifacts.get(PIPELINE).orElseThrow();
            assertThat(fixture.artifacts.delete(PIPELINE, CanonicalHash.of(resource))).isEqualTo(ArtifactMutation.DELETED);
            fixture.state.delete(PIPELINE);
            assertThat(fixture.artifacts.create(resource)).isEqualTo(ArtifactMutation.CREATED);
            String recreated = fixture.artifacts.pipelineIncarnationId(PIPELINE).orElseThrow();
            assertThat(recreated).isNotEqualTo(oldScope.pipelineIncarnationId());
            assertThat(fixture.scopes.current(PIPELINE)).contains(oldScope);
            assertThat(fixture.scopes.bindingIdentity(PIPELINE)).isEqualTo(oldBinding);
            assertThat(WorkloadClaimFence.from(fixture.currentClaim())).isEqualTo(WorkloadClaimFence.from(before));
            assertThat(fixture.desired.read(PIPELINE)).contains(fixture.intent);
            assertThat(fixture.state.read(PIPELINE)).isEmpty();

            fixture.driver.reconcile();

            assertThat(fixture.pending.pending(PIPELINE).orElseThrow().reason()).isEqualTo(PendingReason.START_PENDING);
            assertThat(fixture.scopes.bindingIdentity(PIPELINE)).isEqualTo(oldBinding);
            assertThat(fixture.artifacts.pipelineIncarnationId(PIPELINE)).contains(recreated);
            assertThatThrownBy(() -> fixture.explain.explain(PIPELINE)).isInstanceOfSatisfying(TapstateException.class,
                    refusal -> assertThat(refusal.code()).isEqualTo(MonitorError.NO_OBSERVATION));
            assertThat(fixture.state.read(PIPELINE)).isEmpty();
            assertThat(WorkloadClaimFence.from(fixture.currentClaim())).isEqualTo(WorkloadClaimFence.from(before));
            assertThat(fixture.submissions).hasValue(1);
        }
    }

    @Test
    void aRemoteRecreateAfterTheCurrentObservationReadCannotHideTheRawStartLabel() throws Exception {
        try (Fixture fixture = new Fixture(PipelineState.FAILED)) {
            fixture.observeActualTerminalDecision();
            WorkloadClaim before = fixture.currentClaim();
            var binding = fixture.scopes.bindingIdentity(PIPELINE);
            String oldIncarnation = fixture.artifacts.pipelineIncarnationId(PIPELINE).orElseThrow();
            fixture.occupyWorker(false);
            fixture.driver.reconcile();
            assertThat(fixture.pending.pending(PIPELINE).orElseThrow().reason()).isEqualTo(PendingReason.START_PENDING);
            AtomicReference<String> recreated = new AtomicReference<>();
            doAnswer(invocation -> {
                Object observed = invocation.callRealMethod();
                assertThat((Optional<?>) observed).isPresent();
                var resource = fixture.artifacts.get(PIPELINE).orElseThrow();
                assertThat(fixture.artifacts.delete(PIPELINE, CanonicalHash.of(resource))).isEqualTo(ArtifactMutation.DELETED);
                fixture.state.delete(PIPELINE);
                assertThat(fixture.artifacts.create(resource)).isEqualTo(ArtifactMutation.CREATED);
                recreated.set(fixture.artifacts.pipelineIncarnationId(PIPELINE).orElseThrow());
                assertThat(recreated.get()).isNotEqualTo(oldIncarnation);
                return observed;
            }).when(fixture.current).read(PIPELINE);

            assertThat(fixture.explain.explain(PIPELINE).pending().reason()).isEqualTo(PendingReason.START_PENDING);

            assertThat(fixture.scopes.bindingIdentity(PIPELINE)).isEqualTo(binding);
            assertThat(fixture.artifacts.pipelineIncarnationId(PIPELINE)).contains(recreated.get());
            assertThat(fixture.state.read(PIPELINE)).isEmpty();
            assertThat(WorkloadClaimFence.from(fixture.currentClaim())).isEqualTo(WorkloadClaimFence.from(before));
            assertThat(fixture.submissions).hasValue(1);
        }
    }

    @Test
    void aChangedVisibleMembershipAfterQueueingCannotHideTheRawStartLabel() throws Exception {
        try (Fixture fixture = new Fixture(PipelineState.FAILED)) {
            fixture.observeActualTerminalDecision();
            fixture.occupyWorker(false);
            fixture.driver.reconcile();
            assertThat(fixture.pending.pending(PIPELINE).orElseThrow().reason()).isEqualTo(PendingReason.START_PENDING);
            fixture.gate.canCommit(Set.of("node-b"));

            assertThat(fixture.explain.explain(PIPELINE).pending().reason()).isEqualTo(PendingReason.START_PENDING);
            assertThat(fixture.pending.pending(PIPELINE).orElseThrow().reason()).isEqualTo(PendingReason.START_PENDING);
            assertThat(fixture.submissions).hasValue(1);
        }
    }

    private static Stream<RuntimeException> coldProofFailures() {
        return Stream.of(new TapstateException(io.tapstate.spi.store.IoError.STORE_UNAVAILABLE,
                        Map.of("detail", "controlled cold membership proof outage"),
                        new java.io.IOException("controlled cold membership proof outage")),
                new com.mongodb.MongoException("controlled native membership read outage"));
    }

    @ParameterizedTest(name = "cold proof failure: {0}")
    @MethodSource("coldProofFailures")
    void aColdProofReadFailureKeepsThePublicAndRawQueuedStart(RuntimeException unavailable) throws Exception {
        try (Fixture fixture = new Fixture(PipelineState.FAILED)) {
            fixture.observeActualTerminalDecision();
            WorkloadClaim before = fixture.currentClaim();
            CheckpointDoc checkpoint = fixture.state.read(PIPELINE).orElseThrow();
            fixture.occupyWorker(false);
            fixture.driver.reconcile();
            var dispatch = fixture.work.health();
            int accepted = fixture.work.activeCount();
            var identity = fixture.work.currentIdentity(PIPELINE, fixture.intent);
            assertThat(identity).isNotNull();
            AtomicInteger coldReads = new AtomicInteger();
            doAnswer(invocation -> {
                Object observed = invocation.callRealMethod();
                assertThat((Optional<?>) observed).isEqualTo(Optional.of(fixture.gate.committed()));
                coldReads.incrementAndGet();
                throw unavailable;
            }).when(fixture.membershipStore).read(CLUSTER);

            assertThat(fixture.explain.explain(PIPELINE).pending().reason()).isEqualTo(PendingReason.START_PENDING);

            assertThat(coldReads).as("the public factory crossed the real cold Mongo membership read").hasValue(1);
            assertThat(fixture.pending.pending(PIPELINE).orElseThrow().reason()).isEqualTo(PendingReason.START_PENDING);
            assertThat(fixture.pending.capacityCount()).isZero();
            assertThat(fixture.work.health()).isEqualTo(dispatch);
            assertThat(fixture.work.activeCount()).isEqualTo(accepted);
            assertThat(fixture.work.currentIdentity(PIPELINE, fixture.intent)).isSameAs(identity);
            assertThat(fixture.state.read(PIPELINE)).contains(checkpoint);
            assertThat(fixture.desired.read(PIPELINE)).contains(fixture.intent);
            assertThat(WorkloadClaimFence.from(fixture.currentClaim())).isEqualTo(WorkloadClaimFence.from(before));
            assertThat(fixture.submissions).hasValue(1);
        }
    }

    @Test
    void aDurableMembershipChangeNotYetInstalledLocallyKeepsThePublicRawStart() throws Exception {
        try (Fixture fixture = new Fixture(PipelineState.FAILED)) {
            fixture.observeActualTerminalDecision();
            WorkloadClaim before = fixture.currentClaim();
            CheckpointDoc checkpoint = fixture.state.read(PIPELINE).orElseThrow();
            fixture.occupyWorker(false);
            fixture.driver.reconcile();
            var dispatch = fixture.work.health();
            int accepted = fixture.work.activeCount();
            var identity = fixture.work.currentIdentity(PIPELINE, fixture.intent);
            assertThat(identity).isNotNull();
            var committed = fixture.gate.committed();
            var changed = fixture.membershipStore.compareAndSet(CLUSTER, committed.revision(), MEMBERS).orElseThrow();
            assertThat(changed.revision()).isGreaterThan(committed.revision());
            assertThat(fixture.membershipStore.read(CLUSTER)).contains(changed);
            assertThat(fixture.gate.committed()).isEqualTo(committed);
            assertThat(fixture.gate.businessEligible()).isTrue();

            assertThat(fixture.explain.explain(PIPELINE).pending().reason()).isEqualTo(PendingReason.START_PENDING);

            assertThat(fixture.pending.pending(PIPELINE).orElseThrow().reason()).isEqualTo(PendingReason.START_PENDING);
            assertThat(fixture.pending.capacityCount()).isZero();
            assertThat(fixture.work.health()).isEqualTo(dispatch);
            assertThat(fixture.work.activeCount()).isEqualTo(accepted);
            assertThat(fixture.work.currentIdentity(PIPELINE, fixture.intent)).isSameAs(identity);
            assertThat(fixture.state.read(PIPELINE)).contains(checkpoint);
            assertThat(fixture.desired.read(PIPELINE)).contains(fixture.intent);
            assertThat(WorkloadClaimFence.from(fixture.currentClaim())).isEqualTo(WorkloadClaimFence.from(before));
            assertThat(fixture.submissions).hasValue(1);
        }
    }

    @Test
    void aNewActualDecisionAndQueuedEntryCannotBeHiddenByTheOlderCompletedProof() throws Exception {
        CountDownLatch replacementEntered = new CountDownLatch(1);
        CountDownLatch releaseReplacement = new CountDownLatch(1);
        try (Fixture fixture = new Fixture(PipelineState.FAILED)) {
            fixture.observeActualTerminalDecision();
            WorkloadClaim before = fixture.currentClaim();
            CheckpointDoc checkpoint = fixture.state.read(PIPELINE).orElseThrow();
            PipelineConverger.PendingDecision previousDecision = fixture.actualDecision.get();
            fixture.occupyWorker(false);
            fixture.driver.reconcile();
            var originalProjection = fixture.pending.projection(PIPELINE);
            assertThat(originalProjection.pending().reason()).isEqualTo(PendingReason.START_PENDING);
            var originalDispatch = fixture.work.health();
            Object originalAccepted = fixture.work.currentIdentity(PIPELINE, fixture.intent);
            assertThat(originalAccepted).isNotNull();
            WorkloadClaimKey nodeKey = new WorkloadClaimKey(CLUSTER, WorkloadClaimType.NODE_SESSION, CLIENT.nodeId());
            AtomicInteger nativeReads = new AtomicInteger();
            AtomicReference<LifecyclePendingRegistry.Projection> replacementProjection = new AtomicReference<>();
            AtomicReference<LifecycleWorkDispatcher.Health> replacementDispatch = new AtomicReference<>();
            AtomicReference<Object> replacementAccepted = new AtomicReference<>();
            AtomicInteger replacementWorkCount = new AtomicInteger();
            doAnswer(invocation -> {
                Object observed = invocation.callRealMethod();
                assertThat((Optional<?>) observed).isPresent();
                if (nativeReads.incrementAndGet() == 2) {
                    // The second proof has already read its final native row. Let the actual queued worker decide.
                    fixture.releaseSlow.countDown();
                    awaitProjectionTransition(() -> fixture.actualDecision.get() != previousDecision
                                    && fixture.work.health().pendingPipelines() == 0
                                    && fixture.work.health().activeSlots() == 0
                                    && fixture.work.health().queueDepth() == 0
                                    && fixture.pending.pending(PIPELINE).isEmpty(),
                            "the actual queued terminal decision did not complete");
                    assertThat(fixture.actualDecision.get().action()).isEqualTo(PipelineConverger.PendingAction.NONE);
                    assertThat(fixture.actualDecision.get().intent()).isEqualTo(fixture.intent);
                    assertThat(fixture.actualDecision.get().checkpoint()).contains(checkpoint);
                    assertThat(fixture.pending.projection(PIPELINE).identity()).isNotSameAs(originalProjection.identity());
                    assertThat(fixture.state.read(PIPELINE)).contains(checkpoint);
                    assertThat(WorkloadClaimFence.from(fixture.currentClaim())).isEqualTo(WorkloadClaimFence.from(before));
                    assertThat(fixture.submissions).hasValue(1);

                    // Receive the actual finished work, then occupy the same bounded dispatcher for a fresh raw queue.
                    assertThat(fixture.work.take(PIPELINE)).isNotNull();
                    assertThat(fixture.work.take(SLOW)).isNotNull();
                    DesiredState slowIntent = fixture.desired.read(SLOW).orElseThrow();
                    assertThat(fixture.work.offer(SLOW, slowIntent, () -> {
                        replacementEntered.countDown();
                        Fixture.await(releaseReplacement, "the replacement occupying work was not released");
                        return fixture.loop.converge(SLOW);
                    })).isEqualTo(LifecycleWorkDispatcher.Submission.ACCEPTED);
                    assertThat(replacementEntered.await(WAIT.toNanos(), TimeUnit.NANOSECONDS)).isTrue();
                    fixture.driver.reconcile();
                    var fresh = fixture.pending.projection(PIPELINE);
                    assertThat(fresh.pending().reason()).isEqualTo(PendingReason.START_PENDING);
                    assertThat(fresh.identity()).isNotSameAs(originalProjection.identity());
                    Object accepted = fixture.work.currentIdentity(PIPELINE, fixture.intent);
                    assertThat(accepted).isNotNull().isNotSameAs(originalAccepted);
                    replacementProjection.set(fresh);
                    replacementAccepted.set(accepted);
                    replacementDispatch.set(fixture.work.health());
                    replacementWorkCount.set(fixture.work.activeCount());
                }
                return observed;
            }).when(fixture.store).read(nodeKey);

            try {
                assertThat(fixture.explain.explain(PIPELINE).pending().reason()).isEqualTo(PendingReason.START_PENDING);

                assertThat(nativeReads).as("two complete native proof passes preceded the actual worker transition").hasValue(2);
                assertThat(replacementProjection.get()).isNotNull();
                assertThat(fixture.pending.projection(PIPELINE).identity()).isSameAs(replacementProjection.get().identity());
                assertThat(fixture.pending.pending(PIPELINE).orElseThrow().reason()).isEqualTo(PendingReason.START_PENDING);
                assertThat(fixture.pending.capacityCount()).isZero();
                assertThat(fixture.work.health()).isEqualTo(replacementDispatch.get());
                assertThat(fixture.work.activeCount()).isEqualTo(replacementWorkCount.get());
                assertThat(fixture.work.currentIdentity(PIPELINE, fixture.intent)).isSameAs(replacementAccepted.get());
                assertThat(originalDispatch.activeSlots()).isEqualTo(1);
                assertThat(replacementDispatch.get().activeSlots()).isEqualTo(1);
                assertThat(fixture.state.read(PIPELINE)).contains(checkpoint);
                assertThat(fixture.desired.read(PIPELINE)).contains(fixture.intent);
                assertThat(WorkloadClaimFence.from(fixture.currentClaim())).isEqualTo(WorkloadClaimFence.from(before));
                assertThat(fixture.submissions).hasValue(1);
            } finally { releaseReplacement.countDown(); }
        } finally { releaseReplacement.countDown(); }
    }

    private static void awaitProjectionTransition(java.util.function.BooleanSupplier ready, String detail)
            throws InterruptedException {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!ready.getAsBoolean() && System.nanoTime() - deadline < 0) { Thread.sleep(10); }
        assertThat(ready.getAsBoolean()).as(detail).isTrue();
    }

    private enum Change { INTENT, REBUILD, CLAIM }

    private static void verifyChangedBoundary(Change change) throws Exception {
        try (Fixture fixture = new Fixture(PipelineState.FAILED)) {
            fixture.observeActualTerminalDecision();
            WorkloadClaim before = fixture.currentClaim();
            fixture.occupyWorker(false);
            if (change == Change.INTENT) {
                fixture.desired.save(new DesiredState(PIPELINE, PipelineState.RUNNING, "new-revision", false,
                        fixture.intent.assemblyRevision(), false, null));
            } else if (change == Change.REBUILD) {
                fixture.desired.save(new DesiredState(PIPELINE, PipelineState.RUNNING, fixture.intent.revision(), false,
                        fixture.intent.assemblyRevision(), true, fixture.state.read(PIPELINE).orElseThrow().epoch()));
            } else {
                var membership = fixture.membershipStore.compareAndSet(CLUSTER, before.topologyRevision(), MEMBERS)
                        .orElseThrow();
                fixture.gate.install(membership);
                assertThat(fixture.gate.canCommit(MEMBERS)).isTrue();
                fixture.ownership.retain(List.of(SLOW));
                assertThat(fixture.ownership.permit(PIPELINE).granted()).isTrue();
                WorkloadClaim after = fixture.currentClaim();
                assertThat(after.topologyRevision()).isEqualTo(membership.revision())
                        .isGreaterThan(before.topologyRevision());
                assertThat(after.key()).isEqualTo(before.key());
                assertThat(after.owner()).isEqualTo(before.owner());
                assertThat(after.claimGeneration()).isEqualTo(before.claimGeneration());
                assertThat(after.executionGeneration()).isEqualTo(before.executionGeneration());
                assertThat(WorkloadClaimFence.from(after)).isNotEqualTo(WorkloadClaimFence.from(before));
            }

            fixture.driver.reconcile();

            assertThat(fixture.pending.pending(PIPELINE).orElseThrow().reason()).isEqualTo(PendingReason.START_PENDING);
            assertThat(fixture.explain.explain(PIPELINE).pending().reason()).isEqualTo(PendingReason.START_PENDING);
            assertThat(fixture.submissions).as("provisional projection does not admit or submit another execution")
                    .hasValue(1);
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl());
        private final InMemoryDesiredStore desired = new InMemoryDesiredStore();
        private final ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        private final CountDownLatch slowEntered = new CountDownLatch(1);
        private final CountDownLatch releaseSlow = new CountDownLatch(1);
        private final CountDownLatch decided = new CountDownLatch(1);
        private final AtomicInteger submissions = new AtomicInteger();
        private final AtomicReference<PipelineConverger.PendingDecision> actualDecision = new AtomicReference<>();
        private final AtomicReference<LifecyclePendingRegistry.Context> actualContext = new AtomicReference<>();
        private final DesiredState intent = new DesiredState(PIPELINE, PipelineState.RUNNING, "revision-1", false,
                "assembly-1", false, null);
        private final WorkloadClaimKey key = new WorkloadClaimKey(CLUSTER, WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE);
        private final LifecycleWorkDispatcher work = new LifecycleWorkDispatcher(1, 1);
        private final LifecyclePendingRegistry pending = spy(new LifecyclePendingRegistry());
        private final MongoStateStore state;
        private final MongoArtifactStore artifacts;
        private final MongoWorkloadClaimStore store;
        private final MongoClusterMembershipStore membershipStore;
        private final ClusterMembershipGate gate;
        private final PipelineActuationOwnership ownership;
        private final PipelineConverger loop;
        private final ConvergenceDriver driver;
        private final CurrentObservationReader current;
        private final PipelineExplainService explain;

        private Fixture(PipelineState terminal) throws Exception {
            var database = client.getDatabase("claimed_terminal_pending_" + System.nanoTime());
            store = spy(new MongoWorkloadClaimStore(database.getCollection("workload_claims")));
            state = new MongoStateStore(database.getCollection("pipeline_state"));
            artifacts = new MongoArtifactStore(client, database.getCollection("artifacts"));
            artifacts.create(new DslParser().parse("""
                    version: tapstate/v1
                    kind: pipeline
                    id: orders
                    source: input
                    serve:
                      from: orders
                      sync: [ { source: target } ]
                    """));
            membershipStore = spy(new MongoClusterMembershipStore(database.getCollection("membership")));
            var membership = membershipStore.createIfAbsent(CLUSTER, MEMBERS);
            ClusterProperties properties = new ClusterProperties();
            properties.setId(CLUSTER);
            properties.setNodeId(CLIENT.nodeId());
            properties.setProfile(ClusterProperties.Profile.PROCESS_FAILURE_ONLY);
            properties.setBootstrapMinMembers(2);
            gate = new ClusterMembershipGate(properties);
            gate.install(membership);
            assertThat(gate.canCommit(MEMBERS)).isTrue();
            assertThat(gate.businessEligible()).isTrue();
            WorkloadClaimKey nodeKey = new WorkloadClaimKey(CLUSTER, WorkloadClaimType.NODE_SESSION, CLIENT.nodeId());
            var node = store.acquire(nodeKey, CLIENT, membership.revision(), TTL);
            assertThat(node.acquired()).isTrue();
            assertThat(store.read(nodeKey).orElseThrow().claim().owner()).isEqualTo(CLIENT);
            ownership = new PipelineActuationOwnership(CLUSTER, CLIENT, gate, new ClusterWorkloadClaims(store, gate),
                    TTL, Duration.ofSeconds(10));
            assertThat(ownership.permit(PIPELINE).granted()).isTrue();
            String incarnation = artifacts.pipelineIncarnationId(PIPELINE).orElseThrow();
            LifecycleActuator actuator = new LifecycleActuator() {
                @Override public PreparedStart prepareStart(String id) {
                    if (id.equals(SLOW)) {
                        slowEntered.countDown();
                        await(releaseSlow, "the occupying start was not released");
                    }
                    var execution = ownership.beginExecution(id);
                    assertThat(execution.allowed()).isTrue();
                    if (id.equals(PIPELINE)) {
                        assertThat(execution.admittedClaim()).isPresent();
                        assertThat(execution.fence().executionGeneration())
                                .isEqualTo(currentClaim().executionGeneration()).isPositive();
                        scopes.begin(id, incarnation, execution.fence().executionGeneration());
                    }
                    return new PreparedStart() {
                        @Override public void submit() { if (id.equals(PIPELINE)) { submissions.incrementAndGet(); } }
                        @Override public void close() { }
                    };
                }
                @Override public void start(String id) { throw new AssertionError("the actual preparation seam must admit starts"); }
                @Override public void pause(String id) { }
                @Override public void resume(String id) { }
                @Override public void stop(String id, boolean purge) { }
                @Override public Optional<Throwable> failure(String id) { return Optional.empty(); }
                @Override public Optional<Throwable> lost(String id) { return Optional.empty(); }
                @Override public boolean isCarryingAJob(String id) { return true; }
            };
            loop = new PipelineConverger(desired, state, actuator, Clock.systemUTC());
            desired.save(intent);
            assertThat(loop.converge(PIPELINE).checkpoint()).isPresent();
            assertThat(submissions).hasValue(1);
            CheckpointDoc running = state.read(PIPELINE).orElseThrow();
            state.compareAndSwap(PIPELINE, running.epoch(), StateJson.of(terminal), Instant.now());
            assertThat(state.read(PIPELINE).map(doc -> StateJson.parse(doc.stateJson()))).contains(terminal);
            var latest = new MongoObservationStore(client, database.getCollection("observation"),
                    database.getCollection("observation_chunks"));
            driver = new ConvergenceDriver(loop, desired, new ObservationPublisher(state, latest), null,
                    MetricsExport.none(), gate::businessEligible, ownership, work, scopes, null, pending);
            current = spy(new CurrentObservationReader(artifacts, store, latest, CLUSTER));
            StorePort ports = mock(StorePort.class);
            when(ports.artifacts()).thenReturn(artifacts);
            when(ports.workloadClaims()).thenReturn(store);
            when(ports.state()).thenReturn(state);
            when(ports.desired()).thenReturn(desired);
            when(ports.clusterMembership()).thenReturn(membershipStore);
            when(ports.observations()).thenReturn(latest);
            // Resolve the actual production factory on either side of its internal injection change.
            DefaultListableBeanFactory factory = new DefaultListableBeanFactory();
            factory.registerSingleton("controlPlane", new ControlPlaneConfiguration());
            factory.registerSingleton("artifacts", new ArtifactQueryService(artifacts));
            factory.registerSingleton("observations", current);
            factory.registerSingleton("clock", Clock.systemUTC());
            factory.registerSingleton("pending", pending);
            factory.registerSingleton("store", ports);
            factory.registerSingleton("scopes", scopes);
            factory.registerSingleton("membership", gate);
            factory.registerSingleton("cluster", properties);
            factory.registerSingleton("identities", mock(ClusterIdentityStore.class));
            RootBeanDefinition definition = new RootBeanDefinition(PipelineExplainService.class);
            definition.setFactoryBeanName("controlPlane");
            definition.setFactoryMethodName("pipelineExplainService");
            definition.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR);
            factory.registerBeanDefinition("explain", definition);
            explain = factory.getBean("explain", PipelineExplainService.class);
            doAnswer(invocation -> {
                PipelineConverger.PendingDecision decision = invocation.getArgument(0);
                if (decision.intent().pipelineId().equals(PIPELINE)
                        && decision.action() == PipelineConverger.PendingAction.NONE) {
                    actualDecision.set(decision);
                    actualContext.set(invocation.getArgument(1));
                    Object result = invocation.callRealMethod();
                    decided.countDown();
                    return result;
                }
                return invocation.callRealMethod();
            }).when(pending).decided(any(), any(), any());
        }

        private WorkloadClaim currentClaim() {
            var reading = store.read(key).orElseThrow();
            assertThat(reading.leased()).isTrue();
            return reading.claim();
        }

        private void observeActualTerminalDecision() throws Exception {
            observeActualTerminalDecision(true);
        }

        private void observeActualTerminalDecision(boolean knownBinding) throws Exception {
            driver.reconcile();
            assertThat(decided.await(WAIT.toNanos(), TimeUnit.NANOSECONDS)).isTrue();
            long deadline = System.nanoTime() + WAIT.toNanos();
            while (work.health().pendingPipelines() != 0 && System.nanoTime() - deadline < 0) { Thread.sleep(10); }
            assertThat(work.health().pendingPipelines()).isZero();
            if (work.activeCount() != 0) { driver.reconcile(); }
            assertThat(work.activeCount()).isZero();
            assertThat(pending.pending(PIPELINE)).isEmpty();
            assertThat(actualDecision.get().intent()).isEqualTo(intent);
            assertThat(actualDecision.get().checkpoint()).isEqualTo(state.read(PIPELINE));
            WorkloadClaim actual = currentClaim();
            var context = actualContext.get();
            assertThat(context.owner()).isNotNull();
            assertThat(context.owner().key()).isEqualTo(actual.key());
            assertThat(context.owner().owner()).isEqualTo(actual.owner());
            assertThat(context.owner().claimGeneration()).isEqualTo(actual.claimGeneration());
            assertThat(context.owner().executionGeneration()).isEqualTo(actual.executionGeneration());
            assertThat(context.owner().topologyRevision()).isEqualTo(actual.topologyRevision());
            assertThat(context.binding()).isEqualTo(scopes.bindingIdentity(PIPELINE));
            assertThat(context.binding().known()).isEqualTo(knownBinding);
        }

        private void occupyWorker(boolean fillQueue) throws Exception {
            DesiredState slow = new DesiredState(SLOW, PipelineState.RUNNING, "slow-revision");
            desired.save(slow);
            assertThat(ownership.permit(SLOW).granted()).isTrue();
            assertThat(work.offer(SLOW, slow, () -> loop.converge(SLOW)))
                    .isEqualTo(LifecycleWorkDispatcher.Submission.ACCEPTED);
            assertThat(slowEntered.await(WAIT.toNanos(), TimeUnit.NANOSECONDS)).isTrue();
            if (fillQueue) {
                DesiredState queued = new DesiredState("queued", PipelineState.RUNNING, "queued-revision");
                desired.save(queued);
                assertThat(ownership.permit("queued").granted()).isTrue();
                assertThat(work.offer("queued", queued, () -> loop.converge("queued")))
                        .isEqualTo(LifecycleWorkDispatcher.Submission.ACCEPTED);
            }
        }

        @Override public void close() {
            releaseSlow.countDown();
            try { work.close(); }
            finally {
                try { ownership.stopForShutdown(); ownership.retain(List.of()); }
                finally { client.close(); }
            }
        }

        private static void await(CountDownLatch latch, String detail) {
            try {
                if (!latch.await(WAIT.toNanos(), TimeUnit.NANOSECONDS)) { throw new AssertionError(detail); }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt(); throw new AssertionError(detail, interrupted);
            }
        }
    }
}
