package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.engine.PreparesTargets;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * What a member is allowed to send outside the cluster on a run's behalf, answered from that member's own
 * reading of the run's generations rather than from a store round trip per batch.
 */
class ExecutionAuthorizationTest {

    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration RENEW = Duration.ofSeconds(10);
    private static final Duration WINDOW = Duration.ofSeconds(10);
    private static final WorkloadOwner NODE_A = new WorkloadOwner("node-a", "boot-a");
    private static final WorkloadOwner NODE_B = new WorkloadOwner("node-b", "boot-b");

    private final InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
    private final ClusterMembershipGate membership = eligibleGate();
    private final AtomicLong nanos = new AtomicLong();

    @Test
    void everySubmissionTakesTheNextExecutionGenerationEvenWhenNobodyElseTookTheClaim() {
        PipelineActuationOwnership nodeA = ownership(NODE_A);
        assertThat(nodeA.permit("orders").granted()).isTrue();

        ExecutionFence first = nodeA.beginExecution("orders").fence();
        ExecutionFence second = nodeA.beginExecution("orders").fence();

        assertThat(first.executionGeneration()).isEqualTo(1);
        assertThat(second.executionGeneration())
                .as("the same holder resubmitting is still a different run").isEqualTo(2);
        assertThat(second.claimGeneration())
                .as("and it is not a change of ownership").isEqualTo(first.claimGeneration());
    }

    @Test
    void aMemberThatNoLongerHoldsThePipelineIsRefusedARunToSubmit() {
        PipelineActuationOwnership nodeA = ownership(NODE_A);
        assertThat(nodeA.permit("orders").granted()).isTrue();

        // The lease ran out and the next member took it over.
        claims.elapse(TTL.plusSeconds(1));
        assertThat(ownership(NODE_B).permit("orders").granted()).isTrue();

        PipelineActuationOwnership.Execution refused = nodeA.beginExecution("orders");

        assertThat(refused.allowed()).isFalse();
        assertThat(refused.fence()).isNull();
    }

    @Test
    void aRunWhoseGenerationsAreStillCurrentKeepsWritingAndAcknowledging() {
        ExecutionFence fence = submittedRun();
        CountingWriter target = new CountingWriter();
        AtomicInteger acked = new AtomicInteger();
        ExecutionAuthorization guard = guard(claims);

        SinkWriter writer = FencedSinkWriterFactory.guarded(target, fence, guard);
        SinkAck ack = FencedSinkAckFactory.guarded((chain, position) -> acked.incrementAndGet(), fence, guard);
        writer.write(List.of());
        writer.write(List.of());
        ack.advance("orders", new ChainPosition(new SourceOrder(1, 1), "w1"));

        assertThat(target.batches).hasValue(2);
        assertThat(acked).hasValue(1);
    }

    @Test
    void aSinkFailureOnAnotherMemberMarksItsRunBeforeTheFailedWriteIsReported() {
        ExecutionFence fence = submittedRun();
        CompletableFuture<WriteResult> write = new CompletableFuture<>();
        SinkWriter sink = new SinkWriter() {
            @Override
            public CompletionStage<WriteResult> write(List<Envelope> records) {
                return write;
            }

            @Override
            public void close() {
            }
        };
        CompletionStage<WriteResult> guarded = FencedSinkWriterFactory.guarded(sink, fence, guard(claims))
                .write(List.of());

        write.completeExceptionally(new TapstateException(ConnectorError.WRITE_FAILED,
                Map.of("connector", "e2e_file", "detail", "sink refused the write"), null));

        assertThatThrownBy(() -> guarded.toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(TapstateException.class);
        WorkloadClaim marked = claims.read(new WorkloadClaimKey(
                "cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders")).orElseThrow().claim();
        assertThat(marked.failureClaimGeneration()).isEqualTo(fence.claimGeneration());
        assertThat(marked.failureAfterMemberLoss()).isFalse();
    }

    @Test
    void aSynchronousSinkFailureMarksItsRunBeforeTheWriteThrows() {
        ExecutionFence fence = submittedRun();
        TapstateException failure = new TapstateException(ConnectorError.WRITE_FAILED,
                Map.of("connector", "e2e_file", "detail", "sink refused the write"), null);
        SinkWriter sink = new SinkWriter() {
            @Override
            public CompletionStage<WriteResult> write(List<Envelope> records) {
                throw failure;
            }

            @Override
            public void close() {
            }
        };

        assertThatThrownBy(() -> FencedSinkWriterFactory.guarded(sink, fence, guard(claims)).write(List.of()))
                .isSameAs(failure);
        WorkloadClaim marked = claims.read(new WorkloadClaimKey(
                "cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders")).orElseThrow().claim();
        assertThat(marked.failureClaimGeneration()).isEqualTo(fence.claimGeneration());
        assertThat(marked.failureAfterMemberLoss()).isFalse();
    }

    @Test
    void anUnreachableClaimStoreDoesNotReplaceASynchronousSinkFailure() {
        ExecutionFence fence = submittedRun();
        TapstateException failure = new TapstateException(ConnectorError.WRITE_FAILED,
                Map.of("connector", "e2e_file", "detail", "sink refused the write"), null);
        SinkWriter sink = new SinkWriter() {
            @Override
            public CompletionStage<WriteResult> write(List<Envelope> records) {
                throw failure;
            }

            @Override
            public void close() {
            }
        };
        WorkloadClaimStore unreachableOnFailure = new UnreachableAfterFirstRead(claims);

        assertThatThrownBy(() -> FencedSinkWriterFactory.guarded(sink, fence,
                guard(unreachableOnFailure)).write(List.of()))
                .as("the claim store cannot replace the connector's coded failure")
                .isSameAs(failure);
        WorkloadClaim unmarked = claims.read(new WorkloadClaimKey(
                "cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders")).orElseThrow().claim();
        assertThat(unmarked.failureClaimGeneration()).isZero();
    }

    @Test
    void aLateSinkFailureCannotMarkTheExecutionThatReplacedIt() {
        ExecutionFence first = submittedRun();
        CompletableFuture<WriteResult> write = new CompletableFuture<>();
        SinkWriter sink = new SinkWriter() {
            @Override
            public CompletionStage<WriteResult> write(List<Envelope> records) {
                return write;
            }

            @Override
            public void close() {
            }
        };
        CompletionStage<WriteResult> guarded = FencedSinkWriterFactory.guarded(sink, first, guard(claims))
                .write(List.of());

        claims.elapse(TTL.plusSeconds(1));
        PipelineActuationOwnership nodeB = ownership(NODE_B);
        assertThat(nodeB.permit("orders").granted()).isTrue();
        ExecutionFence replacement = nodeB.beginExecution("orders").fence();
        assertThat(replacement.executionGeneration()).isGreaterThan(first.executionGeneration());

        write.completeExceptionally(new TapstateException(ConnectorError.WRITE_FAILED,
                Map.of("connector", "e2e_file", "detail", "the old write finished late"), null));

        assertThatThrownBy(() -> guarded.toCompletableFuture().join())
                .isInstanceOf(CompletionException.class);
        WorkloadClaim current = claims.read(new WorkloadClaimKey(
                "cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders")).orElseThrow().claim();
        assertThat(current.failureClaimGeneration()).isZero();
    }

    @Test
    void aSupersededRunStopsWritingAndAcknowledgingOnceItsOwnWindowIsOver() {
        ExecutionFence fence = submittedRun();
        CountingWriter target = new CountingWriter();
        AtomicInteger acked = new AtomicInteger();
        ExecutionAuthorization guard = guard(claims);
        SinkWriter writer = FencedSinkWriterFactory.guarded(target, fence, guard);
        SinkAck ack = FencedSinkAckFactory.guarded((chain, position) -> acked.incrementAndGet(), fence, guard);
        writer.write(List.of());

        // The pipeline changes hands and the next owner submits its own run, so both generations move.
        claims.elapse(TTL.plusSeconds(1));
        PipelineActuationOwnership nodeB = ownership(NODE_B);
        assertThat(nodeB.permit("orders").granted()).isTrue();
        assertThat(nodeB.beginExecution("orders").allowed()).isTrue();
        int writtenBefore = target.batches.get();
        int ackedBefore = acked.get();

        // Still inside this member's window, it is acting on the reading it last took.
        writer.write(List.of());
        assertThat(target.batches.get() - writtenBefore)
                .as("the local window is what bounds this, and it has not closed yet").isEqualTo(1);

        nanos.addAndGet(WINDOW.toNanos());

        assertThatThrownBy(() -> writer.write(List.of()))
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code())
                .isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED);
        assertThatThrownBy(() -> ack.advance("orders", new ChainPosition(new SourceOrder(1, 2), "w2")))
                .isInstanceOf(TapstateException.class);
        assertThat(target.batches.get() - writtenBefore - 1)
                .as("no batch of a superseded run is sent after its local deadline").isZero();
        assertThat(acked.get() - ackedBefore)
                .as("and no durable position is advanced by it either").isZero();
    }

    @Test
    void aRunItsOwnHolderRebuiltOverStopsWritingAndAcknowledgingAtItsDeadline() {
        PipelineActuationOwnership nodeA = ownership(NODE_A);
        assertThat(nodeA.permit("orders").granted()).isTrue();
        ExecutionFence fence = nodeA.beginExecution("orders").fence();
        CountingWriter target = new CountingWriter();
        AtomicInteger acked = new AtomicInteger();
        ExecutionAuthorization guard = guard(claims);
        SinkWriter writer = FencedSinkWriterFactory.guarded(target, fence, guard);
        SinkAck ack = FencedSinkAckFactory.guarded((chain, position) -> acked.incrementAndGet(), fence, guard);
        writer.write(List.of());

        // A member went out from under the run and the member already driving the pipeline rebuilt it.
        // Nobody took the pipeline over, so this is the supersession the claim generation cannot see:
        // it stands still, and the two runs are told apart by the execution generation alone.
        ExecutionFence rebuilt = nodeA.beginExecution("orders").fence();
        assertThat(rebuilt.claimGeneration())
                .as("the same member is still driving it, so ownership did not change hands")
                .isEqualTo(fence.claimGeneration());
        assertThat(rebuilt.executionGeneration())
                .as("and the rebuild is a different run all the same")
                .isEqualTo(fence.executionGeneration() + 1);

        int writtenBefore = target.batches.get();
        nanos.addAndGet(WINDOW.toNanos());

        assertThatThrownBy(() -> writer.write(List.of()))
                .as("a member that holds the two generations as one number cannot tell a rebuilt-over run"
                        + " from the run that replaced it, and keeps writing on behalf of both")
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code())
                .isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED);
        assertThatThrownBy(() -> ack.advance("orders", new ChainPosition(new SourceOrder(1, 2), "w2")))
                .isInstanceOf(TapstateException.class);
        assertThat(target.batches.get() - writtenBefore)
                .as("no batch of the rebuilt-over run is sent after its local deadline").isZero();
        assertThat(acked)
                .as("and no durable position is advanced by it either").hasValue(0);
    }

    /**
     * A sink reports through the ack bound to it as a writer, and reports bounds as well as positions - so
     * holding a run to its fence has to reach both, through the binding. The guard used to wrap the member's
     * ack alone: binding a writer then answered with the guarded ack itself, the writer's progress went
     * nowhere, and a bound was dropped without a word.
     *
     * <p>Starting the run's accounting is held the same way, on the member that starts it: a superseded run
     * starting it again would take the accounting over from the run that replaced it.
     */
    @Test
    void aRebuiltOverRunsWritersStopLandingAnythingAndItCannotStartItsAccountingAgain() {
        PipelineActuationOwnership nodeA = ownership(NODE_A);
        assertThat(nodeA.permit("orders").granted()).isTrue();
        ExecutionFence fence = nodeA.beginExecution("orders").fence();
        ExecutionAuthorization guard = guard(claims);
        List<String> landed = new ArrayList<>();
        SinkAck asTheWriter = new SinkAck() {
            @Override
            public void advance(String chain, ChainPosition position) {
                landed.add("advance");
            }

            @Override
            public void bounded(String chain, SourceOrder through) {
                landed.add("bounded");
            }
        };
        SinkAck onTheMember = new SinkAck() {
            @Override
            public void advance(String chain, ChainPosition position) {
                landed.add("unbound");
            }

            @Override
            public SinkAck forWriter(String writerId) {
                landed.add("bound " + writerId);
                return asTheWriter;
            }
        };
        SinkAck writer = FencedSinkAckFactory.guarded(onTheMember, fence, guard).forWriter("serve.s#0");
        HazelcastInstance coordinator = mock(HazelcastInstance.class);
        ConcurrentMap<String, Object> context = new ConcurrentHashMap<>();
        context.put(ExecutionAuthorization.USER_CONTEXT_KEY, guard);
        when(coordinator.getUserContext()).thenReturn(context);
        List<Map<String, List<String>>> started = new ArrayList<>();
        SinkAckFactory accounting = new SinkAckFactory() {
            @Override
            public SinkAck resolve(HazelcastInstance member) {
                return onTheMember;
            }

            @Override
            public void beginRun(HazelcastInstance member, Map<String, List<String>> writersByChain) {
                started.add(writersByChain);
            }
        };
        SinkAckFactory fenced = FencedSinkAckFactory.heldTo(accounting, fence);

        fenced.beginRun(coordinator, Map.of("orders", List.of("serve.s#0")));
        writer.advance("orders", new ChainPosition(new SourceOrder(1, 1), "w1"));
        writer.bounded("orders", new SourceOrder(1, 2));
        assertThat(started).hasSize(1);
        assertThat(landed).containsExactly("bound serve.s#0", "advance", "bounded");

        nodeA.beginExecution("orders");
        nanos.addAndGet(WINDOW.toNanos());

        assertThatThrownBy(() -> writer.advance("orders", new ChainPosition(new SourceOrder(1, 3), "w3")))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> writer.bounded("orders", new SourceOrder(1, 4)))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> fenced.beginRun(coordinator, Map.of("orders", List.of("serve.s#0"))))
                .isInstanceOf(TapstateException.class);
        assertThat(landed)
                .as("nothing the rebuilt-over run's writer says lands, positions and bounds alike")
                .containsExactly("bound serve.s#0", "advance", "bounded");
        assertThat(started).as("and it does not start its accounting over the run that replaced it").hasSize(1);
    }

    /**
     * Preparing a run's target tables is held to the run like writing into them: a clear is a write, and a run
     * that has been rebuilt over clearing a table after its replacement started writing into it would take the
     * replacement's rows with it.
     */
    @Test
    void aRebuiltOverRunCannotPrepareItsTargetsAgain() {
        PipelineActuationOwnership nodeA = ownership(NODE_A);
        assertThat(nodeA.permit("orders").granted()).isTrue();
        ExecutionFence fence = nodeA.beginExecution("orders").fence();
        ExecutionAuthorization guard = guard(claims);
        HazelcastInstance coordinator = mock(HazelcastInstance.class);
        ConcurrentMap<String, Object> context = new ConcurrentHashMap<>();
        context.put(ExecutionAuthorization.USER_CONTEXT_KEY, guard);
        when(coordinator.getUserContext()).thenReturn(context);
        List<String> prepared = new ArrayList<>();
        SupplierEx<? extends SinkWriter> fenced =
                FencedSinkWriterFactory.heldTo(new PreparingWriters(prepared), fence);

        assertThat(fenced).isInstanceOf(PreparesTargets.class);
        ((PreparesTargets) fenced).prepareTargets(coordinator);
        assertThat(prepared).hasSize(1);

        nodeA.beginExecution("orders");
        nanos.addAndGet(WINDOW.toNanos());

        assertThatThrownBy(() -> ((PreparesTargets) fenced).prepareTargets(coordinator))
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code())
                .isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED);
        assertThat(prepared).as("nothing prepared for the run that was rebuilt over").hasSize(1);
    }

    /** Writers of tables prepared for them, recording each preparing. */
    private static final class PreparingWriters implements SupplierEx<SinkWriter>, PreparesTargets {

        private static final long serialVersionUID = 1L;

        private final transient List<String> prepared;

        PreparingWriters(List<String> prepared) {
            this.prepared = prepared;
        }

        @Override
        public void prepareTargets(HazelcastInstance coordinator) {
            prepared.add("prepared");
        }

        @Override
        public SinkWriter getEx() {
            return new CountingWriter();
        }
    }

    @Test
    void aRunWhoseOwnerStoppedRenewingStopsAtTheLeaseEvenThoughNobodyHasTakenItOver() {
        ExecutionFence fence = submittedRun();
        CountingWriter target = new CountingWriter();
        AtomicInteger acked = new AtomicInteger();
        ExecutionAuthorization guard = guard(claims);
        SinkWriter writer = FencedSinkWriterFactory.guarded(target, fence, guard);
        SinkAck ack = FencedSinkAckFactory.guarded((chain, position) -> acked.incrementAndGet(), fence, guard);
        writer.write(List.of());

        // The member holding the pipeline goes away. It renews nothing, and no other member has yet come
        // for the claim -- so the record it left behind still carries the very generations this run was
        // submitted with, and goes on carrying them for as long as nobody takes it over.
        elapse(TTL.plusSeconds(1));
        assertThat(claims.read(new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders"))
                .orElseThrow())
                .as("the generations this member is checking against have not moved at all")
                .satisfies(reading -> {
                    assertThat(reading.claim().claimGeneration()).isEqualTo(fence.claimGeneration());
                    assertThat(reading.claim().executionGeneration()).isEqualTo(fence.executionGeneration());
                    assertThat(reading.leased()).isFalse();
                });
        int writtenBefore = target.batches.get();

        assertThatThrownBy(() -> writer.write(List.of()))
                .as("an expired lease is nobody's run, whoever ends up holding it next")
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code())
                .isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED);
        assertThatThrownBy(() -> ack.advance("orders", new ChainPosition(new SourceOrder(1, 2), "w2")))
                .isInstanceOf(TapstateException.class);

        // Far past any refresh window: without the lease bounding it, each refresh would keep finding the
        // same matching generations and hand out another window, indefinitely.
        elapse(WINDOW.multipliedBy(20));
        assertThatThrownBy(() -> writer.write(List.of())).isInstanceOf(TapstateException.class);
        assertThat(target.batches.get() - writtenBefore).isZero();
        assertThat(acked).hasValue(0);
    }

    /**
     * The other five cases above build the guard <em>after</em> the run they ask about, so the guard has
     * nothing to be behind with. A member carrying a real run has answered for the previous one, and
     * what it answered with is what these two are about: a reading older than the run being asked about
     * is not evidence against that run, and refusing on it does not pause anything — it reaches the job
     * as an exception and ends it, which is the one thing nothing here recovers from.
     */
    @Test
    void aSecondRunFromTheSameHolderIsNotRefusedByTheReadingTakenOfTheFirst() {
        PipelineActuationOwnership nodeA = ownership(NODE_A);
        assertThat(nodeA.permit("orders").granted()).isTrue();
        ExecutionFence first = nodeA.beginExecution("orders").fence();
        ExecutionAuthorization guard = guard(claims);
        // Answering once for the first run is what leaves the reading behind, and a member that wrote a
        // single batch of it has one. The reading is still live when the second run arrives, because a
        // rebuild follows the death that caused it by about a second.
        assertThat(guard.authorized(first)).isTrue();

        ExecutionFence second = nodeA.beginExecution("orders").fence();

        assertThat(second.executionGeneration()).isGreaterThan(first.executionGeneration());
        assertThat(guard.authorized(second))
                .as("the store is the only thing that hands out a generation, so a run carrying more than"
                        + " this member has read is newer than the reading, never superseded by it")
                .isTrue();
    }

    @Test
    void takingOverALapsedClaimDoesNotLeaveThisMemberRefusingTheRunItJustSubmitted() {
        ExecutionFence dead = submittedRun();
        ExecutionAuthorization guard = guard(claims);
        assertThat(guard.authorized(dead)).isTrue();

        elapse(TTL.plusSeconds(1));
        assertThat(guard.authorized(dead))
                .as("a lapsed lease is nobody's run, and saying so is what drops this member's reading")
                .isFalse();

        // And the lapse is exactly what lets the next member have the claim, so the takeover follows it
        // at once -- well inside the interval that a refusal would otherwise keep the store unasked for.
        PipelineActuationOwnership nodeB = ownership(NODE_B);
        assertThat(nodeB.permit("orders").granted()).isTrue();
        ExecutionFence taken = nodeB.beginExecution("orders").fence();

        assertThat(guard.authorized(taken))
                .as("a member with no reading at all must go and get one rather than refuse: the run it"
                        + " would refuse here is the takeover it just submitted")
                .isTrue();
    }

    @Test
    void aMemberThatCannotRefreshStopsAtItsDeadlineRatherThanCarryingOn() {
        ExecutionFence fence = submittedRun();
        CountingWriter target = new CountingWriter();
        UnreachableAfterFirstRead store = new UnreachableAfterFirstRead(claims);
        ExecutionAuthorization guard = guard(store);
        SinkWriter writer = FencedSinkWriterFactory.guarded(target, fence, guard);

        writer.write(List.of());
        assertThat(target.batches).as("the first reading was answered").hasValue(1);

        nanos.addAndGet(WINDOW.toNanos());

        assertThatThrownBy(() -> writer.write(List.of()))
                .as("an unreachable store is not permission to keep writing")
                .isInstanceOf(TapstateException.class);
        assertThat(target.batches).hasValue(1);
        assertThat(store.reads.get())
                .as("and the refused batches do not each turn into a store round trip")
                .isLessThanOrEqualTo(2);
    }

    /** Moves both clocks together, the way waiting moves the store's lease and this member's deadline. */
    private void elapse(Duration elapsed) {
        claims.elapse(elapsed);
        nanos.addAndGet(elapsed.toNanos());
    }

    /** A run of `orders` submitted by node A, fenced by the generations the store handed it. */
    private ExecutionFence submittedRun() {
        PipelineActuationOwnership nodeA = ownership(NODE_A);
        assertThat(nodeA.permit("orders").granted()).isTrue();
        PipelineActuationOwnership.Execution execution = nodeA.beginExecution("orders");
        assertThat(execution.allowed()).isTrue();
        return execution.fence();
    }

    private ExecutionAuthorization guard(WorkloadClaimStore store) {
        return new ExecutionAuthorization("cluster-a", store, WINDOW, nanos::get);
    }

    private PipelineActuationOwnership ownership(WorkloadOwner owner) {
        return new PipelineActuationOwnership(
                "cluster-a", owner, membership, new ClusterWorkloadClaims(claims, membership), TTL, RENEW,
                nanos::get);
    }

    private static ClusterMembershipGate eligibleGate() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        ClusterMembershipGate gate = new ClusterMembershipGate(properties);
        gate.install(new ClusterMembership("cluster-a", 7, Set.of("node-a", "node-b", "node-c")));
        gate.canCommit(Set.of("node-a", "node-b"));
        return gate;
    }

    private static final class CountingWriter implements SinkWriter {

        private final AtomicInteger batches = new AtomicInteger();

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            batches.incrementAndGet();
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }

    /** A store that answers once and is unreachable after that, the way a partitioned member sees it. */
    private static final class UnreachableAfterFirstRead implements WorkloadClaimStore {

        private final WorkloadClaimStore delegate;
        private final AtomicInteger reads = new AtomicInteger();

        private UnreachableAfterFirstRead(WorkloadClaimStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
            if (reads.incrementAndGet() > 1) {
                throw new IllegalStateException("coordination store unreachable");
            }
            return delegate.read(key);
        }

        @Override
        public WorkloadClaimAttempt acquire(
                WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
            return delegate.acquire(key, owner, topologyRevision, ttl);
        }

        @Override
        public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            return delegate.renew(expected, ttl);
        }

        @Override
        public boolean release(WorkloadClaim expected) {
            return delegate.release(expected);
        }

        @Override
        public Optional<WorkloadClaim> advanceExecution(
                WorkloadClaim expected, long topologyRevision, java.util.Set<String> executionNodeIds) {
            return delegate.advanceExecution(expected, topologyRevision, executionNodeIds);
        }

        @Override
        public Optional<WorkloadClaim> recordExecutionFailure(
                WorkloadClaim expected, boolean afterMemberLoss) {
            return delegate.recordExecutionFailure(expected, afterMemberLoss);
        }
    }
}
