package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Where a run's fence is answered once its job is spread over more than one member.
 *
 * <p>One submitted run reaches every member that holds a piece of it, and what can take it away -- the
 * claim under it lapsing, or a newer run replacing it -- shows up in a coordination store each member has
 * to read for itself. A member that cannot reach that store has to stop on its own account: the other
 * members are still reading it happily, so asking any one of them on everybody's behalf would leave the
 * cut-off member writing and acknowledging for a run that is no longer its own. That is the failure this
 * exists for, and acknowledging is the worse half of it -- a position a superseded run advances is where
 * every later run resumes from, so what it skipped is not replayed by anyone.
 *
 * <p>The members here do not form a cluster and do not need to: what is under test is that each one is
 * asked separately, which is visible the moment two of them answer differently.
 */
class EveryParticipatingMemberFencesExternalEffectsLocallyTest {

    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration RENEW = Duration.ofSeconds(10);
    private static final Duration WINDOW = Duration.ofSeconds(10);
    private static final WorkloadOwner NODE_A = new WorkloadOwner("node-a", "boot-a");

    private final InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
    private final ClusterMembershipGate membership = eligibleGate();
    private final AtomicLong readingMemberNanos = new AtomicLong();
    private final AtomicLong cutOffMemberNanos = new AtomicLong();

    private HazelcastInstance readingMember;
    private HazelcastInstance cutOffMember;

    @BeforeEach
    void startTwoMembers() {
        readingMember = standaloneMember();
        cutOffMember = standaloneMember();
    }

    @AfterEach
    void stopThem() {
        for (HazelcastInstance member : List.of(readingMember, cutOffMember)) {
            if (member != null) {
                member.shutdown();
            }
        }
    }

    @Test
    void theMemberThatCannotReachTheStoreStopsAcknowledgingWhileTheOtherCarriesOnWithTheSameRun() {
        ExecutionFence fence = submittedRun();
        ExecutionAuthorization reading =
                new ExecutionAuthorization("cluster-a", claims, WINDOW, readingMemberNanos::get);
        UnreachableAfterFirstRead cutOffStore = new UnreachableAfterFirstRead(claims);
        ExecutionAuthorization cutOff =
                new ExecutionAuthorization("cluster-a", cutOffStore, WINDOW, cutOffMemberNanos::get);
        readingMember.getUserContext().put(ExecutionAuthorization.USER_CONTEXT_KEY, reading);
        cutOffMember.getUserContext().put(ExecutionAuthorization.USER_CONTEXT_KEY, cutOff);

        assertThat(ExecutionAuthorization.of(readingMember))
                .as("the guard a sink is held to is the one bound to the member it lands on")
                .isSameAs(reading);
        assertThat(ExecutionAuthorization.of(cutOffMember)).isSameAs(cutOff);

        List<String> advanced = Collections.synchronizedList(new ArrayList<>());
        SinkAckFactory recording = member -> (chain, position) -> advanced.add(chain);
        SinkAck onTheReadingMember = FencedSinkAckFactory.heldTo(recording, fence).resolve(readingMember);
        SinkAck onTheCutOffMember = FencedSinkAckFactory.heldTo(recording, fence).resolve(cutOffMember);

        onTheReadingMember.advance("orders", position(1));
        onTheCutOffMember.advance("orders", position(2));
        assertThat(advanced)
                .as("both members hold a piece of the same run, and while both can prove it both may act")
                .hasSize(2);

        // The cut-off member's path to the store is gone and its own window runs out. Nothing about the
        // run has changed, and the other member never hears about any of it.
        cutOffMemberNanos.addAndGet(WINDOW.toNanos());

        assertThatThrownBy(() -> onTheCutOffMember.advance("orders", position(3)))
                .as("a member that can no longer prove the run is still its own stops advancing the "
                        + "durable position rather than carrying on from the last answer it liked")
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code())
                .isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED);

        onTheReadingMember.advance("orders", position(4));
        assertThat(advanced)
                .as("and it stopped on its own reading alone: the member that can still reach the store is "
                        + "untouched, so one guard answering for everybody would have let this pair through "
                        + "together -- either both stopped, or, on the reachable member's answer, neither")
                .containsExactly("orders", "orders", "orders");
        assertThat(cutOffStore.reads)
                .as("and the refused advances do not each turn into another attempt at the store")
                .hasValueLessThanOrEqualTo(2);
    }

    @Test
    void aGuardIsNeverTakenFromWhicheverMemberOfThisProcessCameFirst() {
        assertThatThrownBy(ExecutionAuthorization::local)
                .as("a sink writer is opened with no member handle in hand, so it finds its member by "
                        + "there being exactly one. Where there is more than one, picking any of them is a "
                        + "fence over a member chosen by accident, which is no fence at all")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("exactly one local Hazelcast member");
    }

    /** A run of `orders` submitted by node A, fenced by the generations the store handed it. */
    private ExecutionFence submittedRun() {
        PipelineActuationOwnership nodeA = new PipelineActuationOwnership(
                "cluster-a", NODE_A, membership, new ClusterWorkloadClaims(claims, membership), TTL, RENEW,
                readingMemberNanos::get);
        assertThat(nodeA.permit("orders").granted()).isTrue();
        PipelineActuationOwnership.Execution execution = nodeA.beginExecution("orders");
        assertThat(execution.allowed()).isTrue();
        return execution.fence();
    }

    private static ChainPosition position(int order) {
        return new ChainPosition(new SourceOrder(1, order), "w" + order);
    }

    private static ClusterMembershipGate eligibleGate() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        ClusterMembershipGate gate = new ClusterMembershipGate(properties);
        gate.install(new ClusterMembership("cluster-a", 7, Set.of("node-a", "node-b", "node-c")));
        gate.canCommit(Set.of("node-a", "node-b"));
        return gate;
    }

    /** A member of this process alone: no discovery, so two of them never become one cluster. */
    private static HazelcastInstance standaloneMember() {
        Config config = new Config();
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        return Hazelcast.newHazelcastInstance(config);
    }

    /** A store that answers once and is unreachable after that, the way a cut-off member sees it. */
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
        public Optional<WorkloadClaim> advanceUnderClaim(WorkloadClaim expected, long topologyRevision) {
            return delegate.advanceUnderClaim(expected, topologyRevision);
        }
    }
}
