package io.tapstate.app;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ClusterMembershipStore;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the committed set is allowed to become, asked of the thing that writes it.
 *
 * <p>The gate that admits work divides by the size of the committed set, and its own tests pin that two
 * of four is not a majority of four. Nothing there is wrong. The question here is the other half: who is
 * allowed to change the number the gate divides by, and whether a side that could never pass the gate
 * can reach a state where it does by changing that number first.
 */
class ClusterMembershipControllerTest {

    private static final String CLUSTER = "cluster-a";

    /**
     * A side holding two of four must not become eligible, however the views in between are sampled.
     *
     * <p>Losing members one at a time is what an ordinary failure looks like from here, and it is also
     * what a partition looks like: this member is told a peer is unreachable, never why. The two are the
     * same reading, so a rule that judges each step against the set the previous step committed accepts
     * the second reading as readily as the first. This walks the steps a four-member cluster passes
     * through when a cut lands and asks the one question the design answers with "never": may two of the
     * four that were committed when the cut began end up acting.
     */
    @Test
    void aTwoOfFourSideDoesNotBecomeEligibleByCommittingTheMembersItStillSees() {
        InMemoryMembershipStore store = new InMemoryMembershipStore();
        store.createIfAbsent(CLUSTER, Set.of("a", "b", "c", "d"));
        AtomicReference<Set<String>> visible =
                new AtomicReference<>(Set.of("a", "b", "c", "d"));
        ClusterMembershipGate gate = productionGate();

        try (ClusterMembershipController controller = controllerOver(store, gate, visible)) {
            awaitFirstPass(gate);
            controller.reconcile();

            assertThat(gate.businessEligible())
                    .describedAs("all four are here, so this member may act")
                    .isTrue();

            // One peer stops answering. Individually this is the ordinary case the cluster must survive,
            // and three of four is a majority of four, so it commits.
            visible.set(Set.of("a", "b", "c"));
            controller.reconcile();

            // A second peer stops answering, which on this side is indistinguishable from the first.
            visible.set(Set.of("a", "b"));
            controller.reconcile();

            assertThat(gate.businessEligible())
                    .describedAs("two of the four committed when the cut began is not a majority of "
                            + "four, and it does not become one by having committed the smaller sets "
                            + "this side passed through on the way down")
                    .isFalse();
            assertThat(store.held().revision())
                    .describedAs("and no revision was spent getting there -- every write this side "
                            + "could have made was a smaller set, which is the whole of it")
                    .isEqualTo(1);
        }
    }

    /**
     * The same walk, stopped one step earlier, must still admit -- otherwise the case above would pass
     * for a member that can never act at all, and would witness nothing.
     */
    @Test
    void aThreeOfFourSideGoesOnActing() {
        InMemoryMembershipStore store = new InMemoryMembershipStore();
        store.createIfAbsent(CLUSTER, Set.of("a", "b", "c", "d"));
        AtomicReference<Set<String>> visible =
                new AtomicReference<>(Set.of("a", "b", "c", "d"));
        ClusterMembershipGate gate = productionGate();

        try (ClusterMembershipController controller = controllerOver(store, gate, visible)) {
            awaitFirstPass(gate);

            visible.set(Set.of("a", "b", "c"));
            controller.reconcile();

            assertThat(gate.businessEligible())
                    .describedAs("three of four is a majority of four, so the survivors carry on")
                    .isTrue();
            assertThat(store.held().activeNodeIds())
                    .describedAs("and the one that went stays committed: unreachable is all this "
                            + "member was told, and writing it out is exactly what lowers the bar "
                            + "the next departure has to clear")
                    .containsExactlyInAnyOrder("a", "b", "c", "d");
        }
    }

    /**
     * A member that joins is still committed, which is the half that must survive the case above.
     *
     * <p>Without this, a rule that committed nothing at all would pass every other case here: they all
     * assert that some set is <em>not</em> written, and a member that never grows its set satisfies
     * them while quietly making a cluster unable to take a new member on.
     */
    @Test
    void aJoiningMemberIsCommittedWhileEveryCommittedMemberIsHereToAdmitIt() {
        InMemoryMembershipStore store = new InMemoryMembershipStore();
        store.createIfAbsent(CLUSTER, Set.of("a", "b", "c"));
        AtomicReference<Set<String>> visible = new AtomicReference<>(Set.of("a", "b", "c"));
        ClusterMembershipGate gate = productionGate();

        try (ClusterMembershipController controller = controllerOver(store, gate, visible)) {
            awaitFirstPass(gate);

            visible.set(Set.of("a", "b", "c", "d"));
            controller.reconcile();

            assertThat(store.held().activeNodeIds())
                    .describedAs("every committed member is present to admit the new one, so it "
                            + "enters the set")
                    .containsExactlyInAnyOrder("a", "b", "c", "d");
            assertThat(gate.businessEligible())
                    .describedAs("and the grown cluster goes on acting")
                    .isTrue();
        }
    }

    /** A genuine death still shrinks the set, which is the behaviour the case above must not cost. */
    @Test
    void theLastTwoOfThreeStillCarryTheCluster() {
        InMemoryMembershipStore store = new InMemoryMembershipStore();
        store.createIfAbsent(CLUSTER, Set.of("a", "b", "c"));
        AtomicReference<Set<String>> visible = new AtomicReference<>(Set.of("a", "b", "c"));
        ClusterMembershipGate gate = productionGate();

        try (ClusterMembershipController controller = controllerOver(store, gate, visible)) {
            awaitFirstPass(gate);

            visible.set(Set.of("a", "b"));
            controller.reconcile();

            assertThat(gate.businessEligible())
                    .describedAs("two of three is a majority of three")
                    .isTrue();
        }
    }

    private static ClusterMembershipController controllerOver(
            ClusterMembershipStore store,
            ClusterMembershipGate gate,
            AtomicReference<Set<String>> visible) {
        // Far longer than the test, so the only passes that run are the one the constructor schedules
        // immediately and the ones this test asks for by name.
        return new ClusterMembershipController(
                CLUSTER, memberSeeing(visible), store, gate, Duration.ofHours(1));
    }

    /**
     * Waits out the pass the constructor schedules at zero delay, so an explicit reconcile below is never
     * racing it. It installs what is already stored and writes nothing, so what this waits for is only
     * that it has happened.
     */
    private static void awaitFirstPass(ClusterMembershipGate gate) {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (gate.committed() == null) {
            if (System.nanoTime() - deadline >= 0) {
                throw new AssertionError("the controller's first pass never installed a committed set");
            }
            Thread.onSpinWait();
        }
    }

    private static ClusterMembershipGate productionGate() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        properties.setBootstrapMinMembers(3);
        return new ClusterMembershipGate(properties);
    }

    /** A member whose cluster reports whatever the supplied set says, and nothing else. */
    private static HazelcastInstance memberSeeing(AtomicReference<Set<String>> visible) {
        Cluster cluster = proxy(Cluster.class, (proxy, method, arguments) -> {
            if ("getMembers".equals(method.getName())) {
                Set<Member> members = new LinkedHashSet<>();
                for (String nodeId : visible.get()) {
                    members.add(memberNamed(nodeId));
                }
                return members;
            }
            throw new UnsupportedOperationException(method.getName());
        });
        return proxy(HazelcastInstance.class, (proxy, method, arguments) -> {
            if ("getCluster".equals(method.getName())) {
                return cluster;
            }
            throw new UnsupportedOperationException(method.getName());
        });
    }

    private static Member memberNamed(String nodeId) {
        return proxy(Member.class, (proxy, method, arguments) -> {
            if ("getAttribute".equals(method.getName())) {
                return ClusterMembershipGate.NODE_ID_ATTRIBUTE.equals(arguments[0]) ? nodeId : null;
            }
            throw new UnsupportedOperationException(method.getName());
        });
    }

    /**
     * Answers {@link Object}'s own methods rather than throwing on them: these proxies are put in sets,
     * and a handler that refuses hashCode would fail the test for the wrong reason.
     */
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(), new Class<?>[] {type}, (instance, method, arguments) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "hashCode" -> System.identityHashCode(instance);
                            case "equals" -> instance == arguments[0];
                            default -> type.getSimpleName() + "@"
                                    + Integer.toHexString(System.identityHashCode(instance));
                        };
                    }
                    return handler.invoke(instance, method, arguments);
                }));
    }

    /** The durable registry, with the compare-and-set the real one is built around and nothing else. */
    private static final class InMemoryMembershipStore implements ClusterMembershipStore {

        private ClusterMembership held;

        ClusterMembership held() {
            return held;
        }

        @Override
        public synchronized Optional<ClusterMembership> read(String clusterId) {
            return Optional.ofNullable(held);
        }

        @Override
        public synchronized ClusterMembership createIfAbsent(
                String clusterId, Set<String> activeNodeIds) {
            if (held == null) {
                held = new ClusterMembership(clusterId, 1, activeNodeIds);
            }
            return held;
        }

        @Override
        public synchronized Optional<ClusterMembership> compareAndSet(
                String clusterId, long expectedRevision, Set<String> activeNodeIds) {
            if (held == null || held.revision() != expectedRevision) {
                return Optional.empty();
            }
            held = new ClusterMembership(clusterId, expectedRevision + 1, activeNodeIds);
            return Optional.of(held);
        }
    }
}
