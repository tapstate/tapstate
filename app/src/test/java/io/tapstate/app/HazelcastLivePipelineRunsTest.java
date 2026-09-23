package io.tapstate.app;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.jet.JetService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import io.tapstate.control.core.ClusterError;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the topology read says about where work runs when the engine will not say.
 *
 * <p>The engine lists its jobs by asking every member and the one coordinating the cluster. When a member
 * dies, the coordinating role moves, and a listing caught in the move can go unanswered until the engine's
 * own call timeout -- a minute, during which whoever asked who is left waits too. These pin the read to a
 * bounded wait that ends in the refusal the rest of the read already gives while the cluster is changing,
 * and to leaving nothing parked behind it.
 */
class HazelcastLivePipelineRunsTest {

    private static final Duration BOUND = Duration.ofMillis(200);

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aListingTheEngineDoesNotAnswerIsRefusedWithinTheBoundAndLeftNothingParked() throws Exception {
        CountDownLatch listingEnded = new CountDownLatch(1);
        AtomicBoolean listingWasInterrupted = new AtomicBoolean();
        HazelcastLivePipelineRuns runs = new HazelcastLivePipelineRuns(memberListing(() -> {
            try {
                // Answered by nobody: the engine dropped the call and nothing will ever complete it.
                new CountDownLatch(1).await();
                return List.of();
            } catch (InterruptedException interrupted) {
                listingWasInterrupted.set(true);
                throw interrupted;
            } finally {
                listingEnded.countDown();
            }
        }), BOUND);

        long started = System.nanoTime();
        assertThatThrownBy(runs::runs)
                .describedAs("a listing nobody answers is the cluster changing under the read, and is said "
                        + "with the code that means ask again")
                .isInstanceOfSatisfying(TapstateException.class, refused ->
                        assertThat(refused.code()).isEqualTo(ClusterError.MEMBERSHIP_UNREADABLE));
        assertThat(Duration.ofNanos(System.nanoTime() - started))
                .describedAs("and it is said once the bound has passed, not once the engine gives up")
                .isLessThan(Duration.ofSeconds(10));

        assertThat(listingEnded.await(10, TimeUnit.SECONDS))
                .describedAs("the listing given up on stops rather than staying parked on its thread")
                .isTrue();
        assertThat(listingWasInterrupted)
                .describedAs("and it stops because it was told to, which the engine's own waits honour")
                .isTrue();
    }

    @Test
    void aListingThatIsAnsweredIsTheAnswer() {
        HazelcastLivePipelineRuns runs = new HazelcastLivePipelineRuns(memberListing(List::of), BOUND);

        assertThat(runs.runs())
                .describedAs("an engine running nothing says so, and the bound changes nothing about that")
                .isEmpty();
    }

    @Test
    void anEngineThatIsNotActiveIsRefusedWithTheSameCodeItAlwaysWas() {
        HazelcastLivePipelineRuns runs = new HazelcastLivePipelineRuns(memberListing(() -> {
            throw new HazelcastInstanceNotActiveException();
        }), BOUND);

        assertThatThrownBy(runs::runs)
                .describedAs("the refusal made on the listing's own thread reaches the caller as it was made")
                .isInstanceOfSatisfying(TapstateException.class, refused ->
                        assertThat(refused.code()).isEqualTo(ClusterError.MEMBERSHIP_UNREADABLE));
    }

    /**
     * A listing aimed at an address that stopped being a member is the cluster changing, not a fault.
     *
     * <p>Measured on a three-member cluster as a partition healed: the listing went to the member that had
     * been coordinating, which was no longer one, and the read answered an uncoded page.
     */
    @Test
    void aListingSentToAnAddressThatIsNoLongerAMemberIsRefusedWithTheSameCode() {
        HazelcastLivePipelineRuns runs = new HazelcastLivePipelineRuns(memberListing(() -> {
            throw new TargetNotMemberException("Not Member! target: [127.0.0.1]:33301, partitionId: -1");
        }), BOUND);

        assertThatThrownBy(runs::runs)
                .describedAs("the engine marks this as worth asking again, and so does the answer")
                .isInstanceOfSatisfying(TapstateException.class, refused ->
                        assertThat(refused.code()).isEqualTo(ClusterError.MEMBERSHIP_UNREADABLE));
    }

    @Test
    void aMemberThatLeftMidListingIsRefusedWithTheSameCodeEvenWhenHandedOnWrapped() {
        HazelcastLivePipelineRuns runs = new HazelcastLivePipelineRuns(memberListing(() -> {
            throw new HazelcastException("listing failed", new MemberLeftException("the member left"));
        }), BOUND);

        assertThatThrownBy(runs::runs)
                .describedAs("a departure the engine wrapped is still a departure")
                .isInstanceOfSatisfying(TapstateException.class, refused ->
                        assertThat(refused.code()).isEqualTo(ClusterError.MEMBERSHIP_UNREADABLE));
    }

    @Test
    void anyOtherFailureComesOutAsItWent() {
        IllegalStateException defect = new IllegalStateException("a defect, not the cluster being busy");
        HazelcastLivePipelineRuns runs = new HazelcastLivePipelineRuns(memberListing(() -> {
            throw defect;
        }), BOUND);

        assertThatThrownBy(runs::runs)
                .describedAs("a failure filed under a code that says the cluster is busy is one nobody looks for")
                .isSameAs(defect);
    }

    /** A member whose engine answers a listing of its jobs by calling {@code listing}, and nothing else. */
    private static HazelcastInstance memberListing(Callable<List<?>> listing) {
        JetService jet = proxy(JetService.class, (instance, method, arguments) -> {
            if ("getJobs".equals(method.getName()) && (arguments == null || arguments.length == 0)) {
                return listing.call();
            }
            throw new UnsupportedOperationException(method.getName());
        });
        return proxy(HazelcastInstance.class, (instance, method, arguments) -> {
            if ("getJet".equals(method.getName())) {
                return jet;
            }
            throw new UnsupportedOperationException(method.getName());
        });
    }

    /**
     * Answers {@link Object}'s own methods rather than throwing on them, so a proxy that ends up in a log
     * line or a collection fails nothing for the wrong reason.
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
}
