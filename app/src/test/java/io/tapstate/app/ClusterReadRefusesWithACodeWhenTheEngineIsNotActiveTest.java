package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import io.tapstate.control.core.ClusterError;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the two halves of the topology read answer on a member whose engine instance is not active.
 *
 * <p>The window is ordinary rather than exceptional: a member on its way up or down is in it, and so is
 * a member whose instance the library restarts on the smaller side once a split brain heals -- while the
 * HTTP face, which is a different thread in a different framework, goes on serving throughout. Both
 * halves of this read ask the instance something, so both are in the window, and a read that answered
 * half of it would be worse than one that refused.
 *
 * <p>Driven through a member that answers nothing but the library's own refusal, because that is exactly
 * what the library hands a caller here and the point of the code is what we do with it. Nothing is
 * stubbed about the decision itself: the adapters are the real ones.
 */
class ClusterReadRefusesWithACodeWhenTheEngineIsNotActiveTest {

    @Test
    void theMemberHalfRefusesWithACodeRatherThanTheLibrarysOwnException() {
        HazelcastLiveClusterMembers members = new HazelcastLiveClusterMembers(notActive());

        assertThatThrownBy(members::members)
                .describedAs("a library exception reaching the container is an uncoded 500, which a "
                        + "caller cannot tell from the product having fallen over -- and this window is "
                        + "one the product is meant to be in")
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code())
                .isEqualTo(ClusterError.MEMBERSHIP_UNREADABLE);
    }

    @Test
    void theRunHalfRefusesWithTheSameCodeRatherThanAnsweringNoRuns() {
        HazelcastLivePipelineRuns runs = new HazelcastLivePipelineRuns(notActive());

        assertThatThrownBy(runs::runs)
                .describedAs("one cause, one code: a member that cannot be asked who is in the cluster "
                        + "cannot be asked what it is running either, and an empty list here would be "
                        + "this member reporting that the cluster runs nothing")
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code())
                .isEqualTo(ClusterError.MEMBERSHIP_UNREADABLE);
    }

    @Test
    void anythingElseTheEngineThrowsIsLeftAloneToCrash() {
        HazelcastInstance broken = answering(new IllegalArgumentException("a defect, not a window"));

        assertThatThrownBy(new HazelcastLiveClusterMembers(broken)::members)
                .describedAs("the catch is aimed at one library exception with a meaning, not at "
                        + "whatever comes out: laundering a programmer error into a refusal would file "
                        + "a defect under a code that says the member is merely busy")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(new HazelcastLivePipelineRuns(broken)::runs)
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** A member in the window: every call on it refuses the way the library refuses one. */
    private static HazelcastInstance notActive() {
        return answering(new HazelcastInstanceNotActiveException());
    }

    /**
     * A member that answers {@code thrown} to everything it is asked.
     *
     * <p>A proxy rather than a written-out double because the interface carries scores of methods and
     * this cares about two of them; {@code Object}'s own are answered rather than thrown, so that a
     * failing assertion can still print what it was holding instead of failing again while it tries.
     */
    private static HazelcastInstance answering(RuntimeException thrown) {
        return (HazelcastInstance) Proxy.newProxyInstance(
                HazelcastInstance.class.getClassLoader(),
                new Class<?>[] {HazelcastInstance.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> "a member whose instance is not active";
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == args[0];
                            default -> null;
                        };
                    }
                    throw thrown;
                });
    }
}
