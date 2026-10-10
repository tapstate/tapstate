package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.map.IMap;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How an operation on operator state meets the cluster refusing it: waited out while the refusal clears, ended
 * with a code once it has lasted the whole bound, and never mistaken for anything else the operation raises.
 * The clock and the pause are this case's own, so the wait is measured rather than spent.
 */
class ProtectedMapTest {

    private static final Duration BOUND = Duration.ofSeconds(30);

    private final Deque<Supplier<Object>> answers = new ArrayDeque<>();
    private final List<Long> pauses = new ArrayList<>();
    private long nanos;

    @Test
    void aRefusalThatClearsIsWaitedOutAndTheOperationAskedAgain() {
        answers.add(ProtectedMapTest::refused);
        answers.add(ProtectedMapTest::refused);
        answers.add(() -> "v");

        assertThat(map().get("k"))
                .as("the operation the cluster refused twice is asked a third time and answers")
                .isEqualTo("v");
        assertThat(pauses)
                .as("each refusal waits a little longer than the one before it")
                .containsExactly(20L, 40L);
    }

    @Test
    void aRefusalThatLastsTheWholeBoundEndsTheRunWithACode() {
        for (int ask = 0; ask < 10_000; ask++) {
            answers.add(ProtectedMapTest::refused);
        }

        assertThatThrownBy(() -> map().get("k"))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(EngineError.CLUSTER_REFUSED_THE_STATE);
                    assertThat(error.args())
                            .containsEntry("state", "join.p1.widen.fact")
                            .containsEntry("seconds", 30L);
                })
                .hasCauseInstanceOf(SplitBrainProtectionException.class);
        assertThat(pauses.stream().mapToLong(Long::longValue).sum())
                .as("it waited the whole bound, and not a moment of it past what the bound allows")
                .isBetween(BOUND.toMillis(), BOUND.toMillis() + 500);
    }

    @Test
    void anythingElseTheOperationRaisesGoesOnAtOnce() {
        answers.add(() -> {
            throw new IllegalStateException("not the cluster's refusal");
        });

        assertThatThrownBy(() -> map().get("k"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("not the cluster's refusal");
        assertThat(pauses).as("nothing was waited for").isEmpty();
    }

    @Test
    void aRefusalCarriedAsTheCauseOfAnotherFailureIsWaitedOutToo() {
        answers.add(() -> {
            throw new HazelcastException("the invocation failed", new SplitBrainProtectionException("refused"));
        });
        answers.add(() -> "v");

        assertThat(map().get("k")).isEqualTo("v");
        assertThat(pauses).containsExactly(20L);
    }

    private static Object refused() {
        throw new SplitBrainProtectionException("Split brain protection exception: needs-both-members has failed!");
    }

    /** The map under test: its every operation answered from {@link #answers}, on this case's clock. */
    @SuppressWarnings("unchecked")
    private ProtectedMap<String, String> map() {
        IMap<String, String> map = (IMap<String, String>) Proxy.newProxyInstance(
                IMap.class.getClassLoader(), new Class<?>[] {IMap.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getName")) {
                        return "join.p1.widen.fact";
                    }
                    if (answers.isEmpty()) {
                        throw new AssertionError("asked more times than this case answers");
                    }
                    return answers.poll().get();
                });
        return ProtectedMap.of(map, () -> nanos, millis -> {
            pauses.add(millis);
            nanos += Duration.ofMillis(millis).toNanos();
        }, BOUND);
    }
}
