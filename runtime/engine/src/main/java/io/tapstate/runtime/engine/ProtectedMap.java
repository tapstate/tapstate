package io.tapstate.runtime.engine;

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import io.tapstate.core.common.TapstateException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * An operator's state map whose operations wait out the cluster refusing them, for a bounded stretch, rather
 * than ending the run that asked.
 *
 * <p>Operator state is guarded by the cluster's split brain protection, which a member answers from a verdict
 * its own library recomputes as the membership changes. A member that has just joined takes parts of the state
 * over - partitions move to it - before that verdict has caught up, and until it does every operation on those
 * parts is refused. Carried out of the processor, the refusal ends a running join or nest because a member
 * joined, which a member joining must never do to a run already under way.
 *
 * <p>Waiting is safe because a refused operation did not happen: the protection is checked before an operation
 * runs, so asking again is asking for the first time. That holds for each operation on its own, which is why the
 * wait is here, around each one, and not around a caller's sequence of them: taking a change into the state is
 * several operations, and redoing the ones that had already landed would apply them twice.
 *
 * <p>Waiting blocks the thread that asked, so only a processor that is not cooperative may hold one of these: a
 * processor that waits on a cooperative thread stops every other processor sharing that thread.
 *
 * <p>Bounded, by the stretch a change ring's reader waits the same refusal out. A refusal that outlasts it is a
 * cluster that has lost what it needs to accept writes, and the run ends with a code that says so rather than
 * sitting healthy and doing nothing.
 */
public final class ProtectedMap<K, V> {

    /** As long as a change ring's reader waits its refusal out: one stretch for every refusal the cluster clears. */
    public static final Duration REFUSAL_BOUND = Duration.ofSeconds(30);

    private static final long FIRST_PAUSE_MILLIS = 20;
    private static final long LONGEST_PAUSE_MILLIS = 500;

    /** How a refused operation waits before it asks again. */
    @FunctionalInterface
    interface Pause {
        void pause(long millis) throws InterruptedException;
    }

    private final IMap<K, V> map;
    private final LongSupplier nanoTime;
    private final Pause pause;
    private final Duration bound;

    private ProtectedMap(IMap<K, V> map, LongSupplier nanoTime, Pause pause, Duration bound) {
        this.map = Objects.requireNonNull(map, "map");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.pause = Objects.requireNonNull(pause, "pause");
        this.bound = Objects.requireNonNull(bound, "bound");
    }

    /** {@code map}, its operations waiting out the cluster's refusal for {@link #REFUSAL_BOUND}. */
    public static <K, V> ProtectedMap<K, V> of(IMap<K, V> map) {
        return new ProtectedMap<>(map, System::nanoTime, Thread::sleep, REFUSAL_BOUND);
    }

    /** The same, on a given clock, pause and bound, so the wait can be measured without spending it. */
    static <K, V> ProtectedMap<K, V> of(IMap<K, V> map, LongSupplier nanoTime, Pause pause, Duration bound) {
        return new ProtectedMap<>(map, nanoTime, pause, bound);
    }

    public String getName() {
        return map.getName();
    }

    public V get(K key) {
        return waitingOut(() -> map.get(key));
    }

    /** Waited out as a whole, which a read may be: asking again for keys already answered changes nothing. */
    public Map<K, V> getAll(Set<K> keys) {
        return waitingOut(() -> map.getAll(keys));
    }

    public boolean containsKey(K key) {
        return waitingOut(() -> map.containsKey(key));
    }

    public void set(K key, V value) {
        waitingOut(() -> {
            map.set(key, value);
            return null;
        });
    }

    public V put(K key, V value) {
        return waitingOut(() -> map.put(key, value));
    }

    public void delete(K key) {
        waitingOut(() -> {
            map.delete(key);
            return null;
        });
    }

    public <R> R executeOnKey(K key, EntryProcessor<K, V, R> processor) {
        return waitingOut(() -> map.executeOnKey(key, processor));
    }

    /**
     * Waited out as a whole, so only for a {@code processor} that changes nothing: the keys span partitions, and
     * the ones the cluster did not refuse have already run it by the time the refusal of another arrives.
     */
    public <R> Map<K, R> executeOnKeys(Set<K> keys, EntryProcessor<K, V, R> processor) {
        return waitingOut(() -> map.executeOnKeys(keys, processor));
    }

    /** This member's own statistics of the map, which no protection guards. */
    public LocalMapStats getLocalMapStats() {
        return map.getLocalMapStats();
    }

    /**
     * Runs {@code operation}, asking again after a short pause, growing up to half a second, for as long as the
     * cluster refuses it - and ending the run with a code once it has refused for the whole bound. Anything else
     * the operation raises is its own and goes on at once.
     */
    private <T> T waitingOut(Supplier<T> operation) {
        long refusedSince = 0;
        boolean refused = false;
        long pauseMillis = FIRST_PAUSE_MILLIS;
        while (true) {
            try {
                return operation.get();
            } catch (RuntimeException raised) {
                if (!isRefusal(raised)) {
                    throw raised;
                }
                long now = nanoTime.getAsLong();
                if (!refused) {
                    refused = true;
                    refusedSince = now;
                }
                if (now - (refusedSince + bound.toNanos()) >= 0) {
                    throw new TapstateException(EngineError.CLUSTER_REFUSED_THE_STATE,
                            Map.of("state", map.getName(), "seconds", bound.toSeconds()), raised);
                }
                try {
                    pause.pause(pauseMillis);
                } catch (InterruptedException interrupted) {
                    // The run is being stopped. The refusal is what this operation met, so it is what goes on.
                    Thread.currentThread().interrupt();
                    throw raised;
                }
                pauseMillis = Math.min(pauseMillis * 2, LONGEST_PAUSE_MILLIS);
            }
        }
    }

    /** Whether {@code raised} is the cluster's refusal, as itself or as the cause a wrapper carries. */
    static boolean isRefusal(Throwable raised) {
        for (Throwable cause = raised; cause != null; cause = cause.getCause()) {
            if (cause instanceof SplitBrainProtectionException) {
                return true;
            }
        }
        return false;
    }
}
