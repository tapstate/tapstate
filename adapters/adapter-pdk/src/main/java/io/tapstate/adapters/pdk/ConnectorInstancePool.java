package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectionConfig;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A bounded pool of live connector instances, keyed by {@link ConnectionFingerprint}. Opening a
 * connector is not one database connection: it is a fresh isolated class loader, a linked jar, a
 * constructed connector and — once initialized — whatever the connector builds at startup, which for
 * a database connector is a driver connection pool of its own. A face that opens one per query cannot
 * carry a caller that polls once a second, so instances are opened once, handed out one caller at a
 * time, and reused.
 *
 * <p>What the pool holds is an <em>initialized</em> instance. Opening alone builds no connection —
 * the driver's pool appears when the connector is initialized — so a pool that cached un-initialized
 * instances would rebuild that pool on every use and save nothing. The consequence runs the other
 * way too: an idle instance is holding real connections, so evicting it is a resource measure rather
 * than housekeeping.
 *
 * <p>Four limits, each closing a different failure:
 *
 * <ul>
 *   <li><b>per connection</b> — how many callers may be in flight against one set of settings at
 *       once, and therefore how many instances of it exist. An instance serves one caller at a time:
 *       the frozen contract gives an instance to a node, and nowhere promises a function may be
 *       called concurrently.
 *   <li><b>acquire</b> — how long a caller waits for a free instance, <em>fairly</em>. Fairness is
 *       what keeps a once-a-second poll from taking the instance back the moment it frees up and
 *       starving an interactive query, which would only ever see this timeout.
 *   <li><b>call</b> — how long one call may run before its instance is thrown away rather than
 *       returned. The call is still inside that instance, so handing it back would give the next
 *       caller a connector already stuck in someone else's query.
 *   <li><b>total</b> — the host-wide instance ceiling, counted in instances because each one
 *       multiplies out to a driver connection pool. Over the ceiling, the instance idle longest is
 *       closed to make room; with nothing idle to close, the request is refused.
 * </ul>
 *
 * <p>The pool holds no timer of its own: {@link #sweep()} is what evicts, and its owner is what
 * schedules it. Nothing else in here needs a thread, and a pool that started one would be a pool
 * every test has to race against.
 */
final class ConnectorInstancePool<T> implements AutoCloseable {

    /** One call against a checked-out instance; it may throw whatever the connector throws. */
    @FunctionalInterface
    interface PooledCall<T, R> {
        R run(T instance) throws Exception;
    }

    /** What the pool is bounded by; see the class comment for what each one closes. */
    record Limits(int perConnection, int total, Duration acquire, Duration call, Duration idle) {

        Limits withPerConnection(int value) {
            return new Limits(value, total, acquire, call, idle);
        }

        Limits withTotal(int value) {
            return new Limits(perConnection, value, acquire, call, idle);
        }

        Limits withAcquire(Duration value) {
            return new Limits(perConnection, total, value, call, idle);
        }

        Limits withCall(Duration value) {
            return new Limits(perConnection, total, acquire, value, idle);
        }

        Limits withIdle(Duration value) {
            return new Limits(perConnection, total, acquire, call, value);
        }
    }

    /**
     * The shipped limits. Three instances per connection bounds the connection multiplication a
     * database driver performs behind each one; sixteen bounds it across every connection at once.
     */
    static final Limits DEFAULTS = new Limits(
            3, 16, Duration.ofSeconds(2), Duration.ofSeconds(10), Duration.ofMinutes(5));

    private final Function<ConnectionConfig, T> open;
    private final Consumer<T> dispose;
    private final Limits limits;
    private final Clock clock;
    private final ExecutorService calls;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition capacityReturned = lock.newCondition();
    private final Map<String, Slot> slots = new LinkedHashMap<>();
    private final AtomicLong returns = new AtomicLong();
    private int live;
    private int closingReservations;
    private boolean closed;

    ConnectorInstancePool(Function<ConnectionConfig, T> open, Consumer<T> dispose, Limits limits, Clock clock) {
        this.open = open;
        this.dispose = dispose;
        this.limits = limits;
        this.clock = clock;
        // Calls run off the caller's thread so one that never returns can be abandoned at the call
        // limit; daemon threads so an abandoned call cannot hold the host open on the way out.
        this.calls = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "data-browser-call");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Runs {@code action} against an instance opened for {@code config}, reusing a pooled one when
     * there is one. The instance goes back to the pool afterwards — unless the call outran the call
     * limit, in which case it is thrown away instead.
     */
    <R> R call(ConnectionConfig config, PooledCall<T, R> action) {
        return call(config, action, ignored -> true);
    }

    <R> R call(ConnectionConfig config, PooledCall<T, R> action, Predicate<T> reusable) {
        Lease lease = acquire(config);
        R result;
        try {
            result = run(config, lease, action);
        } catch (Poisoned poisoned) {
            // The pool abandoned the call while it was still running inside this instance, so the
            // instance goes away with it. This is deliberately not "the call threw a code": the read
            // face reports a connector's own refusal that way too, and that connection is still fine.
            discard(lease);
            throw poisoned.coded;
        } catch (RuntimeException | Error failed) {
            // The connector reported a failure and is still healthy - a failed query is not a failed
            // connection - so the instance goes back. Treating every failure as poison would reopen a
            // connector for every bad query.
            if (reusable.test(lease.instance)) {
                release(lease);
            } else {
                discard(lease);
            }
            throw failed;
        }
        if (reusable.test(lease.instance)) {
            release(lease);
        } else {
            discard(lease);
        }
        return result;
    }

    /**
     * Closes every instance that has been idle at least as long as the idle limit, and lets go of any
     * connection left holding nothing afterwards.
     *
     * <p>The second half is why this runs even when nothing is idle to close. A slot is filed the first
     * time a set of settings is queried and outlives every instance behind it, so editing a source's
     * connection settings — an ordinary act — left one more semaphore and deque behind for the life of
     * the process, and made every ceiling eviction walk a list that only ever grew.
     */
    void sweep() {
        long now = clock.millis();
        List<T> evicted = new ArrayList<>();
        lock.lock();
        try {
            for (Slot slot : slots.values()) {
                slot.idle.removeIf(entry -> {
                    if (now - entry.idleSince < limits.idle().toMillis()) {
                        return false;
                    }
                    evicted.add(entry.instance);
                    slot.live--;
                    returnPlace();
                    return true;
                });
            }
            slots.values().removeIf(this::unused);
        } finally {
            lock.unlock();
        }
        evicted.forEach(this::disposeQuietly);
    }

    /**
     * Whether a slot can be let go: it holds no instance at all, neither idle nor checked out.
     *
     * <p>A caller on its way in may still hold a reference to it, and that deliberately does not count
     * here — whether someone is about to arrive is a question with no answer, since a caller reads the
     * slot before it does anything this class could observe. What makes dropping it safe sits on the
     * caller's side instead: it settles membership and books its room without letting go of the lock in
     * between, so a slot already let go is recognised rather than written to.
     */
    private boolean unused(Slot slot) {
        return slot.live == 0;
    }

    /** Closes every idle instance and refuses further calls; a call in flight keeps its instance. */
    @Override
    public void close() {
        List<T> remaining = new ArrayList<>();
        lock.lock();
        try {
            closed = true;
            capacityReturned.signalAll();
            for (Slot slot : slots.values()) {
                slot.idle.forEach(entry -> remaining.add(entry.instance));
                live -= slot.idle.size();
                slot.live -= slot.idle.size();
                slot.idle.clear();
            }
        } finally {
            lock.unlock();
        }
        remaining.forEach(this::disposeQuietly);
        calls.shutdownNow();
    }

    // ---- acquire / release -----------------------------------------------------------------------

    /** A booked place in the host-wide ceiling for an instance the pool does not hold. */
    interface Reservation extends AutoCloseable {

        /** Marks cleanup in progress without giving the place back. Idempotent. */
        void beginClose();

        /** Gives the place back. Idempotent. */
        @Override
        void close();
    }

    /**
     * Books one place in the host-wide ceiling for an instance this pool will never hold — a follow,
     * which keeps its connector for as long as somebody is watching rather than for one call.
     *
     * <p>It cannot be pooled and it must still be counted. An instance is an instance: it multiplies
     * out to the same driver connections whether a query or a watcher is holding it, so a ceiling
     * that only counted the pooled ones would be a ceiling anybody could walk around by following a
     * collection. Refused with the same code a read is refused with when nothing can be given up.
     *
     * <p>Nothing here evicts to make room. An idle pooled instance would be a fair thing to close for
     * a query, which is over in seconds; giving one up for a subscription that may hold its place for
     * hours trades a reusable instance for a parked one.
     */
    Reservation reserveOutsidePool() {
        lock.lock();
        try {
            // A closed follow may still be releasing its source cursor. Keep that instance counted,
            // but let a replacement wait for its cleanup, bounded by the existing call limit.
            // A ceiling filled by active follows still refuses immediately.
            long remaining = limits.call().toNanos();
            while (!closed && live >= limits.total() && closingReservations > 0 && remaining > 0) {
                try {
                    remaining = capacityReturned.awaitNanos(remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw instanceLimit(interrupted);
                }
            }
            if (closed || live >= limits.total()) {
                throw instanceLimit(null);
            }
            live++;
            return new OutsideReservation();
        } finally {
            lock.unlock();
        }
    }

    private TapstateException instanceLimit(Throwable cause) {
        return new TapstateException(ConnectorError.INSTANCE_LIMIT_REACHED,
                Map.of("limit", String.valueOf(limits.total())), cause);
    }

    private final class OutsideReservation implements Reservation {
        private boolean closing;
        private boolean released;

        @Override
        public void beginClose() {
            lock.lock();
            try {
                if (!released && !closing) {
                    closing = true;
                    closingReservations++;
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() {
            lock.lock();
            try {
                if (released) {
                    return;
                }
                released = true;
                if (closing) {
                    closingReservations--;
                }
                returnPlace();
            } finally {
                lock.unlock();
            }
        }
    }

    /** How many connections this pool is still keeping bookkeeping for. */
    int trackedConnections() {
        lock.lock();
        try {
            return slots.size();
        } finally {
            lock.unlock();
        }
    }

    /** How many instances this host is holding, pooled and reserved alike. */
    int liveInstances() {
        lock.lock();
        try {
            return live;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Takes one caller's turn against {@code config}: an idle instance if there is one, a fresh one if
     * the ceilings allow, and a coded refusal if neither is true within the acquire limit.
     */
    private Lease acquire(ConnectionConfig config) {
        String key = ConnectionFingerprint.of(config);
        // One deadline across every attempt, so re-reading a reclaimed slot cannot multiply the wait a
        // caller was promised.
        long deadline = System.nanoTime() + limits.acquire().toNanos();
        while (true) {
            Slot slot = slotFor(key);
            // Fair, so the turn goes to whoever has waited longest rather than to whoever asks most often.
            boolean permitted;
            try {
                permitted = slot.turns.tryAcquire(
                        Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw busy(config, e);
            }
            if (!permitted) {
                throw busy(config, null);
            }
            boolean handedOut = false;
            try {
                Claim<T> claim = claim(key, slot);
                if (claim == null) {
                    // The slot was let go between looking it up and this turn becoming ours. Nothing was
                    // counted against it, so the turn goes back and the next pass takes its replacement.
                    continue;
                }
                if (claim.pooled() != null) {
                    handedOut = true;
                    return new Lease(slot, claim.pooled());
                }
                disposeQuietly(claim.evicted());
                T fresh;
                try {
                    fresh = open.apply(config);
                } catch (RuntimeException | Error e) {
                    unreserve(slot);
                    throw e;
                }
                handedOut = true;
                return new Lease(slot, fresh);
            } finally {
                if (!handedOut) {
                    slot.turns.release();
                }
            }
        }
    }

    /**
     * What one turn is worth: an idle instance to reuse, or room booked for a fresh one — together with
     * whatever the ceiling had to give up to make that room. Null when the slot is no longer the one
     * filed under {@code key}, so this turn buys nothing.
     *
     * <p>Settling all of that without letting go of the lock is the point. Checking membership and then
     * booking room in a second lock hold leaves a gap a sweep fits inside, and a slot dropped in that gap
     * takes a booked instance with it: counted on the way in, belonging to nothing on the way out, and
     * invisible to every later sweep — so the host-wide count never falls again, and the ceiling
     * eventually closes on a pool that is actually empty.
     */
    private Claim<T> claim(String key, Slot slot) {
        lock.lock();
        try {
            if (slots.get(key) != slot) {
                return null;
            }
            Idle<T> pooled = slot.idle.pollFirst();
            if (pooled != null) {
                return new Claim<>(pooled.instance(), null);
            }
            T evicted = null;
            if (live >= limits.total()) {
                evicted = takeIdleLongest();
                if (evicted == null) {
                    throw new TapstateException(ConnectorError.INSTANCE_LIMIT_REACHED,
                            Map.of("limit", String.valueOf(limits.total())), null);
                }
            }
            slot.live++;
            live++;
            return new Claim<>(null, evicted);
        } finally {
            lock.unlock();
        }
    }


    private Slot slotFor(String key) {
        lock.lock();
        try {
            return slots.computeIfAbsent(key, ignored -> new Slot(limits.perConnection()));
        } finally {
            lock.unlock();
        }
    }

    private void unreserve(Slot slot) {
        lock.lock();
        try {
            slot.live--;
            returnPlace();
        } finally {
            lock.unlock();
        }
    }

    /** Returns one place and wakes admission waiting for cleanup. The caller holds the pool lock. */
    private void returnPlace() {
        live--;
        capacityReturned.signalAll();
    }

    /** Removes and returns the instance idle longest across every connection, or null when none is. */
    private T takeIdleLongest() {
        Slot oldestSlot = null;
        Idle<T> oldest = null;
        for (Slot slot : slots.values()) {
            Idle<T> candidate = slot.idle.peekLast();
            // Ordered by when each was returned rather than by the clock: two instances returned inside
            // one clock tick are still a definite order, and eviction has to pick exactly one of them.
            if (candidate != null && (oldest == null || candidate.returned < oldest.returned)) {
                oldest = candidate;
                oldestSlot = slot;
            }
        }
        if (oldest == null) {
            return null;
        }
        oldestSlot.idle.pollLast();
        oldestSlot.live--;
        returnPlace();
        return oldest.instance;
    }

    /** Returns a healthy instance to its slot, warmest first, and gives up the caller's turn. */
    private void release(Lease lease) {
        boolean keep;
        lock.lock();
        try {
            keep = !closed;
            if (keep) {
                lease.slot.idle.addFirst(new Idle<>(lease.instance, clock.millis(), returns.incrementAndGet()));
            } else {
                lease.slot.live--;
                returnPlace();
            }
        } finally {
            lock.unlock();
        }
        lease.slot.turns.release();
        if (!keep) {
            disposeQuietly(lease.instance);
        }
    }

    /** Throws an instance away instead of pooling it, and gives up the caller's turn. */
    private void discard(Lease lease) {
        lock.lock();
        try {
            lease.slot.live--;
            returnPlace();
        } finally {
            lock.unlock();
        }
        lease.slot.turns.release();
        disposeQuietly(lease.instance);
    }

    private void disposeQuietly(T instance) {
        if (instance == null) {
            return;
        }
        try {
            dispose.accept(instance);
        } catch (RuntimeException | Error ignore) {
            // best-effort teardown: an instance being thrown away must not mask the outcome already
            // being returned or thrown, and there is nothing left to retry against
        }
    }

    // ---- running one call ------------------------------------------------------------------------

    /**
     * Runs the call on its own thread and waits out the call limit. Abandoning it there is the only
     * way to bound it: a connector's read blocks the thread it runs on, so a limit on the caller's own
     * thread could only be checked after the read it was meant to bound had already returned.
     */
    private <R> R run(ConnectionConfig config, Lease lease, PooledCall<T, R> action) {
        Future<R> running;
        try {
            running = calls.submit(() -> action.run(lease.instance));
        } catch (RejectedExecutionException e) {
            throw new TapstateException(ConnectorError.READ_FAILED,
                    Map.of("connector", config.connectorId(), "detail", "the read face is shutting down"), e);
        }
        try {
            return running.get(limits.call().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            running.cancel(true);
            throw new Poisoned(new TapstateException(ConnectorError.READ_TIMEOUT,
                    Map.of("connector", config.connectorId(), "timeout", millis(limits.call())), e));
        } catch (InterruptedException e) {
            running.cancel(true);
            Thread.currentThread().interrupt();
            // The call is still running inside the instance, exactly as at the call limit, so this
            // leaves through the path that discards it rather than the one that pools it.
            throw new Poisoned(new TapstateException(ConnectorError.READ_FAILED,
                    Map.of("connector", config.connectorId(), "detail", "the read was interrupted"), e));
        } catch (ExecutionException e) {
            throw rethrow(config, e.getCause());
        }
    }

    /**
     * Re-raises what the call threw. A coded failure and an unchecked one carry their own diagnosis
     * and pass through unchanged; anything else is given the read-failure code so it does not reach a
     * user as a bare stack.
     */
    private static RuntimeException rethrow(ConnectionConfig config, Throwable cause) {
        if (cause instanceof RuntimeException unchecked) {
            throw unchecked;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new TapstateException(ConnectorError.READ_FAILED,
                Map.of("connector", config.connectorId(), "detail", PdkSchemaDiscoverer.detail(cause)), cause);
    }

    private TapstateException busy(ConnectionConfig config, Throwable cause) {
        return new TapstateException(ConnectorError.INSTANCES_BUSY,
                Map.of("connector", config.connectorId(), "timeout", millis(limits.acquire())), cause);
    }

    private static String millis(Duration duration) {
        return duration.toMillis() + "ms";
    }

    // ---- state -----------------------------------------------------------------------------------

    /**
     * The pool abandoned a call that is still running inside its instance, so that instance must go.
     * A carrier rather than a code, because what reaches the caller is the code it wraps: whether an
     * instance is poisoned is the pool's own judgement, and cannot be read off the exception type the
     * call failed with — the read face reports a connector's own refusal as a code as well.
     */
    private static final class Poisoned extends RuntimeException {
        private final transient TapstateException coded;

        private Poisoned(TapstateException coded) {
            super(coded);
            this.coded = coded;
        }
    }

    /** What a turn bought: an instance to reuse, or room for a fresh one plus whatever was evicted. */
    private record Claim<X>(X pooled, X evicted) {
    }


    /** One caller's exclusive hold on one instance. */
    private final class Lease {
        private final Slot slot;
        private final T instance;

        private Lease(Slot slot, T instance) {
            this.slot = slot;
            this.instance = instance;
        }
    }

    /** Everything filed under one key: whose turn it is, what is idle, and how many exist. */
    private final class Slot {
        private final Semaphore turns;
        private final Deque<Idle<T>> idle = new ArrayDeque<>();
        private int live;

        private Slot(int perConnection) {
            this.turns = new Semaphore(perConnection, true);
        }
    }

    /** A pooled instance nobody holds: when it went idle, and in what order it was returned. */
    private record Idle<T>(T instance, long idleSince, long returned) {
    }
}
