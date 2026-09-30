package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectionConfig;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The pool of live connector instances: what it reuses, what it refuses, and what it throws away.
 * Every property here is an in-process event no end-to-end run can observe, and each one fails
 * silently if it is wrong — a stale instance keeps answering from the database it was opened against,
 * a poisoned instance is handed to the next caller still stuck in the last one's query, and an
 * un-evicted idle instance holds its driver's connection pool open forever.
 *
 * <p>Instances here are stand-ins, not connectors: the pool's whole contract is about counting,
 * ordering and discarding the things it holds, so the tests turn on how many were opened and which
 * one came back, never on anything a connector does.
 */
class ConnectorInstancePoolTest {

    private static final Duration NEVER = Duration.ofHours(1);

    private final Instances instances = new Instances();
    private final MovableClock clock = new MovableClock();
    private final List<ConnectorInstancePool<FakeInstance>> pools = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();

    @AfterEach
    void closePools() throws InterruptedException {
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(10));
        }
        pools.forEach(ConnectorInstancePool::close);
    }

    // ---- what one key means ----------------------------------------------------------------------

    @Test
    void reusesOneInstanceAcrossCallsForTheSameConnection() {
        // The whole point of the pool: a temporary query must not cost a class loader, a connector
        // construction and a driver connection pool every time it runs.
        ConnectorInstancePool<FakeInstance> pool = pool(limits());

        pool.call(config("conn-1", "mongodb://host/db"), instance -> instance);
        pool.call(config("conn-1", "mongodb://host/db"), instance -> instance);

        assertThat(instances.opened).hasSize(1);
    }

    @Test
    void opensANewInstanceOnceTheConnectionSettingsChange() {
        // An applied config change must reach the next query. An instance kept across it answers from
        // the database it was opened against and reports no error at all, which is the worst shape a
        // wrong answer can take.
        ConnectorInstancePool<FakeInstance> pool = pool(limits());

        pool.call(config("conn-1", "mongodb://host/db"), instance -> instance);
        pool.call(config("conn-1", "mongodb://host/other"), instance -> instance);

        assertThat(instances.opened).hasSize(2);
    }

    // ---- idle eviction ---------------------------------------------------------------------------

    @Test
    void closesAnInstanceThatSatIdlePastTheIdleTimeout() {
        // An idle instance is not merely untidy: it holds its driver's connection pool open, so
        // eviction is how the host gives those connections back.
        ConnectorInstancePool<FakeInstance> pool = pool(limits().withIdle(Duration.ofMinutes(5)));
        FakeInstance first = pool.call(config("conn-1", "mongodb://host/db"), instance -> instance);

        clock.advance(Duration.ofMinutes(5));
        pool.sweep();

        assertThat(instances.disposed).containsExactly(first);
    }

    @Test
    void closesAnEvictedInstanceOnlyOnce() {
        // A second sweep must find nothing left to evict: disposing twice stops and closes a connector
        // that is already gone, and the second stop runs against a connector the driver has torn down.
        ConnectorInstancePool<FakeInstance> pool = pool(limits().withIdle(Duration.ofMinutes(5)));
        pool.call(config("conn-1", "mongodb://host/db"), instance -> instance);
        clock.advance(Duration.ofMinutes(5));
        pool.sweep();

        pool.sweep();

        assertThat(instances.disposed).hasSize(1);
    }

    @Test
    void keepsAnInstanceThatHasNotBeenIdleLongEnough() {
        // The control for the two above: a sweep that evicts on every pass would make the pool a
        // no-op while still passing the eviction tests.
        ConnectorInstancePool<FakeInstance> pool = pool(limits().withIdle(Duration.ofMinutes(5)));
        pool.call(config("conn-1", "mongodb://host/db"), instance -> instance);

        clock.advance(Duration.ofMinutes(4));
        pool.sweep();

        assertThat(instances.disposed).isEmpty();
    }

    @Test
    void opensAFreshInstanceAfterTheIdleOneWasEvicted() {
        ConnectorInstancePool<FakeInstance> pool = pool(limits().withIdle(Duration.ofMinutes(5)));
        pool.call(config("conn-1", "mongodb://host/db"), instance -> instance);
        clock.advance(Duration.ofMinutes(5));
        pool.sweep();

        pool.call(config("conn-1", "mongodb://host/db"), instance -> instance);

        assertThat(instances.opened).hasSize(2);
    }

    // ---- acquire timeout and fairness ------------------------------------------------------------

    @Test
    void refusesWithACodeWhenEveryInstanceStaysBusyForTheAcquireTimeout() throws Exception {
        // The alternative is waiting forever: a caller with no timeout parks behind a query that may
        // never end, and reports nothing at all while it does.
        ConnectorInstancePool<FakeInstance> pool =
                pool(limits().withPerConnection(1).withAcquire(Duration.ofMillis(200)));
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = run(() -> pool.call(config("conn-1", "mongodb://host/db"), instance -> {
            release.await();
            return instance;
        }));
        awaitParked(holder);

        try {
            assertThatThrownBy(() -> pool.call(config("conn-1", "mongodb://host/db"), instance -> instance))
                    .isInstanceOf(TapstateException.class)
                    .extracting(e -> ((TapstateException) e).code().code())
                    .isEqualTo("connector.instances-busy");
        } finally {
            release.countDown();
        }
    }

    @Test
    void handsTheFreedInstanceToWhoeverHasWaitedLongest() throws Exception {
        // Fairness is what keeps a once-a-second poll from starving an interactive query: an unfair
        // queue lets the poll take the instance again the moment it frees up, and the interactive
        // caller only ever sees the acquire timeout.
        ConnectorInstancePool<FakeInstance> pool =
                pool(limits().withPerConnection(1).withAcquire(Duration.ofSeconds(30)));
        CountDownLatch release = new CountDownLatch(1);
        List<String> served = Collections.synchronizedList(new ArrayList<>());
        Thread holder = run(() -> pool.call(config("conn-1", "mongodb://host/db"), instance -> {
            release.await();
            return instance;
        }));
        awaitParked(holder);
        for (String name : List.of("first", "second", "third", "fourth")) {
            Thread waiter = run(() -> pool.call(config("conn-1", "mongodb://host/db"), instance -> served.add(name)));
            awaitParked(waiter);
        }

        release.countDown();
        joinAll();

        assertThat(served).containsExactly("first", "second", "third", "fourth");
    }

    @Test
    void aCallerArrivingLastDoesNotTakeTheTurnAheadOfOneAlreadyWaiting() throws Exception {
        // The starvation this is aimed at: a caller reading on a timer does not wait in line, it arrives
        // again - and every arrival is another chance to take the turn back the moment it frees. Whoever
        // was already waiting is then never served and sees only the acquire timeout.
        //
        // What this actually pins is weaker than that, and the gap is worth stating rather than
        // implying: it proves the turn goes to the waiter that arrived first, which an unfair queue also
        // does once that waiter is enqueued. Only a caller landing inside the window between the turn
        // being freed and the waiter taking it can barge, and that window cannot be opened on demand
        // from outside - so the fair flag on the queue is NOT witnessed by this test or any other here.
        // It is a one-flag guarantee of the queue itself; what these two tests cover is that every
        // waiter is served, in arrival order.
        ConnectorInstancePool<FakeInstance> pool =
                pool(limits().withPerConnection(1).withAcquire(Duration.ofSeconds(30)));
        CountDownLatch release = new CountDownLatch(1);
        List<String> served = Collections.synchronizedList(new ArrayList<>());
        Thread holder = run(() -> pool.call(config("conn-1", "mongodb://host/db"), instance -> {
            release.await();
            return instance;
        }));
        awaitParked(holder);
        Thread waiting = run(() -> pool.call(config("conn-1", "mongodb://host/db"), instance -> served.add("waiting")));
        awaitParked(waiting);

        release.countDown();
        holder.join(TimeUnit.SECONDS.toMillis(10));
        pool.call(config("conn-1", "mongodb://host/db"), instance -> served.add("arrived last"));

        assertThat(served).containsExactly("waiting", "arrived last");
    }

    // ---- the call timeout and the instance it poisons ---------------------------------------------

    @Test
    void refusesWithACodeWhenACallOutlastsTheCallTimeout() {
        ConnectorInstancePool<FakeInstance> pool = pool(limits().withCall(Duration.ofMillis(200)));

        assertThatThrownBy(() -> pool.call(config("conn-1", "mongodb://host/db"), instance -> {
            Thread.sleep(TimeUnit.SECONDS.toMillis(30));
            return instance;
        }))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code().code())
                .isEqualTo("connector.read-timeout");
    }

    @Test
    void throwsAwayTheInstanceACallTimedOutOn() {
        // The call is still running inside that instance, so returning it to the pool hands the next
        // caller a connector already stuck in someone else's query - and that caller then waits out its
        // own timeout for a reason it has no way to see.
        ConnectorInstancePool<FakeInstance> pool = pool(limits().withCall(Duration.ofMillis(200)));
        assertThatThrownBy(() -> pool.call(config("conn-1", "mongodb://host/db"), instance -> {
            Thread.sleep(TimeUnit.SECONDS.toMillis(30));
            return instance;
        })).isInstanceOf(TapstateException.class);

        FakeInstance next = pool.call(config("conn-1", "mongodb://host/db"), instance -> instance);

        assertThat(instances.opened).hasSize(2);
        assertThat(next).isNotSameAs(instances.opened.get(0));
        assertThat(instances.disposed).containsExactly(instances.opened.get(0));
    }

    @Test
    void keepsTheInstanceWhenTheCallItselfFails() {
        // The control for the one above. A connector that reports a failed query is healthy - the
        // connection is fine and the next caller may use it - so treating every failure as poison would
        // turn one bad query into a reopened connector every time.
        ConnectorInstancePool<FakeInstance> pool = pool(limits());
        assertThatThrownBy(() -> pool.call(config("conn-1", "mongodb://host/db"), instance -> {
            throw new IllegalStateException("the connector said no");
        })).isInstanceOf(IllegalStateException.class);

        pool.call(config("conn-1", "mongodb://host/db"), instance -> instance);

        assertThat(instances.opened).hasSize(1);
        assertThat(instances.disposed).isEmpty();
    }

    @Test
    void keepsTheInstanceWhenTheCallFailsWithACode() {
        // The read face reports a connector's own failure as a coded error, and the pool raises its
        // ceilings and limits as coded errors too - so "it threw a code" cannot be what decides that an
        // instance is poisoned. Only the pool's own abandonment can, and this is the case that tells the
        // two apart: a query the connector refused leaves the connection perfectly usable.
        ConnectorInstancePool<FakeInstance> pool = pool(limits());
        assertThatThrownBy(() -> pool.call(config("conn-1", "mongodb://host/db"), instance -> {
            throw new TapstateException(ConnectorError.READ_FAILED,
                    Map.of("connector", "mongodb", "detail", "the connector said no"), null);
        })).isInstanceOf(TapstateException.class);

        pool.call(config("conn-1", "mongodb://host/db"), instance -> instance);

        assertThat(instances.opened).hasSize(1);
        assertThat(instances.disposed).isEmpty();
    }

    // ---- the host-wide instance cap --------------------------------------------------------------

    @Test
    void evictsAnIdleInstanceOfAnotherConnectionToStayUnderTheHostCap() {
        // The cap counts instances, not callers: three instances each carry a driver connection pool,
        // so the ceiling is what bounds how many database connections the host can multiply out to.
        ConnectorInstancePool<FakeInstance> pool = pool(limits().withTotal(2));
        pool.call(config("conn-1", "mongodb://host/one"), instance -> instance);
        pool.call(config("conn-2", "mongodb://host/two"), instance -> instance);

        pool.call(config("conn-3", "mongodb://host/three"), instance -> instance);

        assertThat(instances.opened).hasSize(3);
        assertThat(instances.disposed).containsExactly(instances.opened.get(0));
    }

    @Test
    void refusesWithACodeWhenTheHostCapIsReachedAndEveryInstanceIsBusy() throws Exception {
        // Nothing is evictable here, so the honest answer is a refusal naming the ceiling rather than a
        // silent extra instance over it.
        ConnectorInstancePool<FakeInstance> pool = pool(limits().withTotal(1).withAcquire(NEVER));
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = run(() -> pool.call(config("conn-1", "mongodb://host/one"), instance -> {
            release.await();
            return instance;
        }));
        awaitParked(holder);

        try {
            assertThatThrownBy(() -> pool.call(config("conn-2", "mongodb://host/two"), instance -> instance))
                    .isInstanceOf(TapstateException.class)
                    .extracting(e -> ((TapstateException) e).code().code())
                    .isEqualTo("connector.instance-limit-reached");
        } finally {
            release.countDown();
        }
    }

    // ---- fixtures --------------------------------------------------------------------------------

    private ConnectorInstancePool<FakeInstance> pool(ConnectorInstancePool.Limits limits) {
        ConnectorInstancePool<FakeInstance> pool =
                new ConnectorInstancePool<>(instances::open, instances::dispose, limits, clock);
        pools.add(pool);
        return pool;
    }

    private static ConnectorInstancePool.Limits limits() {
        return new ConnectorInstancePool.Limits(3, 16, NEVER, NEVER, NEVER);
    }

    private static ConnectionConfig config(String id, String uri) {
        return new ConnectionConfig(id, "mongodb", Map.of("uri", uri));
    }

    /** Starts {@code body} on its own thread, swallowing the coded refusals a test asserts elsewhere. */
    private Thread run(Body body) {
        Thread thread = new Thread(() -> {
            try {
                body.run();
            } catch (RuntimeException | InterruptedException ignored) {
                // the assertions are on what the pool opened and disposed, not on this thread's outcome
            }
        });
        threads.add(thread);
        thread.start();
        return thread;
    }

    private void joinAll() throws InterruptedException {
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(10));
        }
    }

    /**
     * Waits until {@code thread} has parked — either inside the pool's queue or inside a call the test
     * is holding open. Waiting on the state rather than on a sleep is what makes the fairness test's
     * arrival order a fact rather than a hope.
     */
    private static void awaitParked(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            Thread.State state = thread.getState();
            if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("thread never parked, it is " + thread.getState());
    }

    @FunctionalInterface
    private interface Body {
        void run() throws InterruptedException;
    }

    /** A stand-in for one live connector instance; the pool only ever opens, hands out and disposes it. */
    private static final class FakeInstance {
    }

    /** Records what the pool opened and what it disposed, in order. */
    private static final class Instances {
        private final List<FakeInstance> opened = Collections.synchronizedList(new ArrayList<>());
        private final List<FakeInstance> disposed = Collections.synchronizedList(new ArrayList<>());

        FakeInstance open(ConnectionConfig config) {
            FakeInstance instance = new FakeInstance();
            opened.add(instance);
            return instance;
        }

        void dispose(FakeInstance instance) {
            disposed.add(instance);
        }
    }

    /** A clock the test moves by hand, so idle eviction is decided rather than waited out. */
    private static final class MovableClock extends Clock {
        private volatile Instant now = Instant.parse("2026-08-13T00:00:00Z");

        void advance(Duration by) {
            now = now.plus(by);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    @Test
    @DisplayName("an instance the pool does not hold still counts against the host-wide ceiling")
    void aReservationCountsTowardsTheCeiling() {
        // A follow keeps its connector for as long as somebody watches rather than for one call, so it
        // cannot be pooled -- and it is still an instance, multiplying out to the same driver
        // connections. A ceiling that counted only the pooled ones would be a ceiling anybody could
        // walk around by following a collection instead of reading it.
        ConnectorInstancePool<FakeInstance> pool = pool(ConnectorInstancePool.DEFAULTS.withTotal(2));

        pool.reserveOutsidePool();
        pool.reserveOutsidePool();

        assertThat(pool.liveInstances()).isEqualTo(2);
        assertThatThrownBy(pool::reserveOutsidePool)
                .isInstanceOf(TapstateException.class)
                .satisfies(refused -> assertThat(((TapstateException) refused).code().code())
                        .isEqualTo("connector.instance-limit-reached"));
    }

    @Test
    @DisplayName("closing a reservation gives its place back, and closing it twice gives back only one")
    void closingAReservationReleasesExactlyOnePlace() {
        ConnectorInstancePool<FakeInstance> pool = pool(ConnectorInstancePool.DEFAULTS.withTotal(1));
        ConnectorInstancePool.Reservation held = pool.reserveOutsidePool();

        held.close();
        held.close();

        assertThat(pool.liveInstances())
                .as("a close that gave back two places would let the next caller past a full ceiling; "
                        + "one that gave back none is the leak this exists to stop")
                .isZero();
        assertThat(pool.reserveOutsidePool()).isNotNull();
    }

    @Test
    void aReplacementWaitsForAClosingReservationWithoutExceedingTheCeiling() throws Exception {
        ConnectorInstancePool<FakeInstance> pool = pool(ConnectorInstancePool.DEFAULTS.withTotal(1));
        ConnectorInstancePool.Reservation held = pool.reserveOutsidePool();
        held.beginClose();
        held.beginClose();
        AtomicReference<ConnectorInstancePool.Reservation> replacement = new AtomicReference<>();
        Thread waiting = run(() -> replacement.set(pool.reserveOutsidePool()));
        try {
            awaitParked(waiting);
            assertThat(pool.liveInstances()).as("cleanup still holds its physical instance").isEqualTo(1);
            assertThat(replacement.get()).as("the replacement cannot open before cleanup finishes").isNull();
        } finally {
            held.close();
        }
        joinAll();
        assertThat(replacement.get()).as("the returned place admits the waiting follow").isNotNull();
        assertThat(pool.liveInstances()).isEqualTo(1);
        held.beginClose();
        long refusing = System.nanoTime();
        assertThatThrownBy(pool::reserveOutsidePool).isInstanceOf(TapstateException.class);
        assertThat(Duration.ofNanos(System.nanoTime() - refusing))
                .as("duplicate cleanup notifications cannot make an active ceiling wait")
                .isLessThan(Duration.ofSeconds(2));
        replacement.get().close();
        assertThat(pool.liveInstances()).isZero();
    }

    @Test
    void anUnfinishedCloseCannotMakeReservationAdmissionWaitWithoutABound() {
        ConnectorInstancePool<FakeInstance> pool = pool(ConnectorInstancePool.DEFAULTS
                .withTotal(1).withCall(Duration.ofMillis(100)));
        try (ConnectorInstancePool.Reservation held = pool.reserveOutsidePool()) {
            held.beginClose();
            long started = System.nanoTime();
            assertThatThrownBy(pool::reserveOutsidePool)
                    .isInstanceOf(TapstateException.class)
                    .satisfies(refused -> assertThat(((TapstateException) refused).code())
                            .isEqualTo(ConnectorError.INSTANCE_LIMIT_REACHED));
            assertThat(Duration.ofNanos(System.nanoTime() - started))
                    .isGreaterThanOrEqualTo(Duration.ofMillis(100))
                    .isLessThan(Duration.ofSeconds(5));
            assertThat(pool.liveInstances()).as("a timeout never frees the unfinished instance").isEqualTo(1);
        }
    }

    @Test
    void returnedPooledCapacityWakesAReplacementWaitingForCleanup() throws Exception {
        ConnectorInstancePool<FakeInstance> pool = pool(ConnectorInstancePool.DEFAULTS
                .withTotal(2).withIdle(Duration.ZERO));
        pool.call(config("pooled", "mongodb://one"), instance -> instance);
        AtomicReference<ConnectorInstancePool.Reservation> replacement = new AtomicReference<>();
        try (ConnectorInstancePool.Reservation held = pool.reserveOutsidePool()) {
            held.beginClose();
            Thread waiting = run(() -> replacement.set(pool.reserveOutsidePool()));
            awaitParked(waiting);
            pool.sweep();
            waiting.join(TimeUnit.SECONDS.toMillis(1));
            assertThat(waiting.isAlive()).as("returned pooled capacity must wake admission immediately").isFalse();
            assertThat(replacement.get()).isNotNull();
            assertThat(pool.liveInstances()).as("the unfinished close and its replacement are still counted")
                    .isEqualTo(2);
        }
        joinAll();
        replacement.get().close();
        assertThat(pool.liveInstances()).isZero();
    }

    @Test
    void closingThePoolWakesAReservationWaitingForCleanup() throws Exception {
        ConnectorInstancePool<FakeInstance> pool = pool(ConnectorInstancePool.DEFAULTS.withTotal(1));
        try (ConnectorInstancePool.Reservation held = pool.reserveOutsidePool()) {
            held.beginClose();
            AtomicReference<Throwable> refusal = new AtomicReference<>();
            Thread waiting = run(() -> {
                try {
                    pool.reserveOutsidePool();
                } catch (TapstateException closed) {
                    refusal.set(closed);
                }
            });
            awaitParked(waiting);
            pool.close();
            waiting.join(TimeUnit.SECONDS.toMillis(1));
            assertThat(waiting.isAlive()).as("pool shutdown ends admission without waiting for cleanup").isFalse();
            assertThat(refusal.get()).isInstanceOf(TapstateException.class);
            assertThat(((TapstateException) refusal.get()).code()).isEqualTo(ConnectorError.INSTANCE_LIMIT_REACHED);
            assertThat(pool.liveInstances()).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("a reservation does not evict a pooled instance to make room")
    void aReservationDoesNotEvictToMakeRoom() {
        // An idle pooled instance is a fair thing to close for a query, which is over in seconds.
        // Giving one up for a follow that may hold its place for hours trades a reusable instance for
        // a parked one.
        ConnectorInstancePool<FakeInstance> pool = pool(ConnectorInstancePool.DEFAULTS.withTotal(1));
        pool.call(config("a", "mongodb://one"), instance -> instance);   // leaves one idle instance behind

        assertThatThrownBy(pool::reserveOutsidePool)
                .isInstanceOf(TapstateException.class);
        assertThat(pool.liveInstances())
                .as("and the pooled instance is still there, not closed on the way to refusing")
                .isEqualTo(1);
    }

    // ---- forgetting a connection nobody uses any more ---------------------------------------------

    @Test
    @DisplayName("a connection whose last instance was evicted stops being tracked")
    void forgetsAConnectionOnceItsLastInstanceIsEvicted() {
        // Every distinct set of settings ever queried filed a slot here -- a fair semaphore and a deque
        // -- and nothing ever removed one. Editing a source's connection settings is an ordinary act, so
        // the bookkeeping grew for the life of the process while the instances behind it were long gone,
        // and every eviction had to walk the whole of it to find the instance idle longest.
        ConnectorInstancePool<FakeInstance> pool = pool(limits().withIdle(Duration.ofMinutes(5)));
        pool.call(config("conn-1", "mongodb://host/db"), instance -> instance);
        pool.call(config("conn-1", "mongodb://host/other"), instance -> instance);
        assertThat(pool.trackedConnections())
                .as("two distinct settings were queried, so both are held before anything is evicted")
                .isEqualTo(2);

        clock.advance(Duration.ofMinutes(5));
        pool.sweep();

        assertThat(pool.trackedConnections()).isZero();
    }

    @Test
    @DisplayName("a connection someone is still holding an instance of survives a sweep")
    void keepsAConnectionWhoseInstanceIsCheckedOut() throws Exception {
        // The control for the one above, and the reason the obvious fix is wrong: a slot is not free to
        // drop merely because it has nothing idle in it. Dropping one out from under a caller leaves the
        // instance to come back to bookkeeping nothing tracks, and the host-wide count never falls again.
        ConnectorInstancePool<FakeInstance> pool = pool(limits().withIdle(Duration.ofMinutes(5)));
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = run(() -> pool.call(config("conn-1", "mongodb://host/db"), instance -> {
            release.await();
            return instance;
        }));
        awaitParked(holder);

        clock.advance(Duration.ofMinutes(5));
        pool.sweep();
        int trackedWhileHeld = pool.trackedConnections();
        release.countDown();
        joinAll();

        assertThat(trackedWhileHeld)
                .as("the caller is still inside that instance, so its connection is still in use")
                .isEqualTo(1);
        assertThat(pool.liveInstances())
                .as("and the instance it hands back is still counted against the host-wide ceiling")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("sweeps running against live callers leave the instance count honest")
    void keepsTheInstanceCountHonestWhenSweepsRaceCallers() throws Exception {
        // A caller looks its connection up and only then waits its turn, so a sweep can land in between.
        // Reclaiming the slot in that window hands the caller bookkeeping the pool has already let go of:
        // the instance it opens is counted on the way in and belongs to nothing on the way out, so the
        // host-wide count only ever rises and the ceiling eventually closes on an empty pool. Nothing
        // here is timing-sensitive to assert -- the count either comes back to zero or it does not.
        ConnectorInstancePool<FakeInstance> pool = pool(limits().withIdle(Duration.ZERO));
        for (int caller = 0; caller < 8; caller++) {
            int which = caller;
            run(() -> {
                for (int round = 0; round < 60; round++) {
                    pool.call(config("conn-1", "mongodb://host/db-" + (which % 3)), instance -> instance);
                }
            });
        }
        for (int pass = 0; pass < 400; pass++) {
            pool.sweep();
        }
        joinAll();

        pool.sweep();

        assertThat(pool.liveInstances())
                .as("every instance opened during the race was either pooled and evicted, or handed back")
                .isZero();
        assertThat(pool.trackedConnections())
                .as("and no connection is left behind holding bookkeeping for instances that are gone")
                .isZero();
    }
}
