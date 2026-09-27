package io.tapstate.runtime.srs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.event.Envelope;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * The member-local snapshot buffer: the seam that carries a source's bounded snapshot rows from the capture
 * side to the source vertex so they can be emitted ahead of the cdc tail. It buffers per consumer pipeline
 * and ring name, drains that consumer's rows once in append order, and isolates both coordinates.
 */
class SnapshotBufferTest {

    private static final String PIPELINE = "orders_pipeline";

    @Test
    @SuppressWarnings("unchecked")
    void retainsALaterAppendWhenAnEmptyDrainRacesWithQueueInsertion() throws Exception {
        SnapshotBuffer buffer = new SnapshotBuffer();
        String ring = "srs.chain.late";
        Envelope first = row("orders", 100);
        Envelope later = row("orders", 101);
        buffer.append(PIPELINE, ring, first);
        assertThat(buffer.drain(PIPELINE, ring)).containsExactly(first);

        CountDownLatch adding = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        Queue<Envelope> queue = new ConcurrentLinkedQueue<>() {
            @Override
            public boolean add(Envelope value) {
                adding.countDown();
                try {
                    if (!resume.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("Timed out releasing the capture append");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
                return super.add(value);
            }
        };
        // Install a real concurrent queue with a scheduling barrier at add: computeIfAbsent can
        // publish an empty queue before append inserts its row. No production operation is replaced.
        var field = SnapshotBuffer.class.getDeclaredField("byConsumerRing");
        field.setAccessible(true);
        ConcurrentMap<SnapshotBuffer.BufferKey, Queue<Envelope>> rings =
                (ConcurrentMap<SnapshotBuffer.BufferKey, Queue<Envelope>>) field.get(buffer);
        rings.put(new SnapshotBuffer.BufferKey(PIPELINE, ring), queue);

        try (var capture = Executors.newSingleThreadExecutor()) {
            var appended = capture.submit(() -> buffer.append(PIPELINE, ring, later));
            try {
                assertThat(adding.await(10, TimeUnit.SECONDS)).as("capture reached queue insertion").isTrue();
                assertThat(buffer.drain(PIPELINE, ring)).isEmpty();
            } finally {
                resume.countDown();
            }
            appended.get(10, TimeUnit.SECONDS);

            // The append finished and the queue holds the row. An explicit next drain rules out
            // a missing processor wakeup: the row must still be reachable through the buffer.
            assertThat(queue).containsExactly(later);
            assertThat(buffer.drain(PIPELINE, ring))
                    .as("row 101 remains reachable after append completes, even after an empty drain")
                    .containsExactly(later);
        }
    }

    private static Envelope row(String ring, int id) {
        return Envelope.read(id, ring, Map.of("id", (long) id), Map.of());
    }

    @Test
    @SuppressWarnings("unchecked")
    void drainReturnsItsInitialRowsWhileCaptureKeepsAppending() throws Exception {
        SnapshotBuffer buffer = new SnapshotBuffer();
        String ring = "srs.chain.busy";
        Envelope first = row("orders", 100);
        AtomicBoolean replenish = new AtomicBoolean(true);
        AtomicInteger appended = new AtomicInteger();

        try (var capture = Executors.newSingleThreadExecutor()) {
            Queue<Envelope> queue = new LinkedBlockingQueue<>() {
                @Override
                public Envelope poll() {
                    // Keep the queue nonempty with a real capture append before each removal.
                    // Cap the producer so an unbounded drain fails without hanging the test.
                    if (replenish.get() && appended.get() < 64) {
                        int id = 100 + appended.incrementAndGet();
                        try {
                            capture.submit(() -> buffer.append(PIPELINE, ring, row("orders", id)))
                                    .get(10, TimeUnit.SECONDS);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(interrupted);
                        } catch (Exception failure) {
                            throw new AssertionError(failure);
                        }
                    }
                    return super.poll();
                }
            };
            queue.add(first);
            var field = SnapshotBuffer.class.getDeclaredField("byConsumerRing");
            field.setAccessible(true);
            ConcurrentMap<SnapshotBuffer.BufferKey, Queue<Envelope>> rings =
                    (ConcurrentMap<SnapshotBuffer.BufferKey, Queue<Envelope>>) field.get(buffer);
            rings.put(new SnapshotBuffer.BufferKey(PIPELINE, ring), queue);

            List<Envelope> drained = buffer.drain(PIPELINE, ring);
            replenish.set(false);

            assertThat(drained)
                    .as("a drain returns its initial rows without chasing the active capture")
                    .containsExactly(first);
            assertThat(appended.get()).isEqualTo(1);
            assertThat(buffer.drain(PIPELINE, ring)).extracting(e -> e.after().get("id")).containsExactly(101L);
            assertThat(buffer.drain(PIPELINE, ring)).isEmpty();
        }
    }

    @Test
    void drainsOneRingsRowsInAppendOrder() {
        SnapshotBuffer buffer = new SnapshotBuffer();
        buffer.append(PIPELINE, "srs.chain.orders", row("orders", 0));
        buffer.append(PIPELINE, "srs.chain.orders", row("orders", 1));
        buffer.append(PIPELINE, "srs.chain.orders", row("orders", 2));

        List<Envelope> drained = buffer.drain(PIPELINE, "srs.chain.orders");

        assertThat(drained).extracting(e -> e.after().get("id")).containsExactly(0L, 1L, 2L);
    }

    @Test
    void drainIsOnceConsumedSoASecondDrainIsEmpty() {
        SnapshotBuffer buffer = new SnapshotBuffer();
        buffer.append(PIPELINE, "srs.chain.orders", row("orders", 0));

        assertThat(buffer.drain(PIPELINE, "srs.chain.orders")).hasSize(1);
        assertThat(buffer.drain(PIPELINE, "srs.chain.orders")).isEmpty();
    }

    @Test
    void drainingANeverAppendedRingIsEmptyNotNull() {
        SnapshotBuffer buffer = new SnapshotBuffer();

        assertThat(buffer.drain(PIPELINE, "srs.chain.absent")).isEmpty();
    }

    @Test
    void keepsEachRingsRowsIsolated() {
        SnapshotBuffer buffer = new SnapshotBuffer();
        buffer.append(PIPELINE, "srs.chain.orders", row("orders", 1));
        buffer.append(PIPELINE, "srs.chain.items", row("items", 2));

        assertThat(buffer.drain(PIPELINE, "srs.chain.orders")).extracting(e -> e.after().get("id")).containsExactly(1L);
        assertThat(buffer.drain(PIPELINE, "srs.chain.items")).extracting(e -> e.after().get("id")).containsExactly(2L);
    }

    @Test
    void keepsEachConsumersRowsIsolatedOnASharedRing() {
        SnapshotBuffer buffer = new SnapshotBuffer();
        String ring = "srs.chain.orders";
        buffer.append("first_pipeline", ring, row("orders", 1));
        buffer.append("second_pipeline", ring, row("orders", 2));

        assertThat(buffer.drain("first_pipeline", ring))
                .as("one consumer cannot take another consumer's initial load")
                .extracting(e -> e.after().get("id"))
                .containsExactly(1L);
        assertThat(buffer.drain("second_pipeline", ring))
                .extracting(e -> e.after().get("id"))
                .containsExactly(2L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void releaseReclaimsAllOfOneConsumersQueuesWithoutTouchingItsNeighbour() throws Exception {
        SnapshotBuffer buffer = new SnapshotBuffer();
        String sharedRing = "srs.chain.orders";
        String otherPipeline = "other_pipeline";
        Envelope first = row("orders", 1);
        Envelope second = row("items", 2);
        Envelope neighbour = row("orders", 3);
        buffer.append(PIPELINE, sharedRing, first);
        buffer.append(PIPELINE, "srs.chain.items", second);
        buffer.append(otherPipeline, sharedRing, neighbour);

        // A normal drain consumes the row but deliberately leaves the empty queue attached. Releasing the
        // pipeline must reclaim that entry as well as a queue whose row the cancelled job never consumed.
        assertThat(buffer.drain(PIPELINE, sharedRing)).containsExactly(first);
        var field = SnapshotBuffer.class.getDeclaredField("byConsumerRing");
        field.setAccessible(true);
        ConcurrentMap<SnapshotBuffer.BufferKey, Queue<Envelope>> rings =
                (ConcurrentMap<SnapshotBuffer.BufferKey, Queue<Envelope>>) field.get(buffer);
        assertThat(rings).containsKeys(
                new SnapshotBuffer.BufferKey(PIPELINE, sharedRing),
                new SnapshotBuffer.BufferKey(PIPELINE, "srs.chain.items"),
                new SnapshotBuffer.BufferKey(otherPipeline, sharedRing));

        buffer.release(PIPELINE);

        assertThat(rings).containsOnlyKeys(new SnapshotBuffer.BufferKey(otherPipeline, sharedRing));
        assertThat(buffer.drain(otherPipeline, sharedRing)).containsExactly(neighbour);
    }

    // ---- a declared load ----------------------------------------------------------------------------

    /**
     * The bound that keeps a load of any size to a few thousand rows on the heap: once a declared load has
     * its capacity of rows waiting, the next append waits until the vertex has taken some, and then goes in.
     */
    @Test
    void aDeclaredLoadWaitsForRoomOnceItsCapacityOfRowsIsWaiting() throws Exception {
        SnapshotBuffer buffer = new SnapshotBuffer(2);
        String ring = "srs.chain.bounded";
        buffer.declareSnapshot(PIPELINE, ring);
        buffer.append(PIPELINE, ring, row("orders", 1));
        buffer.append(PIPELINE, ring, row("orders", 2));

        try (var capture = Executors.newSingleThreadExecutor()) {
            Future<?> third = capture.submit(() -> buffer.append(PIPELINE, ring, row("orders", 3)));
            assertThatThrownBy(() -> third.get(300, TimeUnit.MILLISECONDS))
                    .as("a third row of the load waits while two of it are waiting")
                    .isInstanceOf(TimeoutException.class);

            assertThat(buffer.drain(PIPELINE, ring)).extracting(e -> e.after().get("id")).containsExactly(1L, 2L);
            third.get(10, TimeUnit.SECONDS);
            assertThat(buffer.drain(PIPELINE, ring)).extracting(e -> e.after().get("id")).containsExactly(3L);
        }
    }

    /**
     * A change a ring-less tail hands over behind the load is not part of the load and never waits: the
     * load's one row is still waiting here, and at a capacity of one a row of the load would.
     */
    @Test
    void whatIsAppendedAfterTheLoadEndedNeverWaits() {
        SnapshotBuffer buffer = new SnapshotBuffer(1);
        String ring = "srs.chain.after";
        buffer.declareSnapshot(PIPELINE, ring);
        buffer.append(PIPELINE, ring, row("orders", 1));
        buffer.endSnapshot(PIPELINE, ring);

        buffer.append(PIPELINE, ring, row("orders", 2));
        buffer.append(PIPELINE, ring, row("orders", 3));

        assertThat(buffer.drain(PIPELINE, ring)).extracting(e -> e.after().get("id")).containsExactly(1L, 2L, 3L);
    }

    @Test
    void aCoordinateNobodyDeclaredIsNotHeldToTheCapacity() {
        SnapshotBuffer buffer = new SnapshotBuffer(1);
        String ring = "srs.chain.undeclared";
        buffer.append(PIPELINE, ring, row("orders", 1));
        buffer.append(PIPELINE, ring, row("orders", 2));

        assertThat(buffer.drain(PIPELINE, ring)).hasSize(2);
        assertThat(buffer.snapshotState(PIPELINE, ring)).isEqualTo(SnapshotBuffer.SnapshotState.UNDECLARED);
    }

    /** A stop has to be able to end a load whose reader is parked waiting for a vertex that is gone. */
    @Test
    void releasingThePipelineAbandonsALoadWaitingForRoom() throws Exception {
        SnapshotBuffer buffer = new SnapshotBuffer(1);
        String ring = "srs.chain.released";
        buffer.declareSnapshot(PIPELINE, ring);
        buffer.append(PIPELINE, ring, row("orders", 1));

        try (var capture = Executors.newSingleThreadExecutor()) {
            Future<?> waiting = capture.submit(() -> buffer.append(PIPELINE, ring, row("orders", 2)));
            assertThatThrownBy(() -> waiting.get(200, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);

            buffer.release(PIPELINE);

            assertThatThrownBy(() -> waiting.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(CancellationException.class);
        }
        assertThat(buffer.drain(PIPELINE, ring)).isEmpty();
        assertThat(buffer.snapshotState(PIPELINE, ring)).isEqualTo(SnapshotBuffer.SnapshotState.UNDECLARED);
    }

    @Test
    void anInterruptedWaitAbandonsTheRowAndKeepsTheInterrupt() throws Exception {
        SnapshotBuffer buffer = new SnapshotBuffer(1);
        String ring = "srs.chain.interrupted";
        buffer.declareSnapshot(PIPELINE, ring);
        buffer.append(PIPELINE, ring, row("orders", 1));
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        AtomicBoolean interruptKept = new AtomicBoolean();
        Thread capture = new Thread(() -> {
            try {
                buffer.append(PIPELINE, ring, row("orders", 2));
            } catch (RuntimeException failure) {
                thrown.set(failure);
                interruptKept.set(Thread.currentThread().isInterrupted());
            }
        });
        capture.start();
        awaitWaiting(capture);

        capture.interrupt();
        capture.join(10_000);

        assertThat(thrown.get()).isInstanceOf(CancellationException.class);
        assertThat(interruptKept).as("the interrupt is left for whoever runs the thread next").isTrue();
        assertThat(buffer.drain(PIPELINE, ring)).extracting(e -> e.after().get("id")).containsExactly(1L);
    }

    /**
     * Handed over means ended with none of it left waiting. Ended with rows still waiting is not, and neither
     * is every row taken while more may follow -- the two states an empty drain cannot tell apart.
     */
    @Test
    void aLoadIsHandedOverOnceItHasEndedAndEveryRowOfItHasBeenTaken() {
        SnapshotBuffer buffer = new SnapshotBuffer();
        String ring = "srs.chain.state";
        buffer.declareSnapshot(PIPELINE, ring);
        assertThat(buffer.snapshotState(PIPELINE, ring)).isEqualTo(new SnapshotBuffer.SnapshotState(true, false, false));

        buffer.append(PIPELINE, ring, row("orders", 1));
        buffer.drain(PIPELINE, ring);
        assertThat(buffer.snapshotState(PIPELINE, ring))
                .as("every row so far taken, and the load not yet over")
                .isEqualTo(new SnapshotBuffer.SnapshotState(true, true, false));

        buffer.append(PIPELINE, ring, row("orders", 2));
        buffer.endSnapshot(PIPELINE, ring);
        assertThat(buffer.snapshotState(PIPELINE, ring))
                .as("over, with its last row still waiting")
                .isEqualTo(new SnapshotBuffer.SnapshotState(true, true, false));

        buffer.drain(PIPELINE, ring);
        assertThat(buffer.snapshotState(PIPELINE, ring)).isEqualTo(new SnapshotBuffer.SnapshotState(true, true, true));
    }

    /**
     * A vertex starting after an empty load was over, and a ring-less tail had handed a change over behind
     * it, has nothing of the load to vouch for or against: taking only that change does not begin the load.
     */
    @Test
    void takingOnlyWhatFollowedTheLoadDoesNotCountAsBeginningIt() {
        SnapshotBuffer buffer = new SnapshotBuffer();
        String ring = "srs.chain.empty-load";
        buffer.declareSnapshot(PIPELINE, ring);
        buffer.endSnapshot(PIPELINE, ring);
        buffer.append(PIPELINE, ring, row("orders", 9));

        assertThat(buffer.drain(PIPELINE, ring)).hasSize(1);
        assertThat(buffer.snapshotState(PIPELINE, ring)).isEqualTo(new SnapshotBuffer.SnapshotState(true, false, true));
    }

    @Test
    void endingALoadNobodyDeclaredDoesNothing() {
        SnapshotBuffer buffer = new SnapshotBuffer();
        buffer.endSnapshot(PIPELINE, "srs.chain.never");

        assertThat(buffer.snapshotState(PIPELINE, "srs.chain.never"))
                .isEqualTo(SnapshotBuffer.SnapshotState.UNDECLARED);
    }

    @Test
    void aHandOffHoldsAtLeastOneRow() {
        assertThatThrownBy(() -> new SnapshotBuffer(0)).isInstanceOf(IllegalArgumentException.class);
        assertThat(new SnapshotBuffer().capacity()).isEqualTo(SnapshotBuffer.DEFAULT_CAPACITY);
    }

    /** Waits until {@code thread} is parked, so what follows is sure to reach it while it waits. */
    private static void awaitWaiting(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (thread.getState() != Thread.State.WAITING) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the append never waited: " + thread.getState());
            }
            Thread.sleep(10);
        }
    }
}
