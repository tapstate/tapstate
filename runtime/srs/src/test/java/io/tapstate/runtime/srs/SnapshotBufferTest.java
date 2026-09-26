package io.tapstate.runtime.srs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The member-local snapshot buffer: the seam that carries a source's bounded snapshot rows from the capture
 * side to the source vertex so they can be emitted ahead of the cdc tail. It buffers per consumer pipeline
 * and ring name, drains that consumer's rows once in append order, and isolates both coordinates.
 */
class SnapshotBufferTest {

    @Test
    void anEmptyMomentDuringASnapshotCannotBeMistakenForItsDoneMarker() {
        SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
        String ring = "srs.chain.orders";
        buffer.beginSnapshot(PIPELINE, ring, "run-a");

        assertThat(buffer.drainSnapshot(PIPELINE, ring, "run-a", 1).state())
                .isEqualTo(SnapshotBuffer.SessionState.ACTIVE);
        buffer.appendSnapshot(PIPELINE, ring, "run-a", row("orders", 1));
        SnapshotBuffer.SessionDrain first = buffer.drainSnapshot(PIPELINE, ring, "run-a", 1);
        assertThat(first.rows()).containsExactly(row("orders", 1));
        assertThat(first.state()).isEqualTo(SnapshotBuffer.SessionState.ACTIVE);
        buffer.completeSnapshot(PIPELINE, ring, "run-a");
        SnapshotBuffer.SessionDrain finished = buffer.drainSnapshot(PIPELINE, ring, "run-a", 1);
        assertThat(finished.rows()).isEmpty();
        assertThat(finished.state()).isEqualTo(SnapshotBuffer.SessionState.DONE);
    }

    @Test
    void aFailureMarkerFollowsRowsAlreadyAcceptedForThatSession() {
        SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
        String ring = "srs.chain.orders";
        IllegalStateException sourceFailure = new IllegalStateException("source stopped");
        buffer.beginSnapshot(PIPELINE, ring, "run-a");
        buffer.appendSnapshot(PIPELINE, ring, "run-a", row("orders", 1));
        buffer.failSnapshot(PIPELINE, ring, "run-a", sourceFailure);

        SnapshotBuffer.SessionDrain beforeMarker = buffer.drainSnapshot(PIPELINE, ring, "run-a", 1);
        assertThat(beforeMarker.rows()).containsExactly(row("orders", 1));
        assertThat(beforeMarker.state()).isEqualTo(SnapshotBuffer.SessionState.ACTIVE);
        SnapshotBuffer.SessionDrain failure = buffer.drainSnapshot(PIPELINE, ring, "run-a", 1);
        assertThat(failure.rows()).isEmpty();
        assertThat(failure.state()).isEqualTo(SnapshotBuffer.SessionState.FAILED);
        assertThat(failure.failure()).isSameAs(sourceFailure);
    }

    @Test
    void releasingACompletedButUndrainedSessionReturnsItsGlobalCapacity() throws Exception {
        SnapshotBuffer buffer = new SnapshotBuffer(1, 1, 512, 256);
        String ring = "srs.chain.orders";
        buffer.beginSnapshot(PIPELINE, ring, "run-a");
        buffer.appendSnapshot(PIPELINE, ring, "run-a", row("orders", 1));
        buffer.completeSnapshot(PIPELINE, ring, "run-a");
        buffer.release(PIPELINE);
        buffer.beginSnapshot("next-pipeline", ring, "run-b");

        try (var producer = Executors.newSingleThreadExecutor()) {
            var appended = producer.submit(() ->
                    buffer.appendSnapshot("next-pipeline", ring, "run-b", row("orders", 2)));
            appended.get(5, TimeUnit.SECONDS);
        } finally {
            buffer.release("next-pipeline");
        }
    }

    @Test
    void cancellingAFullSessionWakesTheProducerAndAnOldTokenCannotSeeTheReplacement() throws Exception {
        SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
        String ring = "srs.chain.orders";
        buffer.beginSnapshot(PIPELINE, ring, "run-a");
        buffer.appendSnapshot(PIPELINE, ring, "run-a", row("orders", 1));
        CountDownLatch attempted = new CountDownLatch(1);
        try (var producer = Executors.newSingleThreadExecutor()) {
            var second = producer.submit(() -> {
                attempted.countDown();
                buffer.appendSnapshot(PIPELINE, ring, "run-a", row("orders", 2));
            });
            assertThat(attempted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> second.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);

            buffer.cancelSnapshot(PIPELINE, ring, "run-a");
            assertThatThrownBy(() -> second.get(5, TimeUnit.SECONDS))
                    .isInstanceOfSatisfying(ExecutionException.class,
                            failure -> assertThat(failure.getCause()).isInstanceOf(CancellationException.class));
            assertThat(buffer.drainSnapshot(PIPELINE, ring, "run-a", 1).state())
                    .isEqualTo(SnapshotBuffer.SessionState.CANCELLED);
            buffer.beginSnapshot(PIPELINE, ring, "run-b");
            buffer.appendSnapshot(PIPELINE, ring, "run-b", row("orders", 3));
            assertThat(buffer.drainSnapshot(PIPELINE, ring, "run-a", 1).state())
                    .isEqualTo(SnapshotBuffer.SessionState.CANCELLED);
            assertThat(buffer.drainSnapshot(PIPELINE, ring, "run-b", 1).rows())
                    .containsExactly(row("orders", 3));
        }
    }

    @Test
    void aSingleRowAboveTheLogicalByteBudgetFailsWithACodedSize() {
        SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 256, 128);
        buffer.beginSnapshot(PIPELINE, "srs.chain.orders", "run-a");

        assertThatThrownBy(() -> buffer.appendSnapshot(
                PIPELINE, "srs.chain.orders", "run-a", row("orders", 1)))
                .isInstanceOfSatisfying(TapstateException.class, failure ->
                        assertThat(failure.code()).isEqualTo(CaptureError.SNAPSHOT_ROW_TOO_LARGE));
    }

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
}
