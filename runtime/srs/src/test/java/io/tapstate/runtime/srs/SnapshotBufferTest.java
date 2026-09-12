package io.tapstate.runtime.srs;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.Envelope;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The member-local snapshot buffer: the seam that carries a source's bounded snapshot rows from the capture
 * side to the source vertex so they can be emitted ahead of the cdc tail. It buffers per ring name, drains a
 * ring's rows once in append order, and isolates one ring's rows from another's.
 */
class SnapshotBufferTest {

    @Test
    @SuppressWarnings("unchecked")
    void retainsALaterAppendWhenAnEmptyDrainRacesWithQueueInsertion() throws Exception {
        SnapshotBuffer buffer = new SnapshotBuffer();
        String ring = "srs.chain.late";
        Envelope first = row("orders", 100);
        Envelope later = row("orders", 101);
        buffer.append(ring, first);
        assertThat(buffer.drain(ring)).containsExactly(first);

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
        var field = SnapshotBuffer.class.getDeclaredField("byRing");
        field.setAccessible(true);
        ConcurrentMap<String, Queue<Envelope>> rings =
                (ConcurrentMap<String, Queue<Envelope>>) field.get(buffer);
        rings.put(ring, queue);

        try (var capture = Executors.newSingleThreadExecutor()) {
            var appended = capture.submit(() -> buffer.append(ring, later));
            try {
                assertThat(adding.await(10, TimeUnit.SECONDS)).as("capture reached queue insertion").isTrue();
                assertThat(buffer.drain(ring)).isEmpty();
            } finally {
                resume.countDown();
            }
            appended.get(10, TimeUnit.SECONDS);

            // The append finished and the queue holds the row. An explicit next drain rules out
            // a missing processor wakeup: the row must still be reachable through the buffer.
            assertThat(queue).containsExactly(later);
            assertThat(buffer.drain(ring))
                    .as("row 101 remains reachable after append completes, even after an empty drain")
                    .containsExactly(later);
        }
    }

    private static Envelope row(String ring, int id) {
        return Envelope.read(id, ring, Map.of("id", (long) id), Map.of());
    }

    @Test
    void drainsOneRingsRowsInAppendOrder() {
        SnapshotBuffer buffer = new SnapshotBuffer();
        buffer.append("srs.chain.orders", row("orders", 0));
        buffer.append("srs.chain.orders", row("orders", 1));
        buffer.append("srs.chain.orders", row("orders", 2));

        List<Envelope> drained = buffer.drain("srs.chain.orders");

        assertThat(drained).extracting(e -> e.after().get("id")).containsExactly(0L, 1L, 2L);
    }

    @Test
    void drainIsOnceConsumedSoASecondDrainIsEmpty() {
        SnapshotBuffer buffer = new SnapshotBuffer();
        buffer.append("srs.chain.orders", row("orders", 0));

        assertThat(buffer.drain("srs.chain.orders")).hasSize(1);
        assertThat(buffer.drain("srs.chain.orders")).isEmpty();
    }

    @Test
    void drainingANeverAppendedRingIsEmptyNotNull() {
        SnapshotBuffer buffer = new SnapshotBuffer();

        assertThat(buffer.drain("srs.chain.absent")).isEmpty();
    }

    @Test
    void keepsEachRingsRowsIsolated() {
        SnapshotBuffer buffer = new SnapshotBuffer();
        buffer.append("srs.chain.orders", row("orders", 1));
        buffer.append("srs.chain.items", row("items", 2));

        assertThat(buffer.drain("srs.chain.orders")).extracting(e -> e.after().get("id")).containsExactly(1L);
        assertThat(buffer.drain("srs.chain.items")).extracting(e -> e.after().get("id")).containsExactly(2L);
    }
}
