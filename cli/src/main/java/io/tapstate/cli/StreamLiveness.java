package io.tapstate.cli;

import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/** Probes transport health without mistaking a quiet business stream for a disconnected peer. */
final class StreamLiveness {
    private record Probe(long id, long deadline) {}
    private final long interval;
    private final long timeout;
    private final CountDownLatch closed;
    private final AtomicReference<Probe> pending = new AtomicReference<>();
    private long next;
    private long sequence;

    StreamLiveness(Duration interval, Duration timeout, CountDownLatch closed) {
        this.interval = interval.toNanos();
        this.timeout = timeout.toNanos();
        this.closed = closed;
        this.next = System.nanoTime() + this.interval;
    }

    void tick(WebSocket socket, long now) {
        if (closed.getCount() == 0) return;
        Probe probe = pending.get();
        if (probe != null) {
            if (now - probe.deadline() >= 0 && pending.compareAndSet(probe, null)) closed.countDown();
            return;
        }
        if (now - next < 0) return;
        Probe sent = new Probe(++sequence, now + timeout);
        pending.set(sent);
        next = now + interval;
        try {
            socket.sendPing(ByteBuffer.allocate(Long.BYTES).putLong(sent.id()).flip())
                    .whenComplete((ignored, failure) -> { if (failure != null) closed.countDown(); });
        } catch (RuntimeException unavailable) {
            closed.countDown();
        }
    }

    void pong(ByteBuffer payload, long now) {
        Probe probe = pending.get();
        if (probe != null && payload.remaining() == Long.BYTES && payload.getLong(payload.position()) == probe.id()
                && pending.compareAndSet(probe, null) && now - probe.deadline() >= 0)
            closed.countDown();
    }
}
