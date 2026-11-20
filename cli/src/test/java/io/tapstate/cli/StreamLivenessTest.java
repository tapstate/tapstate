package io.tapstate.cli;

import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class StreamLivenessTest {
    @Test
    void onlyTheMatchingPongCompletesTheOutstandingProbe() {
        var closed = new CountDownLatch(1);
        var socket = new Socket();
        var live = new StreamLiveness(Duration.ofNanos(1), Duration.ofSeconds(10), closed);
        // Advance the test clock past the constructor deadline even on a coarse monotonic clock.
        long now = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        live.tick(socket, now);
        live.pong(ByteBuffer.wrap(new byte[] {1}), now);
        live.pong(ByteBuffer.allocate(8).putLong(99).flip(), now);
        live.tick(socket, now + Duration.ofSeconds(11).toNanos());
        assertThat(closed.getCount()).isZero();
        assertThat(socket.pings).isEqualTo(1);
    }

    @Test
    void aMatchingPongAllowsTheNextProbeWithoutClosingTheStream() {
        var closed = new CountDownLatch(1);
        var socket = new Socket();
        var live = new StreamLiveness(Duration.ofNanos(1), Duration.ofSeconds(10), closed);
        long now = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        live.tick(socket, now);
        live.pong(socket.payload, now);
        live.tick(socket, now + 1);
        assertThat(closed.getCount()).isEqualTo(1);
        assertThat(socket.pings).isEqualTo(2);
    }

    @Test
    void aLatePongCannotResurrectAnExpiredProbe() {
        var closed = new CountDownLatch(1);
        var socket = new Socket();
        var live = new StreamLiveness(Duration.ofNanos(1), Duration.ofNanos(1), closed);
        long now = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        live.tick(socket, now);
        live.pong(socket.payload, now + 1);
        assertThat(closed.getCount()).isZero();
    }

    @Test
    void aFailedPingWriteEndsTheCurrentStreamWithoutRetrying() {
        var closed = new CountDownLatch(1);
        var socket = new Socket();
        socket.write = CompletableFuture.failedFuture(new java.io.IOException("peer closed"));
        var live = new StreamLiveness(Duration.ofNanos(1), Duration.ofSeconds(10), closed);
        long now = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        live.tick(socket, now);
        live.tick(socket, now + 1);
        assertThat(closed.getCount()).isZero();
        assertThat(socket.pings).isEqualTo(1);
    }

    @Test
    void aSynchronousPingFailureEndsTheStream() {
        var closed = new CountDownLatch(1);
        var socket = new Socket();
        socket.throwOnWrite = true;
        var live = new StreamLiveness(Duration.ofNanos(1), Duration.ofSeconds(10), closed);
        long now = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        live.tick(socket, now);
        assertThat(closed.getCount()).isZero();
    }

    private static final class Socket implements WebSocket {
        private int pings;
        private boolean throwOnWrite;
        private ByteBuffer payload;
        private CompletableFuture<WebSocket> write = CompletableFuture.completedFuture(this);
        public CompletableFuture<WebSocket> sendPing(ByteBuffer message) { if (throwOnWrite) throw new IllegalStateException("closed"); pings++; payload = message.asReadOnlyBuffer(); return write; }
        public CompletableFuture<WebSocket> sendText(CharSequence data, boolean last) { throw new AssertionError(); }
        public CompletableFuture<WebSocket> sendBinary(ByteBuffer data, boolean last) { throw new AssertionError(); }
        public CompletableFuture<WebSocket> sendPong(ByteBuffer message) { throw new AssertionError(); }
        public CompletableFuture<WebSocket> sendClose(int code, String reason) { throw new AssertionError(); }
        public void request(long count) { throw new AssertionError(); }
        public String getSubprotocol() { return ""; }
        public boolean isOutputClosed() { return false; }
        public boolean isInputClosed() { return false; }
        public void abort() { throw new AssertionError(); }
    }
}
