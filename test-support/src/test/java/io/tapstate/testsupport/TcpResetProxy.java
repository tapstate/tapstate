package io.tapstate.testsupport;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** A test-only proxy that resets active source connections and restores service on a timer. */
public final class TcpResetProxy implements AutoCloseable {
    private final java.net.ServerSocket listener;
    private final String host;
    private final int port;
    private final java.util.Set<Hop> hops = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final Object admission = new Object();
    private volatile boolean available = true;

    public TcpResetProxy(String host, int port) throws java.io.IOException {
        this.host = host;
        this.port = port;
        listener = new java.net.ServerSocket(0, 16, java.net.InetAddress.getByName("127.0.0.1"));
        Thread.ofVirtual().start(this::accept);
    }

    public int port() {
        return listener.getLocalPort();
    }

    public void resetFor(Duration duration) throws Exception {
        java.util.List<Hop> connected;
        synchronized (admission) {
            available = false;
            connected = java.util.List.copyOf(hops);
        }
        if (connected.isEmpty()) throw new AssertionError("no live connection to reset");
        connected.forEach(Hop::reset);
        // Restoration is a fixture event that ends the configured network outage.
        var restoration = CompletableFuture.runAsync(() -> {
            synchronized (admission) {
                available = true;
            }
        },
                CompletableFuture.delayedExecutor(duration.toMillis(), TimeUnit.MILLISECONDS));
        restoration.get(duration.plusSeconds(10).toMillis(), TimeUnit.MILLISECONDS);
    }

    private void accept() {
        while (!listener.isClosed()) {
            java.net.Socket front;
            try {
                front = listener.accept();
            } catch (java.io.IOException failed) {
                if (!listener.isClosed()) throw new java.io.UncheckedIOException(failed);
                return;
            }
            if (!available) {
                resetFront(front);
                continue;
            }
            connect(front);
        }
    }

    private void connect(java.net.Socket front) {
        var back = new java.net.Socket();
        var hop = new Hop(front, back);
        try {
            back.connect(new java.net.InetSocketAddress(host, port));
        } catch (java.io.IOException refused) {
            hop.reset();
            return;
        }
        boolean admitted;
        synchronized (admission) {
            admitted = available && !listener.isClosed();
            if (admitted) hops.add(hop);
        }
        if (!admitted) {
            hop.reset();
            return;
        }
        Thread.ofVirtual().start(() -> pump(hop, front, back));
        Thread.ofVirtual().start(() -> pump(hop, back, front));
    }

    private static void resetFront(java.net.Socket front) {
        try { front.setSoLinger(true, 0); } catch (java.io.IOException alreadyClosed) {}
        try { front.close(); } catch (java.io.IOException alreadyClosed) {}
    }

    private void pump(Hop hop, java.net.Socket input, java.net.Socket output) {
        try {
            input.getInputStream().transferTo(output.getOutputStream());
        } catch (java.io.IOException closed) {
            // Reset and fixture teardown both close active connections.
        } finally {
            hops.remove(hop);
            hop.close();
        }
    }

    public void close() throws java.io.IOException {
        listener.close();
        for (Hop hop : hops) hop.close();
    }

    private record Hop(java.net.Socket front, java.net.Socket back) {
        void reset() {
            resetFront(front);
            try { back.close(); } catch (java.io.IOException alreadyClosed) {}
        }

        void close() {
            try { front.close(); } catch (java.io.IOException alreadyClosed) {}
            try { back.close(); } catch (java.io.IOException alreadyClosed) {}
        }
    }
}
