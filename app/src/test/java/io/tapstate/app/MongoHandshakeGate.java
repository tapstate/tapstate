package io.tapstate.app;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/** Holds each connection's first Mongo reply while requests reach the real loopback upstream. */
final class MongoHandshakeGate implements AutoCloseable {

    private static final int HEADER_BYTES = 16;
    private static final int MAX_FIRST_REPLY_BYTES = 1024 * 1024;
    private static final int SOCKET_TIMEOUT_MILLIS = 5_000;
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private final Object lifecycle = new Object();
    private final Set<Socket> sockets = new HashSet<>();
    private final CountDownLatch released = new CountDownLatch(1);
    private final InetSocketAddress upstreamAddress;
    private final ServerSocket listener;
    private final ExecutorService workers;
    private int capturedFirstReplies;
    private volatile boolean closed;

    MongoHandshakeGate(String upstreamHost, int upstreamPort) throws IOException {
        if (!"127.0.0.1".equals(upstreamHost)) {
            throw new IllegalArgumentException("The Mongo handshake upstream must be literal IPv4 loopback");
        }
        if (upstreamPort < 1 || upstreamPort > 65535) {
            throw new IllegalArgumentException("The Mongo handshake upstream requires a valid TCP port");
        }
        InetAddress loopback = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
        upstreamAddress = new InetSocketAddress(loopback, upstreamPort);
        listener = new ServerSocket(0, 50, loopback);
        workers = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("mongo-handshake-gate-", 0).factory());
        workers.execute(this::acceptConnections);
    }

    int port() {
        return listener.getLocalPort();
    }

    void awaitFirstReply(Duration timeout) throws InterruptedException {
        awaitFirstReplies(1, timeout);
    }

    void awaitFirstReplies(int count, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (lifecycle) {
            while (capturedFirstReplies < count) {
                long remaining = deadline - System.nanoTime();
                if (closed || remaining <= 0) {
                    throw new AssertionError("No complete first Mongo reply reached the handshake gate before the deadline");
                }
                TimeUnit.NANOSECONDS.timedWait(lifecycle, remaining);
            }
        }
    }

    void release() {
        released.countDown();
    }

    private void acceptConnections() {
        while (!closed) {
            Socket client = null;
            try {
                client = listener.accept();
                own(client);
                Socket accepted = client;
                workers.execute(() -> forward(accepted));
            } catch (IOException | RejectedExecutionException ignored) {
                if (client != null) closeSocket(client);
                return;
            }
        }
    }

    private void forward(Socket client) {
        Socket upstream = new Socket();
        try {
            own(upstream);
            upstream.connect(upstreamAddress, SOCKET_TIMEOUT_MILLIS);
            upstream.setSoTimeout(SOCKET_TIMEOUT_MILLIS);
            Thread replyWorker = Thread.currentThread();
            workers.execute(() -> forwardRequests(client, upstream, replyWorker));
            InputStream replies = upstream.getInputStream();
            byte[] reply = readFirstReply(replies);
            synchronized (lifecycle) {
                capturedFirstReplies++;
                lifecycle.notifyAll();
            }
            released.await();
            if (closed) return;
            upstream.setSoTimeout(0);
            var clientOutput = client.getOutputStream();
            clientOutput.write(reply);
            replies.transferTo(clientOutput);
        } catch (IOException | RejectedExecutionException ignored) {
            // A failed or truncated connection is closed without exposing any wire data.
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            closeSocket(client);
            closeSocket(upstream);
        }
    }

    private void forwardRequests(Socket client, Socket upstream, Thread replyWorker) {
        try {
            client.getInputStream().transferTo(upstream.getOutputStream());
        } catch (IOException ignored) {
            // Either direction ending closes the entire owned connection.
        } finally {
            closeSocket(client);
            closeSocket(upstream);
            replyWorker.interrupt();
        }
    }

    private static byte[] readFirstReply(InputStream input) throws IOException {
        byte[] header = input.readNBytes(HEADER_BYTES);
        if (header.length != HEADER_BYTES) {
            throw new EOFException("Incomplete first Mongo reply header");
        }
        int length = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN).getInt();
        if (length < HEADER_BYTES || length > MAX_FIRST_REPLY_BYTES) {
            throw new IOException("First Mongo reply length is outside the fixture bound");
        }
        byte[] reply = new byte[length];
        System.arraycopy(header, 0, reply, 0, HEADER_BYTES);
        if (input.readNBytes(reply, HEADER_BYTES, length - HEADER_BYTES) != length - HEADER_BYTES) {
            throw new EOFException("Incomplete first Mongo reply body");
        }
        return reply;
    }

    private void own(Socket socket) throws IOException {
        synchronized (lifecycle) {
            if (closed) {
                socket.close();
                throw new SocketException("The Mongo handshake gate is closed");
            }
            sockets.add(socket);
        }
    }

    private void closeSocket(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // Continue closing the other owned sockets and workers.
        } finally {
            synchronized (lifecycle) {
                sockets.remove(socket);
            }
        }
    }

    @Override
    public void close() {
        List<Socket> owned;
        synchronized (lifecycle) {
            closed = true;
            owned = List.copyOf(sockets);
            lifecycle.notifyAll();
        }
        try {
            listener.close();
        } catch (IOException ignored) {
            // Socket cleanup and worker termination still run if listener closure fails.
        }
        released.countDown();
        owned.forEach(this::closeSocket);
        workers.shutdownNow();
        long deadline = System.nanoTime() + CLOSE_TIMEOUT.toNanos();
        boolean interrupted = false;
        try {
            while (!workers.isTerminated()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new AssertionError("Mongo handshake gate workers did not terminate before the deadline");
                }
                try {
                    workers.awaitTermination(remaining, TimeUnit.NANOSECONDS);
                } catch (InterruptedException interruption) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }
}
