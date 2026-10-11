package io.tapstate.testsupport;

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
import java.util.zip.CRC32C;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;

/** Relays real Mongo bytes with the default first-reply or an explicit before-dispatch request barrier. */
public final class MongoHandshakeGate implements AutoCloseable {

    private static final int HEADER_BYTES = 16;
    private static final int MAX_FIRST_REPLY_BYTES = 1024 * 1024;
    private static final int MAX_REQUEST_BYTES = 48 * 1024 * 1024;
    private static final int SOCKET_TIMEOUT_MILLIS = 5_000;
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private final Object lifecycle = new Object();
    private final Set<Socket> sockets = new HashSet<>();
    private final CountDownLatch released = new CountDownLatch(1);
    private final InetSocketAddress upstreamAddress;
    private final ServerSocket listener;
    private final ExecutorService workers;
    private final ProfileRequest selector;
    private int capturedFirstReplies;
    private int capturedProfileRequests;
    private int forwardedProfileRequests;
    private volatile boolean closed;

    public MongoHandshakeGate(String upstreamHost, int upstreamPort) throws IOException {
        this(upstreamHost, upstreamPort, null);
    }

    /** Holds the actual first transactional profile request before any of its bytes reach Mongo. */
    public static MongoHandshakeGate beforeProfileReservation(String upstreamHost, int upstreamPort,
            String database, String collection, String clusterId) throws IOException {
        return new MongoHandshakeGate(upstreamHost, upstreamPort, new ProfileRequest(database, collection, clusterId));
    }

    private MongoHandshakeGate(String upstreamHost, int upstreamPort, ProfileRequest selector) throws IOException {
        if (!"127.0.0.1".equals(upstreamHost)) {
            throw new IllegalArgumentException("The Mongo handshake upstream must be literal IPv4 loopback");
        }
        if (upstreamPort < 1 || upstreamPort > 65535) {
            throw new IllegalArgumentException("The Mongo handshake upstream requires a valid TCP port");
        }
        this.selector = selector;
        InetAddress loopback = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
        upstreamAddress = new InetSocketAddress(loopback, upstreamPort);
        listener = new ServerSocket(0, 50, loopback);
        workers = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("mongo-handshake-gate-", 0).factory());
        workers.execute(this::acceptConnections);
    }

    public int port() {
        return listener.getLocalPort();
    }

    public void awaitFirstReply(Duration timeout) throws InterruptedException {
        awaitFirstReplies(1, timeout);
    }

    public void awaitFirstReplies(int count, Duration timeout) throws InterruptedException {
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

    public void release() {
        released.countDown();
    }

    public void awaitProfileReservation(Duration timeout) throws InterruptedException {
        awaitProfileReservations(1, timeout);
    }

    public void awaitProfileReservations(int count, Duration timeout) throws InterruptedException {
        if (selector == null || count < 1) throw new IllegalArgumentException("A profile-request barrier requires a positive count");
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (lifecycle) {
            while (capturedProfileRequests < count) {
                long remaining = deadline - System.nanoTime();
                if (closed || remaining <= 0) {
                    throw new AssertionError("No complete transactional profile request reached the gate before the deadline");
                }
                TimeUnit.NANOSECONDS.timedWait(lifecycle, remaining);
            }
        }
    }

    public int capturedProfileRequests() {
        synchronized (lifecycle) { return capturedProfileRequests; }
    }

    /** Completed socket writes are observations, not proof that the server executed the request. */
    public int forwardedProfileRequests() {
        synchronized (lifecycle) { return forwardedProfileRequests; }
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
            upstream.setSoTimeout(selector == null ? SOCKET_TIMEOUT_MILLIS : 0);
            Thread replyWorker = Thread.currentThread();
            workers.execute(() -> forwardRequests(client, upstream, replyWorker));
            InputStream replies = upstream.getInputStream();
            if (selector != null) {
                replies.transferTo(client.getOutputStream());
                return;
            }
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
            if (selector == null) {
                client.getInputStream().transferTo(upstream.getOutputStream());
            } else {
                InputStream requests = client.getInputStream();
                for (byte[] request; (request = readRequest(requests)) != null;) {
                    boolean selected = selector.matches(request);
                    if (selected) {
                        synchronized (lifecycle) {
                            capturedProfileRequests++;
                            lifecycle.notifyAll();
                        }
                        released.await();
                        if (closed) return;
                    }
                    upstream.getOutputStream().write(request);
                    if (selected) {
                        synchronized (lifecycle) {
                            forwardedProfileRequests++;
                            lifecycle.notifyAll();
                        }
                    }
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
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

    private static byte[] readRequest(InputStream input) throws IOException {
        byte[] header = input.readNBytes(HEADER_BYTES);
        if (header.length == 0) return null;
        if (header.length != HEADER_BYTES) throw new EOFException("Incomplete Mongo request header");
        int length = integer(header, 0);
        if (length < HEADER_BYTES || length > MAX_REQUEST_BYTES) throw new IOException("Mongo request length is outside the fixture bound");
        byte[] request = new byte[length];
        System.arraycopy(header, 0, request, 0, HEADER_BYTES);
        if (input.readNBytes(request, HEADER_BYTES, length - HEADER_BYTES) != length - HEADER_BYTES) {
            throw new EOFException("Incomplete Mongo request body");
        }
        return request;
    }

    private static int integer(byte[] bytes, int offset) {
        return ByteBuffer.wrap(bytes, offset, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private record ProfileRequest(String database, String collection, String clusterId) {
        private ProfileRequest {
            if (database == null || database.isBlank() || collection == null || collection.isBlank()
                    || clusterId == null || clusterId.isBlank()) {
                throw new IllegalArgumentException("A profile request requires its actual database, collection and cluster identity");
            }
        }

        private boolean matches(byte[] request) throws IOException {
            if (integer(request, 12) != 2013) return false;
            if (request.length < 26) throw new IOException("Incomplete OP_MSG request");
            int flags = integer(request, 16);
            if ((flags & 0x0000fffc) != 0) throw new IOException("Unsupported required OP_MSG flags");
            int end = request.length - ((flags & 1) == 0 ? 0 : Integer.BYTES);
            if ((flags & 1) != 0) {
                CRC32C checksum = new CRC32C();
                checksum.update(request, 0, end);
                if (checksum.getValue() != Integer.toUnsignedLong(integer(request, end))) {
                    throw new IOException("Invalid OP_MSG checksum");
                }
            }
            BsonDocument command = null;
            int cursor = 20;
            while (cursor < end) {
                int kind = Byte.toUnsignedInt(request[cursor++]);
                if (kind == 0) {
                    if (command != null) throw new IOException("Duplicate OP_MSG command body");
                    int length = bsonLength(request, cursor, end);
                    command = decode(request, cursor, length);
                    cursor += length;
                } else if (kind == 1) {
                    if (end - cursor < 5) throw new IOException("Incomplete OP_MSG document sequence");
                    int size = integer(request, cursor);
                    if (size < 5 || size > end - cursor) throw new IOException("Invalid OP_MSG document sequence size");
                    int sequenceEnd = cursor + size;
                    cursor += Integer.BYTES;
                    while (cursor < sequenceEnd && request[cursor] != 0) cursor++;
                    if (cursor == sequenceEnd) throw new IOException("Missing OP_MSG document sequence identifier");
                    cursor++;
                    while (cursor < sequenceEnd) {
                        int length = bsonLength(request, cursor, sequenceEnd);
                        decode(request, cursor, length);
                        cursor += length;
                    }
                } else { throw new IOException("Unknown OP_MSG section kind"); }
            }
            if (command == null || command.isEmpty() || (flags & ~1) != 0 || !"findAndModify".equals(command.getFirstKey())) return false;
            if (!text(command.get("findAndModify"), collection) || !text(command.get("$db"), database)
                    || !bool(command.get("startTransaction"), true) || !bool(command.get("autocommit"), false)) return false;
            BsonValue query = command.get("query"), update = command.get("update");
            if (query == null || !query.isDocument() || query.asDocument().size() != 1
                    || !text(query.asDocument().get("_id"), clusterId)
                    || update == null || !update.isDocument() || update.asDocument().size() != 1) return false;
            BsonValue increment = update.asDocument().get("$inc");
            if (increment == null || !increment.isDocument() || increment.asDocument().size() != 1) return false;
            BsonValue serial = increment.asDocument().get("writeSerial");
            return serial != null && ((serial.isInt64() && serial.asInt64().getValue() == 1)
                    || (serial.isInt32() && serial.asInt32().getValue() == 1));
        }

        private static boolean text(BsonValue value, String expected) {
            return value != null && value.isString() && expected.equals(value.asString().getValue());
        }
        private static boolean bool(BsonValue value, boolean expected) {
            return value != null && value.isBoolean() && value.asBoolean().getValue() == expected;
        }
        private static int bsonLength(byte[] request, int cursor, int end) throws IOException {
            if (end - cursor < 5) throw new IOException("Incomplete BSON document");
            int length = integer(request, cursor);
            if (length < 5 || length > end - cursor || request[cursor + length - 1] != 0) {
                throw new IOException("Invalid BSON document length");
            }
            return length;
        }
        private static BsonDocument decode(byte[] request, int cursor, int length) throws IOException {
            try { return new RawBsonDocument(request, cursor, length).decode(new BsonDocumentCodec()); }
            catch (RuntimeException invalid) { throw new IOException("Invalid BSON command document"); }
        }
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
