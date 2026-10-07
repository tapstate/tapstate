package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.RawBsonDocument;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

/**
 * A finite, single-owner TCP relay. Only a fully decoded renewal of the issued capture fence may be
 * dropped; no reply is manufactured and no command is rewritten.
 */
final class CaptureRenewWireProxy implements AutoCloseable {
    static final int MAX_CONNECTIONS = 128;
    static final int MAX_INSPECT_BYTES = 64 * 1024;
    static final int STREAM_CHUNK_BYTES = 32 * 1024;
    static final int MAX_BUFFER_BYTES = 8 * 1024 * 1024;
    static final int MAX_QUEUED_BUFFERS = 1024;
    static final int MAX_RECORDS = 512;
    static final int MAX_RECORD_BYTES = 64 * 1024;
    static final int MAX_PHASE_BYTES = 2 * 1024 * 1024;
    private static final int OP_MSG = 2013, OP_COMPRESSED = 2012;
    private static final int HEADER_BYTES = 16;
    private static final int MAX_BSON_DEPTH = 16, MAX_BSON_FIELDS = 1024;
    private static final int FIXED_BUFFER_BYTES = STREAM_CHUNK_BYTES
            + MAX_CONNECTIONS * HEADER_BYTES * 2 + MAX_INSPECT_BYTES * 2;

    private final Object lock = new Object();
    private final Selector selector;
    private final ServerSocketChannel listener;
    private final InetSocketAddress upstream;
    private final long deadline;
    private final Thread owner;
    private final CountDownLatch finished = new CountDownLatch(1);
    private final Map<Long, Pair> pairs = new LinkedHashMap<>();
    private final List<Map<String, Object>> records = new ArrayList<>();
    private final List<String> unavailable = new ArrayList<>();
    private final ByteBuffer scratch = ByteBuffer.allocate(STREAM_CHUNK_BYTES);
    private volatile boolean running = true;
    private volatile Throwable failure;
    private Expected expected;
    private boolean fault;
    private boolean closedPairsBytePreserving = true;
    private long nextPair, accepted, closed, clientFrames, serverFrames, matchedForwarded, dropped;
    private int bufferBytes = FIXED_BUFFER_BYTES, peakBufferBytes = FIXED_BUFFER_BYTES;
    private int peakConnections, evidenceBytes, queuedBuffers, peakQueuedBuffers;

    CaptureRenewWireProxy(InetSocketAddress upstream, long deadline) throws IOException {
        this.upstream = Objects.requireNonNull(upstream);
        this.deadline = deadline;
        selector = Selector.open();
        ServerSocketChannel opened = null;
        try {
            opened = ServerSocketChannel.open();
            opened.configureBlocking(false);
            opened.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            opened.register(selector, SelectionKey.OP_ACCEPT);
            listener = opened;
        } catch (Throwable first) {
            if (opened != null) { try { opened.close(); } catch (Throwable close) { first.addSuppressed(close); } }
            try { selector.close(); } catch (Throwable close) { first.addSuppressed(close); }
            throw first;
        }
        owner = new Thread(this::pump, "capture-renew-wire-owner");
        owner.setDaemon(true);
        owner.start();
    }

    int port() throws IOException {
        return ((InetSocketAddress) listener.getLocalAddress()).getPort();
    }

    void expectRenewal(String database, WorkloadClaim issued, Duration ttl) {
        if (issued.key().type() != WorkloadClaimType.CAPTURE || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("an issued capture and positive TTL are required");
        }
        synchronized (lock) {
            if (fault || expected != null) { throw new IllegalStateException("the selector is already configured"); }
            expected = new Expected(database, issued, ttl);
        }
    }

    void arm() {
        synchronized (lock) {
            check();
            if (expected == null || matchedForwarded == 0) {
                throw new AssertionError("no actual matching capture renewal was forwarded before arming");
            }
            if (!unavailable.isEmpty()) { throw new AssertionError("selector unavailable: " + unavailable); }
            fault = true;
        }
    }

    void disarm() {
        synchronized (lock) { fault = false; }
    }

    void check() {
        if (failure != null) { throw new AssertionError("wire owner failed; qualification is unavailable", failure); }
        if (!running) { throw new AssertionError("wire owner ended before qualification completed"); }
    }

    long matchedForwarded() {
        synchronized (lock) { check(); return matchedForwarded; }
    }

    long dropped() {
        synchronized (lock) { check(); return dropped; }
    }

    boolean drainedAndBytePreserving() {
        synchronized (lock) {
            check();
            if (!unavailable.isEmpty() || accepted == 0 || dropped != 0 || !closedPairsBytePreserving) { return false; }
            return pairs.values().stream().allMatch(pair -> pair.drained()
                    && pair.client.read == pair.upstream.written
                    && pair.upstream.read == pair.client.written
                    && digest(pair.client.in).equals(digest(pair.upstream.out))
                    && digest(pair.upstream.in).equals(digest(pair.client.out)));
        }
    }

    Map<String, Object> snapshot() {
        synchronized (lock) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("acceptedPairs", accepted);
            out.put("closedPairs", closed);
            out.put("closedPairsBytePreserving", closedPairsBytePreserving);
            out.put("activePairs", pairs.values().stream().map(Pair::evidence).toList());
            out.put("peakConnections", peakConnections);
            out.put("clientFrames", clientFrames);
            out.put("serverFrames", serverFrames);
            out.put("matchedForwarded", matchedForwarded);
            out.put("selectedDropped", dropped);
            out.put("faultArmed", fault);
            out.put("selectorUnavailable", List.copyOf(unavailable));
            out.put("ownerFailure", failure == null ? "ABSENT" : failure.toString());
            out.put("ownerFinished", finished.getCount() == 0);
            out.put("bufferBytes", bufferBytes);
            out.put("peakBufferBytes", peakBufferBytes);
            out.put("queuedBuffers", queuedBuffers);
            out.put("peakQueuedBuffers", peakQueuedBuffers);
            out.put("evidenceBytes", evidenceBytes);
            out.put("records", List.copyOf(records));
            out.put("expected", expected == null ? Map.of() : expected.evidence);
            return out;
        }
    }

    private void pump() {
        try {
            while (running && System.nanoTime() - deadline < 0) {
                long millis = Math.max(1, Math.min(50,
                        TimeUnit.NANOSECONDS.toMillis(Math.max(0, deadline - System.nanoTime()))));
                selector.select(millis);
                synchronized (lock) {
                    var iterator = selector.selectedKeys().iterator();
                    while (iterator.hasNext()) {
                        SelectionKey key = iterator.next();
                        iterator.remove();
                        if (!key.isValid()) { continue; }
                        if (key.channel() == listener) { accept(); continue; }
                        Endpoint end = (Endpoint) key.attachment();
                        try {
                            if (key.isConnectable()) {
                                if (end.channel.finishConnect()) {
                                    end.connected = true;
                                    interests(end);
                                    finishEof(end.pair);
                                }
                            }
                            if (key.isValid() && key.isReadable()) { read(end); }
                            if (key.isValid() && key.isWritable()) { write(end); }
                        } catch (IOException io) {
                            closePair(end.pair, "ACTUAL_SOCKET_IO", io);
                            unavailable("UNSELECTED_SOCKET_IO:" + io.getClass().getSimpleName());
                        }
                    }
                }
            }
            if (running) { failure = new AssertionError("original qualification deadline expired"); }
        } catch (Throwable first) {
            failure = first;
        } finally {
            synchronized (lock) {
                running = false;
                for (Pair pair : List.copyOf(pairs.values())) {
                    try { closePair(pair, "OWNER_END", null); }
                    catch (Throwable close) { preserveFailure(close); }
                }
                try { listener.close(); } catch (Throwable close) { preserveFailure(close); }
                try { selector.close(); } catch (Throwable close) { preserveFailure(close); }
            }
            finished.countDown();
        }
    }

    private void accept() throws IOException {
        SocketChannel client;
        while ((client = listener.accept()) != null) {
            if (pairs.size() >= MAX_CONNECTIONS) {
                client.close();
                throw new AssertionError("connection-table overflow; qualification unavailable");
            }
            SocketChannel remote = null;
            try {
                client.configureBlocking(false);
                client.socket().setTcpNoDelay(true);
                remote = SocketChannel.open();
                remote.configureBlocking(false);
                remote.socket().setTcpNoDelay(true);
                Pair pair = new Pair(++nextPair, client, remote);
                pair.client.key = client.register(selector, SelectionKey.OP_READ, pair.client);
                pair.upstream.connected = remote.connect(upstream);
                pair.upstream.key = remote.register(selector, pair.upstream.connected
                        ? SelectionKey.OP_READ : SelectionKey.OP_CONNECT, pair.upstream);
                pairs.put(pair.id, pair);
                accepted++;
                peakConnections = Math.max(peakConnections, pairs.size());
                record(Map.of("kind", "ACCEPT", "pair", pair.id, "at", pair.acceptedAt,
                        "client", String.valueOf(client.getRemoteAddress()), "upstream", upstream.toString()));
            } catch (Throwable first) {
                try { client.close(); } catch (Throwable close) { first.addSuppressed(close); }
                if (remote != null) { try { remote.close(); } catch (Throwable close) { first.addSuppressed(close); } }
                throw first;
            }
        }
    }

    private void read(Endpoint from) throws IOException {
        int allowance = readAllowance(from);
        if (allowance == 0) { interests(from); return; }
        scratch.clear();
        scratch.limit(allowance);
        int count = from.channel.read(scratch);
        if (count < 0) {
            from.readEof = true;
            if (from == from.pair.client) { from.pair.requests.eof(); }
            else if (from.pair.responses.headerPosition != 0 || from.pair.responses.remaining != 0) {
                unavailable("TRUNCATED_RESPONSE");
            }
            interests(from);
            finishEof(from.pair);
            return;
        }
        if (count == 0) { return; }
        scratch.flip();
        from.in.update(scratch.asReadOnlyBuffer());
        from.read += count;
        if (from == from.pair.client) {
            from.pair.requests.accept(scratch);
        } else {
            from.pair.responses.accept(scratch.asReadOnlyBuffer());
            enqueueCopy(from.pair.client, scratch);
        }
    }

    private void write(Endpoint to) throws IOException {
        while (!to.pending.isEmpty()) {
            ByteBuffer buffer = to.pending.peekFirst();
            int before = buffer.position();
            int count = to.channel.write(buffer);
            if (count == 0) { break; }
            ByteBuffer written = buffer.asReadOnlyBuffer();
            written.position(before).limit(before + count);
            to.out.update(written);
            to.written += count;
            if (!buffer.hasRemaining()) {
                to.pending.removeFirst();
                queuedBuffers--;
                release(buffer.capacity());
            }
        }
        interests(to);
        finishEof(to.pair);
    }

    private void interests(Endpoint end) {
        if (!end.key.isValid()) { return; }
        int interests = end.connected ? (end.readEof || readAllowance(end) == 0 ? 0 : SelectionKey.OP_READ)
                : SelectionKey.OP_CONNECT;
        if (end.connected && !end.pending.isEmpty()) { interests |= SelectionKey.OP_WRITE; }
        end.key.interestOps(interests);
    }

    private int readAllowance(Endpoint end) {
        int available = MAX_BUFFER_BYTES - bufferBytes;
        if (queuedBuffers >= MAX_QUEUED_BUFFERS) { return 0; }
        if (end == end.pair.upstream) { return Math.min(STREAM_CHUNK_BYTES, available); }
        Requests request = end.pair.requests;
        if (request.frame != null) {
            return Math.min(STREAM_CHUNK_BYTES, request.frame.length - request.position);
        }
        if (request.largeRemaining > 0) {
            return Math.min(Math.min(STREAM_CHUNK_BYTES, available), request.largeRemaining);
        }
        if (request.raw) { return Math.min(STREAM_CHUNK_BYTES, available); }
        // Reserve room for the largest inspected frame before reading the header that allocates it.
        return available >= MAX_INSPECT_BYTES ? HEADER_BYTES - request.position : 0;
    }

    private void refreshReads() {
        for (Pair pair : pairs.values()) {
            interests(pair.client);
            interests(pair.upstream);
        }
    }

    private void finishEof(Pair pair) throws IOException {
        if (pair.closed) { return; }
        for (Endpoint end : List.of(pair.client, pair.upstream)) {
            Endpoint other = end == pair.client ? pair.upstream : pair.client;
            if (other.readEof && end.connected && end.pending.isEmpty() && !end.outputEnded) {
                end.channel.shutdownOutput();
                end.outputEnded = true;
            }
        }
        if (pair.client.readEof && pair.upstream.readEof
                && pair.client.pending.isEmpty() && pair.upstream.pending.isEmpty()) {
            closePair(pair, "ACTUAL_EOF_DRAINED", null);
        }
    }

    private void enqueueCopy(Endpoint end, ByteBuffer input) {
        int size = input.remaining();
        if (size == 0) { return; }
        reserve(size);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put(input).flip();
        queue(end, buffer);
        interests(end);
    }

    private void enqueueOwned(Endpoint end, byte[] bytes) {
        queue(end, ByteBuffer.wrap(bytes));
        interests(end);
    }

    private void queue(Endpoint end, ByteBuffer buffer) {
        if (queuedBuffers >= MAX_QUEUED_BUFFERS) {
            throw new AssertionError("queued-buffer count overflow; qualification unavailable");
        }
        end.pending.addLast(buffer);
        queuedBuffers++;
        peakQueuedBuffers = Math.max(peakQueuedBuffers, queuedBuffers);
        refreshReads();
    }

    private void reserve(int count) {
        if (count < 0 || count > MAX_BUFFER_BYTES - bufferBytes) {
            throw new AssertionError("aggregate buffer overflow; qualification unavailable");
        }
        bufferBytes += count;
        peakBufferBytes = Math.max(peakBufferBytes, bufferBytes);
        refreshReads();
    }

    private void release(int count) {
        bufferBytes -= count;
        if (bufferBytes < FIXED_BUFFER_BYTES) { throw new IllegalStateException("buffer accounting underflow"); }
        refreshReads();
    }

    private void closePair(Pair pair, String reason, Throwable original) throws IOException {
        if (pair.closed) { return; }
        closedPairsBytePreserving &= pair.drained()
                && pair.client.read == pair.upstream.written && pair.upstream.read == pair.client.written
                && digest(pair.client.in).equals(digest(pair.upstream.out))
                && digest(pair.upstream.in).equals(digest(pair.client.out));
        pair.closed = true;
        IOException closeFailure = null;
        for (Endpoint end : List.of(pair.client, pair.upstream)) {
            end.key.cancel();
            try { end.channel.close(); } catch (IOException close) {
                if (closeFailure == null) { closeFailure = close; } else { closeFailure.addSuppressed(close); }
            }
            while (!end.pending.isEmpty()) {
                queuedBuffers--;
                release(end.pending.removeFirst().capacity());
            }
        }
        if (pair.requests.frame != null) { release(pair.requests.frame.length); pair.requests.frame = null; }
        pairs.remove(pair.id);
        closed++;
        Map<String, Object> out = new LinkedHashMap<>(pair.evidence());
        out.put("kind", "CLOSE");
        out.put("at", Instant.now().toString());
        out.put("reason", reason);
        out.put("socketError", original == null ? "ABSENT" : original.toString());
        record(out);
        if (closeFailure != null) {
            if (original != null) { original.addSuppressed(closeFailure); }
            else { throw closeFailure; }
        }
    }

    private void unavailable(String reason) {
        fault = false;
        if (!unavailable.contains(reason)) {
            if (unavailable.size() >= 32) { throw new AssertionError("unavailable-reason overflow"); }
            unavailable.add(reason);
        }
    }

    private void record(Map<String, Object> entry) {
        int size = JsonWriter.write(entry).getBytes(StandardCharsets.UTF_8).length;
        if (records.size() >= MAX_RECORDS || size > MAX_RECORD_BYTES
                || size > MAX_PHASE_BYTES - evidenceBytes) {
            throw new AssertionError("wire evidence overflow; no records were trimmed");
        }
        records.add(entry);
        evidenceBytes += size;
    }

    private void preserveFailure(Throwable close) {
        if (failure == null) { failure = close; }
        else if (failure != close) { failure.addSuppressed(close); }
    }

    @Override
    public void close() {
        disarm();
        running = false;
        selector.wakeup();
        boolean ended = finished.getCount() == 0;
        try {
            long remaining = deadline - System.nanoTime();
            if (!ended && remaining > 0) { ended = finished.await(remaining, TimeUnit.NANOSECONDS); }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("wire-owner end was not confirmed", interrupted);
        }
        if (!ended) { throw new AssertionError("wire-owner end exceeded the original deadline"); }
        if (failure != null) { throw new AssertionError("wire-owner cleanup retained a failure", failure); }
    }

    private final class Pair {
        final long id;
        final String acceptedAt = Instant.now().toString();
        final Endpoint client, upstream;
        final Requests requests = new Requests(this);
        final Responses responses = new Responses(this);
        boolean closed;
        Pair(long id, SocketChannel client, SocketChannel upstream) {
            this.id = id;
            this.client = new Endpoint(this, client, true);
            this.upstream = new Endpoint(this, upstream, false);
        }
        boolean drained() {
            return client.pending.isEmpty() && upstream.pending.isEmpty()
                    && requests.position == 0 && requests.largeRemaining == 0 && requests.frame == null
                    && responses.headerPosition == 0 && responses.remaining == 0;
        }
        Map<String, Object> evidence() {
            return Map.of("pair", id, "acceptedAt", acceptedAt, "closed", closed,
                    "upstreamConnected", upstream.connected,
                    "clientRead", client.read, "upstreamWritten", upstream.written,
                    "upstreamRead", upstream.read, "clientWritten", client.written,
                    "hashes", Map.of("clientInput", digest(client.in), "upstreamOutput", digest(upstream.out),
                            "upstreamInput", digest(upstream.in), "clientOutput", digest(client.out)));
        }
    }

    private static final class Endpoint {
        final Pair pair;
        final SocketChannel channel;
        final ArrayDeque<ByteBuffer> pending = new ArrayDeque<>();
        final MessageDigest in = sha256(), out = sha256();
        SelectionKey key;
        boolean connected;
        boolean readEof, outputEnded;
        long read, written;
        Endpoint(Pair pair, SocketChannel channel, boolean connected) {
            this.pair = pair; this.channel = channel; this.connected = connected;
        }
    }

    private final class Requests {
        final Pair pair;
        final byte[] header = new byte[HEADER_BYTES];
        byte[] frame;
        int position, largeRemaining, largeLength;
        boolean raw;
        MessageDigest largeDigest;
        Requests(Pair pair) { this.pair = pair; }

        void accept(ByteBuffer input) throws IOException {
            while (input.hasRemaining() && !pair.closed) {
                if (raw) { enqueueCopy(pair.upstream, input); return; }
                if (largeRemaining > 0) {
                    int take = Math.min(input.remaining(), largeRemaining);
                    ByteBuffer part = input.slice();
                    part.limit(take);
                    largeDigest.update(part.asReadOnlyBuffer());
                    enqueueCopy(pair.upstream, part);
                    input.position(input.position() + take);
                    largeRemaining -= take;
                    if (largeRemaining == 0) {
                        clientFrames++;
                        record(frameEvidence(pair, header, largeLength,
                                HexFormat.of().formatHex(largeDigest.digest()), "LARGE_UNSELECTED", Map.of()));
                        position = 0;
                    }
                    continue;
                }
                if (frame == null) {
                    int take = Math.min(input.remaining(), HEADER_BYTES - position);
                    input.get(header, position, take);
                    position += take;
                    if (position < HEADER_BYTES) { return; }
                    int length = little(header, 0);
                    int opcode = little(header, 12);
                    if (length < HEADER_BYTES || !(opcode == 2004 || opcode == OP_MSG || opcode == OP_COMPRESSED)) {
                        unavailable("UNSUPPORTED_HEADER_OR_ENCRYPTED_STREAM");
                        raw = true;
                        enqueueCopy(pair.upstream, ByteBuffer.wrap(header));
                        continue;
                    }
                    if (length > MAX_INSPECT_BYTES) {
                        if (little(header, 12) == OP_COMPRESSED) { unavailable("OP_COMPRESSED"); }
                        largeLength = length;
                        largeRemaining = length - HEADER_BYTES;
                        largeDigest = sha256();
                        largeDigest.update(header);
                        enqueueCopy(pair.upstream, ByteBuffer.wrap(header));
                        continue;
                    }
                    reserve(length);
                    frame = new byte[length];
                    System.arraycopy(header, 0, frame, 0, HEADER_BYTES);
                    interests(pair.client);
                }
                int take = Math.min(input.remaining(), frame.length - position);
                input.get(frame, position, take);
                position += take;
                if (position != frame.length) { return; }
                byte[] complete = frame;
                Inspection inspection = inspect(complete);
                if (expected != null && inspection.body != null && expected.renewalNamespace(inspection.body)
                        && (!inspection.selectable || !Expected.SUPPORTED_KEYS.containsAll(inspection.body.keySet()))) {
                    unavailable("UNSUPPORTED_FIND_AND_MODIFY_SELECTION_LAYOUT");
                    inspection = new Inspection(inspection.body, false, "UNAVAILABLE_PASSTHROUGH");
                }
                boolean selected = expected != null && inspection.body != null && inspection.selectable
                        && expected.matches(inspection.body);
                Map<String, Object> details = selected ? expected.observed(inspection.body) : Map.of();
                boolean dropping = selected && fault;
                record(frameEvidence(pair, complete, complete.length, hash(complete),
                        dropping ? "SELECTED_DROP" : selected ? "MATCHED_FORWARD" : inspection.status, details));
                clientFrames++;
                frame = null;
                position = 0;
                if (dropping) {
                    dropped++;
                    release(complete.length);
                    closePair(pair, "SELECTED_CAPTURE_RENEW_EOF", null);
                    return;
                }
                if (selected) { matchedForwarded++; }
                enqueueOwned(pair.upstream, complete);
            }
        }

        void eof() {
            if (frame != null) {
                unavailable("TRUNCATED_CLIENT_FRAME");
                byte[] unfinished = frame;
                release(unfinished.length);
                frame = null;
                enqueueCopy(pair.upstream, ByteBuffer.wrap(unfinished, 0, position));
            } else if (position > 0 && largeRemaining == 0 && !raw) {
                unavailable("TRUNCATED_CLIENT_HEADER");
                enqueueCopy(pair.upstream, ByteBuffer.wrap(header, 0, position));
            } else if (largeRemaining > 0) { unavailable("TRUNCATED_LARGE_CLIENT_FRAME"); }
            position = 0;
            largeRemaining = 0;
        }
    }

    private final class Responses {
        final Pair pair;
        final byte[] header = new byte[HEADER_BYTES];
        int headerPosition, remaining, length;
        MessageDigest frameDigest;
        boolean raw;
        Responses(Pair pair) { this.pair = pair; }
        void accept(ByteBuffer input) {
            while (input.hasRemaining() && !raw) {
                if (remaining == 0) {
                    int take = Math.min(input.remaining(), HEADER_BYTES - headerPosition);
                    input.get(header, headerPosition, take);
                    headerPosition += take;
                    if (headerPosition < HEADER_BYTES) { return; }
                    length = little(header, 0);
                    int opcode = little(header, 12);
                    if (length < HEADER_BYTES || !(opcode == 1 || opcode == OP_MSG || opcode == OP_COMPRESSED)) {
                        unavailable("UNSUPPORTED_RESPONSE_OR_ENCRYPTED_STREAM"); raw = true; return;
                    }
                    if (little(header, 12) == OP_COMPRESSED) { unavailable("OP_COMPRESSED_RESPONSE"); }
                    frameDigest = sha256();
                    frameDigest.update(header);
                    remaining = length - HEADER_BYTES;
                }
                int take = Math.min(input.remaining(), remaining);
                ByteBuffer part = input.slice();
                part.limit(take);
                frameDigest.update(part);
                input.position(input.position() + take);
                remaining -= take;
                if (remaining == 0) {
                    serverFrames++;
                    record(frameEvidence(pair, header, length,
                            HexFormat.of().formatHex(frameDigest.digest()), "RESPONSE_STREAMED", Map.of()));
                    headerPosition = 0;
                }
            }
        }
    }

    private Map<String, Object> frameEvidence(Pair pair, byte[] header, int length,
            String hash, String disposition, Map<String, Object> selection) {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("kind", "FRAME");
        record.put("pair", pair.id);
        record.put("at", Instant.now().toString());
        record.put("length", length);
        record.put("requestId", little(header, 4));
        record.put("responseTo", little(header, 8));
        record.put("opcode", little(header, 12));
        record.put("sha256", hash);
        record.put("disposition", disposition);
        record.put("selection", selection);
        if (disposition.equals("SELECTED_DROP")) { record.put("selectedBytesForwarded", 0); }
        return record;
    }

    private record Inspection(BsonDocument body, boolean selectable, String status) { }

    private Inspection inspect(byte[] bytes) {
        int opcode = little(bytes, 12);
        if (opcode == OP_COMPRESSED) {
            unavailable("OP_COMPRESSED");
            return new Inspection(null, false, "COMPRESSED_PASSTHROUGH");
        }
        if (opcode != OP_MSG) { return new Inspection(null, false, "OTHER_OPCODE_PASSTHROUGH"); }
        try {
            if (bytes.length < 21) { throw new IllegalArgumentException("short OP_MSG"); }
            int flags = little(bytes, 16);
            if ((flags & ~(1 | 2 | 65536)) != 0) {
                throw new IllegalArgumentException("unsupported OP_MSG flags");
            }
            int end = bytes.length - ((flags & 1) == 0 ? 0 : 4);
            if ((flags & 1) != 0) {
                CRC32C checksum = new CRC32C();
                checksum.update(bytes, 0, end);
                if (checksum.getValue() != Integer.toUnsignedLong(little(bytes, end))) {
                    throw new IllegalArgumentException("OP_MSG checksum mismatch");
                }
            }
            int at = 20;
            byte[] body = null;
            boolean sequences = false;
            while (at < end) {
                int kind = Byte.toUnsignedInt(bytes[at++]);
                if (kind == 0) {
                    if (body != null) { throw new IllegalArgumentException("multiple OP_MSG bodies"); }
                    int size = boundedLength(bytes, at, end);
                    validateBson(bytes, at, at + size, 0, new int[] {0});
                    body = Arrays.copyOfRange(bytes, at, at + size);
                    at += size;
                } else if (kind == 1) {
                    sequences = true;
                    int size = boundedLength(bytes, at, end);
                    int sectionEnd = at + size;
                    int documentAt = cstringEnd(bytes, at + 4, sectionEnd);
                    while (documentAt < sectionEnd) {
                        int documentSize = boundedLength(bytes, documentAt, sectionEnd);
                        validateBson(bytes, documentAt, documentAt + documentSize, 0, new int[] {0});
                        documentAt += documentSize;
                    }
                    if (documentAt != sectionEnd) { throw new IllegalArgumentException("invalid sequence boundary"); }
                    at = sectionEnd;
                } else { throw new IllegalArgumentException("unknown OP_MSG section"); }
            }
            if (at != end || body == null) { throw new IllegalArgumentException("missing or incomplete OP_MSG body"); }
            return new Inspection(new RawBsonDocument(body), !sequences && (flags & (2 | 65536)) == 0,
                    "OP_MSG_UNSELECTED");
        } catch (RuntimeException malformed) {
            unavailable("OP_MSG_UNAVAILABLE:" + malformed.getClass().getSimpleName() + ":" + malformed.getMessage());
            return new Inspection(null, false, "UNAVAILABLE_PASSTHROUGH");
        }
    }

    private static final class Expected {
        private static final Set<String> SUPPORTED_KEYS = Set.of("findAndModify", "query", "update", "new", "upsert",
                "writeConcern", "maxTimeMS", "$db", "lsid", "txnNumber", "$clusterTime", "$readPreference",
                "readConcern", "stmtId", "stmtIds");
        final String database;
        final BsonDocument query, update;
        final Map<String, Object> evidence;
        Expected(String database, WorkloadClaim issued, Duration ttl) {
            this.database = Objects.requireNonNull(database);
            BsonDocument id = new BsonDocument("clusterId", new BsonString(issued.key().clusterId()))
                    .append("resourceType", new BsonString(issued.key().type().name()))
                    .append("resourceId", new BsonString(issued.key().resourceId()));
            BsonDocument fence = new BsonDocument("_id", id)
                    .append("ownerNodeId", new BsonString(issued.owner().nodeId()))
                    .append("ownerBootId", new BsonString(issued.owner().bootId()))
                    .append("claimGeneration", new BsonInt64(issued.claimGeneration()))
                    .append("executionGeneration", new BsonInt64(issued.executionGeneration()));
            query = new BsonDocument("$and", new BsonArray(List.of(fence,
                    new BsonDocument("topologyRevision", new BsonInt64(issued.topologyRevision())),
                    new BsonDocument("$expr", new BsonDocument("$gt",
                            new BsonArray(List.of(new BsonString("$leaseUntil"), new BsonString("$$NOW"))))))));
            update = new BsonDocument("$set", new BsonDocument("leaseUntil",
                    new BsonDocument("$dateAdd", new BsonDocument("startDate", new BsonString("$$NOW"))
                            .append("unit", new BsonString("millisecond")).append("amount", new BsonInt64(ttl.toMillis())))));
            evidence = Map.of("database", database, "collection", "workload_claims",
                    "query", query.toJson(), "update", new BsonArray(List.of(update)).toString(),
                    "issuedLeaseUntil", issued.leaseUntil().toString());
        }
        boolean matches(BsonDocument body) {
            return SUPPORTED_KEYS.containsAll(body.keySet()) && renewalNamespace(body)
                    && query.equals(body.get("query"))
                    && query.toJson().equals(body.getDocument("query").toJson())
                    && new BsonArray(List.of(update)).equals(body.get("update"))
                    && absentOrFalse(body, "upsert")
                    && body.get("new") instanceof org.bson.BsonBoolean value && value.getValue();
        }
        boolean renewalNamespace(BsonDocument body) {
            return body.get("$db") instanceof BsonString db && db.getValue().equals(database)
                    && body.get("findAndModify") instanceof BsonString collection
                    && collection.getValue().equals("workload_claims");
        }
        private static boolean absentOrFalse(BsonDocument body, String key) {
            return !body.containsKey(key)
                    || body.get(key) instanceof org.bson.BsonBoolean value && !value.getValue();
        }
        Map<String, Object> observed(BsonDocument body) {
            Map<String, Object> value = new LinkedHashMap<>(evidence);
            value.put("actualQuery", body.getDocument("query").toJson());
            value.put("actualUpdate", body.getArray("update").toString());
            value.put("retryMetadata", Map.of(
                    "lsid", body.containsKey("lsid") ? body.get("lsid").toString() : "ABSENT",
                    "txnNumber", body.containsKey("txnNumber") ? body.get("txnNumber").toString() : "ABSENT",
                    "stmtId", body.containsKey("stmtId") ? body.get("stmtId").toString() : "ABSENT",
                    "stmtIds", body.containsKey("stmtIds") ? body.get("stmtIds").toString() : "ABSENT"));
            return value;
        }
    }

    /** Checks every key before a BSON decoder could collapse duplicate names. */
    private static void validateBson(byte[] bytes, int begin, int end, int depth, int[] fields) {
        if (depth > MAX_BSON_DEPTH || end - begin < 5 || little(bytes, begin) != end - begin
                || bytes[end - 1] != 0) { throw new IllegalArgumentException("BSON document bound"); }
        Set<String> names = new HashSet<>();
        int at = begin + 4;
        while (at < end - 1) {
            if (++fields[0] > MAX_BSON_FIELDS) { throw new IllegalArgumentException("BSON field cap"); }
            int type = Byte.toUnsignedInt(bytes[at++]);
            int afterName = cstringEnd(bytes, at, end - 1);
            String name = utf8(bytes, at, afterName - at - 1);
            if (!names.add(name)) { throw new IllegalArgumentException("duplicate BSON key"); }
            at = afterName;
            switch (type) {
                case 1, 9, 17, 18 -> at = boundedAdvance(at, 8, end - 1);
                case 2, 13, 14 -> at = bsonStringEnd(bytes, at, end - 1);
                case 3, 4 -> {
                    int size = boundedLength(bytes, at, end - 1);
                    validateBson(bytes, at, at + size, depth + 1, fields);
                    at += size;
                }
                case 5 -> {
                    int length = littleChecked(bytes, at, end - 1);
                    if (length < 0) { throw new IllegalArgumentException("negative binary length"); }
                    at = boundedAdvance(at, Math.addExact(length, 5), end - 1);
                }
                case 6, 10, 127, 255 -> { }
                case 7 -> at = boundedAdvance(at, 12, end - 1);
                case 8 -> {
                    if (at >= end - 1 || (bytes[at] != 0 && bytes[at] != 1)) {
                        throw new IllegalArgumentException("invalid BSON boolean");
                    }
                    at++;
                }
                case 11 -> at = cstringEnd(bytes, cstringEnd(bytes, at, end - 1), end - 1);
                case 12 -> at = boundedAdvance(bsonStringEnd(bytes, at, end - 1), 12, end - 1);
                case 15 -> {
                    int total = littleChecked(bytes, at, end - 1);
                    int scopeEnd = boundedAdvance(at, total, end - 1);
                    int scopeBegin = bsonStringEnd(bytes, at + 4, scopeEnd);
                    validateBson(bytes, scopeBegin, scopeEnd, depth + 1, fields);
                    at = scopeEnd;
                }
                case 16 -> at = boundedAdvance(at, 4, end - 1);
                case 19 -> at = boundedAdvance(at, 16, end - 1);
                default -> throw new IllegalArgumentException("unsupported BSON type");
            }
        }
        if (at != end - 1) { throw new IllegalArgumentException("BSON element boundary"); }
    }

    private static int bsonStringEnd(byte[] bytes, int at, int end) {
        int length = littleChecked(bytes, at, end);
        if (length < 1) { throw new IllegalArgumentException("BSON string length"); }
        int after = boundedAdvance(at + 4, length, end);
        if (bytes[after - 1] != 0) { throw new IllegalArgumentException("BSON string terminator"); }
        utf8(bytes, at + 4, length - 1);
        return after;
    }
    private static int boundedLength(byte[] bytes, int at, int end) {
        int length = littleChecked(bytes, at, end);
        if (length < 5 || length > end - at) { throw new IllegalArgumentException("section or BSON length"); }
        return length;
    }
    private static int boundedAdvance(int at, int count, int end) {
        if (count < 0 || at < 0 || count > end - at) { throw new IllegalArgumentException("BSON value bound"); }
        return at + count;
    }
    private static int cstringEnd(byte[] bytes, int at, int end) {
        int start = at;
        while (at < end && bytes[at] != 0) { at++; }
        if (at >= end) { throw new IllegalArgumentException("unterminated BSON cstring"); }
        utf8(bytes, start, at - start);
        return at + 1;
    }
    private static String utf8(byte[] bytes, int at, int length) {
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes, at, length)).toString();
        } catch (CharacterCodingException malformed) { throw new IllegalArgumentException("invalid BSON UTF-8", malformed); }
    }
    private static int littleChecked(byte[] bytes, int at, int end) {
        boundedAdvance(at, 4, end);
        return little(bytes, at);
    }
    private static int little(byte[] bytes, int at) {
        return ByteBuffer.wrap(bytes, at, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }
    private static MessageDigest sha256() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static String hash(byte[] bytes) { return HexFormat.of().formatHex(sha256().digest(bytes)); }
    private static String digest(MessageDigest digest) {
        try { return HexFormat.of().formatHex(((MessageDigest) digest.clone()).digest()); }
        catch (CloneNotSupportedException unavailable) { throw new AssertionError("SHA-256 snapshot unavailable", unavailable); }
    }
}
