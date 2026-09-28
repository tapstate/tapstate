package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A versioned deterministic binary stream codec for one logical observation. */
final class LatestObservationPayloadCodec {

    static final int ENCODING_VERSION = 2;
    static final int INLINE_PAYLOAD_LIMIT = 512 * 1024;
    static final int CHUNK_PAYLOAD_LIMIT = 1024 * 1024;

    private static final int MAGIC = 0x54534c4f; // TSLO

    private LatestObservationPayloadCodec() {
    }

    interface ChunkWriter {
        void begin();
        void write(Chunk chunk);
    }

    record Encoded(byte[] inlinePayload, byte[] payloadDigest, long chunkCount, long encodedBytes) {
        Encoded {
            inlinePayload = inlinePayload == null ? null : Arrays.copyOf(inlinePayload, inlinePayload.length);
            payloadDigest = Arrays.copyOf(Objects.requireNonNull(payloadDigest, "payloadDigest"),
                    payloadDigest.length);
            if (chunkCount < 0 || encodedBytes < 0 || (inlinePayload == null) == (chunkCount == 0)) {
                throw new IllegalArgumentException("encoded payload is either inline or a positive chunk sequence");
            }
        }
        @Override public byte[] inlinePayload() {
            return inlinePayload == null ? null : Arrays.copyOf(inlinePayload, inlinePayload.length);
        }
        @Override public byte[] payloadDigest() { return Arrays.copyOf(payloadDigest, payloadDigest.length); }
        boolean inline() { return inlinePayload != null; }
    }

    record Chunk(long ordinal, byte[] payload, byte[] digest) {
        Chunk {
            if (ordinal < 0) {
                throw new IllegalArgumentException("a chunk ordinal is non-negative");
            }
            payload = Arrays.copyOf(Objects.requireNonNull(payload, "payload"), payload.length);
            digest = Arrays.copyOf(Objects.requireNonNull(digest, "digest"), digest.length);
            if (payload.length == 0 || payload.length > CHUNK_PAYLOAD_LIMIT) {
                throw new IllegalArgumentException("a chunk payload is non-empty and bounded");
            }
        }
        @Override public byte[] payload() { return Arrays.copyOf(payload, payload.length); }
        @Override public byte[] digest() { return Arrays.copyOf(digest, digest.length); }
    }

    static Encoded encode(Observation observation, ChunkWriter chunks) {
        Objects.requireNonNull(observation, "observation");
        ChunkingOutput output = new ChunkingOutput(Objects.requireNonNull(chunks, "chunks"));
        try (PayloadDataOutput data = new PayloadDataOutput(output)) {
            data.writeInt(MAGIC);
            data.writeInt(ENCODING_VERSION);
            writeString(data, observation.pipelineId());
            writeString(data, observation.state().name());
            writeNullableInstant(data, observation.observedAt());
            writeLongMap(data, observation.metrics());
            writeSnapshot(data, observation.snapshot());
            writeStringMap(data, observation.positions());
            writeFailure(data, observation.failure());
            writeFacts(data, observation.facts());
            data.flush();
        } catch (IOException impossible) {
            throw new IllegalStateException("the in-memory observation encoder could not close", impossible);
        }
        return output.finish();
    }

    static Observation decodeInline(byte[] payload, byte[] expectedDigest, long expectedBytes) {
        return decodeInline(payload, expectedDigest, expectedBytes, ENCODING_VERSION);
    }

    static Observation decodeInline(byte[] payload, byte[] expectedDigest, long expectedBytes, int version) {
        requireSupportedVersion(version);
        Objects.requireNonNull(payload, "payload");
        if (payload.length != expectedBytes || !MessageDigest.isEqual(payloadDigest(payload, version), expectedDigest)) {
            throw new IllegalArgumentException("inline payload length or digest does not match its descriptor");
        }
        return decode(new ByteArrayInputStream(payload), expectedBytes, version);
    }

    static Observation decodeChunks(Iterable<Chunk> chunks, long expectedCount,
            long expectedBytes, byte[] expectedDigest) {
        return decodeChunks(chunks, expectedCount, expectedBytes, expectedDigest, ENCODING_VERSION);
    }

    static Observation decodeChunks(Iterable<Chunk> chunks, long expectedCount,
            long expectedBytes, byte[] expectedDigest, int version) {
        requireSupportedVersion(version);
        if (expectedCount <= 0 || expectedBytes <= 0) {
            throw new IllegalArgumentException("a chunk descriptor needs positive count and bytes");
        }
        try (ChunkInput input = new ChunkInput(chunks.iterator(), expectedCount, expectedBytes, expectedDigest, version)) {
            Observation decoded = decode(input, expectedBytes, version);
            input.verifyComplete();
            return decoded;
        } catch (IOException impossible) {
            throw new IllegalStateException("the in-memory observation decoder could not close", impossible);
        }
    }

    static boolean supportsVersion(int version) {
        return version == 1 || version == ENCODING_VERSION;
    }

    private static void requireSupportedVersion(int version) {
        if (!supportsVersion(version)) {
            throw new IllegalArgumentException("observation payload version is not supported");
        }
    }

    private static Observation decode(InputStream input, long expectedBytes, int version) {
        try {
            BudgetInput budget = new BudgetInput(input, expectedBytes, version);
            DataInputStream data = new DataInputStream(budget);
            if (data.readInt() != MAGIC || data.readInt() != version) {
                throw new IllegalArgumentException("observation payload version is not supported");
            }
            String pipelineId = readString(data, budget);
            PipelineState state = PipelineState.valueOf(readString(data, budget));
            Instant observedAt = readNullableInstant(data);
            Map<String, Long> metrics = readLongMap(data, budget);
            Map<String, TableSnapshot> snapshot = readSnapshot(data, budget);
            Map<String, String> positions = readStringMap(data, budget);
            ObservationFailure failure = readFailure(data, budget);
            List<MetricFact> facts = readFacts(data, budget);
            if (data.read() != -1) {
                throw new IllegalArgumentException("observation payload carries trailing content");
            }
            return new Observation(pipelineId, state, metrics, snapshot, positions, failure, observedAt, facts);
        } catch (TapstateException coded) {
            throw coded;
        } catch (EOFException truncated) {
            throw new IllegalArgumentException("observation payload is truncated", truncated);
        } catch (IOException | IllegalArgumentException malformed) {
            throw new IllegalArgumentException("observation payload is not a valid versioned record", malformed);
        }
    }

    private static void writeLongMap(PayloadDataOutput out, Map<String, Long> values) throws IOException {
        out.writeInt(values.size());
        for (Map.Entry<String, Long> entry : values.entrySet()) {
            writeString(out, entry.getKey());
            out.writeLong(entry.getValue());
        }
    }

    private static Map<String, Long> readLongMap(DataInputStream in, BudgetInput budget) throws IOException {
        int count = readCount(in, budget, budget.stringPrefixBytes() + Long.BYTES);
        Map<String, Long> values = new LinkedHashMap<>();
        for (int index = 0; index < count; index++) {
            putDistinct(values, readString(in, budget), in.readLong());
        }
        return values;
    }

    private static void writeStringMap(PayloadDataOutput out, Map<String, String> values) throws IOException {
        out.writeInt(values.size());
        for (Map.Entry<String, String> entry : values.entrySet()) {
            writeString(out, entry.getKey());
            writeString(out, entry.getValue());
        }
    }

    private static Map<String, String> readStringMap(DataInputStream in, BudgetInput budget) throws IOException {
        int count = readCount(in, budget, budget.stringPrefixBytes() * 2);
        Map<String, String> values = new LinkedHashMap<>();
        for (int index = 0; index < count; index++) {
            putDistinct(values, readString(in, budget), readString(in, budget));
        }
        return values;
    }

    private static void writeSnapshot(PayloadDataOutput out, Map<String, TableSnapshot> snapshot)
            throws IOException {
        out.writeInt(snapshot.size());
        for (Map.Entry<String, TableSnapshot> entry : snapshot.entrySet()) {
            writeString(out, entry.getKey());
            TableSnapshot value = entry.getValue();
            out.writeLong(value.rowsDone());
            writeNullableLong(out, value.rowsTotal());
            writeNullableInt(out, value.donePct());
        }
    }

    private static Map<String, TableSnapshot> readSnapshot(DataInputStream in, BudgetInput budget) throws IOException {
        int count = readCount(in, budget, budget.stringPrefixBytes() + Long.BYTES + 2);
        Map<String, TableSnapshot> snapshot = new LinkedHashMap<>();
        for (int index = 0; index < count; index++) {
            String table = readString(in, budget);
            putDistinct(snapshot, table,
                    new TableSnapshot(in.readLong(), readNullableLong(in), readNullableInt(in)));
        }
        return snapshot;
    }

    private static void writeFailure(PayloadDataOutput out, ObservationFailure failure) throws IOException {
        out.writeBoolean(failure != null);
        if (failure != null) {
            writeString(out, failure.code());
            writeStringMap(out, failure.params());
        }
    }

    private static ObservationFailure readFailure(DataInputStream in, BudgetInput budget) throws IOException {
        return readBoolean(in)
                ? new ObservationFailure(readString(in, budget), readStringMap(in, budget)) : null;
    }

    private static void writeFacts(PayloadDataOutput out, List<MetricFact> facts) throws IOException {
        out.writeInt(facts.size());
        for (MetricFact fact : facts) {
            writeString(out, fact.name());
            writeString(out, fact.type().name());
            writeString(out, fact.unit());
            out.writeInt(fact.points().size());
            for (MetricPoint point : fact.points()) {
                writeStringMap(out, point.attributes());
                writeNullableInstant(out, point.startTime());
                out.writeLong(point.observedAt().toEpochMilli());
                out.writeBoolean(point.value() != null);
                if (point.value() != null) {
                    out.writeLong(point.value());
                } else {
                    writeHistogram(out, point.histogram());
                }
            }
        }
    }

    private static List<MetricFact> readFacts(DataInputStream in, BudgetInput budget) throws IOException {
        int factsCount = readCount(in, budget, budget.stringPrefixBytes() * 3 + Integer.BYTES);
        List<MetricFact> facts = new ArrayList<>();
        for (int factIndex = 0; factIndex < factsCount; factIndex++) {
            String name = readString(in, budget);
            MetricType type = MetricType.valueOf(readString(in, budget));
            String unit = readString(in, budget);
            int pointsCount = readCount(in, budget, 22);
            List<MetricPoint> points = new ArrayList<>();
            for (int pointIndex = 0; pointIndex < pointsCount; pointIndex++) {
                Map<String, String> attributes = readStringMap(in, budget);
                Instant startTime = readNullableInstant(in);
                Instant observedAt = Instant.ofEpochMilli(in.readLong());
                Long value = readBoolean(in) ? in.readLong() : null;
                HistogramValue histogram = value == null ? readHistogram(in, budget) : null;
                points.add(new MetricPoint(attributes, startTime, observedAt, value, histogram));
            }
            facts.add(new MetricFact(name, type, unit, points));
        }
        return facts;
    }

    private static void writeHistogram(PayloadDataOutput out, HistogramValue histogram) throws IOException {
        out.writeLong(histogram.count());
        out.writeDouble(histogram.sum());
        out.writeInt(histogram.bounds().size());
        for (double bound : histogram.bounds()) {
            out.writeDouble(bound);
        }
        out.writeInt(histogram.bucketCounts().size());
        for (long count : histogram.bucketCounts()) {
            out.writeLong(count);
        }
    }

    private static HistogramValue readHistogram(DataInputStream in, BudgetInput budget) throws IOException {
        long count = in.readLong();
        double sum = in.readDouble();
        int boundsCount = readCount(in, budget, Double.BYTES);
        List<Double> bounds = new ArrayList<>();
        for (int index = 0; index < boundsCount; index++) {
            bounds.add(in.readDouble());
        }
        int bucketsCount = readCount(in, budget, Long.BYTES);
        List<Long> buckets = new ArrayList<>();
        for (int index = 0; index < bucketsCount; index++) {
            buckets.add(in.readLong());
        }
        return new HistogramValue(count, sum, bounds, buckets);
    }

    private static void writeString(PayloadDataOutput out, String value) throws IOException {
        Objects.requireNonNull(value, "value");
        long length = utf8Length(value);
        out.writeLong(length);
        byte[] buffer = out.stringBytes;
        int buffered = 0;
        for (int offset = 0; offset < value.length();) {
            int codePoint = value.codePointAt(offset);
            offset += Character.charCount(codePoint);
            if (buffered > buffer.length - 4) {
                out.write(buffer, 0, buffered);
                buffered = 0;
            }
            if (codePoint <= 0x7f) {
                buffer[buffered++] = (byte) codePoint;
            } else if (codePoint <= 0x7ff) {
                buffer[buffered++] = (byte) (0xc0 | codePoint >>> 6);
                buffer[buffered++] = (byte) (0x80 | codePoint & 0x3f);
            } else if (codePoint <= 0xffff) {
                buffer[buffered++] = (byte) (0xe0 | codePoint >>> 12);
                buffer[buffered++] = (byte) (0x80 | codePoint >>> 6 & 0x3f);
                buffer[buffered++] = (byte) (0x80 | codePoint & 0x3f);
            } else {
                buffer[buffered++] = (byte) (0xf0 | codePoint >>> 18);
                buffer[buffered++] = (byte) (0x80 | codePoint >>> 12 & 0x3f);
                buffer[buffered++] = (byte) (0x80 | codePoint >>> 6 & 0x3f);
                buffer[buffered++] = (byte) (0x80 | codePoint & 0x3f);
            }
        }
        out.write(buffer, 0, buffered);
    }

    private static String readString(DataInputStream in, BudgetInput budget) throws IOException {
        long length = validateStringByteLength(budget.version == 1 ? in.readInt() : in.readLong(), budget.remaining());
        return budget.readString(in, length);
    }

    static long validateStringByteLength(long length, long remainingBytes) {
        if (length < 0 || length > remainingBytes) {
            throw new IllegalArgumentException("a string byte length fits the remaining payload");
        }
        return length;
    }

    private static long utf8Length(String value) {
        long bytes = 0;
        for (int offset = 0; offset < value.length();) {
            char first = value.charAt(offset);
            int codePoint;
            if (Character.isHighSurrogate(first)) {
                if (offset + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(offset + 1))) {
                    throw new IllegalArgumentException("a stored string contains an unpaired surrogate");
                }
                codePoint = Character.toCodePoint(first, value.charAt(offset + 1));
            } else if (Character.isLowSurrogate(first)) {
                throw new IllegalArgumentException("a stored string contains an unpaired surrogate");
            } else {
                codePoint = first;
            }
            offset += Character.charCount(codePoint);
            bytes = Math.addExact(bytes, codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2
                    : codePoint <= 0xffff ? 3 : 4);
        }
        return bytes;
    }

    private static void writeNullableInstant(PayloadDataOutput out, Instant value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            out.writeLong(value.toEpochMilli());
        }
    }

    private static Instant readNullableInstant(DataInputStream in) throws IOException {
        return readBoolean(in) ? Instant.ofEpochMilli(in.readLong()) : null;
    }

    private static void writeNullableLong(PayloadDataOutput out, Long value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            out.writeLong(value);
        }
    }

    private static Long readNullableLong(DataInputStream in) throws IOException {
        return readBoolean(in) ? in.readLong() : null;
    }

    private static void writeNullableInt(PayloadDataOutput out, Integer value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            out.writeInt(value);
        }
    }

    private static Integer readNullableInt(DataInputStream in) throws IOException {
        return readBoolean(in) ? in.readInt() : null;
    }

    private static boolean readBoolean(DataInputStream input) throws IOException {
        int value = input.readUnsignedByte();
        if (value > 1) {
            throw new IllegalArgumentException("a stored boolean is exactly zero or one");
        }
        return value == 1;
    }

    private static int readCount(DataInputStream in, BudgetInput budget, int minimumEntryBytes) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > budget.remaining() / minimumEntryBytes) {
            throw new IllegalArgumentException("a collection count fits the remaining payload");
        }
        return count;
    }

    private static <K, V> void putDistinct(Map<K, V> target, K key, V value) {
        if (target.putIfAbsent(key, value) != null) {
            throw new IllegalArgumentException("a stored map carries a duplicate key");
        }
    }

    private static byte[] payloadDigest(byte[] payload, int version) {
        MessageDigest digest = digest("payload", version);
        digest.update(payload);
        return digest.digest();
    }

    private static byte[] chunkDigest(byte[] payload) {
        return chunkDigest(payload, ENCODING_VERSION);
    }

    private static byte[] chunkDigest(byte[] payload, int version) {
        MessageDigest digest = digest("chunk", version);
        digest.update(payload);
        return digest.digest();
    }

    private static MessageDigest digest(String kind, int version) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(("tapstate/latest-" + kind + "/v" + version + "\0").getBytes(StandardCharsets.UTF_8));
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(version).array());
            return digest;
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("the JDK has no SHA-256 implementation", impossible);
        }
    }

    private static final class ThresholdBuffer extends ByteArrayOutputStream {
        private ThresholdBuffer() { super(4096); }
        private void drainTo(ChunkingOutput output) { output.writeChunkBytes(buf, 0, count); reset(); }
    }

    private static final class PayloadDataOutput extends DataOutputStream {
        private final byte[] stringBytes = new byte[8192];

        private PayloadDataOutput(OutputStream output) {
            super(output);
        }
    }

    private static final class BudgetInput extends InputStream {
        private final InputStream delegate;
        private final int version;
        private final ByteBuffer stringBytes = ByteBuffer.allocate(8192);
        private final CharBuffer stringChars = CharBuffer.allocate(4096);
        private final CharsetDecoder utf8 = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        private long remaining;

        private BudgetInput(InputStream delegate, long expectedBytes, int version) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
            this.version = version;
            if (expectedBytes < 0) {
                throw new IllegalArgumentException("an encoded payload length is non-negative");
            }
            remaining = expectedBytes;
        }

        private long remaining() {
            return remaining;
        }

        private int stringPrefixBytes() {
            return version == 1 ? Integer.BYTES : Long.BYTES;
        }

        private String readString(DataInputStream input, long length) throws IOException {
            utf8.reset();
            stringBytes.clear().limit(0);
            StringBuilder value = new StringBuilder();
            long unread = length;
            do {
                stringBytes.compact();
                int read = (int) Math.min(unread, stringBytes.remaining());
                input.readFully(stringBytes.array(), stringBytes.position(), read);
                stringBytes.position(stringBytes.position() + read).flip();
                unread -= read;
                CoderResult result;
                do {
                    stringChars.clear();
                    result = utf8.decode(stringBytes, stringChars, unread == 0);
                    stringChars.flip();
                    value.append(stringChars);
                    if (result.isError()) {
                        result.throwException();
                    }
                } while (result.isOverflow());
            } while (unread > 0);
            CoderResult result;
            do {
                stringChars.clear();
                result = utf8.flush(stringChars);
                stringChars.flip();
                value.append(stringChars);
                if (result.isError()) {
                    result.throwException();
                }
            } while (result.isOverflow());
            return value.toString();
        }

        @Override
        public int read() throws IOException {
            if (remaining == 0) {
                return -1;
            }
            int value = delegate.read();
            if (value < 0) {
                return -1;
            }
            remaining--;
            return value;
        }

        @Override
        public int read(byte[] target, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, target.length);
            if (length == 0) {
                return 0;
            }
            if (remaining == 0) {
                return -1;
            }
            int read = delegate.read(target, offset, (int) Math.min(length, remaining));
            if (read > 0) {
                remaining -= read;
            }
            return read;
        }
    }

    private static final class ChunkingOutput extends OutputStream {
        private final ChunkWriter target;
        private final MessageDigest payloadDigest = digest("payload", ENCODING_VERSION);
        private final ThresholdBuffer inline = new ThresholdBuffer();
        private byte[] chunk;
        private int chunkBytes;
        private long totalBytes;
        private long chunks;
        private boolean chunked;
        private boolean finished;

        private ChunkingOutput(ChunkWriter target) { this.target = target; }

        @Override public void write(int value) {
            if (finished) throw new IllegalStateException("payload encoding already finished");
            totalBytes = Math.incrementExact(totalBytes);
            payloadDigest.update((byte) value);
            if (!chunked && inline.size() < INLINE_PAYLOAD_LIMIT) {
                inline.write(value);
                return;
            }
            beginChunks();
            writeChunkByte((byte) value);
        }

        @Override public void write(byte[] bytes, int offset, int length) {
            Objects.checkFromIndexSize(offset, length, bytes.length);
            if (finished) throw new IllegalStateException("payload encoding already finished");
            totalBytes = Math.addExact(totalBytes, length);
            payloadDigest.update(bytes, offset, length);
            if (!chunked && inline.size() + (long) length <= INLINE_PAYLOAD_LIMIT) {
                inline.write(bytes, offset, length);
                return;
            }
            beginChunks();
            writeChunkBytes(bytes, offset, length);
        }

        private void beginChunks() {
            if (!chunked) {
                target.begin();
                chunked = true;
                chunk = new byte[CHUNK_PAYLOAD_LIMIT];
                inline.drainTo(this);
            }
        }

        private void writeChunkByte(byte value) {
            chunk[chunkBytes++] = value;
            if (chunkBytes == chunk.length) flushChunk();
        }

        private void writeChunkBytes(byte[] bytes, int offset, int length) {
            int cursor = offset;
            int left = length;
            while (left > 0) {
                int copied = Math.min(left, chunk.length - chunkBytes);
                System.arraycopy(bytes, cursor, chunk, chunkBytes, copied);
                chunkBytes += copied;
                cursor += copied;
                left -= copied;
                if (chunkBytes == chunk.length) flushChunk();
            }
        }

        private void flushChunk() {
            byte[] payload = Arrays.copyOf(chunk, chunkBytes);
            target.write(new Chunk(chunks, payload, chunkDigest(payload)));
            chunks = Math.incrementExact(chunks);
            chunkBytes = 0;
        }

        private Encoded finish() {
            if (finished) throw new IllegalStateException("payload encoding already finished");
            finished = true;
            if (chunked && chunkBytes > 0) flushChunk();
            byte[] completeDigest = payloadDigest.digest();
            return chunked ? new Encoded(null, completeDigest, chunks, totalBytes)
                    : new Encoded(inline.toByteArray(), completeDigest, 0, totalBytes);
        }
    }

    private static final class ChunkInput extends InputStream {
        private final Iterator<Chunk> chunks;
        private final long expectedCount;
        private final long expectedBytes;
        private final byte[] expectedDigest;
        private final MessageDigest payloadDigest;
        private final int version;
        private ByteArrayInputStream current;
        private long count;
        private long bytes;
        private boolean exhausted;

        private ChunkInput(Iterator<Chunk> chunks, long expectedCount, long expectedBytes,
                byte[] expectedDigest, int version) {
            this.chunks = Objects.requireNonNull(chunks, "chunks");
            this.expectedCount = expectedCount;
            this.expectedBytes = expectedBytes;
            this.version = version;
            this.payloadDigest = digest("payload", version);
            this.expectedDigest = Arrays.copyOf(Objects.requireNonNull(expectedDigest, "expectedDigest"),
                    expectedDigest.length);
        }

        @Override public int read() {
            while (current == null || current.available() == 0) {
                if (!advance()) return -1;
            }
            return current.read();
        }

        @Override public int read(byte[] target, int offset, int length) {
            Objects.checkFromIndexSize(offset, length, target.length);
            if (length == 0) return 0;
            while (current == null || current.available() == 0) {
                if (!advance()) return -1;
            }
            return current.read(target, offset, length);
        }

        private boolean advance() {
            if (!chunks.hasNext()) {
                exhausted = true;
                return false;
            }
            Chunk chunk = chunks.next();
            byte[] payload = chunk.payload();
            if (chunk.ordinal() != count || !MessageDigest.isEqual(chunk.digest(), chunkDigest(payload, version))) {
                throw new IllegalArgumentException("chunk order or digest does not match its descriptor");
            }
            if (count >= expectedCount || payload.length > expectedBytes - bytes) {
                throw new IllegalArgumentException("chunks exceed their descriptor count or byte budget");
            }
            count = Math.incrementExact(count);
            bytes = Math.addExact(bytes, payload.length);
            payloadDigest.update(payload);
            current = new ByteArrayInputStream(payload);
            return true;
        }

        private void verifyComplete() {
            while (read() != -1) {
                // Drain any bytes the record reader did not consume.
            }
            if (!exhausted || count != expectedCount || bytes != expectedBytes
                    || !MessageDigest.isEqual(payloadDigest.digest(), expectedDigest)) {
                throw new IllegalArgumentException("chunk count, bytes, or payload digest does not match");
            }
        }
    }
}
