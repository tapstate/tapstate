package io.tapstate.adapters.mongostore;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.time.Instant;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.lang.management.ManagementFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LatestObservationPayloadCodecTest {

    @Test
    void aSmallPayloadIsEncodedOnceInlineAndRoundTrips() {
        Observation source = observation("small");
        RecordingChunks chunks = new RecordingChunks();

        LatestObservationPayloadCodec.Encoded encoded = LatestObservationPayloadCodec.encode(source, chunks);

        assertThat(encoded.inline()).isTrue();
        assertThat(encoded.chunkCount()).isZero();
        assertThat(encoded.encodedBytes()).isEqualTo(encoded.inlinePayload().length);
        assertThat(chunks.begun).isFalse();
        assertThat(chunks.chunks).isEmpty();
        assertThat(LatestObservationPayloadCodec.decodeInline(encoded.inlinePayload(),
                encoded.payloadDigest(), encoded.encodedBytes())).isEqualTo(source);
    }

    @Test
    void aRoutineSmallObservationDoesNotAllocateTheEntireInlineCeiling() {
        Observation source = observation("small");
        var threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        assertThat(threads.isThreadAllocatedMemorySupported()).isTrue();
        threads.setThreadAllocatedMemoryEnabled(true);
        RecordingChunks target = new RecordingChunks();
        for (int warmup = 0; warmup < 20; warmup++) {
            LatestObservationPayloadCodec.encode(source, target);
        }
        long before = threads.getCurrentThreadAllocatedBytes();
        for (int sample = 0; sample < 32; sample++) {
            LatestObservationPayloadCodec.encode(source, target);
        }
        long allocated = threads.getCurrentThreadAllocatedBytes() - before;

        assertThat(allocated / 32).as("allocated bytes per routine inline encode")
                .isLessThan(64 * 1024L);
        assertThat(target.chunks).isEmpty();
    }

    @Test
    void aLargePayloadStreamsFixedChunksAndRoundTripsWithoutAnInlineCopy() {
        Observation source = observation("x".repeat(3 * 1024 * 1024));
        RecordingChunks chunks = new RecordingChunks();

        LatestObservationPayloadCodec.Encoded encoded = LatestObservationPayloadCodec.encode(source, chunks);

        assertThat(encoded.inline()).isFalse();
        assertThat(encoded.inlinePayload()).isNull();
        assertThat(chunks.begun).isTrue();
        assertThat(chunks.chunks).hasSizeGreaterThan(2)
                .allSatisfy(chunk -> assertThat(chunk.payload().length)
                        .isBetween(1, LatestObservationPayloadCodec.CHUNK_PAYLOAD_LIMIT));
        assertThat(chunks.chunks).extracting(LatestObservationPayloadCodec.Chunk::ordinal)
                .containsExactlyElementsOf(java.util.stream.LongStream.range(0, encoded.chunkCount())
                        .boxed().toList());
        assertThat(chunks.chunks.stream().mapToLong(chunk -> chunk.payload().length).sum())
                .isEqualTo(encoded.encodedBytes());
        assertThat(LatestObservationPayloadCodec.decodeChunks(chunks.chunks, encoded.chunkCount(),
                encoded.encodedBytes(), encoded.payloadDigest())).isEqualTo(source);
    }

    @Test
    void theVersionedEncodingIsDeterministic() {
        Observation source = observation("stable");
        RecordingChunks firstChunks = new RecordingChunks();
        RecordingChunks secondChunks = new RecordingChunks();

        LatestObservationPayloadCodec.Encoded first = LatestObservationPayloadCodec.encode(source, firstChunks);
        LatestObservationPayloadCodec.Encoded second = LatestObservationPayloadCodec.encode(source, secondChunks);

        assertThat(second.inlinePayload()).containsExactly(first.inlinePayload());
        assertThat(second.payloadDigest()).containsExactly(first.payloadDigest());
        assertThat(second.encodedBytes()).isEqualTo(first.encodedBytes());
    }

    @Test
    void mapIterationOrderDoesNotChangeTheVersionedBytes() {
        Map<String, String> ascending = new java.util.LinkedHashMap<>();
        ascending.put("a", "first");
        ascending.put("z", "last");
        Map<String, String> descending = new java.util.LinkedHashMap<>();
        descending.put("z", "last");
        descending.put("a", "first");
        Observation first = new Observation("orders", PipelineState.RUNNING,
                Map.of(), Map.of(), ascending, null, Instant.parse("2026-09-28T04:00:00Z"));
        Observation second = new Observation("orders", PipelineState.RUNNING,
                Map.of(), Map.of(), descending, null, Instant.parse("2026-09-28T04:00:00Z"));

        LatestObservationPayloadCodec.Encoded firstEncoded = LatestObservationPayloadCodec.encode(
                first, new RecordingChunks());
        LatestObservationPayloadCodec.Encoded secondEncoded = LatestObservationPayloadCodec.encode(
                second, new RecordingChunks());

        assertThat(secondEncoded.inlinePayload()).containsExactly(firstEncoded.inlinePayload());
        assertThat(secondEncoded.payloadDigest()).containsExactly(firstEncoded.payloadDigest());
    }

    @Test
    void corruptStringLengthAndUtf8AreRejectedWithoutTrustingTheirAllocationRequest() {
        LatestObservationPayloadCodec.Encoded encoded = LatestObservationPayloadCodec.encode(
                observation("small"), new RecordingChunks());
        byte[] oversized = encoded.inlinePayload();
        ByteBuffer.wrap(oversized).putLong(8, Long.MAX_VALUE);
        assertThatThrownBy(() -> LatestObservationPayloadCodec.decodeInline(
                oversized, payloadDigest(oversized), oversized.length))
                .isInstanceOf(IllegalArgumentException.class);

        byte[] malformed = encoded.inlinePayload();
        malformed[Integer.BYTES * 2 + Long.BYTES] = (byte) 0x80;
        assertThatThrownBy(() -> LatestObservationPayloadCodec.decodeInline(
                malformed, payloadDigest(malformed), malformed.length))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multibyteStringsCrossScratchAndChunkBoundariesWithoutChangingTheirContent() {
        Observation source = observation("\u20ac\ud83d\ude00".repeat(200_000));
        RecordingChunks chunks = new RecordingChunks();

        LatestObservationPayloadCodec.Encoded encoded = LatestObservationPayloadCodec.encode(source, chunks);

        assertThat(encoded.inline()).isFalse();
        assertThat(LatestObservationPayloadCodec.decodeChunks(chunks.chunks, encoded.chunkCount(),
                encoded.encodedBytes(), encoded.payloadDigest())).isEqualTo(source);
    }

    @Test
    void aTinyPayloadCannotRequestAnUnboundedCollection() {
        LatestObservationPayloadCodec.Encoded encoded = LatestObservationPayloadCodec.encode(
                observation("small"), new RecordingChunks());
        byte[] malformed = encoded.inlinePayload();
        int metricsOffset = Integer.BYTES * 2
                + Long.BYTES + "orders".length()
                + Long.BYTES + PipelineState.RUNNING.name().length()
                + 1 + Long.BYTES;
        ByteBuffer.wrap(malformed).putInt(metricsOffset, Integer.MAX_VALUE);

        assertThatThrownBy(() -> LatestObservationPayloadCodec.decodeInline(
                malformed, payloadDigest(malformed), malformed.length))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void missingReorderedOrChangedChunksAreRejected() {
        Observation source = observation("x".repeat(3 * 1024 * 1024));
        RecordingChunks chunks = new RecordingChunks();
        LatestObservationPayloadCodec.Encoded encoded = LatestObservationPayloadCodec.encode(source, chunks);

        assertThatThrownBy(() -> LatestObservationPayloadCodec.decodeChunks(
                chunks.chunks.subList(0, chunks.chunks.size() - 1), encoded.chunkCount(),
                encoded.encodedBytes(), encoded.payloadDigest()))
                .isInstanceOf(IllegalArgumentException.class);

        List<LatestObservationPayloadCodec.Chunk> reordered = new ArrayList<>(chunks.chunks);
        java.util.Collections.swap(reordered, 0, 1);
        assertThatThrownBy(() -> LatestObservationPayloadCodec.decodeChunks(reordered,
                encoded.chunkCount(), encoded.encodedBytes(), encoded.payloadDigest()))
                .isInstanceOf(IllegalArgumentException.class);

        List<LatestObservationPayloadCodec.Chunk> changed = new ArrayList<>(chunks.chunks);
        LatestObservationPayloadCodec.Chunk original = changed.get(0);
        byte[] changedBytes = original.payload();
        changedBytes[0] ^= 1;
        changed.set(0, new LatestObservationPayloadCodec.Chunk(original.ordinal(), changedBytes,
                original.digest()));
        assertThatThrownBy(() -> LatestObservationPayloadCodec.decodeChunks(changed,
                encoded.chunkCount(), encoded.encodedBytes(), encoded.payloadDigest()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aWrongInlineDigestOrLengthIsRejected() {
        Observation source = observation("small");
        LatestObservationPayloadCodec.Encoded encoded = LatestObservationPayloadCodec.encode(
                source, new RecordingChunks());
        byte[] wrong = Arrays.copyOf(encoded.payloadDigest(), encoded.payloadDigest().length);
        wrong[0] ^= 1;

        assertThatThrownBy(() -> LatestObservationPayloadCodec.decodeInline(encoded.inlinePayload(),
                wrong, encoded.encodedBytes())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LatestObservationPayloadCodec.decodeInline(encoded.inlinePayload(),
                encoded.payloadDigest(), encoded.encodedBytes() + 1)).isInstanceOf(IllegalArgumentException.class);
    }

    private static Observation observation(String position) {
        return new Observation("orders", PipelineState.RUNNING,
                Map.of("records.out", 42L, "bytes.out", 840L), Map.of(),
                Map.of("orders", position), null, Instant.parse("2026-09-28T04:00:00Z"));
    }

    private static byte[] payloadDigest(byte[] payload) {
        return payloadDigest(payload, LatestObservationPayloadCodec.ENCODING_VERSION);
    }

    private static byte[] payloadDigest(byte[] payload, int version) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(("tapstate/latest-payload/v" + version + "\0").getBytes(StandardCharsets.UTF_8));
            digest.update(ByteBuffer.allocate(Integer.BYTES)
                    .putInt(version).array());
            return digest.digest(payload);
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    @Test
    void longStringPrefixesAndCanonicalBooleansDoNotBreakThePriorEncoding() {
        long length = (long) Integer.MAX_VALUE + 1;
        assertThat(LatestObservationPayloadCodec.validateStringByteLength(length, length)).isEqualTo(length);
        assertThatThrownBy(() -> LatestObservationPayloadCodec.validateStringByteLength(length, length - 1))
                .isInstanceOf(IllegalArgumentException.class);
        byte[] previous = java.util.HexFormat.of().parseHex(
                "54534c4f00000001000000066f72646572730000000752554e4e494e47000000000000000000000000000000000000");
        Observation expected = new Observation("orders", PipelineState.RUNNING, Map.of(), Map.of());
        assertThat(LatestObservationPayloadCodec.decodeInline(previous, payloadDigest(previous, 1),
                previous.length, 1)).isEqualTo(expected);
        var chunk = new LatestObservationPayloadCodec.Chunk(0, previous, chunkDigest(previous, 1));
        assertThat(LatestObservationPayloadCodec.decodeChunks(List.of(chunk), 1, previous.length,
                payloadDigest(previous, 1), 1)).isEqualTo(expected);

        var encoded = LatestObservationPayloadCodec.encode(observation("small"), new RecordingChunks());
        byte[] malformed = encoded.inlinePayload();
        int timePresence = Integer.BYTES * 2 + Long.BYTES + "orders".length()
                + Long.BYTES + PipelineState.RUNNING.name().length();
        malformed[timePresence] = 2;
        assertThatThrownBy(() -> LatestObservationPayloadCodec.decodeInline(
                malformed, payloadDigest(malformed), malformed.length))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static byte[] chunkDigest(byte[] payload, int version) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(("tapstate/latest-chunk/v" + version + "\0").getBytes(StandardCharsets.UTF_8));
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(version).array());
            return digest.digest(payload);
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static final class RecordingChunks implements LatestObservationPayloadCodec.ChunkWriter {
        private boolean begun;
        private final List<LatestObservationPayloadCodec.Chunk> chunks = new ArrayList<>();

        @Override
        public void begin() {
            assertThat(begun).isFalse();
            begun = true;
        }

        @Override
        public void write(LatestObservationPayloadCodec.Chunk chunk) {
            assertThat(begun).isTrue();
            chunks.add(chunk);
        }
    }
}
