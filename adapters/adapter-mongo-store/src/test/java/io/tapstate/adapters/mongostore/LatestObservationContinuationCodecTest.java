package io.tapstate.adapters.mongostore;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.spi.store.ObservationContinuation;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StopReservation;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LatestObservationContinuationCodecTest {
    private static final Instant PUBLIC_START = Instant.parse("2026-10-01T10:00:00.123456789Z");
    private static final Instant NATIVE_START = PUBLIC_START.plusSeconds(60).plusNanos(321);
    private static final Instant AT = NATIVE_START.plusSeconds(1);
    private static final ObservationStore.Scope SOURCE = new ObservationStore.Scope("inc-a", 41);
    private static final ObservationStore.Scope TARGET = new ObservationStore.Scope("inc-a", 42);
    private static final StopReservation.JobIdentity JOB = new StopReservation.JobIdentity("cluster-a", 77, "boot-a");

    @Test
    void privateNativeEpochAndOriginalStartsRetainTheirExactNanoseconds() {
        var value = known("table-a");
        RecordingChunks chunks = new RecordingChunks();
        var encoded = LatestObservationPayloadCodec.encodeContinuation("orders", value, chunks);
        var decoded = LatestObservationPayloadCodec.decodeContinuationInline(encoded.inlinePayload(),
                encoded.payloadDigest(), encoded.encodedBytes());
        assertThat(encoded.inline()).isTrue();
        assertThat(chunks.begun).isFalse();
        assertThat(decoded.pipelineId()).isEqualTo("orders");
        assertThat(decoded.baselineFacts()).isEqualTo(value.baselineFacts());
        assertThat(decoded.producerStates()).isEqualTo(value.producerStates());
        assertThat(decoded.producerStates().getFirst().nativeStart()).isEqualTo(NATIVE_START);
        assertThat(decoded.producerStates().getFirst().published().getFirst().startTime()).isEqualTo(PUBLIC_START);
    }

    @Test
    void aLargePrivateFactStreamsBoundedChunksWithoutAnInlineCopy() {
        var value = known("x".repeat(2 * LatestObservationPayloadCodec.CHUNK_PAYLOAD_LIMIT));
        RecordingChunks chunks = new RecordingChunks();
        var encoded = LatestObservationPayloadCodec.encodeContinuation("orders", value, chunks);
        assertThat(encoded.inline()).isFalse();
        assertThat(encoded.inlinePayload()).isNull();
        assertThat(chunks.begun).isTrue();
        assertThat(chunks.rows).hasSizeGreaterThan(2).allSatisfy(chunk ->
                assertThat(chunk.payload().length).isBetween(1, LatestObservationPayloadCodec.CHUNK_PAYLOAD_LIMIT));
        var decoded = LatestObservationPayloadCodec.decodeContinuationChunks(chunks.rows, encoded.chunkCount(),
                encoded.encodedBytes(), encoded.payloadDigest());
        assertThat(decoded.baselineFacts()).isEqualTo(value.baselineFacts());
        assertThat(decoded.producerStates()).isEqualTo(value.producerStates());
    }

    @Test
    void anEmptyPrivateCarrierIsUnknownRatherThanAZeroCounter() {
        var unknown = new ObservationContinuation("handoff", SOURCE,
                Optional.of(new ObservationContinuation.Target(TARGET, Optional.of(JOB))),
                Optional.empty(), List.of(), List.of());
        var encoded = LatestObservationPayloadCodec.encodeContinuation("orders", unknown, new RecordingChunks());
        var decoded = LatestObservationPayloadCodec.decodeContinuationInline(encoded.inlinePayload(),
                encoded.payloadDigest(), encoded.encodedBytes());
        assertThat(unknown.knownBaseline()).isFalse();
        assertThat(decoded.baselineFacts()).isEmpty();
        assertThat(decoded.producerStates()).isEmpty();
    }

    @Test
    void privateChunkCorruptionAndPublicPayloadSubstitutionAreRejected() {
        RecordingChunks chunks = new RecordingChunks();
        var encoded = LatestObservationPayloadCodec.encodeContinuation("orders", known("large".repeat(200_000)), chunks);
        var broken = new ArrayList<>(chunks.rows);
        var first = broken.getFirst();
        byte[] payload = first.payload();
        payload[payload.length / 2] ^= 1;
        broken.set(0, new LatestObservationPayloadCodec.Chunk(first.ordinal(), payload, first.digest()));
        assertThatThrownBy(() -> LatestObservationPayloadCodec.decodeContinuationChunks(broken, encoded.chunkCount(),
                encoded.encodedBytes(), encoded.payloadDigest())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LatestObservationPayloadCodec.decodeChunks(chunks.rows, encoded.chunkCount(),
                encoded.encodedBytes(), encoded.payloadDigest())).isInstanceOf(IllegalArgumentException.class);
    }

    private static ObservationContinuation known(String table) {
        MetricPoint baseline = point(table, 7);
        MetricFact fact = MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER, "{record}", baseline);
        var state = new ObservationContinuation.ProducerState(fact.name(), fact.type(), fact.unit(), "out", "",
                NATIVE_START, List.of(), List.of(point(table, 9)));
        return new ObservationContinuation("handoff", SOURCE,
                Optional.of(new ObservationContinuation.Target(TARGET, Optional.of(JOB))),
                Optional.empty(), List.of(fact), List.of(state));
    }

    private static MetricPoint point(String table, long value) {
        return MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, "orders",
                MetricAttributes.TABLE_ID, table, MetricAttributes.DIRECTION, "out"), PUBLIC_START, AT, value);
    }

    private static final class RecordingChunks implements LatestObservationPayloadCodec.ChunkWriter {
        private boolean begun;
        private final List<LatestObservationPayloadCodec.Chunk> rows = new ArrayList<>();
        @Override public void begin() { begun = true; }
        @Override public void write(LatestObservationPayloadCodec.Chunk chunk) { rows.add(chunk); }
    }
}
