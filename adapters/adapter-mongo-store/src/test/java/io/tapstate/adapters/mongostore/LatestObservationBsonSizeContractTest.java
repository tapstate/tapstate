package io.tapstate.adapters.mongostore;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ObservationStore;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class LatestObservationBsonSizeContractTest {

    private static final long LATEST_BUDGET_BYTES = 12L * 1024 * 1024;
    private static final ObservationStore.Scope SCOPE =
            new ObservationStore.Scope("01234567-89ab-cdef-0123-456789abcdef", 1L);

    @Test
    void inlineBoundaryStillFitsBesideTheMaximumPendingDescriptor() {
        Observation observation = new Observation("pipeline", PipelineState.RUNNING,
                Map.of(), Map.of(), Map.of("orders", "x".repeat(400 * 1024)), null,
                Instant.parse("2026-09-27T00:00:00Z"));
        LatestObservationPayloadCodec.Encoded encoded = LatestObservationPayloadCodec.encode(
                observation, new LatestObservationPayloadCodec.ChunkWriter() {
                    @Override public void begin() { throw new AssertionError("payload should stay inline"); }
                    @Override public void write(LatestObservationPayloadCodec.Chunk chunk) {
                        throw new AssertionError("payload should stay inline");
                    }
                });

        assertThat(encoded.inline()).isTrue();
        assertThat(StoredBytes.bsonSize(MongoLatestObservationStorage.inlineManifestForSize(
                observation.pipelineId(), SCOPE, observation.observedAt(), encoded)))
                .isLessThanOrEqualTo(LATEST_BUDGET_BYTES);
    }

    @Test
    void aSharedConnectorOffsetRepeatedAcrossSelectedTablesUsesBoundedChunks() throws IOException {
        // Connector offsets are serialized to opaque Base64 tokens. One source can select many tables,
        // and the current-position projection repeats its sink-acknowledged token for every selected table.
        byte[] offset = new byte[768 * 1024];
        new Random(0L).nextBytes(offset);
        String token = serializedToken(offset);
        Map<String, String> positions = new LinkedHashMap<>();
        for (int table = 0; table < 13; table++) {
            positions.put("table_" + table, token);
        }
        Observation observation = new Observation("pipeline", PipelineState.RUNNING,
                Map.of(), Map.of(), positions, null, Instant.parse("2026-09-27T00:00:00Z"));
        assertChunkedAndBounded(observation, 13_632_353L,
                "BSON bytes with a %s-byte connector token copied across 13 tables".formatted(token.length()));
    }

    @Test
    void aLegalPipelineIdAndFullyBudgetedRecordFactUsesBoundedChunks() {
        String pipelineId = "p".repeat(950);
        Instant at = Instant.parse("2026-09-27T00:00:00Z");
        List<MetricPoint> points = new ArrayList<>();
        for (int table = 0; table < 1_000; table++) {
            for (String direction : MetricAttributes.DIRECTIONS) {
                for (String operation : MetricAttributes.OPS) {
                    points.add(MetricPoint.accumulated(Map.of(
                            MetricAttributes.PIPELINE_ID, pipelineId,
                            MetricAttributes.TABLE_ID, "table_" + table,
                            MetricAttributes.DIRECTION, direction,
                            MetricAttributes.OP, operation), at, at, 1L));
                }
            }
        }
        MetricFact full = new MetricFact("tapstate.pipeline.records", MetricType.COUNTER,
                "{record}", points);
        MetricFact budgeted = CardinalityBudget.folder().fold(full);
        assertThat(budgeted.points()).hasSize(12_000);
        Observation observation = new Observation(pipelineId, PipelineState.RUNNING,
                Map.of("tapstate.pipeline.records", 12_000L), Map.of(), Map.of(), null, at,
                List.of(budgeted));
        assertChunkedAndBounded(observation, 13_494_853L,
                "BSON bytes with %s fully budgeted record points and a %s-byte pipeline id"
                        .formatted(budgeted.points().size(), pipelineId.length()));
    }

    private static void assertChunkedAndBounded(Observation observation, long oldBytes, String description) {
        Document legacy = MongoObservationStore.toDocument(observation)
                .append("pipelineIncarnationId", SCOPE.pipelineIncarnationId())
                .append("executionGeneration", SCOPE.executionGeneration());
        assertThat(StoredBytes.bsonSize(legacy)).as(description).isEqualTo(oldBytes)
                .isLessThan(StoredBytes.DOCUMENT_CEILING)
                .isGreaterThan(LATEST_BUDGET_BYTES);

        List<LatestObservationPayloadCodec.Chunk> chunks = new ArrayList<>();
        LatestObservationPayloadCodec.Encoded encoded = LatestObservationPayloadCodec.encode(
                observation, new LatestObservationPayloadCodec.ChunkWriter() {
                    @Override public void begin() { }
                    @Override public void write(LatestObservationPayloadCodec.Chunk chunk) { chunks.add(chunk); }
                });
        assertThat(encoded.inline()).isFalse();
        assertThat(encoded.chunkCount()).isEqualTo(chunks.size());
        String token = "00000000-0000-0000-0000-000000000000";
        Document manifest = MongoLatestObservationStorage.chunkedManifestForSize(
                observation.pipelineId(), SCOPE, observation.observedAt(), token, encoded);
        assertThat(StoredBytes.bsonSize(manifest)).as("manifest with maximum pending descriptor")
                .isLessThanOrEqualTo(LATEST_BUDGET_BYTES);
        Binary key = MongoLatestObservationStorage.manifestKey(observation.pipelineId());
        Binary owner = MongoLatestObservationStorage.ownerDigest(observation.pipelineId());
        assertThat(chunks).allSatisfy(chunk -> assertThat(StoredBytes.bsonSize(
                MongoLatestObservationStorage.chunkDocument(key, owner, token, chunk)))
                .as("chunk %s", chunk.ordinal()).isLessThanOrEqualTo(LATEST_BUDGET_BYTES));
        Observation decoded = LatestObservationPayloadCodec.decodeChunks(chunks, encoded.chunkCount(),
                encoded.encodedBytes(), encoded.payloadDigest());
        assertThat(decoded).isEqualTo(observation);
    }

    private static String serializedToken(byte[] offset) throws IOException {
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        try (ObjectOutputStream stream = new ObjectOutputStream(encoded)) {
            stream.writeObject(offset);
        }
        return Base64.getEncoder().encodeToString(encoded.toByteArray());
    }
}
