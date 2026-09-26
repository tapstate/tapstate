package io.tapstate.adapters.mongostore;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.PipelineState;
import org.bson.Document;
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

    @Test
    void aSharedConnectorOffsetRepeatedAcrossSelectedTablesStaysWithinTheLatestBudget() throws IOException {
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
        Document stored = MongoObservationStore.toDocument(observation)
                .append("pipelineIncarnationId", "01234567-89ab-cdef-0123-456789abcdef")
                .append("executionGeneration", 1L);

        long encoded = StoredBytes.bsonSize(stored);
        assertThat(encoded).isLessThan(StoredBytes.DOCUMENT_CEILING);
        assertThat(encoded).as("BSON bytes with a %s-byte connector token copied across 13 tables",
                token.length()).isLessThanOrEqualTo(LATEST_BUDGET_BYTES);
    }

    @Test
    void aLegalPipelineIdAndFullyBudgetedRecordFactStayWithinTheLatestBudget() {
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
        Document stored = MongoObservationStore.toDocument(observation)
                .append("pipelineIncarnationId", "01234567-89ab-cdef-0123-456789abcdef")
                .append("executionGeneration", 1L);

        long encoded = StoredBytes.bsonSize(stored);
        assertThat(encoded).isLessThan(StoredBytes.DOCUMENT_CEILING);
        assertThat(encoded).as("BSON bytes with %s fully budgeted record points and a %s-byte pipeline id",
                budgeted.points().size(), pipelineId.length()).isLessThanOrEqualTo(LATEST_BUDGET_BYTES);
    }

    private static String serializedToken(byte[] offset) throws IOException {
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        try (ObjectOutputStream stream = new ObjectOutputStream(encoded)) {
            stream.writeObject(offset);
        }
        return Base64.getEncoder().encodeToString(encoded.toByteArray());
    }
}
