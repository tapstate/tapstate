package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.SrsConsumerId;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkConsumerFrontierDiagnosticsTest {
    private static final String CHAIN = "chain";
    private static final String PIPELINE = "pipeline";
    private static final String SOURCE = "source";
    private static final String TABLE = "orders";

    @Test
    void aRealZeroAckIsRetainedAsAConfirmedValueRatherThanMissingProgress() {
        Document cursor = cursor(List.of("writer"));
        writer(cursor, "writer").put("sinkAckedSeq", 0L);
        writer(cursor, "writer").put("ringDone", 0L);
        table(cursor).put("sinkAckedSeq", 0L);
        cursor.get("perTableRingDone", Document.class).put(TABLE, 0L);
        var result = read(cursor);

        assertThat(result).containsEntry("state", "RECORDED").containsEntry("minimumWriterAckSeq", 0L)
                .containsEntry("readToConfirmedAckSequenceDistance", 75L);
        assertThat(frontier(result, "writer").get("sinkAckedSeq")).isEqualTo(0L);
    }

    @Test
    void everyExpectedWriterConstrainsTheConfirmedMinimum() {
        Document cursor = cursor(List.of("fast", "slow"));
        writer(cursor, "fast").put("sinkAckedSeq", 70L);
        writer(cursor, "fast").put("ringDone", 70L);
        writer(cursor, "slow").put("sinkAckedSeq", 20L);
        writer(cursor, "slow").put("ringDone", 20L);
        table(cursor).put("sinkAckedSeq", 20L);
        cursor.get("perTableRingDone", Document.class).put(TABLE, 20L);
        var result = read(cursor);

        assertThat(result).containsEntry("state", "RECORDED").containsEntry("minimumWriterAckSeq", 20L)
                .containsEntry("minimumWriterRingDone", 20L)
                .containsEntry("readToConfirmedAckSequenceDistance", 55L);
        assertThat(frontier(result, "fast").get("sinkAckedSeq")).isEqualTo(70L);
        assertThat(frontier(result, "slow").get("sinkAckedSeq")).isEqualTo(20L);
    }

    @Test
    void anArrivalRingDoneWithoutActualAckIsUnknown() {
        Document cursor = cursor(List.of("writer"));
        writer(cursor, "writer").remove("sinkAckedEpoch");
        writer(cursor, "writer").remove("sinkAckedSeq");
        writer(cursor, "writer").put("ringDone", 75L);
        var result = read(cursor);

        assertThat(result).containsEntry("state", "UNKNOWN").containsEntry("allWritersConfirmed", false)
                .doesNotContainKeys("minimumWriterAckSeq", "readToConfirmedAckSequenceDistance");
        assertThat(frontier(result, "writer").get("ringDone")).isEqualTo(75L);
        assertThat(frontier(result, "writer").containsKey("sinkAckedSeq")).isFalse();
    }

    @Test
    void aMissingExpectedWriterCannotBeReplacedByAnUnregisteredFastWriter() {
        Document cursor = cursor(List.of("first", "missing"));
        var progress = cursor.get("sinkWriterProgress", Document.class);
        progress.put("unexpected", progress.remove("missing"));
        var result = read(cursor);

        assertThat(result).containsEntry("state", "UNKNOWN").containsEntry("allWritersConfirmed", false)
                .doesNotContainKeys("minimumWriterAckSeq", "minimumWriterRingDone", "readToConfirmedAckSequenceDistance");
        assertThat(((Map<?, ?>) result.get("writerFrontiers")).containsKey("unexpected")).isFalse();
    }

    @Test
    void changedSourceWriterPlanOrFenceUsesTheExistingBareBindingFailure() {
        for (String changed : List.of("identity", "plan", "fence")) {
            Document cursor = cursor(List.of("writer"));
            var binding = binding(cursor);
            switch (changed) {
                case "identity" -> cursor.put("sourceNodeId", "another-source");
                case "plan" -> cursor.put("expectedSinkWriters", new Document(TABLE, List.of("another-writer")));
                case "fence" -> cursor.put("sinkAckFence", new Document("generation", 2L));
                default -> throw new AssertionError("unregistered test mutation");
            }
            assertThatThrownBy(() -> BenchmarkConsumerFrontierDiagnostics.read(binding, 1, cursor))
                    .isInstanceOf(AssertionError.class);
        }
    }

    @Test
    void writerOrTableEpochMismatchIsUnknownAndRetainsTheActualEpoch() {
        for (boolean writerMismatch : List.of(true, false)) {
            Document cursor = cursor(List.of("writer"));
            (writerMismatch ? writer(cursor, "writer") : table(cursor)).put("sinkAckedEpoch", 2L);
            var result = read(cursor);

            assertThat(result).containsEntry("state", "UNKNOWN").containsEntry("unknownReason", "ACK_EPOCH_MISMATCH")
                    .doesNotContainKey("readToConfirmedAckSequenceDistance");
            Map<?, ?> actual = writerMismatch ? frontier(result, "writer") : (Map<?, ?>) result.get("tableFrontier");
            assertThat(actual.get("sinkAckedEpoch")).isEqualTo(2L);
            if (writerMismatch) { assertThat(result).doesNotContainKey("minimumWriterAckSeq"); }
        }
    }

    @Test
    void missingFloatingOrNegativeAckCannotBecomeAConfirmedZero() {
        for (Object actual : new Object[] {null, 50.0, -1L}) {
            Document cursor = cursor(List.of("writer"));
            if (actual == null) { writer(cursor, "writer").remove("sinkAckedSeq"); }
            else { writer(cursor, "writer").put("sinkAckedSeq", actual); }
            var result = read(cursor);

            assertThat(result).containsEntry("state", "UNKNOWN")
                    .doesNotContainKeys("minimumWriterAckSeq", "readToConfirmedAckSequenceDistance");
            assertThat(frontier(result, "writer").get("sinkAckedSeq")).isNotEqualTo(0L);
        }
    }

    @Test
    void aSnapshotOnlyAckIsNotAConfirmedCdcSequence() {
        Document cursor = cursor(List.of("writer"));
        writer(cursor, "writer").put("sinkAckedSeq", SourceOrder.SNAPSHOT_SEQ);
        table(cursor).put("sinkAckedSeq", SourceOrder.SNAPSHOT_SEQ);
        var result = read(cursor);

        assertThat(result).containsEntry("state", "UNKNOWN").containsEntry("unknownReason", "SNAPSHOT_ACK_ONLY")
                .doesNotContainKeys("minimumWriterAckSeq", "readToConfirmedAckSequenceDistance");
        assertThat(frontier(result, "writer").get("sinkAckedSeq")).isEqualTo(SourceOrder.SNAPSHOT_SEQ);
    }

    @Test
    void anAckAheadOfReadIsKeptUnclampedAndNoDistanceIsPublished() {
        Document cursor = cursor(List.of("writer"));
        cursor.get("perTableSeq", Document.class).put(TABLE, 10L);
        var result = read(cursor);

        assertThat(result).containsEntry("state", "UNKNOWN").containsEntry("consumerReadSeq", 10L)
                .containsEntry("minimumWriterAckSeq", 50L).doesNotContainKey("readToConfirmedAckSequenceDistance");
        assertThat(frontier(result, "writer").get("sinkAckedSeq")).isEqualTo(50L);
    }

    @Test
    void tableCrossChecksAndRingDoneBehindActualAckRemainUnknown() {
        for (String changed : List.of("table-ack", "table-ring", "writer-ring")) {
            Document cursor = cursor(List.of("writer"));
            switch (changed) {
                case "table-ack" -> table(cursor).put("sinkAckedSeq", 49L);
                case "table-ring" -> cursor.get("perTableRingDone", Document.class).put(TABLE, 51L);
                case "writer-ring" -> writer(cursor, "writer").put("ringDone", 49L);
                default -> throw new AssertionError("unregistered test mutation");
            }
            var result = read(cursor);
            assertThat(result).containsEntry("state", "UNKNOWN").doesNotContainKey("readToConfirmedAckSequenceDistance");
            assertThat(result.get("unknownReason")).isEqualTo(switch (changed) {
                case "table-ack" -> "TABLE_ACK_DIFFERS_FROM_WRITER_MINIMUM";
                case "table-ring" -> "TABLE_RING_DONE_DIFFERS_FROM_WRITER_MINIMUM";
                case "writer-ring" -> "RING_DONE_BEHIND_ACTUAL_ACK";
                default -> throw new AssertionError("unregistered test mutation");
            });
        }
    }

    @Test
    void theParserIsReadOnlyItsResultIsImmutableAndUnknownScalarsAreJsonSafe() {
        Document cursor = cursor(List.of("writer"));
        writer(cursor, "writer").put("sinkAckedOffset", "opaque-source-position");
        String before = cursor.toJson();
        var result = read(cursor);
        assertThat(cursor.toJson()).isEqualTo(before);
        assertThatThrownBy(result::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> frontier(result, "writer").clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(JsonWriter.write(result)).doesNotContain("opaque-source-position", "sinkAckFence", "sinkAckedOffset");

        writer(cursor, "writer").put("sinkAckedSeq", Double.NaN);
        var unknown = read(cursor);
        assertThat(unknown).containsEntry("state", "UNKNOWN");
        assertThat(JsonWriter.write(unknown)).contains("java.lang.Double").doesNotContain("NaN");
        writer(cursor, "writer").put("sinkAckedSeq", new Document("token", "another-opaque-position"));
        assertThat(JsonWriter.write(read(cursor))).contains("org.bson.Document").doesNotContain("another-opaque-position");
    }

    @Test
    void theExistingWriterBoundsAndMissingOrInvalidReadArePreserved() {
        List<String> writers = new ArrayList<>();
        for (int i = 0; i < 64; i++) { writers.add("writer-" + i); }
        var full = read(cursor(writers));
        assertThat(full).containsEntry("state", "RECORDED").containsEntry("expectedWriterCount", 64);
        writers.add("writer-64");
        assertThatThrownBy(() -> read(cursor(writers))).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> read(cursor(List.of("x".repeat(257))))).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> read(cursor(List.of("same", "same")))).isInstanceOf(AssertionError.class);

        for (Object actual : new Object[] {null, 75.0, -1L}) {
            Document cursor = cursor(List.of("writer"));
            if (actual == null) { cursor.get("perTableSeq", Document.class).remove(TABLE); }
            else { cursor.get("perTableSeq", Document.class).put(TABLE, actual); }
            var result = read(cursor);
            assertThat(result).containsEntry("state", "UNKNOWN").doesNotContainKey("readToConfirmedAckSequenceDistance");
            assertThat(result.get("consumerReadSeq")).isNotEqualTo(0L);
        }
    }

    private static Map<String, Object> read(Document cursor) {
        return BenchmarkConsumerFrontierDiagnostics.read(binding(cursor), 1, cursor);
    }

    private static BenchmarkTableAckGate.Binding binding(Document cursor) {
        return BenchmarkTableAckGate.bind(CHAIN, PIPELINE, SOURCE, TABLE, cursor);
    }

    private static Document cursor(List<String> writers) {
        String consumer = SrsConsumerId.of(PIPELINE, SOURCE).value();
        Document progress = new Document();
        for (String writer : writers) { progress.put(writer, new Document(TABLE, acknowledged())); }
        return new Document("_id", new Document("chain", CHAIN).append("pipeline", consumer))
                .append("miningChainId", CHAIN).append("pipelineId", consumer)
                .append("ownerPipelineId", PIPELINE).append("sourceNodeId", SOURCE)
                .append("expectedSinkWriters", new Document(TABLE, new ArrayList<>(writers)))
                .append("sinkAckFence", new Document("generation", 1L))
                .append("perTableSeq", new Document(TABLE, 75L)).append("sinkWriterProgress", progress)
                .append("sinkAckedByTable", new Document(TABLE, acknowledged()))
                .append("perTableRingDone", new Document(TABLE, 50L));
    }

    private static Document acknowledged() {
        return new Document("sinkAckedEpoch", 1L).append("sinkAckedSeq", 50L).append("ringDone", 50L);
    }

    private static Document writer(Document cursor, String writer) {
        return cursor.get("sinkWriterProgress", Document.class).get(writer, Document.class).get(TABLE, Document.class);
    }

    private static Document table(Document cursor) { return cursor.get("sinkAckedByTable", Document.class).get(TABLE, Document.class); }

    private static Map<?, ?> frontier(Map<String, Object> result, String writer) {
        return (Map<?, ?>) ((Map<?, ?>) result.get("writerFrontiers")).get(writer);
    }
}
