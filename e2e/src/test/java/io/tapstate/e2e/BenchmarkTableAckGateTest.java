package io.tapstate.e2e;

import io.tapstate.runtime.srs.SrsRingbuffer;
import io.tapstate.spi.store.SrsConsumerId;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkTableAckGateTest {
    private static final String CONSUMER = SrsConsumerId.of("pipeline", "source").value();
    private static final BenchmarkTableTerminalObserver.Point POINT = new BenchmarkTableTerminalObserver.Point(
            "marker", SrsRingbuffer.ringName("chain", "orders"), 7L, 41L, null);

    @Test
    void anUnchangedLongValuedFenceKeepsItsExactBsonTypesAndCannotBeMutatedThroughTheBinding() {
        Document cursor = cursor();
        cursor.put("sinkAckFence", new Document("executionGeneration", 7L)
                .append("owner", new Document("generation", 1L)));
        var binding = BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor);
        assertThat(binding.fence().get("executionGeneration")).isInstanceOf(Long.class);
        assertThat(BenchmarkTableAckGate.covers(binding, POINT, cursor)).isTrue();
        binding.fence().get("owner", Document.class).put("generation", 99L);
        assertThat(BenchmarkTableAckGate.covers(binding, POINT, cursor)).isTrue();
    }

    @Test
    void everyWriterAndTheDerivedTableMustConfirmTheExactTokenlessPoint() {
        Document cursor = cursor();
        var binding = BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor);
        assertThat(BenchmarkTableAckGate.covers(binding, POINT, cursor)).isTrue();
        cursor.get("sinkWriterProgress", Document.class).get("second", Document.class)
                .get("orders", Document.class).put("sinkAckedSeq", 40L);
        assertThat(BenchmarkTableAckGate.covers(binding, POINT, cursor)).isFalse();
        cursor.get("perTableSeq", Document.class).put("orders", 999L);
        assertThat(BenchmarkTableAckGate.covers(binding, POINT, cursor)).isFalse();
    }

    @Test
    void aWriterRingCursorOrDerivedAckCannotReplaceItsActualConfirmation() {
        Document cursor = cursor();
        var binding = BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor);
        Document writer = cursor.get("sinkWriterProgress", Document.class).get("second", Document.class)
                .get("orders", Document.class);
        writer.remove("sinkAckedSeq");
        assertThat(BenchmarkTableAckGate.covers(binding, POINT, cursor)).isFalse();
        writer.put("sinkAckedSeq", 41L); writer.put("ringDone", 40L);
        assertThat(BenchmarkTableAckGate.covers(binding, POINT, cursor)).isFalse();
        writer.put("ringDone", 41L);
        cursor.get("sinkAckedByTable", Document.class).get("orders", Document.class).put("sinkAckedSeq", 40L);
        assertThat(BenchmarkTableAckGate.covers(binding, POINT, cursor)).isFalse();
    }

    @Test
    void wrongCaptureGenerationOrTableCannotQualifyThePoint() {
        Document cursor = cursor();
        var binding = BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor);
        var foreign = new BenchmarkTableTerminalObserver.Point("marker", "srs.chain.items", 7L, 41L, null);
        assertThatThrownBy(() -> BenchmarkTableAckGate.covers(binding, foreign, cursor)).isInstanceOf(AssertionError.class);
        cursor.get("sinkWriterProgress", Document.class).get("second", Document.class)
                .get("orders", Document.class).put("sinkAckedEpoch", 8L);
        assertThatThrownBy(() -> BenchmarkTableAckGate.covers(binding, POINT, cursor)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("another capture generation");
    }

    @Test
    void aChangedExecutionFencePlanOrConsumerCannotReuseTheFrozenBinding() {
        Document cursor = cursor();
        var binding = BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor);
        cursor.put("sinkAckFence", new Document("executionGeneration", 9L));
        assertThatThrownBy(() -> BenchmarkTableAckGate.covers(binding, POINT, cursor)).isInstanceOf(AssertionError.class);
        cursor.remove("sinkAckFence"); cursor.get("expectedSinkWriters", Document.class).put("orders", List.of("first"));
        assertThatThrownBy(() -> BenchmarkTableAckGate.covers(binding, POINT, cursor)).isInstanceOf(AssertionError.class);
        cursor.get("expectedSinkWriters", Document.class).put("orders", List.of("first", "second"));
        cursor.put("sourceNodeId", "another-source");
        assertThatThrownBy(() -> BenchmarkTableAckGate.covers(binding, POINT, cursor)).isInstanceOf(AssertionError.class);
    }

    @Test
    void aFractionalAckSequenceCannotBeRoundedIntoCompletion() {
        Document cursor = cursor();
        var binding = BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor);
        cursor.get("sinkWriterProgress", Document.class).get("second", Document.class)
                .get("orders", Document.class).put("sinkAckedSeq", 41.5d);
        assertThatThrownBy(() -> BenchmarkTableAckGate.covers(binding, POINT, cursor)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("non-integer");
    }

    @Test
    void currentWriterDurabilityIsSeparateFromItsLastTokenAndConstrainedByTheSlowestWriter() {
        Document cursor = currentCursor(List.of("first", "second"));
        var binding = BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor);
        assertThat(binding.writerRunId()).isEqualTo("current-run");
        for (String writer : List.of("first", "second")) {
            currentWriter(cursor, writer).append("tokenEpoch", 7L).append("tokenSeq", 40L)
                    .append("token", "older-resumable-position");
        }
        assertThat(BenchmarkTableAckGate.covers(binding, POINT, cursor)).isTrue();
        currentWriter(cursor, "first").put("durableSeq", 999L);
        currentWriter(cursor, "second").put("durableSeq", 40L);
        cursor.get("perTableSeq", Document.class).put("orders", 999L);
        assertThat(BenchmarkTableAckGate.covers(binding, POINT, cursor)).isFalse();
        currentWriter(cursor, "second").put("durableSeq", 41L);
        assertThat(BenchmarkTableAckGate.covers(binding, POINT, cursor)).isTrue();
        cursor.get("sinkAckedByTable", Document.class).get("orders", Document.class).put("sinkAckedSeq", 40L);
        assertThat(BenchmarkTableAckGate.covers(binding, POINT, cursor)).isFalse();
    }

    @Test
    void aMissingCurrentWriterCannotUseCompleteLegacyProgressOrAReadCursorInstead() {
        Document cursor = currentCursor(List.of("first", "second"));
        var binding = BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor);
        currentEntries(cursor).remove(writerKey("second"));
        cursor.get("perTableSeq", Document.class).put("orders", 999L);
        assertThat(BenchmarkTableAckGate.covers(binding, POINT, cursor)).isFalse();
        cursor.get("writerRun", Document.class).put("progress", new Document());
        assertThat(BenchmarkTableAckGate.covers(binding, POINT, cursor)).isFalse();
    }

    @Test
    void aFrozenCurrentRunCannotBeReplacedOrDowngradedToLegacyAccounting() {
        Document cursor = currentCursor(List.of("first", "second"));
        var binding = BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor);
        cursor.get("writerRun", Document.class).put("id", "another-run");
        assertThatThrownBy(() -> BenchmarkTableAckGate.covers(binding, POINT, cursor))
                .isInstanceOf(AssertionError.class).hasMessageContaining("writer run changed");
        cursor.remove("writerRun");
        assertThatThrownBy(() -> BenchmarkTableAckGate.covers(binding, POINT, cursor))
                .isInstanceOf(AssertionError.class).hasMessageContaining("format changed");
    }

    @Test
    void aLegacyBindingCannotAdoptANewWriterRun() {
        Document cursor = cursor();
        var binding = BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor);
        assertThat(binding.writerRunId()).isNull();
        cursor.put("writerRun", currentCursor(List.of("first", "second")).get("writerRun"));
        assertThatThrownBy(() -> BenchmarkTableAckGate.covers(binding, POINT, cursor))
                .isInstanceOf(AssertionError.class).hasMessageContaining("format changed");
    }

    @Test
    void currentWriterIdsUseTheirExactEncodedKeysAndCannotChangeTheFrozenRoster() {
        Document cursor = currentCursor(List.of("serve.first#0", "serve.second#0"));
        var binding = BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor);
        assertThat(BenchmarkTableAckGate.covers(binding, POINT, cursor)).isTrue();
        cursor.get("writerRun", Document.class).get("expected", Document.class)
                .put("orders", List.of("serve.first#0"));
        assertThatThrownBy(() -> BenchmarkTableAckGate.covers(binding, POINT, cursor))
                .isInstanceOf(AssertionError.class);

        Document wrongKey = currentCursor(List.of("first", "second"));
        Document second = (Document) currentEntries(wrongKey).remove(writerKey("second"));
        currentEntries(wrongKey).put("second", second);
        assertThatThrownBy(() -> BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", wrongKey))
                .isInstanceOf(AssertionError.class).hasMessageContaining("writer roster");
    }

    @Test
    void aPresentMalformedWriterRunNeverFallsBackToTheCompleteLegacyShape() {
        for (String mutation : List.of("null-run", "scalar-run", "bad-id", "missing-expected",
                "bad-progress", "missing-durable", "fractional-durable", "foreign-table", "foreign-writer")) {
            Document cursor = currentCursor(List.of("first", "second"));
            Document run = cursor.get("writerRun", Document.class);
            switch (mutation) {
                case "null-run" -> cursor.put("writerRun", null);
                case "scalar-run" -> cursor.put("writerRun", "not-a-run-document");
                case "bad-id" -> run.put("id", 7L);
                case "missing-expected" -> run.remove("expected");
                case "bad-progress" -> run.put("progress", 41L);
                case "missing-durable" -> currentWriter(cursor, "second").remove("durableSeq");
                case "fractional-durable" -> currentWriter(cursor, "second").put("durableSeq", 41.5d);
                case "foreign-table" -> run.get("progress", Document.class).put("items", new Document());
                case "foreign-writer" -> currentEntries(cursor).put(writerKey("unexpected"), durable("unexpected"));
                default -> throw new AssertionError("unregistered writer-run mutation");
            }
            assertThatThrownBy(() -> BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor))
                    .as(mutation).isInstanceOf(AssertionError.class);
        }
    }

    @Test
    void currentEpochSnapshotAndTokenContradictionsCannotQualifyTerminalCoverage() {
        Document cursor = currentCursor(List.of("first", "second"));
        var binding = BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor);
        currentWriter(cursor, "second").put("durableEpoch", 8L);
        assertThatThrownBy(() -> BenchmarkTableAckGate.covers(binding, POINT, cursor))
                .isInstanceOf(AssertionError.class).hasMessageContaining("another capture generation");
        currentWriter(cursor, "second").put("durableEpoch", 7L);
        currentWriter(cursor, "second").put("durableSeq", io.tapstate.core.event.SourceOrder.SNAPSHOT_SEQ);
        assertThat(BenchmarkTableAckGate.covers(binding, POINT, cursor)).isFalse();
        currentWriter(cursor, "second").put("durableSeq", 41L);
        currentWriter(cursor, "second").append("tokenEpoch", 7L).append("tokenSeq", 42L).append("token", "ahead");
        assertThatThrownBy(() -> BenchmarkTableAckGate.covers(binding, POINT, cursor))
                .isInstanceOf(AssertionError.class).hasMessageContaining("ahead of its durable");
    }

    private static Document currentCursor(List<String> writers) {
        Document entries = new Document();
        for (String writer : writers) { entries.put(writerKey(writer), durable(writer)); }
        // Complete old fields remain present so each new-format negative control also rejects fallback.
        return cursor().append("writerRun", new Document("id", "current-run")
                .append("expected", new Document("orders", writers))
                .append("progress", new Document("orders", entries)));
    }

    private static Document currentEntries(Document cursor) {
        return cursor.get("writerRun", Document.class).get("progress", Document.class).get("orders", Document.class);
    }

    private static Document currentWriter(Document cursor, String writer) {
        return currentEntries(cursor).get(writerKey(writer), Document.class);
    }

    private static Document durable(String writer) {
        return new Document("writer", writer).append("durableEpoch", 7L).append("durableSeq", 41L);
    }

    private static String writerKey(String writer) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(writer.getBytes(StandardCharsets.UTF_8));
    }

    private static Document cursor() {
        return new Document("_id", new Document("chain", "chain").append("pipeline", CONSUMER))
                .append("miningChainId", "chain").append("pipelineId", CONSUMER)
                .append("ownerPipelineId", "pipeline").append("sourceNodeId", "source")
                .append("expectedSinkWriters", new Document("orders", List.of("first", "second")))
                .append("sinkWriterProgress", new Document("first", new Document("orders", point()))
                        .append("second", new Document("orders", point())))
                .append("sinkAckedByTable", new Document("orders", point()))
                .append("perTableRingDone", new Document("orders", 41L))
                .append("perTableSeq", new Document("orders", 99L));
    }

    private static Document point() {
        return new Document("sinkAckedEpoch", 7L).append("sinkAckedSeq", 41L).append("ringDone", 41L);
    }
}
