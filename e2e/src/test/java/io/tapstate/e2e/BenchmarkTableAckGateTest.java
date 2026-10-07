package io.tapstate.e2e;

import io.tapstate.runtime.srs.SrsRingbuffer;
import io.tapstate.spi.store.SrsConsumerId;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import java.util.List;
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
