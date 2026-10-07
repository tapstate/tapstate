package io.tapstate.e2e;

import io.tapstate.runtime.srs.SrsRingbuffer;
import io.tapstate.spi.store.SrsConsumerId;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkTableConfirmationOracleTest {
    @Test
    void everyFrozenMeasuredMarkerUsesTheActualEnvelopeOperationAndFinalValue() {
        for (var workload : BenchmarkWorkloadDefinitions.all()) {
            for (var chain : workload.sourceChains()) {
                for (var marker : BenchmarkMeasuredEndMarkers.forChain(workload, chain).entrySet()) {
                    var descriptor = BenchmarkTableCaptureSet.measured(workload, chain, marker.getKey(), marker.getValue(),
                            SrsRingbuffer.ringName("chain", chain.table()));
                    assertThat(descriptor.op()).isEqualTo(BenchmarkMeasuredEndMarkers.operation(marker.getKey()).symbol());
                    assertThat(descriptor.rowId()).isEqualTo(marker.getValue());
                }
            }
        }
    }

    @Test
    void exactTableOrderCanProveItsOwnTerminalWithoutInventingAnOpaqueAckToken() {
        Document cursor = cursor();
        var binding = BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor);
        var point = new BenchmarkTableTerminalObserver.Point("terminal", SrsRingbuffer.ringName("chain", "orders"), 7, 41, null);
        var proof = BenchmarkAckOracle.TableConfirmationProof.from("terminal", "actual-source-token", point, binding, cursor);
        var terminal = new BenchmarkAckOracle.TerminalEvent("terminal", "actual-source-token");
        var chain = new BenchmarkAckOracle.SourceChain("pipeline/source", List.of(terminal), null, null, proof);
        BenchmarkAckOracle.verify(List.of(fork(chain)));
        assertThat(PipelineBenchmarkLiveRunIT.tableConfirmation(proof)).containsEntry("seq", 41L)
                .containsEntry("actualMarkerSourceToken", null);
        var wrongTerminal = new BenchmarkAckOracle.SourceChain("pipeline/source",
                List.of(new BenchmarkAckOracle.TerminalEvent("terminal", "another-source-token")),
                "valid-token-ack", (ack, token) -> true, proof);
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(fork(wrongTerminal)))).isInstanceOf(AssertionError.class);
        var wrongChain = new BenchmarkAckOracle.SourceChain("another/source", List.of(terminal), null, null, proof);
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(fork(wrongChain)))).isInstanceOf(AssertionError.class);
    }

    @Test
    void directConstructionCannotBypassTheConfirmationByteBound() {
        Document cursor = cursor();
        var binding = BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor);
        var point = new BenchmarkTableTerminalObserver.Point("terminal", SrsRingbuffer.ringName("chain", "orders"), 7, 41, null);
        assertThatThrownBy(() -> new BenchmarkAckOracle.TableConfirmationProof(
                "terminal", "source-token", point, binding, "x".repeat(65_537)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("byte budget");
    }

    @Test
    void missingMarkerOrUnconfirmedWriterCannotConstructATableProof() {
        Document cursor = cursor();
        var binding = BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor);
        var point = new BenchmarkTableTerminalObserver.Point("other-terminal", SrsRingbuffer.ringName("chain", "orders"), 7, 41, null);
        assertThatThrownBy(() -> BenchmarkAckOracle.TableConfirmationProof.from(
                "terminal", "actual-source-token", point, binding, cursor)).isInstanceOf(AssertionError.class);
        var correctPoint = new BenchmarkTableTerminalObserver.Point("terminal", point.ring(), 7, 41, null);
        cursor.get("sinkWriterProgress", Document.class).get("writer", Document.class)
                .get("orders", Document.class).put("sinkAckedSeq", 40L);
        assertThatThrownBy(() -> BenchmarkAckOracle.TableConfirmationProof.from(
                "terminal", "actual-source-token", correctPoint, binding, cursor)).isInstanceOf(AssertionError.class);
    }

    private static BenchmarkAckOracle.Fork fork(BenchmarkAckOracle.SourceChain chain) {
        return new BenchmarkAckOracle.Fork("fork", List.of(chain), Map.of("terminal", 1L), "checksum", 0);
    }

    private static Document cursor() {
        String consumer = SrsConsumerId.of("pipeline", "source").value();
        return new Document("_id", new Document("chain", "chain").append("pipeline", consumer))
                .append("miningChainId", "chain").append("pipelineId", consumer)
                .append("ownerPipelineId", "pipeline").append("sourceNodeId", "source")
                .append("expectedSinkWriters", new Document("orders", List.of("writer")))
                .append("sinkWriterProgress", new Document("writer", new Document("orders", point())))
                .append("sinkAckedByTable", new Document("orders", point()))
                .append("perTableRingDone", new Document("orders", 41L));
    }

    private static Document point() {
        return new Document("sinkAckedEpoch", 7L).append("sinkAckedSeq", 41L).append("ringDone", 41L);
    }
}
