package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.spi.store.SrsConsumerId;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Store-shaped controls for metadata selection; these do not certify a live connector run. */
class BenchmarkTerminalMetaReceiptTest {
    private static final String PHYSICAL = "physical-chain";
    private static final String CONSUMER = SrsConsumerId.of("pipeline", "source").value();
    private static final Instant AT = Instant.parse("2026-10-04T00:00:00Z");
    private static final BenchmarkWorkloadDefinitions.SourceChain CHAIN = new BenchmarkWorkloadDefinitions.SourceChain(
            "pipeline/source", "pipeline", "source", "orders", "terminal-id", 7L);

    @Test
    void retainsTheActualConsumerAndKeepsThreeKindsOfProgressSeparate() {
        Document consumer = split(CONSUMER).append("progressKind", "SRS")
                .append("perTableSeq", new Document("orders", 99L))
                .append("sinkAckedByTable", new Document("orders", ack("terminal")));
        Document root = root().append("sourceReadOffset", "captured-after-terminal")
                .append("sourceReadEpoch", 12L).append("sourceReadSeq", 44L)
                .append("sourceReadDurable", true);
        Map<String, Object> receipt = receipt(position(false), root, consumer, null);
        assertThat(receipt).containsEntry("actualConsumerId", CONSUMER).containsEntry("consumerSourceId", "source")
                .containsEntry("consumerReadSeq", 99L).containsEntry("sourceReadDurable", true);
        assertThat(receipt.get("targetAck"))
                .isEqualTo(Map.of("token", "terminal", "epoch", 3L, "seq", 7L));
        assertThat(receipt.get("sourceRead"))
                .isEqualTo(Map.of("token", "captured-after-terminal", "epoch", 12L, "seq", 44L));
    }

    @Test
    void anEligibleLegacyRecordKeepsItsStoredIdentityAndMissingFieldsAbsent() {
        Map<String, Object> receipt = receipt(position(false), root(), null,
                split("pipeline").append("sinkAckedSrcpos", "terminal"));
        assertThat(receipt).containsEntry("actualConsumerId", "pipeline")
                .containsEntry("consumerBinding", "SOLE_SOURCE_LEGACY_ID")
                .doesNotContainKeys("consumerSourceId", "progressKind", "consumerReadSeq", "consumerRingDoneSeq",
                        "sourceRead", "sourceReadDurable", "miningEpoch");
        assertThat(receipt.get("targetAck")).isEqualTo(Map.of("token", "terminal"));
    }

    @Test
    void anEmbeddedConsumerKeepsItsStoredMapKeyAsTheActualIdentity() {
        Document embedded = new Document("progressKind", "SRS")
                .append("sinkAckedByTable", new Document("orders", ack("terminal")));
        Map<String, Object> receipt = receipt(position(false),
                root().append("consumerOffsets", new Document(CONSUMER, embedded)), null, null);

        assertThat(receipt).containsEntry("actualConsumerId", CONSUMER)
                .containsEntry("consumerSourceId", "source")
                .containsEntry("consumerDocument", "EMBEDDED_CHAIN_CONSUMER");
    }

    @Test
    void aSplitConsumerWinsOverTheSameEmbeddedIdentityDuringMigration() {
        Document embedded = new Document("progressKind", "SRS")
                .append("sinkAckedByTable", new Document("orders", ack("before-terminal")));
        Document split = split(CONSUMER).append("progressKind", "SRS")
                .append("sinkAckedByTable", new Document("orders", ack("terminal")));
        Map<String, Object> receipt = receipt(position(false),
                root().append("consumerOffsets", new Document(CONSUMER, embedded)), split, null);

        assertThat(receipt).containsEntry("consumerDocument", "SPLIT_CONSUMER_DOCUMENT");
        assertThat(receipt.get("targetAck"))
                .isEqualTo(Map.of("token", "terminal", "epoch", 3L, "seq", 7L));
    }

    @Test
    void aWrongStoredIdentityCannotBeQualifiedByTheLookupCandidate() {
        Document wrong = split(CONSUMER).append("pipelineId", "another-pipeline")
                .append("progressKind", "SRS")
                .append("sinkAckedByTable", new Document("orders", ack("terminal")));
        assertThatThrownBy(() -> receipt(position(false), root(), wrong, null))
                .isInstanceOf(AssertionError.class).hasMessageContaining("split consumer identity differs");
    }

    @Test
    void aStoredConfirmationDoesNotReplaceAStaleWireTargetAck() {
        String body = JsonWriter.write(Map.of("pipelineId", "pipeline", "chains", List.of(
                Map.of("chainId", PHYSICAL, "sourceId", "source", "tables", List.of("orders"),
                        "targetAcked", Map.of("token", "before-terminal")))));
        ControlPlane.PositionRead stale = ControlPlane.interpretPositionRead(200, body, "pipeline");
        Document confirmed = split(CONSUMER).append("progressKind", "SRS")
                .append("sinkAckedByTable", new Document("orders", ack("terminal")));
        assertThatThrownBy(() -> receipt(stale, root(), confirmed, null))
                .isInstanceOf(AssertionError.class).hasMessageContaining("position target ACK no longer covers");
    }

    @Test
    void aLegacyConsumerCannotStandForTwoSourceNodesOnOnePhysicalChain() {
        assertThatThrownBy(() -> receipt(position(true), root(), null,
                split("pipeline").append("sinkAckedSrcpos", "terminal")))
                .isInstanceOf(AssertionError.class).hasMessageContaining("ambiguous legacy source consumer");
    }

    @Test
    void anotherTablesAckCannotQualifyThisTable() {
        Document consumer = split(CONSUMER).append("progressKind", "SRS")
                .append("sinkAckedSrcpos", "terminal")
                .append("sinkAckedByTable", new Document("items", ack("terminal")));
        assertThatThrownBy(() -> receipt(position(false), root(), consumer, null))
                .isInstanceOf(AssertionError.class).hasMessageContaining("stored table target ACK does not cover");
    }

    @Test
    void anOtherTablesLegacyConfirmationCannotUseTheAggregateFallback() {
        Document legacy = split("pipeline").append("sinkAckedSrcpos", "terminal")
                .append("sinkAckedByTable", new Document("items", ack("terminal")));
        assertThatThrownBy(() -> receipt(position(false), root(), null, legacy))
                .isInstanceOf(AssertionError.class).hasMessageContaining("stored table target ACK does not cover");
    }

    @Test
    void aCheckpointAndAdvancedReadCursorDoNotReplaceAMissingOrStaleTargetAck() {
        for (Document confirmations : List.of(new Document(), new Document("orders", ack("before-terminal")))) {
            Document consumer = split(CONSUMER).append("progressKind", "SRS")
                    .append("perTableSeq", new Document("orders", 999L)).append("sinkAckedByTable", confirmations);
            assertThatThrownBy(() -> receipt(position(false), root().append("sourceReadOffset", "terminal"),
                    consumer, null)).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("stored table target ACK does not cover");
        }
    }

    private static Map<String, Object> receipt(ControlPlane.PositionRead read, Document root,
            Document scoped, Document legacy) {
        return BenchmarkTerminalMetaReceipt.interpret(read, CHAIN, "terminal", String::equals,
                root, scoped, legacy, AT, AT);
    }

    private static Document root() {
        return new Document("_id", PHYSICAL).append("consumerOffsets", new Document());
    }

    private static Document split(String consumer) {
        return new Document("_id", new Document("chain", PHYSICAL).append("pipeline", consumer))
                .append("miningChainId", PHYSICAL).append("pipelineId", consumer);
    }

    private static Document ack(String token) {
        return new Document("sinkAckedSrcpos", token).append("sinkAckedEpoch", 3L).append("sinkAckedSeq", 7L);
    }

    private static ControlPlane.PositionRead position(boolean otherSource) {
        Map<String, Object> mine = Map.of("chainId", PHYSICAL, "sourceId", "source", "tables", List.of("orders"),
                "targetAcked", Map.of("token", "terminal", "epoch", 3, "seq", 7));
        List<Map<String, Object>> chains = otherSource ? List.of(mine,
                Map.of("chainId", PHYSICAL, "sourceId", "other-source", "tables", List.of("items"),
                        "targetAcked", Map.of("token", "other-terminal"))) : List.of(mine);
        return ControlPlane.interpretPositionRead(200, JsonWriter.write(Map.of("pipelineId", "pipeline", "chains", chains)),
                "pipeline");
    }
}
