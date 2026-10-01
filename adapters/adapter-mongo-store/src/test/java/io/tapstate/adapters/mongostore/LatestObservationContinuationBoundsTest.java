package io.tapstate.adapters.mongostore;

import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.spi.store.ObservationContinuation;
import io.tapstate.spi.store.ObservationContinuationBounds;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LatestObservationContinuationBoundsTest {
    private static final String PIPE = "orders", RECORDS = "tapstate.pipeline.records";
    private static final Instant START = Instant.parse("2026-10-01T00:00:00.123456789Z");

    @Test
    void excessiveFactAndPointCountsAreRejectedBeforeTheirCellsAreMaterialized() throws Exception {
        byte[] facts = payload(ObservationContinuationBounds.maximumFacts() + 1, 1, 0, PIPE, false);
        byte[] points = payload(1, ObservationContinuationBounds.maximumPoints(RECORDS) + 1, 0, PIPE, false);
        assertThatThrownBy(() -> decode(facts)).hasRootCauseMessage("a private continuation exceeds its fact limit");
        assertThatThrownBy(() -> decode(points)).hasRootCauseMessage("a private continuation exceeds its point limit");
    }

    @Test
    void duplicateFactsAndNativeGroupsAreRejectedWithinThePrivateDecoder() throws Exception {
        byte[] facts = payload(2, 1, 0, PIPE, false);
        byte[] groups = payload(1, 1, 2, PIPE, false);
        assertThatThrownBy(() -> decode(facts)).hasRootCauseMessage("a private continuation carries a duplicate fact");
        assertThatThrownBy(() -> decode(groups)).hasRootCauseMessage("a private continuation carries a duplicate native group");
    }

    @Test
    void aForeignPointOwnerCannotBeReadOrEncodedAsAnotherPipeline() throws Exception {
        byte[] foreign = payload(1, 1, 0, "others", false);
        assertThatThrownBy(() -> decode(foreign))
                .hasRootCauseMessage("a private continuation point belongs to its encoded pipeline owner");
        MetricFact fact = MetricFact.single(RECORDS, MetricType.COUNTER, "{record}", point(
                Map.of(MetricAttributes.PIPELINE_ID, "others", MetricAttributes.TABLE_ID, "table-a",
                        MetricAttributes.DIRECTION, "out"), 7));
        assertThatThrownBy(() -> LatestObservationPayloadCodec.encodeContinuation(PIPE, value(List.of(fact)), noChunks()))
                .hasMessage("a private continuation point belongs to its encoded pipeline owner");
    }

    @Test
    void anUnsupportedOpenLabelIsRefusedOnlyByThePrivateSnapshotBoundary() throws Exception {
        byte[] unsupported = payload(1, 1, 0, PIPE, true);
        assertThatThrownBy(() -> decode(unsupported)).hasRootCauseMessage("a private point has an undeclared attribute");
        MetricFact publicFact = MetricFact.single(RECORDS, MetricType.COUNTER, "{record}", point(
                Map.of(MetricAttributes.PIPELINE_ID, PIPE, "row.id", "arbitrary", MetricAttributes.DIRECTION, "out"), 7));
        assertThat(publicFact.points()).hasSize(1);
        assertThatThrownBy(() -> value(List.of(publicFact)))
                .hasMessage("a private continuation carries only its instrument's declared attributes");
    }

    @Test
    void declaredOperationLabelsAndAlreadyFoldedOverflowRemainReadable() {
        MetricFact records = new MetricFact(RECORDS, MetricType.COUNTER, "{record}", List.of(
                point(Map.of(MetricAttributes.PIPELINE_ID, PIPE, MetricAttributes.TABLE_ID, "table-a",
                        MetricAttributes.DIRECTION, "in", MetricAttributes.OP, "read"), 7),
                point(Map.of(MetricAttributes.PIPELINE_ID, PIPE, MetricAttributes.OVERFLOW, "true",
                        MetricAttributes.DIRECTION, "out", MetricAttributes.OP, "insert"), 2)));
        String operation = MetricAttributes.STATE_OPERATIONS.stream().sorted().findFirst().orElseThrow();
        MetricFact stateCost = MetricFact.single(CardinalityBudget.STATE_OPERATION_COUNT.instrument(), MetricType.COUNTER,
                "{operation}", point(Map.of(MetricAttributes.PIPELINE_ID, PIPE, MetricAttributes.STATE_NAMESPACE, "state-a",
                        MetricAttributes.STATE_OPERATION, operation, MetricAttributes.STATE_OUTCOME, "success"), 1));
        var value = value(List.of(records, stateCost));
        var encoded = LatestObservationPayloadCodec.encodeContinuation(PIPE, value, noChunks());
        assertThat(LatestObservationPayloadCodec.decodeContinuationInline(encoded.inlinePayload(), encoded.payloadDigest(),
                encoded.encodedBytes()).baselineFacts()).isEqualTo(value.baselineFacts());
    }

    private static ObservationContinuation value(List<MetricFact> facts) {
        return new ObservationContinuation("handoff", new ObservationStore.Scope("inc-a", 1), Optional.empty(),
                Optional.empty(), facts, List.of());
    }

    private static MetricPoint point(Map<String, String> attributes, long total) {
        return MetricPoint.accumulated(attributes, START, START.plusSeconds(10), total);
    }

    private static LatestObservationPayloadCodec.ChunkWriter noChunks() {
        return new LatestObservationPayloadCodec.ChunkWriter() {
            @Override public void begin() { throw new AssertionError("the bounded fixture is inline"); }
            @Override public void write(LatestObservationPayloadCodec.Chunk chunk) { throw new AssertionError("unexpected chunk"); }
        };
    }

    private static LatestObservationPayloadCodec.ContinuationState decode(byte[] payload) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update("tapstate/latest-payload/v3\0".getBytes(StandardCharsets.UTF_8));
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(3).array());
        return LatestObservationPayloadCodec.decodeContinuationInline(payload, digest.digest(payload), payload.length);
    }

    private static byte[] payload(int facts, int points, int groups, String owner, boolean unsupported) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(0x54534c43); out.writeInt(3); string(out, PIPE); out.writeInt(facts);
            for (int fact = 0; fact < facts; fact++) {
                string(out, RECORDS); string(out, "COUNTER"); string(out, "{record}"); out.writeInt(points);
                for (int point = 0; point < points; point++) {
                    Map<String, String> attributes = new LinkedHashMap<>();
                    attributes.put(MetricAttributes.PIPELINE_ID, owner);
                    attributes.put(unsupported ? "row.id" : MetricAttributes.TABLE_ID, "table-" + point);
                    attributes.put(MetricAttributes.DIRECTION, "out");
                    out.writeInt(attributes.size());
                    for (var attribute : attributes.entrySet()) { string(out, attribute.getKey()); string(out, attribute.getValue()); }
                    out.writeBoolean(true); instant(out, START); instant(out, START.plusSeconds(10));
                    out.writeBoolean(true); out.writeLong(7);
                }
            }
            out.writeInt(groups);
            for (int group = 0; group < groups; group++) {
                string(out, RECORDS); string(out, "COUNTER"); string(out, "{record}");
                string(out, "out"); string(out, ""); instant(out, START.plusSeconds(5));
                out.writeInt(0); out.writeInt(0);
            }
        }
        return bytes.toByteArray();
    }

    private static void string(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeLong(bytes.length); out.write(bytes);
    }

    private static void instant(DataOutputStream out, Instant at) throws IOException {
        out.writeLong(at.getEpochSecond()); out.writeInt(at.getNano());
    }
}
