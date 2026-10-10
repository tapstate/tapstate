package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.BsonBinary;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class BenchmarkNativeOperationWallEvidenceTest {
    @Test
    void threeExactPointsPreserveOriginalRolesNativeTypesBytesAndNegativeNanoBrackets() {
        var clock = new AtomicLong(-1_000_000); var calls = new ArrayList<BenchmarkNativeOperationWallEvidence.Query>();
        Map<String, Object> result = BenchmarkNativeOperationWallEvidence.lookup(snapshot(), query -> {
            calls.add(query);
            assertThat(query.limit()).isEqualTo(2); assertThat(query.maxTimeMillis()).isEqualTo(500);
            assertThat(query.nativeOperation()).isEqualTo("u"); assertThat(query.filter().containsKey("op")).isFalse();
            assertThat(query.projection().containsKey("o.applyOps")).isFalse();
            return read(clock, List.of(nativeDocument(nativeBson(query, decodedWall(query.increment()), query.increment()))));
        }, () -> clock.getAndAdd(100));
        assertThat(calls).hasSize(3); assertThat(result).containsEntry("status", "COMPLETE_COMPARISON").containsEntry("lookupCount", 3)
                .containsEntry("cause", "UNKNOWN").containsEntry("performanceAcceptanceEligible", false);
        assertThat(pairs(result)).hasSize(3);
        for (Map<String, Object> pair : pairs(result)) { assertThat(pair).containsEntry("wallComparison", "PERSISTED_WALL_MATCHES_DECODED_EVENT").containsEntry("cause", "UNKNOWN"); }
        assertThat(queryResults(result).getFirst().get("projectedResponseSha256")).isInstanceOf(String.class);
        assertThat(queryResults(result).getFirst().get("originalProjectedBsonBase64")).isInstanceOf(String.class);
        assertThat(((Number) queryResults(result).getFirst().get("driverStartedNanos")).longValue()).isNegative();
        assertThat(result.get("decodedOperationClockEvidence")).isEqualTo(snapshot());
    }

    @Test
    void nativeWallDifferenceIsRetainedWithoutChangingClockOrCauseQualification() {
        var clock = new AtomicLong(10); Map<String, Object> result = BenchmarkNativeOperationWallEvidence.lookup(snapshot(), query ->
                read(clock, List.of(nativeDocument(nativeBson(query, decodedWall(query.increment()) + 1, query.increment())))), () -> clock.getAndAdd(100));
        assertThat(result).containsEntry("status", "COMPLETE_COMPARISON").containsEntry("clockQualification", "NOT_EVALUATED");
        assertThat(pairs(result).getFirst()).containsEntry("wallComparison", "DIFFERENCE").containsEntry("cause", "UNKNOWN");
    }

    @Test
    void wrongNativeTimestampNamespaceWallTypeUuidOperationOrIdentityStopsAfterOneAttempt() {
        for (String broken : List.of("ts", "namespace", "wall type", "uuid", "operation", "id", "id type")) {
            var clock = new AtomicLong(10); AtomicInteger calls = new AtomicInteger();
            Map<String, Object> result = BenchmarkNativeOperationWallEvidence.lookup(snapshot(), query -> {
                calls.incrementAndGet(); BsonDocument doc = nativeBson(query, decodedWall(query.increment()), query.increment());
                switch (broken) {
                    case "ts" -> doc.put("ts", new BsonTimestamp(21, 1));
                    case "namespace" -> doc.put("ns", new BsonString("foreign.orders"));
                    case "wall type" -> doc.put("wall", new BsonInt64(1000));
                    case "uuid" -> doc.put("ui", new BsonBinary((byte) 0, new byte[16]));
                    case "operation" -> doc.put("op", new BsonString("c"));
                    case "id" -> doc.put("o2", new BsonDocument("_id", new BsonInt64(99)));
                    case "id type" -> doc.put("o2", new BsonDocument("_id", new org.bson.BsonInt32(1)));
                    default -> throw new AssertionError("unhandled fixture");
                }
                return read(clock, List.of(nativeDocument(doc)));
            }, () -> clock.getAndAdd(100));
            assertThat(calls).hasValue(1); assertThat(result).containsEntry("status", "UNKNOWN").containsEntry("lookupCount", 1);
            assertThat(queryResults(result)).hasSize(1); assertThat(pairs(result)).isEmpty();
        }
    }

    @Test
    void humanKeyOrOpaqueTokenNeverSuppliesAnAbsentActualDocumentId() {
        Map<String, Object> input = snapshot();
        for (Map<String, Object> record : decodedRecords(input)) { Map<String, Object> event = metadata(record); event.remove("documentKey"); event.put("key", "human_key_does_not_match_999"); }
        var clock = new AtomicLong(10);
        var result = BenchmarkNativeOperationWallEvidence.lookup(input, query ->
                read(clock, List.of(nativeDocument(nativeBson(query, decodedWall(query.increment()), 999)))), () -> clock.getAndAdd(100));
        assertThat(result).containsEntry("status", "COMPLETE_COMPARISON");
        assertThat(pairs(result).getFirst()).containsEntry("documentKeyConstraint", "NOT_SUPPLIED");
        for (Map<String, Object> record : decodedRecords(input)) { metadata(record).put("documentKey", Map.of("status", "MISSING")); }
        var missing = BenchmarkNativeOperationWallEvidence.lookup(input, query ->
                read(clock, List.of(nativeDocument(nativeBson(query, decodedWall(query.increment()), 999)))), () -> clock.getAndAdd(100));
        assertThat(missing).containsEntry("status", "COMPLETE_COMPARISON");
    }

    @Test
    void duplicatesMissingNativeFieldsOrNoResultNeverBecomeACompleteMatch() {
        for (String broken : List.of("duplicate", "empty", "missing")) {
            var clock = new AtomicLong(10); AtomicInteger calls = new AtomicInteger();
            var result = BenchmarkNativeOperationWallEvidence.lookup(snapshot(), query -> {
                calls.incrementAndGet(); BsonDocument doc = nativeBson(query, decodedWall(query.increment()), query.increment());
                if (broken.equals("missing")) { doc.remove("wall"); }
                var one = nativeDocument(doc);
                return read(clock, broken.equals("duplicate") ? List.of(one, one) : broken.equals("empty") ? List.of() : List.of(one));
            }, () -> clock.getAndAdd(100));
            assertThat(result).containsEntry("status", "UNKNOWN").containsEntry("lookupCount", 1); assertThat(calls).hasValue(1);
            assertThat(queryResults(result).getFirst().get("responseCount")).isEqualTo(broken.equals("duplicate") ? 2 : broken.equals("empty") ? 0 : 1);
        }
    }

    @Test
    void retryableWriteMarkersAreRetainedWhileExplicitTransactionLayoutIsUnknown() {
        var clock = new AtomicLong(10);
        var retryable = BenchmarkNativeOperationWallEvidence.lookup(snapshot(), query -> {
            BsonDocument doc = nativeBson(query, decodedWall(query.increment()), query.increment());
            doc.put("lsid", new BsonDocument("id", new BsonBinary((byte) 4, new byte[16]))); doc.put("txnNumber", new BsonInt64(5));
            doc.put("prevOpTime", new BsonDocument("ts", new BsonTimestamp(19, 1))); return read(clock, List.of(nativeDocument(doc)));
        }, () -> clock.getAndAdd(100));
        assertThat(retryable).containsEntry("status", "COMPLETE_COMPARISON");
        assertThat((String) queryResults(retryable).getFirst().get("projectedNativeBson")).contains("lsid", "txnNumber", "prevOpTime");
        var refused = BenchmarkNativeOperationWallEvidence.lookup(snapshot(), query -> {
            BsonDocument doc = nativeBson(query, decodedWall(query.increment()), query.increment()); doc.put("partialTxn", org.bson.BsonBoolean.TRUE);
            return read(clock, List.of(nativeDocument(doc)));
        }, () -> clock.getAndAdd(100));
        assertThat(refused).containsEntry("status", "UNKNOWN").containsEntry("lookupCount", 1);
    }

    @Test
    void invalidScopeRolesStateFractionsOrMissingFieldsInvokeNeitherLookupNorClock() {
        for (String broken : List.of("scope", "role", "state", "fraction", "missing", "recorder")) {
            Map<String, Object> input = snapshot();
            switch (broken) {
                case "scope" -> metadata(decodedRecords(input).getFirst()).put("namespace", "foreign.orders");
                case "role" -> input.put("roles", Map.of("highWater", Map.of("status", "RECORDED", "index", 3), "previousAccepted", Map.of("status", "RECORDED", "index", 1), "currentRejected", Map.of("status", "RECORDED", "index", 2)));
                case "state" -> input.put("state", "ACCEPTING");
                case "fraction" -> metadata(decodedRecords(input).getFirst()).put("wallTime", 1000.5);
                case "missing" -> metadata(decodedRecords(input).getFirst()).remove("observedNanos");
                case "recorder" -> input.put("recorderFailure", "incomplete decoder");
                default -> throw new AssertionError("unhandled fixture");
            }
            AtomicInteger calls = new AtomicInteger(), clocks = new AtomicInteger();
            var result = BenchmarkNativeOperationWallEvidence.lookup(input, query -> { calls.incrementAndGet(); throw new AssertionError("must not query invalid input"); }, () -> { clocks.incrementAndGet(); return 10; });
            assertThat(result).containsEntry("status", "UNKNOWN").containsEntry("lookupCount", 0); assertThat(calls).hasValue(0); assertThat(clocks).hasValue(0);
        }
    }

    @Test
    void repeatedExactPointQueriesOnceButKeepsEveryOriginalEventAndRoleReference() {
        Map<String, Object> input = snapshot();
        Map<String, Object> prior = metadata(decodedRecords(input).get(1)); prior.put("clusterTime", Map.of("seconds", 20L, "increment", 1L)); prior.put("wallTime", 1000L); prior.put("documentKey", "{\"_id\":{\"$numberLong\":\"1\"}}");
        AtomicInteger calls = new AtomicInteger(); var clock = new AtomicLong(10);
        var result = BenchmarkNativeOperationWallEvidence.lookup(input, query -> { calls.incrementAndGet(); return read(clock, List.of(nativeDocument(nativeBson(query, decodedWall(query.increment()), query.increment())))); }, () -> clock.getAndAdd(100));
        assertThat(result).containsEntry("status", "COMPLETE_COMPARISON").containsEntry("lookupCount", 2); assertThat(calls).hasValue(2); assertThat(pairs(result)).hasSize(3);
        assertThat(pairs(result).get(0).get("nativeQueryIndex")).isEqualTo(pairs(result).get(1).get("nativeQueryIndex"));
    }

    @Test
    void oversizeBytesTypedByteMismatchAndCombinedPerEventBudgetRemainUnknownWithActualAttemptCount() {
        for (String broken : List.of("raw size", "typed mismatch", "combined size")) {
            var clock = new AtomicLong(10); var result = BenchmarkNativeOperationWallEvidence.lookup(snapshot(), query -> {
                BsonDocument doc = nativeBson(query, decodedWall(query.increment()), query.increment());
                if (broken.equals("raw size")) { return read(clock, List.of(new BenchmarkNativeOperationWallEvidence.NativeDocument(doc, new byte[65537]))); }
                if (broken.equals("typed mismatch")) { BsonDocument foreign = doc.clone(); foreign.put("wall", new BsonDateTime(123)); return read(clock, List.of(new BenchmarkNativeOperationWallEvidence.NativeDocument(doc, encode(foreign)))); }
                doc.put("lsid", new BsonDocument("boundedProjectedField", new BsonString("x".repeat(12000))));
                return read(clock, List.of(nativeDocument(doc)));
            }, () -> clock.getAndAdd(100));
            assertThat(result).containsEntry("status", "UNKNOWN").containsEntry("lookupCount", 1); assertThat(pairs(result)).isEmpty();
        }
    }

    @Test
    void deadlineAndMalformedQueryReadBracketsStopWithoutRetryOrInventedZeroCommands() {
        var clock = new AtomicLong(10); AtomicInteger calls = new AtomicInteger();
        var late = BenchmarkNativeOperationWallEvidence.lookup(snapshot(), query -> {
            calls.incrementAndGet(); clock.set(query.deadlineNanos() + 1);
            return new BenchmarkNativeOperationWallEvidence.ReadResult(List.of(nativeDocument(nativeBson(query, decodedWall(query.increment()), query.increment()))), 20, 30, null);
        }, () -> clock.getAndAdd(100));
        assertThat(late).containsEntry("status", "UNKNOWN").containsEntry("lookupCount", 1); assertThat(calls).hasValue(1);
        var normal = new AtomicLong(10);
        var wrong = BenchmarkNativeOperationWallEvidence.lookup(snapshot(), query -> new BenchmarkNativeOperationWallEvidence.ReadResult(List.of(nativeDocument(nativeBson(query, decodedWall(query.increment()), query.increment()))), 300, 200, null), () -> normal.getAndAdd(100));
        assertThat(wrong).containsEntry("status", "UNKNOWN").containsEntry("lookupCount", 1);
    }

    @Test
    void nativeFailureAndCompletionClockFailureCannotReplaceTheOriginalAssertion() {
        AssertionError original = new AssertionError("original hard clock refusal"); Error readFailure = new AssertionError("owned lookup failed"); Error clockFailure = new AssertionError("completion clock unavailable");
        AtomicInteger clocks = new AtomicInteger();
        var result = BenchmarkNativeOperationWallEvidence.lookup(snapshot(), query -> { throw readFailure; }, () -> { int call = clocks.incrementAndGet(); if (call == 3) { throw clockFailure; } return call * 100L; }, original);
        assertThat(result).containsEntry("status", "UNKNOWN").containsEntry("lookupCount", 1);
        assertThat(original.getMessage()).isEqualTo("original hard clock refusal"); assertThat(original.getSuppressed()).contains(readFailure);
        assertThat(readFailure.getSuppressed()).contains(clockFailure);
        assertThat(queryResults(result).getFirst().get("driverCompletedNanos")).isEqualTo(Map.of("status", "MISSING"));
    }

    @Test
    void resultDeepFreezesDecodedScopeNativeProjectionAndRoleLists() {
        Map<String, Object> input = snapshot(); var clock = new AtomicLong(10);
        var result = BenchmarkNativeOperationWallEvidence.lookup(input, query -> read(clock, List.of(nativeDocument(nativeBson(query, decodedWall(query.increment()), query.increment())))), () -> clock.getAndAdd(100));
        metadata(decodedRecords(input).getFirst()).put("wallTime", 55L); input.put("state", "mutated");
        assertThat(((Map<?, ?>) result.get("decodedOperationClockEvidence")).get("state")).isEqualTo("REJECTED");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> pairs(result).clear()).isInstanceOf(UnsupportedOperationException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> result.clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    private static long decodedWall(long increment) { return increment == 1 ? 1000 : increment == 2 ? 999 : 996; }
    private static BenchmarkNativeOperationWallEvidence.ReadResult read(AtomicLong clock, List<BenchmarkNativeOperationWallEvidence.NativeDocument> documents) { return new BenchmarkNativeOperationWallEvidence.ReadResult(documents, clock.getAndAdd(100), clock.getAndAdd(100), null); }
    private static BenchmarkNativeOperationWallEvidence.NativeDocument nativeDocument(BsonDocument doc) { return new BenchmarkNativeOperationWallEvidence.NativeDocument(doc, encode(doc)); }
    private static byte[] encode(BsonDocument doc) {
        try (BasicOutputBuffer output = new BasicOutputBuffer(); BsonBinaryWriter writer = new BsonBinaryWriter(output)) {
            new BsonDocumentCodec().encode(writer, doc, EncoderContext.builder().build()); return output.toByteArray();
        }
    }
    private static BsonDocument nativeBson(BenchmarkNativeOperationWallEvidence.Query query, long wall, long id) {
        return new BsonDocument("ts", query.timestamp()).append("wall", new BsonDateTime(wall)).append("ns", new BsonString(query.namespace()))
                .append("op", new BsonString("u")).append("ui", new BsonBinary((byte) 4, new byte[16])).append("o2", new BsonDocument("_id", new BsonInt64(id)));
    }
    private static Map<String, Object> snapshot() {
        List<Map<String, Object>> records = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            Map<String, Object> event = new LinkedHashMap<>(); event.put("namespace", "owned.orders"); event.put("targetId", "owned-target"); event.put("phaseId", "measured");
            event.put("operationType", "UPDATE"); event.put("key", "human-key-" + i); event.put("clusterTime", Map.of("seconds", 20L, "increment", (long) i)); event.put("wallTime", decodedWall(i));
            event.put("startedReadNanos", 10L); event.put("completedReadNanos", 20L); event.put("observedNanos", 20L); event.put("acceptedNanos", i == 3 ? null : 30L);
            event.put("resumeToken", "{\"_data\":\"opaque-token\"}"); event.put("documentKey", "{\"_id\":{\"$numberLong\":\"" + i + "\"}}");
            records.add(Map.of("eventOrdinal", (long) i, "accepted", i != 3, "metadata", event));
        }
        Map<String, Object> out = new LinkedHashMap<>(); out.put("format", "OPERATION_CLOCK_EVIDENCE_V1"); out.put("state", "REJECTED"); out.put("recorderFailure", null);
        out.put("scope", Map.of("namespace", "owned.orders", "targetId", "owned-target", "phaseId", "measured")); out.put("records", records); out.put("acceptedEvents", 2L);
        out.put("highWaterTiePolicy", "FIRST_ACCEPTED_MAX_WALL_TIME"); out.put("rejectionReason", "original four millisecond refusal");
        out.put("roles", Map.of("highWater", Map.of("status", "RECORDED", "index", 0), "previousAccepted", Map.of("status", "RECORDED", "index", 1), "currentRejected", Map.of("status", "RECORDED", "index", 2)));
        return out;
    }
    @SuppressWarnings("unchecked") private static List<Map<String, Object>> pairs(Map<String, Object> out) { return (List<Map<String, Object>>) out.get("eventPairs"); }
    @SuppressWarnings("unchecked") private static List<Map<String, Object>> queryResults(Map<String, Object> out) { return (List<Map<String, Object>>) out.get("queries"); }
    @SuppressWarnings("unchecked") private static List<Map<String, Object>> decodedRecords(Map<String, Object> out) { return (List<Map<String, Object>>) out.get("records"); }
    @SuppressWarnings("unchecked") private static Map<String, Object> metadata(Map<String, Object> record) { return (Map<String, Object>) record.get("metadata"); }
}
