package io.tapstate.e2e;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

/** Prospective bounded lookup of persisted operation metadata; it never evaluates a clock gate. */
final class BenchmarkNativeOperationWallEvidence {
    static final long MAX_RECORD_BYTES = 65536, MAX_EVIDENCE_BYTES = 2L * 1024 * 1024;
    static final long MAX_TIME_MILLIS = 500, OVERALL_NANOS = 6_000_000_000L;
    private static final Set<String> ROLE_NAMES = Set.of("highWater", "previousAccepted", "currentRejected");
    private static final Map<String, Object> MISSING = Map.of("status", "MISSING");
    private BenchmarkNativeOperationWallEvidence() { }

    interface Lookup { ReadResult read(Query query) throws Exception; }

    record Query(String namespace, long seconds, long increment, long deadlineNanos) {
        BsonTimestamp timestamp() { return new BsonTimestamp((int) seconds, (int) increment); }
        String nativeOperation() { return "u"; }
        int limit() { return 2; }
        long maxTimeMillis() { return MAX_TIME_MILLIS; }
        BsonDocument filter() {
            BsonTimestamp point = timestamp();
            return new BsonDocument("ns", new BsonString(namespace)).append("ts",
                    new BsonDocument("$gte", point).append("$lte", point));
        }
        BsonDocument projection() {
            BsonDocument out = new BsonDocument();
            for (String field : List.of("ts", "wall", "ns", "op", "ui", "o2._id", "o._id",
                    "lsid", "txnNumber", "prevOpTime", "partialTxn", "prepare")) { out.append(field, new org.bson.BsonInt32(1)); }
            out.append("_id", new org.bson.BsonInt32(0)); return out;
        }
        Map<String, Object> evidence() {
            return Map.of("namespace", namespace, "seconds", seconds, "increment", increment,
                    "range", "EXACT_INCLUSIVE_POINT", "expectedNativeOperation", nativeOperation(), "limit", limit(),
                    "maxTimeMillis", maxTimeMillis(), "deadlineNanos", deadlineNanos,
                    "filterBson", filter().toJson(), "projectionBson", projection().toJson());
        }
    }

    /** Bytes must be the original projected BSON response, rather than a re-encoding used as provenance. */
    static final class NativeDocument {
        private final BsonDocument document;
        private final byte[] bytes;
        NativeDocument(BsonDocument document, byte[] originalProjectedBytes) {
            require(document != null && originalProjectedBytes != null && originalProjectedBytes.length <= MAX_RECORD_BYTES,
                    "native projected document missing or oversized");
            this.bytes = originalProjectedBytes.clone();
            RawBsonDocument raw = new RawBsonDocument(this.bytes);
            require(sameBson(raw, document), "typed native document differs from original projected BSON");
            this.document = raw.clone();
        }
    }

    record ReadResult(List<NativeDocument> documents, long startedReadNanos, long completedReadNanos, String unknownReason) {
        ReadResult {
            require(documents != null && documents.size() <= 2, "query result count exceeds fixed limit");
            documents = List.copyOf(documents);
            require(unknownReason == null || unknownReason.length() <= 1024, "query reason bound exceeded");
        }
    }

    private record Event(int index, long ordinal, Map<String, Object> metadata, long seconds, long increment,
            long wall, BsonValue actualDocumentId) { }
    private record Validated(Map<String, Object> decoded, List<Event> events, Map<String, List<String>> roles) { }

    static Map<String, Object> lookup(Map<String, Object> decoded, Lookup lookup, LongSupplier clock) {
        return lookup(decoded, lookup, clock, null);
    }

    /** Failure retention cannot replace the caller's original clock assertion or confer acceptance. */
    static Map<String, Object> lookup(Map<String, Object> decoded, Lookup lookup, LongSupplier clock, AssertionError original) {
        Objects.requireNonNull(lookup); Objects.requireNonNull(clock);
        Validated input;
        try { input = validate(decoded); }
        catch (Throwable invalid) { suppress(original, invalid); return unknown(null, List.of(), List.of(), "INPUT_INVALID", invalid); }
        List<Map<String, Object>> queries = new ArrayList<>(3), pairs = new ArrayList<>(3);
        Map<String, Integer> queryIndexes = new LinkedHashMap<>();
        List<BsonDocument> nativeDocuments = new ArrayList<>(3);
        try {
            // Check the combined decoded/envelope budget before invoking even the first lookup.
            output(input.decoded(), queries, pairs, "UNKNOWN", "LOOKUP_NOT_FINISHED", null);
            long anchor = clock.getAsLong(), deadline = Math.addExact(anchor, OVERALL_NANOS);
            for (Event event : input.events()) {
                String namespace = (String) event.metadata().get("namespace");
                String identity = namespace + ":" + event.seconds() + ":" + event.increment();
                Integer queryIndex = queryIndexes.get(identity);
                BsonDocument nativeDocument;
                Map<String, Object> nativeEvidence;
                if (queryIndex == null) {
                    require(queries.size() < 3, "query count exceeds three exact points");
                    Query query = new Query(namespace, event.seconds(), event.increment(), deadline);
                    long before = clock.getAsLong(); remaining(before, anchor, deadline);
                    queryIndex = queries.size();
                    Map<String, Object> pending = attempt(query, event, before, null, null, "INVOKED_COMPLETION_UNKNOWN", null);
                    require(bytes(((List<?>) input.decoded().get("records")).get(event.index()), 0) + bytes(pending, 0)
                            <= MAX_RECORD_BYTES, "combined decoded and query attempt budget exceeded");
                    List<Map<String, Object>> prospective = new ArrayList<>(queries); prospective.add(pending);
                    output(input.decoded(), prospective, pairs, "UNKNOWN", "LOOKUP_IN_PROGRESS", null);
                    queries.add(pending);
                    ReadResult result;
                    try { result = lookup.read(query); }
                    catch (Throwable queryFailure) {
                        Long afterFailure = null;
                        try { afterFailure = clock.getAsLong(); }
                        catch (Throwable clockFailure) { if (clockFailure != queryFailure) { queryFailure.addSuppressed(clockFailure); } }
                        queries.set(queryIndex, attempt(query, event, before, afterFailure, null, "CALL_FAILED", queryFailure));
                        throw queryFailure;
                    }
                    Long after = null;
                    try { after = clock.getAsLong(); }
                    finally { queries.set(queryIndex, attempt(query, event, before, after, result, "RETURNED_UNVALIDATED", null)); }
                    remaining(after, anchor, deadline);
                    require(result != null && result.unknownReason() == null, "native query unavailable");
                    ordered(before, result.startedReadNanos()); ordered(result.startedReadNanos(), result.completedReadNanos()); ordered(result.completedReadNanos(), after);
                    require(result.documents().size() == 1, "native query missing or duplicate exact point");
                    NativeDocument supplied = result.documents().getFirst();
                    nativeDocument = supplied.document;
                    validateNative(nativeDocument, event);
                    nativeEvidence = nativeEvidence(query, result, before, after, supplied, nativeDocument);
                } else {
                    nativeDocument = nativeDocuments.get(queryIndex); validateNative(nativeDocument, event);
                    nativeEvidence = queries.get(queryIndex);
                }
                Map<String, Object> pair = Map.of("decodedRecordIndex", event.index(), "eventOrdinal", event.ordinal(),
                        "roles", input.roles().get(Integer.toString(event.index())), "nativeQueryIndex", queryIndex,
                        "wallComparison", nativeDocument.getDateTime("wall").getValue() == event.wall()
                                ? "PERSISTED_WALL_MATCHES_DECODED_EVENT" : "DIFFERENCE",
                        "documentKeyConstraint", event.actualDocumentId() == null ? "NOT_SUPPLIED" : "ACTUAL_ID_MATCHED",
                        "cause", "UNKNOWN");
                require(bytes(input.decoded().get("records") instanceof List<?> list ? list.get(event.index()) : null, 0)
                        + bytes(nativeEvidence, 0) + bytes(pair, 0) <= MAX_RECORD_BYTES, "combined decoded and native event record budget exceeded");
                List<Map<String, Object>> candidateQueries = new ArrayList<>(queries);
                if (!queryIndexes.containsKey(identity)) { candidateQueries.set(queryIndex, nativeEvidence); }
                List<Map<String, Object>> candidatePairs = new ArrayList<>(pairs); candidatePairs.add(pair);
                output(input.decoded(), candidateQueries, candidatePairs, "COMPLETE_COMPARISON", null, null);
                // Commit only a fully validated, jointly bounded result; never retry a refused point.
                if (!queryIndexes.containsKey(identity)) {
                    queryIndexes.put(identity, queryIndex); queries.set(queryIndex, nativeEvidence); nativeDocuments.add(nativeDocument);
                }
                pairs.add(pair);
            }
            return output(input.decoded(), queries, pairs, "COMPLETE_COMPARISON", null, null);
        } catch (Throwable unavailable) {
            suppress(original, unavailable);
            return unknown(input.decoded(), queries, pairs, "NATIVE_LOOKUP_UNKNOWN", unavailable);
        }
    }

    private static Validated validate(Map<String, Object> decoded) {
        require(decoded != null && bytes(decoded, 0) <= MAX_EVIDENCE_BYTES, "decoded evidence missing or oversized");
        require("OPERATION_CLOCK_EVIDENCE_V1".equals(decoded.get("format")) && "REJECTED".equals(decoded.get("state"))
                && decoded.containsKey("recorderFailure") && decoded.get("recorderFailure") == null, "decoded refusal is incomplete");
        require(decoded.get("scope") instanceof Map<?, ?> && decoded.get("records") instanceof List<?>
                && decoded.get("roles") instanceof Map<?, ?>, "decoded refusal shape missing");
        Map<?, ?> scope = (Map<?, ?>) decoded.get("scope"), roles = (Map<?, ?>) decoded.get("roles");
        require(scope.keySet().equals(Set.of("namespace", "targetId", "phaseId")) && roles.keySet().equals(ROLE_NAMES), "scope or role shape invalid");
        for (Object value : scope.values()) { require(value instanceof String text && !text.isBlank(), "scope value missing"); }
        List<?> records = (List<?>) decoded.get("records"); require(!records.isEmpty() && records.size() <= 3, "decoded record count invalid");
        Map<String, List<String>> selectedRoles = new LinkedHashMap<>();
        for (String role : List.of("highWater", "previousAccepted", "currentRejected")) {
            require(roles.get(role) instanceof Map<?, ?>, "role missing"); Map<?, ?> reference = (Map<?, ?>) roles.get(role);
            require(reference.keySet().equals(Set.of("status", "index")) && "RECORDED".equals(reference.get("status"))
                    && integral(reference.get("index")), "role must have a recorded integral index");
            long index = ((Number) reference.get("index")).longValue(); require(index >= 0 && index < records.size(), "role index out of range");
            selectedRoles.computeIfAbsent(Long.toString(index), ignored -> new ArrayList<>()).add(role);
        }
        require(selectedRoles.size() == records.size(), "unreferenced decoded record");
        List<Event> events = new ArrayList<>(3);
        Set<Long> ordinals = new java.util.HashSet<>();
        for (int index = 0; index < records.size(); index++) {
            require(records.get(index) instanceof Map<?, ?>, "decoded record malformed"); Map<?, ?> record = (Map<?, ?>) records.get(index);
            require(bytes(record, 0) <= MAX_RECORD_BYTES && integral(record.get("eventOrdinal"))
                    && ((Number) record.get("eventOrdinal")).longValue() > 0 && record.get("metadata") instanceof Map<?, ?>, "decoded event identity invalid");
            long ordinal = ((Number) record.get("eventOrdinal")).longValue(); require(ordinals.add(ordinal), "duplicate decoded ordinal");
            List<String> assigned = selectedRoles.get(Integer.toString(index));
            require(assigned.contains("currentRejected") ? Boolean.FALSE.equals(record.get("accepted")) && assigned.size() == 1
                    : Boolean.TRUE.equals(record.get("accepted")), "role gate result invalid");
            Map<String, Object> metadata = stringMap((Map<?, ?>) record.get("metadata"));
            for (String key : List.of("namespace", "targetId", "phaseId")) { require(scope.get(key).equals(metadata.get(key)), "decoded event scope mismatch"); }
            require("UPDATE".equals(metadata.get("operationType")) || "update".equals(metadata.get("operationType")), "only recorded direct update events are eligible");
            require(metadata.get("clusterTime") instanceof Map<?, ?>, "native timestamp absent"); Map<?, ?> timestamp = (Map<?, ?>) metadata.get("clusterTime");
            require(timestamp.keySet().equals(Set.of("seconds", "increment")) && integral(timestamp.get("seconds")) && integral(timestamp.get("increment")) && integral(metadata.get("wallTime")), "decoded native timestamp or wall type invalid");
            long seconds = ((Number) timestamp.get("seconds")).longValue(), increment = ((Number) timestamp.get("increment")).longValue();
            require(seconds >= 0 && seconds <= 0xffffffffL && increment >= 0 && increment <= 0xffffffffL, "native timestamp range invalid");
            for (String key : List.of("key", "startedReadNanos", "completedReadNanos", "observedNanos", "acceptedNanos")) { require(metadata.containsKey(key), "decoded required field absent"); }
            for (String key : List.of("startedReadNanos", "completedReadNanos", "observedNanos")) { require(integral(metadata.get(key)), "decoded read bracket unavailable"); }
            ordered(((Number) metadata.get("startedReadNanos")).longValue(), ((Number) metadata.get("completedReadNanos")).longValue());
            require(((Number) metadata.get("completedReadNanos")).longValue() == ((Number) metadata.get("observedNanos")).longValue(), "decoded read completion mismatch");
            if (Boolean.TRUE.equals(record.get("accepted"))) { require(integral(metadata.get("acceptedNanos")), "decoded accept time absent"); ordered(((Number) metadata.get("observedNanos")).longValue(), ((Number) metadata.get("acceptedNanos")).longValue()); }
            else { require(metadata.get("acceptedNanos") == null || MISSING.equals(metadata.get("acceptedNanos")), "refused event cannot have acceptance time"); }
            BsonValue id = null;
            if (metadata.containsKey("documentKey") && !MISSING.equals(metadata.get("documentKey"))) {
                require(metadata.get("documentKey") instanceof String text && !text.isBlank(), "actual document key must be textual BSON or missing");
                BsonDocument key = BsonDocument.parse((String) metadata.get("documentKey")); require(key.containsKey("_id"), "actual document key lacks id"); id = key.get("_id");
            }
            events.add(new Event(index, ordinal, freezeMap(metadata), seconds, increment, ((Number) metadata.get("wallTime")).longValue(), id));
        }
        require(integral(decoded.get("acceptedEvents")) && ((Number) decoded.get("acceptedEvents")).longValue() > 0, "accepted event count unavailable");
        Event high = events.stream().filter(event -> selectedRoles.get(Integer.toString(event.index())).contains("highWater")).findFirst().orElseThrow();
        Event prior = events.stream().filter(event -> selectedRoles.get(Integer.toString(event.index())).contains("previousAccepted")).findFirst().orElseThrow();
        Event refusal = events.stream().filter(event -> selectedRoles.get(Integer.toString(event.index())).contains("currentRejected")).findFirst().orElseThrow();
        require(prior.ordinal() == ((Number) decoded.get("acceptedEvents")).longValue()
                && refusal.ordinal() == Math.addExact(prior.ordinal(), 1) && high.ordinal() <= prior.ordinal()
                && high.wall() >= prior.wall(), "decoded role history inconsistent");
        require("FIRST_ACCEPTED_MAX_WALL_TIME".equals(decoded.get("highWaterTiePolicy")), "high water tie policy unavailable");
        return new Validated(freezeMap(decoded), List.copyOf(events), Collections.unmodifiableMap(selectedRoles));
    }

    private static void validateNative(BsonDocument document, Event event) {
        require(document.containsKey("ts") && document.get("ts").isTimestamp() && document.containsKey("wall") && document.get("wall").isDateTime()
                && document.containsKey("ns") && document.get("ns").isString() && document.containsKey("op") && document.get("op").isString()
                && document.containsKey("ui") && document.get("ui").isBinary(), "native projected fields missing or wrong BSON types");
        BsonTimestamp ts = document.getTimestamp("ts");
        require(Integer.toUnsignedLong(ts.getTime()) == event.seconds() && Integer.toUnsignedLong(ts.getInc()) == event.increment()
                && document.getString("ns").getValue().equals(event.metadata().get("namespace")), "native exact point or namespace mismatch");
        require("u".equals(document.getString("op").getValue()), "native operation is not direct update");
        require(document.getBinary("ui").getType() == 4 && document.getBinary("ui").getData().length == 16, "native collection UUID type invalid");
        for (String marker : List.of("partialTxn", "prepare")) {
            if (document.containsKey(marker)) { require(document.get(marker).isBoolean() && !document.getBoolean(marker).getValue(), "native transaction layout unsupported"); }
        }
        if (document.get("o") instanceof BsonDocument operation) { require(!operation.containsKey("applyOps"), "native applyOps layout unsupported"); }
        for (String field : List.of("o", "o2")) { if (document.containsKey(field)) { require(document.get(field).isDocument(), "native identity container type invalid"); } }
        BsonValue id = id(document);
        require(id != null, "native update identity missing");
        if (event.actualDocumentId() != null) { require(sameBson(event.actualDocumentId(), id), "native actual document id mismatch"); }
    }

    private static BsonValue id(BsonDocument document) {
        BsonValue one = document.get("o2") instanceof BsonDocument selector ? selector.get("_id") : null;
        BsonValue two = document.get("o") instanceof BsonDocument replacement ? replacement.get("_id") : null;
        require(one == null || two == null || sameBson(one, two), "native identity fields disagree"); return one != null ? one : two;
    }

    private static boolean sameBson(BsonValue one, BsonValue two) {
        if (one.getBsonType() != two.getBsonType()) { return false; }
        if (one.isDocument()) {
            var a = one.asDocument().entrySet().iterator(); var b = two.asDocument().entrySet().iterator();
            while (a.hasNext() && b.hasNext()) {
                var left = a.next(); var right = b.next();
                if (!left.getKey().equals(right.getKey()) || !sameBson(left.getValue(), right.getValue())) { return false; }
            }
            return !a.hasNext() && !b.hasNext();
        }
        if (one.isArray()) {
            List<BsonValue> a = one.asArray().getValues(), b = two.asArray().getValues();
            if (a.size() != b.size()) { return false; }
            for (int at = 0; at < a.size(); at++) { if (!sameBson(a.get(at), b.get(at))) { return false; } }
            return true;
        }
        return one.equals(two);
    }

    private static Map<String, Object> attempt(Query query, Event event, long before, Long after,
            ReadResult result, String state, Throwable problem) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("query", query.evidence()); out.put("decodedRecordIndex", event.index()); out.put("eventOrdinal", event.ordinal());
        out.put("attemptState", state); out.put("driverStartedNanos", before); out.put("driverCompletedNanos", after == null ? MISSING : after);
        out.put("queryStartedNanos", result == null ? MISSING : result.startedReadNanos());
        out.put("queryCompletedNanos", result == null ? MISSING : result.completedReadNanos());
        out.put("responseCount", result == null ? MISSING : result.documents().size());
        out.put("readUnknownReason", result == null ? MISSING : result.unknownReason());
        out.put("failureType", problem == null ? null : problem.getClass().getName()); out.put("cause", "UNKNOWN");
        return freezeMap(out);
    }

    private static Map<String, Object> nativeEvidence(Query query, ReadResult result, long before, long after,
            NativeDocument supplied, BsonDocument document) throws Exception {
        Map<String, Object> out = new LinkedHashMap<>(); out.put("query", query.evidence());
        out.put("driverStartedNanos", before); out.put("driverCompletedNanos", after);
        out.put("queryStartedNanos", result.startedReadNanos()); out.put("queryCompletedNanos", result.completedReadNanos());
        out.put("attemptState", "RETURNED_VALIDATED"); out.put("responseCount", 1); out.put("projectedResponseBytes", supplied.bytes.length);
        out.put("projectedResponseSha256", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(supplied.bytes)));
        out.put("originalProjectedBsonBase64", Base64.getEncoder().encodeToString(supplied.bytes));
        out.put("projectedNativeBson", document.toJson());
        out.put("nativeTimestamp", Map.of("bsonType", "TIMESTAMP", "seconds", query.seconds(), "increment", query.increment()));
        out.put("nativeWall", Map.of("bsonType", "DATE_TIME", "millis", document.getDateTime("wall").getValue()));
        out.put("nativeDocumentIdBson", new BsonDocument("_id", id(document)).toJson());
        out.put("cause", "UNKNOWN"); return freezeMap(out);
    }

    private static Map<String, Object> unknown(Map<String, Object> decoded, List<Map<String, Object>> queries,
            List<Map<String, Object>> pairs, String reason, Throwable problem) {
        try { return output(decoded, queries, pairs, "UNKNOWN", reason, problem); }
        catch (Throwable retention) {
            List<Map<String, Object>> attempts = new ArrayList<>();
            for (Map<String, Object> query : queries) {
                Map<String, Object> boundedAttempt = new LinkedHashMap<>();
                for (String key : List.of("query", "attemptState", "driverStartedNanos", "driverCompletedNanos",
                        "queryStartedNanos", "queryCompletedNanos", "responseCount")) {
                    boundedAttempt.put(key, query.containsKey(key) ? query.get(key) : MISSING);
                }
                boundedAttempt.put("nativeResponseEvidenceStatus", "UNKNOWN_NOT_RETAINED"); attempts.add(freezeMap(boundedAttempt));
            }
            return Map.of("format", "NATIVE_OPERATION_WALL_EVIDENCE_V1", "status", "UNKNOWN", "reason", "COMBINED_EVIDENCE_NOT_RETAINABLE",
                    "decodedEvidenceStatus", "UNKNOWN_NOT_RETAINED", "lookupCount", queries.size(), "queries", List.copyOf(attempts),
                    "cause", "UNKNOWN", "performanceAcceptanceEligible", false);
        }
    }
    private static Map<String, Object> output(Map<String, Object> decoded, List<Map<String, Object>> queries,
            List<Map<String, Object>> pairs, String status, String reason, Throwable problem) {
        Map<String, Object> out = new LinkedHashMap<>(); out.put("format", "NATIVE_OPERATION_WALL_EVIDENCE_V1"); out.put("status", status);
        out.put("decodedEvidenceStatus", decoded == null ? "UNKNOWN_NOT_RETAINED" : "RETAINED"); out.put("decodedOperationClockEvidence", decoded);
        out.put("queries", List.copyOf(queries)); out.put("eventPairs", List.copyOf(pairs)); out.put("lookupCount", queries.size());
        out.put("reason", reason); out.put("failureType", problem == null ? null : problem.getClass().getName());
        out.put("cause", "UNKNOWN"); out.put("clockQualification", "NOT_EVALUATED"); out.put("performanceAcceptanceEligible", false);
        require(bytes(out, 0) <= MAX_EVIDENCE_BYTES, "combined evidence envelope budget exceeded"); return Collections.unmodifiableMap(out);
    }
    private static void remaining(long now, long anchor, long deadline) { ordered(anchor, now); require(Math.subtractExact(deadline, now) > 0, "overall lookup deadline reached"); }
    private static void ordered(long first, long last) { require(Math.subtractExact(last, first) >= 0, "lookup read bracket moved backward or overflowed"); }
    private static void suppress(AssertionError original, Throwable recording) { if (original != null && original != recording) { original.addSuppressed(recording); } }
    private static boolean integral(Object value) { return value instanceof Long || value instanceof Integer; }
    private static Map<String, Object> stringMap(Map<?, ?> map) { Map<String, Object> copy = new LinkedHashMap<>(); map.forEach((k, v) -> { require(k instanceof String, "metadata key must be a string"); copy.put((String) k, v); }); return copy; }
    private static long bytes(Object value, int depth) {
        require(depth <= 16, "native evidence depth exceeded"); if (value == null || value instanceof Number || value instanceof Boolean) { return 16; }
        if (value instanceof String text) { return 4L * text.length() + 16; } long count = 32;
        if (value instanceof Map<?, ?> map) { require(map.size() <= 64, "native evidence map count exceeded"); for (var e : map.entrySet()) { count = Math.addExact(count, bytes(e.getKey(), depth + 1)); count = Math.addExact(count, bytes(e.getValue(), depth + 1)); if (count > MAX_EVIDENCE_BYTES) { return count; } } }
        else if (value instanceof Collection<?> list) { require(list.size() <= 512, "native evidence list count exceeded"); for (Object item : list) { count = Math.addExact(count, bytes(item, depth + 1)); if (count > MAX_EVIDENCE_BYTES) { return count; } } }
        else { throw new AssertionError("unsupported native evidence value"); } return count;
    }
    private static Map<String, Object> freezeMap(Map<String, Object> map) { @SuppressWarnings("unchecked") Map<String, Object> copy = (Map<String, Object>) freeze(map); return copy; }
    private static Object freeze(Object value) {
        if (value instanceof Map<?, ?> map) { Map<String, Object> copy = new LinkedHashMap<>(); map.forEach((k, v) -> { require(k instanceof String, "evidence key must be a string"); copy.put((String) k, freeze(v)); }); return Collections.unmodifiableMap(copy); }
        if (value instanceof Collection<?> list) { return list.stream().map(BenchmarkNativeOperationWallEvidence::freeze).toList(); }
        if (value == null || value instanceof String || value instanceof Integer || value instanceof Long || value instanceof Boolean || value instanceof Double) { return value; }
        throw new AssertionError("unsupported mutable evidence value");
    }
    private static void require(boolean value, String reason) { if (!value) { throw new AssertionError(reason); } }
}
