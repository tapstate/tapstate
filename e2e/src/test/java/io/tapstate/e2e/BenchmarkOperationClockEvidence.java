package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** A bounded history supplied after the authoritative operation-clock gate decides each event. */
final class BenchmarkOperationClockEvidence {
    static final long MAX_RECORD_BYTES = 65536, MAX_EVIDENCE_BYTES = 2L * 1024 * 1024;
    private static final Set<String> REQUIRED = Set.of("namespace", "targetId", "phaseId", "key", "operationType",
            "clusterTime", "wallTime", "startedReadNanos", "completedReadNanos", "observedNanos", "acceptedNanos");
    private static final Set<String> OPTIONAL = Set.of("resumeToken", "documentKey", "transaction", "session");
    private record Event(long ordinal, boolean accepted, Map<String, Object> metadata) {
        Map<String, Object> evidence() { return Map.of("eventOrdinal", ordinal, "accepted", accepted, "metadata", metadata); }
    }

    private String namespace, targetId, phaseId;
    private long acceptedEvents;
    private Event highWater, previous, rejected;
    private String rejectionReason, recorderFailure;
    private Map<String, Object> frozenFailure;

    BenchmarkOperationClockEvidence(String namespace, String targetId, String phaseId) { reset(namespace, targetId, phaseId); }

    /** Reset belongs only to the caller opening an actual new measured phase. */
    void reset(String namespace, String targetId, String phaseId) {
        name(namespace); name(targetId); name(phaseId);
        this.namespace = namespace; this.targetId = targetId; this.phaseId = phaseId;
        acceptedEvents = 0; highWater = null; previous = null; rejected = null;
        rejectionReason = null; recorderFailure = null; frozenFailure = null;
    }

    void accepted(Map<String, Object> metadata) {
        require(frozenFailure == null, "recording already ended");
        try {
            Map<String, Object> event = validated(metadata, true);
            Event candidate = new Event(Math.addExact(acceptedEvents, 1), true, event);
            Event nextHigh = highWater;
            if (integral(event.get("wallTime")) && (highWater == null
                    || ((Number) event.get("wallTime")).longValue() > ((Number) highWater.metadata().get("wallTime")).longValue())) {
                nextHigh = candidate;
            }
            snapshot(nextHigh, candidate, null, candidate.ordinal(), null, null);
            // None of the retained events or ordinals changes before the complete envelope fits.
            highWater = nextHigh; previous = candidate; acceptedEvents = candidate.ordinal();
        } catch (AssertionError | RuntimeException problem) { failed(problem); throw problem; }
    }

    /** Rejection does not advance either accepted role, regardless of the event's wall time. */
    void rejected(Map<String, Object> metadata, String reason) {
        require(frozenFailure == null, "recording already ended");
        try {
            name(reason);
            Event candidate = new Event(Math.addExact(acceptedEvents, 1), false, validated(metadata, false));
            Map<String, Object> snapshot = snapshot(highWater, previous, candidate, acceptedEvents, reason, null);
            rejected = candidate; rejectionReason = reason; frozenFailure = snapshot;
        } catch (AssertionError | RuntimeException problem) {
            if (reason != null && !reason.isBlank() && reason.length() <= 8192) { rejectionReason = reason; }
            failed(problem); throw problem;
        }
    }

    /** A convenience for callers that propagate the original rejection on this same thread. */
    void rejected(Map<String, Object> metadata, AssertionError original) {
        Objects.requireNonNull(original, "original");
        try { rejected(metadata, Objects.toString(original.getMessage(), original.getClass().getName())); }
        catch (Throwable recording) { if (recording != original) { original.addSuppressed(recording); } }
        throw original;
    }

    Map<String, Object> evidence() {
        return frozenFailure != null ? frozenFailure
                : snapshot(highWater, previous, rejected, acceptedEvents, rejectionReason, recorderFailure);
    }

    void recordingFailed(Throwable problem) { failed(Objects.requireNonNull(problem)); }

    void authoritativeRejectionAfterRecordingFailure(String reason) {
        name(reason);
        require(recorderFailure != null, "a recorder failure must already be retained");
        rejectionReason = reason;
        frozenFailure = snapshot(highWater, previous, rejected, acceptedEvents, reason, recorderFailure);
    }

    private void failed(Throwable problem) {
        if (frozenFailure != null) { return; }
        recorderFailure = problem.getClass().getSimpleName() + ":" + Objects.toString(problem.getMessage());
        // Failure descriptions are bounded independently; oversize metadata itself is never retained.
        if (recorderFailure.length() > 1024) { recorderFailure = problem.getClass().getSimpleName() + ":DESCRIPTION_BOUND_EXCEEDED"; }
        frozenFailure = snapshot(highWater, previous, rejected, acceptedEvents, rejectionReason, recorderFailure);
    }

    private Map<String, Object> snapshot(Event high, Event prior, Event refusal, long count, String reason, String failure) {
        List<Event> events = new ArrayList<>(3);
        if (high != null) { events.add(high); }
        if (prior != null && prior != high) { events.add(prior); }
        if (refusal != null && refusal != high && refusal != prior) { events.add(refusal); }
        Map<String, Object> roles = new LinkedHashMap<>();
        roles.put("highWater", role(high, events, count > 0 ? "UNKNOWN_NO_ACCEPTED_WALL" : "NOT_RECORDED"));
        roles.put("previousAccepted", role(prior, events, "NOT_RECORDED"));
        roles.put("currentRejected", role(refusal, events, failure != null ? "UNKNOWN_RECORDER_FAILED" : "NOT_RECORDED"));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("format", "OPERATION_CLOCK_EVIDENCE_V1");
        out.put("scope", Map.of("namespace", namespace, "targetId", targetId, "phaseId", phaseId));
        out.put("state", failure != null ? "RECORDER_FAILED" : refusal != null ? "REJECTED" : "ACCEPTING");
        out.put("highWaterTiePolicy", "FIRST_ACCEPTED_MAX_WALL_TIME"); out.put("acceptedEvents", count);
        out.put("records", events.stream().map(Event::evidence).toList()); out.put("roles", Collections.unmodifiableMap(roles));
        out.put("rejectionReason", reason); out.put("recorderFailure", failure);
        out.put("metadataRepresentation", "SUPPLIED_JAVA_DRIVER_DECODED_EVENT_METADATA");
        out.put("clockQualification", "NOT_EVALUATED_BY_RECORDER"); out.put("performanceAcceptanceEligible", false);
        require(bytes(out, 0) <= MAX_EVIDENCE_BYTES, "whole clock evidence byte budget exceeded");
        return Collections.unmodifiableMap(out);
    }

    private static Map<String, Object> role(Event event, List<Event> records, String missing) {
        return event == null ? Map.of("status", missing) : Map.of("status", "RECORDED", "index", records.indexOf(event));
    }

    private Map<String, Object> validated(Map<String, Object> raw, boolean accepted) {
        require(raw != null && raw.size() <= REQUIRED.size() + OPTIONAL.size(), "metadata field count exceeded");
        require(bytes(raw, 0) <= MAX_RECORD_BYTES, "metadata record byte budget exceeded");
        require(raw.keySet().containsAll(REQUIRED) && raw.keySet().stream().allMatch(key -> REQUIRED.contains(key) || OPTIONAL.contains(key)), "metadata fields missing or unknown");
        require(namespace.equals(raw.get("namespace")) && targetId.equals(raw.get("targetId")) && phaseId.equals(raw.get("phaseId")), "event scope mismatch");
        require(raw.get("key") != null && (!(raw.get("key") instanceof String text) || !text.isBlank()), "event key missing");
        require(raw.get("operationType") instanceof String operation && !operation.isBlank(), "operation type missing");
        Object cluster = raw.get("clusterTime");
        if (!missing(cluster)) {
            require(cluster instanceof Map<?, ?>, "native cluster timestamp must be structured");
            Map<?, ?> timestamp = (Map<?, ?>) cluster;
            require(timestamp.keySet().equals(Set.of("seconds", "increment")) && integral(timestamp.get("seconds")) && integral(timestamp.get("increment")), "native cluster timestamp malformed");
            for (Object number : timestamp.values()) { long value = ((Number) number).longValue(); require(value >= 0 && value <= 0xffffffffL, "native timestamp component out of range"); }
        }
        require(integral(raw.get("wallTime")) || missing(raw.get("wallTime")), "wall time missing or non-integral");
        for (String key : List.of("startedReadNanos", "completedReadNanos", "observedNanos")) { require(integral(raw.get(key)) || missing(raw.get(key)), "read timestamp missing or non-integral"); }
        require(accepted ? integral(raw.get("acceptedNanos")) : raw.get("acceptedNanos") == null || missing(raw.get("acceptedNanos")), "accept timestamp inconsistent with gate result");
        ordered(raw.get("startedReadNanos"), raw.get("completedReadNanos"), "read bracket moved backward or overflowed");
        if (integral(raw.get("observedNanos")) && integral(raw.get("completedReadNanos"))) {
            require(((Number) raw.get("observedNanos")).longValue() == ((Number) raw.get("completedReadNanos")).longValue(), "observation must equal actual read completion");
        }
        if (accepted) { ordered(raw.get("observedNanos"), raw.get("acceptedNanos"), "accept timestamp precedes observation or overflowed"); }
        if (raw.containsKey("resumeToken")) { require(missing(raw.get("resumeToken")) || raw.get("resumeToken") instanceof String token && !token.isBlank(), "resume token must be actual textual BSON or explicit missing"); }
        if (raw.containsKey("documentKey")) { require(missing(raw.get("documentKey")) || raw.get("documentKey") instanceof String key && !key.isBlank(), "document key must be actual textual BSON or explicit missing"); }
        for (String key : List.of("transaction", "session")) { if (raw.containsKey(key)) { require(raw.get(key) != null && !missing(raw.get(key)), "optional transaction or session must actually be present"); } }
        return freezeMap(raw, 0);
    }

    private static void ordered(Object first, Object last, String reason) {
        if (!integral(first) || !integral(last)) { return; }
        try { require(Math.subtractExact(((Number) last).longValue(), ((Number) first).longValue()) >= 0, reason); }
        catch (ArithmeticException overflow) { throw new AssertionError(reason, overflow); }
    }

    private static boolean missing(Object value) { return value instanceof Map<?, ?> map && map.equals(Map.of("status", "MISSING")); }
    private static boolean integral(Object value) { return value instanceof Long || value instanceof Integer; }
    private static void name(String value) { require(value != null && !value.isBlank() && value.length() <= 8192, "nonempty bounded value required"); }

    /** The same logical object accounting used by bounded native evidence, independent of JSON encoding. */
    private static long bytes(Object value, int depth) {
        require(depth <= 12, "clock metadata depth exceeded");
        if (value == null || value instanceof Number || value instanceof Boolean) { return 16; }
        if (value instanceof String text) { return 4L * text.length() + 16; }
        long count = 32;
        if (value instanceof Map<?, ?> map) {
            require(map.size() <= 64, "clock metadata map count exceeded");
            for (var entry : map.entrySet()) { require(entry.getKey() instanceof String, "clock metadata key must be a string"); count = Math.addExact(count, bytes(entry.getKey(), depth + 1)); count = Math.addExact(count, bytes(entry.getValue(), depth + 1)); if (count > MAX_EVIDENCE_BYTES) { return count; } }
        } else if (value instanceof Collection<?> items) {
            require(items.size() <= 512, "clock metadata list count exceeded");
            for (Object item : items) { count = Math.addExact(count, bytes(item, depth + 1)); if (count > MAX_EVIDENCE_BYTES) { return count; } }
        } else { throw new AssertionError("unsupported decoded clock metadata value"); }
        return count;
    }

    private static Map<String, Object> freezeMap(Map<String, Object> map, int depth) {
        Map<String, Object> copy = new LinkedHashMap<>(); map.forEach((key, value) -> copy.put(key, freeze(value, depth + 1)));
        return Collections.unmodifiableMap(copy);
    }
    private static Object freeze(Object value, int depth) {
        require(depth <= 12, "clock metadata depth exceeded");
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            map.forEach((key, item) -> { require(key instanceof String, "clock metadata key must be a string"); copy.put((String) key, freeze(item, depth + 1)); });
            return Collections.unmodifiableMap(copy);
        }
        if (value instanceof Collection<?> items) { return items.stream().map(item -> freeze(item, depth + 1)).toList(); }
        if (value instanceof Double number) { require(Double.isFinite(number), "non-finite clock metadata value"); return number; }
        if (value == null || value instanceof String || value instanceof Boolean || value instanceof Integer || value instanceof Long) { return value; }
        throw new AssertionError("unsupported mutable clock metadata value");
    }
    private static void require(boolean condition, String reason) { if (!condition) { throw new AssertionError(reason); } }
}
