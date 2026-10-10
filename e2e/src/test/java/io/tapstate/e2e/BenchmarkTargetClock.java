package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import java.util.Date;
import java.util.Map;

/** Server clock reads are bracketed outside workload timing and bind one actual target primary. */
final class BenchmarkTargetClock {
    private BenchmarkTargetClock() { }
    static final int ENDPOINT_RESOLUTION_ERROR_MILLIS = 2;
    static final int INTERIOR_SAMPLE_INTERVAL_MILLIS = 200;
    static final int MAX_INTERIOR_SAMPLE_GAP_MILLIS = 500;
    static final int MAX_INTERIOR_SAMPLES = 6_000;
    // Operation dates and logical slots are sampled separately; their ordering does not identify a clock step.
    static final String OPERATION_DATE_REFUSAL_SCOPE =
            "; evidenceScope=OPERATION_DATE_ORDER_IN_LOGICAL_STREAM; clockCause=UNKNOWN";
    static final String OPERATION_DATE_MEASUREMENT_METHOD = "FIXED_MIDDLE_OPERATION_DATE_DIAGNOSTIC_V9";

    static boolean rejectOperationDate(Long value, WallSamples legacyOrder, boolean legacyDiagnostic) {
        boolean withinLegacyTolerance = legacyOrder.accept(value);
        return value == null || legacyDiagnostic && !withinLegacyTolerance;
    }

    static Map<String, Object> operationDateTimeEvidence() {
        return Map.of("state", "UNQUALIFIED", "reason", "OPERATION_TIME_ERROR_BOUND_NOT_ESTABLISHED",
                "timeField", "MONGO_CHANGE_EVENT_WALL_TIME", "storageUnit", "ms",
                "measurementMethod", OPERATION_DATE_MEASUREMENT_METHOD, "performanceAcceptanceEligible", false);
    }

    static boolean operationDateTimingQualified() { return false; }

    static void requireOperationTimeErrorBound() {
        // This date-based method has no measured cache-age, scheduling or timestamp error bound.
        throw new AssertionError("operation time lacks measured error bounds; method=" + OPERATION_DATE_MEASUREMENT_METHOD);
    }

    /** Retains the fixed operation-date ordering tolerance without assigning the cause of a refusal. */
    static final class WallSamples {
        private Long highWater;
        boolean accept(Long current) {
            if (current == null || highWater != null && current < highWater
                    && Math.subtractExact(highWater, current) > ENDPOINT_RESOLUTION_ERROR_MILLIS) { return false; }
            highWater = highWater == null ? current : Math.max(highWater, current);
            return true;
        }
        Long highWaterMillis() { return highWater; }
    }
    record Reading(String primary, String processId, long serverWallMillis,
                   long startedAtNanos, long completedAtNanos, long localUtcBeforeMillis, long localUtcAfterMillis) {
        Reading {
            if (primary == null || processId == null || completedAtNanos < startedAtNanos
                    || localUtcAfterMillis < localUtcBeforeMillis) { throw new AssertionError("target clock read has no ordered actual identity"); }
        }
        Map<String, Object> evidence() {
            return Map.of("primary", primary, "processId", processId, "serverWallMillis", serverWallMillis,
                    "startedAtNanos", startedAtNanos, "completedAtNanos", completedAtNanos,
                    "localUtcBeforeMillis", localUtcBeforeMillis, "localUtcAfterMillis", localUtcAfterMillis,
                    "resolutionMillis", 1, "scope", "SERVER_LOCAL_TIME_REQUEST_BRACKET");
        }
    }

    static Reading read(String uri) {
        var address = new ConnectionString(uri);
        if (address.getDatabase() == null) { throw new AssertionError("target clock needs the owned database URI"); }
        try (var client = MongoClients.create(uri)) {
            client.getDatabase("admin").runCommand(new Document("ping", 1));
            return read(client.getDatabase("admin"));
        }
    }

    static Reading read(com.mongodb.client.MongoDatabase admin) {
            long utcBefore = System.currentTimeMillis(); long began = System.nanoTime();
            Document hello = admin.runCommand(new Document("hello", 1));
            long ended = System.nanoTime(); long utcAfter = System.currentTimeMillis();
            Document topology = hello.get("topologyVersion", Document.class);
            if (!(hello.get("localTime") instanceof Date date) || topology == null
                    || topology.get("processId") == null || !(hello.get("primary") instanceof String primary)) {
                throw new AssertionError("actual target primary clock provenance is incomplete");
            }
            return new Reading(primary, topology.get("processId").toString(), date.getTime(), began, ended, utcBefore, utcAfter);
    }

    static Map<String, Object> validate(Reading before, Reading after) {
        if (!before.primary().equals(after.primary()) || !before.processId().equals(after.processId())) {
            throw new AssertionError("target primary changed across the output window");
        }
        long minimumElapsedMillis = (after.startedAtNanos() - before.completedAtNanos()) / 1_000_000L;
        long maximumElapsedMillis = (after.completedAtNanos() - before.startedAtNanos()) / 1_000_000L;
        long serverElapsedMillis = Math.subtractExact(after.serverWallMillis(), before.serverWallMillis());
        if (minimumElapsedMillis < 0 || serverElapsedMillis < minimumElapsedMillis - ENDPOINT_RESOLUTION_ERROR_MILLIS
                || serverElapsedMillis > maximumElapsedMillis + ENDPOINT_RESOLUTION_ERROR_MILLIS) {
            throw new AssertionError("target server clock stepped outside its measured request uncertainty"
                    + "; before=" + before.evidence() + "; after=" + after.evidence()
                    + "; serverElapsedMillis=" + serverElapsedMillis
                    + "; minimumElapsedMillis=" + minimumElapsedMillis
                    + "; maximumElapsedMillis=" + maximumElapsedMillis);
        }
        return Map.of("state", "QUALIFIED", "before", before.evidence(), "after", after.evidence(),
                "serverElapsedMillis", serverElapsedMillis, "minimumElapsedMillis", minimumElapsedMillis,
                "maximumElapsedMillis", maximumElapsedMillis, "endpointResolutionErrorMillis", ENDPOINT_RESOLUTION_ERROR_MILLIS);
    }

    static Map<String, Object> validateSeries(java.util.List<Reading> readings) {
        if (readings.size() < 3 || readings.size() > MAX_INTERIOR_SAMPLES) {
            throw new AssertionError("target clock lacks bounded interior samples");
        }
        long maximumGap = 0;
        for (int i = 1; i < readings.size(); i++) {
            Reading before = readings.get(i - 1), after = readings.get(i);
            validate(before, after);
            long gap = Math.max(0, (after.startedAtNanos() - before.completedAtNanos()) / 1_000_000L);
            maximumGap = Math.max(maximumGap, gap);
            if (gap > MAX_INTERIOR_SAMPLE_GAP_MILLIS) { throw new AssertionError("target clock has an unqualified interior sampling gap"); }
        }
        return Map.of("state", "QUALIFIED_SAMPLED_INTERIOR", "samples", readings.size(),
                "sampleIntervalMillis", INTERIOR_SAMPLE_INTERVAL_MILLIS, "maximumObservedGapMillis", maximumGap,
                "maximumAllowedGapMillis", MAX_INTERIOR_SAMPLE_GAP_MILLIS,
                "endpointResolutionErrorMillis", ENDPOINT_RESOLUTION_ERROR_MILLIS,
                "readings", readings.stream().map(Reading::evidence).toList());
    }

    static long earliestLocalNanos(Reading bracket, long serverWallMillis) {
        return Math.addExact(bracket.startedAtNanos(), Math.multiplyExact(serverWallMillis - bracket.serverWallMillis(), 1_000_000L));
    }

    static long latestLocalNanos(Reading bracket, long serverWallMillis) {
        return Math.addExact(bracket.completedAtNanos(), Math.multiplyExact(serverWallMillis - bracket.serverWallMillis(), 1_000_000L));
    }

    record LocalWindow(long earliestStartNanos, long latestStartNanos, long earliestEndNanos, long latestEndNanos) { }

    static LocalWindow mapWindow(Reading before, Reading after, long firstServerWallMillis, long lastServerWallMillis) {
        validate(before, after);
        if (firstServerWallMillis < before.serverWallMillis() || lastServerWallMillis > after.serverWallMillis()
                || lastServerWallMillis <= firstServerWallMillis) {
            throw new AssertionError("target operation endpoints are outside their actual clock brackets");
        }
        long error = ENDPOINT_RESOLUTION_ERROR_MILLIS * 1_000_000L;
        long earlyStart = Math.min(earliestLocalNanos(before, firstServerWallMillis), earliestLocalNanos(after, firstServerWallMillis)) - error;
        long lateStart = Math.max(latestLocalNanos(before, firstServerWallMillis), latestLocalNanos(after, firstServerWallMillis)) + error;
        long earlyEnd = Math.min(earliestLocalNanos(before, lastServerWallMillis), earliestLocalNanos(after, lastServerWallMillis)) - error;
        long lateEnd = Math.max(latestLocalNanos(before, lastServerWallMillis), latestLocalNanos(after, lastServerWallMillis)) + error;
        if (lateStart >= earlyEnd) { throw new AssertionError("target operation window is smaller than clock uncertainty"); }
        return new LocalWindow(earlyStart, lateStart, earlyEnd, lateEnd);
    }

    static void requireSharedClock(java.util.List<Reading> readings) {
        if (readings.isEmpty() || readings.size() > 2) { throw new AssertionError("target clock set is empty or exceeds its fixed workload"); }
        Reading first = readings.getFirst();
        for (Reading next : readings) {
            if (!first.primary().equals(next.primary()) || !first.processId().equals(next.processId())) {
                throw new AssertionError("target collections do not share the same actual primary clock");
            }
        }
    }
}
