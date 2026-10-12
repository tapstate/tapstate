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
            if (primary == null || processId == null || completedAtNanos < startedAtNanos) {
                throw new AssertionError("target clock read has no ordered actual identity");
            }
        }
        Map<String, Object> evidence() {
            return Map.of("primary", primary, "processId", processId, "serverWallMillis", serverWallMillis,
                    "startedAtNanos", startedAtNanos, "completedAtNanos", completedAtNanos,
                    "localUtcBeforeMillis", localUtcBeforeMillis, "localUtcAfterMillis", localUtcAfterMillis,
                    "resolutionMillis", 1, "scope", "SERVER_LOCAL_TIME_REQUEST_BRACKET");
        }
    }

    static Reading read(String uri) {
        return read(uri, false);
    }

    static Reading readForAck(String uri) {
        return read(uri, true);
    }

    private static Reading read(String uri, boolean ackContext) {
        var address = new ConnectionString(uri);
        if (address.getDatabase() == null) { throw new AssertionError("target clock needs the owned database URI"); }
        try (var client = MongoClients.create(uri)) {
            client.getDatabase("admin").runCommand(new Document("ping", 1));
            var admin = client.getDatabase("admin");
            return ackContext ? readForAck(admin) : read(admin);
        }
    }

    static Reading read(com.mongodb.client.MongoDatabase admin) {
        Reading reading = readForAck(admin);
        requireLegacyReadOrder(reading);
        return reading;
    }

    static Reading readForAck(com.mongodb.client.MongoDatabase admin) {
            long utcBefore = System.currentTimeMillis(); long began = BenchmarkReturnWorkBudget.rootNanoTime();
            Document hello = admin.runCommand(new Document("hello", 1));
            long ended = BenchmarkReturnWorkBudget.rootNanoTime(); long utcAfter = System.currentTimeMillis();
            Document topology = hello.get("topologyVersion", Document.class);
            if (!(hello.get("localTime") instanceof Date date) || topology == null
                    || topology.get("processId") == null || !(hello.get("primary") instanceof String primary)) {
                throw new AssertionError("actual target primary clock provenance is incomplete");
            }
            var work = BenchmarkReturnWorkBudget.current();
            if (work != null) {
                BenchmarkReturnWorkBudget.requireLocalMaterial(work, BenchmarkReturnWorkBudget.Activity.HELLO_READ, primary);
                if (!(topology.get("processId") instanceof org.bson.types.ObjectId)) {
                    throw new AssertionError("target clock process identity has no bounded original ObjectId shape");
                }
                BenchmarkReturnWorkBudget.requireLocalMaterial(work, BenchmarkReturnWorkBudget.Activity.HELLO_READ, topology.get("processId"));
            }
            return new Reading(primary, topology.get("processId").toString(), date.getTime(), began, ended, utcBefore, utcAfter);
    }

    static Map<String, Object> validate(Reading before, Reading after) {
        // Wall ordering remains mandatory for the legacy calibration and date-based paths.
        requireLegacyReadOrder(before); requireLegacyReadOrder(after);
        if (!before.primary().equals(after.primary()) || !before.processId().equals(after.processId())) {
            throw new AssertionError("target primary changed across the output window");
        }
        // Integer dates need an inward lower threshold; truncation could admit a fractional violation.
        long minimumElapsedMillis = Math.ceilDiv(after.startedAtNanos() - before.completedAtNanos(), 1_000_000L);
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

    static void requireLegacyReadOrder(Reading reading) {
        if (reading.localUtcAfterMillis() < reading.localUtcBeforeMillis()) {
            throw new AssertionError("target clock read has no ordered actual identity");
        }
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

    /** Outer pairs can span a whole phase; the interior sampler's gap policy is not an outer-pair limit. */
    static Map<String, Object> validateAckPair(Reading before, Reading after) {
        return BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.VERIFY_PHASE,
                () -> validateAckPairBounded(before, after));
    }

    private static Map<String, Object> validateAckPairBounded(Reading before, Reading after) {
        requireAckPair(before, after);
        var result = new java.util.LinkedHashMap<String, Object>();
        result.put("state", "ACK_HELLO_PAIR_IDENTITY_AND_ORDER_VERIFIED");
        result.put("before", before.evidence()); result.put("after", after.evidence());
        result.put("wallComparison", wallDiagnosticComparison(before, after));
        result.put("wallComparisonScope", "DIAGNOSTIC_ONLY_NOT_RETURN_POINT_MAPPING_CALIBRATION");
        result.put("clockCause", "UNKNOWN");
        BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> result.put(flag, false));
        return Map.copyOf(result);
    }

    /** Verifies the unchanged interior policy without requiring wall clocks to calibrate the return coordinate. */
    static Map<String, Object> validateAckSeries(java.util.List<Reading> readings) {
        return BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.VERIFY_PHASE,
                () -> validateAckSeriesBounded(readings));
    }

    private static Map<String, Object> validateAckSeriesBounded(java.util.List<Reading> readings) {
        var retained = ackRoster(readings);
        Reading first = retained.getFirst();
        long maximumGap = 0;
        long adjacentRefusals = 0;
        long firstToEachRefusals = 0;
        var facts = new java.util.ArrayList<Map<String, Object>>(retained.size());
        for (int i = 0; i < retained.size(); i++) {
            Reading current = retained.get(i);
            var fact = new java.util.LinkedHashMap<String, Object>(current.evidence());
            if (i > 0) {
                Reading previous = retained.get(i - 1);
                requireAckPair(previous, current);
                long gap = Math.subtractExact(current.startedAtNanos(), previous.completedAtNanos());
                if (gap > MAX_INTERIOR_SAMPLE_GAP_MILLIS * 1_000_000L) {
                    throw new AssertionError("ACK hello has an unqualified interior sampling gap");
                }
                maximumGap = Math.max(maximumGap, gap);
                var adjacent = wallDiagnosticComparison(previous, current);
                var fromFirst = wallDiagnosticComparison(first, current);
                fact.put("adjacentWallComparison", adjacent);
                fact.put("firstToEachWallComparison", fromFirst);
                if (!"WITHIN_LEGACY_TOLERANCE".equals(adjacent.get("state"))) { adjacentRefusals++; }
                if (!"WITHIN_LEGACY_TOLERANCE".equals(fromFirst.get("state"))) { firstToEachRefusals++; }
            } else {
                fact.put("adjacentWallComparison", "NOT_APPLICABLE");
                fact.put("firstToEachWallComparison", "NOT_APPLICABLE");
            }
            facts.add(Map.copyOf(fact));
        }
        var result = new java.util.LinkedHashMap<String, Object>();
        result.put("state", "ACK_HELLO_SERIES_IDENTITY_ORDER_AND_GAPS_VERIFIED");
        result.put("samples", retained.size());
        result.put("sampleIntervalMillis", INTERIOR_SAMPLE_INTERVAL_MILLIS);
        result.put("maximumObservedGapNominalNanos", maximumGap);
        result.put("maximumAllowedGapMillis", MAX_INTERIOR_SAMPLE_GAP_MILLIS);
        result.put("wallComparisonScope", "DIAGNOSTIC_ONLY_NOT_RETURN_POINT_MAPPING_CALIBRATION");
        result.put("wallComparisonToleranceMillis", ENDPOINT_RESOLUTION_ERROR_MILLIS);
        result.put("adjacentWallRefusals", adjacentRefusals);
        result.put("firstToEachWallRefusals", firstToEachRefusals);
        result.put("clockCause", "UNKNOWN"); result.put("readings", java.util.List.copyOf(facts));
        BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> result.put(flag, false));
        return Map.copyOf(result);
    }

    static Map<String, Object> verifyAckHello(java.util.List<Reading> readings,
            long firstSourceIssuedAtNanos, long lastReturnObservedAtNanos) {
        return BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.VERIFY_PHASE,
                () -> verifyAckHelloBounded(readings, firstSourceIssuedAtNanos, lastReturnObservedAtNanos));
    }

    private static Map<String, Object> verifyAckHelloBounded(java.util.List<Reading> readings,
            long firstSourceIssuedAtNanos, long lastReturnObservedAtNanos) {
        var retained = ackRoster(readings);
        if (firstSourceIssuedAtNanos < 0 || lastReturnObservedAtNanos < firstSourceIssuedAtNanos) {
            throw new AssertionError("ACK hello phase coverage endpoints are invalid");
        }
        if (retained.getFirst().completedAtNanos() > firstSourceIssuedAtNanos
                || retained.getLast().startedAtNanos() < lastReturnObservedAtNanos) {
            throw new AssertionError("ACK hello phase coverage does not contain the original source and return window");
        }
        var result = new java.util.LinkedHashMap<String, Object>(validateAckSeries(retained));
        result.put("state", "ACK_HELLO_IDENTITY_ORDER_AND_COVERAGE_VERIFIED");
        result.put("firstSourceIssuedAtNanos", firstSourceIssuedAtNanos);
        result.put("lastReturnObservedAtNanos", lastReturnObservedAtNanos);
        return Map.copyOf(result);
    }

    private static java.util.List<Reading> ackRoster(java.util.List<Reading> readings) {
        if (readings == null || readings.size() < 3 || readings.size() > MAX_INTERIOR_SAMPLES) {
            throw new AssertionError("ACK hello lacks bounded interior samples");
        }
        int size = readings.size();
        var retained = new java.util.ArrayList<Reading>(size);
        for (int i = 0; i < size; i++) {
            Reading reading = readings.get(i);
            requireAckReading(reading);
            retained.add(reading);
        }
        if (readings.size() != size) { throw new AssertionError("ACK hello roster changed during its bounded copy"); }
        return java.util.List.copyOf(retained);
    }

    private static void requireAckPair(Reading before, Reading after) {
        requireAckReading(before); requireAckReading(after);
        if (!before.primary().equals(after.primary()) || !before.processId().equals(after.processId())) {
            throw new AssertionError("ACK hello target primary changed across the output window");
        }
        if (before.startedAtNanos() < 0 || after.startedAtNanos() < 0
                || after.startedAtNanos() <= before.startedAtNanos()
                || after.startedAtNanos() < before.completedAtNanos()) {
            throw new AssertionError("ACK hello requests are not ordered serial requests");
        }
    }

    private static void requireAckReading(Reading reading) {
        if (reading == null || reading.primary().isEmpty() || reading.processId().isEmpty()
                || reading.primary().length() > 65_536 || reading.processId().length() > 24) {
            throw new AssertionError("ACK hello actual identity exceeds its bounded raw shape");
        }
        if (reading.startedAtNanos() < 0 || reading.completedAtNanos() < reading.startedAtNanos()) {
            throw new AssertionError("ACK hello root request bracket is invalid");
        }
    }

    private static Map<String, Object> wallDiagnosticComparison(Reading before, Reading after) {
        var result = new java.util.LinkedHashMap<String, Object>();
        result.put("rootElapsedLowerNominalNanos", Math.subtractExact(after.startedAtNanos(), before.completedAtNanos()));
        result.put("rootElapsedUpperNominalNanos", Math.subtractExact(after.completedAtNanos(), before.startedAtNanos()));
        result.put("localUtcReadOrder", before.localUtcAfterMillis() >= before.localUtcBeforeMillis()
                && after.localUtcAfterMillis() >= after.localUtcBeforeMillis());
        result.put("toleranceMillis", ENDPOINT_RESOLUTION_ERROR_MILLIS);
        try {
            result.put("serverElapsedMillis", Math.subtractExact(after.serverWallMillis(), before.serverWallMillis()));
            result.put("hostWallOuterLowerMillis", Math.subtractExact(after.localUtcBeforeMillis(), before.localUtcAfterMillis()));
            result.put("hostWallOuterUpperMillis", Math.subtractExact(after.localUtcAfterMillis(), before.localUtcBeforeMillis()));
            // Reproduce the numeric legacy predicate without constructing a large refusal message.
            long minimum = Math.ceilDiv(after.startedAtNanos() - before.completedAtNanos(), 1_000_000L);
            long maximum = (after.completedAtNanos() - before.startedAtNanos()) / 1_000_000L;
            long server = (Long) result.get("serverElapsedMillis");
            boolean within = before.localUtcAfterMillis() >= before.localUtcBeforeMillis()
                    && after.localUtcAfterMillis() >= after.localUtcBeforeMillis()
                    && minimum >= 0 && server >= minimum - ENDPOINT_RESOLUTION_ERROR_MILLIS
                    && server <= maximum + ENDPOINT_RESOLUTION_ERROR_MILLIS;
            result.put("state", within ? "WITHIN_LEGACY_TOLERANCE" : "LEGACY_WALL_COMPARISON_REFUSED");
            result.put("failureType", within ? "NONE" : "java.lang.AssertionError");
        } catch (ArithmeticException refused) {
            result.put("state", "WALL_DELTA_ARITHMETIC_REFUSED"); result.put("failureType", "java.lang.ArithmeticException");
        }
        return Map.copyOf(result);
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
