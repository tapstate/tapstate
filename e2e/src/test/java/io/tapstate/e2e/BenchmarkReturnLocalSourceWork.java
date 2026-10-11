package io.tapstate.e2e;

import java.util.EnumMap;
import java.util.Map;

/** Inclusive source bounds per entered local helper; opaque provider instructions are excluded. */
final class BenchmarkReturnLocalSourceWork {
    static final int PAGE_BYTES = 65_536, FRAMES = 512, PART_ROWS = 512, CALL_ROWS = 1024;
    static final int ROWS = 262_144, SAMPLES = 512, RESOURCE_ATTEMPTS = 4096, KEYS = 2;
    static final int ROUTING_TEXT_BYTES = 512;

    record Capacity(long fullRows, long cohortRows, long sourceBatches) {
        Capacity {
            require(fullRows > 0 && fullRows <= ROWS && cohortRows > 0
                            && Math.multiplyExact(cohortRows, 2) == fullRows
                            && sourceBatches > 0 && sourceBatches <= fullRows,
                    "original half-cohort and registered source batches are required");
        }
    }
    enum AssociationRecipe { ORIGINAL_FIXED_UPDATE_TWO_INTEGER_KEYS }

    private BenchmarkReturnLocalSourceWork() { }

    static Map<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> bounds(Capacity capacity) {
        return bounds(capacity, AssociationRecipe.ORIGINAL_FIXED_UPDATE_TWO_INTEGER_KEYS);
    }

    static Map<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> bounds(Capacity capacity, AssociationRecipe recipe) {
        require(capacity != null, "registered source capacity is missing");
        require(recipe == AssociationRecipe.ORIGINAL_FIXED_UPDATE_TWO_INTEGER_KEYS,
                "SOURCE_GAP_ORIGINAL_EXPECTED_CHANGE_AND_KEY_EXTRACTOR_RECIPE");
        var result = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        long n = capacity.fullRows(), cohort = capacity.cohortRows(), batches = capacity.sourceBatches();
        long text = Math.min(PAGE_BYTES / 2L, add(2, mul(FRAMES, add(7, PART_ROWS))));
        long errors = Math.min(mul(FRAMES, PART_ROWS), add(PAGE_BYTES / 7L, 1));
        long pageRows = Math.min(mul(FRAMES, PART_ROWS), add(PAGE_BYTES / 5L, 1));
        long decodeVisits = add(FRAMES, text, errors, pageRows);
        // Text prefixes/read/UTF-8/ByteBuffer calls, fixed frame fields, details and two-key rows.
        put(result, BenchmarkReturnWorkBudget.Activity.DECODE,
                add(1, decodeVisits), add(128, mul(16, text), mul(96, FRAMES), mul(8, errors), mul(16, pageRows)),
                pageRows, FRAMES, add(mul(8, PAGE_BYTES), mul(64, decodeVisits)), 8,
                add(mul(8, PAGE_BYTES), decodeVisits), add(mul(4, FRAMES), mul(4, pageRows), errors));

        // One part may finish a full 1024-row call; immutable constructors copy all three lists.
        long frameVisits = add(CALL_ROWS, PART_ROWS, KEYS);
        put(result, BenchmarkReturnWorkBudget.Activity.FRAME_VISIT,
                add(1, frameVisits), add(128, mul(16, CALL_ROWS), mul(8, PART_ROWS)), CALL_ROWS, 1,
                add(mul(8, CALL_ROWS), mul(5, ROUTING_TEXT_BYTES)), 4, frameVisits, add(CALL_ROWS, 16));

        // The original recipe visits at most two target plans. Each row has two integer key reads,
        // boxing/string conversion, tuple-map writes and bounded origin/seen/output updates.
        long replayRows = maximumFrameRows();
        long expectedVisits = add(mul(2, n), mul(2, batches));
        long expectedRequests = mul(32, expectedVisits);
        long extractorRequests = mul(16, replayRows);
        long associationVisits = add(expectedVisits, mul(2, replayRows), mul(4, FRAMES), batches);
        put(result, BenchmarkReturnWorkBudget.Activity.ASSOCIATION,
                add(1, associationVisits), add(256, mul(32, batches), mul(128, FRAMES), mul(64, replayRows), expectedRequests, extractorRequests),
                replayRows, FRAMES, add(mul(128, replayRows), mul(3L * ROUTING_TEXT_BYTES, FRAMES)), 8,
                associationVisits, add(mul(8, replayRows), expectedVisits, mul(4, FRAMES), batches));

        // Causal coverage scans no more than 512 samples. Native/nominal wrappers retain that scan
        // and add checked interval arithmetic; they do not read another clock or transport.
        var mapping = new BenchmarkReturnWorkBudget.Work(add(8, SAMPLES), add(mul(2, SAMPLES), 10),
                0, 0, 64, 1, add(SAMPLES, 8), 4);
        result.put(BenchmarkReturnWorkBudget.Activity.RETURN_MAP, mapping);
        result.put(BenchmarkReturnWorkBudget.Activity.CALL_POINT_CHECK, mapping);
        result.put(BenchmarkReturnWorkBudget.Activity.SOURCE_MAP,
                new BenchmarkReturnWorkBudget.Work(3, 2, 0, 0, 16, 1, 2, 2));

        // Inclusive parent bounds cover both six-API sort pipelines and their owned projections.
        // Child actors may conservatively repeat this allowance; complete actor counts are required.
        put(result, BenchmarkReturnWorkBudget.Activity.SORT,
                add(4, mul(2, cohort)), 32, cohort, 0, mul(16, cohort), 2, mul(2, cohort), 0);
        put(result, BenchmarkReturnWorkBudget.Activity.SPAN,
                add(8, mul(4, cohort)), 32, cohort, 0, mul(32, cohort), 4, mul(4, cohort), 8);

        // Four window extrema, two attempt traversals, OptionalLong access, fixed maps and brackets.
        put(result, BenchmarkReturnWorkBudget.Activity.RESOURCE_BOUNDS,
                add(64, cohort, mul(2, RESOURCE_ATTEMPTS)), add(256, mul(8, cohort), mul(48, RESOURCE_ATTEMPTS)),
                cohort, 0, add(mul(32, cohort), mul(64, RESOURCE_ATTEMPTS), 4096), 3,
                add(cohort, mul(2, RESOURCE_ATTEMPTS)), add(64, mul(4, cohort), mul(8, RESOURCE_ATTEMPTS)));
        return Map.copyOf(result);
    }

    static long maximumFrameRows() { return mul(FRAMES, PART_ROWS); }
    private static void put(EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> values,
                            BenchmarkReturnWorkBudget.Activity activity, long calls, long requests, long records, long frames,
                            long bytes, long passes, long elements, long updates) {
        values.put(activity, new BenchmarkReturnWorkBudget.Work(calls, requests, records, frames, bytes, passes, elements, updates));
    }
    private static long add(long... values) {
        long result = 0; for (long value : values) { result = Math.addExact(result, value); } return result;
    }
    private static long mul(long left, long right) { return Math.multiplyExact(left, right); }
    private static void require(boolean condition, String reason) { if (!condition) { throw new AssertionError("local return source work " + reason); } }
}
