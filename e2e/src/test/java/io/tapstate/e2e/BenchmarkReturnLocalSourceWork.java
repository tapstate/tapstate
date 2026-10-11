package io.tapstate.e2e;

import java.util.EnumMap;
import java.util.Map;

/** Inclusive source bounds per entered local helper; opaque provider instructions are excluded. */
final class BenchmarkReturnLocalSourceWork {
    // Stored V3 source lower: fixed85 +five UTF8prefixes10 +keyCount1 +keyPrefix2 +rowCount4 +row5 +storedLength4.
    static final int RAW_LEDGER_BYTES = 2097152, MIN_STORED_FRAME_BYTES = 85 + 5 * Short.BYTES + 1 + Short.BYTES
            + Integer.BYTES + 1 + Integer.BYTES + Integer.BYTES;
    static final int MIN_PAGE_HEADER_BYTES = 32, MAX_PAGE_HEADER_BYTES = 32 + 512 + 128;
    static final int MAX_STORED_PIECE_BYTES = 65536 - 2048;
    // Complete-piece packing: adjacent nonempty page payloads sum>A. Initial empty validation is not retained.
    // These fixed expressions fit in int and are compiler constants; no initializer/provider work is added.
    static final int HEADER_PAGE_RESERVATION = 2 * (RAW_LEDGER_BYTES / (MAX_STORED_PIECE_BYTES + 1)) + 1;
    static final int STORED_LEDGER_BYTES = RAW_LEDGER_BYTES - HEADER_PAGE_RESERVATION * MAX_PAGE_HEADER_BYTES;
    static final int MAX_STORED_FRAMES = STORED_LEDGER_BYTES / MIN_STORED_FRAME_BYTES;
    static final int MAX_STORED_CALLS = MAX_STORED_FRAMES;
    static final int MAX_PAGE_FRAMES = (65536 - MIN_PAGE_HEADER_BYTES) / MIN_STORED_FRAME_BYTES;
    static final int MAX_ROWS = 262144;
    static final int PAGE_BYTES = 65_536, FRAMES = BenchmarkReturnLocalSourceWork.MAX_STORED_FRAMES, PAGE_FRAMES = BenchmarkReturnLocalSourceWork.MAX_PAGE_FRAMES, PART_ROWS = 512, CALL_ROWS = 1024;
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
    enum AssociationRecipe {
        ORIGINAL_FIXED_UPDATE_TWO_INTEGER_KEYS,
        ORIGINAL_FIXED_ONE_OR_TWO_INTEGER_KEYS_AND_COLD_SQL
    }
    static final int MEASURED_SQL_CHARS = 8192, MEASURED_BATCHES = 960;
    // Even a caller-supplied bounded SQL string cannot have more tuple/list members than
    // its prior character count plus one. This is independent of the original100-row batch.
    static final int COLD_TUPLES_PER_STATEMENT = MEASURED_SQL_CHARS + 1;

    private BenchmarkReturnLocalSourceWork() { }

    /** Run's local registration work; byte/ZIP provider work remains in its separate IO actors. */
    static BenchmarkReturnWorkBudget.Work registrationLocalUpper() {
        long files = BenchmarkReturnWorkBudgetProfile.FileRole.values().length;
        long sources = BenchmarkReturnWorkBudgetBindings.SourceRole.values().length;
        long classes = BenchmarkReturnWorkBudgetBindings.MAX_CLASSES;
        long origins = mul(2, classes), classpath = BenchmarkReturnWorkBudgetBindings.MAX_CLASSPATH;
        long jdk = BenchmarkReturnWorkBudgetBindings.JdkRole.values().length;
        long roster = add(128, files, sources, classes, origins, classpath, jdk);
        // CP parsing/hash is bounded before split, including one serializer delimiter per origin.
        // Typed source/class/origin/JDK fields are bounded by the reader before the local loops.
        long characters = add(mul(classpath, 4097), mul(files, add(4096, 128, 64, 20)),
                mul(sources, add(512, 64, 64, 20)), mul(classes, add(256, 64, 64, 20)),
                mul(origins, add(4096, 1024, 256, 128, 64, 64, 20)), mul(jdk, add(4096, 128, 64, 20)),
                4096, mul(2, BenchmarkReturnWorkBudgetBindings.MAX_MANIFEST_BYTES));
        // Inclusive API slots: string/path checks, stream predicates and projections, records,
        // map/list/enum/boxing, checked arithmetic, hashing setup and immutable evidence fields.
        return new BenchmarkReturnWorkBudget.Work(add(64, mul(16, roster)),
                add(1024, mul(32, characters), mul(256, roster)), 0, 0,
                add(mul(4, characters), mul(128, roster)), 12,
                add(mul(4, characters), mul(16, roster)), mul(8, roster));
    }

    static Map<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> bounds(Capacity capacity) {
        return bounds(capacity, AssociationRecipe.ORIGINAL_FIXED_UPDATE_TWO_INTEGER_KEYS);
    }

    static Map<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> bounds(Capacity capacity, AssociationRecipe recipe) {
        require(capacity != null, "registered source capacity is missing");
        require(recipe == AssociationRecipe.ORIGINAL_FIXED_UPDATE_TWO_INTEGER_KEYS
                        || recipe == AssociationRecipe.ORIGINAL_FIXED_ONE_OR_TWO_INTEGER_KEYS_AND_COLD_SQL,
                "SOURCE_GAP_ORIGINAL_EXPECTED_CHANGE_AND_KEY_EXTRACTOR_RECIPE");
        var result = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        long n = capacity.fullRows(), cohort = capacity.cohortRows(), batches = capacity.sourceBatches();
        long text = Math.min(PAGE_BYTES / 2L, add(2, mul(PAGE_FRAMES, add(7, PART_ROWS))));
        long errors = Math.min(mul(PAGE_FRAMES, PART_ROWS), add(PAGE_BYTES / 7L, 1));
        long pageRows = Math.min(mul(PAGE_FRAMES, PART_ROWS), add(PAGE_BYTES / 5L, 1));
        long decodeVisits = add(PAGE_FRAMES, text, errors, pageRows);
        // Text prefixes/read/UTF-8/ByteBuffer calls, fixed frame fields, details and two-key rows.
        put(result, BenchmarkReturnWorkBudget.Activity.DECODE,
                add(1, decodeVisits), add(128, mul(16, text), mul(96, PAGE_FRAMES), mul(8, errors), mul(16, pageRows)),
                pageRows, PAGE_FRAMES, add(mul(8, PAGE_BYTES), mul(64, decodeVisits)), 8,
                add(mul(8, PAGE_BYTES), decodeVisits), add(mul(4, PAGE_FRAMES), mul(4, pageRows), errors));

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

        // Size/length accessors establish the SQL prior before either existing parser route.
        result.merge(BenchmarkReturnWorkBudget.Activity.ASSOCIATION, new BenchmarkReturnWorkBudget.Work(1,
                add(32, mul(8, 2L * MEASURED_BATCHES)), 0, 0, mul(16, 2L * MEASURED_BATCHES),
                1, 2L * MEASURED_BATCHES, 0), BenchmarkReturnWorkBudget.Work::plus);

        if (recipe == AssociationRecipe.ORIGINAL_FIXED_ONE_OR_TWO_INTEGER_KEYS_AND_COLD_SQL) {
            require(capacity.sourceBatches() <= MEASURED_BATCHES, "original measured SQL batch roster differs");
            // The unchanged original recipe uses100 rows per statement. The general caller
            // material upper instead follows the enforced8192-character prior, including any
            // malformed split/group/range refusal. CDC has at most two statements/batch.
            // Prefix/substring/split/Matcher groups/parseLong/boxing/list/copy and update plans
            // are owned source requests; regex/sort provider instructions remain opaque.
            long statements = mul(2, capacity.sourceBatches());
            long tupleVisits = mul(statements, COLD_TUPLES_PER_STATEMENT);
            // The Workload constructor admits positive multiples-of-four below100000.
            // A generic bounded UPDATE may visit every id before the declared total refuses.
            long provisionalUpdateRows = mul(mul(statements, 99996), 2);
            tupleVisits = add(tupleVisits, provisionalUpdateRows);
            long sourceChars = mul(statements, MEASURED_SQL_CHARS);
            var cold = new BenchmarkReturnWorkBudget.Work(add(1, tupleVisits, statements),
                    add(64, mul(32, statements), mul(48, tupleVisits)), tupleVisits, 0,
                    add(mul(8, sourceChars), mul(128, tupleVisits)), 8,
                    add(mul(4, sourceChars), mul(8, tupleVisits)), mul(12, tupleVisits));
            result.merge(BenchmarkReturnWorkBudget.Activity.ASSOCIATION, cold, BenchmarkReturnWorkBudget.Work::plus);
            // The three existing immutable list copies are separate actual COPY entries.
            result.put(BenchmarkReturnWorkBudget.Activity.COPY, new BenchmarkReturnWorkBudget.Work(1,
                    add(32, mul(8, ROWS)), ROWS, 0, mul(16, ROWS), 2, ROWS, ROWS));
        }

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

    static void guardMeasuredAssociationSql(BenchmarkWorkloadDefinitions.Phase phase) {
        if (BenchmarkReturnWorkBudget.current() == null) return;
        require(phase != null && phase.statementsPerBatch() >= 1 && phase.statementsPerBatch() <= 2
                        && phase.sql().size() <= 2 * MEASURED_BATCHES
                        && phase.sql().size() % phase.statementsPerBatch() == 0
                        && phase.sql().size() / phase.statementsPerBatch() <= MEASURED_BATCHES,
                "measured SQL roster exceeds its original prior shape");
        for (String sql : phase.sql()) require(sql != null && sql.length() <= MEASURED_SQL_CHARS,
                "measured SQL exceeds its prior material bound");
    }

    static long maximumFrameRows() { return ROWS; }
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
