package io.tapstate.e2e;

import java.util.List;
import java.util.Map;

/** Source bounds for cold counter work; hot reads are separately counted without another clock point. */
final class BenchmarkReturnRootClockWork {
    private static final long HASH_BYTES = 16L * 1024 * 1024 + 4L * 256 * 1024 + 5L * 8192;
    private static final long NATIVE_COPY_REQUESTS = 2053, NATIVE_COPY_BYTES = 16593;
    private static final long ROOT_FIELDS = 32, SNAPSHOT_FIELDS = 20, CLASS_FIELDS = 4;
    private static final long MAPS = 7;
    private static final long ENTRIES = ROOT_FIELDS + 2 * SNAPSHOT_FIELDS + 2 * CLASS_FIELDS;
    private static final long NODES = 1 + 2 * ENTRIES;
    // Eleven bounded native fact strings, snapshot keys, and four digest/name pairs per snapshot.
    private static final long TEXT = 2 * (11L * 512 + SNAPSHOT_FIELDS * 64 + CLASS_FIELDS * (64 + 32))
            + ROOT_FIELDS * (128 + 128);
    // All native metadata text is printable ASCII; JSON escaping needs at most two characters.
    private static final long ENCODED_ASCII = 2 * TEXT + 28 * NODES;
    private static final long JSON_REQUESTS = 2 + 5 * MAPS + 7 * ENTRIES + 3 * (ENTRIES + NODES) + 3 * TEXT + 2 * NODES;
    private static final long FIXED_CHECKS = 8192;
    private BenchmarkReturnRootClockWork() { }

    static BenchmarkReturnWorkBudget.Work metadataUpper() {
        long material = Math.addExact(HASH_BYTES, NATIVE_COPY_BYTES);
        long requests = Math.addExact(Math.multiplyExact(2, HASH_BYTES + 5),
                Math.addExact(2 * NATIVE_COPY_REQUESTS + FIXED_CHECKS,
                        Math.addExact(JSON_REQUESTS, 2 + 2 * ENCODED_ASCII)));
        return new BenchmarkReturnWorkBudget.Work(1, requests, 0, 0,
                Math.addExact(Math.multiplyExact(4, material), 65536), 64,
                Math.addExact(Math.multiplyExact(8, material), 65536),
                Math.addExact(Math.multiplyExact(8, material), 65536));
    }

    static BenchmarkReturnWorkBudget.Work coldInitializationUpper() {
        // Two metadata-sized allocations conservatively include constructor identity/path/load,
        // JNI_OnLoad route setup and the initial snapshot; they are charged once per root run.
        return metadataUpper().times(2).plus(new BenchmarkReturnWorkBudget.Work(1, 64, 0, 0, 4096, 8, 8192, 8192));
    }

    static Map<String, Object> evidence() {
        return Map.of("state", "SOURCE_UPPER_ONLY_ROOT_COLD_INITIALIZATION",
                "coldInitializationUpper", coldInitializationUpper().evidence(),
                "perMetadataUpper", metadataUpper().evidence(),
                "sourceSteps", List.of("ONE_ROOT_CONSTRUCTOR_PID_START_PROPERTY_PATH_LOAD_AND_JNI_ONLOAD",
                        "EACH_METADATA_NATIVE_FACTS_LIBRARY_FOUR_CLASS_HASHES_PREFIX_HASH_AND_JSON",
                        "HOT_ORIGINAL_RAW_READ_ONCE_THEN_ACTUAL_REQUEST_ACCOUNTING"),
                "metadataJsonNodeUpper", NODES, "metadataJsonTextUpper", TEXT,
                "metadataJsonAsciiUpper", ENCODED_ASCII,
                "actualColdInitializationObserved", false, "wholeMethodCostQualified", false,
                "performanceAcceptanceEligible", false);
    }
}
