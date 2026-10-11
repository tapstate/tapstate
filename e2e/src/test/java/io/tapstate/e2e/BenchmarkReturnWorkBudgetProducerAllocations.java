package io.tapstate.e2e;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Independent arithmetic for one frozen producer definition; runtime coverage is verified separately. */
final class BenchmarkReturnWorkBudgetProducerAllocations {
    static final String DEFINITION_ID = "WRITE_RETURN_PRODUCER_WORK_V1";
    static final String FORMULA_SHA256 = "5e7e3babad7f8e2bd19f22c8ce0aac1ba5164e72e3867d5957cc568a6bfa4c89";
    static final List<String> UNITS = List.of("calls", "requests", "records", "frames", "bytes", "passes", "elements", "updates");
    private static final List<String> FLAGS = List.of("structuralBudgetQualified", "accountingErrorBoundQualified",
            "wholeMethodCostQualified", "collectionCostUpperBoundQualified", "samplingCostQualified",
            "causalOverheadQualified", "costAcceptanceEligible", "performanceAcceptanceEligible", "formalPerformance");
    private static final List<String> ROOT_FIELDS = List.of("schemaVersion", "enabled", "definitionId", "sourceFormulaSha256",
            "inputSha256", "runId", "pid", "jvmStartTimeMillis", "stage", "lifecycle", "generation", "nonceSha256",
            "state", "limits", "materialLimits", "actualMaterial", "sourceUpper", "inputRowsAttempted",
            "sourceUpperIsActualWork", "limitsIndependentlyVerified", "activities", "bookkeepingInvocations",
            "epochs", "pending", "openWriters", "omitted", "overflow", "overBudget", "gaps", "lateMutations",
            "sourceSealTailUpper", "sealTailIncludedInSourceUpper", "actualSealTailInCompanion", "processExitObserved",
            "crashDurabilityQualified", "opaqueScope", "excluded", "costUnit", "structuralBudgetQualified",
            "accountingErrorBoundQualified", "wholeMethodCostQualified", "collectionCostUpperBoundQualified",
            "samplingCostQualified", "causalOverheadQualified", "costAcceptanceEligible", "performanceAcceptanceEligible", "formalPerformance");
    private static final List<String> ACTORS = List.of("CONFIG", "INSTALL", "WRITER_INSTALL", "WRITER_ROSTER",
            "WRITER_CLOSE_ATTEMPT", "WRITER_CLOSE", "WRITE_WRAPPER", "BEGIN", "ACTIVE_BEGIN", "INACTIVE_BEGIN",
            "REJECTION", "IDENTITY_ROW", "IDENTITY_PIECE", "KEY_SHAPE", "CALLBACK_WRAPPER", "CALLBACK",
            "CALLBACK_ERROR", "CALLBACK_EXIT", "RETURN_OBSERVATION", "COMPLETE", "RECEIPT_PIECE", "PUBLICATION",
            "COST_AGGREGATION", "CLOCK_READ", "CLOCK_INITIALIZATION", "CLOCK_METADATA", "SCOPE_INSTALL",
            "SCOPE_CACHE_ENTRY", "SCOPE_BEFORE", "SCOPE_AFTER", "TRANSACTION_WRAPPER", "HASH_READ", "HASH_MATERIAL",
            "ARGUMENT", "ARGUMENT_MATERIAL", "MBEAN_SCALAR", "MBEAN_START", "MBEAN_STOP", "MBEAN_PAGE",
            "PAGE_FRAME_VISIT", "MBEAN_COST", "MBEAN_BUDGET", "ARM", "SHUTDOWN", "FINAL_OWNER", "FINAL_WAIT_ITERATION", "BOOKKEEPING", "COLD_ORIGINS");
    private static final Set<String> ROW_FIELDS = Set.of("invocations", "completed", "failed", "pending");
    private static final List<String> MATERIAL_FIELDS = List.of("connectorHashBytes", "runtimeArguments", "runtimeArgumentBytes", "primaryKeys");
    private static final List<String> EPOCH_FIELDS = List.of("epoch", "windowBase64");
    private static final long INPUT_BYTES = 16384, FRAME_BYTES = 65536, SEAL_BYTES = 256;
    private static final long EPOCHS = 16, GAPS = 32, REASON_CHARS = 128, WINDOW_BYTES = 512;
    private static final long WINDOW_BASE64 = mul(4, (WINDOW_BYTES + 2) / 3);
    // Structural slots come from the frozen rosters, independently of receipt values or limits.
    private static final long MAPS = add(1, 4, 1, 1, ACTORS.size(), EPOCHS);
    private static final long ENTRIES = add(ROOT_FIELDS.size(), mul(4, UNITS.size()), MATERIAL_FIELDS.size(),
            ACTORS.size(), mul(ACTORS.size(), ROW_FIELDS.size()), mul(EPOCHS, EPOCH_FIELDS.size()));
    private static final long LIST_ELEMENTS = add(EPOCHS, GAPS);
    private static final long NODES = add(1, ENTRIES, LIST_ELEMENTS);
    private static final long KEY_CHARS = add(chars(ROOT_FIELDS), mul(4, chars(UNITS)), chars(MATERIAL_FIELDS),
            chars(ACTORS), mul(ACTORS.size(), chars(List.of("invocations", "completed", "failed", "pending"))), mul(EPOCHS, chars(EPOCH_FIELDS)));
    private static final long STRING_CHARS = add(KEY_CHARS, mul(ROOT_FIELDS.size(), 128), mul(EPOCHS, WINDOW_BASE64), mul(GAPS, REASON_CHARS));
    private static final long JSON_CHARS = add(mul(2, STRING_CHARS), mul(20, NODES), mul(8, NODES));
    private static final long BUILD_REQUESTS = add(6, ACTORS.size(), 1, ENTRIES, NODES, 2, mul(2, ACTORS.size()),
            1, add(mul(2, FLAGS.size()), 2), 5, mul(4, UNITS.size()), 1);
    private static final long JSON_REQUESTS = add(2, mul(5, MAPS), mul(7, ENTRIES), mul(4, 2), mul(3, LIST_ELEMENTS),
            mul(3, add(ENTRIES, NODES)), mul(3, STRING_CHARS), mul(2, NODES));
    private static final long REPORT_REQUESTS = add(BUILD_REQUESTS, JSON_REQUESTS, 2, mul(2, JSON_CHARS));
    private static final long REPORT_BYTES = add(mul(2, STRING_CHARS), JSON_CHARS, 64, 32, 64);
    private static final long REPORT_PASSES = add(4, 1, 1, MAPS, 2, ENTRIES, NODES, 1);
    private static final long REPORT_ELEMENTS = mul(8, add(NODES, ENTRIES, STRING_CHARS, JSON_CHARS));
    private static final String TRANSPORT_SOURCE = "ledger-raw=2097152;stored=2052128;page-header=672;nonempty-pages=67;min-stored-v3-piece=111;stored-frames=18487;part-rows=512;total-rows=262144;cost-stage-calls=512;admitted-storage-only-not-future-total-calls;";
    private static final long TRANSPORT_SOURCE_CHARS = 210;
    private static final long CONFIG_REQUESTS = add(mul(6, INPUT_BYTES), mul(12, INPUT_BYTES), mul(5, INPUT_BYTES),
            mul(64, 32), mul(8, ENTRIES), mul(4, KEY_CHARS), mul(4, mul(ACTORS.size(), UNITS.size())), 16);
    private static final long CLOCK_HASH_BYTES = add(16L * 1024 * 1024, 4L * 256 * 1024, 5L * 8192);
    private static final long CLOCK_MATERIAL = add(CLOCK_HASH_BYTES, 16593);
    private static final long CLOCK_REQUESTS = add(2053, 1024, mul(2, add(CLOCK_HASH_BYTES, 5)));
    private static final long CLOCK_TRANSFER = add(mul(4, CLOCK_MATERIAL), 65536);
    private static final long CLOCK_ELEMENTS = add(mul(8, CLOCK_MATERIAL), 65536);
    // Fixed cold roster, shape and material caps; no receipt maxima or producer limit is used.
    private static final long COLD_CLASSES = 32, COLD_CLASS_BYTES = 1048576, COLD_PACKET_BYTES = 2080768;
    private static final long COLD_NODES = add(128, mul(COLD_CLASSES, 96));
    private static final long COLD_ENTRIES = add(18, 18, 8, mul(COLD_CLASSES, add(15, 18)));
    private static final long COLD_TEXT = add(8192, mul(COLD_CLASSES, 64512));
    private static final long COLD_LOCAL_TEXT = add(2L * 18448, 2L * 4096, 2L * 1024, 2L * 256,
            512, 128, chars(BenchmarkReturnProducerOrigins.NAMES));
    private static final long COLD_JSON_REQUESTS = add(2, mul(5, COLD_CLASSES + 3), mul(7, COLD_ENTRIES),
            mul(3, add(COLD_ENTRIES, COLD_NODES)), mul(3, COLD_TEXT), mul(2, COLD_NODES), 4, mul(3, 8));
    private static final long COLD_PROVIDER_REQUESTS = add(128, mul(2, COLD_CLASS_BYTES + 1));
    private static final BenchmarkReturnWorkBudget.Work COLD_ORIGINS = work(
            add(1, mul(16, COLD_NODES), mul(8, COLD_TEXT)),
            add(COLD_PROVIDER_REQUESTS, mul(32, COLD_LOCAL_TEXT), COLD_JSON_REQUESTS, mul(3, COLD_PACKET_BYTES), mul(16, COLD_NODES)),
            1, 0, add(mul(4, add(COLD_CLASS_BYTES + 1, COLD_PACKET_BYTES)), mul(128, COLD_NODES), mul(COLD_CLASSES, 8192)),
            add(16, mul(8, COLD_CLASSES), mul(2, COLD_NODES)), mul(8, add(COLD_CLASS_BYTES + 1, COLD_PACKET_BYTES, COLD_NODES)),
            mul(8, add(COLD_TEXT, COLD_NODES)));
    private static final long BOOK_REQUESTS = add(3, UNITS.size(), 3, 2, 2, mul(2, UNITS.size()),
            2, 6, 2, mul(2, REASON_CHARS), 8, 8);
    private static final long BOOK_BYTES = add(8, mul(2, 4), mul(4, WINDOW_BYTES), WINDOW_BYTES, WINDOW_BASE64);
    private static final long BOOK_ELEMENTS = add(mul(8, UNITS.size()), mul(2, REASON_CHARS), WINDOW_BYTES, WINDOW_BASE64, 16);
    private static final BenchmarkReturnWorkBudget.Work BOOKKEEPING = work(1, BOOK_REQUESTS, 0, 0, BOOK_BYTES, 6, BOOK_ELEMENTS, BOOK_ELEMENTS);
    private static final long SEAL_REQUESTS = add(UNITS.size(), 4, 1, 3, 10, 2, 2, 2, 2, 2, 1, 1, 1, 1);
    private static final BenchmarkReturnWorkBudget.Work SEAL_TAIL = work(1, add(REPORT_REQUESTS, SEAL_REQUESTS), 0, 0,
            add(REPORT_BYTES, mul(3, FRAME_BYTES), mul(8, SEAL_BYTES)), add(REPORT_PASSES, 4),
            add(REPORT_ELEMENTS, mul(8, SEAL_BYTES), FRAME_BYTES), add(REPORT_ELEMENTS, mul(8, UNITS.size()), mul(8, SEAL_BYTES)));
    private static final Map<String, BenchmarkReturnWorkBudget.Work> TABLE = table();

    private BenchmarkReturnWorkBudgetProducerAllocations() { }

    /** Recomputes a finite observed prefix or shutdown body; it never grants method or runtime credit. */
    static Map<String, Object> evaluate(Map<String, Object> body) {
        require(body != null && body.size() == ROOT_FIELDS.size() && body.keySet().equals(Set.copyOf(ROOT_FIELDS)), "root roster differs");
        require(number(body, "schemaVersion") == 1 && Boolean.TRUE.equals(body.get("enabled"))
                        && DEFINITION_ID.equals(body.get("definitionId")) && FORMULA_SHA256.equals(body.get("sourceFormulaSha256")),
                "source definition differs");
        require(Boolean.FALSE.equals(body.get("sourceUpperIsActualWork"))
                        && Boolean.FALSE.equals(body.get("limitsIndependentlyVerified"))
                        && Boolean.FALSE.equals(body.get("processExitObserved")) && Boolean.FALSE.equals(body.get("crashDurabilityQualified")),
                "producer claimed an unsupported binding");
        for (String flag : FLAGS) require(Boolean.FALSE.equals(body.get(flag)), "producer claimed a qualification");
        require("STRUCTURAL_WORK_VECTOR_NOT_CPU_OR_ELAPSED".equals(body.get("costUnit"))
                        && "REQUESTS_AND_BOUNDED_MATERIAL_ONLY".equals(body.get("opaqueScope"))
                        && "OPAQUE_PROVIDER_INSTRUCTIONS_CPU_ELAPSED_AND_VM_EXIT_INTERNALS".equals(body.get("excluded")), "material units differ");
        require("RECORDED_STRUCTURAL_DIAGNOSTIC".equals(body.get("state")) || "UNKNOWN".equals(body.get("state")), "producer state differs");
        require(number(body, "omitted") == 0 && number(body, "lateMutations") == 0
                        && Boolean.FALSE.equals(body.get("overflow")), "basis is omitted, late or overflowed");
        Map<?, ?> rows = object(body.get("activities"));
        require(rows.size() == ACTORS.size() && rows.keySet().equals(TABLE.keySet()), "activity roster differs");
        var copiedRows = new LinkedHashMap<String, Object>();
        var recomputed = BenchmarkReturnWorkBudget.Work.zero();
        long failed = 0, pending = 0;
        try {
            for (String actor : ACTORS) {
                Map<?, ?> row = object(rows.get(actor));
                require(row.size() == ROW_FIELDS.size() && row.keySet().equals(ROW_FIELDS), "activity count roster differs");
                long count = number(row, "invocations"), complete = number(row, "completed");
                long rowFailed = number(row, "failed"), rowPending = number(row, "pending");
                require(count == add(complete, rowFailed, rowPending), "activity pairing differs");
                failed = Math.addExact(failed, rowFailed); pending = Math.addExact(pending, rowPending);
                recomputed = recomputed.plus(TABLE.get(actor).times(count));
                copiedRows.put(actor, Map.of("invocations", count, "completed", complete, "failed", rowFailed, "pending", rowPending));
            }
            require(pending == number(body, "pending"), "pending total differs");
            recomputed = recomputed.plus(BOOKKEEPING.times(number(body, "bookkeepingInvocations")));
            boolean tail = bool(body, "sealTailIncludedInSourceUpper");
            require(tail == bool(body, "actualSealTailInCompanion"), "tail markers differ");
            require(vector(body.get("sourceSealTailUpper")).equals(SEAL_TAIL), "declared tail allocation differs");
            if (tail) recomputed = recomputed.plus(SEAL_TAIL);
        } catch (ArithmeticException overflow) { throw new AssertionError("producer allocation checked arithmetic overflow", overflow); }
        var declared = vector(body.get("sourceUpper"));
        require(declared.equals(recomputed), "declared upper differs from independent source sum");
        var actualMaterial = vector(body.get("actualMaterial"));
        var callerLimits = vector(body.get("limits"));
        boolean withinCallerCeiling = recomputed.within(callerLimits);
        require(bool(body, "overBudget") != withinCallerCeiling, "declared ceiling result differs");
        materialBounds(body.get("materialLimits"));
        require(body.get("epochs") instanceof List<?> epochs && epochs.size() <= EPOCHS, "epoch roster exceeds its bound");
        require(body.get("gaps") instanceof List<?> gaps && gaps.size() <= GAPS, "gap roster exceeds its bound");
        boolean complete = failed == 0 && pending == 0 && number(body, "openWriters") == 0
                && withinCallerCeiling && ((List<?>) body.get("gaps")).isEmpty();
        var result = new LinkedHashMap<String, Object>();
        result.put("state", "SOURCE_UPPER_RECOMPUTED_DIAGNOSTIC"); result.put("definitionId", DEFINITION_ID);
        result.put("sourceFormulaSha256", FORMULA_SHA256); result.put("sourceDefinitionMatched", true);
        result.put("recomputedSourceUpper", recomputed.evidence()); result.put("declaredSourceUpper", declared.evidence());
        result.put("fixedSealTail", SEAL_TAIL.evidence()); result.put("sealTailIncluded", bool(body, "sealTailIncludedInSourceUpper"));
        result.put("activityBasis", Map.copyOf(copiedRows)); result.put("bookkeepingInvocations", number(body, "bookkeepingInvocations"));
        result.put("actualMaterial", actualMaterial.evidence()); result.put("activeInputRowsAttempted", number(body, "inputRowsAttempted"));
        result.put("callerAdmissionCeilings", callerLimits.evidence()); result.put("withinCallerCeilings", withinCallerCeiling);
        result.put("countPairingComplete", complete); result.put("failedOperations", failed); result.put("pendingOperations", pending);
        result.put("producerState", body.get("state"));
        result.put("sourceCoverageVerified", false); result.put("classArtifactBindingVerified", false);
        result.put("callerCeilingsAuthoritative", false); result.put("futureInvocationBoundClaimed", false);
        result.put("sourceUpperIsActualWork", false); result.put("cpuOrElapsedConversionQualified", false);
        for (String flag : FLAGS) result.put(flag, false);
        return Map.copyOf(result);
    }

    private static Map<String, BenchmarkReturnWorkBudget.Work> table() {
        var table = new LinkedHashMap<String, BenchmarkReturnWorkBudget.Work>();
        // Both fixed operands fit in long; compiler folding adds no initializer/provider calls.
        table.put("CONFIG", work(1, CONFIG_REQUESTS, 0, 0, INPUT_BYTES * 8L + TRANSPORT_SOURCE_CHARS * 8L, 16,
                INPUT_BYTES * 16L + TRANSPORT_SOURCE_CHARS * 16L, INPUT_BYTES * 16L + TRANSPORT_SOURCE_CHARS * 16L));
        table.put("INSTALL", work(1, 24, 0, 0, 2048, 4, 256, 64));
        table.put("WRITER_INSTALL", work(1, 16, 0, 0, 2048, 4, 512, 64));
        table.put("WRITER_ROSTER", work(1, 4, 0, 0, 0, 1, 16, 16));
        table.put("WRITER_CLOSE_ATTEMPT", work(1, 0, 0, 0, 0, 1, 16, 16));
        table.put("WRITER_CLOSE", work(1, 0, 0, 0, 0, 1, 16, 16));
        table.put("WRITE_WRAPPER", work(1, 0, 0, 0, 0, 1, 32, 32));
        table.put("BEGIN", work(1, 12, 0, 0, 4096, 4, 512, 128));
        table.put("ACTIVE_BEGIN", work(1, 0, 0, 0, 0, 1, 16, 16));
        table.put("INACTIVE_BEGIN", work(1, 0, 0, 0, 0, 1, 16, 16));
        table.put("REJECTION", work(1, 0, 0, 0, 0, 1, 16, 16));
        table.put("IDENTITY_ROW", work(0, 128, 1, 0, 512, 4, 512, 128));
        table.put("IDENTITY_PIECE", work(0, 16, 0, 1, mul(FRAME_BYTES, 8), 8, mul(FRAME_BYTES, 8), 64));
        table.put("KEY_SHAPE", work(0, 32, 0, 0, 1024, 4, 512, 16));
        table.put("CALLBACK_WRAPPER", work(1, 0, 0, 0, 0, 1, 32, 16));
        table.put("CALLBACK", work(1, 16, 0, 0, 0, 2, 64, 64));
        table.put("CALLBACK_ERROR", work(0, 64, 0, 0, 4096, 8, 4096, 64));
        table.put("CALLBACK_EXIT", work(1, 4, 0, 0, 0, 1, 32, 32));
        table.put("RETURN_OBSERVATION", work(1, 4, 0, 0, 0, 1, 32, 16));
        table.put("COMPLETE", work(1, 16, 0, 0, 4096, 4, 512, 128));
        table.put("RECEIPT_PIECE", work(0, 32, 0, 1, mul(FRAME_BYTES, 8), 8, mul(FRAME_BYTES, 8), 128));
        table.put("PUBLICATION", work(0, 4, 0, 1, 0, 1, 16, 16));
        table.put("COST_AGGREGATION", work(1, 8, 0, 0, 0, 4, 64, 64));
        table.put("CLOCK_READ", work(1, 2, 0, 0, 0, 1, 16, 16));
        table.put("CLOCK_INITIALIZATION", work(1, CLOCK_REQUESTS, 0, 0, CLOCK_TRANSFER, 64, CLOCK_ELEMENTS, CLOCK_ELEMENTS));
        table.put("CLOCK_METADATA", work(1, CLOCK_REQUESTS, 0, 0, CLOCK_TRANSFER, 64, CLOCK_ELEMENTS, CLOCK_ELEMENTS));
        table.put("SCOPE_INSTALL", work(1, 256, 0, 0, 32768, 8, 4096, 256));
        table.put("SCOPE_CACHE_ENTRY", work(0, 4, 0, 0, 0, 1, 4, 4));
        table.put("SCOPE_BEFORE", work(1, 40, 0, 0, 32768, 4, 32768, 64));
        table.put("SCOPE_AFTER", work(1, 48, 0, 0, 32768, 6, 32768, 96));
        table.put("TRANSACTION_WRAPPER", work(1, 4, 0, 0, 0, 1, 16, 16));
        table.put("HASH_READ", work(0, 3, 0, 0, 0, 3, 0, 4));
        table.put("HASH_MATERIAL", work(0, 0, 0, 0, 4, 4, 4, 0));
        table.put("ARGUMENT", work(0, 16, 0, 0, 0, 4, 32, 16));
        table.put("ARGUMENT_MATERIAL", work(0, 0, 0, 0, 4, 4, 4, 0));
        table.put("MBEAN_SCALAR", work(1, 4, 0, 0, 64, 1, 8, 4));
        table.put("MBEAN_START", work(1, 8, 0, 0, 8192, 4, 4096, 128));
        table.put("MBEAN_STOP", work(1, 0, 0, 0, 0, 1, 16, 16));
        table.put("MBEAN_PAGE", work(1, 32, 0, 0, mul(FRAME_BYTES, 8), 8, mul(FRAME_BYTES, 8), 128));
        table.put("PAGE_FRAME_VISIT", work(0, 4, 0, 1, 0, 1, 8, 4));
        table.put("MBEAN_COST", work(1, 32, 0, 0, mul(8192, 4), 6, mul(8192, 4), 128));
        table.put("MBEAN_BUDGET", work(1, REPORT_REQUESTS, 0, 0, REPORT_BYTES, REPORT_PASSES, REPORT_ELEMENTS, REPORT_ELEMENTS));
        table.put("ARM", work(1, 64, 0, 0, 16384, 8, 16384, 128));
        table.put("SHUTDOWN", work(1, 16, 0, 0, 4096, 4, 512, 64));
        table.put("FINAL_OWNER", work(1, 32, 0, 0, 4096, 4, 512, 64));
        table.put("FINAL_WAIT_ITERATION", work(1, 4, 0, 0, 0, 2, 32, 16));
        table.put("BOOKKEEPING", BOOKKEEPING);
        table.put("COLD_ORIGINS", COLD_ORIGINS);
        require(table.keySet().equals(Set.copyOf(ACTORS)), "trusted source table is incomplete");
        return Map.copyOf(table);
    }

    private static void materialBounds(Object raw) {
        Map<?, ?> map = object(raw);
        require(map.size() == MATERIAL_FIELDS.size() && map.keySet().equals(Set.copyOf(MATERIAL_FIELDS)), "material roster differs");
        require(number(map, "connectorHashBytes") > 0 && number(map, "connectorHashBytes") <= 128L * 1024 * 1024
                        && number(map, "runtimeArguments") <= 128 && number(map, "runtimeArgumentBytes") <= 2093056
                        && number(map, "primaryKeys") == 2, "material constraints differ");
    }
    private static BenchmarkReturnWorkBudget.Work vector(Object raw) {
        Map<?, ?> map = object(raw);
        require(map.size() == UNITS.size() && map.keySet().equals(Set.copyOf(UNITS)), "unit roster differs");
        return work(number(map, "calls"), number(map, "requests"), number(map, "records"), number(map, "frames"),
                number(map, "bytes"), number(map, "passes"), number(map, "elements"), number(map, "updates"));
    }
    private static BenchmarkReturnWorkBudget.Work work(long c, long q, long r, long f, long b, long p, long e, long u) {
        return new BenchmarkReturnWorkBudget.Work(c, q, r, f, b, p, e, u);
    }
    private static Map<?, ?> object(Object raw) { require(raw instanceof Map<?, ?>, "required object is missing"); return (Map<?, ?>) raw; }
    private static long number(Map<?, ?> map, String key) {
        Object raw = map.get(key);
        require(raw instanceof Long || raw instanceof Integer, "strict integer is missing");
        long value = ((Number) raw).longValue(); require(value >= 0, "integer is negative"); return value;
    }
    private static boolean bool(Map<?, ?> map, String key) { require(map.get(key) instanceof Boolean, "boolean is missing"); return (Boolean) map.get(key); }
    private static long add(long... values) { long total = 0; for (long value : values) total = Math.addExact(total, value); return total; }
    private static long mul(long a, long b) { return Math.multiplyExact(a, b); }
    private static long chars(List<String> values) { long total = 0; for (String value : values) total = Math.addExact(total, value.length()); return total; }
    private static void require(boolean condition, String reason) { if (!condition) throw new AssertionError("producer source allocations " + reason); }
}
