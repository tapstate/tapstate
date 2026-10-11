package io.tapstate.e2e;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/** Fixed source tails excluded from a preceding live counter prefix; completion is bound externally. */
final class BenchmarkReturnWorkBudgetSourceTails {
    static final int FACTORY_CALLS = 7, ROOT_ACTIVITIES = 57, SELF_COUNTERS = 20, UNITS = 8;
    private static final long PILOT_ROWS = 96000, INITIALIZER_ROWS = 12000, PENDING = 128;
    private static final int PHASES = 6, NEST_ITEMS_PER_ROOT = 1 + 1 + 64 + 1;
    private static final long ROW_FIELDS = 6, FIELD_CHARS = 64, ROW_CHARS = ROW_FIELDS * (FIELD_CHARS + 3);
    private static final long SQL_PREFIX_CHARS = 4 * FIELD_CHARS;
    private BenchmarkReturnWorkBudgetSourceTails() { }

    static BenchmarkReturnWorkBudget.Work factoryUpper(BenchmarkReturnWorkBudgetProfile.WorkloadInput workload) {
        return factory(workload).upper();
    }
    static BenchmarkReturnWorkBudget.Work terminalUpper(BenchmarkReturnWorkBudgetDefinitions.OutputPlan output) {
        return terminal(output).upper();
    }
    static Map<String, Object> factoryEvidence(BenchmarkReturnWorkBudgetProfile.WorkloadInput workload) { return factory(workload).evidence(); }
    static Map<String, Object> terminalEvidence(BenchmarkReturnWorkBudgetDefinitions.OutputPlan output) { return terminal(output).evidence(); }

    private record Tail(BenchmarkReturnWorkBudget.Work upper, Map<String, Object> evidence) {
        private Tail { evidence = Map.copyOf(evidence); }
    }

    private static Tail factory(BenchmarkReturnWorkBudgetProfile.WorkloadInput workload) {
        require(workload != null && "stateless".equals(workload.id()) && workload.rows() == PILOT_ROWS
                        && workload.seed() == 424242 && "POSTGRES".equals(workload.database())
                        && workload.pipelines().size() == 1 && workload.sourceChains().size() == 1
                        && workload.phases().size() == PHASES, "original stateless scalar recipe is missing");
        // No call to original()/steadyPilot() is made here: this source tail must not construct an
        // eighth fixture merely to account for the seven explicitly registered caller constructs.
        long targetRows = 0, coverageRows = 0, statements = workload.setupStatements();
        for (var phase : workload.phases()) {
            require(phase.statements() >= 0 && phase.statements() <= PILOT_ROWS && phase.statementsPerBatch() == 1
                            && phase.batches() == phase.statements() && phase.targets().size() == 1
                            && phase.logicalCoverage().size() <= PILOT_ROWS + 1
                            && phase.cadenceNanos() == (phase.measured() ? 1_000_000 : 0), "phase material exceeds the original source domain");
            statements = add(statements, phase.statements()); coverageRows = add(coverageRows, phase.logicalCoverage().size());
            for (var target : phase.targets()) {
                require(target.rows() > 0 && target.rows() <= PILOT_ROWS + 2, "target row recipe is outside its domain");
                targetRows = add(targetRows, target.rows());
            }
        }
        long selectedRecords = add(PILOT_ROWS, coverageRows, targetRows);
        long selectedChars = add(mul(PILOT_ROWS, ROW_CHARS), mul(coverageRows, FIELD_CHARS),
                mul(targetRows, ROW_CHARS), mul(statements, SQL_PREFIX_CHARS));
        // ALL constructs copy/stateless/stateful at 12k once. Their target-line rosters are 4N+1,
        // 4N+2, and 12N+2; setup is N+N+3N+32. The stateful nested list has at most 67 items.
        long initializerSetup = add(mul(5, INITIALIZER_ROWS), 32);
        long initializerCoverage = add(mul(2, INITIALIZER_ROWS), 128, 1,
                INITIALIZER_ROWS, INITIALIZER_ROWS / 2, 128, 1,
                mul(6, INITIALIZER_ROWS), 32, 514, 64, 4);
        long initializerTargets = add(mul(20, INITIALIZER_ROWS), 5);
        long nestedMaterial = mul(mul(PHASES, INITIALIZER_ROWS + 1), NEST_ITEMS_PER_ROOT);
        long initializerRecords = add(initializerSetup, initializerCoverage, initializerTargets, nestedMaterial);
        long initializerChars = add(mul(initializerSetup, ROW_CHARS), mul(initializerCoverage, FIELD_CHARS),
                mul(initializerTargets, ROW_CHARS), mul(nestedMaterial, FIELD_CHARS));
        long records = add(mul(FACTORY_CALLS, selectedRecords), initializerRecords);
        long characters = add(mul(FACTORY_CALLS, selectedChars), initializerChars);
        // Request slots per row: six scalar renderings, one concatenation, one list insertion,
        // three checksum UTF-8/update operations, and four map/key/boxing operations. Sorting and
        // providers are opaque requests; their instructions/CPU and comparator interiors are excluded.
        long rowRequestSlots = add(ROW_FIELDS, 1, 1, 3, 4);
        long targetSortCalls = add(mul(FACTORY_CALLS, PHASES), 4, 4, 12, mul(PHASES, INITIALIZER_ROWS + 1));
        long scalarSlots = add(mul(PHASES, 32), mul(ROOT_ACTIVITIES, UNITS), SELF_COUNTERS);
        long recipeChars = add(128, 64, 20, 20, mul(ROOT_ACTIVITIES, add(128, mul(UNITS, 21))));
        long requests = add(mul(records, rowRequestSlots), targetSortCalls,
                mul(FACTORY_CALLS, scalarSlots), mul(3, recipeChars), 3);
        long elements = add(mul(records, ROW_FIELDS), mul(FACTORY_CALLS, scalarSlots), recipeChars);
        long bytes = add(mul(4, characters), mul(4, recipeChars));
        var upper = work(FACTORY_CALLS + 3L, requests, records, 0, bytes,
                add(mul(FACTORY_CALLS, 8), 3 * 8L, targetSortCalls), elements, elements)
                .plus(BenchmarkSharedSupportSourceWork.factoryUpper());
        var evidence = metadata("FACTORY_SOURCE_UPPER_ONLY", upper, List.of(
                "SEVEN_ORIGINAL_STATELESS_96000_CONSTRUCTS", "ONE_ALL_COPY_STATELESS_STATEFUL_12000_INITIALIZER",
                "SETUP_SQL_ROW_TEMPLATES_500_BATCHES_AND_UPDATE_RANGES100", "SIX_PHASE_COVERAGE_AND_TARGET_CHECKSUM_TRAVERSALS",
                "BOUNDED_NESTED_ITEM_MATERIAL67_PER_ROOT", "SCALAR_WORKLOAD_PROJECTION_AND_CANONICAL_SOURCE_RECIPE"));
        evidence.put("factoryCalls", FACTORY_CALLS); evidence.put("initializerFactories", 3);
        evidence.put("selectedRecordsPerCall", selectedRecords); evidence.put("initializerRecords", initializerRecords);
        evidence.put("fixedFactoryRecipe", "stateless:96000:424242:POSTGRES:6");
        evidence.put("sourceStepsIdentity", "SEVEN_SELECTED_PLUS_SINGLE_BOUNDED_ALL_V1");
        return new Tail(upper, evidence);
    }

    private static Tail terminal(BenchmarkReturnWorkBudgetDefinitions.OutputPlan output) {
        require(output != null, "registered terminal output plan is missing");
        // Admission parses at most three eight-unit vectors per row, checks counts, recomputes two
        // total vectors and verifies self arithmetic. Map/JDK/checked arithmetic/boxing stay requests.
        long verificationRowRequests = add(16, mul(10, UNITS));
        long verificationRequests = add(mul(ROOT_ACTIVITIES, verificationRowRequests), mul(SELF_COUNTERS, 8),
                mul(UNITS, 32), 128);
        long snapshotNodes = add(1024, mul(128, ROOT_ACTIVITIES), mul(8, PENDING));
        long snapshotChars = add(4096, mul(1024, ROOT_ACTIVITIES), mul(128, PENDING));
        // finish verifies each of 57 rows and emits one immutable snapshot. Source fields include
        // actual/per-operation/upper vectors, 20 self counters, output binding and release source tail.
        long finishRequests = add(mul(ROOT_ACTIVITIES, add(80, mul(4, UNITS))),
                mul(16, snapshotNodes), mul(2, snapshotChars), mul(SELF_COUNTERS, 2), 1);
        // JsonWriter's map/iterator/string/scalar paths have at most sixteen API calls per node or
        // source text character; UTF-8 scans and final printer consume the independently planned bytes.
        long encodingBytes = output.encodedUpperBytes();
        long renderRequests = add(128, mul(16, add(output.nodeUpper(), output.stringUtf8Upper())), mul(2, encodingBytes));
        long releaseRequests = add(1, 1, 1); // close counter, atomic scope clear, and method return.
        long requests = add(verificationRequests, finishRequests, renderRequests, 1, 1, releaseRequests);
        long bytes = add(mul(32, snapshotNodes), mul(6, snapshotChars), mul(3, encodingBytes));
        long elements = add(mul(ROOT_ACTIVITIES, mul(8, UNITS)), mul(SELF_COUNTERS, 4), snapshotNodes,
                output.nodeUpper(), output.stringUtf8Upper(), encodingBytes, 4);
        long updates = add(mul(ROOT_ACTIVITIES, 32), mul(SELF_COUNTERS, 4), snapshotNodes,
                mul(2, add(output.nodeUpper(), output.stringUtf8Upper())), 4);
        var upper = work(1, requests, 0, 0, bytes,
                add(mul(ROOT_ACTIVITIES, 8), SELF_COUNTERS, snapshotNodes, output.nodeUpper(), 4), elements, updates);
        var evidence = metadata("TERMINAL_SOURCE_UPPER_REQUIRING_EXTERNAL_AFTER_RECEIPT", upper, List.of(
                "INDEPENDENT_ADMISSION_57_ACTIVITY_20_SELF_EIGHT_UNIT_VERIFICATION", "FINAL_FINISH_ROW_CHECKS_AND_IMMUTABLE_SNAPSHOT",
                "BOUNDED_JSONWRITER_NODE_TEXT_AND_SCALAR_RENDERING", "PRIOR_UTF8_UPPER_ONE_PRINTLN_AND_METHOD_RETURN",
                "ONE_SCOPE_CLOSE_COUNTER_AND_ATOMIC_PRIVATE_RELEASE"));
        evidence.put("outputPlanIdentity", planIdentity(output)); evidence.put("outputPlanSha256", planHash(output));
        evidence.put("rootActivityRows", ROOT_ACTIVITIES); evidence.put("selfCounterFields", SELF_COUNTERS);
        evidence.put("unitFields", UNITS); evidence.put("releaseCalls", 1);
        evidence.put("verificationRequestsUpper", verificationRequests); evidence.put("finishRequestsUpper", finishRequests);
        evidence.put("renderRequestsUpper", renderRequests);
        return new Tail(upper, evidence);
    }

    private static java.util.LinkedHashMap<String, Object> metadata(String state, BenchmarkReturnWorkBudget.Work upper, List<String> steps) {
        var data = new java.util.LinkedHashMap<String, Object>();
        data.put("state", state); data.put("upper", upper.evidence()); data.put("sourceSteps", steps);
        data.put("actualCompletionObserved", false); data.put("futureLifetimeCountBoundClaimed", false);
        data.put("opaqueExcluded", "JDK_SORT_PROVIDER_PRINTSTREAM_CPU_ELAPSED_AND_EXTERNAL_SINK_INTERIORS");
        data.put("observedEncodedBytesUsedAsPriorBound", false);
        data.put("structuralWorkBudgetQualified", false); data.put("wholeMethodCostQualified", false);
        data.put("costAcceptanceEligible", false); data.put("performanceAcceptanceEligible", false); data.put("formalPerformance", false);
        return data;
    }
    private static Map<String, Object> planIdentity(BenchmarkReturnWorkBudgetDefinitions.OutputPlan output) {
        return Map.of("nodeUpper", output.nodeUpper(), "depthUpper", output.depthUpper(), "leafUtf8Upper", output.leafUtf8Upper(),
                "stringUtf8Upper", output.stringUtf8Upper(), "keyUtf8Upper", output.keyUtf8Upper(), "numberTextUpper", output.numberTextUpper());
    }
    private static String planHash(BenchmarkReturnWorkBudgetDefinitions.OutputPlan output) {
        String text = "TERMINAL_SOURCE_PLAN_V1:" + output.nodeUpper() + ":" + output.depthUpper() + ":" + output.leafUtf8Upper()
                + ":" + output.stringUtf8Upper() + ":" + output.keyUtf8Upper() + ":" + output.numberTextUpper();
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.US_ASCII))); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static BenchmarkReturnWorkBudget.Work work(long c, long q, long r, long f, long b, long p, long e, long u) {
        return new BenchmarkReturnWorkBudget.Work(c, q, r, f, b, p, e, u);
    }
    private static long add(long... values) { long total = 0; for (long value : values) total = Math.addExact(total, value); return total; }
    private static long mul(long a, long b) { return Math.multiplyExact(a, b); }
    private static void require(boolean condition, String reason) { if (!condition) throw new AssertionError("source tail " + reason); }
}
