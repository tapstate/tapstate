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

    record Geometry(int phases, int measuredPhases, int pipelines, int chains, int targets,
                    long measuredOutputs, long cohortRows, long measuredBatches, long allOutputs,
                    long targetRows, long coverageEntries, long sourceRows, long statements,
                    long nestedItems, long nestedRoots, long checksumCalls) { }

    /** Bounded scalar shape only; canonical source/checksum equality remains a separate caller check. */
    static Geometry geometry(BenchmarkReturnWorkBudgetProfile.WorkloadInput workload) {
        require(workload != null && workload.rows() == PILOT_ROWS && workload.seed() == 424242,
                "original pilot row or seed scalars differ");
        scalar(workload.id(), 9); scalar(workload.database(), 8);
        String[] ids; String[] stages; long[] outputs; int[] sql; int[] perBatch; int[] coverage;
        // The fixed sub-capacity offer of each measured phase; unmeasured phases issue without a cadence.
        long[] cadence;
        String database; int pipelines, chains, targets, setup;
        switch (workload.id()) {
            case "copy" -> {
                ids = new String[]{"snapshot", "warm-up", "cdc-update", "terminal"};
                stages = new String[]{"SNAPSHOT", "WARM_UP", "CDC_UPDATE", "TERMINAL"};
                outputs = new long[]{PILOT_ROWS, 128, PILOT_ROWS, 1};
                sql = new int[]{0, 1, 960, 1}; perBatch = new int[]{1, 1, 1, 1}; coverage = new int[]{1, 1, 1, 1};
                cadence = new long[]{0, 0, 80_000_000, 0};
                database = "MYSQL"; pipelines = 1; chains = 1; targets = 1; setup = 193;
            }
            case "stateless" -> {
                ids = new String[]{"snapshot", "warm-up", "cdc-settling-raised", "cdc-settling-restored", "cdc-update", "terminal"};
                stages = new String[]{"SNAPSHOT", "WARM_UP", "WARM_UP", "WARM_UP", "CDC_UPDATE", "TERMINAL"};
                // Fixed preflight2 and boundary258 both lie in the protected even first-quarter domain.
                outputs = new long[]{PILOT_ROWS, 256, 23996, 23996, PILOT_ROWS, 2};
                sql = new int[]{0, 1, 240, 240, 960, 1}; perBatch = new int[]{1, 1, 1, 1, 1, 1}; coverage = new int[]{1, 1, 1, 1, 1, 1};
                cadence = new long[]{0, 0, 0, 0, 80_000_000, 0};
                database = "POSTGRES"; pipelines = 1; chains = 1; targets = 1; setup = 194;
            }
            case "stateful" -> {
                ids = new String[]{"snapshot", "warm-up", "hot-state", "cold-read", "cdc-update", "terminal"};
                stages = new String[]{"SNAPSHOT", "WARM_UP", "HOT_STATE", "COLD_READ", "CDC_UPDATE", "TERMINAL"};
                outputs = new long[]{2 * PILOT_ROWS, 888, 64, PILOT_ROWS, 2 * PILOT_ROWS, 2};
                sql = new int[]{0, 4, 1, 960, 1920, 4}; perBatch = new int[]{1, 1, 1, 1, 2, 1}; coverage = new int[]{4, 4, 1, 1, 2, 4};
                cadence = new long[]{0, 0, 0, 200_000_000, 200_000_000, 0};
                database = "MYSQL"; pipelines = 2; chains = 4; targets = 2; setup = 581;
            }
            default -> throw new AssertionError("source tail workload selector differs");
        }
        require(database.equals(workload.database()) && workload.setupStatements() == setup
                        && workload.pipelines().size() == pipelines && workload.sourceChains().size() == chains
                        && workload.phases().size() == ids.length, "original pilot topology or setup scalars differ");
        // All caller roster caps are established before traversal, formatting or further copies.
        for (String value : workload.pipelines()) scalar(value, 64);
        for (String value : workload.sourceChains()) scalar(value, 512);
        long measured = 0, full = 0, batches = 0, all = 0, targetRows = 0, coverageEntries = 0, statements = setup;
        for (int index = 0; index < ids.length; index++) {
            var phase = workload.phases().get(index);
            require(phase != null, "phase scalar is missing"); scalar(phase.id(), 32); scalar(phase.stage(), 16);
            boolean selected = "cold-read".equals(ids[index]) || "cdc-update".equals(ids[index]);
            require(ids[index].equals(phase.id()) && stages[index].equals(phase.stage()) && phase.measured() == selected
                            && phase.logicalOutputs() == outputs[index] && phase.statements() == sql[index]
                            && phase.statementsPerBatch() == perBatch[index] && phase.batches() == sql[index] / perBatch[index]
                            && phase.cadenceNanos() == cadence[index]
                            && phase.targets().size() == targets && phase.logicalCoverage().size() == coverage[index],
                    "original phase scalar recipe differs");
            for (var entry : phase.logicalCoverage().entrySet()) {
                scalar(entry.getKey(), 64);
                require(entry.getValue() != null && entry.getValue() >= 0 && entry.getValue() <= 3 * PILOT_ROWS + 32,
                        "logical coverage scalar exceeds the fixed domain");
            }
            for (var target : phase.targets()) {
                require(target != null, "target scalar is missing");
                scalar(target.pipeline(), 64); scalar(target.table(), 64); scalar(target.location(), 32); scalar(target.projection(), 16);
                scalar(target.checksum(), 64);
                require(target.checksum().matches("[0-9a-f]{64}"), "target checksum scalar is not canonical");
                long terminal = index == ids.length - 1 ? (workload.id().equals("stateless") ? 2 : 1) : 0;
                require(target.rows() == PILOT_ROWS + terminal, "target row scalar differs from the fixed recipe");
                targetRows = add(targetRows, target.rows());
            }
            statements = add(statements, phase.statements()); coverageEntries = add(coverageEntries, phase.logicalCoverage().size());
            all = add(all, phase.logicalOutputs());
            if (selected) { measured++; full = add(full, phase.logicalOutputs()); batches = add(batches, phase.batches()); }
        }
        boolean stateful = "stateful".equals(workload.id());
        // Source tuple lambdas: three N setup domains plus32 customers,256 warm,64 hot,N cold,
        // and four fixed terminal tuples. Other recipes render their N source rows and one terminal.
        long sourceRows = stateful ? add(mul(4, PILOT_ROWS), 356) : PILOT_ROWS + 1;
        // Across the six stateful target recipes: six base lists, five warm256 sets, four hot64
        // sets, three cold-N sets and one terminal item. A single root list never exceeds67.
        long nestedItems = stateful ? add(mul(9, PILOT_ROWS), 5 * 256L, 4 * 64L, 1) : 0;
        long nestedRoots = stateful ? add(mul(6, PILOT_ROWS), 1) : 0;
        return new Geometry(ids.length, Math.toIntExact(measured), pipelines, chains, targets, full, full / 2, batches,
                all, targetRows, coverageEntries, sourceRows, statements, nestedItems, nestedRoots, mul(ids.length, targets));
    }

    private static Tail factory(BenchmarkReturnWorkBudgetProfile.WorkloadInput workload) {
        Geometry shape = geometry(workload);
        // No call to original()/steadyPilot() is made here: this source tail must not construct an
        // eighth fixture merely to account for the seven explicitly registered caller constructs.
        long selectedRecords = add(shape.sourceRows(), shape.coverageEntries(), shape.targetRows(), shape.nestedItems());
        long selectedChars = add(mul(shape.sourceRows(), ROW_CHARS), mul(shape.coverageEntries(), FIELD_CHARS),
                mul(shape.targetRows(), ROW_CHARS), mul(shape.statements(), SQL_PREFIX_CHARS), mul(shape.nestedItems(), FIELD_CHARS));
        // ALL constructs copy/stateless/stateful at 12k once. Their target-line rosters are 4N+1,
        // 4N+2, and 12N+2; setup is N+N+3N+32. The stateful nested list has at most 67 items.
        long initializerCoverage = 4 + 4 + 16L;
        long initializerTargets = add(mul(20, INITIALIZER_ROWS), 5);
        long nestedMaterial = add(mul(9, INITIALIZER_ROWS), 5 * 256L, 4 * 64L, 1);
        // Shared ALL initializer source rendering includes stateful warm/hot/cold/terminal rows.
        long initializerSourceRows = add(mul(6, INITIALIZER_ROWS), 358);
        long initializerRecords = add(initializerSourceRows, initializerCoverage, initializerTargets, nestedMaterial);
        long initializerStatements = 147 + 148 + 446L;
        long initializerChars = add(mul(initializerSourceRows, ROW_CHARS), mul(initializerCoverage, FIELD_CHARS),
                mul(initializerTargets, ROW_CHARS), mul(nestedMaterial, FIELD_CHARS), mul(initializerStatements, SQL_PREFIX_CHARS));
        long records = add(mul(FACTORY_CALLS, selectedRecords), initializerRecords);
        long characters = add(mul(FACTORY_CALLS, selectedChars), initializerChars);
        // Request slots per row: six scalar renderings, one concatenation, one list insertion,
        // three checksum UTF-8/update operations, and four map/key/boxing operations. Sorting and
        // providers are opaque requests; their instructions/CPU and comparator interiors are excluded.
        long rowRequestSlots = add(ROW_FIELDS, 1, 1, 3, 4);
        long targetSortCalls = add(mul(FACTORY_CALLS, shape.checksumCalls() + shape.nestedRoots()),
                4, 4, 12, mul(6, INITIALIZER_ROWS), 1);
        // The owned comparator performs indexOf/substring/parseLong. At most2m^2 key extracts
        // per m-item list and m<=67 imply <=2*67*allDeclaredItems, independent of observed counts.
        long comparatorVisits = mul(2L * NEST_ITEMS_PER_ROOT, add(mul(FACTORY_CALLS, shape.nestedItems()), nestedMaterial));
        long comparatorChars = mul(comparatorVisits, 20); // generated decimal long prefixes precede the colon
        long scalarSlots = add(mul(shape.phases(), 32), mul(ROOT_ACTIVITIES, UNITS), SELF_COUNTERS);
        long recipeChars = add(128, 64, 20, 20, mul(ROOT_ACTIVITIES, add(128, mul(UNITS, 21))));
        // Two ReportPlan shape guards and two factoryEvidence shape guards precede activation.
        var shapeChecks = geometryUpper().times(4);
        long requests = add(mul(records, rowRequestSlots), targetSortCalls, mul(4, comparatorVisits),
                mul(FACTORY_CALLS, scalarSlots), mul(3, recipeChars), 3, shapeChecks.requests());
        long elements = add(mul(records, ROW_FIELDS), mul(FACTORY_CALLS, scalarSlots), recipeChars, comparatorVisits);
        long bytes = add(mul(4, characters), mul(4, recipeChars), mul(4, comparatorChars), shapeChecks.bytes());
        var upper = work(FACTORY_CALLS + 3L, requests, records, 0, bytes,
                add(mul(FACTORY_CALLS, 8), 3 * 8L, targetSortCalls, shapeChecks.passes()),
                add(elements, shapeChecks.elements()), add(elements, comparatorVisits, shapeChecks.updates()))
                .plus(BenchmarkSharedSupportSourceWork.factoryUpper())
                // The two existing profile inspections each assemble one new catalogue and one
                // component table. The reused private token adds one cached-definition pipeline;
                // the known readless authority has one fixed owned initializer, without IO.
                .plus(BenchmarkReturnComponentSourceWork.factoryUpper(2))
                .plus(BenchmarkReturnCaptureMaterialBounds.factoryUpper().times(2))
                .plus(BenchmarkReturnWorkBudgetReportPlan.returnCatalogueUpper(2))
                .plus(BenchmarkReturnComponentSourceWork.trustedCacheUpper())
                .plus(BenchmarkReturnComponentSourceWork.authorityInitializationUpper())
                .plus(BenchmarkReturnWorkBudgetRun.nativeClassPacketRosterFactoryUpper())
                .plus(BenchmarkReturnBridgeSourceWork.addedPreScopeUpper());
        var evidence = metadata("FACTORY_SOURCE_UPPER_ONLY", upper, List.of(
                "SEVEN_ORIGINAL_SELECTED_96000_CONSTRUCTS", "ONE_ALL_COPY_STATELESS_STATEFUL_12000_INITIALIZER",
                "SETUP_SQL_ROW_TEMPLATES_500_BATCHES_AND_UPDATE_RANGES100", "ORIGINAL_FOUR_OR_SIX_PHASE_COVERAGE_AND_TARGET_CHECKSUM_TRAVERSALS",
                "BOUNDED_NESTED_ITEM_MATERIAL67_PER_ROOT", "SCALAR_WORKLOAD_PROJECTION_AND_CANONICAL_SOURCE_RECIPE"));
        evidence.put("factoryCalls", FACTORY_CALLS); evidence.put("initializerFactories", 3);
        evidence.put("selectedRecordsPerCall", selectedRecords); evidence.put("initializerRecords", initializerRecords);
        evidence.put("fixedFactoryRecipe", workload.id() + ":96000:424242:" + workload.database() + ":" + shape.phases());
        evidence.put("selectedSourceRows", shape.sourceRows()); evidence.put("selectedTargetRows", shape.targetRows());
        evidence.put("selectedNestedItems", shape.nestedItems()); evidence.put("selectedNestedRoots", shape.nestedRoots());
        evidence.put("nestedComparatorKeyExtractionsUpper", comparatorVisits);
        evidence.put("nativeClassPacketRosterFactories", 1);
        evidence.put("nativeClassPacketRosterFactoryUpper", BenchmarkReturnWorkBudgetRun.nativeClassPacketRosterFactoryUpper().evidence());
        evidence.put("returnBridgeTableConstructions", 2);
        evidence.put("returnBridgeAddedPreScopeUpper", BenchmarkReturnBridgeSourceWork.addedPreScopeUpper().evidence());
        evidence.put("sourceStepsIdentity", "SEVEN_SELECTED_PLUS_SINGLE_BOUNDED_ALL_V1");
        evidence.put("returnComponentTableConstructions", 2);
        evidence.put("captureMaterialTableConstructions", 2);
        evidence.put("captureMaterialFactoryUpper", BenchmarkReturnCaptureMaterialBounds.factoryUpper().times(2).evidence());
        evidence.put("returnComponentCatalogueConstructions", 2);
        evidence.put("privateAuthenticatedProfileCacheConstructions", 1);
        evidence.put("knownReadlessAuthorityInitializers", 1);
        evidence.put("returnComponentFactoryUpper", BenchmarkReturnComponentSourceWork.factoryUpper(2).evidence());
        evidence.put("returnComponentCatalogueUpper", BenchmarkReturnWorkBudgetReportPlan.returnCatalogueUpper(2).evidence());
        evidence.put("privateAuthenticatedProfileCacheUpper", BenchmarkReturnComponentSourceWork.trustedCacheUpper().evidence());
        evidence.put("knownReadlessAuthorityInitializerUpper", BenchmarkReturnComponentSourceWork.authorityInitializationUpper().evidence());
        return new Tail(upper, evidence);
    }

    static BenchmarkReturnWorkBudget.Work geometryUpper() {
        // Fixed six-phase/four-chain/two-target shape,48 local descriptor scalars and arrays.
        // Twelve source API slots cover each length/accessor/iterator/equals/checked field visit.
        long fields = 48 + PHASES * (32 + 4 * 4L + 2 * 8L);
        return work(0, 128 + 12 * fields, 0, 0, 32 * fields + 4 * 512 * 4L, 4, 4 * fields, 2 * fields);
    }

    private static void scalar(String value, int maximum) {
        require(value != null && value.length() <= maximum, "source scalar exceeds its prior character cap");
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
