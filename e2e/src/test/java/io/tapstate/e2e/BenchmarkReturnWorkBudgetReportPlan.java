package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Prior material envelope for a complete fixed pilot report or one controlled diagnostic packet. */
final class BenchmarkReturnWorkBudgetReportPlan {
    private static final long RAW_BYTES = 2L * 1024 * 1024;
    private static final int PAGE_BYTES = 65536, PAGES = 512, FRAMES = 512, RETURN_ROWS = 262144;
    private static final int CLOCK_SAMPLES = 512, RESOURCE_ATTEMPTS = 4096;
    private static final int SOURCE_PACKET_NODES = 4096, SOURCE_PACKET_DEPTH = 16;
    private static final int SOURCE_LEAF_CHARS = 65536, SOURCE_PACKET_BYTES = 2 * 1024 * 1024;
    private static final int OWNED_RECEIPT_BYTES = 2 * 1024 * 1024, OWNED_RECEIPT_DEPTH = 8;
    private static final int PRODUCER_ACTORS = 48, ROOT_ACTORS = 57, PENDING = 128;
    private static final int WRITERS = 4;
    private static final int OUTER_DEPTH = 12;
    private static final int SOURCE_KEY_BYTES = 4 * SOURCE_LEAF_CHARS;
    private static final int MAX_LEAF = Math.max(SOURCE_KEY_BYTES, base64(PAGE_BYTES));

    record Component(String id, String sourceBranch, long rows, long nodesPerRow, long stringsPerRow,
                     int leafUtf8Upper, int localDepthUpper, String boundBasis) {
        Component {
            require(id != null && sourceBranch != null && boundBasis != null && rows >= 0
                            && nodesPerRow > 0 && stringsPerRow >= 0 && leafUtf8Upper > 0 && localDepthUpper > 0,
                    "source component is incomplete");
            Math.multiplyExact(rows, nodesPerRow); Math.multiplyExact(rows, stringsPerRow);
        }
        long nodes() { return Math.multiplyExact(rows, nodesPerRow); }
        long strings() { return Math.multiplyExact(rows, stringsPerRow); }
        Map<String, Object> evidence() {
            return Map.of("id", id, "sourceBranch", sourceBranch, "rows", rows, "nodesPerRow", nodesPerRow,
                    "stringUtf8PerRow", stringsPerRow, "leafUtf8Upper", leafUtf8Upper,
                    "localDepthUpper", localDepthUpper, "boundBasis", boundBasis);
        }
    }
    record Prepared(BenchmarkReturnWorkBudgetDefinitions.OutputPlan plan, List<Component> components,
                    Map<String, Object> metadata) {
        Prepared { components = List.copyOf(components); metadata = Map.copyOf(metadata); }
    }
    private BenchmarkReturnWorkBudgetReportPlan() { }

    static BenchmarkReturnWorkBudgetDefinitions.OutputPlan completePilot(BenchmarkReturnWorkBudgetProfile.WorkloadInput workload) {
        return prepare(workload).plan();
    }

    static Prepared prepare(BenchmarkReturnWorkBudgetProfile.WorkloadInput workload) {
        require(workload != null && "stateless".equals(workload.id()) && workload.rows() == 96000 && workload.seed() == 424242,
                "only the unchanged stateless pilot is registered");
        require(workload.equals(BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateless")),
                "original pipeline, target, phase, coverage or cadence inputs differ");
        long measuredPhases = workload.phases().stream().filter(BenchmarkReturnWorkBudgetProfile.PhaseInput::measured).count();
        require(measuredPhases == 1 && workload.pipelines().size() == 1 && workload.sourceChains().size() == 1,
                "fixed pilot topology differs");
        var phase = workload.phases().stream().filter(BenchmarkReturnWorkBudgetProfile.PhaseInput::measured).findFirst().orElseThrow();
        require(phase.logicalOutputs() == 96000 && phase.batches() == 960 && phase.targets().size() == 1,
                "fixed pilot output or source batch roster differs");
        long full = phase.logicalOutputs(), cohort = full / 2;
        long sourceCoverage = 0, allOutputs = 0;
        for (var candidate : workload.phases()) {
            sourceCoverage = Math.addExact(sourceCoverage, candidate.logicalCoverage().size());
            allOutputs = Math.addExact(allOutputs, candidate.logicalOutputs());
        }
        long chains = workload.sourceChains().size();
        var parts = new ArrayList<Component>();
        // Scalar maps: raw fork, fork summary, phase, timeline, schedule, confirmation, anchor,
        // resources, interval/rate, common-window certificate, root CPU scope, and disabled branches.
        int[] scalarFields = {29, 24, 50, 14, 8, 10, 5, 6, 9, 40, 10, 12};
        long scalarNodes = 0, scalarStrings = 0;
        for (int fields : scalarFields) {
            scalarNodes = Math.addExact(scalarNodes, mapNodes(fields));
            scalarStrings = Math.addExact(scalarStrings, Math.multiplyExact(fields, 128L + 512));
        }
        add(parts, "SCALAR_REPORT_SCAFFOLDS", "RealBenchmarkForkDriverIT.run/PipelineBenchmarkLiveRunIT.fork/phaseEvidence",
                1, scalarNodes, scalarStrings, 512, 4, "FIXED_MAP_FIELD_INVENTORIES_AND_LITERAL_TEXT");
        add(parts, "FORK_DELIVERY_LATENCIES", "PipelineBenchmarkLiveRunIT.fork.deliveryNanos",
                cohort + 1, 1, 0, 32, 2, "ORIGINAL_COMPLETE_MIDDLE_HALF");
        add(parts, "FULL_AND_COHORT_TIMELINES", "PipelineBenchmarkLiveRunIT.phaseEvidence.deliveryTimeline",
                Math.addExact(full, Math.multiplyExact(3, cohort)) + 5, 1, 0, 32, 4,
                "FULL_OBSERVATION_PLUS_COHORT_OBSERVATION_OPERATION_AND_STREAM_ARRAYS");
        long coverageEntries = Math.addExact(sourceCoverage, Math.addExact(Math.multiplyExact(2, allOutputs), chains));
        add(parts, "ALL_COVERAGE_MAPS", "PipelineBenchmarkLiveRunIT.fork.declaredSourceCoverage/observedTargetCoverage/logicalCoverage",
                coverageEntries + 3, 2, 128, 128, 2, "SUM_ALL_ORIGINAL_PHASE_DOMAINS_WITHOUT_DEDUPLICATION");
        add(parts, "SOURCE_BATCH_DOUBLE_REPRESENTATION", "phaseEvidence.sourceBatches/BenchmarkWriteReturnPhaseEvidence.sourceBatches",
                phase.batches(), mapNodes(8) + mapNodes(3), 8L * 64 + 4L * 40 + 3L * 64, 128, 4,
                "ORIGINAL_960_BATCH_ROSTER_IN_BOTH_MAP_SHAPES");
        // A single encoded tree can include a full resource trace and its resource-bounds projection,
        // plus the retained refusal trace. These are separate representations of the same attempts.
        add(parts, "RESOURCE_TRACES_AND_REFUSALS", "samplingEvidence/BenchmarkReturnResourceBounds/collectorSamplingRefusal",
                3L * RESOURCE_ATTEMPTS, mapNodes(11) + 2L * 5, 11L * 64 + 4L * 40 + 64 + MAX_LEAF,
                MAX_LEAF, 6, "RETAINED_ATTEMPT_CAP_4096_WITH_READING_AND_UTC_FIELDS");
        add(parts, "RAW_RETURN_BASE64", "BenchmarkWriteReturnPhaseEvidence.pagesBase64/BenchmarkWriteReturnCapture.retainedEvidence",
                1, 2L * (PAGES + 1), 2L * base64Total(RAW_BYTES, PAGES), base64(PAGE_BYTES), 3,
                "COMPLETE_RAW_TWO_MIB_WITH_INDEPENDENT_PADDING_FOR_EACH_OF_512_PAGES");
        add(parts, "RETURN_CLOCK_ROSTERS", "BenchmarkWriteReturnPhaseEvidence.clockSamples/Capture.retainedEvidence.samples",
                2L * CLOCK_SAMPLES, mapNodes(6), 6L * 64, 128, 4, "COMPLETE_PERIODIC_CAP_512_NO_CONTROL_SAMPLES_INSERTED");
        add(parts, "RETURN_CALL_AND_ROW_REPRESENTATION", "BenchmarkWriteReturnLedger/Assembly/Expectations retained diagnostics",
                FRAMES, mapNodes(24), 24L * 128, 512, 6, "FRAME_CAP_512_CALL_ROW_CAP_1024_NO_FULL_ROW_METADATA_DUPLICATION");
        add(parts, "RETURN_ROWS_IF_MATERIALIZED", "bounded return row/key/association failure projections",
                RETURN_ROWS, mapNodes(12), 12L * 128, 512, 8, "DERIVED_512_PIECES_TIMES_512_ROWS_INCLUDES_PARTIAL_REFUSALS");
        add(parts, "TABLE_CONFIRMATIONS", "PipelineBenchmarkLiveRunIT.tableConfirmation/terminalMetaReceipts/sourceChains",
                2L * chains, SOURCE_PACKET_NODES + mapNodes(11) + WRITERS + 1,
                PAGE_BYTES + 2L * MAX_LEAF + 11L * 128 + WRITERS * 256L, MAX_LEAF, SOURCE_PACKET_DEPTH,
                "PRIOR_64_KIB_JSON_DEPTH16_TOKEN4096_SOURCE_TOKEN_AND_FOUR_WRITER_BINDING");
        add(parts, "OWNED_RUNTIME_AND_FAILURE_RECEIPT", "BenchmarkOwnedProcessReceipt.buildEvidence/runtime-at-failure",
                2, OWNED_RECEIPT_BYTES / 16L, OWNED_RECEIPT_BYTES, 4 * 8192, OWNED_RECEIPT_DEPTH,
                "WHOLE_RECEIPT_TWO_MIB_16_BYTE_MINIMUM_NODE_CHARGE_DEPTH8_ARGUMENTS512_STRING8192");
        // Both cold native reads contain owned/root parsed and raw evidence. The source guards apply
        // before local consumption; this uses their caps, not a sampled encoded packet size.
        add(parts, "NATIVE_METADATA_AND_REGISTRATIONS", "targetClockQualification.nativeClockBefore/nativeClockAfter/registrationBefore/After",
                8, SOURCE_PACKET_NODES, SOURCE_PACKET_BYTES, MAX_LEAF, SOURCE_PACKET_DEPTH,
                "TWO_COLD_READS_FOUR_ROOT_OWNED_RAW_PARSED_PACKET_FORMS");
        add(parts, "PRODUCER_COLD_ORIGIN_PACKETS", "nativeClockBefore/nativeClockAfter/producerClassMetadata",
                6, SOURCE_PACKET_NODES, 2L * 2080768 + 8192, 64512, 8,
                "TWO_COLD_READS_TWO_DIRECT_AND_TWO_NESTED_POST_EXIT_BINDINGS_PLAIN_RAW34_AND_TYPED32_PRIOR_PART_CAPS");
        add(parts, "ROOT_CLASS_ORIGIN_PACKETS", "actualInputBindings.actualRootOrigins/producer.actualRootOrigins",
                2, 128L + 512L * mapNodes(7), 32768L + 512L * (7L * 128 + 256 + 4096 + 1024 + 64 + 256),
                4096, 5, "FIXED_512_NAMED_CLASSES_SEVEN_ROW_FIELDS_ASCII_PATH4096_ENTRY1024_NAME256_SHA64_HEADER128_NODES");
        add(parts, "EMITTED_COMPILER_ROSTER", "actualInputBindings.actualEmittedRoster",
                1, 128L + BenchmarkReturnWorkBudgetBindings.SourceRole.values().length * mapNodes(4) + 512L * mapNodes(7),
                32768L + BenchmarkReturnWorkBudgetBindings.SourceRole.values().length * (4L * 128 + 128 + 512 + 64)
                        + 512L * (7L * 128 + 128 + 256 + 256 + 64 + 4096 + 1024),
                4096, 5, "THREE_OWNED_COMPILER_OUTPUTS_ALL_NAMED_SOURCE_ROLES_512_SELECTED_MEMBERS_PRIOR_FIELD_CAPS");
        add(parts, "NATIVE_BASELINE_CHECKPOINTS", "BenchmarkNativeCounterBaseline.evidence.before/after",
                2L * (1 + 2 + 1), mapNodes(10) + 2L * 4096 + 128 + 1,
                PAGE_BYTES + 10L * 64 + 4L * 512, 512, 5,
                "C0_TWO_SUCCESSORS_AND_LAST_ONLY_VECTOR4096_SINKS128_COMBINED64_KIB");
        add(parts, "LEGACY_TARGET_HELLO_TIMELINE", "targetClockQualification.sampledInterior/allBefore/allAfter",
                6000L + 2, mapNodes(9), 9L * 64 + 4L * SOURCE_LEAF_CHARS + 24, MAX_LEAF, 5,
                "FIXED_NINE_FIELDS_PRIMARY65536_CHAR_GUARD_OBJECTID24_HEX_NO_RAW_HELLO_PAYLOAD");
        add(parts, "COMMAND_AND_TARGET_READ_RECEIPTS", "mongoCommands/targetObserverReadCosts/targetWitnessReadReceipts",
                4, SOURCE_PACKET_NODES + 256L * mapNodes(6), SOURCE_PACKET_BYTES + 256L * 6 * 64,
                MAX_LEAF, SOURCE_PACKET_DEPTH, "CONSUMED_COMMAND_PACKET_BOUND_AND_SLOW_READ_RETENTION256");
        add(parts, "ROOT_CPU_ENVELOPE", "targetClockQualification.rootCpuDiagnostics/rootCpuDiagnosticScope",
                1, 4096, 4096, 4096, 8, "THREE_CUTOFFS_FOUR_OPERATIONS_FIXED_FOUR_KIB_TRANSPORT");
        add(parts, "PRODUCER_WORK_BODY_AND_EVALUATION", "producer post-exit body/BenchmarkReturnWorkBudgetProducerAllocations",
                2, 2L * PRODUCER_ACTORS * mapNodes(4) + 2L * PRODUCER_ACTORS + 16L * mapNodes(2) + 4L * mapNodes(8),
                PAGE_BYTES, PAGE_BYTES, 8, "TRUSTED_48_ACTIVITY_ROSTER_16_EPOCHS_BOUNDED_SEAL_BODY");
        add(parts, "ROOT_WORK_SELF_AND_CLOSED_SEAL", "BenchmarkReturnWorkBudget.Scope.snapshot/finish/output self evidence",
                2, 1024L + 128L * ROOT_ACTORS + 8L * PENDING,
                4096L + 1024L * ROOT_ACTORS + 128L * PENDING, MAX_LEAF, 8,
                "ROOT57_ACTIVITY_VECTOR_MAPS_SELF_COUNTERS_PENDING128_AND_PRIVATE_RELEASE_TAIL");
        add(parts, "CLOCK_REQUEST_COST_AND_FAILURE_PACKETS", "Capture.clockRequests/producerCostStages/native-clock refusal packet",
                3, SOURCE_PACKET_NODES, SOURCE_PACKET_BYTES, MAX_LEAF, SOURCE_PACKET_DEPTH,
                "FIVE_REQUEST_ROLES_BOUNDED_EIGHT_KIB_AND_GUARDED_NESTED_FAILURE_PACKETS");
        long nodes = 1, strings = 0;
        for (Component part : parts) {
            nodes = Math.addExact(nodes, part.nodes()); strings = Math.addExact(strings, part.strings());
        }
        var plan = new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(nodes,
                OUTER_DEPTH + SOURCE_PACKET_DEPTH, MAX_LEAF, strings, SOURCE_KEY_BYTES, 64);
        var metadata = Map.<String, Object>ofEntries(
                Map.entry("state", "SOURCE_DERIVED_COMPLETE_PILOT_REPORT_ENVELOPE"),
                Map.entry("scope", "ONE_COMPLETE_FORK_OR_ONE_CONTROLLED_STDOUT_FAILURE_TREE"),
                Map.entry("mode", "B_PLAIN_ON_PERIODIC_NOMINAL"), Map.entry("rows", workload.rows()), Map.entry("seed", workload.seed()),
                Map.entry("components", parts.stream().map(Component::evidence).toList()),
                Map.entry("encodingInvocationBasis", "COMPLETE_ACTUAL_REPORT_SERIALIZE_CALLS_NO_FUTURE_STDOUT_COUNT_BOUND"),
                Map.entry("sourceGuardDependencies", List.of(
                        "CONFIRMED_CONSUMER_JSON_PRIOR_UTF8_65536_DEPTH16_TOKENS4096",
                        "RESUME_BSON_LOCAL_MATERIAL_BEFORE_TOJSON_NODES4096_DEPTH16_BYTES2097152",
                        "HELLO_PRIMARY_CHARACTERS65536_AND_ORIGINAL_OBJECTID24_HEX_BEFORE_READING_DTO",
                        "CONTROLLED_FAILURE_PACKET_PRIOR_PLAN_VALIDATION_BEFORE_JSON_ENCODING")),
                Map.entry("opaqueStdoutExcluded", "JVM_LOGGING_STACK_TRACES_AND_OPAQUE_SERVICE_OUTPUT"),
                Map.entry("actualSizeUsedAsPriorBound", false), Map.entry("wholeMethodCostQualified", false),
                Map.entry("performanceAcceptanceEligible", false));
        return new Prepared(plan, parts, metadata);
    }

    private static void add(List<Component> parts, String id, String source, long rows, long nodes,
                            long strings, int leaf, int depth, String basis) {
        parts.add(new Component(id, source, rows, nodes, strings, leaf, depth, basis));
    }
    private static long mapNodes(long fields) { return Math.addExact(1, Math.multiplyExact(2, fields)); }
    private static int base64(int bytes) { return Math.toIntExact(Math.multiplyExact(4L, (bytes + 2L) / 3)); }
    private static long base64Total(long bytes, long pages) { return Math.multiplyExact(4, Math.addExact(bytes, Math.multiplyExact(2, pages)) / 3); }
    private static void require(boolean condition, String reason) { if (!condition) throw new AssertionError("complete report plan " + reason); }
}
