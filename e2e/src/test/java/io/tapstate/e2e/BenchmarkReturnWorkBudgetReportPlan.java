package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Prior material envelope for a complete fixed pilot report or one controlled diagnostic packet. */
final class BenchmarkReturnWorkBudgetReportPlan {
    private static final long RAW_BYTES = 2L * 1024 * 1024;
    private static final int PAGE_BYTES = 65536, PAGES = 512, FRAMES = BenchmarkReturnLocalSourceWork.MAX_STORED_FRAMES, RETURN_ROWS = BenchmarkReturnLocalSourceWork.MAX_ROWS;
    private static final int CLOCK_SAMPLES = 512, RESOURCE_ATTEMPTS = 4096;
    private static final int SOURCE_PACKET_NODES = 4096, SOURCE_PACKET_DEPTH = 16;
    private static final int SOURCE_LEAF_CHARS = 65536, SOURCE_PACKET_BYTES = 2 * 1024 * 1024;
    private static final int OWNED_RECEIPT_BYTES = 2 * 1024 * 1024, OWNED_RECEIPT_DEPTH = 8;
    private static final int PRODUCER_ACTORS = 48, ROOT_ACTORS = 57, PENDING = 128;
    private static final int WRITERS = 4;
    private static final int OUTER_DEPTH = 12;
    private static final int SOURCE_KEY_BYTES = 4 * SOURCE_LEAF_CHARS;
    private static final int MAX_LEAF = Math.max(SOURCE_KEY_BYTES, base64(PAGE_BYTES));
    static final int RETURN_COMPONENT_CATALOGUE_ROWS = 19, CATALOGUE_ROWS = 64;

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
        return prepare(workload, false);
    }

    static Prepared prepare(BenchmarkReturnWorkBudgetProfile.WorkloadInput workload, boolean separateProducerWriter) {
        return prepare(workload, separateProducerWriter, false);
    }

    static Prepared prepareWithReturnComponents(BenchmarkReturnWorkBudgetProfile.WorkloadInput workload,
                                                boolean separateProducerWriter) {
        return prepare(workload, separateProducerWriter, true);
    }

    private static Prepared prepare(BenchmarkReturnWorkBudgetProfile.WorkloadInput workload,
                                    boolean separateProducerWriter, boolean returnComponents) {
        var shape = BenchmarkReturnWorkBudgetSourceTails.geometry(workload);
        // Keep this existing canonical-source comparison once per profile inspection. The
        // preceding shape guard bounds every caller roster before equals traverses its values.
        require(workload.equals(BenchmarkReturnWorkBudgetProfile.WorkloadInput.original(workload.id())),
                "original pipeline, target, phase, coverage or cadence inputs differ");
        long measuredPhases = shape.measuredPhases();
        long full = shape.measuredOutputs(), cohort = shape.cohortRows();
        long sourceCoverage = shape.coverageEntries(), allOutputs = shape.allOutputs();
        long chains = shape.chains();
        var parts = new ArrayList<Component>();
        // Scalar maps: raw fork, fork summary, phase, timeline, schedule, confirmation, anchor,
        // resources, interval/rate, common-window certificate, root CPU scope, and disabled branches.
        int[] scalarFields = {29, 24, 50, 14, 8, 10, 5, 6, 9, 40, 10, 12};
        long scalarNodes = 0, scalarStrings = 0;
        long[] scalarCopies = {1, 1, measuredPhases, measuredPhases, shape.phases(),
                Math.multiplyExact(shape.phases(), shape.targets()), chains, measuredPhases, measuredPhases,
                measuredPhases, measuredPhases, measuredPhases};
        for (int index = 0; index < scalarFields.length; index++) {
            int fields = scalarFields[index]; long copies = scalarCopies[index];
            scalarNodes = Math.addExact(scalarNodes, Math.multiplyExact(copies, mapNodes(fields)));
            scalarStrings = Math.addExact(scalarStrings, Math.multiplyExact(copies, Math.multiplyExact(fields, 128L + 512)));
        }
        add(parts, "SCALAR_REPORT_SCAFFOLDS", "RealBenchmarkForkDriverIT.run/PipelineBenchmarkLiveRunIT.fork/phaseEvidence",
                1, scalarNodes, scalarStrings, 512, 4, "FIXED_MAP_FIELD_INVENTORIES_AND_LITERAL_TEXT");
        add(parts, "FORK_DELIVERY_LATENCIES", "PipelineBenchmarkLiveRunIT.fork.deliveryNanos",
                cohort + 1, 1, 0, 32, 2, "ORIGINAL_COMPLETE_MIDDLE_HALF");
        add(parts, "FULL_AND_COHORT_TIMELINES", "PipelineBenchmarkLiveRunIT.phaseEvidence.deliveryTimeline",
                Math.addExact(Math.addExact(full, Math.multiplyExact(3, cohort)), Math.multiplyExact(5, measuredPhases)), 1, 0, 32, 4,
                "FULL_OBSERVATION_PLUS_COHORT_OBSERVATION_OPERATION_AND_STREAM_ARRAYS");
        long coverageEntries = Math.addExact(sourceCoverage, Math.addExact(Math.multiplyExact(2, allOutputs), chains));
        add(parts, "ALL_COVERAGE_MAPS", "PipelineBenchmarkLiveRunIT.fork.declaredSourceCoverage/observedTargetCoverage/logicalCoverage",
                coverageEntries + 3, 2, 128, 128, 2, "SUM_ALL_ORIGINAL_PHASE_DOMAINS_WITHOUT_DEDUPLICATION");
        add(parts, "SOURCE_BATCH_DOUBLE_REPRESENTATION", "phaseEvidence.sourceBatches/BenchmarkWriteReturnPhaseEvidence.sourceBatches",
                shape.measuredBatches(), mapNodes(8) + mapNodes(3), 8L * 64 + 4L * 40 + 3L * 64, 128, 4,
                "SUM_ORIGINAL_MEASURED_BATCH_ROSTERS_EACH_960_IN_BOTH_MAP_SHAPES");
        // A single encoded tree can include a full resource trace and its resource-bounds projection,
        // plus the retained refusal trace. These are separate representations of the same attempts.
        add(parts, "RESOURCE_TRACES_AND_REFUSALS", "samplingEvidence/BenchmarkReturnResourceBounds/collectorSamplingRefusal",
                Math.multiplyExact(Math.multiplyExact(3, measuredPhases), RESOURCE_ATTEMPTS), mapNodes(11) + 2L * 5, 11L * 64 + 4L * 40 + 64 + MAX_LEAF,
                MAX_LEAF, 6, "RETAINED_ATTEMPT_CAP_4096_WITH_READING_AND_UTC_FIELDS");
        add(parts, "RAW_RETURN_BASE64", "BenchmarkWriteReturnPhaseEvidence.pagesBase64/BenchmarkWriteReturnCapture.retainedEvidence",
                measuredPhases, 2L * (PAGES + 1), 2L * base64Total(RAW_BYTES, PAGES), base64(PAGE_BYTES), 3,
                "COMPLETE_RAW_TWO_MIB_WITH_INDEPENDENT_PADDING_FOR_EACH_OF_512_PAGES");
        add(parts, "RETURN_CLOCK_ROSTERS", "BenchmarkWriteReturnPhaseEvidence.clockSamples/Capture.retainedEvidence.samples",
                Math.multiplyExact(Math.multiplyExact(2, measuredPhases), CLOCK_SAMPLES), mapNodes(6), 6L * 64, 128, 4, "COMPLETE_PERIODIC_CAP_512_NO_CONTROL_SAMPLES_INSERTED");
        add(parts, "RETURN_CALL_AND_ROW_REPRESENTATION", "BenchmarkWriteReturnLedger/Assembly/Expectations retained diagnostics",
                Math.multiplyExact(measuredPhases, FRAMES), mapNodes(24), 24L * 128, 512, 6, "SOURCE_BYTE_DERIVED_STORED_PIECE_CAP_CALL_ROW_CAP1024_NO_FULL_ROW_METADATA_DUPLICATION");
        add(parts, "RETURN_ROWS_IF_MATERIALIZED", "bounded return row/key/association failure projections",
                Math.multiplyExact(measuredPhases, RETURN_ROWS), mapNodes(12), 12L * 128, 512, 8, "INDEPENDENT_RETURN_ROW_CAP262144_INCLUDES_PARTIAL_REFUSALS");
        add(parts, "TABLE_CONFIRMATIONS", "PipelineBenchmarkLiveRunIT.tableConfirmation/terminalMetaReceipts/sourceChains",
                Math.multiplyExact(Math.multiplyExact(2, chains), measuredPhases), SOURCE_PACKET_NODES + mapNodes(11) + WRITERS + 1,
                PAGE_BYTES + 2L * MAX_LEAF + 11L * 128 + WRITERS * 256L, MAX_LEAF, SOURCE_PACKET_DEPTH,
                "PRIOR_64_KIB_JSON_DEPTH16_TOKEN4096_SOURCE_TOKEN_AND_FOUR_WRITER_BINDING");
        add(parts, "OWNED_RUNTIME_AND_FAILURE_RECEIPT", "BenchmarkOwnedProcessReceipt.buildEvidence/runtime-at-failure",
                2, OWNED_RECEIPT_BYTES / 16L, OWNED_RECEIPT_BYTES, 4 * 8192, OWNED_RECEIPT_DEPTH,
                "WHOLE_RECEIPT_TWO_MIB_16_BYTE_MINIMUM_NODE_CHARGE_DEPTH8_ARGUMENTS512_STRING8192");
        // Both cold native reads contain owned/root parsed and raw evidence. The source guards apply
        // before local consumption; this uses their caps, not a sampled encoded packet size.
        add(parts, "NATIVE_METADATA_AND_REGISTRATIONS", "targetClockQualification.nativeClockBefore/nativeClockAfter/registrationBefore/After",
                Math.multiplyExact(8, measuredPhases), SOURCE_PACKET_NODES, SOURCE_PACKET_BYTES, MAX_LEAF, SOURCE_PACKET_DEPTH,
                "TWO_COLD_READS_FOUR_ROOT_OWNED_RAW_PARSED_PACKET_FORMS");
        add(parts, "PRODUCER_COLD_ORIGIN_PACKETS", "nativeClockBefore/nativeClockAfter/producerClassMetadata",
                Math.multiplyExact(6, measuredPhases), SOURCE_PACKET_NODES, 2L * 2080768 + 8192, 64512, 8,
                "TWO_COLD_READS_TWO_DIRECT_AND_TWO_NESTED_POST_EXIT_BINDINGS_PLAIN_RAW34_AND_TYPED32_PRIOR_PART_CAPS");
        add(parts, "ROOT_CLASS_ORIGIN_PACKETS", "actualInputBindings.actualRootOrigins/producer.actualRootOrigins",
                2, 128L + 512L * mapNodes(7), 32768L + 512L * (7L * 128 + 256 + 4096 + 1024 + 64 + 256),
                4096, 5, "FIXED_512_NAMED_CLASSES_SEVEN_ROW_FIELDS_ASCII_PATH4096_ENTRY1024_NAME256_SHA64_HEADER128_NODES");
        add(parts, "EMITTED_COMPILER_ROSTER", "actualInputBindings.actualEmittedRoster",
                1, 128L + BenchmarkReturnWorkBudgetBindings.SourceRole.values().length * mapNodes(4) + 512L * mapNodes(7),
                32768L + BenchmarkReturnWorkBudgetBindings.SourceRole.values().length * (4L * 128 + 128 + 512 + 64)
                        + 512L * (7L * 128 + 128 + 256 + 256 + 64 + 4096 + 1024),
                4096, 5, "FOUR_OWNED_COMPILER_OUTPUTS_ALL_NAMED_SOURCE_ROLES_512_SELECTED_MEMBERS_PRIOR_FIELD_CAPS");
        add(parts, "NATIVE_BASELINE_CHECKPOINTS", "BenchmarkNativeCounterBaseline.evidence.before/after",
                Math.multiplyExact(measuredPhases, 2L * (1 + 2 + 1)), mapNodes(10) + 2L * 4096 + 128 + 1,
                PAGE_BYTES + 10L * 64 + 4L * 512, 512, 5,
                "C0_TWO_SUCCESSORS_AND_LAST_ONLY_VECTOR4096_SINKS128_COMBINED64_KIB");
        add(parts, "LEGACY_TARGET_HELLO_TIMELINE", "targetClockQualification.sampledInterior/allBefore/allAfter",
                Math.multiplyExact(measuredPhases, 6000L + 2), mapNodes(9), 9L * 64 + 4L * SOURCE_LEAF_CHARS + 24, MAX_LEAF, 5,
                "FIXED_NINE_FIELDS_PRIMARY65536_CHAR_GUARD_OBJECTID24_HEX_NO_RAW_HELLO_PAYLOAD");
        add(parts, "COMMAND_AND_TARGET_READ_RECEIPTS", "mongoCommands/targetObserverReadCosts/targetWitnessReadReceipts",
                Math.multiplyExact(4, measuredPhases), SOURCE_PACKET_NODES + 256L * mapNodes(6), SOURCE_PACKET_BYTES + 256L * 6 * 64,
                MAX_LEAF, SOURCE_PACKET_DEPTH, "CONSUMED_COMMAND_PACKET_BOUND_AND_SLOW_READ_RETENTION256");
        add(parts, "ROOT_CPU_ENVELOPE", "targetClockQualification.rootCpuDiagnostics/rootCpuDiagnosticScope",
                measuredPhases, 4096, 4096, 4096, 8, "THREE_CUTOFFS_FOUR_OPERATIONS_FIXED_FOUR_KIB_TRANSPORT");
        add(parts, "PRODUCER_WORK_BODY_AND_EVALUATION", "producer post-exit body/BenchmarkReturnWorkBudgetProducerAllocations",
                2, 2L * PRODUCER_ACTORS * mapNodes(4) + 2L * PRODUCER_ACTORS + 16L * mapNodes(2) + 4L * mapNodes(8),
                PAGE_BYTES, PAGE_BYTES, 8, "TRUSTED_48_ACTIVITY_ROSTER_16_EPOCHS_BOUNDED_SEAL_BODY");
        add(parts, "ROOT_WORK_SELF_AND_CLOSED_SEAL", "BenchmarkReturnWorkBudget.Scope.snapshot/finish/output self evidence",
                2, 1024L + 128L * ROOT_ACTORS + 8L * PENDING,
                4096L + 1024L * ROOT_ACTORS + 128L * PENDING, MAX_LEAF, 8,
                "ROOT57_ACTIVITY_VECTOR_MAPS_SELF_COUNTERS_PENDING128_AND_PRIVATE_RELEASE_TAIL");
        add(parts, "CLOCK_REQUEST_COST_AND_FAILURE_PACKETS", "Capture.clockRequests/producerCostStages/native-clock refusal packet",
                Math.multiplyExact(3, measuredPhases), SOURCE_PACKET_NODES, SOURCE_PACKET_BYTES, MAX_LEAF, SOURCE_PACKET_DEPTH,
                "FIVE_REQUEST_ROLES_BOUNDED_EIGHT_KIB_AND_GUARDED_NESTED_FAILURE_PACKETS");
        if (separateProducerWriter) {
            // Actual report projections: profile.producerWriter, inputBindings.producerWriter,
            // inputBindings.actualIo.producerWriter. Each fixed tuple has ten fields; the registered
            // projection contract conservatively permits 32, two 4096 paths, one 512 source path,
            // one 256 binary name, two 64 digests, two 20 numeric scalars and 128 fixed literals.
            long writerStrings = 32L * 128 + 2L * 4096 + 512 + 256 + 2L * 64 + 2L * 20 + 128;
            add(parts, "OPTIONAL_PRODUCER_WRITER_SCALAR_PROJECTIONS",
                    "Profile.inputRegistrationInfo/Run.bindingEvidence/BindingsIo.actualWriter.evidence",
                    3, mapNodes(32), writerStrings, 4096, 3,
                    "THREE_FIXED_PRODUCER_ONLY_TUPLE_MAPS_PRIOR_PATH4096_FIELDS32_NO_OBSERVED_SIZE_CAP");
        }
        if (returnComponents) { addReturnComponentCatalogue(parts, measuredPhases, shape.measuredBatches()); }
        long nodes = 1, strings = 0;
        for (Component part : parts) {
            nodes = Math.addExact(nodes, part.nodes()); strings = Math.addExact(strings, part.strings());
        }
        var plan = new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(nodes,
                OUTER_DEPTH + SOURCE_PACKET_DEPTH, MAX_LEAF, strings, SOURCE_KEY_BYTES, 64);
        Map<String, Object> metadata = Map.<String, Object>ofEntries(
                Map.entry("state", "SOURCE_DERIVED_COMPLETE_PILOT_REPORT_ENVELOPE"),
                Map.entry("scope", "ONE_COMPLETE_FORK_OR_ONE_CONTROLLED_STDOUT_FAILURE_TREE"),
                Map.entry("mode", separateProducerWriter ? "A_PLAIN_PERIODIC_NOMINAL_SEPARATE_PRODUCER_WRITER" : "B_PLAIN_ON_PERIODIC_NOMINAL"), Map.entry("rows", workload.rows()), Map.entry("seed", workload.seed()),
                Map.entry("components", parts.stream().map(Component::evidence).toList()),
                Map.entry("workload", workload.id()), Map.entry("measuredPhases", measuredPhases),
                Map.entry("fullMeasuredOutputs", full), Map.entry("pooledCohortRows", cohort),
                Map.entry("measuredSourceBatches", shape.measuredBatches()),
                Map.entry("encodingInvocationBasis", "COMPLETE_ACTUAL_REPORT_SERIALIZE_CALLS_NO_FUTURE_STDOUT_COUNT_BOUND"),
                Map.entry("sourceGuardDependencies", List.of(
                        "CONFIRMED_CONSUMER_JSON_PRIOR_UTF8_65536_DEPTH16_TOKENS4096",
                        "RESUME_BSON_LOCAL_MATERIAL_BEFORE_TOJSON_NODES4096_DEPTH16_BYTES2097152",
                        "HELLO_PRIMARY_CHARACTERS65536_AND_ORIGINAL_OBJECTID24_HEX_BEFORE_READING_DTO",
                        "CONTROLLED_FAILURE_PACKET_PRIOR_PLAN_VALIDATION_BEFORE_JSON_ENCODING")),
                Map.entry("opaqueStdoutExcluded", "JVM_LOGGING_STACK_TRACES_AND_OPAQUE_SERVICE_OUTPUT"),
                Map.entry("actualSizeUsedAsPriorBound", false), Map.entry("wholeMethodCostQualified", false),
                Map.entry("performanceAcceptanceEligible", false));
        if (returnComponents) {
            var additional = new java.util.LinkedHashMap<String, Object>(metadata);
            additional.put("returnComponentsIncluded", true);
            additional.put("returnComponentProjectionScope",
                    "ONE_METHOD_AND_AUTHORITY_HEADER_ONE_BOUND_EVIDENCE_AND_COMMON_ACCESSOR_PER_ORIGINAL_MEASURED_PHASE");
            additional.put("sourceGuardDependencies", List.of(
                    "CONFIRMED_CONSUMER_JSON_PRIOR_UTF8_65536_DEPTH16_TOKENS4096",
                    "RESUME_BSON_LOCAL_MATERIAL_BEFORE_TOJSON_NODES4096_DEPTH16_BYTES2097152",
                    "HELLO_PRIMARY_CHARACTERS65536_AND_ORIGINAL_OBJECTID24_HEX_BEFORE_READING_DTO",
                    "CONTROLLED_FAILURE_PACKET_PRIOR_PLAN_VALIDATION_BEFORE_JSON_ENCODING",
                    "METHOD_PHASE_MAX6_SOURCEBATCH960_CLOCK512_RAW_TWO_MIB_BEFORE_COPY_AND_CONSUMPTION",
                    "ONE_BOUND_EVIDENCE_AND_ONE_COMMONWINDOW_ACCESSOR_PER_ORIGINAL_MEASURED_PHASE"));
            metadata = Map.copyOf(additional);
        }
        return new Prepared(plan, parts, metadata);
    }

    private static void addReturnComponentCatalogue(List<Component> parts, long phases, long batches) {
        BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.COMPONENT_FACTORY,
                () -> { addReturnComponentCatalogueBounded(parts, phases, batches); return null; });
    }

    private static void addReturnComponentCatalogueBounded(List<Component> parts, long phases, long batches) {
        require(phases > 0 && phases <= 2 && batches == Math.multiplyExact(phases, 960),
                "return component projection scalar roster differs");
        require(parts != null && parts.size() <= CATALOGUE_ROWS - RETURN_COMPONENT_CATALOGUE_ROWS,
                "return report prior descriptor roster exceeds its cap");
        // These are additional representations, even when a legacy report already contains the
        // original phase/raw/clock/hello data. No spare capacity from an unrelated row is borrowed.
        add(parts, "METHOD_PHASE_SCAFFOLDS", "BenchmarkReturnMethodAdmission.verifyPhase/PhaseEvidence.recordNominal",
                phases, mapNodes(64) + 3L * mapNodes(4), 64L * (128 + 512), 8192, 6,
                "FIXED_PHASE_SUMMARY_INTERVAL_RATE_AND_EIGHTEEN_FALSE_FIELDS");
        add(parts, "METHOD_SOURCE_BATCH_PROJECTION", "BenchmarkWriteReturnPhaseEvidence.sourceBatches",
                Math.addExact(batches, phases), mapNodes(3), 3L * 64, 128, 4,
                "ONE_THREE_NUMERIC_FIELD_MAP_PER_ORIGINAL_BATCH_PLUS_LIST_NODE");
        add(parts, "METHOD_CLOCK_SAMPLE_PROJECTION", "BenchmarkWriteReturnPhaseEvidence.clockSamples",
                Math.multiplyExact(phases, CLOCK_SAMPLES + 1L), mapNodes(6), 6L * 64, 128, 4,
                "ONE_COMPLETE_PERIODIC_SAMPLE_ROSTER_MAX512_PLUS_LIST_NODE");
        add(parts, "METHOD_RAW_RETURN_PAGES", "BenchmarkWriteReturnPhaseEvidence.pagesBase64",
                phases, PAGES + 1L, base64Total(RAW_BYTES, PAGES), base64(PAGE_BYTES), 3,
                "ADDITIONAL_COMPLETE_TWO_MIB_RAW_PAGES_WITH_PRIOR_512_PADDING");
        add(parts, "METHOD_NOMINAL_MAPPING", "BenchmarkNativeNominalReturnClock.evidence",
                phases, 4096, 65536, 8192, 16,
                "OWNED_IMMUTABLE_ROUTE_METADATA_PRIOR_FREEZE4096_NODES_65536_UTF8");
        add(parts, "METHOD_ADMISSION_HEADERS", "AdmittedMethod.evidence/CostRecipe.evidence",
                Math.addExact(phases, 1), 4096, 65536, 8192, 16,
                "ONE_ROOT_HEADER_PLUS_ONE_NESTED_HEADER_EACH_PHASE_ROUTE_AND_COST_RECIPE_INCLUDED");
        long authorityNodes = mapNodes(64) + 6 + 5 + 2 + mapNodes(4);
        long authorityStrings = 64L * (128 + 512) + 6L * 512 + 4L * (128 + 64) + 5L * 512;
        add(parts, "METHOD_NATIVE_SOURCE_AUTHORITY", "BenchmarkNativeSourceAuthority.sourceRecipeEvidence",
                1, authorityNodes, authorityStrings, 512, 4,
                "FIXED64_FIELD_ENVELOPE_SIX_PATH_FOUR_CLASS_FIVE_OBLIGATION_LITERALS");
        long commonNodes = mapNodes(40) + 2L * (mapNodes(3) + 2L * 3) + 11 + 10L * mapNodes(3) + 3L * mapNodes(2);
        long commonStrings = 40L * (128 + 512) + 2L * (3 * 128L + 512) + 10L * 3 * 128 + 3L * 2 * 128;
        add(parts, "METHOD_COMMON_WINDOW_CERTIFICATES", "BoundPhase.evidence.commonWindowCertificate/BoundPhase.commonWindow",
                Math.multiplyExact(2, phases), commonNodes, commonStrings, 8192, 6,
                "TWO_PROJECTIONS_MAX_PER_PHASE_FORTY_FIELDS_TWO_TARGETS_TEN_BINS_THREE_RATIOS");
        add(parts, "METHOD_SERIAL_HELLO_READINGS", "BenchmarkTargetClock.validateSeries.readings",
                Math.multiplyExact(phases, 6000), mapNodes(9), 9L * 64 + 4L * 65536 + 4L * 24 + 64,
                MAX_LEAF, 5,
                "GUARD_BEFORE_LISTCOPY_AND_PROJECTION_PRIMARY65536_CHARS_PROCESS24_CHARS_NINE_FIELDS");
        add(parts, "METHOD_SERIAL_HELLO_HEADER", "BenchmarkTargetClock.validateSeries",
                phases, mapNodes(7) + 1, 7L * (64 + 128), 128, 4,
                "SEVEN_FIXED_HEADER_FIELDS_READINGS_MATERIAL_COUNTED_SEPARATELY");
        add(parts, "METHOD_TYPED_REFUSAL", "BenchmarkReturnMethodAdmission.Refusal.retainedEvidence",
                1, mapNodes(64), 64L * (128 + 256), 512, 4,
                "FIXED_REASON_AND_EIGHTEEN_FALSE_FLAGS_NO_CAUSE_STACK_OR_ARBITRARY_CALLBACK_BODY");
        add(parts, "RETURN_COMPONENT_SOURCE_MATRIX", "BenchmarkReturnComponentSourceWork.PreparedBounds.sourceEvidence",
                1, mapNodes(48) + mapNodes(57) + 61L * mapNodes(8) + mapNodes(7) + mapNodes(6),
                48L * (128 + 512) + 57L * (128 + 8L * 128) + 4L * 8 * 128 + 7L * 128 + 6L * 128,
                512, 5, "FIXED_ACTOR_INVENTORY_EIGHT_UNITS_SIX_PLAN_FIELDS_SEVEN_MATERIAL_SCALARS_TWO_POOLED_ROWS");
        add(parts, "RETURN_COMPONENT_CATALOGUE_METADATA", "ReportPlan.Prepared.metadata.components",
                1, mapNodes(32) + CATALOGUE_ROWS * mapNodes(9),
                32L * (128 + 512) + CATALOGUE_ROWS * (9L * 128 + 3L * 512),
                512, 6, "PRIOR64_SOURCE_DESCRIPTOR_MAPS_ID_BRANCH_BASIS512_WITH_FIXED_NUMERIC_FIELDS");
        add(parts, "RETURN_COMPONENT_FACTORY_TAIL_EVIDENCE", "SourceTails.factoryEvidence/preScopeSourceTail",
                1, mapNodes(37) + 8L * mapNodes(8) + 8,
                37L * (128 + 512) + 6L * 512 + 8L * 8 * 128,
                512, 5, "THIRTYSEVEN_FIXED_FIELDS_EIGHT_VECTORS_INCLUDE_BRIDGE_SOURCE_FACTORY_AND_ORIGINAL_GEOMETRY");
        add(parts, "METHOD_EARLY_ROOT_OBSERVATION_BASIS", "Run.rootClockMetadataBeforeEvidence/rootClockMetadataBeforeBasis",
                phases, mapNodes(3), 3L * 128 + 2L * 128, 512, 3,
                "ADDITIONAL_FIXED_EARLY_OR_FRESH_BASIS_FIELD_NATIVE_PAYLOAD_REMAINS_IN_ITS_ORIGINAL_PACKET_ROW");
        long packets = Math.multiplyExact(2, phases);
        add(parts, "NATIVE_CLASS_PHASE_AND_CUT_ROSTER", "Run.recordNativeClassMetadata/afterExit producer phase/cut projections",
                3, mapNodes(2) + Math.multiplyExact(packets, mapNodes(2)) + phases + 1,
                2L * 128 + Math.multiplyExact(packets, 2L * 128 + 128 + 5) + Math.multiplyExact(phases, 128),
                128, 5, "THREE_BOUNDED_REPORT_PROJECTIONS_TWO_PACKETS_PER_ORIGINAL_MEASURED_PHASE_KEYS128_CUT5");
        add(parts, "TYPED_RETURN_FORK_CARRIER", "BenchmarkReturnForkBridge.Fork.evidence",
                1, mapNodes(48) + mapNodes(3) + 3L * mapNodes(2) + 7 + 4096,
                48L * (128 + 512) + 2L * 160 + 128 + 6L * 128 + 9L * 128 + 65536,
                8192, 16, "EXACT_CURRENT_FORK_FIELDS_DIRECTED_DECIMAL160_FOUR_INTERVAL_MAPS_PHASEIDS6_ADMISSION_METADATA");
        add(parts, "RETURN_SERIAL_HELLO_RAW", "RealBenchmarkForkDriver.returnMethodPhase.serialHelloRaw",
                phases * 6000, mapNodes(9), 9L * 128 + 4L * 65536 + 4L * 24 + 7L * 32,
                65536, 4, "ORIGINAL_SIX_THOUSAND_SERIAL_READINGS_WITH_NINE_FIXED_FIELDS_NO_NEW_READS");
        add(parts, "RETURN_BRIDGE_FACTORY_TAIL", "BenchmarkReturnWorkBudgetSourceTails.factoryEvidence",
                1, mapNodes(2) + mapNodes(8), 2L * 128 + 8L * 128, 128, 4,
                "TWO_ADDED_FACTORY_FIELDS_ONE_EIGHT_UNIT_VECTOR_NO_WORKLOAD_FACTORY_CHANGE");
        require(parts.size() <= CATALOGUE_ROWS, "return report catalogue exceeds its prior descriptor roster");
    }

    /** Additional owned catalogue construction; original workload validation remains in its old source tail. */
    static BenchmarkReturnWorkBudget.Work returnCatalogueUpper(long entries) {
        require(entries >= 0, "return catalogue entry count is negative");
        long rows = RETURN_COMPONENT_CATALOGUE_ROWS;
        // Per added row: eight constructor/guard, eight checked arithmetic, eight list/carrier,
        // eight plan accumulation and sixteen descriptor/boxing APIs. The 64 descriptors include
        // the original rows when the final immutable catalogue metadata is built.
        long requests = Math.addExact(64, Math.addExact(Math.multiplyExact(48, rows), Math.multiplyExact(32, CATALOGUE_ROWS)));
        long scalarSlots = Math.addExact(Math.multiplyExact(24, rows), Math.multiplyExact(18, CATALOGUE_ROWS));
        long descriptorChars = Math.multiplyExact(CATALOGUE_ROWS, 9L * 128 + 3L * 512);
        return new BenchmarkReturnWorkBudget.Work(1, requests, rows, 0,
                Math.addExact(Math.multiplyExact(8, scalarSlots), Math.multiplyExact(4, descriptorChars)), 4,
                Math.addExact(Math.multiplyExact(2, scalarSlots), descriptorChars), scalarSlots).times(entries);
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
