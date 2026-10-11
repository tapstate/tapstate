package io.tapstate.e2e;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Source-derived pilot inputs and component bounds; missing source rows refuse activation. */
final class BenchmarkReturnWorkBudgetProfile {
    static final int ROWS = 96_000;
    static final long SEED = 424_242L;
    static final int PAGE_BYTES = 65_536, RAW_BYTES = 2 * 1024 * 1024, PAGES = 512, FRAMES = 512;
    static final int RETURN_ROWS = 262_144, CLOCK_SAMPLES = 512, RESOURCE_ATTEMPTS = 4096;
    static final String DEFINITION_ID = "WRITE_RETURN_ROOT_FIXED_PILOT_V1";

    enum FileRole {
        APP_JAR, NATIVE_LIBRARY, ROOT_CPU_PROVIDER_LIBRARY,
        CONNECTOR_JAR_1, CONNECTOR_JAR_2, CONNECTOR_JAR_3, CONNECTOR_JAR_4, CONNECTOR_JAR_5,
        COMMON_HELPER_MANIFEST, JDK_INPUT_MANIFEST
    }
    record FileInput(FileRole role, String sha256, long bytes) {
        FileInput { require(role != null && bytes > 0 && bytes < Long.MAX_VALUE, "file scalar registration is missing"); digest(sha256); }
    }
    record TargetInput(String pipeline, String table, String location, String projection, int rows, String checksum) { }
    record PhaseInput(String id, String stage, boolean measured, long logicalOutputs,
                      int statements, int statementsPerBatch, int batches, long cadenceNanos,
                      Map<String, Long> logicalCoverage, List<TargetInput> targets) {
        PhaseInput { text(id, 128); text(stage, 32); logicalCoverage = Map.copyOf(logicalCoverage); targets = List.copyOf(targets); }
    }
    record WorkloadInput(String id, int rows, long seed, String database, int setupStatements,
                         List<String> pipelines, List<String> sourceChains, List<PhaseInput> phases) {
        WorkloadInput { text(id, 32); text(database, 32); pipelines = List.copyOf(pipelines); sourceChains = List.copyOf(sourceChains); phases = List.copyOf(phases); }
        static WorkloadInput original(String id) { return from(BenchmarkWorkloadDefinitions.steadyPilot(id)); }
        static WorkloadInput from(BenchmarkWorkloadDefinitions.Workload workload) {
            require(workload != null, "workload scalar registration is missing");
            var phases = workload.phases().stream().map(phase -> new PhaseInput(phase.id(), phase.stage().name(), phase.measured(),
                    phase.expectedLogicalOutputChanges(), phase.sql().size(), phase.statementsPerBatch(),
                    phase.sql().size() / phase.statementsPerBatch(), phase.batchInterval().toNanos(), phase.expectedLogicalCoverage(),
                    phase.targets().stream().map(target -> new TargetInput(target.pipelineId(), target.table(), target.location().name(),
                            target.projection().name(), target.rows(), target.checksum())).toList())).toList();
            return new WorkloadInput(workload.id(), workload.rows(), workload.seed(), workload.database().name(), workload.setupSql().size(),
                    workload.pipelineIds(), workload.sourceChains().stream().map(chain -> chain.id() + "/" + chain.pipelineId() + "/"
                            + chain.sourceId() + "/" + chain.table() + "/" + chain.terminalLogicalId() + "/" + chain.terminalRowId()).toList(), phases);
        }
    }
    record Flags(String arm, String captureMode, String clockMode, String collectorMode,
                 boolean steadyPilot, boolean nativeLibrary, boolean nominalEnclosure,
                 boolean explicitOutput, boolean rootCpu, Set<String> otherControls) {
        Flags { otherControls = Set.copyOf(otherControls); }
    }
    record Inputs(String runId, String inputSha256, WorkloadInput workload, List<FileInput> files, Flags flags) {
        Inputs { text(runId, 128); digest(inputSha256); require(workload != null && flags != null, "profile scalar inputs are missing"); files = List.copyOf(files); }
    }
    record Gap(String id, String sourceLocation) {
        Gap { text(id, 128); text(sourceLocation, 256); }
    }
    record Prepared(Inputs inputs, Map<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> perOperationUpper,
                    Set<BenchmarkReturnWorkBudget.Activity> explicitExcludedActivities,
                    BenchmarkReturnWorkBudgetDefinitions.OutputPlan fieldsRegisteredOutputPlan,
                    Map<String, Object> inputRegistrationInfo, List<Gap> gaps) {
        Prepared {
            perOperationUpper = Map.copyOf(perOperationUpper); explicitExcludedActivities = Set.copyOf(explicitExcludedActivities);
            inputRegistrationInfo = Map.copyOf(inputRegistrationInfo); gaps = List.copyOf(gaps);
        }
        BenchmarkReturnWorkBudget.Definition requireDefinition() {
            if (!gaps.isEmpty()) { throw new ProfileRefusal(inputRegistrationInfo, gaps); }
            return new BenchmarkReturnWorkBudget.Definition(DEFINITION_ID, recipeDigest(inputs, perOperationUpper),
                    Map.of(), perOperationUpper, explicitExcludedActivities);
        }
    }
    static final class ProfileRefusal extends AssertionError {
        private final Map<String, Object> evidence;
        ProfileRefusal(Map<String, Object> registration, List<Gap> gaps) {
            super("return work profile has unresolved source bounds");
            evidence = Map.of("state", "UNKNOWN", "inputRegistration", registration,
                    "sourceGaps", gaps.stream().map(gap -> Map.of("id", gap.id(), "sourceLocation", gap.sourceLocation())).toList(),
                    "structuralWorkBudgetQualified", false, "wholeMethodCostQualified", false);
        }
        Map<String, Object> retainedEvidence() { return evidence; }
    }

    private BenchmarkReturnWorkBudgetProfile() { }

    static Prepared inspect(Inputs inputs) {
        require(inputs != null, "profile scalar inputs are missing");
        var flags = inputs.flags();
        require("B".equals(flags.arm()) && "PLAIN".equals(flags.captureMode()) && "PERIODIC".equals(flags.clockMode())
                        && "ON".equals(flags.collectorMode()) && flags.steadyPilot() && flags.nativeLibrary()
                        && flags.nominalEnclosure() && flags.explicitOutput() && flags.otherControls().isEmpty(),
                "unsupported profile mode or conflicting control");
        require(inputs.workload().rows() == ROWS && inputs.workload().seed() == SEED,
                "original pilot row count or seed differs");
        require(inputs.workload().equals(WorkloadInput.original(inputs.workload().id())),
                "original pilot phase, cadence, coverage or batch scalars differ");
        var files = new EnumMap<FileRole, FileInput>(FileRole.class);
        for (var file : inputs.files()) { require(files.put(file.role(), file) == null, "file role is repeated"); }
        var required = EnumSet.allOf(FileRole.class);
        if (!flags.rootCpu()) { required.remove(FileRole.ROOT_CPU_PROVIDER_LIBRARY); }
        require(files.keySet().containsAll(required), "required frozen file scalars are missing");
        for (var role : List.of(FileRole.CONNECTOR_JAR_1, FileRole.CONNECTOR_JAR_2, FileRole.CONNECTOR_JAR_3, FileRole.CONNECTOR_JAR_4, FileRole.CONNECTOR_JAR_5)) {
            require(files.get(role).bytes() <= BenchmarkReturnWorkBudget.MAX_CONNECTOR_INPUT_BYTES, "connector scalar exceeds its source cap");
        }
        for (var role : List.of(FileRole.COMMON_HELPER_MANIFEST, FileRole.JDK_INPUT_MANIFEST)) {
            require(files.get(role).bytes() <= BenchmarkReturnWorkBudgetBindings.MAX_MANIFEST_BYTES,
                    "manifest scalar exceeds its source cap");
        }

        var upper = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        var excluded = EnumSet.noneOf(BenchmarkReturnWorkBudget.Activity.class);
        if (!flags.rootCpu()) { excluded.add(BenchmarkReturnWorkBudget.Activity.ROOT_CPU); }
        var reporting = BenchmarkReturnWorkBudgetReportPlan.prepare(inputs.workload());
        knownComponentBounds(upper, inputs, reporting.plan());
        knownRootBounds(upper, inputs, reporting.plan());
        var measured = inputs.workload().phases().stream().filter(PhaseInput::measured).findFirst().orElseThrow();
        upper.putAll(BenchmarkReturnLocalSourceWork.bounds(new BenchmarkReturnLocalSourceWork.Capacity(
                measured.logicalOutputs(), measured.logicalOutputs() / 2, measured.batches())));
        long manifestBytes = Math.max(files.get(FileRole.COMMON_HELPER_MANIFEST).bytes(), files.get(FileRole.JDK_INPUT_MANIFEST).bytes());
        int manifestFields = BenchmarkReturnBindingsManifestReader.maximumFields();
        var decode = BenchmarkReturnBindingsManifestReader.decodingWorkUpper(manifestBytes, manifestFields);
        var encode = BenchmarkReturnBindingsManifestReader.encodingWorkUpper(manifestBytes, manifestFields);
        // A reader entry includes canonical re-encoding. Standalone encoding has its own COPY entry.
        upper.merge(BenchmarkReturnWorkBudget.Activity.DECODE, decode.plus(encode), BenchmarkReturnWorkBudget.Work::plus);
        upper.merge(BenchmarkReturnWorkBudget.Activity.COPY, encode, BenchmarkReturnWorkBudget.Work::plus);
        var localRegistration = BenchmarkReturnLocalSourceWork.registrationLocalUpper();
        upper.merge(BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, localRegistration, BenchmarkReturnWorkBudget.Work::plus);
        var localIo = BenchmarkReturnWorkBudgetBindingsIo.localIoUpper();
        localIo.forEach((activity, bound) -> upper.merge(activity, bound, BenchmarkReturnWorkBudget.Work::plus));
        var rootOriginLocal = BenchmarkReturnRootOrigins.localUpper();
        upper.merge(BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, rootOriginLocal, BenchmarkReturnWorkBudget.Work::plus);
        var producerOriginLocal = BenchmarkReturnProducerOrigins.localUpper();
        upper.merge(BenchmarkReturnWorkBudget.Activity.DECODE, producerOriginLocal, BenchmarkReturnWorkBudget.Work::plus);
        var producerBindingLocal = BenchmarkReturnWorkBudgetRun.producerOriginBindingUpper();
        upper.merge(BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, producerBindingLocal, BenchmarkReturnWorkBudget.Work::plus);
        var coldTransportLocal = BenchmarkReturnWorkBudgetRun.coldPacketTransportUpper();
        upper.merge(BenchmarkReturnWorkBudget.Activity.NATIVE_METADATA, coldTransportLocal, BenchmarkReturnWorkBudget.Work::plus);
        var emittedLocal = BenchmarkReturnEmittedRoster.localUpper();
        upper.merge(BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, emittedLocal, BenchmarkReturnWorkBudget.Work::plus);
        var gaps = new ArrayList<Gap>();
        for (var activity : BenchmarkReturnWorkBudget.Activity.values()) {
            if (!upper.containsKey(activity) && !excluded.contains(activity)) {
                gaps.add(new Gap("SOURCE_ROW_" + activity.name(), sourceLocation(activity)));
            }
        }
        var registration = new LinkedHashMap<String, Object>();
        registration.put("state", "REGISTERED_SCALARS_REQUIRING_ACTUAL_IN_SCOPE_BINDING");
        registration.put("runId", inputs.runId()); registration.put("inputSha256", inputs.inputSha256());
        registration.put("workload", inputs.workload().id()); registration.put("rows", ROWS); registration.put("seed", SEED);
        registration.put("files", inputs.files().stream().map(file -> Map.of("role", file.role().name(), "sha256", file.sha256(), "bytes", file.bytes())).toList());
        registration.put("rootCpu", flags.rootCpu()); registration.put("outputPlanScope", "FULL_FORK_CONTROLLED_STDOUT_AND_FAILURE_TREES");
        registration.put("outputPlanSource", reporting.metadata());
        registration.put("preScopeSourceTail", BenchmarkReturnWorkBudgetSourceTails.factoryEvidence(inputs.workload()));
        registration.put("rootCounterColdSourceTail", BenchmarkReturnRootClockWork.evidence());
        registration.put("manifestCodecSourceUpper", Map.of("priorByteCap", manifestBytes, "priorFieldCap", manifestFields,
                "decoder", decode.evidence(), "encoder", encode.evidence(), "opaqueProviderInstructionsExcluded", true));
        registration.put("registrationLocalSourceUpper", localRegistration.evidence());
        registration.put("bindingIoLocalSourceUpper", localIo.entrySet().stream().collect(java.util.stream.Collectors.toMap(
                entry -> entry.getKey().name(), entry -> entry.getValue().evidence())));
        registration.put("rootOriginLocalSourceUpper", rootOriginLocal.evidence());
        registration.put("producerOriginDecodeSourceUpper", producerOriginLocal.evidence());
        registration.put("producerOriginBindingSourceUpper", producerBindingLocal.evidence());
        registration.put("producerColdTransportSourceUpper", coldTransportLocal.evidence());
        registration.put("emittedRosterLocalSourceUpper", emittedLocal.evidence());
        registration.put("terminalSourceTail", BenchmarkReturnWorkBudgetSourceTails.terminalEvidence(reporting.plan()));
        registration.put("structuralWorkBudgetQualified", false); registration.put("wholeMethodCostQualified", false);
        return new Prepared(inputs, upper, excluded, reporting.plan(), registration, gaps);
    }

    private static void knownComponentBounds(EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> upper, Inputs inputs,
                                            BenchmarkReturnWorkBudgetDefinitions.OutputPlan output) {
        long fileBytes = inputs.files().stream().mapToLong(FileInput::bytes).max().orElseThrow();
        long hashBytes = Math.max(Math.addExact(Math.multiplyExact(2, fileBytes), 2), Math.max(output.encodedUpperBytes(), 2L * 1024 * 1024));
        long nodes = BenchmarkReturnWorkBudget.MAX_MATERIAL_NODES, materialBytes = BenchmarkReturnWorkBudget.MAX_MATERIAL_BYTES;
        long samples = CLOCK_SAMPLES, cohort = inputs.workload().phases().stream().filter(PhaseInput::measured)
                .mapToLong(phase -> phase.logicalOutputs() / 2).max().orElseThrow();
        require(cohort > 0 && cohort <= RETURN_ROWS, "fixed cohort exceeds the return row cap");
        long commonElements = Math.addExact(Math.multiplyExact((long) FRAMES, FRAMES), Math.multiplyExact(10L, FRAMES));
        put(upper, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, 1, 8, 0, 0, hashBytes, 2, hashBytes, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, 1, 1, 0, 0, materialBytes, 1, nodes, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.REGISTRATION_READ, 1, 1, 0, 0, 128, 0, 1, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.CLOCK_REQUEST, 1, 1, 0, 0, 496, 0, 10, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.SUMMARY_RPC, 1, 1, 0, 0, 3828, 0, 28, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.PAGE_RPC, 1, 1, 0, 0, PAGE_BYTES, 0, 1, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.CONTROL_RPC, 1, 1, 0, 0, BenchmarkReturnWorkBudget.MAX_CONNECTOR_INPUT_BYTES, 1,
                BenchmarkReturnWorkBudget.MAX_CONNECTOR_INPUT_BYTES, 0);
        upper.put(BenchmarkReturnWorkBudget.Activity.NATIVE_METADATA, BenchmarkReturnRootClockWork.metadataUpper());
        put(upper, BenchmarkReturnWorkBudget.Activity.RESOURCE_CHECKPOINT, 1, 0, 0, 0, 0, 0, 0, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.RESOURCE_OPEN, 1, 0, 0, 0, 0, 0, 0, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.RESOURCE_READ, 1, 1, 1, 0, 0, 1, 4, 1);
        put(upper, BenchmarkReturnWorkBudget.Activity.RESOURCE_FINAL, 1, 1, 1, 0, 0, 1, 4, 1);
        put(upper, BenchmarkReturnWorkBudget.Activity.CPU_FALLBACK, 1, 1, 0, 0, 128, 0, 1, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.HEAP_READ, 1, 1, 0, 0, 0, 0, 0, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.GC_READ, 1, 1, 0, 0, 128, 0, 1, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.GC_ROSTER, 1, 1, 0, 0, 0, 1, nodes, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.PROCESS_INFO, 1, 1, 0, 0, materialBytes, 1, nodes, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.RSS_PROCESS, 1, 1, 0, 0, 4L * 65_536, 0, 0, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.COMMAND_CLIENT, 1, 1, 0, 0, 0, 0, 0, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.COMMAND_READ, 1, 1, 0, 0, materialBytes, 1, nodes, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.HELLO_CLIENT, 1, 0, 0, 0, 0, 0, 0, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.HELLO_READ, 1, 1, 0, 0, materialBytes, 1, nodes, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.PING_REQUEST, 1, 1, 0, 0, materialBytes, 1, nodes, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.TARGET_STREAM, 1, 1, 0, 0, 0, 0, 0, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.TABLE_STREAM, 1, 1, 0, 0, 0, 0, 0, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.STREAM_READ, 1, 1, 0, 0, 0, 0, 0, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.STREAM_ACCEPT, 1, 0, 1, 0, materialBytes, 1, nodes, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.STREAM_BARRIER, 1, 1, 0, 0, 0, 0, 0, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.DECODE, 1, 0, 0, 0, PAGE_BYTES, 1, 0, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.FRAME_VISIT, 1, 0, 512, 1, 0, 1, 512, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.ROW_VISIT, 1, 0, 1, 0, 0, 1, 1, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.RAW_RECHECK, 1, 0, 0, 0, RAW_BYTES, 2, RETURN_ROWS, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.COPY, 1, 1, 0, 0, Math.multiplyExact(4, Math.addExact(output.encodedUpperBytes(), 4096)), 1,
                Math.max(RETURN_ROWS, output.nodeUpper()), 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.BASE64, 1, 1, 0, 0, 4L * ((PAGE_BYTES + 2) / 3), 1, PAGE_BYTES, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.ASSOCIATION, 1, 0, RETURN_ROWS, 0, 0, 1, RETURN_ROWS, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.RETURN_MAP, 1, 0, 0, 0, 0, 1, samples, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.SOURCE_MAP, 1, 0, 0, 0, 0, 1, 1, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.CALL_POINT_CHECK, 1, 0, 0, 0, 0, 1, samples, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.SORT, 1, 6, 0, 0, 0, 1, cohort, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.SPAN, 1, 6, 0, 0, 0, 4, Math.multiplyExact(4, cohort), 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.COMMON_WINDOW, 1, 1, 1, 0, 0, 1, Math.addExact(commonElements, Math.multiplyExact(2L, FRAMES)), 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.RESOURCE_BOUNDS, 1, 0, 1, 0, 0, 2, Math.addExact(cohort, Math.multiplyExact(2L, RESOURCE_ATTEMPTS)), 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.PRODUCER_TRANSPORT, 1, 1, 0, 0, 4L * 65_536, 0, 1, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.PRODUCER_FINAL_SEAL, 1, 1, 0, 0, 128, 0, 1, 0);
    }
    private static void knownRootBounds(EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> upper, Inputs inputs,
                                       BenchmarkReturnWorkBudgetDefinitions.OutputPlan output) {
        long materialNodes = BenchmarkReturnWorkBudget.MAX_MATERIAL_NODES;
        long materialBytes = BenchmarkReturnWorkBudget.MAX_MATERIAL_BYTES;
        long guards = Math.max(inputs.workload().pipelines().size(), inputs.workload().sourceChains().size());
        long serializationRequests = Math.addExact(128, Math.addExact(Math.multiplyExact(16,
                Math.addExact(output.nodeUpper(), output.stringUtf8Upper())), Math.multiplyExact(2, output.encodedUpperBytes())));
        put(upper, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, 1, 1, 0, 0, materialBytes, 1, materialNodes, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.ACK_POLL, 1, 1, 0, 0, materialBytes, 1, Math.max(guards, materialNodes), 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.NATIVE_STATUS_POLL, 1, 1, 0, 0, materialBytes, 1, Math.max(guards, materialNodes), 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, 1, 8, 0, 0,
                Math.max(output.encodedUpperBytes(), materialBytes), 1, materialNodes, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, 1, 1, 0, 0, materialBytes, 1, materialNodes, 0);
        if (inputs.flags().rootCpu()) {
            put(upper, BenchmarkReturnWorkBudget.Activity.ROOT_CPU, 1, 1, 0, 0, materialBytes, 2, materialNodes, 0);
        }
        put(upper, BenchmarkReturnWorkBudget.Activity.CLEANUP, 1, 1, 0, 0, 0, 0, 0, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.FAILURE_RETENTION, 1, serializationRequests, 0, 0,
                output.encodedUpperBytes(), 4, output.nodeUpper(), output.nodeUpper());
        put(upper, BenchmarkReturnWorkBudget.Activity.BOOKKEEPING, 1, serializationRequests, 0, 0,
                output.encodedUpperBytes(), 4, output.nodeUpper(), output.nodeUpper());
        put(upper, BenchmarkReturnWorkBudget.Activity.REPORT_SERIALIZE, 1, serializationRequests, 1, 0,
                Math.multiplyExact(4, output.encodedUpperBytes()), 4, output.nodeUpper(), output.nodeUpper());
        put(upper, BenchmarkReturnWorkBudget.Activity.REPORT_WRITE, 1, 1, 0, 0, output.encodedUpperBytes(), 0, 0, 0);
        put(upper, BenchmarkReturnWorkBudget.Activity.REPORT_CLOSE, 1, 1, 0, 0, 0, 0, 0, 0);
    }
    private static void put(EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> map,
                            BenchmarkReturnWorkBudget.Activity activity, long calls, long requests, long records, long frames,
                            long bytes, long passes, long elements, long updates) {
        map.put(activity, new BenchmarkReturnWorkBudget.Work(calls, requests, records, frames, bytes, passes, elements, updates));
    }
    private static String sourceLocation(BenchmarkReturnWorkBudget.Activity activity) {
        return switch (activity) {
            case INSTALL_BINDINGS, SHARED_SUPPORT, ACK_POLL, NATIVE_STATUS_POLL -> "BenchmarkForkEnvironment/RealBenchmarkForkDriver fixture and poll call sites";
            case INPUT_FINGERPRINT, RUNTIME_READ, REGISTRATION_READ, PROCESS_INFO, RSS_PROCESS, GC_ROSTER -> "BenchmarkOwnedProcessReceipt/BenchmarkProcessProbe source service and material paths";
            case TARGET_STREAM, TABLE_STREAM, STREAM_READ, STREAM_ACCEPT, STREAM_BARRIER, DOCUMENT_READ -> "BenchmarkMongoDeliveryObserver/BenchmarkTableTerminalObserver/BenchmarkTableCaptureSet source loops";
            case REPORT_SERIALIZE, REPORT_WRITE, REPORT_CLOSE, FAILURE_RETENTION, BOOKKEEPING, CLEANUP -> "BenchmarkReturnWorkBudget/RealBenchmarkForkDriverIT lifecycle and reporter paths";
            case ROOT_CPU -> "RealBenchmarkForkDriver.openRootCpuDiagnostics/rootCpuCheckpoint/collectRootCpuRefusal";
            case RESOURCE_BOUNDS -> "BenchmarkReturnResourceBounds.evidence";
            default -> "BenchmarkReturnWorkBudget.Activity." + activity.name() + " active source hook";
        };
    }
    private static String recipeDigest(Inputs inputs, Map<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> upper) {
        var source = new StringBuilder(DEFINITION_ID).append(':').append(inputs.inputSha256()).append(':').append(ROWS).append(':').append(SEED);
        for (var activity : BenchmarkReturnWorkBudget.Activity.values()) {
            source.append(':').append(activity.name()); var work = upper.get(activity);
            if (work == null) { source.append(":EXCLUDED"); }
            else { source.append(':').append(work.calls()).append(':').append(work.requests()).append(':').append(work.records()).append(':').append(work.frames())
                    .append(':').append(work.bytes()).append(':').append(work.passes()).append(':').append(work.elements()).append(':').append(work.updates()); }
        }
        try { return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(source.toString().getBytes(StandardCharsets.US_ASCII))); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static void text(String value, int bound) { require(value != null && !value.isBlank() && value.length() <= bound
            && value.chars().allMatch(character -> character >= 32 && character <= 126), "profile text is missing or exceeds its bound"); }
    private static void digest(String value) { require(value != null && value.matches("[a-f0-9]{64}"), "profile digest is missing"); }
    private static void require(boolean condition, String reason) { if (!condition) { throw new AssertionError("return work profile " + reason); } }
}
