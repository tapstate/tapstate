package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Source-recipe admission is distinct from later fork facts and performance-window acceptance. */
final class BenchmarkReturnMethodAdmission {
    static final String METHOD = "ACKNOWLEDGED_FULL_CALL_RETURN_NOMINAL_V1";
    static final String ENDPOINT = "ordinary nontransactional acknowledged writeRecord successful return";
    private static final String JNI_SOURCE = "f25f42fdf196d1af702782cd6a87a86f7b696f10689cf27e768b377a19cff4a1";
    private static final String JNI_BINARY = "4ce1204977af372080e3a3a8ada2c03ce678a15b337e3201f13d30b22a4700c9";
    private static final List<String> CLOCK_CLASSES = List.of("PdkBenchmarkClock", "PdkBenchmarkClock$Clock",
            "PdkBenchmarkClock$JniAccess", "PdkBenchmarkClock$NativeAccess");
    private static final List<String> INVOCATION_PATH = List.of("PdkBenchmarkClock.nanoTime/source",
            "Clock.read", "JniAccess.read", "nativeNanoTime", "mach_absolute_time", "unsigned128 multiply/divide");

    enum ProviderContract { MACH_ABSOLUTE_TIME_NOMINAL_MONOTONIC_EXCLUDING_SLEEP }

    /** Facts to inspect, never a boolean or digest masquerading as semantic approval. */
    record InspectionRequest(String method, ProviderContract providerContract, List<String> invocationPath,
                             String jniSourceSha256, String jniBinarySha256, Map<String, Object> selectedRootRoute) {
        InspectionRequest { invocationPath = List.copyOf(invocationPath); selectedRootRoute = freezeMetadata(selectedRootRoute); }
    }

    /** Integration must install an independently reviewed source/API authority, not a caller marker. */
    @FunctionalInterface interface IndependentSemanticReview {
        void requireInspectedDirectInvocationAndProviderContract(InspectionRequest inspected);
    }

    /** This recipe contains no completed-method or measured-CPU claim. */
    static final class CostRecipe {
        private final BenchmarkReturnWorkBudgetBindings.AuthenticatedProfile profile;
        private CostRecipe(BenchmarkReturnWorkBudgetBindings.AuthenticatedProfile profile) { this.profile = profile; }
        BenchmarkReturnWorkBudgetBindings.AuthenticatedProfile profile() { return profile; }
        Set<String> supportedWorkloads() { return Set.of("stateless"); }
        Map<String, Object> evidence() {
            var out = facts("CACHED_STRUCTURAL_RECIPE_REQUIRING_SEPARATE_COMPLETE_COST_PROOF");
            out.put("definitionId", profile.definition().definitionId());
            out.put("sourceFormulaSha256", profile.definition().sourceFormulaSha256());
            out.put("supportedWorkloads", List.of("stateless")); return Map.copyOf(out);
        }
    }

    record Preflight(String method, CostRecipe costRecipe,
                     BenchmarkReturnWorkBudgetBindings.HelperManifest helper,
                     BenchmarkReturnWorkBudgetBindingsIo.Result authenticatedBytes,
                     BenchmarkCausalClock.Identity root, String library, String actualRootMetadata) { }

    /** Constructed only after recipe, binding and independent semantic review, never from JSON flags. */
    static final class AdmittedMethod {
        private final CostRecipe cost;
        private final BenchmarkCausalClock.Identity root;
        private final String library;
        private final Map<String, Object> route;
        private final boolean controlOnly;
        private AdmittedMethod(Preflight input, Map<String, Object> route, boolean controlOnly) {
            this.cost = input.costRecipe(); this.root = input.root(); this.library = input.library();
            this.route = freezeMetadata(route); this.controlOnly = controlOnly;
        }
        Set<String> supportedWorkloads() { return cost.supportedWorkloads(); }
        void requireProductionAuthority() { require(!controlOnly, "CONTROL_REVIEW_CANNOT_SELECT_FORMAL_METHOD"); }
        Map<String, Object> evidence() {
            var out = facts("SOURCE_RECIPE_ADMITTED_AWAITING_ORIGINAL_FORK_FACTS");
            out.put("method", METHOD); out.put("endpoint", ENDPOINT); out.put("rootPid", root.pid());
            out.put("rootJvmStartTimeMillis", root.jvmStartTimeMillis());
            out.put("providerContract", ProviderContract.MACH_ABSOLUTE_TIME_NOMINAL_MONOTONIC_EXCLUDING_SLEEP.name());
            out.put("selectedRootRoute", route); out.put("costRecipe", cost.evidence()); out.put("controlOnly", controlOnly);
            out.put("costProofScope", "SEPARATE_TYPED_COMPLETE_METHOD_COMPONENT_NOT_IMPLIED_BY_CACHED_RECIPE");
            return Map.copyOf(out);
        }
    }

    record PhaseFacts(BenchmarkWorkloadDefinitions.Workload workload, BenchmarkWorkloadDefinitions.Phase phase,
                      List<BenchmarkForkEnvironment.BatchResult> sourceBatches, BenchmarkWriteReturnCapture.Result capture,
                      BenchmarkCausalClock.Identity root, BenchmarkCausalClock.Identity owned,
                      Map<String, Object> nativeBefore, Map<String, Object> nativeAfter,
                      List<BenchmarkTargetClock.Reading> serialHello) {
        PhaseFacts {
            require(sourceBatches != null && sourceBatches.size() <= 960 && serialHello != null
                    && serialHello.size() <= BenchmarkTargetClock.MAX_INTERIOR_SAMPLES,
                    "PHASE_ROSTER_EXCEEDS_PRIOR_CAP");
            require(capture != null && !capture.calls().isEmpty(), "CAPTURE_CALL_ROSTER_EMPTY");
            require(capture != null && capture.calls().size() <= 512 && capture.samples().size() <= 512
                    && capture.pagesBase64().size() <= 512 && capture.costStages().isEmpty(),
                    "CAPTURE_ROSTER_EXCEEDS_PRIOR_CAP_OR_CONFLICTS");
            sourceBatches = List.copyOf(sourceBatches); serialHello = List.copyOf(serialHello);
            nativeBefore = freezeCold(nativeBefore); nativeAfter = freezeCold(nativeAfter);
        }
    }

    /** Bound clock/cohort facts can exist while the unchanged steady-window certificate fails. */
    static final class BoundPhase {
        private final Map<String, Object> evidence;
        private final Map<String, Object> commonWindow;
        private final List<BenchmarkReturnTimeBounds.Delivery> deliveries;
        private BoundPhase(Map<String, Object> evidence, Map<String, Object> commonWindow, List<BenchmarkReturnTimeBounds.Delivery> deliveries,
                           BenchmarkReturnWorkBudgetDefinitions.OutputPlan plan) {
            require(deliveries != null && deliveries.size() <= 262144, "FULL_COHORT_PROJECTION_EXCEEDS_PRIOR_CAP");
            this.deliveries = List.copyOf(deliveries);
            this.evidence = freeze(evidence, plan); this.commonWindow = freezeMetadata(commonWindow);
        }
        Map<String, Object> evidence() { return evidence; }
        Map<String, Object> commonWindow() { return commonWindow; }
        List<BenchmarkReturnTimeBounds.Delivery> deliveries() { return deliveries; }
        void requireSteadyWindow() {
            require(Boolean.TRUE.equals(commonWindow.get("commonWindowCertificatePassed"))
                    && "PASS".equals(commonWindow.get("state"))
                    && "ALL_CONSERVATIVE_COMMON_WINDOW_RULES_HOLD".equals(commonWindow.get("reason")),
                    "RETURN_COMMON_WINDOW_NOT_QUALIFIED");
        }
    }

    static final class Refusal extends AssertionError {
        private final Map<String, Object> retained;
        private Refusal(String reason) { this(reason, null); }
        private Refusal(String reason, Throwable cause) {
            super("return method admission " + reason, cause);
            var out = facts("UNKNOWN"); out.put("reason", reason); retained = Map.copyOf(out);
        }
        Map<String, Object> retainedEvidence() { return retained; }
    }

    private BenchmarkReturnMethodAdmission() { }

    static CostRecipe structuralRecipe(BenchmarkReturnWorkBudgetBindings.AuthenticatedProfile profile) {
        require(profile != null && profile.prepared() != null, "STRUCTURAL_RECIPE_MISSING");
        var input = profile.prepared().inputs();
        require("stateless".equals(input.workload().id()) && input.workload().rows() == 96000
                && input.workload().seed() == 424242, "STRUCTURAL_RECIPE_WORKLOAD_UNSUPPORTED");
        require(profile.definition().sourceFormulaSha256().equals(profile.prepared().requireDefinition().sourceFormulaSha256()),
                "STRUCTURAL_RECIPE_DEFINITION_DIFFERS");
        return new CostRecipe(profile);
    }

    /** No production semantic authority is installed by this standalone proposal. */
    static AdmittedMethod admit(Preflight input) {
        return checkedAdmit(input, inspected -> { throw new Refusal("INDEPENDENT_NATIVE_SEMANTIC_REVIEW_NOT_INSTALLED"); }, false);
    }

    /** Pure component controls cannot issue a production-selection token. */
    static AdmittedMethod admitForControl(Preflight input, IndependentSemanticReview review) {
        return checkedAdmit(input, review, true);
    }

    private static AdmittedMethod checkedAdmit(Preflight input, IndependentSemanticReview review, boolean controlOnly) {
        require(input != null && METHOD.equals(input.method()) && input.costRecipe() != null
                && input.helper() != null && input.authenticatedBytes() != null && input.root() != null,
                "PREFLIGHT_COMPONENT_MISSING_OR_METHOD_DIFFERS");
        require(review != null, "INDEPENDENT_NATIVE_SEMANTIC_REVIEW_MISSING");
        var helper = input.helper(); var bytes = input.authenticatedBytes(); var profile = input.costRecipe().profile();
        require(bytes.sources().size() <= BenchmarkReturnWorkBudgetBindings.SourceRole.values().length
                && bytes.classes().size() <= 512 && bytes.origins().size() <= 1024 && bytes.files().size() <= 10,
                "PREFLIGHT_AUTHENTICATED_ROSTER_EXCEEDS_PRIOR_CAP");
        for (var source : bytes.sources()) require(source.blob().bytes() <= 1048576, "PREFLIGHT_SOURCE_MEMBER_EXCEEDS_PRIOR_CAP");
        for (var member : bytes.classes()) require(member.blob().bytes() <= 1048576, "PREFLIGHT_CLASS_MEMBER_EXCEEDS_PRIOR_CAP");
        require(BenchmarkReturnWorkBudgetBindings.supportedHelper(helper)
                && profile.prepared().inputs().runId().equals(helper.runId())
                && profile.prepared().inputs().inputSha256().equals(helper.inputSha256())
                && profile.definition().definitionId().equals(helper.rootDefinitionId())
                && profile.definition().sourceFormulaSha256().equals(helper.rootFormulaSha256()), "PREFLIGHT_RECIPE_BINDING_DIFFERS");
        require(Objects.equals(profile.prepared().inputs().producerWriter(), helper.producerWriter())
                && Objects.equals(helper.producerWriter(), bytes.producerWriter()),
                "PREFLIGHT_PRODUCER_WRITER_DIFFERS_FROM_CACHED_RECIPE");
        require(helper.sources().equals(bytes.sources()) && helper.classes().equals(bytes.classes())
                && Objects.equals(helper.producerWriter(), bytes.producerWriter()), "PREFLIGHT_AUTHENTICATED_MATERIAL_DIFFERS");
        var nativeFile = bytes.files().stream().filter(file -> file.role() == BenchmarkReturnWorkBudgetProfile.FileRole.NATIVE_LIBRARY).toList();
        require(nativeFile.size() == 1 && nativeFile.getFirst().path().equals(input.library())
                && nativeFile.getFirst().blob().bytes() <= 65536
                && nativeFile.getFirst().blob().sha256().equals(JNI_BINARY), "PREFLIGHT_NATIVE_FILE_DIFFERS");
        var registeredNative = profile.prepared().inputs().files().stream()
                .filter(file -> file.role() == BenchmarkReturnWorkBudgetProfile.FileRole.NATIVE_LIBRARY).toList();
        require(registeredNative.size() == 1
                && registeredNative.getFirst().bytes() == nativeFile.getFirst().blob().bytes()
                && registeredNative.getFirst().sha256().equals(nativeFile.getFirst().blob().sha256()),
                "PREFLIGHT_NATIVE_FILE_DIFFERS_FROM_CACHED_RECIPE");
        var source = bytes.sources().stream().filter(item -> item.role() == BenchmarkReturnWorkBudgetBindings.SourceRole.JNI_SOURCE).toList();
        require(source.size() == 1 && source.getFirst().blob().sha256().equals(JNI_SOURCE), "PREFLIGHT_NATIVE_SOURCE_DIFFERS");
        Map<String, Object> parsed;
        try { parsed = BenchmarkNativeClockEvidence.parse(input.actualRootMetadata(), input.root(), input.library()); }
        catch (RuntimeException | Error malformed) { throw new Refusal("ACTUAL_ROOT_NATIVE_METADATA_REFUSED", malformed); }
        require("NATIVE_RECORDED".equals(parsed.get("state")), "ACTUAL_ROOT_NATIVE_PROVIDER_UNKNOWN");
        Map<String, Object> route;
        try { route = BenchmarkNativeReturnClock.supportedRootSnapshot(parsed); }
        catch (RuntimeException | Error unsupported) { throw new Refusal("ACTUAL_ROOT_NATIVE_ROUTE_UNSUPPORTED", unsupported); }
        var classes = new LinkedHashMap<String, Object>();
        for (String name : CLOCK_CLASSES) {
            String binary = "io.tapstate.adapters.pdk." + name;
            var members = bytes.classes().stream().filter(item -> item.binaryName().equals(binary)).toList();
            require(members.size() == 1 && members.getFirst().role() == BenchmarkReturnWorkBudgetBindings.SourceRole.PDK_CLOCK,
                    "PREFLIGHT_CLOCK_CLASS_ROSTER_DIFFERS");
            classes.put(name, members.getFirst().blob().sha256());
        }
        require(classes.equals(route.get("classHashes")), "ACTUAL_ROOT_CLOCK_CLASS_BYTES_DIFFER");
        review.requireInspectedDirectInvocationAndProviderContract(new InspectionRequest(METHOD,
                ProviderContract.MACH_ABSOLUTE_TIME_NOMINAL_MONOTONIC_EXCLUDING_SLEEP, INVOCATION_PATH,
                JNI_SOURCE, JNI_BINARY, route));
        return new AdmittedMethod(input, route, controlOnly);
    }

    static BoundPhase verifyPhase(AdmittedMethod admitted, PhaseFacts input) {
        require(admitted != null && input != null && input.workload() != null && input.phase() != null && input.capture() != null,
                "ORIGINAL_PHASE_FACTS_MISSING");
        require(admitted.supportedWorkloads().contains(input.workload().id()) && input.workload().rows() == 96000
                && input.workload().seed() == 424242
                && BenchmarkReturnWorkBudgetProfile.WorkloadInput.from(input.workload())
                .equals(admitted.cost.profile().prepared().inputs().workload()), "ORIGINAL_WORKLOAD_DIFFERS_OR_UNSUPPORTED");
        require(input.phase().equals(input.workload().phase(input.phase().id())), "ORIGINAL_PHASE_SQL_OR_CADENCE_DIFFERS");
        require(admitted.root.equals(input.root()) && input.owned() != null && input.owned().pid() != input.root().pid(),
                "ORIGINAL_ROOT_OR_OWNED_IDENTITY_DIFFERS");
        BenchmarkNativeNominalReturnClock clock;
        try { clock = new BenchmarkNativeNominalReturnClock(input.owned(), input.root(), admitted.library,
                input.capture().samples(), input.nativeBefore(), input.nativeAfter()); }
        catch (RuntimeException | Error invalid) { throw new Refusal("ORIGINAL_FORK_NATIVE_MAPPING_REFUSED", invalid); }
        require(admitted.route.equals(clock.evidence().get("snapshot")), "ORIGINAL_FORK_ROUTE_DIFFERS_FROM_ADMITTED_ROOT");
        var previous = new HashMap<Integer, Long>();
        for (var call : input.capture().calls().stream().sorted(java.util.Comparator.comparingLong(BenchmarkWriteReturnAssembly.FullCall::sequence)).toList()) {
            Long last = previous.put(call.writer(), call.observedNanos());
            require(last == null || call.beganNanos() >= last, "SERIAL_WRITER_COUNTER_REGRESSION");
        }
        require(!input.sourceBatches().isEmpty() && input.capture().samples().size() >= 2, "SOURCE_OR_CLOCK_ROSTER_MISSING");
        var first = input.capture().samples().getFirst(); var last = input.capture().samples().getLast();
        BenchmarkForkEnvironment.BatchResult previousSource = null;
        for (var source : input.sourceBatches()) {
            require(source.issuedAtNanos() >= first.driverBeforeNanos() && source.completedAtNanos() <= last.driverAfterNanos(),
                    "SOURCE_POINTS_OUTSIDE_BOUND_CAPTURE_WINDOW");
            if (previousSource != null) {
                require(source.issuedAtNanos() >= previousSource.completedAtNanos(), "ORIGINAL_SOURCE_SERIAL_ORDER_DIFFERS");
            }
            clock.sourcePoint(source.issuedAtNanos()); clock.sourcePoint(source.completedAtNanos());
            previousSource = source;
        }
        require(input.serialHello().size() >= 3 && input.serialHello().size() <= BenchmarkTargetClock.MAX_INTERIOR_SAMPLES,
                "SERIAL_HELLO_ROSTER_MISSING");
        var hello = BenchmarkTargetClock.validateSeries(input.serialHello());
        for (var reading : input.serialHello().subList(1, input.serialHello().size())) {
            BenchmarkTargetClock.validate(input.serialHello().getFirst(), reading);
        }
        BenchmarkTargetClock.validate(input.serialHello().getFirst(), input.serialHello().getLast());
        require(input.serialHello().getFirst().completedAtNanos() <= input.sourceBatches().getFirst().issuedAtNanos()
                && input.serialHello().getLast().startedAtNanos() >= input.capture().calls().stream()
                .mapToLong(BenchmarkWriteReturnAssembly.FullCall::observedNanos).max().orElseThrow(), "SERIAL_HELLO_DOES_NOT_COVER_ORIGINAL_PHASE");
        Map<String, Object> phase;
        try { phase = BenchmarkWriteReturnPhaseEvidence.recordNominal(input.workload(), input.phase(), input.sourceBatches(), input.capture(), clock); }
        catch (RuntimeException | Error invalid) { throw new Refusal("ORIGINAL_RAW_SOURCE_OR_COHORT_REFUSED", invalid); }
        var associations = BenchmarkWriteReturnExpectations.associate(input.workload(), input.phase(), input.sourceBatches(), input.capture().calls());
        var deliveries = BenchmarkReturnTimeBounds.map(associations, input.owned(), clock);
        var common = BenchmarkReturnCommonWindow.evidence(deliveries);
        var out = new LinkedHashMap<String, Object>(phase);
        out.put("method", METHOD); out.put("endpoint", ENDPOINT);
        out.put("methodAdmission", admitted.evidence()); out.put("helloSerialValidation", hello);
        out.put("helloRootCoordinateBasis", "ORIGINAL_TARGETCLOCK_READ_USES_WORKBUDGET_ROOTNANOTIME_TO_PDK_NATIVE_SOURCE");
        out.put("commonWindowCertificate", common);
        out.put("newConsumerWorkAccounting", "DUPLICATE_RAW_ASSOCIATION_MAPPING_AND_FREEZE_NOT_YET_IN_STRUCTURAL_SOURCE_RECIPE");
        out.put("componentState", "ORIGINAL_FORK_CLOCK_COHORT_FACTS_MATCHED_NO_FORMAL_METRIC_CREDIT");
        BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> out.put(flag, false));
        return new BoundPhase(out, common, deliveries, admitted.cost.profile().prepared().fieldsRegisteredOutputPlan());
    }

    private static LinkedHashMap<String, Object> facts(String state) {
        var out = new LinkedHashMap<String, Object>(); out.put("state", state);
        BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> out.put(flag, false)); return out;
    }
    private static Map<String, Object> freezeMetadata(Map<String, Object> value) {
        return freeze(value, new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(4096, 16, 8192, 65536, 512, 24));
    }
    private static Map<String, Object> freezeCold(Map<String, Object> value) {
        // Two original raw/typed 34-part forms plus bounded root/owned clock maps; no observed size is a cap.
        long packets = 4L * 2080768 + 4L * 8192;
        return freeze(value, new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(32768, 16, 64512, packets, 512, 24));
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> freeze(Map<String, Object> value,
            BenchmarkReturnWorkBudgetDefinitions.OutputPlan plan) {
        require(value != null, "BOUNDED_FACT_OBJECT_MISSING");
        return (Map<String, Object>) BenchmarkReturnWorkBudgetDefinitions.freezeOutputTree(value, plan).value();
    }
    private static void require(boolean condition, String reason) { if (!condition) throw new Refusal(reason); }
}
