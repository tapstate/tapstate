package io.tapstate.e2e;

import com.sun.jdi.*;
import com.sun.jdi.connect.Connector;
import com.sun.jdi.connect.ListeningConnector;
import com.sun.jdi.event.*;
import com.sun.jdi.request.*;
import io.tapstate.core.common.JsonWriter;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/** Passive, artifact-pinned observation of one native pipeline; fault actions belong to its caller. */
final class NativeTelemetryIdentityJdiSession implements AutoCloseable {
    interface OwnedLauncher { RealProcessServer launch(Path jar, List<String> jvmArguments) throws Exception; }
    enum Target { JOB, ADMISSION, REPLACEMENT_ADMISSION, RAW_CONTINUATION, SUBMIT, LOG, OFFER, VISIBLE, PRODUCE, PREPARE, FOLDER_FORGET, EXPORT_FORGET, EXPORT_INCARNATION,
        PUBLISHER_SWEEP, EXPORT_SWEEP, FOLDER_SWEEP, JOINED_TAKEOVER }
    enum BindingProfile { FULL, RESTORED_BEFORE_FIRST_START }
    record Binding(String type, String method, String descriptor, String origin,
            String codeSha256, long loaderId, List<Integer> returns) { }
    record Counts(long entries, long normalReturns, long exceptionalExits, long inFlight) { }
    record AuthorityReceipt(String pipelineId, String clusterId, String incarnation, long generation,
            String coordinationId, String capturedAt) {
        AuthorityReceipt {
            Objects.requireNonNull(pipelineId); Objects.requireNonNull(clusterId);
            Objects.requireNonNull(incarnation); Objects.requireNonNull(coordinationId); Objects.requireNonNull(capturedAt);
            if (pipelineId.isBlank() || clusterId.isBlank() || incarnation.isBlank()
                    || generation < 1 || coordinationId.isBlank()) { throw invalid("invalid caller authority receipt"); }
        }
        Map<String, Object> scope() { return Map.of("incarnation", incarnation, "generation", generation); }
    }
    record CapturedJob(ObjectReference proxy, long proxyId, Map<String, Object> job,
            Map<String, Object> scope, long receiverId, long executionObjectId) { }
    record ClaimedSubmission(CapturedJob job, long admissionObjectId, long ownershipReceiverId,
            long submitReceiverId, Map<String, Object> fence, List<String> members, Map<String, Object> claim) {
        ClaimedSubmission {
            fence = Map.copyOf(fence); members = List.copyOf(members); claim = Map.copyOf(claim);
        }
    }
    record ClaimedReplacement(ClaimedSubmission submission, long successorAdmissionObjectId,
            long reservationObjectId, long advancedClaimObjectId, Map<String, Object> reservation) {
        ClaimedReplacement { reservation = Map.copyOf(reservation); }
    }
    record Boundary(String phase, long sequence, String jarSha256, String pipelineId,
            Map<Target, Binding> bindings, Map<Target, Counts> counts, List<Map<String, Object>> records,
            List<AuthorityReceipt> authorityReceipts, Set<String> unverified, Set<String> decodedLayouts,
            Map<String, Object> logFamilyCalibration,
            String vmVersion, long events, long handlingNanos, int openCalls, boolean queueDrained,
            boolean ownedVmDeath, boolean ownedVmDisconnected, BindingProfile bindingProfile,
            Set<Target> deferredUnpreparedBindings, boolean replacementObservationEnabled,
            boolean rawContinuationObservationEnabled, boolean joinedTakeoverObservationEnabled) {
        Boundary {
            bindings = Map.copyOf(bindings); counts = Map.copyOf(counts); records = List.copyOf(records);
            authorityReceipts = List.copyOf(authorityReceipts); unverified = Set.copyOf(unverified);
            decodedLayouts = Set.copyOf(decodedLayouts);
            logFamilyCalibration = Map.copyOf(logFamilyCalibration);
            Objects.requireNonNull(bindingProfile);
            deferredUnpreparedBindings = Set.copyOf(deferredUnpreparedBindings);
        }
        boolean invocationDrainComplete() {
            return queueDrained && openCalls == 0 && counts.values().stream().allMatch(count ->
                    count.inFlight() == 0 && count.entries() == count.normalReturns() + count.exceptionalExits());
        }
        boolean decodedAndAuthorityBound() {
            return unverified.isEmpty() && bindings.size() == Target.values().length - (replacementObservationEnabled ? 0 : 1)
                    - (rawContinuationObservationEnabled ? 0 : 1) - (joinedTakeoverObservationEnabled ? 0 : 1)
                    - deferredUnpreparedBindings.size()
                    && invocationDrainComplete() && !authorityReceipts.isEmpty();
        }
        Map<String, Object> evidence() {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("phase", phase); out.put("sequence", sequence); out.put("jarSha256", jarSha256);
            out.put("pipelineId", pipelineId);
            Map<String, Object> bound = new LinkedHashMap<>(), counted = new LinkedHashMap<>();
            bindings.forEach((target, binding) -> bound.put(target.name(), Map.of(
                    "type", binding.type(), "method", binding.method(), "descriptor", binding.descriptor(),
                    "origin", binding.origin(), "codeSha256", binding.codeSha256(),
                    "loaderId", binding.loaderId(), "returnOffsets", binding.returns())));
            counts.forEach((target, count) -> counted.put(target.name(), Map.of(
                    "entries", count.entries(), "normalReturns", count.normalReturns(),
                    "exceptionalExits", count.exceptionalExits(), "inFlight", count.inFlight())));
            out.put("bindings", bound); out.put("counts", counted); out.put("records", records);
            out.put("authorityReceipts", authorityReceipts.stream().map(receipt -> Map.of(
                    "pipelineId", receipt.pipelineId(), "clusterId", receipt.clusterId(),
                    "incarnation", receipt.incarnation(), "generation", receipt.generation(),
                    "coordinationId", receipt.coordinationId(), "capturedAt", receipt.capturedAt())).toList());
            out.put("unverified", unverified.stream().sorted().toList());
            out.put("requiredBindingProfile", bindingProfile.name());
            out.put("deferredUnpreparedBindings", deferredUnpreparedBindings.stream().map(Enum::name).sorted().toList());
            out.put("decodedLayouts", decodedLayouts.stream().sorted().toList());
            out.put("logFamilyCalibration", logFamilyCalibration);
            out.put("vmVersion", vmVersion); out.put("events", events); out.put("handlingNanos", handlingNanos);
            out.put("openCalls", openCalls); out.put("queueDrained", queueDrained);
            out.put("ownedVmDeath", ownedVmDeath); out.put("ownedVmDisconnected", ownedVmDisconnected);
            out.put("submissionObservationMode", replacementObservationEnabled
                    ? "ORDINARY_AND_REPLACEMENT_ADMISSION" : "ORDINARY_ADMISSION_ONLY");
            out.put("replacementObservationEnabled", replacementObservationEnabled);
            out.put("rawContinuationObservationEnabled", rawContinuationObservationEnabled);
            out.put("joinedTakeoverObservationEnabled", joinedTakeoverObservationEnabled);
            out.put("passiveObserver", true); out.put("performanceAcceptanceEligible", false);
            return Map.copyOf(out);
        }
    }
    /** Crash receipts retain observed data without claiming drained calls or a decoded old heap. */
    record OwnedCrash(long ownedPid, long markedAtEvent, boolean processDeadBeforeCleanup,
            boolean processDeadAfterCleanup, boolean disconnectBeforeDispose, String disconnectReceipt,
            boolean cleanupForcedKill, boolean artifactUnchanged, boolean pumpStopped,
            Boundary observed, List<Map<String, Object>> unpairedCalls, String evidenceFailure,
            String observerFailure, String cleanupFailure) {
        OwnedCrash { if (unpairedCalls != null) { unpairedCalls = List.copyOf(unpairedCalls); } }
        boolean ownedFaultObserved() {
            return processDeadBeforeCleanup && disconnectBeforeDispose && !cleanupForcedKill;
        }
        Map<String, Object> evidence() {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("status", "INCOMPLETE"); out.put("ownedPid", ownedPid); out.put("markedAtEvent", markedAtEvent);
            out.put("ownedFaultObserved", ownedFaultObserved());
            out.put("processDeadBeforeCleanup", processDeadBeforeCleanup);
            out.put("processDeadAfterCleanup", processDeadAfterCleanup);
            out.put("disconnectBeforeDispose", disconnectBeforeDispose); out.put("disconnectReceipt", disconnectReceipt);
            out.put("cleanupForcedKill", cleanupForcedKill); out.put("artifactUnchanged", artifactUnchanged);
            out.put("pumpStopped", pumpStopped); out.put("normalDrain", false); out.put("decodedComplete", false);
            out.put("oldHeapCleanup", "UNKNOWN");
            out.put("observed", observed == null ? null : observed.evidence()); out.put("unpairedCalls", unpairedCalls);
            out.put("evidenceStatus", evidenceFailure == null && observed != null ? "RETAINED_INCOMPLETE" : "UNKNOWN");
            out.put("evidenceFailure", evidenceFailure); out.put("observerFailure", observerFailure);
            out.put("cleanupFailure", cleanupFailure); out.put("passiveObserver", true);
            out.put("performanceAcceptanceEligible", false);
            return Collections.unmodifiableMap(out);
        }
    }
    private record Spec(Target target, String type, String method, String descriptor, int arguments, int pipeline) { }
    private record Image(String origin, Map<String, byte[]> methods, Map<String, String> fields) { }
    private record Site(Target target, boolean entry) { }
    private record UnqualifiedEntry(Target target, long thread, long receiver, int depth, long eventOrder) { }
    private record Replacement(long argumentId, long reservationId, Long advancedClaimId, Map<String, Object> reservation) { }
    private record Admission(long objectId, long ownershipReceiverId, Map<String, Object> fence,
            List<String> members, Map<String, Object> claim, Optional<Replacement> replacement) { }
    private static final class Totals { long entries, normal, exceptional; }
    private static final class Call {
        final long id;
        final Spec spec;
        final int depth;
        final ObjectReference receiver;
        final List<Value> arguments;
        final Map<String, Object> entry;
        final boolean qualified;
        boolean logCalibration;
        final List<Map<String, Object>> included = new ArrayList<>();
        boolean escaping;
        MethodExitRequest exit;
        CapturedJob submittedJob;
        Call(long id, Spec spec, int depth, ObjectReference receiver,
                List<Value> arguments, Map<String, Object> entry, boolean qualified) {
            this.id = id; this.spec = spec; this.depth = depth; this.receiver = receiver;
            this.arguments = arguments; this.entry = entry; this.qualified = qualified;
        }
    }
    private static final class ThreadState {
        final ThreadReference thread;
        final Deque<Call> calls = new ArrayDeque<>();
        final ExceptionRequest exceptions;
        final ThreadDeathRequest death;
        ThreadState(ThreadReference thread, ExceptionRequest exceptions, ThreadDeathRequest death) {
            this.thread = thread; this.exceptions = exceptions; this.death = death;
        }
    }
    private static final String LOADER = "org.springframework.boot.loader.launch.LaunchedClassLoader";
    private static final String OTEL = "io.tapstate.adapters.otel.";
    private static final String SCOPE = "Lio/tapstate/spi/store/ObservationStore$Scope;";
    private static final String EXECUTION = "Lio/tapstate/app/PipelineActuationOwnership$Execution;";
    private static final String EXECUTION_JOB = "Lio/tapstate/runtime/engine/Engine$ExecutionJob;";
    private static final String FAILURE = "Lio/tapstate/core/lifecycle/ObservationFailure;";
    private static final int MAX_EVENTS = 50000, MAX_THREADS = 64, MAX_CALLS = 64, MAX_FRAMES = 512;
    private static final int MAX_RECORDS = 512, MAX_CLASSES = 128, MAX_CLASS_BYTES = 1048576;
    private static final int MAX_OPEN_CALLS = 128;
    private static final long MAX_PHASE_BYTES = 2L * 1024 * 1024;
    private static final long MAX_RECORD_BYTES = 65536;
    private static final long MAX_PRODUCE_RECORD_BYTES = 131072;
    private static final List<Spec> SPECS = List.of(
            new Spec(Target.JOB, "io.tapstate.runtime.engine.Engine", "executionJob",
                    "(Ljava/lang/String;Lcom/hazelcast/jet/Job;)Lio/tapstate/runtime/engine/Engine$ExecutionJob;", 2, 0),
            new Spec(Target.ADMISSION, "io.tapstate.app.PipelineActuationOwnership", "beginExecution",
                    "(Ljava/lang/String;)" + EXECUTION, 1, 0),
            new Spec(Target.REPLACEMENT_ADMISSION, "io.tapstate.app.PipelineActuationOwnership", "adoptAdmission",
                    "(Lio/tapstate/spi/store/SuccessorAdmission;)" + EXECUTION, 1, -1),
            new Spec(Target.RAW_CONTINUATION, "io.tapstate.app.ObservationScopeRegistry", "prepareContinuationPublication",
                    "(Lio/tapstate/runtime/scheduler/ObservationPublisher$Prepared;"
                            + "Lio/tapstate/app/ObservationScopeRegistry$ActualTarget;"
                            + "Ljava/util/function/BooleanSupplier;)Ljava/util/Optional;", 3, -1),
            new Spec(Target.SUBMIT, "io.tapstate.app.EngineLifecycleActuator$2", "submit", "()V", 0, -1),
            new Spec(Target.LOG, "io.tapstate.core.logging.RingBufferLogSink", "append",
                    "(Ljava/lang/String;Lio/tapstate/core/logging/LogSink$Scope;Lio/tapstate/core/logging/LogLine;)V", 3, 0),
            new Spec(Target.OFFER, OTEL + "FactsMetricProducer", "offerFoldedScoped",
                    "(Ljava/lang/String;Lio/tapstate/spi/metrics/MetricsExport$ScopeToken;"
                            + "Lio/tapstate/core/lifecycle/PipelineState;Ljava/time/Instant;Ljava/util/List;)V", 5, 0),
            new Spec(Target.VISIBLE, OTEL + "FactsMetricProducer", "visible",
                    "(Ljava/lang/String;Lio/tapstate/adapters/otel/FactsMetricProducer$Offered;)Z", 2, 0),
            new Spec(Target.PRODUCE, OTEL + "FactsMetricProducer", "produce",
                    "(Lio/opentelemetry/sdk/resources/Resource;)Ljava/util/Collection;", 1, -1),
            new Spec(Target.PREPARE, "io.tapstate.runtime.scheduler.ObservationPublisher", "prepare",
                    "(Ljava/lang/String;" + FAILURE + SCOPE
                            + "Ljava/util/function/BooleanSupplier;Ljava/lang/Runnable;)Ljava/util/Optional;", 5, 0),
            new Spec(Target.FOLDER_FORGET, "io.tapstate.core.lifecycle.CardinalityBudget$Folder", "forgetPipeline",
                    "(Ljava/lang/String;)V", 1, 0),
            new Spec(Target.EXPORT_FORGET, OTEL + "FactsMetricProducer", "forgetPipeline",
                    "(Ljava/lang/String;)V", 1, 0),
            new Spec(Target.EXPORT_INCARNATION, OTEL + "FactsMetricProducer", "forgetIncarnation",
                    "(Ljava/lang/String;Ljava/lang/String;)V", 2, 0),
            new Spec(Target.PUBLISHER_SWEEP, "io.tapstate.runtime.scheduler.ObservationPublisher", "forgetPipelinesOutside",
                    "(Ljava/util/Collection;)V", 1, -1),
            new Spec(Target.EXPORT_SWEEP, OTEL + "FactsMetricProducer", "forgetPipelinesOutside",
                    "(Ljava/util/Collection;)V", 1, -1),
            new Spec(Target.FOLDER_SWEEP, "io.tapstate.core.lifecycle.CardinalityBudget$Folder", "forgetPipelinesOutside",
                    "(Ljava/util/Collection;)V", 1, -1),
            new Spec(Target.JOINED_TAKEOVER, "io.tapstate.runtime.srs.CaptureRunUnit", "begin",
                    "(Lio/tapstate/runtime/srs/CaptureRunSpec;Lio/tapstate/runtime/srs/CaptureHandoff;Z)"
                            + "Lio/tapstate/runtime/srs/CaptureRun;", 3, -1));

    private final Path jar;
    private final String sha, pipeline;
    private final boolean replacementObservationEnabled, rawContinuationObservationEnabled, joinedTakeoverObservationEnabled;
    private final RealProcessServer server;
    private final VirtualMachine vm;
    private final String vmVersion;
    private final Thread pump;
    private final Object lock = new Object(), commands = new Object();
    private final Map<String, Image> images;
    private final Map<Target, Spec> specs = new EnumMap<>(Target.class);
    private final Map<Target, Method> methods = new EnumMap<>(Target.class);
    private final Map<Target, Binding> bindings = new EnumMap<>(Target.class);
    private final Map<Target, Totals> totals = new EnumMap<>(Target.class);
    private final Map<Long, ThreadState> threads = new HashMap<>();
    private final Set<Long> producers = new HashSet<>();
    private final Set<Long> publishers = new HashSet<>();
    private final Set<Long> publisherFolders = new HashSet<>();
    private final Map<Long, CapturedJob> jobs = new LinkedHashMap<>();
    private final Map<Long, Admission> admissions = new LinkedHashMap<>();
    private final Map<Long, ClaimedSubmission> claimedSubmissions = new LinkedHashMap<>();
    private final Map<Long, ClaimedReplacement> claimedReplacements = new LinkedHashMap<>();
    private final List<Map<String, Object>> records = new ArrayList<>();
    private final List<AuthorityReceipt> authorities = new ArrayList<>();
    private final Set<String> unverified = new LinkedHashSet<>(), layouts = new LinkedHashSet<>();
    private final Set<String> validatedTypes = new HashSet<>();
    private final AtomicReference<AssertionError> failure = new AtomicReference<>();
    private volatile CompletableFuture<Boundary> command;
    private volatile String requestedPhase;
    private volatile BindingProfile requestedBindingProfile = BindingProfile.FULL;
    private boolean restoredBeforeStartFinished;
    private volatile boolean running = true;
    private boolean closing, closed, vmDeath, disconnected;
    private long loaderId = -1, events, sequence, calls, handlingNanos;
    private long phaseBytes;
    private Boundary terminal;
    private UnqualifiedEntry lastUnqualifiedEntry;
    private boolean logFamilyCalibrationStarted;
    private Map<String, Object> logFamilyCalibration = Map.of();
    private boolean ownedCrashRequested, localDisposeStarted, crashDisconnectBeforeDispose, crashCleanupForcedKill;
    private long crashOwnedPid, crashMarkedAtEvent;
    private String crashDisconnectReceipt, crashReadInterruption, activeObservation = "EVENT_QUEUE";
    private OwnedCrash crashTerminal;

    private NativeTelemetryIdentityJdiSession(Path jar, String sha, String pipeline,
            Map<String, Image> images, RealProcessServer server, VirtualMachine vm, boolean replacementObservationEnabled,
            boolean rawContinuationObservationEnabled, boolean joinedTakeoverObservationEnabled) throws Exception {
        this.jar = jar; this.sha = sha; this.pipeline = pipeline; this.images = images;
        this.server = server; this.vm = vm; this.vmVersion = vm.version();
        this.replacementObservationEnabled = replacementObservationEnabled;
        this.rawContinuationObservationEnabled = rawContinuationObservationEnabled;
        this.joinedTakeoverObservationEnabled = joinedTakeoverObservationEnabled;
        for (Spec spec : SPECS) {
            if (spec.target() == Target.REPLACEMENT_ADMISSION && !replacementObservationEnabled
                    || spec.target() == Target.RAW_CONTINUATION && !rawContinuationObservationEnabled
                    || spec.target() == Target.JOINED_TAKEOVER && !joinedTakeoverObservationEnabled) { continue; }
            totals.put(spec.target(), new Totals());
            Image image = images.get(spec.type());
            if (image == null || !image.methods().containsKey(spec.method() + spec.descriptor())) {
                unknown("ARTIFACT_METHOD_MISSING:" + spec.target()); continue;
            }
            specs.put(spec.target(), spec);
        }
        for (String type : specs.values().stream().map(Spec::type).distinct().toList()) {
            ClassPrepareRequest request = vm.eventRequestManager().createClassPrepareRequest();
            request.addClassFilter(type); request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD); request.enable();
        }
        pump = new Thread(this::loop, "native-telemetry-identity-events");
        pump.setDaemon(true); pump.start();
    }

    static NativeTelemetryIdentityJdiSession start(String storeUri, String operatorDatabase, Path input,
            String expectedSha256, String pipelineId, List<String> applicationArguments) throws Exception {
        return start(input, expectedSha256, pipelineId, (jar, jvmArguments) ->
                RealProcessServer.launchingWithJvmArguments(storeUri, operatorDatabase, jar,
                        jvmArguments, List.copyOf(applicationArguments)));
    }

    /** A cluster caller can supply its existing routable owned launch without changing debug ownership. */
    static NativeTelemetryIdentityJdiSession start(Path input, String expectedSha256, String pipelineId,
            OwnedLauncher launcher) throws Exception {
        return start(input, expectedSha256, pipelineId, launcher, false, false);
    }

    /** A single owned debugger also observes the actual atomic replacement admission and native binding. */
    static NativeTelemetryIdentityJdiSession startWithReplacementObservation(Path input, String expectedSha256,
            String pipelineId, OwnedLauncher launcher) throws Exception {
        return start(input, expectedSha256, pipelineId, launcher, true, false);
    }

    /** The same owned debugger can retain raw continuation entries and their actual publication returns. */
    static NativeTelemetryIdentityJdiSession startWithContinuationObservation(Path input, String expectedSha256,
            String pipelineId, OwnedLauncher launcher) throws Exception {
        return start(input, expectedSha256, pipelineId, launcher, true, true);
    }

    /** Adds only the cold, caller-qualified joined takeover receipt to the same bounded debugger. */
    static NativeTelemetryIdentityJdiSession startWithJoinedTakeoverObservation(Path input, String expectedSha256,
            String pipelineId, OwnedLauncher launcher) throws Exception {
        return start(input, expectedSha256, pipelineId, launcher, false, false, true);
    }

    private static NativeTelemetryIdentityJdiSession start(Path input, String expectedSha256, String pipelineId,
            OwnedLauncher launcher, boolean replacementObservationEnabled, boolean rawContinuationObservationEnabled) throws Exception {
        return start(input, expectedSha256, pipelineId, launcher, replacementObservationEnabled,
                rawContinuationObservationEnabled, false);
    }

    private static NativeTelemetryIdentityJdiSession start(Path input, String expectedSha256, String pipelineId,
            OwnedLauncher launcher, boolean replacementObservationEnabled, boolean rawContinuationObservationEnabled,
            boolean joinedTakeoverObservationEnabled) throws Exception {
        Objects.requireNonNull(expectedSha256); Objects.requireNonNull(pipelineId);
        Objects.requireNonNull(launcher);
        if (!expectedSha256.matches("[0-9a-f]{64}") || pipelineId.isBlank() || pipelineId.length() > 256) {
            throw invalid("invalid immutable artifact or pipeline input");
        }
        Path jar = input.toRealPath();
        if (!Files.isRegularFile(jar) || Files.size(jar) > 512L * 1024 * 1024
                || !expectedSha256.equals(hash(jar))) { throw invalid("immutable input hash mismatch"); }
        Map<String, Image> images = images(jar, joinedTakeoverObservationEnabled);
        if (!expectedSha256.equals(hash(jar))) { throw invalid("artifact changed during metadata read"); }
        ListeningConnector connector = Bootstrap.virtualMachineManager().listeningConnectors().stream()
                .filter(value -> value.name().equals("com.sun.jdi.SocketListen")).findFirst()
                .orElseThrow(() -> invalid("loopback debug listener unavailable"));
        Map<String, Connector.Argument> options = connector.defaultArguments();
        options.get("localAddress").setValue("127.0.0.1"); options.get("port").setValue("0");
        options.get("timeout").setValue("15000");
        String listeningAddress = connector.startListening(options);
        boolean listening = true;
        RealProcessServer server = null;
        VirtualMachine vm = null;
        NativeTelemetryIdentityJdiSession session = null;
        Throwable primary = null;
        try {
            String address = BenchmarkJdiCostObserver.numericLoopbackDialAddress(
                    options.get("localAddress").value(), listeningAddress, options.get("port").value());
            server = launcher.launch(jar,
                    List.of("-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=" + address));
            vm = connector.accept(options);
            if (!vm.canGetBytecodes() || !vm.canGetMethodReturnValues()) { throw invalid("required mirror capability missing"); }
            connector.stopListening(options); listening = false;
            session = new NativeTelemetryIdentityJdiSession(jar, expectedSha256, pipelineId, images, server, vm,
                    replacementObservationEnabled, rawContinuationObservationEnabled, joinedTakeoverObservationEnabled);
            server.awaitHealthy(); session.check();
            return session;
        } catch (Throwable problem) {
            primary = problem;
            if (session != null) {
                try { session.close(); } catch (Throwable cleanup) { problem.addSuppressed(cleanup); }
            } else {
                boolean detached = vm == null;
                if (vm != null) {
                    try { vm.dispose(); detached = true; }
                    catch (VMDisconnectedException gone) { detached = true; }
                    catch (Throwable cleanup) { problem.addSuppressed(cleanup); }
                }
                if (server != null) { if (detached) { server.close(); } else { server.kill(); } }
            }
            throw problem;
        } finally {
            if (listening) {
                try { connector.stopListening(options); }
                catch (Throwable cleanup) {
                    if (session != null) { try { session.close(); } catch (Throwable ignored) { cleanup.addSuppressed(ignored); } }
                    else if (server != null) { server.kill(); }
                    if (primary != null) { primary.addSuppressed(cleanup); } else { throw cleanup; }
                }
            }
        }
    }

    /** One existing sweep ENTRY can hold its event thread while another member normally acquires Q. */
    final class PublisherSweepPark implements AutoCloseable {
        private final String keptPipeline, node, boot;
        private final long deadline;
        private final boolean holding;
        private final CompletableFuture<Map<String, Object>> entered = new CompletableFuture<>();
        private EventSet held;
        private Map<String, Object> receipt;
        private String releaseReason;
        private long parkedAtNanos, releasedAtNanos;
        private boolean done, releasing;

        private PublisherSweepPark(String keptPipeline, String node, String boot, long deadline, boolean holding) {
            this.keptPipeline = keptPipeline; this.node = node; this.boot = boot; this.deadline = deadline;
            this.holding = holding;
        }

        Map<String, Object> awaitEntry() throws Exception {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) { throw afterWaitFailure("UNSELECTED: sweep park deadline expired before ENTRY", null); }
            try { return entered.get(remaining, TimeUnit.NANOSECONDS); }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw afterWaitFailure("UNSELECTED: sweep ENTRY wait interrupted", interrupted);
            } catch (Exception failed) {
                throw afterWaitFailure("UNSELECTED: sweep ENTRY was not observed within the original bound", failed);
            }
        }

        private AssertionError afterWaitFailure(String message, Throwable cause) {
            AssertionError primary = new AssertionError(message, cause);
            try { close(); }
            catch (Throwable cleanup) { append(primary, cleanup); }
            return primary;
        }

        boolean held() {
            synchronized (lock) { return held != null && !done && !releasing && System.nanoTime() - deadline < 0; }
        }

        Map<String, Object> evidence() {
            synchronized (lock) {
                Map<String, Object> out = new LinkedHashMap<>();
                if (receipt != null) { out.putAll(receipt); }
                out.put("releaseReason", releaseReason == null ? "NOT_RELEASED" : releaseReason);
                out.put("done", done); out.put("parkedAtNanos", parkedAtNanos); out.put("releasedAtNanos", releasedAtNanos);
                out.put("deadlineNanos", deadline); return Map.copyOf(out);
            }
        }

        @Override public void close() { releasePublisherSweepPark(this, "CALLER_RELEASE"); }
    }

    private volatile PublisherSweepPark sweepPark;
    private boolean sweepParkUsed;

    PublisherSweepPark armPublisherSweepPark(String keptPipeline, String node, String boot, long deadline) {
        return armPublisherSweep(keptPipeline, node, boot, deadline, true);
    }

    /** A readiness receipt uses the same existing ENTRY and resumes before its future is answered. */
    PublisherSweepPark observePublisherSweep(String keptPipeline, String node, String boot, long deadline) {
        return armPublisherSweep(keptPipeline, node, boot, deadline, false);
    }

    private PublisherSweepPark armPublisherSweep(String keptPipeline, String node, String boot,
            long deadline, boolean holding) {
        synchronized (lock) {
            check();
            if (!joinedTakeoverObservationEnabled || sweepParkUsed || closing
                    || keptPipeline == null || keptPipeline.isBlank() || node == null || node.isBlank()
                    || boot == null || boot.isBlank() || deadline - System.nanoTime() <= 0) {
                throw invalid("UNSELECTED: invalid one-shot sweep parking request");
            }
            sweepParkUsed = true; sweepPark = new PublisherSweepPark(keptPipeline, node, boot, deadline, holding);
            return sweepPark;
        }
    }

    private void releasePublisherSweepPark(PublisherSweepPark expected, String reason) {
        EventSet eventSet;
        synchronized (lock) {
            if (expected == null || expected != sweepPark || expected.done || expected.releasing) { return; }
            expected.releasing = true; eventSet = expected.held;
        }
        boolean resumed = false;
        try {
            if (eventSet != null) {
                try { eventSet.resume(); } catch (VMDisconnectedException gone) { }
            }
            resumed = true;
        } finally {
            synchronized (lock) {
                expected.releasing = false;
                if (resumed) {
                    expected.done = true; expected.releaseReason = reason; expected.releasedAtNanos = System.nanoTime();
                    expected.held = null;
                    if (!expected.entered.isDone()) {
                        expected.entered.completeExceptionally(invalid("UNSELECTED: sweep was not observed: " + reason));
                    }
                }
            }
        }
    }

    private long publisherSweepPollMillis() {
        synchronized (lock) {
            if (sweepPark == null || sweepPark.done) { return 25; }
            long remaining = sweepPark.deadline - System.nanoTime();
            if (remaining <= 0) { return 1; }
            return Math.min(25, Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
        }
    }

    private void releaseExpiredPublisherSweepPark() {
        PublisherSweepPark current;
        synchronized (lock) {
            current = sweepPark;
            if (current == null || current.done || System.nanoTime() - current.deadline < 0) { return; }
        }
        releasePublisherSweepPark(current, "DEADLINE_RELEASE");
    }

    private void completeObservedPublisherSweep(PublisherSweepPark expected) {
        synchronized (lock) {
            if (expected != sweepPark || expected.holding || expected.done || expected.releasing) { return; }
            expected.done = true; expected.releaseReason = "OBSERVATION_RELEASE";
            expected.releasedAtNanos = System.nanoTime();
            if (expected.releasedAtNanos - expected.deadline <= 0) { expected.entered.complete(expected.receipt); }
            else { expected.entered.completeExceptionally(invalid("UNSELECTED: sweep observation exceeded its original deadline")); }
        }
    }

    /** The matcher is independent from Q's account/receiver qualification and cannot count as Q evidence. */
    private PublisherSweepPark parkPublisherSweepEntry(BreakpointEvent event, EventSet eventSet) throws Exception {
        PublisherSweepPark current = sweepPark;
        if (current == null || current.done || current.releasing || current.held != null || System.nanoTime() - current.deadline >= 0
                || !(event.request().getProperty("native-identity-site") instanceof Site site)
                || site.target() != Target.PUBLISHER_SWEEP || !site.entry()) { return null; }
        if (eventSet.suspendPolicy() != EventRequest.SUSPEND_EVENT_THREAD) {
            throw invalid("UNSELECTED: sweep parking requires only its event thread");
        }
        List<StackFrame> frames = frames(event.thread());
        StackFrame top = frames.getFirst(), caller = null;
        for (StackFrame frame : frames) {
            Method method = frame.location().method();
            if (method.declaringType().name().equals("io.tapstate.app.ConvergenceDriver")
                    && method.name().equals("reconcile") && method.signature().equals("()V")) {
                if (caller != null) { throw invalid("UNSELECTED: ambiguous convergence sweep caller"); }
                caller = frame;
            }
        }
        if (caller == null) { return null; }
        NativeTelemetryMirror mirror = mirror();
        List<Object> kept = mirror.scalars(top.getArgumentValues().getFirst());
        if (!kept.equals(List.of(current.keptPipeline))) { return null; }
        Method method = caller.location().method(); validate(method.declaringType());
        byte[] expected = images.get("io.tapstate.app.ConvergenceDriver").methods().get("reconcile()V");
        if (expected == null || method.isObsolete() || !Arrays.equals(expected, method.bytecodes())) {
            throw invalid("UNSELECTED: sweep caller code differs from selected artifact");
        }
        ObjectReference driver = caller.thisObject(), publisher = top.thisObject();
        if (driver == null || publisher == null || mirror.object(mirror.field(driver, "publisher",
                "Lio/tapstate/runtime/scheduler/ObservationPublisher;")).uniqueID() != publisher.uniqueID()) {
            throw invalid("UNSELECTED: sweep receiver is not this convergence publisher");
        }
        ObjectReference ownership = requiredObject(mirror.field(driver, "actuation", "Lio/tapstate/app/PipelineActuationOwnership;"),
                "io.tapstate.app.PipelineActuationOwnership", mirror);
        Map<String, Object> owner = workloadOwner(mirror.field(ownership, "owner", "Lio/tapstate/spi/store/WorkloadOwner;"), mirror);
        if (!current.node.equals(owner.get("nodeId")) || !current.boot.equals(owner.get("bootId"))) {
            throw invalid("UNSELECTED: parked VM has a different actual member owner");
        }
        if (!vm.canGetOwnedMonitorInfo() || !event.thread().ownedMonitors().isEmpty()) {
            throw invalid("UNSELECTED: sweep ENTRY has unavailable or held monitor evidence");
        }
        Map<String, Object> receipt = new LinkedHashMap<>();
        receipt.put("target", current.holding ? "P_ONLY_PUBLISHER_SWEEP_PARK" : "P_ONLY_PUBLISHER_SWEEP_OBSERVED");
        receipt.put("keptIds", kept);
        receipt.put("owner", owner); receipt.put("publisherReceiver", publisher.uniqueID());
        receipt.put("driverReceiver", driver.uniqueID()); receipt.put("threadId", event.thread().uniqueID());
        receipt.put("threadName", event.thread().name()); receipt.put("entryOrder", events);
        receipt.put("callerCodeSha256", hash(expected)); receipt.put("callerLoaderId", loaderId);
        receipt.put("ownedMonitors", List.of()); receipt.put("suspendPolicy", "EVENT_THREAD");
        current.receipt = Map.copyOf(receipt); current.parkedAtNanos = System.nanoTime();
        if (current.holding) { current.held = eventSet; current.entered.complete(current.receipt); }
        return current;
    }

    RealProcessServer server() { return server; }
    void check() { AssertionError problem = failure.get(); if (problem != null) { throw problem; } }

    /** The future integration test supplies receipts read from its actual authoritative Mongo documents. */
    void recordAuthority(AuthorityReceipt receipt) {
        synchronized (lock) {
            check();
            if (!receipt.pipelineId().equals(pipeline)) { throw invalid("authority receipt names another pipeline"); }
            for (AuthorityReceipt previous : authorities) {
                if (previous.scope().equals(receipt.scope())) {
                    if (!previous.clusterId().equals(receipt.clusterId())
                            || !previous.coordinationId().equals(receipt.coordinationId())) {
                        throw invalid("one observed scope has conflicting authority receipts");
                    }
                    return;
                }
            }
            if (authorities.size() >= 8) { throw invalid("authority receipt budget exceeded"); }
            authorities.add(receipt);
        }
    }

    /** Returns a real observed proxy; this class exposes no method that cancels or mutates it. */
    Optional<CapturedJob> capturedJob(AuthorityReceipt receipt) {
        synchronized (lock) {
            check();
            if (!authorities.contains(receipt)) { throw invalid("job requested without a registered authority receipt"); }
            return jobs.values().stream().filter(job -> job.scope().equals(receipt.scope())
                    && receipt.clusterId().equals(job.job().get("clusterId"))).reduce((first, second) -> {
                if (first.proxyId() != second.proxyId()) { throw invalid("multiple physical jobs match one receipt"); }
                return first;
            });
        }
    }

    /** A claimed result requires both the actual admission return and the same submit object's Job. */
    Optional<ClaimedSubmission> claimedSubmission(AuthorityReceipt receipt) {
        synchronized (lock) {
            check();
            if (!authorities.contains(receipt)) { throw invalid("submission requested without an authority receipt"); }
            return claimedSubmissions.values().stream().filter(submission ->
                    submission.job().scope().equals(receipt.scope())
                            && receipt.clusterId().equals(submission.job().job().get("clusterId")))
                    .reduce((first, second) -> {
                        if (!first.equals(second)) { throw invalid("multiple claimed submissions match one authority scope"); }
                        return first;
                    });
        }
    }

    Optional<ClaimedReplacement> claimedReplacement(AuthorityReceipt receipt) {
        synchronized (lock) {
            check();
            if (!replacementObservationEnabled || !authorities.contains(receipt)) {
                throw invalid("replacement requested without its observation mode and authority receipt");
            }
            return claimedReplacements.values().stream().filter(replacement ->
                    replacement.submission().job().scope().equals(receipt.scope())
                            && receipt.clusterId().equals(replacement.submission().job().job().get("clusterId")))
                    .reduce((first, second) -> {
                        if (!first.equals(second)) { throw invalid("multiple replacements match one authority scope"); }
                        return first;
                    });
        }
    }

    /** An event-pump barrier, never a VM-wide suspension; open calls are explicit carry-in/carry-out. */
    Boundary boundary(String phase) throws Exception {
        return boundary(phase, BindingProfile.FULL);
    }

    /** This profile cannot certify submission; every other enabled target remains required. */
    Boundary boundary(String phase, BindingProfile profile) throws Exception {
        Objects.requireNonNull(profile);
        synchronized (commands) {
            check();
            if (phase == null || phase.isBlank() || phase.length() > 128) { throw invalid("invalid phase"); }
            if (!sha.equals(hash(jar))) { throw invalid("immutable artifact changed"); }
            synchronized (lock) {
                if (closing || closed || disconnected || ownedCrashRequested) { throw invalid("boundary requested after close"); }
                if (profile == BindingProfile.RESTORED_BEFORE_FIRST_START && restoredBeforeStartFinished) {
                    throw invalid("restored binding profile requested after the first START boundary closed");
                }
                requestedPhase = phase; requestedBindingProfile = profile; command = new CompletableFuture<>();
            }
            CompletableFuture<Boundary> waiting = command;
            try { return waiting.get(30, TimeUnit.SECONDS); }
            catch (Exception | Error problem) { fail(problem); throw problem; }
            finally { command = null; }
        }
    }

    /** Arm the full requirement before the caller issues its first real START; this performs no target work. */
    void requireFullBindingsBeforeStart() {
        synchronized (commands) {
            synchronized (lock) {
                check();
                if (closing || closed || disconnected || ownedCrashRequested) { throw invalid("binding requirement changed after close"); }
                restoredBeforeStartFinished = true;
            }
        }
    }

    private void loop() {
        try {
            while (running) {
                releaseExpiredPublisherSweepPark();
                activeObservation = "EVENT_QUEUE";
                EventSet set = vm.eventQueue().remove(publisherSweepPollMillis());
                if (set != null) { handle(set); }
                CompletableFuture<Boundary> request = command;
                if (request != null) {
                    int drain = 0;
                    EventSet next;
                    while ((next = vm.eventQueue().remove(1)) != null) {
                        releaseExpiredPublisherSweepPark();
                        if (++drain > 2048) { throw invalid("phase event-drain budget exceeded"); }
                        handle(next);
                    }
                    synchronized (lock) {
                        check(); request.complete(snapshot(requestedPhase, true, requestedBindingProfile));
                        records.clear(); phaseBytes = 0; command = null;
                    }
                }
            }
        } catch (VMDisconnectedException gone) {
            if (!observeOwnedCrashDisconnect("JDI_EXCEPTION:" + activeObservation)) { fail(gone); }
        } catch (Throwable problem) { fail(problem); }
        finally {
            try { releasePublisherSweepPark(sweepPark, "PUMP_EXIT"); }
            catch (Throwable cleanup) { fail(cleanup); }
        }
    }

    private void handle(EventSet set) throws Exception {
        long started = System.nanoTime();
        boolean parked = false, processed = false;
        PublisherSweepPark observedSweep = null;
        try {
            synchronized (lock) {
                for (Event event : set) {
                    if (++events > MAX_EVENTS) { throw invalid("event budget exceeded"); }
                    activeObservation = event instanceof BreakpointEvent breakpoint
                            && breakpoint.request().getProperty("native-identity-site") instanceof Site site
                            ? (site.entry() ? "ENTRY:" : "RETURN_SITE:") + site.target()
                            : event instanceof MethodExitEvent exit
                            ? "NORMAL_EXIT:call=" + exit.request().getProperty("native-identity-call")
                            : event.getClass().getSimpleName();
                    if (event instanceof ClassPrepareEvent prepare) { bind(prepare.referenceType()); }
                    else if (event instanceof BreakpointEvent breakpoint) {
                        try {
                            PublisherSweepPark selected = parkPublisherSweepEntry(breakpoint, set);
                            if (selected != null) { observedSweep = selected; parked = selected.holding; }
                        }
                        catch (NativeTelemetryMirror.Unavailable unavailable) {
                            throw new AssertionError("UNSELECTED: sweep parking layout is unavailable: " + unavailable.getMessage(), unavailable);
                        }
                        breakpoint(breakpoint);
                    }
                    else if (event instanceof MethodExitEvent exit) { normalExit(exit); }
                    else if (event instanceof ExceptionEvent exception) { exception(exception); }
                    else if (event instanceof ThreadDeathEvent death) { threadDeath(death); }
                    else if (event instanceof VMDeathEvent) {
                        if (!ownedCrashRequested && (!closing || !threads.isEmpty())) {
                            throw invalid("VM death lost open calls or owned close");
                        }
                        vmDeath = true;
                    } else if (event instanceof VMDisconnectEvent) {
                        if (!observeOwnedCrashDisconnect("VMDisconnectEvent")) {
                            if (!closing || !vmDeath) { throw invalid("disconnect lacked owned VM death"); }
                            disconnected = true; running = false;
                        }
                    } else if (!(event instanceof VMStartEvent)) { throw invalid("unmapped capture event"); }
                }
            }
            processed = true;
        } finally {
            handlingNanos += System.nanoTime() - started;
            if (!parked) {
                try {
                    set.resume();
                    if (observedSweep != null && processed) { completeObservedPublisherSweep(observedSweep); }
                }
                catch (VMDisconnectedException gone) {
                    if (!observeOwnedCrashDisconnect("EVENT_SET_RESUME:" + activeObservation)
                            && (!closing || !vmDeath)) { throw gone; }
                }
            }
        }
    }

    private boolean observeOwnedCrashDisconnect(String receipt) {
        synchronized (lock) {
            if (!ownedCrashRequested) { return false; }
            if (!localDisposeStarted && !crashDisconnectBeforeDispose) {
                crashDisconnectBeforeDispose = true; crashDisconnectReceipt = receipt;
            }
            if (receipt.startsWith("JDI_EXCEPTION:") && !receipt.equals("JDI_EXCEPTION:EVENT_QUEUE")) {
                crashReadInterruption = receipt;
            }
            disconnected = true; running = false;
            CompletableFuture<Boundary> waiting = command;
            if (waiting != null) { waiting.completeExceptionally(invalid("owned crash interrupted boundary")); }
            return true;
        }
    }

    private void bind(ReferenceType type) throws Exception {
        ClassLoaderReference loader = type.classLoader();
        if (loader == null || !LOADER.equals(loader.referenceType().name())
                || loaderId != -1 && loaderId != loader.uniqueID()) { throw invalid("unexpected application loader"); }
        loaderId = loader.uniqueID();
        validate(type);
        for (Spec spec : specs.values()) {
            if (!type.name().equals(spec.type())) { continue; }
            if (bindings.containsKey(spec.target())) { throw invalid("duplicate live binding"); }
            List<Method> matches = type.methodsByName(spec.method(), spec.descriptor());
            if (matches.size() != 1) { throw invalid("missing or ambiguous exact live method"); }
            Method method = matches.getFirst();
            byte[] code = images.get(spec.type()).methods().get(spec.method() + spec.descriptor());
            if (method.isStatic() || method.isAbstract() || method.isNative() || method.isBridge()
                    || method.isObsolete() || !Arrays.equals(code, method.bytecodes())) { throw invalid("method differs from input artifact"); }
            List<Integer> returns = BenchmarkJdiCostObserver.returnOffsets(code);
            if (returns.isEmpty()) { throw invalid("method has no verified normal return"); }
            install(method, 0, new Site(spec.target(), true));
            for (int offset : returns) { install(method, offset, new Site(spec.target(), false)); }
            methods.put(spec.target(), method);
            bindings.put(spec.target(), new Binding(type.name(), method.name(), method.signature(),
                    images.get(spec.type()).origin(), hash(code), loaderId, returns));
        }
    }

    private void install(Method method, long offset, Site site) {
        Location location = method.locationOfCodeIndex(offset);
        if (location == null || location.codeIndex() != offset) { throw invalid("exact instruction unavailable"); }
        BreakpointRequest request = vm.eventRequestManager().createBreakpointRequest(location);
        request.putProperty("native-identity-site", site);
        request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD); request.enable();
    }

    private NativeTelemetryMirror mirror() { return new NativeTelemetryMirror(this::validate, layouts); }
    private void validate(ReferenceType type) throws Exception {
        if (type.name().startsWith("java.")) { return; }
        String identity = type.name() + "@" + (type.classLoader() == null ? -1 : type.classLoader().uniqueID());
        if (validatedTypes.contains(identity)) { return; }
        Image image = images.get(type.name());
        if (image == null) { throw new NativeTelemetryMirror.Unavailable("ARTIFACT_CLASS_MISSING:" + type.name()); }
        if (type.classLoader() == null || type.classLoader().uniqueID() != loaderId) { throw invalid("decoded object loader differs"); }
        for (var expected : image.fields().entrySet()) {
            Field field = type.fieldByName(expected.getKey());
            if (field == null || !field.signature().equals(expected.getValue())) { throw invalid("live field layout differs from artifact"); }
        }
        validatedTypes.add(identity);
        if (validatedTypes.size() > MAX_CLASSES) { throw invalid("decoded class budget exceeded"); }
    }

    private void breakpoint(BreakpointEvent event) throws Exception {
        if (!(event.request().getProperty("native-identity-site") instanceof Site site)
                || !event.location().method().equals(methods.get(site.target()))) { throw invalid("lost breakpoint binding"); }
        List<StackFrame> frames = frames(event.thread());
        StackFrame frame = frames.getFirst();
        List<Value> arguments = frame.getArgumentValues();
        Spec spec = specs.get(site.target());
        if (arguments.size() != spec.arguments()) { throw invalid("actual arguments unavailable"); }
        ObjectReference receiver = frame.thisObject();
        if (receiver == null) { throw invalid("receiver unavailable"); }
        NativeTelemetryMirror mirror = mirror();
        boolean qualified = site.target() == Target.JOINED_TAKEOVER
                ? joinedCaller(frames) != null && pipeline.equals(mirror.text(mirror.field(
                        requiredObject(arguments.getFirst(), "io.tapstate.runtime.srs.CaptureRunSpec", mirror),
                        "pipelineId", "Ljava/lang/String;")))
                : site.target() == Target.SUBMIT
                ? pipeline.equals(mirror.text(mirror.field(receiver, "val$pipelineId", "Ljava/lang/String;")))
                        && (replacementObservationEnabled
                                || mirror.field(receiver, "val$accepted", "Lio/tapstate/spi/store/SuccessorAdmission;") == null)
                : site.target() == Target.REPLACEMENT_ADMISSION
                        ? pipeline.equals(replacementPipeline(arguments.getFirst(), mirror))
                : site.target() == Target.RAW_CONTINUATION
                        ? pipeline.equals(rawPipeline(arguments.getFirst(), mirror))
                : site.target() == Target.PUBLISHER_SWEEP ? publishers.contains(receiver.uniqueID())
                : site.target() == Target.FOLDER_SWEEP ? publisherFolders.contains(receiver.uniqueID())
                : spec.pipeline() < 0 ? producers.contains(receiver.uniqueID())
                : pipeline.equals(mirror.text(arguments.get(spec.pipeline())));
        if (site.target() == Target.FOLDER_FORGET) { qualified &= publisherFolders.contains(receiver.uniqueID()); }
        ThreadState state = threads.get(event.thread().uniqueID());
        if (state != null) { reconcile(state, frames, site.entry() ? site.target() : null); }
        if (site.entry()) {
            if (state == null) { state = threadState(event.thread()); }
            if (state.calls.size() >= MAX_CALLS || threads.values().stream()
                    .mapToInt(value -> value.calls.size()).sum() >= MAX_OPEN_CALLS
                    || !state.calls.isEmpty() && state.calls.peek().depth >= frames.size()) {
                throw invalid("observed entry overlapped an unreturned call or exceeded its bound");
            }
            Map<String, Object> entry = new LinkedHashMap<>();
            boolean logCalibration = false;
            if (qualified) {
                try {
                    if (spec.target() == Target.JOINED_TAKEOVER) {
                        entry.putAll(joinedTakeover(frames, arguments, mirror));
                    } else { entry(spec, receiver, arguments, mirror, entry); }
                    if (spec.target() == Target.LOG) { entry.put("callers", logCallers(frames)); }
                    if (evidenceBytes(entry, 0) > MAX_RECORD_BYTES) {
                        entry.clear();
                        throw new NativeTelemetryMirror.Unavailable("ENTRY_RECORD_BYTE_BUDGET");
                    }
                }
                catch (NativeTelemetryMirror.Unavailable missing) {
                    decoderUnavailable(entry, spec.target(), "ENTRY", missing.getMessage());
                }
            } else {
                lastUnqualifiedEntry = new UnqualifiedEntry(site.target(), event.thread().uniqueID(),
                        receiver.uniqueID(), frames.size(), events);
                if (joinedTakeoverObservationEnabled && site.target() == Target.LOG
                        && !logFamilyCalibrationStarted && arguments.get(1) != null) {
                    logFamilyCalibrationStarted = true; logCalibration = true;
                    try {
                        entry(spec, receiver, arguments, mirror, entry);
                        entry.put("calibrationScope", entry.remove("scope"));
                        entry.put("foreignPipelineId", mirror.text(arguments.getFirst()));
                        entry.put("queryPipelineId", pipeline);
                        Binding binding = bindings.get(Target.LOG);
                        entry.put("bindingCodeSha256", binding.codeSha256());
                        entry.put("bindingLoaderId", binding.loaderId());
                        entry.put("ownedPid", server.pid());
                        entry.put("interpretation", "COMMON_LOG_DECODER_AND_BINDING_ONLY");
                        if (evidenceBytes(entry, 0) > MAX_RECORD_BYTES) {
                            entry.clear(); throw new NativeTelemetryMirror.Unavailable("CALIBRATION_RECORD_BYTE_BUDGET");
                        }
                    } catch (NativeTelemetryMirror.Unavailable missing) {
                        entry.remove("scope");
                        decoderUnavailable(entry, Target.LOG, "FAMILY_CALIBRATION", missing.getMessage());
                    }
                }
            }
            entry.put("entryOrder", events);
            Call call = new Call(++calls, spec, frames.size(), receiver,
                    Collections.unmodifiableList(new ArrayList<>(arguments)),
                    Collections.unmodifiableMap(new LinkedHashMap<>(entry)), qualified);
            call.logCalibration = logCalibration;
            state.calls.push(call);
            if (qualified) { totals.get(site.target()).entries++; }
        } else {
            Call call = state == null ? null : state.calls.peek();
            if (call == null || call.spec.target() != site.target() || call.depth != frames.size()
                    || call.receiver.uniqueID() != receiver.uniqueID() || call.escaping || call.exit != null) {
                throw returnCorrelationFailure(event, site, receiver, frames.size(), qualified, call);
            }
            // Registration can change during this call. Its entry decision still owns its return.
            if (!call.qualified && !call.logCalibration) {
                state.calls.pop(); removeEmpty(state);
                return;
            }
            MethodExitRequest exit = vm.eventRequestManager().createMethodExitRequest();
            exit.addThreadFilter(event.thread()); exit.addClassFilter(spec.type()); exit.addCountFilter(1);
            exit.putProperty("native-identity-call", call.id);
            exit.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD); call.exit = exit; exit.enable();
        }
    }

    /**
     * Complete caller rows: type, method, descriptor, code index, loader id, loader type, origin,
     * provenance and code SHA. String positions index a record-local table; -1 preserves absence.
     * The second array retains every frame in consecutive groups of nine columns.
     */
    private List<Object> logCallers(List<StackFrame> frames) throws Exception {
        Map<String, Integer> strings = new LinkedHashMap<>();
        List<Object> callers = new ArrayList<>();
        for (StackFrame frame : frames) {
            Method method = frame.location().method();
            ReferenceType type = method.declaringType();
            String name = type.name();
            ClassLoaderReference loader = type.classLoader();
            Image image = images.get(name);
            String origin = image == null ? null : image.origin();
            String provenance = "UNVERIFIED_CALLER_METHOD";
            String codeSha256 = null;
            byte[] expected = image == null ? null : image.methods().get(method.name() + method.signature());
            if (loader != null && loader.uniqueID() == loaderId && LOADER.equals(loader.referenceType().name())
                    && !method.isNative() && !method.isAbstract() && !method.isObsolete() && expected != null) {
                validate(type);
                byte[] actual = method.bytecodes();
                if (!Arrays.equals(expected, actual)) { throw invalid("log caller differs from selected input artifact"); }
                provenance = "EXACT_ARTIFACT_METHOD"; codeSha256 = hash(actual);
            }
            callers.addAll(List.of(callerString(strings, name), callerString(strings, method.name()),
                    callerString(strings, method.signature()),
                    frame.location().codeIndex(), loader == null ? -1L : loader.uniqueID(),
                    callerString(strings, loader == null ? "BOOTSTRAP" : loader.referenceType().name()),
                    callerString(strings, origin), callerString(strings, provenance), callerString(strings, codeSha256)));
        }
        return List.of(List.copyOf(strings.keySet()), List.copyOf(callers));
    }

    private static int callerString(Map<String, Integer> strings, String value) {
        return value == null ? -1 : strings.computeIfAbsent(value, ignored -> strings.size());
    }

    private AssertionError returnCorrelationFailure(BreakpointEvent event, Site site,
            ObjectReference receiver, int depth, boolean qualified, Call call) {
        String expected = call == null ? "NONE" : "invocation=" + call.id + ", target=" + call.spec.target()
                + ", receiver=" + call.receiver.uniqueID() + ", depth=" + call.depth
                + ", entryOrder=" + call.entry.get("entryOrder") + ", escaping=" + call.escaping
                + ", qualifiedAtEntry=" + call.qualified + ", exitArmed=" + (call.exit != null);
        UnqualifiedEntry ignored = lastUnqualifiedEntry;
        boolean sameIgnoredEntry = ignored != null && ignored.target() == site.target()
                && ignored.thread() == event.thread().uniqueID() && ignored.receiver() == receiver.uniqueID()
                && ignored.depth() == depth;
        return invalid("normal return site lost exact entry correlation: target=" + site.target()
                + ", method=" + event.location().method().name() + event.location().method().signature()
                + ", codeIndex=" + event.location().codeIndex() + ", thread=" + event.thread().uniqueID()
                + ", receiver=" + receiver.uniqueID() + ", depth=" + depth + ", eventOrder=" + events
                + ", qualifiedNow=" + qualified + ", topCall={" + expected + "}"
                + ", lastUnqualifiedEntry=" + ignored + ", sameUnqualifiedEntry=" + sameIgnoredEntry);
    }

    private ThreadState threadState(ThreadReference thread) {
        if (threads.size() >= MAX_THREADS) { throw invalid("observed thread budget exceeded"); }
        ExceptionRequest exceptions = vm.eventRequestManager().createExceptionRequest(null, true, true);
        exceptions.addThreadFilter(thread); exceptions.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
        ThreadDeathRequest death = vm.eventRequestManager().createThreadDeathRequest();
        death.addThreadFilter(thread); death.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
        exceptions.enable(); death.enable();
        ThreadState state = new ThreadState(thread, exceptions, death);
        threads.put(thread.uniqueID(), state); return state;
    }

    private static StackFrame joinedCaller(List<StackFrame> frames) {
        StackFrame found = null;
        for (StackFrame frame : frames) {
            Method method = frame.location().method();
            if (method.declaringType().name().equals("io.tapstate.app.StoreBackedPipelineCaptureCoordinator")
                    && method.name().equals("tailWhatNobodyTails") && method.signature().equals("()V")) {
                if (found != null) { throw invalid("ambiguous joined takeover caller"); }
                found = frame;
            }
        }
        return found;
    }

    private static Value callerLocal(StackFrame frame, String name, String signature) throws Exception {
        try {
            LocalVariable variable = frame.visibleVariableByName(name);
            if (variable == null || !signature.equals(variable.signature())) {
                throw new NativeTelemetryMirror.Unavailable("JOINED_LOCAL_UNAVAILABLE:" + name);
            }
            return frame.getValue(variable);
        } catch (AbsentInformationException missing) {
            throw new NativeTelemetryMirror.Unavailable("JOINED_LVT_UNAVAILABLE:" + name);
        }
    }

    private Map<String, Object> joinedTakeover(List<StackFrame> frames, List<Value> arguments,
            NativeTelemetryMirror mirror) throws Exception {
        StackFrame caller = joinedCaller(frames);
        if (caller == null) { throw new NativeTelemetryMirror.Unavailable("JOINED_CALLER_UNAVAILABLE"); }
        Method method = caller.location().method();
        validate(method.declaringType());
        byte[] expected = images.get(method.declaringType().name()).methods().get("tailWhatNobodyTails()V");
        if (expected == null || method.isObsolete() || !Arrays.equals(expected, method.bytecodes())) {
            throw new NativeTelemetryMirror.Unavailable("JOINED_CALLER_CODE_MISMATCH");
        }
        String prefix = "io.tapstate.app.StoreBackedPipelineCaptureCoordinator$";
        ObjectReference reader = requiredObject(callerLocal(caller, "reader", "L" + prefix.replace('.', '/')
                + "JoinedReader;"), prefix + "JoinedReader", mirror);
        ObjectReference logical = requiredObject(mirror.field(reader, "logical", "L" + prefix.replace('.', '/')
                + "PipelineRun;"), prefix + "PipelineRun", mirror);
        ObjectReference opening = requiredObject(callerLocal(caller, "opening", "L" + prefix.replace('.', '/')
                + "OpeningClaim;"), prefix + "OpeningClaim", mirror);
        ObjectReference capture = requiredObject(callerLocal(caller, "captureId", "Lio/tapstate/runtime/srs/CaptureId;"),
                "io.tapstate.runtime.srs.CaptureId", mirror);
        ObjectReference supplied = requiredObject(arguments.getFirst(), "io.tapstate.runtime.srs.CaptureRunSpec", mirror);
        if (supplied.uniqueID() != mirror.object(callerLocal(caller, "tailSpec",
                "Lio/tapstate/runtime/srs/CaptureRunSpec;")).uniqueID()
                || !Boolean.TRUE.equals(mirror.scalar(arguments.get(2)))) {
            throw new NativeTelemetryMirror.Unavailable("JOINED_TAIL_ARGUMENT_MISMATCH");
        }
        ObjectReference original = requiredObject(mirror.field(logical, "spec", "Lio/tapstate/runtime/srs/CaptureRunSpec;"),
                "io.tapstate.runtime.srs.CaptureRunSpec", mirror);
        ObjectReference originalRun = requiredObject(mirror.field(logical, "run", "Lio/tapstate/runtime/srs/CaptureRun;"),
                "io.tapstate.runtime.srs.CaptureRun", mirror);
        ObjectReference logOwner = requiredObject(mirror.field(logical, "logOwner", "L" + prefix.replace('.', '/')
                + "LogOwnerState;"), prefix + "LogOwnerState", mirror);
        ObjectReference admitted = requiredObject(mirror.field(logOwner, "admitted", "L" + prefix.replace('.', '/')
                + "AdmittedLogOwner;"), prefix + "AdmittedLogOwner", mirror);
        ObjectReference close = requiredObject(mirror.field(logical, "closeState", "L" + prefix.replace('.', '/')
                + "CloseState;"), prefix + "CloseState", mirror);
        Map<String, Object> scope = mirror.scope(mirror.field(reader, "scope", "Lio/tapstate/core/logging/LogSink$Scope;"));
        String source = mirror.text(mirror.field(original, "sourceId", "Ljava/lang/String;"));
        String consumer = mirror.text(mirror.field(original, "consumerId", "Ljava/lang/String;"));
        String writer = mirror.text(mirror.field(original, "snapshotWriterToken", "Ljava/lang/String;"));
        if (!pipeline.equals(mirror.text(mirror.field(original, "pipelineId", "Ljava/lang/String;")))
                || !Boolean.TRUE.equals(mirror.scalar(mirror.field(logical, "managed", "Z")))
                || !Boolean.TRUE.equals(mirror.scalar(mirror.field(logical, "sharedTail", "Z")))
                || Boolean.TRUE.equals(mirror.scalar(mirror.field(close, "completed", "Z")))
                || Boolean.TRUE.equals(mirror.scalar(mirror.field(logOwner, "retired", "Z")))
                || mirror.object(mirror.field(admitted, "run", "Lio/tapstate/runtime/srs/CaptureRun;")).uniqueID() != originalRun.uniqueID()
                || mirror.object(mirror.field(admitted, "spec", "Lio/tapstate/runtime/srs/CaptureRunSpec;")).uniqueID() != original.uniqueID()
                || !writer.equals(mirror.text(mirror.field(admitted, "writerToken", "Ljava/lang/String;")))
                || !scope.equals(mirror.scope(mirror.field(admitted, "scope", "Lio/tapstate/core/logging/LogSink$Scope;")))) {
            throw new NativeTelemetryMirror.Unavailable("JOINED_LOGICAL_ADMISSION_MISMATCH");
        }
        for (String field : List.of("pipelineId", "sourceId", "consumerId", "snapshotWriterToken")) {
            if (!mirror.text(mirror.field(original, field, "Ljava/lang/String;"))
                    .equals(mirror.text(mirror.field(supplied, field, "Ljava/lang/String;")))) {
                throw new NativeTelemetryMirror.Unavailable("JOINED_SPEC_IDENTITY_MISMATCH:" + field);
            }
        }
        if (!"CDC_ONLY".equals(mirror.enumName(mirror.field(supplied, "readMode", "Lio/tapstate/core/model/ReadMode;")))
                || !Boolean.TRUE.equals(mirror.scalar(mirror.field(supplied, "srsEnabled", "Z")))
                || Boolean.TRUE.equals(mirror.scalar(mirror.field(opening, "lost", "Z")))
                || mirror.field(opening, "published", "L" + prefix.replace('.', '/') + "OwnedCapture;") != null) {
            throw new NativeTelemetryMirror.Unavailable("JOINED_OPENING_STATE_MISMATCH");
        }
        String captureId = mirror.text(mirror.field(capture, "value", "Ljava/lang/String;"));
        for (ObjectReference holder : List.of(logical, opening)) {
            ObjectReference held = requiredObject(mirror.field(holder, "captureId", "Lio/tapstate/runtime/srs/CaptureId;"),
                    "io.tapstate.runtime.srs.CaptureId", mirror);
            if (!captureId.equals(mirror.text(mirror.field(held, "value", "Ljava/lang/String;")))) {
                throw new NativeTelemetryMirror.Unavailable("JOINED_HELD_CAPTURE_ID_MISMATCH");
            }
        }
        if (mirror.object(mirror.field(opening, "this$0", "Lio/tapstate/app/StoreBackedPipelineCaptureCoordinator;"))
                .uniqueID() != caller.thisObject().uniqueID()) {
            throw new NativeTelemetryMirror.Unavailable("JOINED_OPENING_RECEIVER_MISMATCH");
        }
        ObjectReference permit = requiredObject(mirror.field(opening, "permit", "Lio/tapstate/app/CaptureOwnership$Permit;"),
                "io.tapstate.app.CaptureOwnership$Permit", mirror);
        if (!Boolean.TRUE.equals(mirror.scalar(mirror.field(permit, "acquired", "Z")))) {
            throw new NativeTelemetryMirror.Unavailable("JOINED_CAPTURE_PERMIT_REFUSED");
        }
        Map<String, Object> claim = workloadClaim(mirror.field(permit, "claim", "Lio/tapstate/spi/store/WorkloadClaim;"), mirror);
        Map<?, ?> key = (Map<?, ?>) claim.get("key");
        if (!"CAPTURE".equals(key.get("type")) || !captureId.equals(key.get("resourceId"))) {
            throw new NativeTelemetryMirror.Unavailable("JOINED_CAPTURE_CLAIM_MISMATCH");
        }
        ObjectReference config = requiredObject(mirror.field(supplied, "config", "Lio/tapstate/spi/capture/CaptureConfig;"),
                "io.tapstate.spi.capture.CaptureConfig", mirror);
        ObjectReference node = requiredObject(mirror.field(config, "node", "Lio/tapstate/core/model/PipelineNode;"),
                "io.tapstate.core.model.PipelineNode", mirror);
        if (!pipeline.equals(mirror.text(mirror.field(node, "pipelineId", "Ljava/lang/String;")))
                || !source.equals(mirror.text(mirror.field(node, "nodeId", "Ljava/lang/String;")))) {
            throw new NativeTelemetryMirror.Unavailable("JOINED_CONFIG_NODE_MISMATCH");
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("scope", scope); out.put("pipelineId", pipeline); out.put("sourceId", source); out.put("consumerId", consumer);
        out.put("captureId", captureId); out.put("captureClaim", claim); out.put("writerToken", writer);
        out.put("coordinatorReceiver", caller.thisObject().uniqueID()); out.put("readerObject", reader.uniqueID());
        out.put("logicalRunObject", originalRun.uniqueID()); out.put("logicalSpecObject", original.uniqueID());
        out.put("logOwnerObject", logOwner.uniqueID()); out.put("admittedObject", admitted.uniqueID());
        out.put("openingObject", opening.uniqueID()); out.put("tailSpecObject", supplied.uniqueID());
        out.put("callerCodeSha256", hash(expected)); out.put("callerLoaderId", loaderId);
        return Map.copyOf(out);
    }

    private void entry(Spec spec, ObjectReference receiver, List<Value> arguments,
            NativeTelemetryMirror mirror, Map<String, Object> out) throws Exception {
        if (spec.target() == Target.ADMISSION || spec.target() == Target.REPLACEMENT_ADMISSION) {
            out.put("claimed", mirror.scalar(mirror.field(receiver, "fenced", "Z")));
            out.put("clusterId", mirror.text(mirror.field(receiver, "clusterId", "Ljava/lang/String;")));
            Value owner = mirror.field(receiver, "owner", "Lio/tapstate/spi/store/WorkloadOwner;");
            if (owner != null) { out.put("owner", workloadOwner(owner, mirror)); }
            if (spec.target() == Target.REPLACEMENT_ADMISSION) {
                out.putAll(replacementArgument(arguments.getFirst(), mirror));
            }
        } else if (spec.target() == Target.SUBMIT) {
            ObjectReference execution = requiredObject(mirror.field(receiver, "val$execution", EXECUTION),
                    "io.tapstate.app.PipelineActuationOwnership$Execution", mirror);
            Admission admission = admissions.get(execution.uniqueID());
            if (admission == null) { throw new NativeTelemetryMirror.Unavailable("SUBMIT_HAS_NO_OBSERVED_ADMISSION"); }
            Value accepted = mirror.field(receiver, "val$accepted", "Lio/tapstate/spi/store/SuccessorAdmission;");
            if (admission.replacement().isPresent()) {
                Replacement replacement = admission.replacement().orElseThrow();
                ObjectReference actual = requiredObject(accepted, "io.tapstate.spi.store.SuccessorAdmission", mirror);
                Map<String, Object> evidence = replacementArgument(actual, mirror);
                if (actual.uniqueID() != replacement.argumentId()
                        || !Long.valueOf(replacement.reservationId()).equals(evidence.get("reservationObject"))
                        || !Objects.equals(replacement.advancedClaimId(), evidence.get("advancedClaimObject"))
                        || !replacement.reservation().equals(evidence.get("reservation"))
                        || !admission.claim().equals(evidence.get("claim"))) {
                    throw new NativeTelemetryMirror.Unavailable("SUBMIT_HAS_DIFFERENT_REPLACEMENT_ARGUMENT");
                }
                out.put("successorAdmissionObject", actual.uniqueID());
                out.put("reservationObject", replacement.reservationId());
                out.put("advancedClaimObject", replacement.advancedClaimId());
                out.put("reservation", replacement.reservation());
            } else if (accepted != null) {
                throw new NativeTelemetryMirror.Unavailable("REPLACEMENT_HAS_NO_OBSERVED_ADOPTION");
            }
            ObjectReference actuator = requiredObject(mirror.field(receiver, "this$0", "Lio/tapstate/app/EngineLifecycleActuator;"),
                    "io.tapstate.app.EngineLifecycleActuator", mirror);
            ObjectReference owner = requiredObject(mirror.field(actuator, "actuation",
                    "Lio/tapstate/app/PipelineActuationOwnership;"), "io.tapstate.app.PipelineActuationOwnership", mirror);
            if (owner.uniqueID() != admission.ownershipReceiverId()) {
                throw new NativeTelemetryMirror.Unavailable("SUBMIT_ADMISSION_OWNER_RECEIVER_MISMATCH");
            }
            ObjectReference engine = requiredObject(mirror.field(actuator, "engine", "Lio/tapstate/runtime/engine/Engine;"),
                    "io.tapstate.runtime.engine.Engine", mirror);
            Map<String, Object> scope = mirror.scope(mirror.field(receiver, "val$observationScope", SCOPE));
            if (!Objects.equals(scope.get("generation"), admission.fence().get("executionGeneration"))) {
                throw new NativeTelemetryMirror.Unavailable("SUBMIT_SCOPE_ADMISSION_MISMATCH");
            }
            if (!Boolean.FALSE.equals(mirror.scalar(mirror.field(receiver, "submitted", "Z")))
                    || !Boolean.FALSE.equals(mirror.scalar(mirror.field(receiver, "closed", "Z")))) {
                throw new NativeTelemetryMirror.Unavailable("SUBMIT_ALREADY_USED_OR_CLOSED");
            }
            out.put("admissionObject", execution.uniqueID());
            out.put("ownershipReceiver", owner.uniqueID()); out.put("engineReceiver", engine.uniqueID());
            out.put("submissionBootId", mirror.text(mirror.field(engine, "bootId", "Ljava/lang/String;")));
            out.put("requestScope", scope); out.put("fence", admission.fence());
            out.put("members", admission.members()); out.put("claim", admission.claim());
            out.put("admissionKind", admission.replacement().isPresent() ? "REPLACEMENT" : "ORDINARY");
        } else if (spec.target() == Target.RAW_CONTINUATION) {
            ObjectReference prepared = requiredObject(arguments.getFirst(),
                    "io.tapstate.runtime.scheduler.ObservationPublisher$Prepared", mirror);
            ObjectReference observation = requiredObject(mirror.field(prepared, "observation",
                    "Lio/tapstate/core/lifecycle/Observation;"), "io.tapstate.core.lifecycle.Observation", mirror);
            ObjectReference target = requiredObject(arguments.get(1), "io.tapstate.app.ObservationScopeRegistry$ActualTarget", mirror);
            out.put("preparedObject", prepared.uniqueID()); out.put("rawObservationObject", observation.uniqueID());
            out.put("actualTargetObject", target.uniqueID());
            out.put("scope", mirror.scope(mirror.field(target, "scope", SCOPE)));
            out.put("job", mirror.job(mirror.field(target, "job", "Lio/tapstate/spi/store/StopReservation$JobIdentity;")));
            out.put("observedAt", mirror.scalar(mirror.field(observation, "observedAt", "Ljava/time/Instant;")));
            out.put("state", mirror.enumName(mirror.field(observation, "state", "Lio/tapstate/core/lifecycle/PipelineState;")));
            out.put("facts", facts(mirror.field(observation, "facts", "Ljava/util/List;"), mirror));
        } else if (spec.target() == Target.OFFER) {
            producers.add(receiver.uniqueID());
            out.put("scope", mirror.scope(arguments.get(1))); out.put("state", mirror.enumName(arguments.get(2)));
            out.put("observedAt", mirror.scalar(arguments.get(3))); out.put("facts", facts(arguments.get(4), mirror));
        } else if (spec.target() == Target.LOG) {
            Value scope = arguments.get(1);
            out.put("scope", scope == null ? null : mirror.scope(scope));
            if (scope == null) {
                out.put("identityStatus", "UNSCOPED");
                out.put("identityUnverified", true);
            }
            ObjectReference line = mirror.object(arguments.get(2));
            out.put("timestampMillis", mirror.integral(mirror.field(line, "timestampMillis", "J")));
            out.put("level", mirror.text(mirror.field(line, "level", "Ljava/lang/String;")));
            out.put("message", mirror.text(mirror.field(line, "message", "Ljava/lang/String;")));
        } else if (spec.target() == Target.VISIBLE) {
            out.putAll(offered(mirror.object(arguments.get(1)), mirror));
        } else if (spec.target() == Target.PREPARE) {
            if (publishers.size() >= MAX_CLASSES && !publishers.contains(receiver.uniqueID())) {
                throw invalid("observed publisher budget exceeded");
            }
            publishers.add(receiver.uniqueID());
            ObjectReference folder = mirror.object(mirror.field(receiver, "cardinality",
                    "Lio/tapstate/core/lifecycle/CardinalityBudget$Folder;"));
            publisherFolders.add(folder.uniqueID());
            out.put("folderReceiver", folder.uniqueID());
            out.put("requestScope", arguments.get(2) == null ? "UNSCOPED" : mirror.scope(arguments.get(2)));
            if (arguments.get(2) == null) { unknown("UNSCOPED_PREPARATION"); }
            out.put("nonNullFailure", arguments.get(1) != null);
        } else if (spec.target() == Target.FOLDER_FORGET) {
            out.put("before", folderNames(receiver, mirror));
        } else if (spec.target() == Target.EXPORT_FORGET || spec.target() == Target.EXPORT_INCARNATION) {
            out.put("beforeNamed", exporterNames(receiver, mirror));
            if (spec.target() == Target.EXPORT_INCARNATION) {
                out.put("requestedIncarnation", mirror.text(arguments.get(1)));
                out.put("cachedBefore", cachedProof(receiver, mirror));
            }
        } else if (spec.target() == Target.PUBLISHER_SWEEP || spec.target() == Target.EXPORT_SWEEP
                || spec.target() == Target.FOLDER_SWEEP) {
            out.put("keptIds", mirror.scalars(arguments.getFirst()));
            if (((List<?>) out.get("keptIds")).stream().anyMatch(value -> !(value instanceof String))) {
                throw new NativeTelemetryMirror.Unavailable("KEPT_PIPELINE_ID_LAYOUT");
            }
            if (spec.target() == Target.PUBLISHER_SWEEP) { out.put("beforeAccount", accountPresence(receiver, mirror)); }
            else if (spec.target() == Target.EXPORT_SWEEP) {
                out.put("beforeNamed", exporterNames(receiver, mirror)); out.put("cachedBefore", cachedProof(receiver, mirror));
            } else { out.put("before", folderNames(receiver, mirror)); }
        }
    }

    private void normalExit(MethodExitEvent event) throws Exception {
        ThreadState state = threads.get(event.thread().uniqueID());
        Call call = state == null ? null : state.calls.peek();
        if (call == null || (!call.qualified && !call.logCalibration) || call.exit != event.request()
                || !Objects.equals(event.request().getProperty("native-identity-call"), call.id)
                || !event.method().equals(methods.get(call.spec.target())) || call.escaping) {
            throw invalid("method exit lost its exact return-site correlation");
        }
        vm.eventRequestManager().deleteEventRequest(call.exit); call.exit = null;
        Map<String, Object> out = new LinkedHashMap<>(call.entry);
        if (!call.logCalibration) {
            try { returned(call, event.returnValue(), state, out, mirror()); }
            catch (NativeTelemetryMirror.Unavailable missing) {
                decoderUnavailable(out, call.spec.target(), "RETURN", missing.getMessage());
            }
        }
        out.put("target", call.logCalibration ? "LOG_FAMILY_CALIBRATION" : call.spec.target().name());
        out.put("invocation", call.id);
        out.put("returnOrder", events);
        out.put("receiver", call.receiver.uniqueID()); out.put("normalReturn", true);
        if (call.logCalibration) { out.put("calibrationComplete", !out.containsKey("decoderStatus")); }
        addRecord(out);
        if (call.logCalibration) {
            logFamilyCalibration = Collections.unmodifiableMap(new LinkedHashMap<>(out));
        } else { totals.get(call.spec.target()).normal++; }
        state.calls.pop(); removeEmpty(state);
    }

    private void returned(Call call, Value returned, ThreadState state, Map<String, Object> out,
            NativeTelemetryMirror mirror) throws Exception {
        switch (call.spec.target()) {
            case JOINED_TAKEOVER -> {
                if (call.entry.containsKey("decoderStatus")) {
                    throw new NativeTelemetryMirror.Unavailable("JOINED_ENTRY_UNVERIFIED");
                }
                Map<String, Object> after = joinedTakeover(frames(state.thread), call.arguments, mirror);
                if (!after.equals(call.entry.entrySet().stream().filter(entry -> !entry.getKey().equals("entryOrder"))
                        .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))) {
                    throw new NativeTelemetryMirror.Unavailable("JOINED_CALLER_CHANGED_DURING_BEGIN");
                }
                ObjectReference actual = requiredObject(returned, "io.tapstate.runtime.srs.CaptureRun", mirror);
                ObjectReference optional = requiredObject(mirror.field(actual, "chainId", "Ljava/util/Optional;"),
                        "java.util.Optional", mirror);
                ObjectReference chain = requiredObject(mirror.field(optional, "value", "Ljava/lang/Object;"),
                        "io.tapstate.runtime.srs.MiningChainId", mirror);
                if (Long.valueOf(actual.uniqueID()).equals(call.entry.get("logicalRunObject"))) {
                    throw new NativeTelemetryMirror.Unavailable("JOINED_RETURN_REUSED_LOGICAL_HANDLE");
                }
                out.put("returnedRunObject", actual.uniqueID());
                out.put("returnedChainId", mirror.text(mirror.field(chain, "value", "Ljava/lang/String;")));
                out.put("callerStable", true);
            }
            case ADMISSION -> {
                if (call.entry.containsKey("decoderStatus")) {
                    throw new NativeTelemetryMirror.Unavailable("ADMISSION_ENTRY_UNVERIFIED");
                }
                ObjectReference execution = requiredObject(returned,
                        "io.tapstate.app.PipelineActuationOwnership$Execution", mirror);
                out.put("admissionObject", execution.uniqueID());
                Object allowed = mirror.scalar(mirror.field(execution, "allowed", "Z"));
                out.put("allowed", allowed);
                if (!Boolean.TRUE.equals(allowed)) { out.put("admissionStatus", "REFUSED"); break; }
                Map<String, Object> fence = executionFence(mirror.field(execution, "fence", "Lio/tapstate/app/ExecutionFence;"), mirror);
                List<String> members = executionMembers(mirror.field(execution, "executionNodeIds", "Ljava/util/Set;"), mirror);
                ObjectReference optional = requiredObject(mirror.field(execution, "admittedClaim", "Ljava/util/Optional;"),
                        "java.util.Optional", mirror);
                Value value = mirror.field(optional, "value", "Ljava/lang/Object;");
                Map<String, Object> claim = value == null ? Map.of() : workloadClaim(value, mirror);
                boolean claimed = Boolean.TRUE.equals(call.entry.get("claimed"));
                qualifyAdmission(fence, members, claim, claimed, call.entry);
                if (admissions.size() >= 8 && !admissions.containsKey(execution.uniqueID())) {
                    throw invalid("captured admission budget exceeded");
                }
                admissions.put(execution.uniqueID(), new Admission(execution.uniqueID(), call.receiver.uniqueID(),
                        fence, members, claim, Optional.empty()));
                out.put("fence", fence); out.put("members", members); out.put("claim", claim);
                out.put("admissionStatus", claimed ? "CLAIMED" : "STANDALONE");
            }
            case REPLACEMENT_ADMISSION -> {
                if (call.entry.containsKey("decoderStatus")) {
                    throw new NativeTelemetryMirror.Unavailable("REPLACEMENT_ENTRY_UNVERIFIED");
                }
                Map<String, Object> argument = replacementArgument(call.arguments.getFirst(), mirror);
                for (String key : List.of("successorAdmissionObject", "reservationObject", "advancedClaimObject", "reservation", "claim")) {
                    if (!Objects.equals(argument.get(key), call.entry.get(key))) {
                        throw new NativeTelemetryMirror.Unavailable("REPLACEMENT_ARGUMENT_CHANGED_DURING_ADOPTION");
                    }
                }
                ObjectReference execution = requiredObject(returned,
                        "io.tapstate.app.PipelineActuationOwnership$Execution", mirror);
                out.put("admissionObject", execution.uniqueID());
                Object allowed = mirror.scalar(mirror.field(execution, "allowed", "Z"));
                out.put("allowed", allowed);
                if (!Boolean.TRUE.equals(allowed)) { out.put("admissionStatus", "REFUSED"); break; }
                Map<String, Object> fence = executionFence(mirror.field(execution, "fence", "Lio/tapstate/app/ExecutionFence;"), mirror);
                List<String> members = executionMembers(mirror.field(execution, "executionNodeIds", "Ljava/util/Set;"), mirror);
                ObjectReference returnedClaim = requiredObject(mirror.field(execution, "admittedClaim", "Ljava/util/Optional;"),
                        "java.util.Optional", mirror);
                if (mirror.field(returnedClaim, "value", "Ljava/lang/Object;") != null) {
                    throw new NativeTelemetryMirror.Unavailable("REPLACEMENT_RETURN_CARRIES_UNEXPECTED_CLAIM");
                }
                out.put("returnedAdmittedClaimStatus", "ABSENT");
                out.put("claimSource", "SUCCESSOR_ADMISSION_ARGUMENT");
                @SuppressWarnings("unchecked") Map<String, Object> claim = (Map<String, Object>) argument.get("claim");
                @SuppressWarnings("unchecked") Map<String, Object> reservation = (Map<String, Object>) argument.get("reservation");
                qualifyAdmission(fence, members, claim, Boolean.TRUE.equals(call.entry.get("claimed")), call.entry);
                qualifyReplacement(fence, claim, reservation, call.entry);
                if (admissions.size() >= 8 && !admissions.containsKey(execution.uniqueID())) {
                    throw invalid("captured admission budget exceeded");
                }
                Replacement replacement = new Replacement(((Number) argument.get("successorAdmissionObject")).longValue(),
                        ((Number) argument.get("reservationObject")).longValue(),
                        claim.isEmpty() ? null : ((Number) argument.get("advancedClaimObject")).longValue(), reservation);
                admissions.put(execution.uniqueID(), new Admission(execution.uniqueID(), call.receiver.uniqueID(),
                        fence, members, claim, Optional.of(replacement)));
                out.put("fence", fence); out.put("members", members);
                out.put("admissionStatus", claim.isEmpty() ? "STANDALONE_REPLACEMENT" : "CLAIMED_REPLACEMENT");
            }
            case SUBMIT -> {
                if (call.entry.containsKey("decoderStatus")) {
                    throw new NativeTelemetryMirror.Unavailable("SUBMIT_ENTRY_UNVERIFIED");
                }
                ObjectReference registered = requiredObject(mirror.field(call.receiver, "submittedExecution", EXECUTION_JOB),
                        "io.tapstate.runtime.engine.Engine$ExecutionJob", mirror);
                CapturedJob actual = call.submittedJob;
                if (!Boolean.TRUE.equals(mirror.scalar(mirror.field(call.receiver, "submitted", "Z")))
                        || !Boolean.FALSE.equals(mirror.scalar(mirror.field(call.receiver, "closed", "Z")))
                        || actual == null || actual.executionObjectId() != registered.uniqueID()) {
                    throw new NativeTelemetryMirror.Unavailable("SUBMIT_HAS_NO_EXACT_RETURNED_JOB");
                }
                Admission admission = admissions.get(((Number) call.entry.get("admissionObject")).longValue());
                if (admission == null || !actual.scope().equals(call.entry.get("requestScope"))
                        || actual.receiverId() != ((Number) call.entry.get("engineReceiver")).longValue()
                        || !actual.job().get("bootId").equals(call.entry.get("submissionBootId"))) {
                    throw new NativeTelemetryMirror.Unavailable("SUBMIT_JOB_ADMISSION_MISMATCH");
                }
                if (admission.replacement().isPresent()) {
                    Replacement expected = admission.replacement().orElseThrow();
                    ObjectReference execution = requiredObject(mirror.field(call.receiver, "val$execution", EXECUTION),
                            "io.tapstate.app.PipelineActuationOwnership$Execution", mirror);
                    Map<String, Object> returnedArgument = replacementArgument(mirror.field(call.receiver, "val$accepted",
                            "Lio/tapstate/spi/store/SuccessorAdmission;"), mirror);
                    if (execution.uniqueID() != admission.objectId()
                            || !Long.valueOf(expected.argumentId()).equals(returnedArgument.get("successorAdmissionObject"))
                            || !Long.valueOf(expected.reservationId()).equals(returnedArgument.get("reservationObject"))
                            || !Objects.equals(expected.advancedClaimId(), returnedArgument.get("advancedClaimObject"))
                            || !expected.reservation().equals(returnedArgument.get("reservation"))
                            || !admission.claim().equals(returnedArgument.get("claim"))) {
                        throw new NativeTelemetryMirror.Unavailable("SUBMIT_REPLACEMENT_ARGUMENT_CHANGED");
                    }
                    Map<?, ?> slot = (Map<?, ?>) admission.replacement().orElseThrow().reservation().get("successor");
                    if (!actual.scope().equals(slot.get("scope"))
                            || !actual.job().get("bootId").equals(slot.get("submissionBootId"))) {
                        throw new NativeTelemetryMirror.Unavailable("SUBMIT_JOB_REPLACEMENT_SLOT_MISMATCH");
                    }
                }
                if (!admission.claim().isEmpty()) {
                    Map<?, ?> key = (Map<?, ?>) admission.claim().get("key");
                    if (!actual.job().get("clusterId").equals(key.get("clusterId"))) {
                        throw new NativeTelemetryMirror.Unavailable("SUBMIT_JOB_CLAIM_CLUSTER_MISMATCH");
                    }
                    long jobId = ((Number) actual.job().get("jobId")).longValue();
                    if (claimedSubmissions.size() + claimedReplacements.size() >= 8
                            && !claimedSubmissions.containsKey(jobId) && !claimedReplacements.containsKey(jobId)) {
                        throw invalid("claimed submission budget exceeded");
                    }
                    ClaimedSubmission observed = new ClaimedSubmission(actual, admission.objectId(),
                            admission.ownershipReceiverId(), call.receiver.uniqueID(), admission.fence(),
                            admission.members(), admission.claim());
                    if (admission.replacement().isPresent()) {
                        Replacement replacement = admission.replacement().orElseThrow();
                        ClaimedReplacement captured = new ClaimedReplacement(observed, replacement.argumentId(),
                                replacement.reservationId(), replacement.advancedClaimId(), replacement.reservation());
                        ClaimedReplacement previous = claimedReplacements.putIfAbsent(jobId, captured);
                        if (previous != null && !previous.equals(captured) || claimedSubmissions.containsKey(jobId)) {
                            throw invalid("one Job has conflicting replacement admissions");
                        }
                    } else {
                        ClaimedSubmission previous = claimedSubmissions.putIfAbsent(jobId, observed);
                        if (previous != null && !previous.equals(observed) || claimedReplacements.containsKey(jobId)) {
                            throw invalid("one Job has conflicting claimed admissions");
                        }
                    }
                }
                out.put("registered", true); out.put("scope", actual.scope()); out.put("job", actual.job());
                out.put("proxy", actual.proxyId()); out.put("executionJobObject", registered.uniqueID());
            }
            case JOB -> {
                if (returned == null) { out.put("jobStatus", "ABSENT"); break; }
                ObjectReference execution = mirror.object(returned);
                Map<String, Object> scope = mirror.scope(mirror.field(execution, "scope", SCOPE));
                Map<String, Object> job = mirror.job(mirror.field(execution, "job",
                        "Lio/tapstate/spi/store/StopReservation$JobIdentity;"));
                ObjectReference proxy = mirror.object(call.arguments.get(1));
                long nativeId = mirror.integral(mirror.field(proxy, "jobId", "J"));
                if (nativeId != ((Number) job.get("jobId")).longValue()) { throw invalid("actual proxy and job identity differ"); }
                if (jobs.size() >= 8 && !jobs.containsKey(nativeId)) { throw invalid("captured job budget exceeded"); }
                CapturedJob captured = new CapturedJob(proxy, proxy.uniqueID(), job, scope,
                        call.receiver.uniqueID(), execution.uniqueID());
                jobs.put(nativeId, captured);
                for (Call submit : state.calls) {
                    if (submit.spec.target() == Target.SUBMIT && submit.qualified
                            && !submit.entry.containsKey("decoderStatus")
                            && call.receiver.uniqueID() == ((Number) submit.entry.get("engineReceiver")).longValue()
                            && scope.equals(submit.entry.get("requestScope"))) {
                        if (submit.submittedJob != null && !submit.submittedJob.job().equals(job)) {
                            throw invalid("one submit returned different native Job identities");
                        }
                        submit.submittedJob = captured;
                    }
                }
                out.put("scope", scope); out.put("job", job); out.put("proxy", proxy.uniqueID());
                out.put("executionJobObject", execution.uniqueID());
            }
            case OFFER -> {
                Value cached = mirror.lookup(mirror.field(call.receiver, "latest", "Ljava/util/Map;"), pipeline);
                if (cached == null) { out.put("accepted", false); break; }
                ObjectReference reference = mirror.object(cached);
                Map<String, Object> actual = offered(reference, mirror);
                boolean sameEntryFrame = List.of("scope", "state", "observedAt", "facts").stream()
                        .allMatch(key -> call.entry.containsKey(key)
                                && Objects.equals(actual.get(key), call.entry.get(key)));
                if (sameEntryFrame) {
                    // Every cached field is available in this record's complete entry frame. The actual
                    // cached object and its fully decoded hash qualify the reference without repeating facts.
                    Map<String, Object> proof = new LinkedHashMap<>(offeredProof(reference, actual));
                    proof.put("frameReference", "THIS_RECORD_ENTRY_FIELDS");
                    proof.put("entryInvocation", call.id);
                    proof.put("entryOrder", call.entry.get("entryOrder"));
                    proof.put("frameAvailable", true);
                    out.put("cached", Collections.unmodifiableMap(proof));
                } else {
                    out.put("cached", actual);
                }
                out.put("accepted", actual.get("scope").equals(call.entry.get("scope"))
                        && actual.get("observedAt").equals(call.entry.get("observedAt"))
                        && actual.get("facts").equals(call.entry.get("facts")));
            }
            case VISIBLE -> {
                if (!(returned instanceof BooleanValue value)) { throw invalid("visible returned no actual boolean"); }
                out.put("included", value.value());
                if (value.value()) {
                    Call produce = state.calls.stream().filter(parent -> parent.qualified
                            && parent.spec.target() == Target.PRODUCE
                            && parent.receiver.uniqueID() == call.receiver.uniqueID()).findFirst().orElse(null);
                    if (produce == null) { unknown("VISIBLE_WITHOUT_PRODUCE"); }
                    else {
                        if (produce.included.size() >= 8) { throw invalid("included frame budget exceeded"); }
                        produce.included.add(Map.copyOf(call.entry));
                    }
                }
            }
            case PRODUCE -> {
                out.put("includedFrames", List.copyOf(call.included));
                out.put("metrics", metrics(returned, mirror));
                out.put("namedBackstop", exporterNames(call.receiver, mirror));
            }
            case PREPARE -> {
                out.put("account", account(call.receiver, mirror));
                out.put("preparedPresent", mirror.field(mirror.object(returned), "value", "Ljava/lang/Object;") != null);
            }
            case RAW_CONTINUATION -> {
                if (call.entry.containsKey("decoderStatus")) {
                    throw new NativeTelemetryMirror.Unavailable("RAW_CONTINUATION_ENTRY_UNVERIFIED");
                }
                ObjectReference optional = requiredObject(returned, "java.util.Optional", mirror);
                Value value = mirror.field(optional, "value", "Ljava/lang/Object;");
                out.put("publicationPresent", value != null);
                if (value != null) {
                    ObjectReference publication = requiredObject(value,
                            "io.tapstate.app.ObservationScopeRegistry$ContinuationPublication", mirror);
                    ObjectReference projected = requiredObject(mirror.field(publication, "projected",
                            "Lio/tapstate/runtime/scheduler/ObservationPublisher$Prepared;"),
                            "io.tapstate.runtime.scheduler.ObservationPublisher$Prepared", mirror);
                    ObjectReference observation = requiredObject(mirror.field(projected, "observation",
                            "Lio/tapstate/core/lifecycle/Observation;"), "io.tapstate.core.lifecycle.Observation", mirror);
                    Map<String, Object> scope = mirror.scope(mirror.field(publication, "scope", SCOPE));
                    Object at = mirror.scalar(mirror.field(observation, "observedAt", "Ljava/time/Instant;"));
                    if (!pipeline.equals(mirror.text(mirror.field(observation, "pipelineId", "Ljava/lang/String;")))
                            || !scope.equals(call.entry.get("scope")) || !Objects.equals(at, call.entry.get("observedAt"))) {
                        throw new NativeTelemetryMirror.Unavailable("RAW_CONTINUATION_PUBLICATION_MISMATCH");
                    }
                    out.put("publicationObject", publication.uniqueID()); out.put("projectedPreparedObject", projected.uniqueID());
                    out.put("projectedObservationObject", observation.uniqueID()); out.put("publicationScope", scope);
                    out.put("publicationObservedAt", at);
                }
            }
            case FOLDER_FORGET -> out.put("after", folderNames(call.receiver, mirror));
            case EXPORT_FORGET, EXPORT_INCARNATION -> {
                out.put("afterNamed", exporterNames(call.receiver, mirror));
                Value cached = mirror.lookup(mirror.field(call.receiver, "latest", "Ljava/util/Map;"), pipeline);
                if (cached == null) {
                    out.put("cachedAfter", "ABSENT");
                    if (call.spec.target() == Target.EXPORT_INCARNATION) { out.put("cachedAfterProof", "ABSENT"); }
                } else {
                    ObjectReference reference = mirror.object(cached);
                    Map<String, Object> complete = offered(reference, mirror);
                    out.put("cachedAfter", complete);
                    if (call.spec.target() == Target.EXPORT_INCARNATION) {
                        out.put("cachedAfterProof", offeredProof(reference, complete));
                    }
                }
            }
            case PUBLISHER_SWEEP -> out.put("afterAccount", accountPresence(call.receiver, mirror));
            case EXPORT_SWEEP -> {
                out.put("afterNamed", exporterNames(call.receiver, mirror));
                out.put("cachedAfter", cachedProof(call.receiver, mirror));
            }
            case FOLDER_SWEEP -> out.put("after", folderNames(call.receiver, mirror));
        }
    }

    private static ObjectReference requiredObject(Value value, String type, NativeTelemetryMirror mirror)
            throws Exception {
        ObjectReference object = mirror.object(value);
        if (!object.referenceType().name().equals(type)) {
            throw new NativeTelemetryMirror.Unavailable("OBJECT_TYPE:" + type);
        }
        return object;
    }

    private static String rawPipeline(Value value, NativeTelemetryMirror mirror) throws Exception {
        ObjectReference prepared = requiredObject(value, "io.tapstate.runtime.scheduler.ObservationPublisher$Prepared", mirror);
        ObjectReference observation = requiredObject(mirror.field(prepared, "observation",
                "Lio/tapstate/core/lifecycle/Observation;"), "io.tapstate.core.lifecycle.Observation", mirror);
        return mirror.text(mirror.field(observation, "pipelineId", "Ljava/lang/String;"));
    }

    private static String replacementPipeline(Value value, NativeTelemetryMirror mirror) throws Exception {
        ObjectReference admission = requiredObject(value, "io.tapstate.spi.store.SuccessorAdmission", mirror);
        ObjectReference marker = requiredObject(mirror.field(admission, "reservation", "Lio/tapstate/spi/store/StopReservation;"),
                "io.tapstate.spi.store.StopReservation", mirror);
        return mirror.text(mirror.field(marker, "pipelineId", "Ljava/lang/String;"));
    }

    private static Map<String, Object> replacementArgument(Value value, NativeTelemetryMirror mirror) throws Exception {
        ObjectReference admission = requiredObject(value, "io.tapstate.spi.store.SuccessorAdmission", mirror);
        ObjectReference marker = requiredObject(mirror.field(admission, "reservation", "Lio/tapstate/spi/store/StopReservation;"),
                "io.tapstate.spi.store.StopReservation", mirror);
        ObjectReference optional = requiredObject(mirror.field(admission, "advancedClaim", "Ljava/util/Optional;"),
                "java.util.Optional", mirror);
        Value advanced = mirror.field(optional, "value", "Ljava/lang/Object;");
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("successorAdmissionObject", admission.uniqueID()); result.put("reservationObject", marker.uniqueID());
        result.put("advancedClaimObject", advanced == null ? null : mirror.object(advanced).uniqueID());
        result.put("advancedClaimStatus", advanced == null ? "ABSENT" : "PRESENT");
        result.put("reservation", reservation(marker, mirror));
        result.put("claim", advanced == null ? Map.of() : workloadClaim(advanced, mirror));
        return Collections.unmodifiableMap(result);
    }

    private static Map<String, Object> reservation(ObjectReference marker, NativeTelemetryMirror mirror) throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("pipelineId", mirror.text(mirror.field(marker, "pipelineId", "Ljava/lang/String;")));
        result.put("token", mirror.text(mirror.field(marker, "token", "Ljava/lang/String;")));
        result.put("sourceEpoch", mirror.integral(mirror.field(marker, "sourceEpoch", "J")));
        result.put("reservedEpoch", mirror.integral(mirror.field(marker, "reservedEpoch", "J")));
        result.put("formatVersion", mirror.integral(mirror.field(marker, "formatVersion", "I")));
        result.put("phase", mirror.enumName(mirror.field(marker, "phase", "Lio/tapstate/spi/store/StopReservation$Phase;")));
        result.put("counterPolicy", mirror.enumName(mirror.field(marker, "counterPolicy", "Lio/tapstate/spi/store/StopReservation$CounterPolicy;")));
        ObjectReference desired = requiredObject(mirror.field(marker, "originalDesired", "Lio/tapstate/core/lifecycle/DesiredState;"),
                "io.tapstate.core.lifecycle.DesiredState", mirror);
        Map<String, Object> intent = new LinkedHashMap<>();
        for (String field : List.of("pipelineId", "revision", "assemblyRevision")) {
            intent.put(field, mirror.scalar(mirror.field(desired, field, "Ljava/lang/String;")));
        }
        intent.put("targetState", mirror.enumName(mirror.field(desired, "targetState", "Lio/tapstate/core/lifecycle/PipelineState;")));
        intent.put("purgeState", mirror.scalar(mirror.field(desired, "purgeState", "Z")));
        intent.put("reassemble", mirror.scalar(mirror.field(desired, "reassemble", "Z")));
        intent.put("rebuiltAtStateEpoch", mirror.scalar(mirror.field(desired, "rebuiltAtStateEpoch", "Ljava/lang/Long;")));
        result.put("originalDesired", Collections.unmodifiableMap(intent));
        ObjectReference source = requiredObject(mirror.field(marker, "source", "Lio/tapstate/spi/store/StopReservation$Source;"),
                "io.tapstate.spi.store.StopReservation$Source", mirror);
        Value sourceScope = mirror.field(source, "scope", SCOPE);
        Value oldJob = mirror.field(source, "oldJob", "Lio/tapstate/spi/store/StopReservation$JobIdentity;");
        Map<String, Object> old = new LinkedHashMap<>();
        old.put("clusterId", mirror.text(mirror.field(source, "clusterId", "Ljava/lang/String;")));
        old.put("scope", sourceScope == null ? null : mirror.scope(sourceScope));
        old.put("oldJob", oldJob == null ? null : mirror.job(oldJob));
        result.put("source", Collections.unmodifiableMap(old));
        ObjectReference authority = requiredObject(mirror.field(marker, "writerAuthority", "Lio/tapstate/spi/store/StopAuthority;"),
                "io.tapstate.spi.store.StopAuthority", mirror);
        Value claimed = mirror.field(authority, "claim", "Lio/tapstate/spi/store/WorkloadClaimFence;");
        result.put("writerAuthority", Map.of("clusterId", mirror.text(mirror.field(authority, "clusterId", "Ljava/lang/String;")),
                "executionGeneration", mirror.integral(mirror.field(authority, "executionGeneration", "J")),
                "claim", claimed == null ? Map.of() : workloadFence(claimed, mirror)));
        ObjectReference successor = requiredObject(mirror.field(marker, "successor", "Lio/tapstate/spi/store/StopReservation$Successor;"),
                "io.tapstate.spi.store.StopReservation$Successor", mirror);
        Map<String, Object> slot = new LinkedHashMap<>();
        slot.put("scope", mirror.scope(mirror.field(successor, "scope", SCOPE)));
        slot.put("submissionBootId", mirror.text(mirror.field(successor, "submissionBootId", "Ljava/lang/String;")));
        Value job = mirror.field(successor, "job", "Lio/tapstate/spi/store/StopReservation$JobIdentity;");
        slot.put("job", job == null ? null : mirror.job(job));
        result.put("successor", Collections.unmodifiableMap(slot));
        return Collections.unmodifiableMap(result);
    }

    private static Map<String, Object> workloadFence(Value value, NativeTelemetryMirror mirror) throws Exception {
        ObjectReference fence = requiredObject(value, "io.tapstate.spi.store.WorkloadClaimFence", mirror);
        ObjectReference key = requiredObject(mirror.field(fence, "key", "Lio/tapstate/spi/store/WorkloadClaimKey;"),
                "io.tapstate.spi.store.WorkloadClaimKey", mirror);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", Map.of("clusterId", mirror.text(mirror.field(key, "clusterId", "Ljava/lang/String;")),
                "resourceId", mirror.text(mirror.field(key, "resourceId", "Ljava/lang/String;")),
                "type", mirror.enumName(mirror.field(key, "type", "Lio/tapstate/spi/store/WorkloadClaimType;"))));
        result.put("owner", workloadOwner(mirror.field(fence, "owner", "Lio/tapstate/spi/store/WorkloadOwner;"), mirror));
        for (String field : List.of("claimGeneration", "executionGeneration", "topologyRevision")) {
            result.put(field, mirror.integral(mirror.field(fence, field, "J")));
        }
        return Map.copyOf(result);
    }

    private void qualifyReplacement(Map<String, Object> fence, Map<String, Object> claim,
            Map<String, Object> reservation, Map<String, Object> ownerEntry) throws NativeTelemetryMirror.Unavailable {
        Map<?, ?> slot = (Map<?, ?>) reservation.get("successor");
        Map<?, ?> scope = (Map<?, ?>) slot.get("scope");
        Map<?, ?> writer = (Map<?, ?>) reservation.get("writerAuthority");
        Map<?, ?> source = (Map<?, ?>) reservation.get("source");
        Map<?, ?> intent = (Map<?, ?>) reservation.get("originalDesired");
        String token = (String) reservation.get("token");
        if (!pipeline.equals(reservation.get("pipelineId")) || !pipeline.equals(intent.get("pipelineId"))
                || !"RUNNING".equals(intent.get("targetState")) || !"SUCCESSOR_ADMITTED".equals(reservation.get("phase"))
                || ((Number) reservation.get("formatVersion")).longValue() != 16
                || !List.of("CONTINUE", "RESET").contains(reservation.get("counterPolicy"))
                || token.isBlank() || token.length() > 256 || ((Number) reservation.get("sourceEpoch")).longValue() < 0
                || ((Number) reservation.get("reservedEpoch")).longValue() <= ((Number) reservation.get("sourceEpoch")).longValue()
                || !fence.get("executionGeneration").equals(scope.get("generation"))
                || !fence.get("executionGeneration").equals(writer.get("executionGeneration"))
                || !source.get("clusterId").equals(writer.get("clusterId"))
                || !Objects.equals(ownerEntry.get("clusterId"), writer.get("clusterId"))
                || slot.get("job") != null || ((String) slot.get("submissionBootId")).isBlank()
                || source.get("scope") instanceof Map<?, ?> old && !old.get("incarnation").equals(scope.get("incarnation"))) {
            throw new NativeTelemetryMirror.Unavailable("REPLACEMENT_RESERVATION_EXECUTION_MISMATCH");
        }
        Map<?, ?> authorityClaim = (Map<?, ?>) writer.get("claim");
        if (claim.isEmpty()) {
            if (!authorityClaim.isEmpty()) { throw new NativeTelemetryMirror.Unavailable("STANDALONE_REPLACEMENT_HAS_A_CLAIM"); }
        } else {
            for (String key : List.of("key", "owner", "claimGeneration", "executionGeneration", "topologyRevision")) {
                if (!Objects.equals(claim.get(key), authorityClaim.get(key))) {
                    throw new NativeTelemetryMirror.Unavailable("REPLACEMENT_ADVANCED_CLAIM_AUTHORITY_MISMATCH");
                }
            }
            if (!writer.get("clusterId").equals(((Map<?, ?>) claim.get("key")).get("clusterId"))) {
                throw new NativeTelemetryMirror.Unavailable("REPLACEMENT_ADVANCED_CLAIM_CLUSTER_MISMATCH");
            }
        }
    }

    private static Map<String, Object> workloadOwner(Value value, NativeTelemetryMirror mirror) throws Exception {
        ObjectReference owner = requiredObject(value, "io.tapstate.spi.store.WorkloadOwner", mirror);
        String node = mirror.text(mirror.field(owner, "nodeId", "Ljava/lang/String;"));
        String boot = mirror.text(mirror.field(owner, "bootId", "Ljava/lang/String;"));
        if (node.isBlank() || boot.isBlank()) { throw new NativeTelemetryMirror.Unavailable("EMPTY_CLAIM_OWNER"); }
        return Map.of("nodeId", node, "bootId", boot);
    }

    private static Map<String, Object> executionFence(Value value, NativeTelemetryMirror mirror) throws Exception {
        ObjectReference fence = requiredObject(value, "io.tapstate.app.ExecutionFence", mirror);
        String pipeline = mirror.text(mirror.field(fence, "pipelineId", "Ljava/lang/String;"));
        long claimed = mirror.integral(mirror.field(fence, "claimGeneration", "J"));
        long execution = mirror.integral(mirror.field(fence, "executionGeneration", "J"));
        if (pipeline.isBlank() || claimed < 0 || execution < 1) {
            throw new NativeTelemetryMirror.Unavailable("INVALID_ADMISSION_FENCE");
        }
        return Map.of("pipelineId", pipeline, "claimGeneration", claimed, "executionGeneration", execution);
    }

    private static List<String> executionMembers(Value value, NativeTelemetryMirror mirror) throws Exception {
        List<String> names = new ArrayList<>();
        for (Value item : mirror.sequence(value)) {
            String name = mirror.text(item);
            if (name.isBlank() || names.contains(name)) {
                throw new NativeTelemetryMirror.Unavailable("INVALID_ADMITTED_MEMBER_SET");
            }
            names.add(name);
        }
        names.sort(String::compareTo);
        return List.copyOf(names);
    }

    private static Map<String, Object> workloadClaim(Value value, NativeTelemetryMirror mirror) throws Exception {
        ObjectReference claim = requiredObject(value, "io.tapstate.spi.store.WorkloadClaim", mirror);
        ObjectReference key = requiredObject(mirror.field(claim, "key", "Lio/tapstate/spi/store/WorkloadClaimKey;"),
                "io.tapstate.spi.store.WorkloadClaimKey", mirror);
        String cluster = mirror.text(mirror.field(key, "clusterId", "Ljava/lang/String;"));
        String resource = mirror.text(mirror.field(key, "resourceId", "Ljava/lang/String;"));
        String type = mirror.enumName(mirror.field(key, "type", "Lio/tapstate/spi/store/WorkloadClaimType;"));
        if (cluster.isBlank() || resource.isBlank()) { throw new NativeTelemetryMirror.Unavailable("EMPTY_CLAIM_KEY"); }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", Map.of("clusterId", cluster, "resourceId", resource, "type", type));
        result.put("owner", workloadOwner(mirror.field(claim, "owner", "Lio/tapstate/spi/store/WorkloadOwner;"), mirror));
        for (String field : List.of("claimGeneration", "executionGeneration", "topologyRevision",
                "contextExecutionGeneration", "executionClaimGeneration", "failureClaimGeneration")) {
            result.put(field, mirror.integral(mirror.field(claim, field, "J")));
        }
        result.put("leaseUntil", mirror.scalar(mirror.field(claim, "leaseUntil", "Ljava/time/Instant;")));
        result.put("executionNodeIds", executionMembers(mirror.field(claim, "executionNodeIds", "Ljava/util/Set;"), mirror));
        result.put("failureAfterMemberLoss", mirror.scalar(mirror.field(claim, "failureAfterMemberLoss", "Z")));
        return Map.copyOf(result);
    }

    private void qualifyAdmission(Map<String, Object> fence, List<String> members, Map<String, Object> claim,
            boolean claimed, Map<String, Object> entry) throws NativeTelemetryMirror.Unavailable {
        if (!pipeline.equals(fence.get("pipelineId"))) {
            throw new NativeTelemetryMirror.Unavailable("ADMISSION_NAMES_ANOTHER_PIPELINE");
        }
        long claimGeneration = ((Number) fence.get("claimGeneration")).longValue();
        if (!claimed) {
            if (claimGeneration != 0 || !claim.isEmpty() || !members.isEmpty()) {
                throw new NativeTelemetryMirror.Unavailable("STANDALONE_ADMISSION_HAS_CLAIMED_CONTEXT");
            }
            return;
        }
        if (claim.isEmpty() || !(claim.get("key") instanceof Map<?, ?> key)
                || claimGeneration < 1 || !"PIPELINE_ACTUATION".equals(key.get("type"))
                || !pipeline.equals(key.get("resourceId")) || !entry.get("clusterId").equals(key.get("clusterId"))
                || !Objects.equals(entry.get("owner"), claim.get("owner"))
                || !fence.get("claimGeneration").equals(claim.get("claimGeneration"))
                || !fence.get("executionGeneration").equals(claim.get("executionGeneration"))
                || !claim.get("executionGeneration").equals(claim.get("contextExecutionGeneration"))
                || !claim.get("claimGeneration").equals(claim.get("executionClaimGeneration"))
                || ((Number) claim.get("topologyRevision")).longValue() < 0
                || members.isEmpty() || !members.equals(claim.get("executionNodeIds"))
                || ((Number) claim.get("failureClaimGeneration")).longValue() != 0
                || !Boolean.FALSE.equals(claim.get("failureAfterMemberLoss"))) {
            throw new NativeTelemetryMirror.Unavailable("CLAIMED_ADMISSION_RETURN_MISMATCH");
        }
    }

    /** Decode the whole immutable frame before replacing duplicate sweep payload with a complete proof. */
    private Object cachedProof(ObjectReference producer, NativeTelemetryMirror mirror) throws Exception {
        Value value = mirror.lookup(mirror.field(producer, "latest", "Ljava/util/Map;"), pipeline);
        if (value == null) { return "ABSENT"; }
        ObjectReference reference = mirror.object(value);
        return offeredProof(reference, offered(reference, mirror));
    }

    private Map<String, Object> offeredProof(ObjectReference reference, Map<String, Object> complete) throws Exception {
        if (evidenceBytes(complete, 0) > MAX_RECORD_BYTES) {
            throw new NativeTelemetryMirror.Unavailable("COMPLETE_FRAME_BYTE_BUDGET");
        }
        List<?> facts = (List<?>) complete.get("facts");
        long points = 0;
        for (Object value : facts) { points += ((List<?>) ((Map<?, ?>) value).get("points")).size(); }
        Map<String, Object> proof = new LinkedHashMap<>();
        proof.put("scope", complete.get("scope")); proof.put("observedAt", complete.get("observedAt"));
        proof.put("state", complete.get("state")); proof.put("offeredReference", reference.uniqueID());
        proof.put("completeFrame", true); proof.put("factCount", facts.size()); proof.put("pointCount", points);
        proof.put("frameSha256", hash(JsonWriter.write(canonicalFrame(complete, 0)).getBytes(StandardCharsets.UTF_8)));
        return Collections.unmodifiableMap(proof);
    }

    /** Map ordering is canonical; fact/point/bucket order, nulls and every decoded value are retained. */
    private static Object canonicalFrame(Object value, int depth) {
        if (depth > 16) { throw invalid("complete-frame nesting budget exceeded"); }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> ordered = new TreeMap<>();
            for (var item : map.entrySet()) {
                if (!(item.getKey() instanceof String key)) { throw invalid("complete-frame key is not a string"); }
                ordered.put(key, canonicalFrame(item.getValue(), depth + 1));
            }
            return ordered;
        }
        if (value instanceof List<?> values) {
            List<Object> ordered = new ArrayList<>(values.size());
            for (Object item : values) { ordered.add(canonicalFrame(item, depth + 1)); }
            return ordered;
        }
        if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean) { return value; }
        throw invalid("unmapped complete-frame evidence type");
    }

    private Map<String, Object> offered(ObjectReference offered, NativeTelemetryMirror mirror) throws Exception {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("scope", mirror.scope(mirror.field(offered, "scope", "Lio/tapstate/spi/metrics/MetricsExport$ScopeToken;")));
        out.put("observedAt", mirror.scalar(mirror.field(offered, "observedAt", "Ljava/time/Instant;")));
        out.put("state", mirror.enumName(mirror.field(offered, "state", "Lio/tapstate/core/lifecycle/PipelineState;")));
        out.put("facts", facts(mirror.field(offered, "facts", "Ljava/util/List;"), mirror));
        return out;
    }

    private List<Map<String, Object>> facts(Value values, NativeTelemetryMirror mirror) throws Exception {
        List<Map<String, Object>> facts = new ArrayList<>();
        for (Value value : mirror.sequence(values)) {
            ObjectReference fact = mirror.object(value);
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("name", mirror.text(mirror.field(fact, "name", "Ljava/lang/String;")));
            out.put("unit", mirror.text(mirror.field(fact, "unit", "Ljava/lang/String;")));
            out.put("type", mirror.enumName(mirror.field(fact, "type", "Lio/tapstate/core/lifecycle/MetricType;")));
            List<Map<String, Object>> points = new ArrayList<>();
            for (Value pointValue : mirror.sequence(mirror.field(fact, "points", "Ljava/util/List;"))) {
                ObjectReference point = mirror.object(pointValue);
                Map<String, Object> decoded = new LinkedHashMap<>();
                decoded.put("attributes", mirror.strings(mirror.field(point, "attributes", "Ljava/util/Map;")));
                decoded.put("startTime", mirror.scalar(mirror.field(point, "startTime", "Ljava/time/Instant;")));
                decoded.put("observedAt", mirror.scalar(mirror.field(point, "observedAt", "Ljava/time/Instant;")));
                Value numeric = mirror.field(point, "value", "Ljava/lang/Long;");
                decoded.put("value", numeric == null ? null : mirror.integral(numeric));
                Value histogram = mirror.field(point, "histogram", "Lio/tapstate/core/lifecycle/HistogramValue;");
                if (histogram != null) {
                    ObjectReference data = mirror.object(histogram);
                    decoded.put("histogram", Map.of("count", mirror.integral(mirror.field(data, "count", "J")),
                            "sum", mirror.scalar(mirror.field(data, "sum", "D")),
                            "bounds", mirror.scalars(mirror.field(data, "bounds", "Ljava/util/List;")),
                            "buckets", mirror.scalars(mirror.field(data, "bucketCounts", "Ljava/util/List;"))));
                }
                points.add(Collections.unmodifiableMap(decoded));
            }
            out.put("points", List.copyOf(points)); facts.add(Map.copyOf(out));
        }
        return List.copyOf(facts);
    }

    private Map<String, Object> account(ObjectReference publisher, NativeTelemetryMirror mirror) throws Exception {
        Value scopes = mirror.field(publisher, "currentScopes", "Ljava/util/Map;");
        Value tokens = mirror.field(publisher, "accountTokens", "Ljava/util/Map;");
        Value scope = mirror.lookup(scopes, pipeline), token = mirror.lookup(tokens, pipeline);
        if (scope == null || token == null) { throw new NativeTelemetryMirror.Unavailable("ACCOUNT_NOT_REGISTERED"); }
        long tokenId = mirror.object(token).uniqueID();
        Map<String, Object> owner = mirror.scope(scope);
        Value counts = mirror.lookup(mirror.field(publisher, "failuresByPipelineAndCode", "Ljava/util/Map;"), pipeline);
        Value current = mirror.lookup(mirror.field(publisher, "currentFailures", "Ljava/util/Map;"), pipeline);
        Value since = mirror.lookup(mirror.field(publisher, "failureCountingSinceByPipeline", "Ljava/util/Map;"), pipeline);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("scope", owner); out.put("token", tokenId);
        out.put("counts", counts == null ? Map.of() : mirror.strings(counts));
        out.put("countingSince", mirror.scalar(since));
        out.put("currentFailureCode", current == null ? null : mirror.text(
                mirror.field(mirror.object(current), "code", "Ljava/lang/String;")));
        out.put("named", folderNames(mirror.object(mirror.field(publisher, "cardinality",
                "Lio/tapstate/core/lifecycle/CardinalityBudget$Folder;")), mirror));
        Value afterScope = mirror.lookup(scopes, pipeline), afterToken = mirror.lookup(tokens, pipeline);
        if (afterScope == null || afterToken == null || mirror.object(afterToken).uniqueID() != tokenId
                || !mirror.scope(afterScope).equals(owner)) { throw new NativeTelemetryMirror.Unavailable("ACCOUNT_CHANGED_DURING_READ"); }
        return Collections.unmodifiableMap(out);
    }

    private Map<String, Object> accountPresence(ObjectReference publisher, NativeTelemetryMirror mirror) throws Exception {
        Value scopes = mirror.field(publisher, "currentScopes", "Ljava/util/Map;");
        Value tokens = mirror.field(publisher, "accountTokens", "Ljava/util/Map;");
        Value scope = mirror.lookup(scopes, pipeline), token = mirror.lookup(tokens, pipeline);
        if (scope != null && token != null) {
            Map<String, Object> present = new LinkedHashMap<>(account(publisher, mirror));
            present.put("presence", "PRESENT");
            return Collections.unmodifiableMap(present);
        }
        if (scope != null || token != null || mirror.lookup(mirror.field(publisher,
                "failuresByPipelineAndCode", "Ljava/util/Map;"), pipeline) != null
                || mirror.lookup(mirror.field(publisher, "currentFailures", "Ljava/util/Map;"), pipeline) != null
                || mirror.lookup(mirror.field(publisher, "failureCountingSinceByPipeline", "Ljava/util/Map;"), pipeline) != null) {
            throw new NativeTelemetryMirror.Unavailable("PARTIAL_ACCOUNT_ABSENCE");
        }
        Map<String, Object> named = folderNames(mirror.object(mirror.field(publisher, "cardinality",
                "Lio/tapstate/core/lifecycle/CardinalityBudget$Folder;")), mirror);
        if (mirror.lookup(scopes, pipeline) != null || mirror.lookup(tokens, pipeline) != null) {
            throw new NativeTelemetryMirror.Unavailable("ACCOUNT_CHANGED_DURING_ABSENCE_READ");
        }
        return Map.of("presence", "ABSENT", "named", named);
    }

    private Map<String, Object> folderNames(ObjectReference folder, NativeTelemetryMirror mirror) throws Exception {
        Map<String, Object> out = new LinkedHashMap<>();
        for (NativeTelemetryMirror.Pair instrument : mirror.map(mirror.field(folder, "named", "Ljava/util/Map;"))) {
            Value names = mirror.lookup(instrument.value(), pipeline);
            if (names != null) { out.put(mirror.text(instrument.key()), mirror.scalars(names)); }
        }
        return Map.copyOf(out);
    }

    private Map<String, Object> exporterNames(ObjectReference producer, NativeTelemetryMirror mirror) throws Exception {
        Map<String, Object> out = new LinkedHashMap<>();
        for (NativeTelemetryMirror.Pair instrument : mirror.map(mirror.field(producer, "named", "Ljava/util/Map;"))) {
            List<Map<String, Object>> own = new ArrayList<>();
            for (Value attributes : mirror.sequence(instrument.value())) {
                Map<String, Object> values = mirror.strings(attributes);
                if (pipeline.equals(values.get("tapstate.pipeline.id"))) { own.add(values); }
            }
            if (!own.isEmpty()) { out.put(mirror.text(instrument.key()), List.copyOf(own)); }
        }
        return Map.copyOf(out);
    }

    private List<Map<String, Object>> metrics(Value values, NativeTelemetryMirror mirror) throws Exception {
        List<Map<String, Object>> metrics = new ArrayList<>();
        for (Value value : mirror.sequence(values)) {
            ObjectReference metric = mirror.object(value);
            String name = mirror.text(mirror.field(metric, "name", "Ljava/lang/String;"));
            if (!name.startsWith("tapstate.pipeline.")) { continue; }
            String type = mirror.enumName(mirror.field(metric, "type", "Lio/opentelemetry/sdk/metrics/data/MetricDataType;"));
            ObjectReference data = mirror.object(mirror.field(metric, "data", "Lio/opentelemetry/sdk/metrics/data/Data;"));
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("name", name); out.put("type", type); out.put("unit", mirror.text(mirror.field(metric, "unit", "Ljava/lang/String;")));
            List<Map<String, Object>> points = new ArrayList<>();
            for (Value pointValue : mirror.sequence(mirror.field(data, "points", "Ljava/util/Collection;"))) {
                ObjectReference point = mirror.object(pointValue);
                boolean histogram = type.equals("HISTOGRAM");
                String prefix = histogram ? "get" : "";
                String start = histogram ? "getStartEpochNanos" : "startEpochNanos";
                String epoch = histogram ? "getEpochNanos" : "epochNanos";
                Value rawAttributes = mirror.field(point, histogram ? "getAttributes" : "attributes",
                        "Lio/opentelemetry/api/common/Attributes;");
                Map<String, Object> attributes = attributes(mirror.object(rawAttributes), mirror);
                if (!pipeline.equals(attributes.get("tapstate.pipeline.id"))) { continue; }
                Map<String, Object> decoded = new LinkedHashMap<>();
                decoded.put("attributes", attributes);
                decoded.put("startEpochNanos", mirror.integral(mirror.field(point, start, "J")));
                decoded.put("epochNanos", mirror.integral(mirror.field(point, epoch, "J")));
                if (histogram) {
                    decoded.put("count", mirror.integral(mirror.field(point, prefix + "Count", "J")));
                    decoded.put("sum", mirror.scalar(mirror.field(point, prefix + "Sum", "D")));
                    decoded.put("bounds", mirror.scalars(mirror.field(point, "getBoundaries", "Ljava/util/List;")));
                    decoded.put("buckets", mirror.scalars(mirror.field(point, "getCounts", "Ljava/util/List;")));
                } else {
                    if (!type.equals("LONG_SUM") && !type.equals("LONG_GAUGE")) {
                        throw new NativeTelemetryMirror.Unavailable("SDK_POINT_TYPE:" + type);
                    }
                    decoded.put("value", mirror.integral(mirror.field(point, "value", "J")));
                }
                points.add(Map.copyOf(decoded));
            }
            if (!points.isEmpty()) {
                out.put("points", List.copyOf(points));
                if (type.equals("LONG_SUM")) {
                    out.put("monotonic", mirror.scalar(mirror.field(data, "monotonic", "Z")));
                    out.put("temporality", mirror.enumName(mirror.field(data, "aggregationTemporality",
                            "Lio/opentelemetry/sdk/metrics/data/AggregationTemporality;")));
                }
                metrics.add(Map.copyOf(out));
            }
        }
        return List.copyOf(metrics);
    }

    private Map<String, Object> attributes(ObjectReference attributes, NativeTelemetryMirror mirror) throws Exception {
        Value data = mirror.field(attributes, "data", "[Ljava/lang/Object;");
        if (!(data instanceof ArrayReference array) || array.length() > 128 || (array.length() & 1) != 0) {
            throw new NativeTelemetryMirror.Unavailable("SDK_ATTRIBUTE_LAYOUT");
        }
        List<Value> pairs = array.getValues();
        Map<String, Object> values = new LinkedHashMap<>();
        for (int index = 0; index < pairs.size(); index += 2) {
            String key = mirror.text(mirror.field(mirror.object(pairs.get(index)), "key", "Ljava/lang/String;"));
            if (values.putIfAbsent(key, mirror.scalar(pairs.get(index + 1))) != null) {
                throw new NativeTelemetryMirror.Unavailable("DUPLICATE_SDK_ATTRIBUTE");
            }
        }
        return Map.copyOf(values);
    }

    private void exception(ExceptionEvent event) throws Exception {
        ThreadState state = threads.get(event.thread().uniqueID());
        if (state == null) { throw invalid("exception lost observed thread"); }
        List<StackFrame> frames = frames(event.thread());
        reconcile(state, frames, null);
        if (state.calls.isEmpty()) { return; }
        List<Integer> candidates = new ArrayList<>();
        if (event.catchLocation() == null) { candidates.add(frames.size()); }
        else {
            for (int index = 0; index < frames.size(); index++) {
                if (frames.get(index).location().method().equals(event.catchLocation().method())) { candidates.add(index); }
            }
            if (candidates.isEmpty()) { throw invalid("exception catch frame unavailable"); }
        }
        for (Call call : state.calls) {
            int index = frames.size() - call.depth;
            boolean outside = index < candidates.getFirst();
            if (candidates.stream().anyMatch(candidate -> (index < candidate) != outside)) {
                throw invalid("ambiguous exception unwind");
            }
            if (outside) { call.escaping = true; }
        }
    }

    private void reconcile(ThreadState state, List<StackFrame> frames, Target newEntry) {
        while (!state.calls.isEmpty() && state.calls.peek().escaping) {
            Call call = state.calls.peek();
            boolean present = present(call, frames);
            if (present && !(newEntry == call.spec.target() && frames.size() == call.depth)) { break; }
            if (ownedCrashRequested) { exceptional(call); state.calls.pop(); }
            else { state.calls.pop(); exceptional(call); }
        }
        for (Call call : state.calls) { if (!present(call, frames)) { throw invalid("call vanished without observed unwind"); } }
        if (newEntry == null) { removeEmpty(state); }
    }
    private boolean present(Call call, List<StackFrame> frames) {
        int index = frames.size() - call.depth;
        return index >= 0 && index < frames.size() && frames.get(index).location().method().equals(methods.get(call.spec.target()));
    }
    private void exceptional(Call call) {
        if (call.exit != null) { vm.eventRequestManager().deleteEventRequest(call.exit); call.exit = null; }
        if (call.qualified) { totals.get(call.spec.target()).exceptional++; }
        if (call.logCalibration) { unknown("FAMILY_CALIBRATION:LOG:EXCEPTIONAL_EXIT"); }
    }
    private void threadDeath(ThreadDeathEvent event) {
        ThreadState state = threads.get(event.thread().uniqueID());
        if (state == null) { return; }
        if (ownedCrashRequested) { return; }
        while (!state.calls.isEmpty()) {
            Call call = state.calls.pop();
            if (!call.escaping) { throw invalid("thread death lost an unaccounted call"); }
            exceptional(call);
        }
        removeEmpty(state);
    }
    private void removeEmpty(ThreadState state) {
        if (state.calls.isEmpty()) {
            vm.eventRequestManager().deleteEventRequest(state.exceptions);
            vm.eventRequestManager().deleteEventRequest(state.death);
            threads.remove(state.thread.uniqueID());
        }
    }
    private List<StackFrame> frames(ThreadReference thread) throws Exception {
        List<StackFrame> frames = thread.frames();
        if (frames.isEmpty() || frames.size() > MAX_FRAMES) { throw invalid("stack unavailable or unbounded"); }
        return frames;
    }
    private void unknown(String reason) {
        if (unverified.size() >= 64 && !unverified.contains(reason)) { throw invalid("unverified-reason budget exceeded"); }
        unverified.add(reason);
    }
    private void decoderUnavailable(Map<String, Object> record, Target target, String stage, String reason) {
        String context = stage + ":" + target.name() + ":" + reason;
        unknown(context);
        List<String> failures = new ArrayList<>();
        if (record.get("decoderReasons") instanceof List<?> previous) {
            for (Object value : previous) {
                if (!(value instanceof String text)) { throw invalid("decoder reason layout changed"); }
                failures.add(text);
            }
        }
        if (failures.size() >= 2) { throw invalid("decoder reason budget exceeded"); }
        failures.add(context);
        record.put("decoderStatus", "UNVERIFIED");
        record.put("decoderReason", reason);
        record.put("decoderStage", stage);
        record.put("decoderTarget", target.name());
        record.put("decoderReasons", List.copyOf(failures));
    }
    private void addRecord(Map<String, Object> record) {
        if (records.size() >= MAX_RECORDS) { throw invalid("phase record budget exceeded"); }
        long bytes = evidenceBytes(record, 0);
        long recordLimit = Target.PRODUCE.name().equals(record.get("target"))
                ? MAX_PRODUCE_RECORD_BYTES : MAX_RECORD_BYTES;
        if (bytes > recordLimit || phaseBytes + bytes > MAX_PHASE_BYTES) {
            throw invalid("phase or record byte budget exceeded: target=" + record.get("target")
                    + ", recordBytes=" + bytes + ", maxRecordBytes=" + recordLimit
                    + ", phaseBytes=" + phaseBytes + ", maxPhaseBytes=" + MAX_PHASE_BYTES
                    + ", retainedRecords=" + records.size()
                    + ", callerBytes=" + evidenceBytes(record.get("callers"), 0)
                    + ", messageBytes=" + evidenceBytes(record.get("message"), 0));
        }
        phaseBytes += bytes;
        records.add(Collections.unmodifiableMap(new LinkedHashMap<>(record)));
    }
    private Boundary snapshot(String phase, boolean drained) throws Exception {
        return snapshot(phase, drained, BindingProfile.FULL);
    }

    private Boundary snapshot(String phase, boolean drained, BindingProfile profile) throws Exception {
        boolean deferSubmit = false;
        if (profile == BindingProfile.RESTORED_BEFORE_FIRST_START) {
            if (restoredBeforeStartFinished) { throw invalid("restored binding profile cannot follow the first START"); }
            for (Target target : List.of(Target.ADMISSION, Target.SUBMIT)) {
                Totals total = totals.get(target);
                if (total.entries != 0 || total.normal != 0 || total.exceptional != 0
                        || threads.values().stream().flatMap(state -> state.calls.stream())
                                .anyMatch(call -> call.spec.target() == target)) {
                    throw invalid("restored binding profile cannot follow real admission or submission activity");
                }
            }
            Spec submit = specs.get(Target.SUBMIT);
            // ClassPrepare remains armed. A prepared but unbound class is still an unverified gap.
            deferSubmit = submit != null && !bindings.containsKey(Target.SUBMIT)
                    && vm.classesByName(submit.type()).stream().noneMatch(ReferenceType::isPrepared);
        }
        Map<Target, Counts> counts = new EnumMap<>(Target.class);
        Set<String> missing = new LinkedHashSet<>(unverified);
        for (Target target : Target.values()) {
            if (target == Target.REPLACEMENT_ADMISSION && !replacementObservationEnabled
                    || target == Target.RAW_CONTINUATION && !rawContinuationObservationEnabled
                    || target == Target.JOINED_TAKEOVER && !joinedTakeoverObservationEnabled) { continue; }
            if (!bindings.containsKey(target)) {
                if (target != Target.SUBMIT || !deferSubmit) { missing.add("LIVE_BINDING_UNAVAILABLE:" + target); }
            } else if (!disconnected) {
                Method method = methods.get(target);
                Spec spec = specs.get(target);
                if (method.isObsolete() || !Arrays.equals(method.bytecodes(),
                        images.get(spec.type()).methods().get(spec.method() + spec.descriptor()))) {
                    throw invalid("method provenance changed at boundary");
                }
            }
            Totals total = totals.get(target);
            long open = threads.values().stream().flatMap(state -> state.calls.stream())
                    .filter(call -> call.qualified && call.spec.target() == target).count();
            if (total.entries != total.normal + total.exceptional + open) { throw invalid("call accounting mismatch"); }
            counts.put(target, new Counts(total.entries, total.normal, total.exceptional, open));
        }
        if (authorities.isEmpty()) { missing.add("AUTHORITY_RECEIPTS_UNVERIFIED"); }
        for (Map<String, Object> record : records) { qualifyScopes(record, missing, 0); }
        if (!logFamilyCalibration.isEmpty()
                && evidenceBytes(Map.of("records", records, "logFamilyCalibration", logFamilyCalibration), 0) > MAX_PHASE_BYTES) {
            throw invalid("log calibration and retained records exceed the existing phase budget");
        }
        return new Boundary(phase, ++sequence, sha, pipeline, bindings, counts, records, authorities,
                missing, layouts, logFamilyCalibration, vmVersion, events, handlingNanos,
                threads.values().stream().mapToInt(state -> state.calls.size()).sum(), drained, vmDeath, disconnected, profile,
                deferSubmit ? Set.of(Target.SUBMIT) : Set.of(),
                replacementObservationEnabled, rawContinuationObservationEnabled, joinedTakeoverObservationEnabled);
    }
    private void qualifyScopes(Object value, Set<String> missing, int depth) {
        if (depth > 16) { throw invalid("scope qualification depth exceeded"); }
        if (value instanceof Map<?, ?> map) {
            for (var entry : map.entrySet()) {
                if ((entry.getKey().equals("scope") || entry.getKey().equals("requestScope"))
                        && entry.getValue() instanceof Map<?, ?> scope
                        && scope.containsKey("incarnation") && scope.containsKey("generation")
                        && authorities.stream().noneMatch(receipt -> receipt.scope().equals(scope))) {
                    missing.add("OBSERVED_SCOPE_HAS_NO_AUTHORITY_RECEIPT");
                }
                qualifyScopes(entry.getValue(), missing, depth + 1);
            }
        } else if (value instanceof Collection<?> values) {
            for (Object item : values) { qualifyScopes(item, missing, depth + 1); }
        }
    }
    private static long evidenceBytes(Object value, int depth) {
        if (depth > 16) { throw invalid("evidence nesting budget exceeded"); }
        if (value == null || value instanceof Number || value instanceof Boolean) { return 16; }
        if (value instanceof String text) { return 4L * text.length() + 16; }
        long bytes = 32;
        if (value instanceof Map<?, ?> map) {
            for (var entry : map.entrySet()) {
                bytes += evidenceBytes(entry.getKey(), depth + 1) + evidenceBytes(entry.getValue(), depth + 1);
                if (bytes > MAX_PHASE_BYTES) { return bytes; }
            }
            return bytes;
        }
        if (value instanceof Collection<?> values) {
            for (Object item : values) {
                bytes += evidenceBytes(item, depth + 1);
                if (bytes > MAX_PHASE_BYTES) { return bytes; }
            }
            return bytes;
        }
        throw invalid("unmapped immutable evidence type");
    }

    private void fail(Throwable problem) {
        AssertionError error = problem instanceof AssertionError assertion ? assertion
                : invalid("capture failed: " + problem.getClass().getName());
        if (error != problem) { error.initCause(problem); }
        failure.compareAndSet(null, error); running = false;
        error = failure.get();
        if (error != problem && error.getCause() != problem) { append(error, problem); }
        CompletableFuture<Boundary> waiting = command;
        if (waiting != null) { waiting.completeExceptionally(error); }
        detach(error);
    }
    private Throwable detach(Throwable primary) {
        try { releasePublisherSweepPark(sweepPark, "DETACH"); }
        catch (Throwable problem) { primary = append(primary, problem); }
        synchronized (lock) { localDisposeStarted = true; }
        try { vm.dispose(); }
        catch (VMDisconnectedException gone) { }
        catch (Throwable problem) {
            primary = append(primary, problem);
            synchronized (lock) { if (ownedCrashRequested && server.isAlive()) { crashCleanupForcedKill = true; } }
            try { server.kill(); }
            catch (Throwable cleanup) { primary = append(primary, cleanup); }
        }
        return primary;
    }

    private static Throwable append(Throwable primary, Throwable next) {
        if (primary == null) { return next; }
        if (primary != next) { primary.addSuppressed(next); }
        return primary;
    }

    /** The caller marks its actual owned process immediately before calling server().kill(). */
    Map<String, Object> markOwnedCrash() {
        synchronized (commands) {
            synchronized (lock) {
                check();
                if (ownedCrashRequested || closing || closed || disconnected || command != null || !server.isAlive()
                        || sweepPark != null && !sweepPark.done) { throw invalid("owned crash cannot be marked now"); }
                crashOwnedPid = server.pid(); crashMarkedAtEvent = events; ownedCrashRequested = true;
                return Map.of("ownedPid", crashOwnedPid, "markedAtEvent", crashMarkedAtEvent,
                        "callerMustKillOwnedProcess", true);
            }
        }
    }

    /** No live target reads: preserve actual counters and entries even when SIGKILL interrupts decoding. */
    private Boundary crashSnapshot() {
        Map<Target, Counts> counts = new EnumMap<>(Target.class);
        Set<String> missing = new LinkedHashSet<>(unverified);
        missing.add("OWNED_CRASH_INCOMPLETE"); missing.add("LIVE_PROVENANCE_AT_CRASH_UNKNOWN");
        if (crashReadInterruption != null) { missing.add("DISCONNECT_INTERRUPTED_OBSERVATION:" + crashReadInterruption); }
        for (var target : totals.entrySet()) {
            if (!bindings.containsKey(target.getKey())) { missing.add("LIVE_BINDING_UNAVAILABLE:" + target.getKey()); }
            long open = threads.values().stream().flatMap(state -> state.calls.stream())
                    .filter(call -> call.qualified && call.spec.target() == target.getKey()).count();
            Totals total = target.getValue();
            if (total.entries != total.normal + total.exceptional + open) {
                missing.add("CRASH_CALL_ACCOUNTING_INCOMPLETE:" + target.getKey());
            }
            counts.put(target.getKey(), new Counts(total.entries, total.normal, total.exceptional, open));
        }
        if (authorities.isEmpty()) { missing.add("AUTHORITY_RECEIPTS_UNVERIFIED"); }
        for (Map<String, Object> record : records) { qualifyScopes(record, missing, 0); }
        return new Boundary("owned-crash", ++sequence, sha, pipeline, bindings, counts, records, authorities,
                missing, layouts, logFamilyCalibration, vmVersion, events, handlingNanos,
                threads.values().stream().mapToInt(state -> state.calls.size()).sum(), false, vmDeath, disconnected, BindingProfile.FULL,
                Set.of(), replacementObservationEnabled, rawContinuationObservationEnabled, joinedTakeoverObservationEnabled);
    }

    private List<Map<String, Object>> crashOpenCalls() {
        List<Map<String, Object>> open = new ArrayList<>();
        for (var thread : threads.entrySet()) {
            for (Call call : thread.getValue().calls) {
                open.add(Map.of("thread", thread.getKey(), "invocation", call.id, "target", call.spec.target().name(),
                        "depth", call.depth, "qualifiedAtEntry", call.qualified, "escapingObserved", call.escaping,
                        "returnRequestCreated", call.exit != null, "entry", call.entry, "logCalibration", call.logCalibration));
            }
        }
        if (open.size() > MAX_OPEN_CALLS) { throw invalid("crash open-call budget exceeded"); }
        if (evidenceBytes(Map.of("records", records, "unpairedCalls", open, "logFamilyCalibration", logFamilyCalibration), 0) > MAX_PHASE_BYTES) {
            throw invalid("crash retained evidence exceeds the existing phase byte budget");
        }
        return List.copyOf(open);
    }

    private static String problemText(Throwable problem) {
        return problem == null ? null : problem.getClass().getName() + ":" + problem.getMessage();
    }

    /** Fault substrate only; missing drain, interrupted decoding and prior observer errors remain explicit. */
    OwnedCrash finishOwnedCrash() throws Exception {
        synchronized (commands) {
            synchronized (lock) {
                if (!ownedCrashRequested) { throw invalid("owned crash was not marked before the caller kill"); }
                if (closed) {
                    if (crashTerminal == null) { throw invalid("owned crash evidence was not saved"); }
                    return crashTerminal;
                }
            }
            boolean deadBeforeCleanup = false, artifactUnchanged = false;
            Throwable cleanup = null, evidenceProblem = null;
            Boundary observed = null;
            List<Map<String, Object>> open = null;
            try {
                pump.join(5000);
                artifactUnchanged = sha.equals(hash(jar));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt(); evidenceProblem = interrupted;
            } catch (Exception | Error problem) { evidenceProblem = problem; }
            finally {
                deadBeforeCleanup = !server.isAlive();
                running = false;
                cleanup = detach(null);
                if (server.isAlive()) {
                    synchronized (lock) { crashCleanupForcedKill = true; }
                    try { server.kill(); } catch (Throwable problem) { cleanup = append(cleanup, problem); }
                }
                try { server.close(); } catch (Throwable problem) { cleanup = append(cleanup, problem); }
                pump.interrupt();
                try { pump.join(2000); }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt(); cleanup = append(cleanup, interrupted);
                }
                synchronized (lock) {
                    closed = true;
                    if (pump.isAlive()) {
                        cleanup = append(cleanup, invalid("owned event pump did not stop"));
                        evidenceProblem = append(evidenceProblem, invalid("crash evidence is not stable while the pump is alive"));
                    } else {
                        try { observed = crashSnapshot(); open = crashOpenCalls(); }
                        catch (Exception | Error problem) { evidenceProblem = append(evidenceProblem, problem); }
                    }
                    crashTerminal = new OwnedCrash(crashOwnedPid, crashMarkedAtEvent, deadBeforeCleanup,
                            !server.isAlive(), crashDisconnectBeforeDispose, crashDisconnectReceipt,
                            crashCleanupForcedKill, artifactUnchanged, !pump.isAlive(), observed, open,
                            problemText(evidenceProblem), problemText(failure.get()), problemText(cleanup));
                }
            }
            return crashTerminal;
        }
    }

    Boundary shutdownAndFinish() throws Exception {
        synchronized (commands) {
            if (ownedCrashRequested) { throw invalid("owned crash requires finishOwnedCrash"); }
            if (closed) { check(); if (terminal == null) { throw invalid("no qualified terminal boundary"); } return terminal; }
            Throwable primary = null;
            try {
                releasePublisherSweepPark(sweepPark, "OWNED_CLOSE");
                check();
                synchronized (lock) { closing = true; }
                server.close(); pump.join(5000); check();
                synchronized (lock) {
                    if (server.isAlive() || pump.isAlive() || !vmDeath || !disconnected || !threads.isEmpty()) {
                        throw invalid("owned VM death/disconnect/call drain incomplete");
                    }
                    if (!sha.equals(hash(jar))) { throw invalid("artifact changed during owned run"); }
                    terminal = snapshot("owned-shutdown", true);
                    if (!terminal.invocationDrainComplete()) { throw invalid("terminal calls did not drain"); }
                    return terminal;
                }
            } catch (Exception | Error problem) { primary = problem; fail(problem); throw problem; }
            finally {
                running = false;
                Throwable cleanup = detach(null);
                try { server.close(); }
                catch (Throwable problem) { cleanup = append(cleanup, problem); }
                pump.interrupt();
                try { pump.join(2000); }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    cleanup = append(cleanup, interrupted);
                } finally { closed = true; }
                if (pump.isAlive()) { cleanup = append(cleanup, invalid("owned event pump did not stop")); }
                if (cleanup != null) {
                    if (primary != null) { append(primary, cleanup); }
                    else if (cleanup instanceof Exception exception) { throw exception; }
                    else { throw (Error) cleanup; }
                }
            }
        }
    }
    @Override public void close() throws Exception {
        if (!closed) {
            if (ownedCrashRequested) {
                OwnedCrash crash = finishOwnedCrash();
                check();
                if (!crash.ownedFaultObserved() || !crash.pumpStopped() || !crash.artifactUnchanged()
                        || crash.evidenceFailure() != null || crash.cleanupFailure() != null) {
                    throw invalid("owned crash remains unqualified; retained crash evidence is available");
                }
            } else { shutdownAndFinish(); }
        }
    }

    private static Map<String, Image> images(Path jar, boolean joinedTakeoverObservationEnabled) throws Exception {
        Map<String, Image> images = new LinkedHashMap<>();
        Set<String> wanted = new HashSet<>(SPECS.stream()
                .filter(spec -> spec.target() != Target.JOINED_TAKEOVER || joinedTakeoverObservationEnabled)
                .map(Spec::type).toList());
        if (joinedTakeoverObservationEnabled) {
            wanted.addAll(List.of("io.tapstate.app.StoreBackedPipelineCaptureCoordinator",
                    "io.tapstate.app.StoreBackedPipelineCaptureCoordinator$JoinedReader",
                    "io.tapstate.app.StoreBackedPipelineCaptureCoordinator$PipelineRun",
                    "io.tapstate.app.StoreBackedPipelineCaptureCoordinator$LogOwnerState",
                    "io.tapstate.app.StoreBackedPipelineCaptureCoordinator$AdmittedLogOwner",
                    "io.tapstate.app.StoreBackedPipelineCaptureCoordinator$OpeningClaim",
                    "io.tapstate.app.StoreBackedPipelineCaptureCoordinator$CloseState",
                    "io.tapstate.app.CaptureOwnership$Permit", "io.tapstate.runtime.srs.CaptureRunSpec",
                    "io.tapstate.runtime.srs.CaptureRun", "io.tapstate.runtime.srs.CaptureId",
                    "io.tapstate.runtime.srs.MiningChainId", "io.tapstate.spi.capture.CaptureConfig",
                    "io.tapstate.core.model.PipelineNode", "io.tapstate.core.model.ReadMode"));
        }
        wanted.addAll(List.of(OTEL + "FactsMetricProducer$Offered", OTEL + "FactMetricData",
                "io.tapstate.app.EngineLifecycleActuator", "io.tapstate.app.PipelineActuationOwnership$Execution",
                "io.tapstate.app.ExecutionFence", "io.tapstate.spi.store.WorkloadClaim",
                "io.tapstate.spi.store.WorkloadClaimKey", "io.tapstate.spi.store.WorkloadOwner",
                "io.tapstate.spi.store.WorkloadClaimType",
                "io.tapstate.spi.store.SuccessorAdmission", "io.tapstate.spi.store.StopReservation",
                "io.tapstate.spi.store.StopReservation$Source", "io.tapstate.spi.store.StopReservation$Successor",
                "io.tapstate.spi.store.StopReservation$Phase", "io.tapstate.spi.store.StopReservation$CounterPolicy",
                "io.tapstate.spi.store.StopAuthority", "io.tapstate.spi.store.WorkloadClaimFence",
                "io.tapstate.core.lifecycle.DesiredState", "io.tapstate.core.lifecycle.Observation",
                "io.tapstate.runtime.scheduler.ObservationPublisher$Prepared",
                "io.tapstate.app.ObservationScopeRegistry$ActualTarget",
                "io.tapstate.app.ObservationScopeRegistry$ContinuationPublication",
                "io.tapstate.runtime.engine.Engine$ExecutionJob", "io.tapstate.spi.store.StopReservation$JobIdentity",
                "io.tapstate.spi.store.ObservationStore$Scope", "io.tapstate.core.logging.LogSink$Scope",
                "io.tapstate.core.logging.LogLine", "io.tapstate.spi.metrics.MetricsExport$ScopeToken",
                "io.tapstate.adapters.pdk.PdkCapturePort", "io.tapstate.adapters.pdk.PdkConnector",
                "io.tapstate.adapters.pdk.ConnectorLog", "io.tapstate.adapters.pdk.PdkSinkWriter",
                "io.tapstate.app.PipelineLogAppender", "io.tapstate.app.ConvergenceDriver",
                "io.tapstate.core.lifecycle.MetricFact", "io.tapstate.core.lifecycle.MetricPoint",
                "io.tapstate.core.lifecycle.MetricType", "io.tapstate.core.lifecycle.PipelineState",
                "io.tapstate.core.lifecycle.HistogramValue", "io.tapstate.core.lifecycle.ObservationFailure",
                "com.hazelcast.jet.impl.JobProxy", "com.hazelcast.jet.impl.AbstractJobProxy",
                "io.opentelemetry.sdk.metrics.data.MetricDataType",
                "io.opentelemetry.sdk.metrics.data.AggregationTemporality",
                "io.opentelemetry.sdk.metrics.internal.data.AutoValue_ImmutableSumData",
                "io.opentelemetry.sdk.metrics.internal.data.AutoValue_ImmutableGaugeData",
                "io.opentelemetry.sdk.metrics.internal.data.AutoValue_ImmutableLongPointData",
                "io.opentelemetry.sdk.metrics.internal.data.AutoValue_ImmutableHistogramData",
                "io.opentelemetry.sdk.metrics.internal.data.AutoValue_ImmutableHistogramPointData",
                "io.opentelemetry.api.common.ArrayBackedAttributes",
                "io.opentelemetry.api.internal.ImmutableKeyValuePairs",
                "io.opentelemetry.api.internal.InternalAttributeKeyImpl"));
        try (ZipFile boot = new ZipFile(jar.toFile())) {
            for (String type : wanted) {
                ZipEntry entry = boot.getEntry("BOOT-INF/classes/" + type.replace('.', '/') + ".class");
                if (entry != null) {
                    try (InputStream input = boot.getInputStream(entry)) { putImage(images, type, entry.getName(), input); }
                }
            }
            List<? extends ZipEntry> libraries = boot.stream().filter(entry ->
                    entry.getName().startsWith("BOOT-INF/lib/") && entry.getName().endsWith(".jar")).toList();
            if (libraries.size() > 512) { throw invalid("artifact library budget exceeded"); }
            for (ZipEntry library : libraries) {
                if (library.getSize() < 0 || library.getSize() > 128L * 1024 * 1024) { throw invalid("artifact library size exceeded"); }
                try (ZipInputStream nested = new ZipInputStream(boot.getInputStream(library))) {
                    ZipEntry entry; int entries = 0;
                    while ((entry = nested.getNextEntry()) != null) {
                        if (++entries > 100000) { throw invalid("artifact library entry budget exceeded"); }
                        if (!entry.getName().endsWith(".class")) { continue; }
                        String type = entry.getName().substring(0, entry.getName().length() - 6).replace('/', '.');
                        if (wanted.contains(type)) { putImage(images, type, library.getName() + "!/" + entry.getName(), nested); }
                    }
                }
            }
        }
        return Map.copyOf(images);
    }
    private static void putImage(Map<String, Image> images, String type, String origin, InputStream input) throws Exception {
        byte[] bytes = input.readNBytes(MAX_CLASS_BYTES + 1);
        if (bytes.length > MAX_CLASS_BYTES || images.containsKey(type)) { throw invalid("duplicate or oversized artifact class"); }
        try (DataInputStream data = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (data.readInt() != 0xcafebabe) { throw invalid("invalid class header"); }
            data.readUnsignedShort(); data.readUnsignedShort();
            String[] text = new String[data.readUnsignedShort()];
            for (int index = 1; index < text.length; index++) {
                int tag = data.readUnsignedByte();
                switch (tag) {
                    case 1 -> text[index] = data.readUTF();
                    case 3, 4, 9, 10, 11, 12, 17, 18 -> data.skipNBytes(4);
                    case 5, 6 -> { data.skipNBytes(8); index++; }
                    case 7, 8, 16, 19, 20 -> data.skipNBytes(2);
                    case 15 -> data.skipNBytes(3);
                    default -> throw invalid("unsupported constant-pool tag");
                }
            }
            data.skipNBytes(6); data.skipNBytes(data.readUnsignedShort() * 2L);
            Map<String, String> fields = new LinkedHashMap<>();
            int fieldCount = data.readUnsignedShort();
            for (int index = 0; index < fieldCount; index++) {
                data.readUnsignedShort();
                fields.put(text[data.readUnsignedShort()], text[data.readUnsignedShort()]);
                skipAttributes(data);
            }
            Map<String, byte[]> methods = new LinkedHashMap<>();
            int methodCount = data.readUnsignedShort();
            for (int index = 0; index < methodCount; index++) {
                data.readUnsignedShort(); String key = text[data.readUnsignedShort()] + text[data.readUnsignedShort()];
                int attributes = data.readUnsignedShort();
                for (int attribute = 0; attribute < attributes; attribute++) {
                    String name = text[data.readUnsignedShort()]; int length = data.readInt();
                    if (length < 0 || length > MAX_CLASS_BYTES) { throw invalid("class attribute budget exceeded"); }
                    byte[] value = data.readNBytes(length);
                    if (value.length != length) { throw invalid("truncated class attribute"); }
                    if (name.equals("Code")) {
                        try (DataInputStream code = new DataInputStream(new ByteArrayInputStream(value))) {
                            code.skipNBytes(4); int size = code.readInt();
                            if (size < 1 || size > 65535) { throw invalid("invalid method code size"); }
                            byte[] body = code.readNBytes(size);
                            if (body.length != size || methods.putIfAbsent(key, body) != null) { throw invalid("truncated or duplicate method"); }
                        }
                    }
                }
            }
            images.put(type, new Image(origin, Map.copyOf(methods), Map.copyOf(fields)));
        }
    }
    private static void skipAttributes(DataInputStream input) throws Exception {
        int count = input.readUnsignedShort();
        for (int index = 0; index < count; index++) {
            input.readUnsignedShort(); int size = input.readInt();
            if (size < 0 || size > MAX_CLASS_BYTES) { throw invalid("class field attribute budget exceeded"); }
            input.skipNBytes(size);
        }
    }
    private static String hash(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = Files.newInputStream(path)) {
            byte[] buffer = new byte[65536]; int length;
            while ((length = input.read(buffer)) != -1) { digest.update(buffer, 0, length); }
        }
        return HexFormat.of().formatHex(digest.digest());
    }
    private static String hash(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
    private static AssertionError invalid(String reason) { return new AssertionError("Invalid passive telemetry witness: " + reason); }
}
