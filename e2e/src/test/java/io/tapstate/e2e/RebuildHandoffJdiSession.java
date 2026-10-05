package io.tapstate.e2e;

import com.sun.jdi.*;
import com.sun.jdi.connect.Connector;
import com.sun.jdi.connect.ListeningConnector;
import com.sun.jdi.event.*;
import com.sun.jdi.request.EventRequest;
import com.sun.jdi.request.MethodExitRequest;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StopReservation;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/** Holds one exact lifecycle instruction for an OS crash and reads immutable native measurement inputs. */
final class RebuildHandoffJdiSession implements AutoCloseable {
    enum Cut { PRE_ADMISSION, POST_ADMISSION_PRE_SUBMIT, SUBMIT_PRE_BIND }
    private record Site(String type, String name, String signature) { }
    private record Image(String origin, byte[] code) { }
    record Binding(String type, String name, String signature, String origin, String codeSha256,
            String loaderType, long loaderIdentity, long codeIndex) {
        Map<String, Object> evidence() {
            return Map.of("type", type, "name", name, "signature", signature, "origin", origin,
                    "codeSha256", codeSha256, "loaderType", loaderType, "loaderIdentity", loaderIdentity,
                    "codeIndex", codeIndex);
        }
    }
    record Held(Cut cut, String pipelineId, String token, ObservationStore.Scope scope,
            StopReservation.JobIdentity submittedJob, long threadIdentity, long atNanos,
            Binding binding, List<String> callers) {
        Held { callers = List.copyOf(callers); }
        Map<String, Object> evidence() {
            Map<String, Object> value = new LinkedHashMap<>();
            value.put("cut", cut.name()); value.put("pipelineId", pipelineId);
            if (token != null) { value.put("token", token); }
            else { value.put("tokenArgumentAvailability", "ABSENT"); }
            value.put("scope", scopeEvidence(scope));
            if (submittedJob != null) { value.put("submittedJob", jobEvidence(submittedJob)); }
            value.put("threadIdentity", threadIdentity); value.put("atNanos", atNanos);
            value.put("binding", binding.evidence()); value.put("callers", callers);
            value.put("suspendPolicy", "EVENT_THREAD");
            return value;
        }
    }
    record Raw(ObservationStore.Scope scope, StopReservation.JobIdentity job, Instant observedAt,
            List<MetricFact> facts) {
        Raw { facts = List.copyOf(facts); }
    }
    private record RawKey(ObservationStore.Scope scope, Instant observedAt) { }
    record JobProof(ObservationStore.Scope scope, StopReservation.JobIdentity job, long threadIdentity,
            long entryAtNanos, long returnedAtNanos, long returnCodeIndex, Binding binding) {
        Map<String, Object> evidence() {
            return Map.of("scope", scopeEvidence(scope), "job", jobEvidence(job),
                    "threadIdentity", threadIdentity, "entryAtNanos", entryAtNanos, "returnedAtNanos", returnedAtNanos,
                    "returnCodeIndex", returnCodeIndex, "binding", binding.evidence(), "source", "GENUINE_METHOD_RETURN");
        }
    }
    private record JobCall(long receiver, long entryAtNanos, int depth, List<Method> callers, MethodExitRequest exit) {
        JobCall { callers = List.copyOf(callers); }
    }

    private static final String LOADER = "org.springframework.boot.loader.launch.LaunchedClassLoader";
    private static final String SCOPE = "io.tapstate.spi.store.ObservationStore$Scope";
    private static final String JOB = "io.tapstate.spi.store.StopReservation$JobIdentity";
    private static final String MARKER = "io.tapstate.spi.store.StopReservation";
    private static final Site JOB_LOOKUP = new Site("io.tapstate.runtime.engine.Engine", "executionJob",
            "(Ljava/lang/String;)Ljava/util/Optional;");
    private static final Site RAW = new Site("io.tapstate.app.ObservationScopeRegistry", "prepareContinuationPublication",
            "(Lio/tapstate/runtime/scheduler/ObservationPublisher$Prepared;"
                    + "Lio/tapstate/app/ObservationScopeRegistry$ActualTarget;Ljava/util/function/BooleanSupplier;)Ljava/util/Optional;");
    private static final Map<Cut, Site> CUTS = Map.of(
            Cut.PRE_ADMISSION, new Site("io.tapstate.adapters.mongostore.MongoStateStore", "admitSuccessor",
                    "(Lio/tapstate/spi/store/StopReservation;Ljava/lang/String;Ljava/lang/String;Ljava/time/Instant;)Ljava/util/Optional;"),
            Cut.POST_ADMISSION_PRE_SUBMIT, new Site("io.tapstate.runtime.engine.Engine", "submit",
                    "(Ljava/lang/String;Lcom/hazelcast/jet/core/DAG;Ljava/util/Map;"
                            + "Lio/tapstate/runtime/engine/nest/NestSettings;Ljava/lang/String;"
                            + "Lio/tapstate/spi/store/ObservationStore$Scope;)V"),
            Cut.SUBMIT_PRE_BIND, new Site("io.tapstate.adapters.mongostore.MongoStateStore", "bindSuccessor",
                    "(Lio/tapstate/spi/store/StopReservation;Lio/tapstate/spi/store/ObservationStore$Scope;"
                            + "Lio/tapstate/spi/store/StopReservation$JobIdentity;Ljava/time/Instant;)Ljava/util/Optional;"));
    private static final Site MEMBER_ADMISSION = new Site("io.tapstate.adapters.mongostore.MongoStateStore", "admitSuccessor",
            "(Lio/tapstate/spi/store/StopReservation;Ljava/lang/String;Ljava/lang/String;Ljava/util/Set;"
                    + "Ljava/time/Instant;)Ljava/util/Optional;");
    static final Set<String> INSTRUMENTS = Set.of("tapstate.pipeline.records", "tapstate.pipeline.bytes",
            "tapstate.pipeline.record.delivery.duration");
    private static final int MAX_EVENTS = 20_000, MAX_FRAMES = 128, MAX_FACTS = 256, MAX_POINTS = 64;
    private static final int MAX_RAW = 64, MAX_ATTRIBUTES = 16, MAX_CLASS_BYTES = 1_048_576;
    private static final int MAX_JOB_CALLS = 32, MAX_JOB_PROOFS = 4;
    private static final Duration MAX_WAIT = Duration.ofMinutes(3);

    private final Path jar;
    private final String artifactSha, pipelineId, table;
    private final Cut cut;
    private final Site cutSite;
    private final Map<Site, Image> images;
    private final Map<Site, Method> methods = new HashMap<>();
    private final Map<Site, Binding> bindings = new HashMap<>();
    private final List<EventRequest> requests = new ArrayList<>();
    private final LinkedHashMap<RawKey, Raw> raw = new LinkedHashMap<>();
    private final Map<Long, JobCall> jobCalls = new HashMap<>();
    private final LinkedHashMap<ObservationStore.Scope, JobProof> jobProofs = new LinkedHashMap<>();
    private final Path retainedOutput;
    private final RealProcessServer server;
    private final VirtualMachine vm;
    private final Object lock = new Object();
    private final CompletableFuture<Void> ready = new CompletableFuture<>();
    private final CompletableFuture<Held> held = new CompletableFuture<>();
    private final AtomicReference<AssertionError> failure = new AtomicReference<>();
    private final Thread pump;
    private volatile boolean running = true, closing, crashing;
    private ObservationStore.Scope armedSource;
    private EventSet heldSet;
    private long events;

    private RebuildHandoffJdiSession(Path jar, String artifactSha, String pipelineId, String table,
            Cut cut, Site selectedCut, Map<Site, Image> images, RealProcessServer server, VirtualMachine vm, Path retainedOutput) {
        this.jar = jar; this.artifactSha = artifactSha; this.pipelineId = pipelineId; this.table = table;
        this.cut = cut; this.cutSite = selectedCut;
        this.images = images; this.server = server; this.vm = vm;
        this.retainedOutput = retainedOutput;
        pump = new Thread(this::loop, "rebuild-handoff-jdi-events"); pump.setDaemon(true);
    }

    @FunctionalInterface
    interface OwnedLauncher {
        RealProcessServer launch(Path artifact, List<String> debugArguments) throws Exception;
    }

    static RebuildHandoffJdiSession start(String storeUri, String operatorDatabase, Path selectedJar,
            String pipelineId, String table, Cut cut, Path logDirectory, String processLabel) throws Exception {
        return startObserved(selectedJar, pipelineId, table, cut, logDirectory, processLabel, false,
                (artifact, debug) -> RealProcessServer.launchingWithJvmArguments(storeUri, operatorDatabase,
                        artifact, debug, List.of("--tapstate.metrics.history.sample-interval=PT2S")));
    }

    /** A claimed crash uses the same bounded event pump for its cut, genuine Job lookup and raw frame. */
    static RebuildHandoffJdiSession startClaimed(Path selectedJar, String pipelineId, String table,
            Path logDirectory, String processLabel, OwnedLauncher launcher) throws Exception {
        return startObserved(selectedJar, pipelineId, table, Cut.PRE_ADMISSION, logDirectory,
                processLabel, true, Objects.requireNonNull(launcher, "launcher"));
    }

    private static RebuildHandoffJdiSession startObserved(Path selectedJar, String pipelineId, String table,
            Cut cut, Path logDirectory, String processLabel, boolean rawAlongsideCut,
            OwnedLauncher launcher) throws Exception {
        Objects.requireNonNull(pipelineId); Objects.requireNonNull(table);
        Objects.requireNonNull(logDirectory); Objects.requireNonNull(processLabel);
        if (!logDirectory.isAbsolute() || !processLabel.matches("[a-z]{1,16}")) { throw invalid("owned log destination unavailable"); }
        if (pipelineId.isBlank() || pipelineId.length() > 256 || table.isBlank() || table.length() > 256) {
            throw invalid("observed identity is blank or unbounded");
        }
        Path jar = selectedJar.toRealPath();
        if (!Files.isRegularFile(jar) || Files.size(jar) > 512L * 1024 * 1024) { throw invalid("artifact unavailable or unbounded"); }
        String sha = PipelineBenchmarkLiveRunIT.sha256(jar);
        Set<Site> selected = cut == null ? Set.of(RAW) : Set.of(CUTS.get(cut), JOB_LOOKUP);
        Site selectedCut = cut == null ? null : CUTS.get(cut);
        Map<Site, Image> images;
        if (cut == Cut.PRE_ADMISSION) {
            Set<Site> requested = rawAlongsideCut
                    ? Set.of(selectedCut, MEMBER_ADMISSION, JOB_LOOKUP, RAW)
                    : Set.of(selectedCut, MEMBER_ADMISSION, JOB_LOOKUP);
            Map<Site, Image> available = images(jar, requested, Set.of(MEMBER_ADMISSION));
            if (available.containsKey(MEMBER_ADMISSION)) { selectedCut = MEMBER_ADMISSION; }
            if (rawAlongsideCut && !selectedCut.equals(MEMBER_ADMISSION)) {
                throw invalid("the claimed crash has no factual-member admission binding");
            }
            Map<Site, Image> pinned = new HashMap<>();
            pinned.put(selectedCut, available.get(selectedCut));
            pinned.put(JOB_LOOKUP, available.get(JOB_LOOKUP));
            if (rawAlongsideCut) { pinned.put(RAW, available.get(RAW)); }
            images = Map.copyOf(pinned);
        } else {
            images = images(jar, selected, Set.of());
        }
        if (!sha.equals(PipelineBenchmarkLiveRunIT.sha256(jar))) { throw invalid("artifact changed during provenance reads"); }
        ListeningConnector connector = Bootstrap.virtualMachineManager().listeningConnectors().stream()
                .filter(value -> value.name().equals("com.sun.jdi.SocketListen")).findFirst()
                .orElseThrow(() -> invalid("loopback JDI listener unavailable"));
        Map<String, Connector.Argument> arguments = connector.defaultArguments();
        arguments.get("localAddress").setValue("127.0.0.1"); arguments.get("port").setValue("0");
        arguments.get("timeout").setValue("15000");
        String reported = connector.startListening(arguments);
        boolean listening = true;
        RealProcessServer server = null; VirtualMachine vm = null; RebuildHandoffJdiSession session = null;
        Throwable primary = null;
        try {
            String address = BenchmarkJdiCostObserver.numericLoopbackDialAddress(
                    arguments.get("localAddress").value(), reported, arguments.get("port").value());
            server = launcher.launch(jar,
                    List.of("-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=" + address));
            vm = connector.accept(arguments);
            if (!vm.canGetBytecodes()) { throw invalid("live Code unavailable"); }
            if (cut != null && !vm.canGetMethodReturnValues()) { throw invalid("genuine native Job return values unavailable"); }
            connector.stopListening(arguments); listening = false;
            Path retained = logDirectory.resolve(processLabel + "-" + server.pid() + "-server.out");
            session = new RebuildHandoffJdiSession(jar, sha, pipelineId, table, cut, selectedCut, images, server, vm, retained);
            session.install(); session.pump.start(); server.awaitHealthy(); session.awaitReady(MAX_WAIT);
            session.retainOutput();
            return session;
        } catch (Exception | Error problem) {
            primary = problem;
            if (session != null) {
                try { session.close(); } catch (Exception | Error cleanup) { if (cleanup != problem) { problem.addSuppressed(cleanup); } }
            } else {
                try { if (vm != null) { vm.dispose(); } } catch (VMDisconnectedException ignored) { }
                catch (Exception | Error cleanup) { if (cleanup != problem) { problem.addSuppressed(cleanup); } }
                finally {
                    if (server != null) {
                        try { server.kill(); } catch (Exception | Error cleanup) { if (cleanup != problem) { problem.addSuppressed(cleanup); } }
                        try { retainOutput(server, logDirectory.resolve(processLabel + "-" + server.pid() + "-server.out")); }
                        catch (Exception | Error recording) { problem.addSuppressed(recording); }
                    }
                }
            }
            throw problem;
        } finally {
            if (listening) {
                try { connector.stopListening(arguments); }
                catch (Exception | Error cleanup) {
                    if (primary != null) { primary.addSuppressed(invalid("owned listener cleanup failed")); }
                    else { throw invalid("owned listener cleanup failed"); }
                }
            }
        }
    }

    RealProcessServer server() { return server; }
    Path retainedOutput() { return retainedOutput; }
    Binding rawBinding() { synchronized (lock) { check(); return bindings.get(RAW); } }
    Optional<JobProof> observedJob(ObservationStore.Scope scope) {
        synchronized (lock) { check(); return Optional.ofNullable(jobProofs.get(scope)); }
    }
    private void awaitReady(Duration timeout) throws Exception { await(ready, timeout); }
    Held awaitHeld(Duration timeout) throws Exception { return await(held, timeout); }

    void arm(ObservationStore.Scope source) throws Exception {
        synchronized (lock) {
            check();
            if (cut == null || armedSource != null || held.isDone() || bindings.size() != images.size()
                    || !artifactSha.equals(PipelineBenchmarkLiveRunIT.sha256(jar))) { throw invalid("crash barrier cannot be armed"); }
            armedSource = Objects.requireNonNull(source);
        }
    }

    Optional<Raw> rawAt(ObservationStore.Scope scope, Instant observedAt) {
        synchronized (lock) {
            check();
            // Mongo's public timestamp has millisecond precision; keep the actual native time in evidence.
            List<Raw> matches = raw.values().stream().filter(value -> value.scope().equals(scope)
                    && value.observedAt().toEpochMilli() == observedAt.toEpochMilli()).toList();
            if (matches.size() > 1) { throw invalid("stored time names more than one native frame"); }
            return matches.stream().findFirst();
        }
    }

    /** Kills while the exact lifecycle thread remains suspended; no cleanup callback is allowed to run. */
    void killHeldProcess() throws Exception {
        synchronized (lock) {
            check();
            if (!held.isDone() || heldSet == null || crashing || closing) { throw invalid("no owned crash boundary is held"); }
            crashing = true;
        }
        server.kill();
        if (server.isAlive()) { throw invalid("owned OS process survived its forced kill"); }
        retainOutput();
        pump.join(5_000);
        if (pump.isAlive()) { throw invalid("event pump survived the killed process"); }
        check();
    }

    private <T> T await(CompletableFuture<T> result, Duration timeout) throws Exception {
        if (timeout == null || timeout.isNegative() || timeout.isZero() || timeout.compareTo(MAX_WAIT) > 0) {
            throw invalid("observer wait is outside its fixed bound");
        }
        check(); T value = result.get(timeout.toNanos(), TimeUnit.NANOSECONDS); check(); return value;
    }

    private void install() {
        for (String type : images.keySet().stream().map(Site::type).distinct().toList()) {
            var request = vm.eventRequestManager().createClassPrepareRequest();
            request.addClassFilter(type); request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
            request.enable(); requests.add(request);
        }
    }

    private void loop() {
        try {
            while (running) {
                EventSet set = vm.eventQueue().remove(50);
                if (set != null) { handle(set); }
            }
        } catch (VMDisconnectedException gone) { if (!crashing && !closing) { fail(gone); } }
        catch (InterruptedException interrupted) { if (!closing) { fail(interrupted); } }
        catch (Exception | Error problem) { if (!closing) { fail(problem); } }
    }

    private void handle(EventSet set) throws Exception {
        boolean retained = false;
        try {
            synchronized (lock) {
                for (Event event : set) {
                    if (++events > MAX_EVENTS) { throw invalid("event budget exceeded"); }
                    if (event instanceof VMStartEvent) { continue; }
                    if (event instanceof ClassPrepareEvent prepared) { bind(prepared.referenceType()); }
                    else if (event instanceof BreakpointEvent breakpoint) {
                        Site site = (Site) breakpoint.request().getProperty("handoff-site");
                        if (site == null || !breakpoint.location().method().equals(methods.get(site))) { throw invalid("unmapped exact breakpoint"); }
                        verify(site);
                        if (site.equals(RAW)) { captureRaw(breakpoint); }
                        else if (site.equals(JOB_LOOKUP)) { captureJobEntry(breakpoint); }
                        else if (captureCut(breakpoint, set)) { retained = true; }
                    } else if (event instanceof MethodExitEvent returned) {
                        if (returned.method().equals(methods.get(JOB_LOOKUP))) { captureJobReturn(returned); }
                    } else if (event instanceof VMDeathEvent || event instanceof VMDisconnectEvent) {
                        if (!crashing && !closing) { throw invalid("owned Boot exited before the witness finished"); }
                        running = false;
                    } else { throw invalid("unmapped observer event"); }
                }
            }
        } finally {
            if (!retained) {
                try { set.resume(); } catch (VMDisconnectedException gone) { if (!crashing && !closing) { throw gone; } }
            }
        }
    }

    private void bind(ReferenceType type) {
        for (Site site : images.keySet().stream().filter(value -> value.type().equals(type.name())).toList()) {
            if (bindings.containsKey(site)) { throw invalid("duplicate target class binding"); }
            var loader = type.classLoader();
            if (loader == null || !LOADER.equals(loader.referenceType().name())) { throw invalid("unexpected application loader"); }
            if (!bindings.isEmpty() && bindings.values().iterator().next().loaderIdentity() != loader.uniqueID()) {
                throw invalid("observed methods do not share the pinned application loader");
            }
            List<Method> choices = type.methodsByName(site.name(), site.signature());
            if (choices.size() != 1) { throw invalid("target method missing or ambiguous"); }
            Method method = choices.getFirst();
            if (!method.declaringType().equals(type) || method.isStatic() || method.isNative() || method.isAbstract()
                    || method.isBridge() || method.isObsolete() || !Arrays.equals(images.get(site).code(), method.bytecodes())) {
                throw invalid("live target differs from immutable Code");
            }
            Location entry = method.locationOfCodeIndex(0);
            if (entry == null || entry.codeIndex() != 0 || BenchmarkJdiCostObserver.returnOffsets(method.bytecodes()).isEmpty()) {
                throw invalid("target entry or returns unavailable");
            }
            var request = vm.eventRequestManager().createBreakpointRequest(entry);
            request.putProperty("handoff-site", site); request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
            request.enable(); requests.add(request);
            methods.put(site, method);
            bindings.put(site, new Binding(site.type(), site.name(), site.signature(), images.get(site).origin(),
                    digest(images.get(site).code()), LOADER, loader.uniqueID(), 0));
        }
        if (bindings.size() == images.size()) { ready.complete(null); }
    }

    private void verify(Site site) {
        Method method = methods.get(site);
        if (method == null || method.isObsolete() || !Arrays.equals(method.bytecodes(), images.get(site).code())
                || method.declaringType().classLoader() == null
                || method.declaringType().classLoader().uniqueID() != bindings.get(site).loaderIdentity()) {
            throw invalid("live method or loader changed");
        }
    }

    private void captureJobEntry(BreakpointEvent event) throws Exception {
        List<StackFrame> frames = event.thread().frames();
        if (frames.isEmpty() || frames.size() > MAX_FRAMES) { throw invalid("Job lookup stack unavailable or unbounded"); }
        StackFrame frame = frames.getFirst();
        List<Value> arguments = frame.getArgumentValues();
        if (arguments.size() != 1) { throw invalid("Job lookup arguments changed"); }
        if (!pipelineId.equals(text(arguments.getFirst()))) { return; }
        ObjectReference receiver = frame.thisObject();
        if (receiver == null || jobCalls.containsKey(event.thread().uniqueID()) || jobCalls.size() >= MAX_JOB_CALLS) {
            throw invalid("Job lookup receiver absent or call overlap exceeds its bound");
        }
        MethodExitRequest exit = vm.eventRequestManager().createMethodExitRequest();
        exit.addClassFilter(methods.get(JOB_LOOKUP).declaringType()); exit.addThreadFilter(event.thread());
        exit.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD); exit.enable(); requests.add(exit);
        jobCalls.put(event.thread().uniqueID(), new JobCall(receiver.uniqueID(), System.nanoTime(), frames.size(),
                frames.subList(1, frames.size()).stream().map(caller -> caller.location().method()).toList(), exit));
    }

    private void captureJobReturn(MethodExitEvent event) throws Exception {
        if (!event.method().equals(methods.get(JOB_LOOKUP))) { return; }
        JobCall call = jobCalls.get(event.thread().uniqueID());
        if (call == null || !event.request().equals(call.exit())) { throw invalid("Job return lost its exact entry correlation"); }
        verify(JOB_LOOKUP);
        List<StackFrame> frames = event.thread().frames();
        int firstCaller = frames.size() - call.callers().size();
        if (frames.isEmpty() || frames.size() > MAX_FRAMES || firstCaller < 0 || firstCaller > 1
                || firstCaller == 1 && (frames.size() != call.depth()
                        || !frames.getFirst().location().method().equals(methods.get(JOB_LOOKUP))
                        || frames.getFirst().thisObject() == null
                        || frames.getFirst().thisObject().uniqueID() != call.receiver())) {
            throw invalid("Job return changed its stack depth or receiver");
        }
        for (int i = 0; i < call.callers().size(); i++) {
            if (!frames.get(firstCaller + i).location().method().equals(call.callers().get(i))) {
                throw invalid("Job return changed its observed caller chain");
            }
        }
        long codeIndex = event.location().codeIndex();
        if (codeIndex < 0 || !BenchmarkJdiCostObserver.returnOffsets(images.get(JOB_LOOKUP).code()).contains(Math.toIntExact(codeIndex))) {
            throw invalid("Job return is outside its pinned normal return instructions");
        }
        ObjectReference optional = object(event.returnValue(), "java.util.Optional");
        Value found = value(optional, "value", "Ljava/lang/Object;");
        call.exit().disable(); vm.eventRequestManager().deleteEventRequest(call.exit()); requests.remove(call.exit());
        jobCalls.remove(event.thread().uniqueID());
        if (found == null) { return; }
        ObjectReference execution = object(found, "io.tapstate.runtime.engine.Engine$ExecutionJob");
        if (execution.referenceType().classLoader() == null
                || execution.referenceType().classLoader().uniqueID() != bindings.get(JOB_LOOKUP).loaderIdentity()) {
            throw invalid("genuine Job return changed its application loader");
        }
        ObservationStore.Scope scope = scope(object(value(execution, "scope", "Lio/tapstate/spi/store/ObservationStore$Scope;"), SCOPE));
        StopReservation.JobIdentity job = job(object(value(execution, "job", "Lio/tapstate/spi/store/StopReservation$JobIdentity;"), JOB));
        jobProofs.put(scope, new JobProof(scope, job, event.thread().uniqueID(), call.entryAtNanos(), System.nanoTime(),
                codeIndex, bindings.get(JOB_LOOKUP)));
        while (jobProofs.size() > MAX_JOB_PROOFS) { jobProofs.remove(jobProofs.keySet().iterator().next()); }
    }

    private boolean captureCut(BreakpointEvent event, EventSet set) throws Exception {
        if (armedSource == null) { return false; }
        List<StackFrame> frames = event.thread().frames();
        if (frames.isEmpty() || frames.size() > MAX_FRAMES) { throw invalid("held stack unavailable or unbounded"); }
        List<Value> arguments = frames.getFirst().getArgumentValues();
        String id; String token = null; ObservationStore.Scope scope; StopReservation.JobIdentity job = null;
        if (cut == Cut.POST_ADMISSION_PRE_SUBMIT) {
            if (arguments.size() != 6) { throw invalid("scope-bearing submission arguments changed"); }
            id = text(arguments.getFirst()); if (!pipelineId.equals(id)) { return false; }
            scope = scope(object(arguments.get(5), SCOPE));
            if (!scope.pipelineIncarnationId().equals(armedSource.pipelineIncarnationId())
                    || scope.executionGeneration() != Math.incrementExact(armedSource.executionGeneration())) { return false; }
        } else {
            int expectedArguments = cut == Cut.PRE_ADMISSION && cutSite.equals(MEMBER_ADMISSION) ? 5 : 4;
            if (arguments.size() != expectedArguments) { throw invalid("reservation boundary arguments changed"); }
            ObjectReference marker = object(arguments.getFirst(), MARKER);
            id = text(value(marker, "pipelineId", "Ljava/lang/String;")); if (!pipelineId.equals(id)) { return false; }
            token = text(value(marker, "token", "Ljava/lang/String;"));
            ObjectReference source = object(value(marker, "source", "Lio/tapstate/spi/store/StopReservation$Source;"), MARKER + "$Source");
            ObservationStore.Scope sourceScope = scope(object(value(source, "scope", "Lio/tapstate/spi/store/ObservationStore$Scope;"), SCOPE));
            if (!armedSource.equals(sourceScope)) { throw invalid("held marker no longer names the real paused source"); }
            if (!"CONTINUE".equals(enumName(value(marker, "counterPolicy", "Lio/tapstate/spi/store/StopReservation$CounterPolicy;")))) {
                throw invalid("genuine resume lost its frozen continuation policy");
            }
            scope = cut == Cut.PRE_ADMISSION ? sourceScope : scope(object(arguments.get(1), SCOPE));
            if (cut == Cut.SUBMIT_PRE_BIND) { job = job(object(arguments.get(2), JOB)); }
        }
        if (held.isDone()) { throw invalid("the single crash boundary was hit again"); }
        if (set.suspendPolicy() != EventRequest.SUSPEND_EVENT_THREAD || frames.stream().noneMatch(frame ->
                frame.location().declaringType().name().equals("io.tapstate.runtime.scheduler.PipelineConverger")
                        && (frame.location().method().name().equals("admitReplacement")
                                || frame.location().method().name().startsWith("lambda$admitReplacement$")))) {
            throw invalid("held method is not this real replacement's lifecycle call");
        }
        event.request().disable(); heldSet = set;
        held.complete(new Held(cut, id, token, scope, job, event.thread().uniqueID(), System.nanoTime(),
                bindings.get(cutSite), frames.stream().limit(16).map(frame -> frame.location().declaringType().name()
                        + "#" + frame.location().method().name()).toList()));
        return true;
    }

    private void captureRaw(BreakpointEvent event) throws Exception {
        List<StackFrame> frames = event.thread().frames();
        if (frames.isEmpty() || frames.size() > MAX_FRAMES) { throw invalid("raw frame stack unavailable or unbounded"); }
        List<Value> arguments = frames.getFirst().getArgumentValues();
        if (arguments.size() != 3) { throw invalid("raw publication arguments changed"); }
        ObjectReference prepared = object(arguments.getFirst(), "io.tapstate.runtime.scheduler.ObservationPublisher$Prepared");
        ObjectReference observation = object(value(prepared, "observation", "Lio/tapstate/core/lifecycle/Observation;"),
                "io.tapstate.core.lifecycle.Observation");
        if (!pipelineId.equals(text(value(observation, "pipelineId", "Ljava/lang/String;")))) { return; }
        ObjectReference target = object(arguments.get(1), "io.tapstate.app.ObservationScopeRegistry$ActualTarget");
        ObservationStore.Scope scope = scope(object(value(target, "scope", "Lio/tapstate/spi/store/ObservationStore$Scope;"), SCOPE));
        StopReservation.JobIdentity job = job(object(value(target, "job", "Lio/tapstate/spi/store/StopReservation$JobIdentity;"), JOB));
        Instant at = instant(value(observation, "observedAt", "Ljava/time/Instant;"));
        List<MetricFact> facts = new ArrayList<>();
        for (Value item : list(value(observation, "facts", "Ljava/util/List;"), MAX_FACTS)) {
            ObjectReference fact = object(item, "io.tapstate.core.lifecycle.MetricFact");
            String name = text(value(fact, "name", "Ljava/lang/String;"));
            if (!INSTRUMENTS.contains(name)) { continue; }
            MetricType type = MetricType.valueOf(enumName(value(fact, "type", "Lio/tapstate/core/lifecycle/MetricType;")));
            String unit = text(value(fact, "unit", "Ljava/lang/String;"));
            List<MetricPoint> points = new ArrayList<>();
            for (Value pointValue : list(value(fact, "points", "Ljava/util/List;"), MAX_POINTS)) {
                ObjectReference point = object(pointValue, "io.tapstate.core.lifecycle.MetricPoint");
                Map<String, String> attributes = attributes(value(point, "attributes", "Ljava/util/Map;"));
                if (!table.equals(attributes.get(MetricAttributes.TABLE_ID)) || type != MetricType.HISTOGRAM
                        && !"out".equals(attributes.get(MetricAttributes.DIRECTION))) { continue; }
                Instant start = instantOrNull(value(point, "startTime", "Ljava/time/Instant;"));
                Instant measured = instant(value(point, "observedAt", "Ljava/time/Instant;"));
                if (type == MetricType.COUNTER) {
                    points.add(new MetricPoint(attributes, start, measured, boxedLong(value(point, "value", "Ljava/lang/Long;")), null));
                } else if (type == MetricType.HISTOGRAM) {
                    ObjectReference distribution = object(value(point, "histogram", "Lio/tapstate/core/lifecycle/HistogramValue;"),
                            "io.tapstate.core.lifecycle.HistogramValue");
                    List<Double> bounds = list(value(distribution, "bounds", "Ljava/util/List;"), 64).stream()
                            .map(RebuildHandoffJdiSession::boxedDouble).toList();
                    List<Long> buckets = list(value(distribution, "bucketCounts", "Ljava/util/List;"), 65).stream()
                            .map(RebuildHandoffJdiSession::boxedLong).toList();
                    points.add(new MetricPoint(attributes, start, measured, null,
                            new HistogramValue(longValue(value(distribution, "count", "J")),
                                    doubleValue(value(distribution, "sum", "D")), bounds, buckets)));
                } else { throw invalid("selected cumulative instrument changed type"); }
            }
            if (!points.isEmpty()) { facts.add(new MetricFact(name, type, unit, points)); }
        }
        Raw reading = new Raw(scope, job, at, facts);
        RawKey key = new RawKey(scope, at);
        Raw previous = raw.putIfAbsent(key, reading);
        if (previous != null && !previous.equals(reading)) { throw invalid("one raw frame time named different native facts"); }
        while (raw.size() > MAX_RAW) { raw.remove(raw.keySet().iterator().next()); }
    }

    private static Map<String, String> attributes(Value input) {
        ObjectReference map = object(input, null);
        if (map.referenceType().name().startsWith("java.util.Collections$Unmodifiable")) {
            map = object(value(map, "m", "Ljava/util/Map;"), null);
        }
        Map<String, String> result = new TreeMap<>();
        String type = map.referenceType().name();
        if (type.equals("java.util.TreeMap")) {
            int size = intValue(value(map, "size", "I"));
            if (size < 0 || size > MAX_ATTRIBUTES) { throw invalid("attribute count unbounded"); }
            Value root = value(map, "root", "Ljava/util/TreeMap$Entry;");
            ArrayDeque<ObjectReference> pending = new ArrayDeque<>(); Set<Long> seen = new HashSet<>();
            if (root != null) { pending.add(object(root, "java.util.TreeMap$Entry")); }
            while (!pending.isEmpty()) {
                ObjectReference entry = pending.removeFirst();
                if (!seen.add(entry.uniqueID()) || seen.size() > MAX_ATTRIBUTES) { throw invalid("attribute tree cyclic or unbounded"); }
                putAttribute(result, value(entry, "key", "Ljava/lang/Object;"), value(entry, "value", "Ljava/lang/Object;"));
                for (String child : List.of("left", "right")) {
                    Value next = value(entry, child, "Ljava/util/TreeMap$Entry;");
                    if (next != null) { pending.add(object(next, "java.util.TreeMap$Entry")); }
                }
            }
            if (result.size() != size) { throw invalid("attribute tree does not match its recorded size"); }
        } else if (type.equals("java.util.ImmutableCollections$Map1")) {
            putAttribute(result, value(map, "k0", "Ljava/lang/Object;"), value(map, "v0", "Ljava/lang/Object;"));
        } else if (type.equals("java.util.ImmutableCollections$MapN")) {
            Value arrayValue = value(map, "table", "[Ljava/lang/Object;");
            if (!(arrayValue instanceof ArrayReference entries) || entries.length() > MAX_ATTRIBUTES * 4) {
                throw invalid("immutable attribute map unbounded");
            }
            List<Value> entriesList = entries.getValues();
            for (int i = 0; i < entriesList.size(); i += 2) {
                if (entriesList.get(i) != null) { putAttribute(result, entriesList.get(i), entriesList.get(i + 1)); }
            }
        } else { throw invalid("unsupported immutable attribute map: " + type); }
        return Map.copyOf(result);
    }

    private static void putAttribute(Map<String, String> map, Value key, Value value) {
        if (map.putIfAbsent(text(key), text(value)) != null || map.size() > MAX_ATTRIBUTES) {
            throw invalid("duplicate or unbounded attribute");
        }
    }

    private static List<Value> list(Value input, int bound) {
        ObjectReference list = object(input, null);
        if (list.referenceType().name().equals("java.util.ImmutableCollections$ListN")) {
            Value content = value(list, "elements", "[Ljava/lang/Object;");
            if (!(content instanceof ArrayReference array) || array.length() > bound) { throw invalid("immutable list unbounded"); }
            return array.getValues();
        }
        if (list.referenceType().name().equals("java.util.ImmutableCollections$List12")) {
            Value first = value(list, "e0", "Ljava/lang/Object;"); Value second = value(list, "e1", "Ljava/lang/Object;");
            if (second instanceof ObjectReference sentinel && sentinel.referenceType().name().equals("java.lang.Object")) {
                return List.of(first);
            }
            if (bound < 2) { throw invalid("immutable list exceeded selected bound"); }
            return List.of(first, second);
        }
        throw invalid("unsupported immutable list: " + list.referenceType().name());
    }

    private static ObservationStore.Scope scope(ObjectReference value) {
        return new ObservationStore.Scope(text(value(value, "pipelineIncarnationId", "Ljava/lang/String;")),
                longValue(value(value, "executionGeneration", "J")));
    }
    private static StopReservation.JobIdentity job(ObjectReference value) {
        return new StopReservation.JobIdentity(text(value(value, "clusterId", "Ljava/lang/String;")),
                longValue(value(value, "jobId", "J")), text(value(value, "bootId", "Ljava/lang/String;")));
    }
    private static Instant instantOrNull(Value value) { return value == null ? null : instant(value); }
    private static Instant instant(Value value) {
        ObjectReference instant = object(value, "java.time.Instant");
        return Instant.ofEpochSecond(longValue(value(instant, "seconds", "J")), intValue(value(instant, "nanos", "I")));
    }
    private static String enumName(Value input) { return text(value(object(input, null), "name", "Ljava/lang/String;")); }
    private static long boxedLong(Value value) { return longValue(value(object(value, "java.lang.Long"), "value", "J")); }
    private static double boxedDouble(Value value) { return doubleValue(value(object(value, "java.lang.Double"), "value", "D")); }
    private static long longValue(Value value) { if (value instanceof LongValue number) { return number.value(); } throw invalid("long field unavailable"); }
    private static int intValue(Value value) { if (value instanceof IntegerValue number) { return number.value(); } throw invalid("integer field unavailable"); }
    private static double doubleValue(Value value) { if (value instanceof DoubleValue number) { return number.value(); } throw invalid("double field unavailable"); }
    private static String text(Value value) {
        if (value instanceof StringReference text && !text.value().isBlank() && text.value().length() <= 512) { return text.value(); }
        throw invalid("bounded string field unavailable");
    }
    private static ObjectReference object(Value value, String type) {
        if (value instanceof ObjectReference object && (type == null || type.equals(object.referenceType().name()))) { return object; }
        throw invalid("typed live object unavailable");
    }
    private static Value value(ObjectReference object, String name, String signature) {
        List<Field> fields = object.referenceType().allFields().stream().filter(field -> field.name().equals(name)).toList();
        if (fields.size() != 1 || fields.getFirst().isStatic() || !fields.getFirst().signature().equals(signature)) {
            throw invalid("typed field unavailable: " + object.referenceType().name() + "." + name);
        }
        return object.getValue(fields.getFirst());
    }

    static Map<String, Object> scopeEvidence(ObservationStore.Scope scope) {
        return Map.of("pipelineIncarnationId", scope.pipelineIncarnationId(), "executionGeneration", scope.executionGeneration());
    }
    static Map<String, Object> jobEvidence(StopReservation.JobIdentity job) {
        return Map.of("clusterId", job.clusterId(), "jobId", job.jobId(), "bootId", job.bootId());
    }
    private void check() { AssertionError problem = failure.get(); if (problem != null) { throw problem; } }
    private void fail(Throwable problem) {
        AssertionError safe = problem instanceof AssertionError assertion ? assertion
                : invalid("observer failed with " + problem.getClass().getName());
        if (safe != problem) { safe.initCause(problem); }
        failure.compareAndSet(null, safe); running = false; ready.completeExceptionally(safe); held.completeExceptionally(safe);
    }

    @Override public void close() throws Exception {
        closing = true; running = false;
        // A failed witness with a held instruction is also killed, never allowed to execute on cleanup.
        try {
            if (heldSet != null && server.isAlive()) { server.kill(); }
            try { vm.dispose(); } catch (VMDisconnectedException ignored) { }
        } finally {
            pump.interrupt();
            try { pump.join(2_000); } finally { server.close(); retainOutput(); }
        }
        if (pump.isAlive()) { throw invalid("owned observer thread did not stop"); }
        check();
        if (!artifactSha.equals(PipelineBenchmarkLiveRunIT.sha256(jar))) { throw invalid("artifact changed during the witness"); }
    }

    private static Map<Site, Image> images(Path jar, Set<Site> selected, Set<Site> optional) throws Exception {
        Map<Site, Image> found = new HashMap<>();
        try (ZipFile boot = new ZipFile(jar.toFile())) {
            for (String type : selected.stream().map(Site::type).distinct().toList()) {
                ZipEntry direct = boot.getEntry("BOOT-INF/classes/" + type.replace('.', '/') + ".class");
                if (direct != null) {
                    try (InputStream input = boot.getInputStream(direct)) {
                        addImages(found, selected.stream().filter(site -> site.type().equals(type)).toList(), direct.getName(), input, optional);
                    }
                }
            }
            List<? extends ZipEntry> libraries = boot.stream().filter(entry -> entry.getName().startsWith("BOOT-INF/lib/")
                    && entry.getName().endsWith(".jar")).toList();
            if (libraries.size() > 512) { throw invalid("artifact libraries unbounded"); }
            for (ZipEntry library : libraries) {
                if (library.getSize() < 0 || library.getSize() > 128L * 1024 * 1024) { throw invalid("nested library unbounded"); }
                try (ZipInputStream nested = new ZipInputStream(boot.getInputStream(library))) {
                    ZipEntry entry; int entries = 0;
                    while ((entry = nested.getNextEntry()) != null) {
                        if (++entries > 100_000) { throw invalid("nested entries unbounded"); }
                        String resource = entry.getName();
                        List<Site> sites = selected.stream().filter(site -> resource.equals(site.type().replace('.', '/') + ".class")).toList();
                        if (!sites.isEmpty()) {
                            addImages(found, sites, library.getName() + "!/" + resource, nested, optional);
                        }
                    }
                }
            }
        }
        if (!found.keySet().containsAll(selected.stream().filter(site -> !optional.contains(site)).toList())) {
            throw invalid("artifact lacks the selected exact methods");
        }
        return Map.copyOf(found);
    }

    private static void addImages(Map<Site, Image> found, List<Site> sites, String origin, InputStream input, Set<Site> optional) throws Exception {
        byte[] bytes = input.readNBytes(MAX_CLASS_BYTES + 1);
        if (bytes.length > MAX_CLASS_BYTES) { throw invalid("class byte bound exceeded"); }
        for (Site site : sites) {
            byte[] code = methodCode(bytes, site.name() + site.signature());
            if (code == null && optional.contains(site)) { continue; }
            if (code == null || found.putIfAbsent(site, new Image(origin, code)) != null) { throw invalid("method absent or duplicated"); }
        }
    }

    private void retainOutput() throws Exception { retainOutput(server, retainedOutput); }
    private static void retainOutput(RealProcessServer server, Path destination) throws Exception {
        if (!Files.isRegularFile(server.output()) || Files.size(server.output()) > 128L * 1024 * 1024) {
            throw invalid("owned server output absent or exceeds its bound");
        }
        Files.createDirectories(destination.getParent());
        Files.copy(server.output(), destination, StandardCopyOption.REPLACE_EXISTING);
    }

    /** Reads immutable class metadata without loading, generating or rewriting a target class. */
    private static byte[] methodCode(byte[] bytes, String selected) throws Exception {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (input.readInt() != 0xcafebabe) { throw invalid("invalid class header"); }
            input.skipNBytes(4); String[] utf = new String[input.readUnsignedShort()];
            for (int i = 1; i < utf.length; i++) {
                switch (input.readUnsignedByte()) {
                    case 1 -> utf[i] = input.readUTF();
                    case 3, 4, 9, 10, 11, 12, 17, 18 -> input.skipNBytes(4);
                    case 5, 6 -> { input.skipNBytes(8); i++; }
                    case 7, 8, 16, 19, 20 -> input.skipNBytes(2);
                    case 15 -> input.skipNBytes(3);
                    default -> throw invalid("unknown constant-pool tag");
                }
            }
            input.skipNBytes(6); input.skipNBytes(input.readUnsignedShort() * 2L);
            int fields = input.readUnsignedShort();
            for (int i = 0; i < fields; i++) { input.skipNBytes(6); skipAttributes(input); }
            byte[] found = null; int methods = input.readUnsignedShort();
            for (int i = 0; i < methods; i++) {
                input.readUnsignedShort(); String key = utf[input.readUnsignedShort()] + utf[input.readUnsignedShort()];
                int attributes = input.readUnsignedShort();
                for (int j = 0; j < attributes; j++) {
                    String name = utf[input.readUnsignedShort()]; long size = Integer.toUnsignedLong(input.readInt());
                    if (size > MAX_CLASS_BYTES) { throw invalid("class attribute unbounded"); }
                    if (selected.equals(key) && name.equals("Code")) {
                        byte[] attribute = input.readNBytes((int) size);
                        if (attribute.length != size || found != null) { throw invalid("Code truncated or duplicated"); }
                        try (DataInputStream code = new DataInputStream(new ByteArrayInputStream(attribute))) {
                            code.skipNBytes(4); int length = code.readInt();
                            if (length < 1 || length > 65_535) { throw invalid("invalid Code length"); }
                            found = code.readNBytes(length); if (found.length != length) { throw invalid("Code truncated"); }
                        }
                    } else { input.skipNBytes(size); }
                }
            }
            return found;
        }
    }
    private static void skipAttributes(DataInputStream input) throws Exception {
        int attributes = input.readUnsignedShort();
        for (int i = 0; i < attributes; i++) {
            input.skipNBytes(2); long size = Integer.toUnsignedLong(input.readInt());
            if (size > MAX_CLASS_BYTES) { throw invalid("class attribute unbounded"); } input.skipNBytes(size);
        }
    }
    private static String digest(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (java.security.NoSuchAlgorithmException unavailable) { throw new AssertionError(unavailable); }
    }
    private static AssertionError invalid(String reason) { return new AssertionError("Invalid rebuild crash witness: " + reason); }
}
