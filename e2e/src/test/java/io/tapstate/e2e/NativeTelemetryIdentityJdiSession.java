package io.tapstate.e2e;

import com.sun.jdi.*;
import com.sun.jdi.connect.Connector;
import com.sun.jdi.connect.ListeningConnector;
import com.sun.jdi.event.*;
import com.sun.jdi.request.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
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
    enum Target { JOB, LOG, OFFER, VISIBLE, PRODUCE, PREPARE, FOLDER_FORGET, EXPORT_FORGET, EXPORT_INCARNATION }
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
            Map<String, Object> scope, long receiverId) { }
    record Boundary(String phase, long sequence, String jarSha256, String pipelineId,
            Map<Target, Binding> bindings, Map<Target, Counts> counts, List<Map<String, Object>> records,
            List<AuthorityReceipt> authorityReceipts, Set<String> unverified, Set<String> decodedLayouts,
            String vmVersion, long events, long handlingNanos, int openCalls, boolean queueDrained,
            boolean ownedVmDeath, boolean ownedVmDisconnected) {
        Boundary {
            bindings = Map.copyOf(bindings); counts = Map.copyOf(counts); records = List.copyOf(records);
            authorityReceipts = List.copyOf(authorityReceipts); unverified = Set.copyOf(unverified);
            decodedLayouts = Set.copyOf(decodedLayouts);
        }
        boolean invocationDrainComplete() {
            return queueDrained && openCalls == 0 && counts.values().stream().allMatch(count ->
                    count.inFlight() == 0 && count.entries() == count.normalReturns() + count.exceptionalExits());
        }
        boolean decodedAndAuthorityBound() {
            return unverified.isEmpty() && bindings.size() == Target.values().length
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
            out.put("decodedLayouts", decodedLayouts.stream().sorted().toList());
            out.put("vmVersion", vmVersion); out.put("events", events); out.put("handlingNanos", handlingNanos);
            out.put("openCalls", openCalls); out.put("queueDrained", queueDrained);
            out.put("ownedVmDeath", ownedVmDeath); out.put("ownedVmDisconnected", ownedVmDisconnected);
            out.put("passiveObserver", true); out.put("performanceAcceptanceEligible", false);
            return Map.copyOf(out);
        }
    }
    private record Spec(Target target, String type, String method, String descriptor, int arguments, int pipeline) { }
    private record Image(String origin, Map<String, byte[]> methods, Map<String, String> fields) { }
    private record Site(Target target, boolean entry) { }
    private static final class Totals { long entries, normal, exceptional; }
    private static final class Call {
        final long id;
        final Spec spec;
        final int depth;
        final ObjectReference receiver;
        final List<Value> arguments;
        final Map<String, Object> entry;
        final List<Map<String, Object>> included = new ArrayList<>();
        boolean escaping;
        MethodExitRequest exit;
        Call(long id, Spec spec, int depth, ObjectReference receiver,
                List<Value> arguments, Map<String, Object> entry) {
            this.id = id; this.spec = spec; this.depth = depth; this.receiver = receiver;
            this.arguments = arguments; this.entry = entry;
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
                    "(Ljava/lang/String;Ljava/lang/String;)V", 2, 0));

    private final Path jar;
    private final String sha, pipeline;
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
    private final Set<Long> publisherFolders = new HashSet<>();
    private final Map<Long, CapturedJob> jobs = new LinkedHashMap<>();
    private final List<Map<String, Object>> records = new ArrayList<>();
    private final List<AuthorityReceipt> authorities = new ArrayList<>();
    private final Set<String> unverified = new LinkedHashSet<>(), layouts = new LinkedHashSet<>();
    private final Set<String> validatedTypes = new HashSet<>();
    private final AtomicReference<AssertionError> failure = new AtomicReference<>();
    private volatile CompletableFuture<Boundary> command;
    private volatile String requestedPhase;
    private volatile boolean running = true;
    private boolean closing, closed, vmDeath, disconnected;
    private long loaderId = -1, events, sequence, calls, handlingNanos;
    private long phaseBytes;
    private Boundary terminal;

    private NativeTelemetryIdentityJdiSession(Path jar, String sha, String pipeline,
            Map<String, Image> images, RealProcessServer server, VirtualMachine vm) throws Exception {
        this.jar = jar; this.sha = sha; this.pipeline = pipeline; this.images = images;
        this.server = server; this.vm = vm; this.vmVersion = vm.version();
        for (Spec spec : SPECS) {
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
        Objects.requireNonNull(expectedSha256); Objects.requireNonNull(pipelineId);
        Objects.requireNonNull(launcher);
        if (!expectedSha256.matches("[0-9a-f]{64}") || pipelineId.isBlank() || pipelineId.length() > 256) {
            throw invalid("invalid immutable artifact or pipeline input");
        }
        Path jar = input.toRealPath();
        if (!Files.isRegularFile(jar) || Files.size(jar) > 512L * 1024 * 1024
                || !expectedSha256.equals(hash(jar))) { throw invalid("immutable input hash mismatch"); }
        Map<String, Image> images = images(jar);
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
            session = new NativeTelemetryIdentityJdiSession(jar, expectedSha256, pipelineId, images, server, vm);
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

    /** An event-pump barrier, never a VM-wide suspension; open calls are explicit carry-in/carry-out. */
    Boundary boundary(String phase) throws Exception {
        synchronized (commands) {
            check();
            if (phase == null || phase.isBlank() || phase.length() > 128) { throw invalid("invalid phase"); }
            if (!sha.equals(hash(jar))) { throw invalid("immutable artifact changed"); }
            synchronized (lock) {
                if (closing || closed || disconnected) { throw invalid("boundary requested after close"); }
                requestedPhase = phase; command = new CompletableFuture<>();
            }
            CompletableFuture<Boundary> waiting = command;
            try { return waiting.get(30, TimeUnit.SECONDS); }
            catch (Exception | Error problem) { fail(problem); throw problem; }
            finally { command = null; }
        }
    }

    private void loop() {
        try {
            while (running) {
                EventSet set = vm.eventQueue().remove(25);
                if (set != null) { handle(set); }
                CompletableFuture<Boundary> request = command;
                if (request != null) {
                    int drain = 0;
                    EventSet next;
                    while ((next = vm.eventQueue().remove(1)) != null) {
                        if (++drain > 2048) { throw invalid("phase event-drain budget exceeded"); }
                        handle(next);
                    }
                    synchronized (lock) {
                        check(); request.complete(snapshot(requestedPhase, true));
                        records.clear(); phaseBytes = 0; command = null;
                    }
                }
            }
        } catch (Throwable problem) { fail(problem); }
    }

    private void handle(EventSet set) throws Exception {
        long started = System.nanoTime();
        try {
            synchronized (lock) {
                for (Event event : set) {
                    if (++events > MAX_EVENTS) { throw invalid("event budget exceeded"); }
                    if (event instanceof ClassPrepareEvent prepare) { bind(prepare.referenceType()); }
                    else if (event instanceof BreakpointEvent breakpoint) { breakpoint(breakpoint); }
                    else if (event instanceof MethodExitEvent exit) { normalExit(exit); }
                    else if (event instanceof ExceptionEvent exception) { exception(exception); }
                    else if (event instanceof ThreadDeathEvent death) { threadDeath(death); }
                    else if (event instanceof VMDeathEvent) {
                        if (!closing || !threads.isEmpty()) { throw invalid("VM death lost open calls or owned close"); }
                        vmDeath = true;
                    } else if (event instanceof VMDisconnectEvent) {
                        if (!closing || !vmDeath) { throw invalid("disconnect lacked owned VM death"); }
                        disconnected = true; running = false;
                    } else if (!(event instanceof VMStartEvent)) { throw invalid("unmapped capture event"); }
                }
            }
        } finally {
            handlingNanos += System.nanoTime() - started;
            try { set.resume(); }
            catch (VMDisconnectedException gone) { if (!closing || !vmDeath) { throw gone; } }
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
        boolean qualified = spec.pipeline() < 0
                ? producers.contains(receiver.uniqueID()) : pipeline.equals(mirror.text(arguments.get(spec.pipeline())));
        if (site.target() == Target.FOLDER_FORGET) { qualified &= publisherFolders.contains(receiver.uniqueID()); }
        ThreadState state = threads.get(event.thread().uniqueID());
        if (state != null) { reconcile(state, frames, site.entry() ? site.target() : null); }
        if (!qualified) { if (state != null) { removeEmpty(state); } return; }
        if (site.entry()) {
            if (state == null) { state = threadState(event.thread()); }
            if (state.calls.size() >= MAX_CALLS || threads.values().stream()
                    .mapToInt(value -> value.calls.size()).sum() >= MAX_OPEN_CALLS) {
                throw invalid("observed call depth budget exceeded");
            }
            Map<String, Object> entry = new LinkedHashMap<>();
            try {
                entry(spec, receiver, arguments, mirror, entry);
                if (evidenceBytes(entry, 0) > MAX_RECORD_BYTES) {
                    entry.clear();
                    throw new NativeTelemetryMirror.Unavailable("ENTRY_RECORD_BYTE_BUDGET");
                }
            }
            catch (NativeTelemetryMirror.Unavailable missing) {
                decoderUnavailable(entry, spec.target(), "ENTRY", missing.getMessage());
            }
            Call call = new Call(++calls, spec, frames.size(), receiver,
                    Collections.unmodifiableList(new ArrayList<>(arguments)), entry);
            state.calls.push(call); totals.get(site.target()).entries++;
        } else {
            Call call = state == null ? null : state.calls.peek();
            if (call == null || call.spec.target() != site.target() || call.depth != frames.size()
                    || call.receiver.uniqueID() != receiver.uniqueID() || call.escaping || call.exit != null) {
                throw invalid("normal return site lost exact entry correlation");
            }
            MethodExitRequest exit = vm.eventRequestManager().createMethodExitRequest();
            exit.addThreadFilter(event.thread()); exit.addClassFilter(spec.type()); exit.addCountFilter(1);
            exit.putProperty("native-identity-call", call.id);
            exit.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD); call.exit = exit; exit.enable();
        }
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

    private void entry(Spec spec, ObjectReference receiver, List<Value> arguments,
            NativeTelemetryMirror mirror, Map<String, Object> out) throws Exception {
        if (spec.target() == Target.OFFER) {
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
            ObjectReference folder = mirror.object(mirror.field(receiver, "cardinality",
                    "Lio/tapstate/core/lifecycle/CardinalityBudget$Folder;"));
            publisherFolders.add(folder.uniqueID());
            out.put("requestScope", arguments.get(2) == null ? "UNSCOPED" : mirror.scope(arguments.get(2)));
            if (arguments.get(2) == null) { unknown("UNSCOPED_PREPARATION"); }
            out.put("nonNullFailure", arguments.get(1) != null);
        } else if (spec.target() == Target.FOLDER_FORGET) {
            out.put("before", folderNames(receiver, mirror));
        } else if (spec.target() == Target.EXPORT_FORGET || spec.target() == Target.EXPORT_INCARNATION) {
            out.put("beforeNamed", exporterNames(receiver, mirror));
        }
    }

    private void normalExit(MethodExitEvent event) throws Exception {
        ThreadState state = threads.get(event.thread().uniqueID());
        Call call = state == null ? null : state.calls.peek();
        if (call == null || call.exit != event.request()
                || !Objects.equals(event.request().getProperty("native-identity-call"), call.id)
                || !event.method().equals(methods.get(call.spec.target())) || call.escaping) {
            throw invalid("method exit lost its exact return-site correlation");
        }
        vm.eventRequestManager().deleteEventRequest(call.exit); call.exit = null;
        Map<String, Object> out = new LinkedHashMap<>(call.entry);
        try { returned(call, event.returnValue(), state, out, mirror()); }
        catch (NativeTelemetryMirror.Unavailable missing) {
            decoderUnavailable(out, call.spec.target(), "RETURN", missing.getMessage());
        }
        out.put("target", call.spec.target().name()); out.put("invocation", call.id);
        out.put("receiver", call.receiver.uniqueID()); out.put("normalReturn", true);
        addRecord(out);
        totals.get(call.spec.target()).normal++;
        state.calls.pop(); removeEmpty(state);
    }

    private void returned(Call call, Value returned, ThreadState state, Map<String, Object> out,
            NativeTelemetryMirror mirror) throws Exception {
        switch (call.spec.target()) {
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
                jobs.put(nativeId, new CapturedJob(proxy, proxy.uniqueID(), job, scope, call.receiver.uniqueID()));
                out.put("scope", scope); out.put("job", job); out.put("proxy", proxy.uniqueID());
            }
            case OFFER -> {
                Value cached = mirror.lookup(mirror.field(call.receiver, "latest", "Ljava/util/Map;"), pipeline);
                if (cached == null) { out.put("accepted", false); break; }
                Map<String, Object> actual = offered(mirror.object(cached), mirror);
                out.put("cached", actual);
                out.put("accepted", actual.get("scope").equals(call.entry.get("scope"))
                        && actual.get("observedAt").equals(call.entry.get("observedAt"))
                        && actual.get("facts").equals(call.entry.get("facts")));
            }
            case VISIBLE -> {
                if (!(returned instanceof BooleanValue value)) { throw invalid("visible returned no actual boolean"); }
                out.put("included", value.value());
                if (value.value()) {
                    Call produce = state.calls.stream().filter(parent -> parent.spec.target() == Target.PRODUCE
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
            case FOLDER_FORGET -> out.put("after", folderNames(call.receiver, mirror));
            case EXPORT_FORGET, EXPORT_INCARNATION -> {
                out.put("afterNamed", exporterNames(call.receiver, mirror));
                Value cached = mirror.lookup(mirror.field(call.receiver, "latest", "Ljava/util/Map;"), pipeline);
                out.put("cachedAfter", cached == null ? "ABSENT" : offered(mirror.object(cached), mirror));
            }
        }
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
            state.calls.pop(); exceptional(call);
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
        totals.get(call.spec.target()).exceptional++;
    }
    private void threadDeath(ThreadDeathEvent event) {
        ThreadState state = threads.get(event.thread().uniqueID());
        if (state == null) { return; }
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
                    + ", retainedRecords=" + records.size());
        }
        phaseBytes += bytes;
        records.add(Collections.unmodifiableMap(new LinkedHashMap<>(record)));
    }
    private Boundary snapshot(String phase, boolean drained) throws Exception {
        Map<Target, Counts> counts = new EnumMap<>(Target.class);
        Set<String> missing = new LinkedHashSet<>(unverified);
        for (Target target : Target.values()) {
            if (!bindings.containsKey(target)) { missing.add("LIVE_BINDING_UNAVAILABLE:" + target); }
            else if (!disconnected) {
                Method method = methods.get(target);
                Spec spec = specs.get(target);
                if (method.isObsolete() || !Arrays.equals(method.bytecodes(),
                        images.get(spec.type()).methods().get(spec.method() + spec.descriptor()))) {
                    throw invalid("method provenance changed at boundary");
                }
            }
            Totals total = totals.get(target);
            long open = threads.values().stream().flatMap(state -> state.calls.stream())
                    .filter(call -> call.spec.target() == target).count();
            if (total.entries != total.normal + total.exceptional + open) { throw invalid("call accounting mismatch"); }
            counts.put(target, new Counts(total.entries, total.normal, total.exceptional, open));
        }
        if (authorities.isEmpty()) { missing.add("AUTHORITY_RECEIPTS_UNVERIFIED"); }
        for (Map<String, Object> record : records) { qualifyScopes(record, missing, 0); }
        return new Boundary(phase, ++sequence, sha, pipeline, bindings, counts, records, authorities,
                missing, layouts, vmVersion, events, handlingNanos,
                threads.values().stream().mapToInt(state -> state.calls.size()).sum(), drained, vmDeath, disconnected);
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
        failure.compareAndSet(null, error); running = false;
        CompletableFuture<Boundary> waiting = command;
        if (waiting != null) { waiting.completeExceptionally(error); }
        detach();
    }
    private void detach() {
        try { vm.dispose(); }
        catch (VMDisconnectedException gone) { }
        catch (Throwable problem) { server.kill(); }
    }

    Boundary shutdownAndFinish() throws Exception {
        synchronized (commands) {
            if (closed) { check(); if (terminal == null) { throw invalid("no qualified terminal boundary"); } return terminal; }
            Throwable primary = null;
            try {
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
                running = false; detach(); server.close(); pump.interrupt();
                try { pump.join(2000); }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    if (primary != null) { primary.addSuppressed(interrupted); } else { throw interrupted; }
                } finally { closed = true; }
                if (pump.isAlive()) { throw invalid("owned event pump did not stop"); }
            }
        }
    }
    @Override public void close() throws Exception { if (!closed) { shutdownAndFinish(); } }

    private static Map<String, Image> images(Path jar) throws Exception {
        Map<String, Image> images = new LinkedHashMap<>();
        Set<String> wanted = new HashSet<>(SPECS.stream().map(Spec::type).toList());
        wanted.addAll(List.of(OTEL + "FactsMetricProducer$Offered", OTEL + "FactMetricData",
                "io.tapstate.runtime.engine.Engine$ExecutionJob", "io.tapstate.spi.store.StopReservation$JobIdentity",
                "io.tapstate.spi.store.ObservationStore$Scope", "io.tapstate.core.logging.LogSink$Scope",
                "io.tapstate.core.logging.LogLine", "io.tapstate.spi.metrics.MetricsExport$ScopeToken",
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
