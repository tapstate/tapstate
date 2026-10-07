package io.tapstate.e2e;

import com.sun.jdi.*;
import com.sun.jdi.connect.Connector;
import com.sun.jdi.connect.ListeningConnector;
import com.sun.jdi.event.*;
import com.sun.jdi.request.*;
import io.tapstate.core.common.JsonWriter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.*;

/** One owned debugger correlates genuine source delivery, native refusal and the same handle's end. */
final class NativeCaptureCloseJdiSession implements AutoCloseable {
    private static final String PDK = "io.tapstate.adapters.pdk.PdkCapturePort";
    private static final String CONNECTOR = "io.tapstate.adapters.pdk.PdkConnector";
    private static final String OWNERSHIP = "io.tapstate.app.PipelineActuationOwnership";
    private static final String ENGINE = "io.tapstate.runtime.engine.Engine";
    private static final String LOADER = "org.springframework.boot.loader.launch.LaunchedClassLoader";
    private static final int MAX_EVENTS = 20_000, MAX_FRAMES = 512, MAX_REQUESTS = 8;
    private static final int MAX_CLASS_BYTES = 1_048_576, MAX_CLASSES = 128, MAX_CALLS = 64;
    private static final int MAX_RECORDS = 512, MAX_RECORD_BYTES = 65_536, MAX_PHASE_BYTES = 2 * 1024 * 1024;
    private static final Duration MAX_WAIT = Duration.ofMinutes(2);
    private record Spec(String type, String name, String signature) { }
    private static final List<Spec> SPECS = List.of(
            new Spec(PDK, "cdc", "(Lio/tapstate/spi/capture/CaptureConfig;Lio/tapstate/spi/capture/CaptureStart;Lio/tapstate/spi/capture/CaptureListener;)Lio/tapstate/spi/capture/Subscription;"),
            new Spec(PDK, "handOver", "(Lio/tapstate/adapters/pdk/PdkConnector;Ljava/util/Map;Lio/tapstate/spi/capture/CaptureListener;Ljava/util/List;Ljava/lang/Object;)V"),
            new Spec(PDK, "shutDown", "(Lio/tapstate/adapters/pdk/PdkConnector;Ljava/lang/Thread;Lio/tapstate/adapters/pdk/PdkCapturePort$CdcDelivery;J)V"),
            new Spec(PDK, "awaitCaptureEnd", "(Ljava/lang/Thread;Lio/tapstate/adapters/pdk/PdkCapturePort$CdcDelivery;J)Z"),
            new Spec(PDK + "$1", "close", "()V"), new Spec(CONNECTOR, "stopStrict", "()V"),
            new Spec(OWNERSHIP, "beginExecution", "(Ljava/lang/String;)Lio/tapstate/app/PipelineActuationOwnership$Execution;"),
            new Spec(ENGINE, "executionJob", "(Ljava/lang/String;Lcom/hazelcast/jet/Job;)Lio/tapstate/runtime/engine/Engine$ExecutionJob;"));
    private record Image(String origin, Map<String, byte[]> methods, Map<String, String> fields) { }
    private record Bound(Method method, List<Integer> returns, Map<String, Object> evidence) { }
    private record Call(Spec spec, ThreadReference thread, ObjectReference receiver, List<Value> arguments,
            int depth, List<String> callers, boolean qualified, long order, long began, Map<String, Object> entry) { }
    private record Tail(ObjectReference subscription, ObjectReference connector, ThreadReference reader,
            ObjectReference delivery, ObjectReference listener, Map<String, Object> scope, Map<String, Object> evidence) { }
    private final Path jar;
    private final String sha, pipeline, source, table;
    private final RealProcessServer server;
    private final VirtualMachine vm;
    private final Map<String, Image> images;
    private final Map<Spec, Bound> bound = new HashMap<>();
    private final Map<Long, Deque<Call>> calls = new HashMap<>();
    private final List<EventRequest> requests = new ArrayList<>();
    private final List<Map<String, Object>> records = new ArrayList<>();
    private final Set<String> layouts = new HashSet<>(), validated = new HashSet<>();
    private final Object lock = new Object();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final Thread pump;
    private volatile boolean running = true, closing;
    private String lastEvent = "NONE", lastPreparedType = "NONE";
    private boolean vmStartObserved, vmStartResumed;
    private enum Phase { STARTUP, WARM, DRAINING, NATIVE_CLOSE, QUIET }
    private Phase phase = Phase.STARTUP;
    private boolean observationEnabled, closePhaseRequested;
    private CompletableFuture<Void> phaseReady;
    private final Map<Spec, BreakpointRequest> entryRequests = new HashMap<>();
    private final Map<Long, MethodExitRequest> threadExits = new HashMap<>();
    private final Map<Long, String> threadExitFilters = new HashMap<>();
    private ClassPrepareRequest preparation;
    private ExceptionRequest exceptions;
    private int peakLiveRequests;
    private long createdRequests;
    private long loaderId, events, order, bytes, ignoredUnmatchedEntries, loopHeaderRevisits;
    private Tail tail;
    private Map<String, Object> admission, job, armedScope, heldEvidence, refusal, unresolved;
    private EventSet held;
    private ThreadReference heldThread;
    private long shutdownBegan, shutdownBudget;
    private boolean strictEntered, strictReturned, shutdownReturned, subscriptionReturned, heldReleased, heldCallConcluded;
    private final Map<Long, List<Long>> waits = new HashMap<>();
    private final Set<Long> finalFalseParents = new HashSet<>();

    private NativeCaptureCloseJdiSession(Path jar, String sha, String pipeline, String source, String table,
            Map<String, Image> images, RealProcessServer server, VirtualMachine vm) {
        this.jar = jar; this.sha = sha; this.pipeline = pipeline; this.source = source; this.table = table;
        this.images = images; this.server = server; this.vm = vm;
        pump = new Thread(this::loop, "native-capture-close-jdi"); pump.setDaemon(true);
    }

    static NativeCaptureCloseJdiSession start(Path selected, String expectedSha, String pipeline, String source,
            String table, NativeTelemetryIdentityJdiSession.OwnedLauncher launcher) throws Exception {
        Path jar = selected.toRealPath();
        if (!Files.isRegularFile(jar) || Files.size(jar) > 512L * 1024 * 1024 || !hash(jar).equals(expectedSha)) {
            throw invalid("immutable application identity unavailable");
        }
        Map<String, Image> images = images(jar);
        if (!hash(jar).equals(expectedSha)) { throw invalid("application changed while reading classes"); }
        ListeningConnector connector = Bootstrap.virtualMachineManager().listeningConnectors().stream()
                .filter(value -> value.name().equals("com.sun.jdi.SocketListen")).findFirst().orElseThrow();
        Map<String, Connector.Argument> arguments = connector.defaultArguments();
        arguments.get("localAddress").setValue("127.0.0.1"); arguments.get("port").setValue("0");
        arguments.get("timeout").setValue("15000");
        String listening = connector.startListening(arguments); boolean listeningOpen = true;
        RealProcessServer server = null; VirtualMachine vm = null; NativeCaptureCloseJdiSession session = null;
        Throwable primary = null;
        try {
            String address = BenchmarkJdiCostObserver.numericLoopbackDialAddress(arguments.get("localAddress").value(),
                    listening, arguments.get("port").value());
            server = launcher.launch(jar, List.of("-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=" + address));
            vm = connector.accept(arguments); connector.stopListening(arguments); listeningOpen = false;
            if (!vm.canGetBytecodes() || !vm.canGetMethodReturnValues()) { throw invalid("genuine Code/return values unavailable"); }
            session = new NativeCaptureCloseJdiSession(jar, expectedSha, pipeline, source, table, images, server, vm);
            session.install(); session.pump.start();
            try { server.awaitHealthy(); session.check(); }
            catch (Exception | Error startup) {
                startup.addSuppressed(invalid("startup observer evidence: " + JsonWriter.write(session.evidence())));
                throw startup;
            }
            return session;
        } catch (Exception | Error problem) {
            primary = problem;
            if (session != null) {
                try { session.close(); } catch (Exception | Error cleanup) { if (cleanup != problem) { problem.addSuppressed(cleanup); } }
                try { server.close(); } catch (RuntimeException | Error cleanup) { if (cleanup != problem) { problem.addSuppressed(cleanup); } }
            }
            else {
                if (vm != null) { try { vm.dispose(); } catch (VMDisconnectedException ignored) { } }
                if (server != null) { try { server.close(); } catch (RuntimeException | Error cleanup) { problem.addSuppressed(cleanup); } }
            }
            throw problem;
        } finally {
            try { if (listeningOpen) { connector.stopListening(arguments); } }
            catch (Exception cleanup) { if (primary != null) { primary.addSuppressed(cleanup); } else { throw cleanup; } }
        }
    }

    RealProcessServer server() { return server; }
    /** Warm sites are armed before the first actual START; no global method event is enabled. */
    void enableObservation() {
        synchronized (lock) {
            check();
            if (closing || phase != Phase.STARTUP || tail != null || admission != null || job != null || !calls.isEmpty()) {
                throw invalid("warm observation must precede actual capture/admission");
            }
            phase = Phase.WARM; observationEnabled = true; exceptions.enable();
            for (Spec spec : SPECS) { if (warm(spec) && bound.containsKey(spec)) { installEntry(spec); } }
        }
    }

    /** Fence warm entries, drain their actual event queue/stacks, then select only the old native handle. */
    void finishWarmObservation(boolean selectedNativeHandle, long deadline) throws Exception {
        CompletableFuture<Void> ready;
        synchronized (lock) {
            check();
            if (closing || phase != Phase.WARM || phaseReady != null) { throw invalid("invalid repeated warm phase transition"); }
            if (selectedNativeHandle && (tail == null || admission == null || job == null || bound.size() != SPECS.size())) {
                throw invalid("the selected native phase requires every actual prepared site and warm receipt");
            }
            for (Spec spec : new ArrayList<>(entryRequests.keySet())) { delete(entryRequests.remove(spec)); }
            phase = Phase.DRAINING; closePhaseRequested = selectedNativeHandle; ready = phaseReady = new CompletableFuture<>();
        }
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0 || remaining > MAX_WAIT.toNanos()) { throw invalid("original warm drain deadline unavailable"); }
        try { ready.get(remaining, TimeUnit.NANOSECONDS); }
        catch (TimeoutException timeout) { throw invalid("actual warm calls/event queue did not drain within original deadline", timeout); }
        check();
    }
    private static boolean warm(Spec spec) { return spec.name().equals("cdc") || spec.name().equals("beginExecution") || spec.name().equals("executionJob"); }
    private void finishDrainedWarmPhase() {
        if (phase != Phase.DRAINING || !calls.isEmpty() || !threadExits.isEmpty()) { return; }
        delete(preparation); preparation = null;
        if (closePhaseRequested) {
            if (bound.size() != SPECS.size()) { throw invalid("native phase cannot bind an unprepared target"); }
            phase = Phase.NATIVE_CLOSE;
            for (Spec spec : SPECS) { if (!warm(spec)) { installEntry(spec); } }
        } else { delete(exceptions); exceptions = null; phase = Phase.QUIET; }
        phaseReady.complete(null); lock.notifyAll();
    }

    Optional<Map<String, Object>> admission() { synchronized (lock) { check(); return Optional.ofNullable(admission); } }
    Optional<Map<String, Object>> job() { synchronized (lock) { check(); return Optional.ofNullable(job); } }
    Optional<Map<String, Object>> tail() { synchronized (lock) { check(); return Optional.ofNullable(tail).map(Tail::evidence); } }
    void arm(Map<String, Object> scope, long deadline) { synchronized (lock) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0 || remaining > MAX_WAIT.toNanos()) { throw invalid("original observer deadline unavailable"); }
        check(); if (phase != Phase.NATIVE_CLOSE || armedScope != null || held != null || tail == null || !tail.scope().equals(scope)) { throw invalid("unqualified or repeated arm"); }
        if (job == null || !scope.equals(job.get("scope")) || admission == null) { throw invalid("native Job/admission not qualified"); }
        Map<?, ?> actualClaim = (Map<?, ?>) admission.get("claim");
        if (!actualClaim.get("executionGeneration").equals(scope.get("generation"))
                || !((Map<?, ?>) actualClaim.get("key")).get("clusterId").equals(((Map<?, ?>) job.get("job")).get("clusterId"))) {
            throw invalid("actual claim and native Job scopes disagree");
        }
        armedScope = Map.copyOf(scope);
    } }
    Optional<Map<String, Object>> held() { synchronized (lock) { check(); return Optional.ofNullable(heldEvidence); } }
    Optional<Map<String, Object>> outcome() { synchronized (lock) {
        check(); if (refusal != null) { return Optional.of(refusal); }
        if (strictEntered && !strictReturned && shutdownBegan > 0 && System.nanoTime() - shutdownBegan >= shutdownBudget) {
            if (unresolved == null) { unresolved = Map.of("status", "UNRESOLVED_STOP_STRICT", "shutdownEntryAtNanos", shutdownBegan,
                    "elapsedNanos", System.nanoTime() - shutdownBegan, "heldDeliveryReleased", heldReleased); }
            return Optional.of(unresolved);
        }
        return Optional.empty();
    } }
    void release() { synchronized (lock) {
        if (held != null) {
            EventSet actual = held;
            try { actual.resume(); held = null; heldReleased = true; }
            catch (VMDisconnectedException ended) { held = null; if (!closing) { failure.compareAndSet(null, ended); } }
        }
    } }
    boolean sameHandleEnded() throws Exception { synchronized (lock) {
        check(); if (tail == null || !heldReleased || !heldCallConcluded || !shutdownReturned || !subscriptionReturned) { return false; }
        NativeTelemetryMirror mirror = mirror();
        boolean ended = Boolean.TRUE.equals(mirror.scalar(mirror.field(tail.subscription(), "ended", "Z")));
        return ended && tail.reader().status() == ThreadReference.THREAD_STATUS_ZOMBIE
                && mirror.map(mirror.field(tail.delivery(), "active", "Ljava/util/Map;")).isEmpty()
                && mirror.map(mirror.field(tail.delivery(), "acknowledging", "Ljava/util/Map;")).isEmpty();
    } }
    Map<String, Object> evidence() { synchronized (lock) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("events", events); result.put("requestCount", requests.size()); result.put("records", List.copyOf(records));
        result.put("peakLiveRequests", peakLiveRequests); result.put("createdRequests", createdRequests);
        result.put("observationPhase", phase.name()); result.put("liveThreadExitRequests", threadExits.size());
        Map<String, String> filters = new LinkedHashMap<>();
        threadExitFilters.forEach((thread, filter) -> filters.put(Long.toString(thread), filter));
        result.put("threadExitFilters", Map.copyOf(filters));
        result.put("applicationSha256", sha); result.put("bindings", bound.values().stream().map(Bound::evidence).toList());
        result.put("layouts", layouts.stream().sorted().toList()); result.put("heldReleased", heldReleased);
        result.put("remainingCalls", calls.values().stream().mapToInt(Deque::size).sum());
        result.put("ignoredUnmatchedEntries", ignoredUnmatchedEntries); result.put("loopHeaderRevisits", loopHeaderRevisits);
        result.put("failure", failure.get() == null ? "ABSENT" : failure.get().toString());
        result.put("lastEvent", lastEvent); result.put("lastPreparedType", lastPreparedType);
        result.put("vmStartObserved", vmStartObserved); result.put("vmStartResumed", vmStartResumed);
        result.put("observationEnabled", observationEnabled);
        result.put("eventPumpState", pump.getState().name());
        result.put("eventPumpStack", Arrays.stream(pump.getStackTrace()).limit(8).map(StackTraceElement::toString).toList());
        return Collections.unmodifiableMap(result);
    } }
    private void check() { Throwable problem = failure.get(); if (problem != null) { throw invalid("passive capture failed", problem); } }
    private NativeTelemetryMirror mirror() { return new NativeTelemetryMirror(this::validateType, layouts); }

    private void install() throws Exception {
        var manager = vm.eventRequestManager();
        preparation = manager.createClassPrepareRequest(); preparation.addClassFilter("io.tapstate.*"); add(preparation, true);
        exceptions = manager.createExceptionRequest(null, true, true); exceptions.addClassFilter("io.tapstate.*"); add(exceptions, false);
        for (ReferenceType type : vm.allClasses()) { bind(type); }
    }
    private void capacity(String next) {
        if (requests.size() >= MAX_REQUESTS) { throw invalid("LIVE_REQUEST_PEAK: phase=" + phase + ", live=" + requests.size()
                + ", activeThreads=" + threadExits.size() + ", requested=" + next + ", maximum=" + MAX_REQUESTS); }
    }
    private void add(EventRequest request, boolean enabled) {
        capacity(request.getClass().getSimpleName());
        request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD); requests.add(request); createdRequests++;
        peakLiveRequests = Math.max(peakLiveRequests, requests.size()); if (enabled) { request.enable(); }
    }
    private void delete(EventRequest request) {
        if (request == null) { return; }
        request.disable(); vm.eventRequestManager().deleteEventRequest(request); requests.remove(request);
    }
    private void installEntry(Spec spec) {
        if (entryRequests.containsKey(spec)) { return; }
        capacity("ENTRY:" + spec.name());
        Location location = bound.get(spec).method().locationOfCodeIndex(0);
        if (location == null || location.codeIndex() != 0) { throw invalid("exact entry unavailable"); }
        BreakpointRequest entry = vm.eventRequestManager().createBreakpointRequest(location);
        entry.putProperty("source-close-entry", spec); add(entry, true); entryRequests.put(spec, entry);
    }
    private void observeThreadExit(ThreadReference thread, Spec spec) {
        String filter = phase == Phase.NATIVE_CLOSE ? "io.tapstate.adapters.pdk.Pdk*" : spec.type();
        if (threadExits.containsKey(thread.uniqueID())) {
            String existing = threadExitFilters.get(thread.uniqueID());
            if (!existing.equals(spec.type()) && !(existing.endsWith("*")
                    && spec.type().startsWith(existing.substring(0, existing.length() - 1)))) {
                throw invalid("observed nested entry is outside its original return class filter");
            }
            return;
        }
        capacity("THREAD_EXIT:" + thread.uniqueID());
        MethodExitRequest result = vm.eventRequestManager().createMethodExitRequest();
        result.addThreadFilter(thread); result.addClassFilter(filter);
        result.putProperty("source-close-thread", thread.uniqueID()); add(result, true);
        threadExits.put(thread.uniqueID(), result); threadExitFilters.put(thread.uniqueID(), filter);
    }
    private void drainedThread(long id) {
        calls.remove(id); delete(threadExits.remove(id));
        threadExitFilters.remove(id);
    }
    private void bind(ReferenceType type) throws Exception {
        for (Spec spec : SPECS) {
            if (!type.name().equals(spec.type()) || bound.containsKey(spec)) { continue; }
            validateType(type); List<Method> found = type.methodsByName(spec.name(), spec.signature());
            if (found.size() != 1) { throw invalid("pinned method missing/ambiguous"); }
            Method method = found.getFirst(); byte[] code = images.get(spec.type()).methods().get(spec.name() + spec.signature());
            if (code == null || method.isNative() || method.isAbstract() || method.isObsolete() || !Arrays.equals(code, method.bytecodes())) {
                throw invalid("live method differs from pinned Code");
            }
            List<Integer> returns = BenchmarkJdiCostObserver.returnOffsets(code);
            if (returns.isEmpty()) { throw invalid("normal return instructions unavailable"); }
            Map<String, Object> evidence = Map.of("type", spec.type(), "name", spec.name(), "signature", spec.signature(),
                    "origin", images.get(spec.type()).origin(), "codeSha256", hash(code), "loaderId", loaderId, "normalReturns", returns);
            bound.put(spec, new Bound(method, returns, evidence));
            if (phase == Phase.WARM && warm(spec)) { installEntry(spec); }
        }
    }
    private void validateType(ReferenceType type) throws Exception {
        if (type.name().startsWith("java.")) { return; }
        if (type.classLoader() == null || !LOADER.equals(type.classLoader().referenceType().name())) { throw invalid("unexpected app loader"); }
        if (loaderId == 0) { loaderId = type.classLoader().uniqueID(); }
        if (loaderId != type.classLoader().uniqueID()) { throw invalid("mixed app loaders"); }
        String key = type.name() + ":" + loaderId;
        if (validated.contains(key)) { return; }
        Image image = images.get(type.name());
        if (image == null) { throw invalid("decoded model not pinned: " + type.name()); }
        for (var field : image.fields().entrySet()) {
            Field actual = type.fieldByName(field.getKey());
            if (actual == null || !actual.signature().equals(field.getValue())) { throw invalid("model field layout differs"); }
        }
        validated.add(key); if (validated.size() > MAX_CLASSES) { throw invalid("class layout budget exceeded"); }
    }
    private Spec spec(Method method) { return SPECS.stream().filter(value -> value.type().equals(method.declaringType().name())
            && value.name().equals(method.name()) && value.signature().equals(method.signature())).findFirst().orElse(null); }
    private List<StackFrame> frames(ThreadReference thread) throws Exception {
        List<StackFrame> result = thread.frames(); if (result.isEmpty() || result.size() > MAX_FRAMES) { throw invalid("stack unavailable/unbounded"); }
        return result;
    }
    private List<String> callers(List<StackFrame> frames) { return frames.stream().skip(1).map(frame -> {
        Method m = frame.location().method(); return m.declaringType().name() + "." + m.name() + m.signature(); }).toList(); }
    private void loop() {
        try {
            while (running) {
                EventSet set = vm.eventQueue().remove(250);
                if (set == null) { synchronized (lock) { finishDrainedWarmPhase(); } continue; }
                boolean park = false, starting = false;
                try { synchronized (lock) {
                    for (Event event : set) {
                        if (++events > MAX_EVENTS) { throw invalid("event budget exceeded"); }
                        lastEvent = event.getClass().getSimpleName();
                        if (event instanceof VMStartEvent) { vmStartObserved = true; starting = true; }
                        else if (event instanceof ClassPrepareEvent prepared) {
                            lastPreparedType = prepared.referenceType().name(); bind(prepared.referenceType());
                        }
                        else if (event instanceof BreakpointEvent entry) { park |= entry(entry.thread(), entry.location(), set); }
                        else if (event instanceof MethodExitEvent exit) { returned(exit); }
                        else if (event instanceof ExceptionEvent exceptional) { exceptional(exceptional); }
                        else if (event instanceof VMDeathEvent || event instanceof VMDisconnectEvent) {
                            if (!closing) { throw invalid("owned VM ended before drain"); } running = false;
                        }
                    }
                    lock.notifyAll();
                } } finally {
                    if (!park) {
                        set.resume();
                        if (starting) { synchronized (lock) { vmStartResumed = true; } }
                    }
                }
            }
        } catch (InterruptedException ended) { if (!closing) { failure.compareAndSet(null, ended); } Thread.currentThread().interrupt(); }
        catch (VMDisconnectedException ended) { if (!closing) { failure.compareAndSet(null, ended); } }
        catch (Exception | Error problem) {
            failure.compareAndSet(null, problem);
            try { release(); } catch (RuntimeException | Error cleanup) { problem.addSuppressed(cleanup); }
            synchronized (lock) {
                for (EventRequest request : requests) {
                    try { request.disable(); } catch (VMDisconnectedException ended) { }
                    catch (RuntimeException | Error cleanup) { problem.addSuppressed(cleanup); }
                }
                try { vm.dispose(); } catch (VMDisconnectedException ended) { }
                catch (RuntimeException | Error cleanup) { problem.addSuppressed(cleanup); }
                if (phaseReady != null && !phaseReady.isDone()) { phaseReady.completeExceptionally(problem); }
                running = false;
            }
        }
    }
    private boolean entry(ThreadReference thread, Location location, EventSet set) throws Exception {
        Spec spec = spec(location.method()); if (spec == null) { return false; }
        Bound pinned = bound.get(spec); if (pinned == null || !pinned.method().equals(location.method()) || location.codeIndex() != 0
                || pinned.method().isObsolete() || !Arrays.equals(images.get(spec.type()).methods().get(spec.name() + spec.signature()), pinned.method().bytecodes())) {
            throw invalid("entry binding missing or not exact");
        }
        List<StackFrame> stack = frames(thread); StackFrame top = stack.getFirst(); List<Value> args = Collections.unmodifiableList(new ArrayList<>(top.getArgumentValues()));
        NativeTelemetryMirror mirror = mirror(); ObjectReference receiver = top.thisObject();
        boolean qualified = false; Map<String, Object> data = new LinkedHashMap<>();
        if (spec.type().equals(OWNERSHIP) || spec.type().equals(ENGINE)) {
            qualified = pipeline.equals(mirror.text(args.getFirst()));
        } else if (spec.name().equals("cdc")) {
            Value nativeNode = mirror.field(mirror.object(args.getFirst()), "node", "Lio/tapstate/core/model/PipelineNode;");
            ObjectReference node = nativeNode == null ? null : mirror.object(nativeNode);
            qualified = node != null && pipeline.equals(mirror.text(mirror.field(node, "pipelineId", "Ljava/lang/String;")))
                    && source.equals(mirror.text(mirror.field(node, "nodeId", "Ljava/lang/String;")));
            if (qualified) {
                ObjectReference config = mirror.object(args.getFirst());
                if (!"mongodb".equals(mirror.text(mirror.field(config, "connectorId", "Ljava/lang/String;")))) { throw invalid("expected genuine Mongo source"); }
                List<String> streams = new ArrayList<>(); for (Value stream : mirror.sequence(mirror.field(config, "streams", "Ljava/util/List;"))) { streams.add(mirror.text(stream)); }
                if (!streams.equals(List.of(table))) { throw invalid("source stream selection differs"); }
                data.put("scope", mirror.scope(mirror.field(receiver, "logScope", "Lio/tapstate/core/logging/LogSink$Scope;")));
                data.put("listenerId", mirror.object(args.get(2)).uniqueID()); data.put("configObjectId", config.uniqueID());
            }
        } else if (tail != null) {
            if (spec.name().equals("handOver") || spec.name().equals("shutDown")) {
                qualified = mirror.object(args.getFirst()).uniqueID() == tail.connector().uniqueID();
            } else if (spec.name().equals("awaitCaptureEnd")) {
                qualified = mirror.object(args.getFirst()).uniqueID() == tail.reader().uniqueID()
                        && mirror.object(args.get(1)).uniqueID() == tail.delivery().uniqueID();
            } else if (spec.name().equals("close")) { qualified = receiver != null && receiver.uniqueID() == tail.subscription().uniqueID(); }
            else if (spec.name().equals("stopStrict")) {
                qualified = receiver != null && receiver.uniqueID() == tail.connector().uniqueID()
                        && calls.getOrDefault(thread.uniqueID(), new ArrayDeque<>()).stream()
                                .anyMatch(call -> call.qualified() && call.spec().name().equals("shutDown"));
            }
        }
        if (qualified && tail != null && !spec.name().equals("cdc")) {
            data.put("tail", tail.evidence());
            if (spec.name().equals("shutDown")) { data.put("actualGraceMillis", mirror.integral(args.get(3))); }
            if (spec.name().equals("awaitCaptureEnd")) { data.put("actualDeadlineNanos", mirror.integral(args.get(2))); }
        }
        Deque<Call> existingCalls = calls.get(thread.uniqueID());
        Call existing = existingCalls == null ? null : existingCalls.peek();
        if (existing != null && existing.spec().equals(spec) && existing.depth() == stack.size()
                && Objects.equals(existing.receiver(), receiver) && existing.arguments().equals(args)
                && existing.callers().equals(callers(stack))) {
            byte[] code = images.get(spec.type()).methods().get(spec.name() + spec.signature());
            // The pinned wait loop branches from offset 32 back to its first instruction.
            if (!spec.name().equals("awaitCaptureEnd") || code.length <= 34 || (code[32] & 255) != 0xa7
                    || (short) (((code[33] & 255) << 8) | (code[34] & 255)) != -32) {
                throw invalid("repeated entry has no verified same-frame loop backedge");
            }
            loopHeaderRevisits++;
            if (existing.qualified()) { record("LOOP_HEADER_REVISIT", existing,
                    Map.of("codeIndex", 0, "backedgeCodeIndex", 32, "sameOriginalEntry", existing.order())); }
            return false;
        }
        if (!qualified && !threadExits.containsKey(thread.uniqueID())) { ignoredUnmatchedEntries++; return false; }
        observeThreadExit(thread, spec);
        long id = ++order;
        Call call = new Call(spec, thread, receiver, args, stack.size(), callers(stack), qualified, id, System.nanoTime(), Collections.unmodifiableMap(data));
        Deque<Call> active = calls.computeIfAbsent(thread.uniqueID(), ignored -> new ArrayDeque<>()); active.push(call);
        if (calls.values().stream().mapToInt(Deque::size).sum() > MAX_CALLS) { throw invalid("open call budget exceeded"); }
        if (!qualified) { return false; }
        record("ENTRY", call, data);
        if (spec.name().equals("shutDown")) {
            if (mirror.object(args.get(1)).uniqueID() != tail.reader().uniqueID()
                    || mirror.object(args.get(2)).uniqueID() != tail.delivery().uniqueID()) { throw invalid("shutdown substituted an actual handle"); }
            if (shutdownBegan == 0) {
                shutdownBegan = call.began();
                long grace = mirror.integral(args.get(3));
                Field join = location.method().declaringType().fieldByName("SHUTDOWN_JOIN_MILLIS");
                long extra = mirror.integral(location.method().declaringType().getValue(join));
                shutdownBudget = TimeUnit.MILLISECONDS.toNanos(Math.addExact(grace, extra));
            }
        } else if (spec.name().equals("stopStrict") && held != null) { strictEntered = true; }
        if (!spec.name().equals("handOver") || armedScope == null || heldEvidence != null) { return false; }
        if (mirror.object(args.get(2)).uniqueID() != tail.listener().uniqueID() || !tail.scope().equals(armedScope)) { throw invalid("delivery owner/listener changed"); }
        ObjectReference delivery = null;
        for (StackFrame frame : stack) {
            Method m = frame.location().method();
            if (m.declaringType().name().equals(PDK + "$CdcDelivery") && m.name().equals("accept") && m.signature().equals("(Ljava/lang/Runnable;)V")) {
                validateType(m.declaringType()); byte[] code = images.get(m.declaringType().name()).methods().get(m.name() + m.signature());
                if (!Arrays.equals(code, m.bytecodes())) { throw invalid("active delivery caller differs from pinned Code"); }
                delivery = frame.thisObject(); break;
            }
        }
        if (delivery == null || delivery.uniqueID() != tail.delivery().uniqueID()) { throw invalid("handOver lacks its actual active delivery"); }
        List<Value> batch = mirror.sequence(args.get(3)); boolean change = false;
        for (Value value : batch) {
            ReferenceType type = mirror.object(value).referenceType(); boolean control = false;
            while (type instanceof ClassType klass) {
                if (type.name().equals("io.tapdata.entity.event.control.ControlEvent")) { control = true; break; }
                type = klass.superclass();
            }
            if (!control) { change = true; }
        }
        if (!change) { return false; }
        long depth = 0;
        for (NativeTelemetryMirror.Pair pair : mirror.map(mirror.field(delivery, "active", "Ljava/util/Map;"))) {
            if (mirror.object(pair.key()).uniqueID() == thread.uniqueID()) { depth = mirror.integral(pair.value()); }
        }
        if (depth <= 0 || Boolean.TRUE.equals(mirror.scalar(mirror.field(delivery, "closed", "Z")))) { throw invalid("native delivery not actually active"); }
        if (set.suspendPolicy() != EventRequest.SUSPEND_EVENT_THREAD) { throw invalid("hold must suspend only the genuine delivery thread"); }
        held = set; heldThread = thread;
        heldEvidence = Map.of("status", "QUALIFIED_ACTIVE_NATIVE_DELIVERY_ENTRY", "threadId", thread.uniqueID(),
                "deliveryId", delivery.uniqueID(), "activeDepth", depth, "entryOrder", id,
                "scope", armedScope, "batchSize", batch.size(), "offsetObjectId", args.get(4) == null ? "ABSENT" : mirror.object(args.get(4)).uniqueID(),
                "tail", tail.evidence());
        return true;
    }

    private void returned(MethodExitEvent event) throws Exception {
        Spec spec = spec(event.method()); if (spec == null) { return; }
        Deque<Call> active = calls.get(event.thread().uniqueID());
        boolean entryActive = entryRequests.containsKey(spec);
        MethodExitRequest actualRequest = threadExits.get(event.thread().uniqueID());
        if (event.request() != actualRequest) {
            if (entryActive || active != null && active.stream().anyMatch(call -> call.spec().equals(spec))) {
                throw invalid("subject return is not from its original thread-filtered exit request");
            }
            return;
        }
        if (active == null || active.isEmpty()) {
            if (entryActive) { throw invalid("active entry site returned without its original captured call"); }
            return;
        }
        List<StackFrame> stack = frames(event.thread());
        Call call = active.stream().filter(value -> value.spec().equals(spec) && value.depth() == stack.size()
                && (value.receiver() == null || stack.getFirst().thisObject() != null
                        && value.receiver().uniqueID() == stack.getFirst().thisObject().uniqueID()))
                .findFirst().orElse(null);
        // A common filter sees inactive sites too; only an exact original entry permits a subject return.
        if (call == null) {
            if (entryActive) { throw invalid("active entry site returned without its exact original frame"); }
            return;
        }
        if (active.peek() != call) { throw invalid("normal return crosses an original nested observed call"); }
        if (!call.spec().equals(spec) || event.method().isObsolete()
                || !Arrays.equals(images.get(spec.type()).methods().get(spec.name() + spec.signature()), event.method().bytecodes())
                || stack.size() != call.depth() || !call.callers().equals(callers(stack))
                || !bound.get(spec).returns().contains(Math.toIntExact(event.location().codeIndex()))
                || call.receiver() != null && (stack.getFirst().thisObject() == null
                        || call.receiver().uniqueID() != stack.getFirst().thisObject().uniqueID())) {
            throw invalid("entry/normal return identity differs");
        }
        if (call.qualified()) {
            NativeTelemetryMirror mirror = mirror(); Map<String, Object> result = new LinkedHashMap<>();
            if (spec.name().equals("beginExecution")) {
                ObjectReference actual = mirror.object(event.returnValue());
                if (Boolean.TRUE.equals(mirror.scalar(mirror.field(actual, "allowed", "Z")))) {
                    ObjectReference optional = mirror.object(mirror.field(actual, "admittedClaim", "Ljava/util/Optional;"));
                    Value issued = mirror.field(optional, "value", "Ljava/lang/Object;");
                    if (issued == null) { throw invalid("ordinary claimed admission lacks its actual receipt"); }
                    Map<String, Object> claim = workloadClaim(issued, mirror);
                    if (!pipeline.equals(((Map<?, ?>) claim.get("key")).get("resourceId"))
                            || !"PIPELINE_ACTUATION".equals(((Map<?, ?>) claim.get("key")).get("type"))) { throw invalid("wrong admitted authority"); }
                    Map<String, Object> owner = workloadOwner(mirror.field(call.receiver(), "owner", "Lio/tapstate/spi/store/WorkloadOwner;"), mirror);
                    if (!owner.equals(claim.get("owner")) || !mirror.text(mirror.field(call.receiver(), "clusterId", "Ljava/lang/String;"))
                            .equals(((Map<?, ?>) claim.get("key")).get("clusterId"))) { throw invalid("admission receipt owner differs"); }
                    ObjectReference fence = mirror.object(mirror.field(actual, "fence", "Lio/tapstate/app/ExecutionFence;"));
                    List<String> members = executionMembers(mirror.field(actual, "executionNodeIds", "Ljava/util/Set;"), mirror);
                    if (!pipeline.equals(mirror.text(mirror.field(fence, "pipelineId", "Ljava/lang/String;")))
                            || !claim.get("claimGeneration").equals(mirror.integral(mirror.field(fence, "claimGeneration", "J")))
                            || !claim.get("executionGeneration").equals(mirror.integral(mirror.field(fence, "executionGeneration", "J")))
                            || !claim.get("executionGeneration").equals(claim.get("contextExecutionGeneration"))
                            || !claim.get("claimGeneration").equals(claim.get("executionClaimGeneration"))
                            || members.isEmpty() || !members.equals(claim.get("executionNodeIds"))) { throw invalid("actual admission context differs"); }
                    admission = Map.of("claim", claim, "executionObjectId", actual.uniqueID(), "entryOrder", call.order(),
                            "returnOrder", ++order, "normalReturn", true);
                    result.putAll(admission);
                } else { result.put("status", "REFUSED"); }
            } else if (spec.name().equals("executionJob")) {
                ObjectReference actual = mirror.object(event.returnValue());
                Map<String, Object> scope = mirror.scope(mirror.field(actual, "scope", "Lio/tapstate/spi/store/ObservationStore$Scope;"));
                Map<String, Object> identity = mirror.job(mirror.field(actual, "job", "Lio/tapstate/spi/store/StopReservation$JobIdentity;"));
                job = Map.of("scope", scope, "job", identity, "nativeJobObjectId", mirror.object(call.arguments().get(1)).uniqueID(),
                        "executionJobObjectId", actual.uniqueID(), "entryOrder", call.order(), "returnOrder", ++order, "normalReturn", true);
                result.putAll(job);
            } else if (spec.name().equals("cdc")) {
                ObjectReference actual = mirror.object(event.returnValue()); validateType(actual.referenceType());
                if (!actual.referenceType().name().equals(PDK + "$1")) { throw invalid("unexpected real subscription type"); }
                ObjectReference connector = mirror.object(mirror.field(actual, "val$connector", "Lio/tapstate/adapters/pdk/PdkConnector;"));
                ObjectReference delivery = mirror.object(mirror.field(actual, "val$delivery", "Lio/tapstate/adapters/pdk/PdkCapturePort$CdcDelivery;"));
                ObjectReference reader = mirror.object(mirror.field(actual, "val$thread", "Ljava/lang/Thread;"));
                ObjectReference ack = mirror.object(mirror.field(actual, "val$acknowledgements", "Lio/tapstate/adapters/pdk/PdkCapturePort$Acknowledgements;"));
                ObjectReference listener = mirror.object(mirror.field(ack, "listener", "Lio/tapstate/spi/capture/CaptureListener;"));
                if (!(reader instanceof ThreadReference nativeThread) || listener.uniqueID() != mirror.object(call.arguments().get(2)).uniqueID()
                        || connector.uniqueID() != mirror.object(mirror.field(ack, "connector", "Lio/tapstate/adapters/pdk/PdkConnector;")).uniqueID()
                        || !pipeline.equals(mirror.text(mirror.field(connector, "pipelineId", "Ljava/lang/String;")))
                        || !"mongodb".equals(mirror.text(mirror.field(connector, "connectorId", "Ljava/lang/String;")))) {
                    throw invalid("actual subscription linkage unavailable");
                }
                Map<String, Object> scope = mirror.scope(mirror.field(connector, "logScope", "Lio/tapstate/core/logging/LogSink$Scope;"));
                if (!scope.equals(call.entry().get("scope"))) { throw invalid("native reader owner differs from cdc entry"); }
                if (tail != null && heldEvidence != null && tail.subscription().uniqueID() != actual.uniqueID()) { throw invalid("new reader cannot replace held handle"); }
                Map<String, Object> evidence = Map.of("subscriptionId", actual.uniqueID(), "connectorId", connector.uniqueID(),
                        "readerId", reader.uniqueID(), "deliveryId", delivery.uniqueID(), "listenerId", listener.uniqueID(),
                        "scope", scope, "cdcEntryOrder", call.order(), "cdcReturnOrder", ++order, "readerOpenProof", "REQUIRES_ACTUAL_DELIVERY");
                tail = new Tail(actual, connector, nativeThread, delivery, listener, scope, evidence); result.putAll(evidence);
            } else if (spec.name().equals("awaitCaptureEnd")) {
                if (!(event.returnValue() instanceof BooleanValue actual)) { throw invalid("end return is not a genuine boolean"); }
                long root = shutdownParent(active); List<Long> seen = waits.computeIfAbsent(root, ignored -> new ArrayList<>());
                seen.add(mirror.integral(call.arguments().get(2)));
                String stage = seen.size() == 1 ? "GRACE" : "FINAL";
                if (!actual.value() && stage.equals("FINAL")) { finalFalseParents.add(root); }
                if (waits.size() > MAX_CALLS || finalFalseParents.size() > MAX_CALLS) { throw invalid("shutdown proof budget exceeded"); }
                result.put("actualReturnValue", actual.value()); result.put("stage", stage); result.put("shutdownEntryOrder", root);
                result.put("actualDeadlineNanos", seen.getLast());
            } else if (spec.name().equals("stopStrict")) { strictReturned = true; }
            else if (spec.name().equals("shutDown")) { shutdownReturned = true; }
            else if (spec.name().equals("close")) { subscriptionReturned = true; }
            if (call.spec().name().equals("handOver") && heldEvidence != null && Long.valueOf(call.order()).equals(heldEvidence.get("entryOrder"))) { heldCallConcluded = true; }
            record("NORMAL_RETURN", call, result);
        }
        active.pop(); if (active.isEmpty()) { drainedThread(event.thread().uniqueID()); }
    }
    private long shutdownParent(Deque<Call> active) {
        return active.stream().filter(call -> call.spec().name().equals("shutDown") && call.qualified())
                .findFirst().map(Call::order).orElseThrow(() -> invalid("end wait lacks its exact shutdown parent"));
    }
    private void exceptional(ExceptionEvent event) throws Exception {
        Deque<Call> active = calls.get(event.thread().uniqueID()); if (active == null || active.isEmpty()) { return; }
        List<StackFrame> stack = frames(event.thread()); int catcher = stack.size();
        if (event.catchLocation() != null) {
            for (int i = 0; i < stack.size(); i++) { if (stack.get(i).location().method().equals(event.catchLocation().method())) { catcher = i; break; } }
        }
        Map<String, Object> coded = Map.of();
        ObjectReference thrown = event.exception();
        if (thrown.referenceType().name().equals("io.tapstate.core.common.TapstateException")) {
            NativeTelemetryMirror mirror = mirror(); ObjectReference code = mirror.object(mirror.field(thrown, "code", "Lio/tapstate/core/common/TapstateErrorCode;"));
            if (code.referenceType().name().equals("io.tapstate.adapters.pdk.ConnectorError")) {
                Map<String, Object> actual = new LinkedHashMap<>();
                actual.put("exceptionId", thrown.uniqueID()); actual.put("exceptionType", thrown.referenceType().name());
                actual.put("codeEnum", mirror.enumName(code)); actual.put("code", mirror.text(mirror.field(code, "code", "Ljava/lang/String;")));
                actual.put("args", mirror.strings(mirror.field(thrown, "args", "Ljava/util/Map;")));
                Value cause = mirror.field(thrown, "cause", "Ljava/lang/Throwable;");
                actual.put("originalCause", cause == null ? Map.of("availability", "ABSENT")
                        : Map.of("availability", "PRESENT", "objectId", mirror.object(cause).uniqueID(), "type", mirror.object(cause).referenceType().name()));
                coded = Map.copyOf(actual);
            }
        }
        while (!active.isEmpty()) {
            Call call = active.peek(); int index = stack.size() - call.depth();
            if (index < 0 || index >= catcher) { break; }
            if (!stack.get(index).location().method().equals(bound.get(call.spec()).method())) { throw invalid("exception unwind lost original call"); }
            if (call.spec().name().equals("handOver") && heldEvidence != null && Long.valueOf(call.order()).equals(heldEvidence.get("entryOrder"))) { heldCallConcluded = true; }
            if (call.qualified()) {
                record("EXCEPTIONAL_EXIT", call, coded.isEmpty()
                        ? Map.of("exceptionId", thrown.uniqueID(), "exceptionType", thrown.referenceType().name()) : coded);
                if ((call.spec().name().equals("shutDown") || call.spec().name().equals("stopStrict"))
                        && heldEvidence != null && !heldReleased && "CAPTURE_FAILED".equals(coded.get("codeEnum"))) {
                    boolean nativeStop = call.spec().name().equals("stopStrict");
                    long parent = active.stream().filter(value -> value.spec().name().equals("shutDown") && value.qualified())
                            .findFirst().map(Call::order).orElse(-1L);
                    boolean sameFinalFalse = finalFalseParents.contains(parent);
                    if (nativeStop || sameFinalFalse) {
                        Map<String, Object> evidence = new LinkedHashMap<>(coded);
                        evidence.put("status", "ACTUAL_NATIVE_CLOSE_REFUSAL"); evidence.put("finalAwaitFalse", sameFinalFalse);
                        evidence.put("shutdownEntryOrder", parent); evidence.put("nativeStopRefused", nativeStop);
                        evidence.put("entryOrder", call.order()); evidence.put("escapedPinnedFrame", true);
                        evidence.put("tail", tail.evidence()); evidence.put("held", heldEvidence);
                        refusal = Collections.unmodifiableMap(evidence);
                    }
                }
            }
            active.pop();
        }
        if (active.isEmpty()) { drainedThread(event.thread().uniqueID()); }
    }
    private void record(String phase, Call call, Map<String, Object> data) {
        Map<String, Object> row = new LinkedHashMap<>(); row.put("phase", phase); row.put("site", call.spec().name());
        row.put("threadId", call.thread().uniqueID()); row.put("entryOrder", call.order()); row.put("atNanos", System.nanoTime());
        row.put("receiverId", call.receiver() == null ? "STATIC" : call.receiver().uniqueID()); row.put("details", data);
        int length = JsonWriter.write(row).getBytes(StandardCharsets.UTF_8).length;
        if (records.size() >= MAX_RECORDS || length > MAX_RECORD_BYTES || bytes + length > MAX_PHASE_BYTES) { throw invalid("record/byte budget exceeded"); }
        bytes += length; records.add(Collections.unmodifiableMap(row));
    }
    private static ObjectReference requiredObject(Value value, String type, NativeTelemetryMirror mirror) throws Exception {
        ObjectReference result = mirror.object(value); if (!type.equals(result.referenceType().name())) { throw invalid("unexpected native model type"); } return result;
    }

    private static Map<String, Object> workloadOwner(Value value, NativeTelemetryMirror mirror) throws Exception {
        ObjectReference owner = requiredObject(value, "io.tapstate.spi.store.WorkloadOwner", mirror);
        String node = mirror.text(mirror.field(owner, "nodeId", "Ljava/lang/String;"));
        String boot = mirror.text(mirror.field(owner, "bootId", "Ljava/lang/String;"));
        if (node.isBlank() || boot.isBlank()) { throw new NativeTelemetryMirror.Unavailable("EMPTY_CLAIM_OWNER"); }
        return Map.of("nodeId", node, "bootId", boot);
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


    @Override public void close() throws Exception {
        Throwable cleanup = null; long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        synchronized (lock) {
            closing = true;
            try { release(); } catch (RuntimeException | Error problem) { cleanup = problem; }
            for (EventRequest request : requests) {
                if (!(request instanceof BreakpointRequest || request instanceof ClassPrepareRequest)) { continue; }
                try { request.disable(); } catch (VMDisconnectedException ended) { }
                catch (RuntimeException | Error problem) { if (cleanup == null) { cleanup = problem; } else if (cleanup != problem) { cleanup.addSuppressed(problem); } }
            }
            while (calls.values().stream().flatMap(Collection::stream).anyMatch(Call::qualified) && failure.get() == null) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) { Throwable problem = invalid("qualified entry/exit did not drain"); if (cleanup == null) { cleanup = problem; } else { cleanup.addSuppressed(problem); } break; }
                try { TimeUnit.NANOSECONDS.timedWait(lock, remaining); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); if (cleanup == null) { cleanup = interrupted; } else { cleanup.addSuppressed(interrupted); } break; }
            }
            for (EventRequest request : requests) {
                try { request.disable(); } catch (VMDisconnectedException ended) { }
                catch (RuntimeException | Error problem) { if (cleanup == null) { cleanup = problem; } else if (cleanup != problem) { cleanup.addSuppressed(problem); } }
            }
            running = false;
            try { vm.dispose(); } catch (VMDisconnectedException ended) { }
            catch (RuntimeException | Error problem) { if (cleanup == null) { cleanup = problem; } else if (cleanup != problem) { cleanup.addSuppressed(problem); } }
        }
        pump.interrupt();
        long remaining = deadline - System.nanoTime();
        try { if (remaining > 0) { TimeUnit.NANOSECONDS.timedJoin(pump, remaining); } }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); if (cleanup == null) { cleanup = interrupted; } else { cleanup.addSuppressed(interrupted); } }
        if (pump.isAlive()) { Throwable problem = invalid("event pump did not end"); if (cleanup == null) { cleanup = problem; } else { cleanup.addSuppressed(problem); } }
        if (failure.get() != null) { if (cleanup == null) { cleanup = failure.get(); } else if (cleanup != failure.get()) { cleanup.addSuppressed(failure.get()); } }
        try { if (!hash(jar).equals(sha)) { throw invalid("immutable artifact changed"); } }
        catch (Exception | Error changed) { if (cleanup == null) { cleanup = changed; } else if (cleanup != changed) { cleanup.addSuppressed(changed); }
        }
        if (cleanup instanceof Exception e) { throw e; } if (cleanup instanceof Error e) { throw e; }
    }

    private static Map<String, Image> images(Path jar) throws Exception {
        Map<String, Image> images = new LinkedHashMap<>();
        Set<String> wanted = new HashSet<>(SPECS.stream().map(Spec::type).toList());
        wanted.addAll(List.of(PDK + "$CdcDelivery", PDK + "$Acknowledgements", "io.tapstate.spi.capture.CaptureConfig",
                "io.tapstate.core.model.PipelineNode", "io.tapstate.core.logging.LogSink$Scope",
                "io.tapstate.core.common.TapstateException", "io.tapstate.adapters.pdk.ConnectorError",
                "io.tapstate.app.PipelineActuationOwnership$Execution", "io.tapstate.app.ExecutionFence",
                "io.tapstate.spi.store.WorkloadClaim", "io.tapstate.spi.store.WorkloadClaimKey", "io.tapstate.spi.store.WorkloadClaimType",
                "io.tapstate.spi.store.WorkloadOwner", "io.tapstate.spi.store.ObservationStore$Scope",
                "io.tapstate.spi.store.StopReservation$JobIdentity", "io.tapstate.runtime.engine.Engine$ExecutionJob"));
        try (ZipFile boot = new ZipFile(jar.toFile())) {
            for (String type : wanted) {
                ZipEntry entry = boot.getEntry("BOOT-INF/classes/" + type.replace('.', '/') + ".class");
                if (entry != null) { try (InputStream input = boot.getInputStream(entry)) { putImage(images, type, entry.getName(), input); } }
            }
            List<? extends ZipEntry> libraries = boot.stream().filter(entry -> entry.getName().startsWith("BOOT-INF/lib/")
                    && entry.getName().endsWith(".jar")).toList();
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
        if (!images.keySet().containsAll(wanted)) { throw invalid("required actual layout is missing from immutable artifact"); }
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
    private static AssertionError invalid(String reason) { return new AssertionError("UNVERIFIED passive native-close witness: " + reason); }
    private static AssertionError invalid(String reason, Throwable cause) { return new AssertionError("UNVERIFIED passive native-close witness: " + reason, cause); }
}
