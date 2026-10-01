package io.tapstate.e2e;

import com.sun.jdi.*;
import com.sun.jdi.connect.Connector;
import com.sun.jdi.connect.ListeningConnector;
import com.sun.jdi.event.*;
import com.sun.jdi.request.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/** Holds only one real source tasklet close while observing the stop worker's genuine Jet wait. */
final class HungStopJdiSession implements AutoCloseable {
    record BoundMethod(String type, String name, String signature, String artifactOrigin,
            String methodSha256, long loaderIdentity, List<Integer> normalReturns) {
        BoundMethod { normalReturns = List.copyOf(normalReturns); }
        Map<String, Object> evidence() {
            return Map.of("type", type, "name", name, "signature", signature,
                    "artifactOrigin", artifactOrigin, "methodSha256", methodSha256,
                    "loaderIdentity", loaderIdentity, "normalReturns", normalReturns);
        }
    }
    record HeldClose(long atNanos, long threadIdentity, String pipelineId,
            String wrappedType, BoundMethod method, boolean eventThreadOnly) {
        Map<String, Object> evidence() {
            return Map.of("atNanos", atNanos, "threadIdentity", threadIdentity,
                    "pipelineId", pipelineId, "wrappedType", wrappedType,
                    "method", method.evidence(), "eventThreadOnly", eventThreadOnly);
        }
    }
    record WaitEntry(long atNanos, long threadIdentity, String pipelineId,
            long budgetSeconds, int budgetNanos, BoundMethod method, String caller) {
        Map<String, Object> evidence() {
            return Map.of("atNanos", atNanos, "threadIdentity", threadIdentity,
                    "pipelineId", pipelineId, "budgetSeconds", budgetSeconds,
                    "budgetNanos", budgetNanos, "method", method.evidence(), "caller", caller);
        }
    }
    record WaitExit(WaitEntry entry, long atNanos, long elapsedNanos,
            boolean actualReturnValue, long returnCodeIndex) {
        Map<String, Object> evidence() {
            return Map.of("entry", entry.evidence(), "atNanos", atNanos,
                    "elapsedNanos", elapsedNanos, "actualReturnValue", actualReturnValue,
                    "returnCodeIndex", returnCodeIndex);
        }
    }

    private enum Target {
        SOURCE_CLOSE("io.tapstate.runtime.engine.StageOutputPressureProcessor", "close", "()V"),
        JET_WAIT("io.tapstate.runtime.engine.Engine", "awaitTerminal",
                "(Ljava/lang/String;Ljava/time/Duration;)Z");
        final String type, name, signature;
        Target(String type, String name, String signature) {
            this.type = type; this.name = name; this.signature = signature;
        }
    }
    private record Image(String origin, byte[] code) { }
    private static final String APP_LOADER = "org.springframework.boot.loader.launch.LaunchedClassLoader";
    private static final String SOURCE = "io.tapstate.runtime.srs.SrsSourceProcessor";
    private static final String WRAPPER = "com.hazelcast.jet.impl.processor.ProcessorWrapper";
    private static final String TASKLET = "com.hazelcast.jet.impl.execution.ProcessorTasklet";
    private static final String ACTUATOR = "io.tapstate.app.EngineLifecycleActuator";
    private static final int MAX_CLASS_BYTES = 1_048_576, MAX_EVENTS = 30_000, MAX_FRAMES = 512;
    private static final int SNAPSHOT_MAX_THREADS = 512, SNAPSHOT_MAX_FRAMES = 64;
    private static final long SNAPSHOT_BUDGET_NANOS = TimeUnit.SECONDS.toNanos(2);
    private static final Duration MAX_WAIT = Duration.ofMinutes(3);

    private final Path jar;
    private final String artifactSha256, pipelineId;
    private final Map<Target, Image> images;
    private final RealProcessServer server;
    private final VirtualMachine vm;
    private final Object lock = new Object();
    private final Map<Target, Method> methods = new EnumMap<>(Target.class);
    private final Map<Target, BoundMethod> bindings = new EnumMap<>(Target.class);
    private final List<EventRequest> requests = new ArrayList<>();
    private final CompletableFuture<Map<Target, BoundMethod>> ready = new CompletableFuture<>();
    private final CompletableFuture<HeldClose> closeHeld = new CompletableFuture<>();
    private final CompletableFuture<WaitEntry> waitEntered = new CompletableFuture<>();
    private final CompletableFuture<WaitExit> waitExited = new CompletableFuture<>();
    private final AtomicReference<AssertionError> failure = new AtomicReference<>();
    private final Thread pump;
    private volatile boolean running = true, closing;
    private boolean armed;
    private EventSet heldCloseSet;
    private BreakpointRequest closeRequest, waitRequest;
    private MethodExitRequest waitExitRequest;
    private ExceptionRequest waitExceptions;
    private ThreadDeathRequest waitThreadDeath;
    private ThreadReference waitThread;
    private ObjectReference waitReceiver;
    private int waitDepth;
    private List<Method> waitCallers = List.of();
    private WaitEntry entered;
    private boolean sawNativeTimeout;
    private long events;

    private HungStopJdiSession(Path jar, String sha, String pipelineId, Map<Target, Image> images,
            RealProcessServer server, VirtualMachine vm) {
        this.jar = jar; this.artifactSha256 = sha; this.pipelineId = pipelineId;
        this.images = images; this.server = server; this.vm = vm;
        pump = new Thread(this::loop, "hung-stop-jdi-events");
        pump.setDaemon(true);
    }

    static HungStopJdiSession start(String storeUri, String operatorDatabase, Path selectedJar,
            String pipelineId) throws Exception {
        Objects.requireNonNull(storeUri); Objects.requireNonNull(operatorDatabase);
        Objects.requireNonNull(pipelineId);
        if (pipelineId.isBlank() || pipelineId.length() > 256) { throw invalid("invalid observed pipeline identity"); }
        Path jar = selectedJar.toRealPath();
        if (!Files.isRegularFile(jar) || Files.size(jar) > 512L * 1024 * 1024) {
            throw invalid("application artifact unavailable or exceeded its bound");
        }
        String sha = sha256(jar);
        Map<Target, Image> images = images(jar);
        if (!sha.equals(sha256(jar))) { throw invalid("application artifact changed during provenance reads"); }
        ListeningConnector connector = Bootstrap.virtualMachineManager().listeningConnectors().stream()
                .filter(value -> value.name().equals("com.sun.jdi.SocketListen")).findFirst()
                .orElseThrow(() -> invalid("loopback JDI listener unavailable"));
        Map<String, Connector.Argument> arguments = connector.defaultArguments();
        arguments.get("localAddress").setValue("127.0.0.1");
        arguments.get("port").setValue("0");
        arguments.get("timeout").setValue("15000");
        String reported = connector.startListening(arguments);
        boolean listening = true;
        RealProcessServer server = null;
        VirtualMachine vm = null;
        HungStopJdiSession session = null;
        Throwable primary = null;
        try {
            String address = BenchmarkJdiCostObserver.numericLoopbackDialAddress(
                    arguments.get("localAddress").value(), reported, arguments.get("port").value());
            server = RealProcessServer.launchingWithJvmArguments(storeUri, operatorDatabase, jar,
                    List.of("-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=" + address), List.of());
            vm = connector.accept(arguments);
            if (!vm.canGetBytecodes() || !vm.canGetMethodReturnValues()) {
                throw invalid("live bytecodes or genuine method return values unavailable");
            }
            connector.stopListening(arguments); listening = false;
            session = new HungStopJdiSession(jar, sha, pipelineId, images, server, vm);
            session.install();
            session.pump.start();
            server.awaitHealthy();
            session.checkCapture();
            return session;
        } catch (Throwable problem) {
            primary = problem;
            if (session != null) {
                try { session.close(); }
                catch (Throwable cleanup) { if (cleanup != problem) { problem.addSuppressed(cleanup); } }
            } else {
                if (vm != null) { try { vm.dispose(); } catch (VMDisconnectedException ignored) { } }
                if (server != null) { server.close(); }
            }
            throw problem;
        } finally {
            if (listening) {
                try { connector.stopListening(arguments); }
                catch (Throwable cleanup) {
                    AssertionError safe = invalid("owned JDI listener cleanup failed");
                    if (primary != null) { primary.addSuppressed(safe); } else { throw safe; }
                }
            }
        }
    }

    RealProcessServer server() { return server; }
    void checkCapture() { AssertionError problem = failure.get(); if (problem != null) { throw problem; } }

    void arm() throws Exception {
        synchronized (lock) {
            checkCapture();
            if (armed || bindings.size() != Target.values().length || closeHeld.isDone() || waitEntered.isDone()) {
                throw invalid("the real stop observer was not ready to arm exactly once");
            }
            if (!artifactSha256.equals(sha256(jar))) { throw invalid("application artifact changed before arming"); }
            armed = true;
        }
    }

    void awaitBindings(Duration timeout) throws Exception {
        await(ready, timeout, "both immutable method bindings");
    }

    HeldClose awaitHeldClose(Duration timeout) throws Exception {
        return await(closeHeld, timeout, "the real source tasklet close");
    }

    WaitEntry awaitWaitEntry(Duration timeout) throws Exception {
        return await(waitEntered, timeout, "the real stop teardown wait");
    }

    WaitExit awaitFullBudget(Duration timeout) throws Exception {
        return await(waitExited, timeout, "the real thirty-second Jet wait");
    }

    void releaseClose() {
        EventSet release;
        synchronized (lock) { release = heldCloseSet; heldCloseSet = null; }
        if (release == null) { throw invalid("no source tasklet close was held for release"); }
        try { release.resume(); }
        catch (RuntimeException failed) { fail(failed); throw failed; }
    }

    /** Samples bounded stack metadata without touching the held A close thread or suspending the VM. */
    Map<String, Object> threadSnapshot() {
        long started = System.nanoTime();
        long deadline = started + SNAPSHOT_BUDGET_NANOS;
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("sampledAtNanos", started);
        List<Map<String, Object>> entries = new ArrayList<>();
        result.put("threads", entries);
        try {
            synchronized (lock) {
                checkCapture();
                List<ThreadReference> all = vm.allThreads();
                result.put("totalThreads", all.size());
                long heldIdentity = closeHeld.isDone() && !closeHeld.isCompletedExceptionally()
                        ? closeHeld.getNow(null).threadIdentity() : -1L;
                List<ThreadReference> eligible = all;
                result.put("eligibleThreads", eligible.size());
                result.put("excludedOtherThreads", all.size() - eligible.size());
                List<ThreadReference> selected = eligible.stream()
                        .sorted(Comparator.comparingInt(thread -> threadPriority(thread.name())))
                        .limit(SNAPSHOT_MAX_THREADS).toList();
                int unavailable = 0;
                boolean resumeFailed = false;
                for (ThreadReference thread : selected) {
                    if (System.nanoTime() - deadline >= 0) { break; }
                    String category = threadCategory(thread.name());
                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("threadIdentity", thread.uniqueID());
                    entry.put("category", category);
                    if (thread.uniqueID() == heldIdentity) {
                        entry.put("availability", "HELD_A_CLOSE_UNTOUCHED");
                        entries.add(entry);
                        continue;
                    }
                    boolean addedSuspension = false;
                    try {
                        thread.suspend();
                        addedSuspension = true;
                        int fullCount = thread.frameCount();
                        int count = Math.min(fullCount, SNAPSHOT_MAX_FRAMES);
                        List<Map<String, Object>> stack = new ArrayList<>(count);
                        for (StackFrame frame : count == 0 ? List.<StackFrame>of() : thread.frames(0, count)) {
                            Location location = frame.location();
                            stack.add(Map.of("class", location.declaringType().name(),
                                    "method", location.method().name(),
                                    "codeIndex", location.codeIndex(), "line", location.lineNumber()));
                        }
                        entry.put("frames", stack);
                        entry.put("availability", "AVAILABLE");
                        entry.put("truncatedFrames", fullCount > SNAPSHOT_MAX_FRAMES);
                    } catch (Exception | Error unavailableThread) {
                        unavailable++;
                        entry.put("availability", "UNAVAILABLE");
                        entry.put("reasonType", unavailableThread.getClass().getName());
                    } finally {
                        if (addedSuspension) {
                            try { thread.resume(); }
                            catch (ObjectCollectedException ended) {
                                unavailable++;
                                entry.put("availability", "UNAVAILABLE_THREAD_ENDED");
                                entry.put("reasonType", ended.getClass().getName());
                            }
                            catch (Exception | Error cannotResume) {
                                fail(cannotResume);
                                resumeFailed = true;
                                entry.put("availability", "UNAVAILABLE_RESUME_FAILED");
                                entry.put("reasonType", cannotResume.getClass().getName());
                            }
                        }
                    }
                    entries.add(entry);
                    if (resumeFailed) { break; }
                }
                result.put("capturedThreads", entries.size());
                result.put("unavailableThreads", unavailable);
                boolean truncated = eligible.size() > SNAPSHOT_MAX_THREADS || entries.size() < selected.size();
                result.put("truncatedThreads", truncated);
                result.put("elapsedNanos", System.nanoTime() - started);
                result.put("availability", resumeFailed || entries.isEmpty() ? "UNAVAILABLE"
                        : unavailable == 0 && !truncated ? "AVAILABLE" : "PARTIAL");
                if (entries.isEmpty()) { result.put("reasonType", "NO_ELIGIBLE_THREADS"); }
            }
        } catch (Exception | Error unavailable) {
            result.put("availability", "UNAVAILABLE");
            result.put("reasonType", unavailable.getClass().getName());
            result.put("elapsedNanos", System.nanoTime() - started);
        }
        return result;
    }

    private static int threadPriority(String name) {
        String lower = name.toLowerCase(Locale.ROOT);
        if (lower.contains("converge") || lower.contains("scheduling")) { return 0; }
        if (lower.contains("telemetry") || lower.contains("latest") || lower.contains("capture")) { return 1; }
        if (lower.contains("lifecycle")) { return 2; }
        if (lower.startsWith("hz.") || lower.contains("hazelcast") || lower.contains("jet")
                || lower.contains("source") || lower.contains("sink")) { return 3; }
        return 4;
    }

    private static String threadCategory(String name) {
        return switch (threadPriority(name)) {
            case 0 -> "CONVERGENCE";
            case 1 -> "TELEMETRY";
            case 2 -> "LIFECYCLE";
            case 3 -> "HAZELCAST_JET";
            default -> "OTHER";
        };
    }

    private <T> T await(CompletableFuture<T> future, Duration timeout, String stage) throws Exception {
        if (timeout == null || timeout.isZero() || timeout.isNegative() || timeout.compareTo(MAX_WAIT) > 0) {
            throw invalid("observer wait must be positive and at most three minutes");
        }
        checkCapture();
        try {
            T result = future.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            checkCapture();
            return result;
        } catch (TimeoutException expired) {
            AssertionError problem = invalid("timed out awaiting " + stage);
            fail(problem); throw problem;
        } catch (InterruptedException interrupted) {
            fail(interrupted); Thread.currentThread().interrupt(); throw interrupted;
        } catch (ExecutionException failed) {
            checkCapture(); throw invalid("observer future failed without a capture failure");
        }
    }

    private void install() {
        for (Target target : Target.values()) {
            ClassPrepareRequest prepare = vm.eventRequestManager().createClassPrepareRequest();
            prepare.addClassFilter(target.type);
            prepare.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
            prepare.enable(); requests.add(prepare);
        }
        VMDeathRequest death = vm.eventRequestManager().createVMDeathRequest();
        death.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD); death.enable(); requests.add(death);
    }

    private void loop() {
        try {
            while (running) {
                EventSet set = vm.eventQueue().remove(25);
                if (set != null) { handle(set); }
            }
        } catch (InterruptedException interrupted) {
            if (!closing) { fail(interrupted); }
        } catch (Throwable problem) {
            if (!closing) { fail(problem); }
        }
    }

    private void handle(EventSet set) throws Exception {
        boolean retained = false;
        try {
            synchronized (lock) {
                for (Event event : set) {
                    if (++events > MAX_EVENTS) { throw invalid("observer event budget exceeded"); }
                    if (event instanceof VMStartEvent) { continue; }
                    if (event instanceof ClassPrepareEvent prepared) { bind(prepared.referenceType()); }
                    else if (event instanceof BreakpointEvent breakpoint) {
                        if (breakpoint.request().equals(closeRequest)) {
                            if (sourceClose(breakpoint, set)) { retained = true; }
                        } else if (breakpoint.request().equals(waitRequest)) {
                            waitEntry(breakpoint);
                        } else { throw invalid("unmapped exact method breakpoint"); }
                    } else if (event instanceof MethodExitEvent returned) { waitExit(returned); }
                    else if (event instanceof ExceptionEvent thrown) { waitException(thrown); }
                    else if (event instanceof ThreadDeathEvent death) {
                        if (!death.request().equals(waitThreadDeath) || !waitExited.isDone()) {
                            throw invalid("real stop worker died before its terminal wait returned");
                        }
                    }
                    else if (event instanceof VMDeathEvent || event instanceof VMDisconnectEvent) {
                        throw invalid("owned Boot died before the stop witness finished");
                    } else { throw invalid("unmapped stop observer event"); }
                }
            }
        } finally {
            if (!retained) { set.resume(); }
        }
    }

    private void bind(ReferenceType type) throws Exception {
        Target target = Arrays.stream(Target.values()).filter(value -> value.type.equals(type.name()))
                .findFirst().orElseThrow(() -> invalid("unmapped prepared class"));
        if (bindings.containsKey(target)) { throw invalid("duplicate application method binding"); }
        ClassLoaderReference loader = type.classLoader();
        if (loader == null || !APP_LOADER.equals(loader.referenceType().name())) {
            throw invalid("target method used an unexpected application loader");
        }
        if (!bindings.isEmpty() && bindings.values().iterator().next().loaderIdentity() != loader.uniqueID()) {
            throw invalid("the two target methods used different application loaders");
        }
        List<Method> selected = type.methodsByName(target.name, target.signature);
        if (selected.size() != 1) { throw invalid("exact target method missing or ambiguous"); }
        Method method = selected.getFirst();
        if (!method.declaringType().equals(type) || method.isStatic() || method.isNative()
                || method.isAbstract() || method.isBridge() || method.isObsolete()
                || !Arrays.equals(images.get(target).code(), method.bytecodes())) {
            throw invalid("live method differs from immutable artifact Code");
        }
        List<Integer> returns = BenchmarkJdiCostObserver.returnOffsets(images.get(target).code());
        if (returns.isEmpty()) { throw invalid("target method has no normal return instruction"); }
        Location entry = method.locationOfCodeIndex(0);
        if (entry == null || entry.codeIndex() != 0) { throw invalid("exact method entry unavailable"); }
        BoundMethod bound = new BoundMethod(target.type, target.name, target.signature,
                images.get(target).origin(), sha256(images.get(target).code()), loader.uniqueID(), returns);
        methods.put(target, method); bindings.put(target, bound);
        BreakpointRequest request = vm.eventRequestManager().createBreakpointRequest(entry);
        request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD); request.enable(); requests.add(request);
        if (target == Target.SOURCE_CLOSE) { closeRequest = request; } else { waitRequest = request; }
        if (bindings.size() == Target.values().length) { ready.complete(Map.copyOf(bindings)); }
    }

    private void verifyBinding(Target target) throws Exception {
        Method method = methods.get(target);
        BoundMethod bound = bindings.get(target);
        if (method == null || bound == null || method.isObsolete()
                || !Arrays.equals(images.get(target).code(), method.bytecodes())
                || !artifactSha256.equals(sha256(jar))) {
            throw invalid("live method or immutable artifact changed after binding");
        }
        ClassLoaderReference loader = method.declaringType().classLoader();
        if (loader == null || loader.uniqueID() != bound.loaderIdentity()
                || !APP_LOADER.equals(loader.referenceType().name())) {
            throw invalid("live method changed its application loader");
        }
    }

    private boolean sourceClose(BreakpointEvent event, EventSet set) throws Exception {
        if (!event.location().method().equals(methods.get(Target.SOURCE_CLOSE))) {
            throw invalid("source close callback changed its bound method");
        }
        StackFrame frame = firstFrame(event.thread());
        ObjectReference wrapper = frame.thisObject();
        if (wrapper == null || !wrapper.referenceType().equals(methods.get(Target.SOURCE_CLOSE).declaringType())) {
            throw invalid("source close receiver unavailable");
        }
        if (!(wrapper.referenceType() instanceof ClassType wrapperType)) {
            throw invalid("source close wrapper was not a class");
        }
        ClassType base = wrapperType.superclass();
        if (base == null || !WRAPPER.equals(base.name())) { throw invalid("unexpected source wrapper base"); }
        Field wrappedField = exactField(base, "wrapped", "Lcom/hazelcast/jet/core/Processor;");
        if (!(wrapper.getValue(wrappedField) instanceof ObjectReference delegate)) {
            throw invalid("wrapped source processor unavailable");
        }
        ReferenceType sourceType = delegate.referenceType();
        ClassLoaderReference sourceLoader = sourceType.classLoader();
        if (!SOURCE.equals(sourceType.name()) || sourceLoader == null
                || sourceLoader.uniqueID() != bindings.get(Target.SOURCE_CLOSE).loaderIdentity()) {
            return false;
        }
        String actualPipeline = string(delegate.getValue(exactField(sourceType, "pipelineId", "Ljava/lang/String;")));
        if (!pipelineId.equals(actualPipeline)) { return false; }
        if (!armed) { return false; }
        if (closeHeld.isDone()) { throw invalid("duplicate A source close callback"); }
        verifyBinding(Target.SOURCE_CLOSE);
        boolean taskletCaller = frames(event.thread()).stream().anyMatch(candidate ->
                TASKLET.equals(candidate.location().declaringType().name())
                        && "closeProcessor".equals(candidate.location().method().name()));
        if (!taskletCaller || set.suspendPolicy() != EventRequest.SUSPEND_EVENT_THREAD) {
            throw invalid("A close was not the real tasklet callback on only its event thread");
        }
        closeRequest.disable();
        heldCloseSet = set;
        closeHeld.complete(new HeldClose(System.nanoTime(), event.thread().uniqueID(), actualPipeline,
                sourceType.name(), bindings.get(Target.SOURCE_CLOSE), true));
        return true;
    }

    private void waitEntry(BreakpointEvent event) throws Exception {
        if (!event.location().method().equals(methods.get(Target.JET_WAIT))) {
            throw invalid("Jet wait entry changed its exact method");
        }
        StackFrame frame = firstFrame(event.thread());
        List<Value> arguments = frame.getArgumentValues();
        if (arguments.size() != 2 || !(arguments.getFirst() instanceof StringReference name)
                || !(arguments.get(1) instanceof ObjectReference budget)) {
            throw invalid("Jet wait arguments unavailable");
        }
        if (!pipelineId.equals(name.value())) { return; }
        if (!armed) { return; }
        if (waitEntered.isDone()) { throw invalid("duplicate A terminal wait"); }
        verifyBinding(Target.JET_WAIT);
        ReferenceType duration = budget.referenceType();
        if (!"java.time.Duration".equals(duration.name())) { throw invalid("terminal budget was not Duration"); }
        Value seconds = budget.getValue(exactField(duration, "seconds", "J"));
        Value nanos = budget.getValue(exactField(duration, "nanos", "I"));
        if (!(seconds instanceof LongValue longSeconds) || longSeconds.value() != 30
                || !(nanos instanceof IntegerValue intNanos) || intNanos.value() != 0) {
            throw invalid("real terminal wait did not receive the thirty-second budget");
        }
        List<StackFrame> frames = frames(event.thread());
        if (frames.size() < 2 || !ACTUATOR.equals(frames.get(1).location().declaringType().name())
                || !"stopInternal".equals(frames.get(1).location().method().name())) {
            throw invalid("terminal wait was not called by actual lifecycle teardown");
        }
        ObjectReference receiver = frame.thisObject();
        if (receiver == null) { throw invalid("Jet wait receiver unavailable"); }
        waitThread = event.thread(); waitReceiver = receiver; waitDepth = frames.size();
        waitCallers = frames.subList(1, frames.size()).stream().map(candidate -> candidate.location().method()).toList();
        entered = new WaitEntry(System.nanoTime(), waitThread.uniqueID(), pipelineId, 30, 0,
                bindings.get(Target.JET_WAIT), ACTUATOR + "#stopInternal");
        waitRequest.disable();
        waitExitRequest = vm.eventRequestManager().createMethodExitRequest();
        waitExitRequest.addClassFilter(methods.get(Target.JET_WAIT).declaringType());
        waitExitRequest.addThreadFilter(waitThread);
        waitExitRequest.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
        waitExitRequest.enable(); requests.add(waitExitRequest);
        waitExceptions = vm.eventRequestManager().createExceptionRequest(null, true, true);
        waitExceptions.addThreadFilter(waitThread);
        waitExceptions.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
        waitExceptions.enable(); requests.add(waitExceptions);
        waitThreadDeath = vm.eventRequestManager().createThreadDeathRequest();
        waitThreadDeath.addThreadFilter(waitThread);
        waitThreadDeath.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
        waitThreadDeath.enable(); requests.add(waitThreadDeath);
        waitEntered.complete(entered);
    }

    private void waitException(ExceptionEvent event) throws Exception {
        if (!event.request().equals(waitExceptions) || waitThread == null
                || event.thread().uniqueID() != waitThread.uniqueID() || waitExited.isDone()) {
            throw invalid("stop worker exception lost its exact wait correlation");
        }
        List<StackFrame> frames = frames(event.thread());
        int waitIndex = frames.size() - waitDepth;
        if (waitIndex < 0 || waitIndex >= frames.size()
                || !frames.get(waitIndex).location().method().equals(methods.get(Target.JET_WAIT))) {
            throw invalid("observed Jet wait disappeared while handling an exception");
        }
        Location caught = event.catchLocation();
        if (caught == null) { throw invalid("uncaught exception escaped the actual Jet wait"); }
        List<Integer> candidates = new ArrayList<>();
        for (int i = 0; i < frames.size(); i++) {
            if (frames.get(i).location().method().equals(caught.method())) { candidates.add(i); }
        }
        if (candidates.isEmpty() || candidates.stream().anyMatch(index -> index > waitIndex)) {
            throw invalid("an exception escaped the actual Jet wait");
        }
        if ("java.util.concurrent.TimeoutException".equals(event.exception().referenceType().name())
                && caught.method().equals(methods.get(Target.JET_WAIT))) {
            sawNativeTimeout = true;
        }
    }

    private void waitExit(MethodExitEvent event) throws Exception {
        if (!event.request().equals(waitExitRequest) || waitThread == null
                || event.thread().uniqueID() != waitThread.uniqueID()) {
            throw invalid("Jet wait exit lost its observed worker");
        }
        if (!event.method().equals(methods.get(Target.JET_WAIT))) { return; }
        verifyBinding(Target.JET_WAIT);
        List<StackFrame> frames = frames(event.thread());
        int firstCaller = frames.size() - waitCallers.size();
        if (firstCaller < 0 || firstCaller > 1 || frames.isEmpty()
                || firstCaller == 1 && (frames.size() != waitDepth
                        || !frames.getFirst().location().method().equals(methods.get(Target.JET_WAIT))
                        || frames.getFirst().thisObject() == null
                        || frames.getFirst().thisObject().uniqueID() != waitReceiver.uniqueID())) {
            throw invalid("Jet wait exit lost its entry depth or receiver");
        }
        for (int i = 0; i < waitCallers.size(); i++) {
            if (!frames.get(firstCaller + i).location().method().equals(waitCallers.get(i))) {
                throw invalid("Jet wait exit changed its caller chain");
            }
        }
        long offset = event.location().codeIndex();
        if (offset < 0 || !bindings.get(Target.JET_WAIT).normalReturns().contains(Math.toIntExact(offset))) {
            throw invalid("Jet wait exited outside pinned return instructions");
        }
        if (!(event.returnValue() instanceof BooleanValue result) || result.value()) {
            throw invalid("real Jet wait did not time out with Boolean false");
        }
        if (!sawNativeTimeout) { throw invalid("false terminal wait had no actual caught TimeoutException"); }
        long ended = System.nanoTime();
        long elapsed = ended - entered.atNanos();
        if (elapsed < TimeUnit.SECONDS.toNanos(30)) {
            throw invalid("Jet wait returned false before its full native budget");
        }
        waitExitRequest.disable();
        waitExceptions.disable();
        waitThreadDeath.disable();
        waitExited.complete(new WaitExit(entered, ended, elapsed, false, offset));
    }

    private static StackFrame firstFrame(ThreadReference thread) throws Exception {
        return frames(thread).getFirst();
    }

    private static List<StackFrame> frames(ThreadReference thread) throws Exception {
        List<StackFrame> frames = thread.frames();
        if (frames.isEmpty() || frames.size() > MAX_FRAMES) { throw invalid("observer stack unavailable or unbounded"); }
        return frames;
    }

    private static Field exactField(ReferenceType type, String name, String signature) {
        List<Field> found = type.fields().stream().filter(value -> name.equals(value.name())).toList();
        if (found.size() != 1) { throw invalid("bound field missing or ambiguous"); }
        Field field = found.getFirst();
        if (!field.declaringType().equals(type) || !signature.equals(field.signature()) || field.isStatic()) {
            throw invalid("field differs from its exact declaration");
        }
        return field;
    }

    private static String string(Value value) {
        if (!(value instanceof StringReference text) || text.value().isBlank()) {
            throw invalid("live pipeline identity unavailable");
        }
        return text.value();
    }

    private void fail(Throwable problem) {
        AssertionError safe = problem instanceof AssertionError assertion ? assertion
                : invalid("observer failed; type=" + problem.getClass().getName());
        if (safe != problem) { safe.initCause(problem); }
        failure.compareAndSet(null, safe);
        ready.completeExceptionally(failure.get());
        closeHeld.completeExceptionally(failure.get());
        waitEntered.completeExceptionally(failure.get());
        waitExited.completeExceptionally(failure.get());
        running = false;
        EventSet release;
        synchronized (lock) { release = heldCloseSet; heldCloseSet = null; }
        try { if (release != null) { release.resume(); } }
        catch (VMDisconnectedException ignored) { }
        try { vm.dispose(); } catch (VMDisconnectedException ignored) { }
    }

    @Override public void close() throws Exception {
        closing = true; running = false;
        EventSet release;
        synchronized (lock) { release = heldCloseSet; heldCloseSet = null; }
        Throwable cleanup = null;
        try { if (release != null) { release.resume(); } }
        catch (Throwable failed) { cleanup = failed; }
        try { vm.eventRequestManager().deleteEventRequests(List.copyOf(requests)); }
        catch (VMDisconnectedException ignored) { }
        catch (Throwable failed) { if (cleanup == null) { cleanup = failed; } else { cleanup.addSuppressed(failed); } }
        try { vm.dispose(); } catch (VMDisconnectedException ignored) { }
        catch (Throwable failed) { if (cleanup == null) { cleanup = failed; } else { cleanup.addSuppressed(failed); } }
        pump.interrupt();
        if (pump.isAlive()) { pump.join(2_000); }
        server.close();
        if (pump.isAlive() || cleanup != null || failure.get() != null) {
            AssertionError problem = invalid("observer did not close cleanly");
            if (failure.get() != null) { problem.initCause(failure.get()); }
            if (cleanup != null) { problem.addSuppressed(cleanup); }
            if (pump.isAlive()) { problem.addSuppressed(invalid("event pump did not stop")); }
            throw problem;
        }
    }

    private static Map<Target, Image> images(Path jar) throws Exception {
        Map<Target, Image> found = new EnumMap<>(Target.class);
        try (ZipFile boot = new ZipFile(jar.toFile())) {
            for (Target target : Target.values()) {
                String resource = target.type.replace('.', '/') + ".class";
                ZipEntry direct = boot.getEntry("BOOT-INF/classes/" + resource);
                if (direct != null) {
                    try (InputStream input = boot.getInputStream(direct)) {
                        add(found, target, direct.getName(), input);
                    }
                }
            }
            List<? extends ZipEntry> libraries = boot.stream().filter(entry ->
                    entry.getName().startsWith("BOOT-INF/lib/") && entry.getName().endsWith(".jar")).toList();
            if (libraries.size() > 512) { throw invalid("application library count exceeded its bound"); }
            for (ZipEntry library : libraries) {
                if (library.getSize() < 0 || library.getSize() > 128L * 1024 * 1024) {
                    throw invalid("application library exceeded its bound");
                }
                try (ZipInputStream nested = new ZipInputStream(boot.getInputStream(library))) {
                    ZipEntry entry; int entries = 0;
                    while ((entry = nested.getNextEntry()) != null) {
                        if (++entries > 100_000) { throw invalid("application library entry count exceeded its bound"); }
                        for (Target target : Target.values()) {
                            String resource = target.type.replace('.', '/') + ".class";
                            if (resource.equals(entry.getName())) { add(found, target, library.getName() + "!/" + resource, nested); }
                        }
                    }
                }
            }
        }
        if (found.size() != Target.values().length) { throw invalid("immutable JAR lacked both exact methods"); }
        return Map.copyOf(found);
    }

    private static void add(Map<Target, Image> found, Target target, String origin, InputStream input) throws Exception {
        byte[] bytes = input.readNBytes(MAX_CLASS_BYTES + 1);
        if (bytes.length > MAX_CLASS_BYTES) { throw invalid("target class exceeded its bound"); }
        byte[] code = methodCode(bytes, target.name + target.signature);
        if (code == null || found.putIfAbsent(target, new Image(origin, code)) != null) {
            throw invalid("immutable target method missing or duplicated");
        }
    }

    private static byte[] methodCode(byte[] bytes, String selected) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (input.readInt() != 0xcafebabe) { throw invalid("invalid target class header"); }
            input.skipNBytes(4);
            String[] utf = new String[input.readUnsignedShort()];
            for (int i = 1; i < utf.length; i++) {
                switch (input.readUnsignedByte()) {
                    case 1 -> utf[i] = input.readUTF();
                    case 3, 4, 9, 10, 11, 12, 17, 18 -> input.skipNBytes(4);
                    case 5, 6 -> { input.skipNBytes(8); i++; }
                    case 7, 8, 16, 19, 20 -> input.skipNBytes(2);
                    case 15 -> input.skipNBytes(3);
                    default -> throw invalid("unsupported target class constant-pool entry");
                }
            }
            input.skipNBytes(6); input.skipNBytes(input.readUnsignedShort() * 2L);
            int fields = input.readUnsignedShort();
            for (int i = 0; i < fields; i++) { input.skipNBytes(6); skipAttributes(input); }
            byte[] found = null;
            int methods = input.readUnsignedShort();
            for (int i = 0; i < methods; i++) {
                input.readUnsignedShort();
                String key = utf[input.readUnsignedShort()] + utf[input.readUnsignedShort()];
                int attributes = input.readUnsignedShort();
                for (int j = 0; j < attributes; j++) {
                    String name = utf[input.readUnsignedShort()];
                    long size = Integer.toUnsignedLong(input.readInt());
                    if (size > MAX_CLASS_BYTES) { throw invalid("method attribute exceeded its bound"); }
                    if (selected.equals(key) && "Code".equals(name)) {
                        byte[] attribute = input.readNBytes((int) size);
                        if (attribute.length != size || found != null) { throw invalid("Code truncated or duplicated"); }
                        try (DataInputStream code = new DataInputStream(new ByteArrayInputStream(attribute))) {
                            code.skipNBytes(4);
                            int length = code.readInt();
                            if (length < 1 || length > 65_535) { throw invalid("invalid Code length"); }
                            found = code.readNBytes(length);
                            if (found.length != length) { throw invalid("instructions truncated"); }
                        }
                    } else { input.skipNBytes(size); }
                }
            }
            return found;
        }
    }

    private static void skipAttributes(DataInputStream input) throws IOException {
        int count = input.readUnsignedShort();
        for (int i = 0; i < count; i++) {
            input.skipNBytes(2);
            long size = Integer.toUnsignedLong(input.readInt());
            if (size > MAX_CLASS_BYTES) { throw invalid("class attribute exceeded its bound"); }
            input.skipNBytes(size);
        }
    }

    private static String sha256(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = Files.newInputStream(path)) {
            byte[] buffer = new byte[65_536]; int count;
            while ((count = input.read(buffer)) != -1) { digest.update(buffer, 0, count); }
        }
        return HexFormat.of().formatHex(digest.digest());
    }
    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
    private static AssertionError invalid(String reason) { return new AssertionError("Invalid native hung-stop capture: " + reason); }
}
