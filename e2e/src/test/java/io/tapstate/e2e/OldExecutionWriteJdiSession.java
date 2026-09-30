package io.tapstate.e2e;

import com.sun.jdi.*;
import com.sun.jdi.connect.AttachingConnector;
import com.sun.jdi.connect.Connector;
import com.sun.jdi.event.*;
import com.sun.jdi.request.*;
import io.tapstate.spi.store.ObservationStore;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/** Holds one real old execution write and its genuine false return; never a performance measurement. */
final class OldExecutionWriteJdiSession implements AutoCloseable {
    record Binding(String type, String method, String signature, String artifactOrigin, String methodSha256,
            long loaderIdentity, String loaderType, long entryOffset, List<Integer> normalReturnOffsets) {
        Binding { normalReturnOffsets = List.copyOf(normalReturnOffsets); }
        Map<String, Object> evidence() {
            return Map.of("type", type, "method", method, "signature", signature, "artifactOrigin", artifactOrigin,
                    "methodSha256", methodSha256, "loaderIdentity", loaderIdentity, "loaderType", loaderType,
                    "entryOffset", entryOffset, "normalReturnOffsets", normalReturnOffsets);
        }
    }
    record ThreadIdentity(long identity, String name) {
        Map<String, Object> evidence() { return Map.of("identity", identity, "name", name); }
    }
    record Entry(long atNanos, long codeIndex, int frameDepth, long receiverIdentity,
            long observationIdentity, long scopeIdentity) {
        Map<String, Object> evidence() {
            return Map.of("atNanos", atNanos, "codeIndex", codeIndex, "frameDepth", frameDepth,
                    "receiverIdentity", receiverIdentity, "observationIdentity", observationIdentity,
                    "scopeIdentity", scopeIdentity);
        }
    }
    record Held(String artifactSha256, String pipelineId, ObservationStore.Scope actualScope,
            Binding binding, ThreadIdentity thread, Entry entry, boolean suspendAll) {
        Map<String, Object> evidence() {
            return Map.of("artifactSha256", artifactSha256, "pipelineId", pipelineId,
                    "actualScope", Map.of("pipelineIncarnationId", actualScope.pipelineIncarnationId(),
                            "executionGeneration", actualScope.executionGeneration()),
                    "binding", binding.evidence(), "thread", thread.evidence(), "entry", entry.evidence(),
                    "suspendAll", suspendAll);
        }
    }
    record Exit(Held held, long atNanos, long codeIndex, int frameDepth, boolean normalReturn,
            boolean actualReturnValue, boolean suspendAll, long events) {
        Map<String, Object> evidence() {
            return Map.of("held", held.evidence(), "atNanos", atNanos, "codeIndex", codeIndex,
                    "frameDepth", frameDepth, "normalReturn", normalReturn, "actualReturnValue", actualReturnValue,
                    "suspendAll", suspendAll, "events", events);
        }
    }

    private enum Phase { WAITING, ENTRY_HELD, RELEASED, EXIT_HELD, CLOSING, CLOSED }
    private record Image(String origin, byte[] code) { }
    private static final String TYPE = "io.tapstate.adapters.mongostore.MongoLatestObservationStorage";
    private static final String METHOD = "save";
    private static final String SIGNATURE = "(Lio/tapstate/core/lifecycle/Observation;"
            + "Lio/tapstate/spi/store/ObservationStore$Scope;)Z";
    private static final String OBSERVATION = "io.tapstate.core.lifecycle.Observation";
    private static final String SCOPE = "io.tapstate.spi.store.ObservationStore$Scope";
    private static final String LOADER = "org.springframework.boot.loader.launch.LaunchedClassLoader";
    private static final int MAX_CLASS_BYTES = 1_048_576, MAX_EVENTS = 20_000, MAX_FRAMES = 512, MAX_REQUESTS = 8;
    private static final Duration MAX_WAIT = Duration.ofMinutes(2);

    private final Path jar;
    private final String artifactSha256, pipelineId;
    private final ObservationStore.Scope expectedScope;
    private final Image image;
    private final VirtualMachine vm;
    private final Object lock = new Object(), actionLock = new Object();
    private final List<EventRequest> requests = new ArrayList<>();
    private final CompletableFuture<Held> heldResult = new CompletableFuture<>();
    private final CompletableFuture<Exit> exitResult = new CompletableFuture<>();
    private final AtomicReference<AssertionError> failure = new AtomicReference<>();
    private final AtomicReference<Throwable> cleanupFailure = new AtomicReference<>();
    private final AtomicBoolean cleanupStarted = new AtomicBoolean();
    private final Thread pump, cleanupThread;
    private volatile boolean running = true;
    private volatile boolean closing;
    private volatile Phase phase = Phase.WAITING;
    private ReferenceType boundType;
    private Method method;
    private Binding binding;
    private BreakpointRequest entryRequest;
    private MethodEntryRequest reentryRequest;
    private MethodExitRequest exitRequest;
    private ExceptionRequest exceptionRequest;
    private ThreadDeathRequest threadDeathRequest;
    private EventSet suspendedSet;
    private Held held;
    private ThreadReference observedThread;
    private ObjectReference receiver, observation, scope;
    private List<Method> callers = List.of();
    private long events;

    private OldExecutionWriteJdiSession(Path jar, String sha, String pipelineId,
            ObservationStore.Scope expectedScope, Image image, VirtualMachine vm) {
        this.jar = jar; this.artifactSha256 = sha; this.pipelineId = pipelineId;
        this.expectedScope = expectedScope; this.image = image; this.vm = vm;
        pump = new Thread(this::loop, "old-execution-write-jdi-events");
        pump.setDaemon(true);
        cleanupThread = new Thread(this::cleanup, "old-execution-write-jdi-cleanup");
        cleanupThread.setDaemon(true);
    }

    static OldExecutionWriteJdiSession attach(int loopbackPort, Path immutableBootJar, String pipelineId,
            ObservationStore.Scope expectedScope) throws Exception {
        Objects.requireNonNull(immutableBootJar, "immutableBootJar");
        Objects.requireNonNull(pipelineId, "pipelineId"); Objects.requireNonNull(expectedScope, "expectedScope");
        if (loopbackPort < 1 || loopbackPort > 65_535 || pipelineId.isBlank() || pipelineId.length() > 256) {
            throw invalid("invalid loopback port or observed pipeline identity");
        }
        Path jar = immutableBootJar.toRealPath();
        if (!Files.isRegularFile(jar) || Files.size(jar) > 512L * 1024 * 1024) {
            throw invalid("application artifact unavailable or exceeded its bound");
        }
        String sha = sha256(jar);
        Image image = image(jar);
        if (!sha.equals(sha256(jar))) { throw invalid("application artifact changed during provenance reads"); }
        AttachingConnector connector = Bootstrap.virtualMachineManager().attachingConnectors().stream()
                .filter(value -> value.name().equals("com.sun.jdi.SocketAttach")).findFirst()
                .orElseThrow(() -> invalid("loopback JDI attach connector unavailable"));
        Map<String, Connector.Argument> arguments = connector.defaultArguments();
        arguments.get("hostname").setValue("127.0.0.1");
        arguments.get("port").setValue(Integer.toString(loopbackPort)); arguments.get("timeout").setValue("10000");
        VirtualMachine vm = connector.attach(arguments);
        OldExecutionWriteJdiSession session = null;
        boolean suspended = false;
        try {
            session = new OldExecutionWriteJdiSession(jar, sha, pipelineId, expectedScope, image, vm);
            if (!vm.canGetBytecodes() || !vm.canGetMethodReturnValues()) {
                throw invalid("live bytecode or genuine method return values unavailable");
            }
            vm.suspend(); suspended = true;
            session.install();
            session.pump.start();
            vm.resume(); suspended = false;
            session.checkCapture();
            return session;
        } catch (Exception | Error problem) {
            if (session != null) {
                session.failure.compareAndSet(null, safeFailure(problem));
                try { session.close(); } catch (Throwable cleanup) { problem.addSuppressed(cleanup); }
            } else {
                try { if (suspended) { vm.resume(); } }
                catch (VMDisconnectedException ignored) { }
                try { vm.dispose(); } catch (VMDisconnectedException ignored) { }
            }
            throw problem;
        }
    }

    private void install() throws Exception {
        ClassPrepareRequest prepare = own(vm.eventRequestManager().createClassPrepareRequest());
        prepare.addClassFilter(TYPE); prepare.setSuspendPolicy(EventRequest.SUSPEND_ALL); prepare.enable();
        VMDeathRequest death = own(vm.eventRequestManager().createVMDeathRequest());
        death.setSuspendPolicy(EventRequest.SUSPEND_ALL); death.enable();
        List<ReferenceType> loaded = vm.classesByName(TYPE);
        if (loaded.size() > 1) { throw invalid("duplicate live storage class binding"); }
        if (!loaded.isEmpty()) { bind(loaded.getFirst()); }
    }

    private <T extends EventRequest> T own(T request) {
        if (requests.size() >= MAX_REQUESTS) { throw invalid("owned JDI request budget exceeded"); }
        requests.add(request);
        return request;
    }

    private void bind(ReferenceType type) throws Exception {
        if (!TYPE.equals(type.name())) { throw invalid("unexpected prepared storage class"); }
        if (boundType != null) {
            if (!boundType.equals(type)) { throw invalid("duplicate storage loader binding"); }
            verifyBinding();
            return;
        }
        ClassLoaderReference loader = type.classLoader();
        if (loader == null || !LOADER.equals(loader.referenceType().name())) {
            throw invalid("storage class used an unexpected application loader");
        }
        List<Method> selected = type.methodsByName(METHOD, SIGNATURE);
        if (selected.size() != 1) { throw invalid("exact storage save method missing or ambiguous"); }
        Method candidate = selected.getFirst();
        if (!candidate.declaringType().equals(type) || candidate.isStatic() || candidate.isNative()
                || candidate.isAbstract() || candidate.isBridge() || candidate.isObsolete()
                || !Arrays.equals(image.code(), candidate.bytecodes())) {
            throw invalid("live storage method differs from immutable artifact Code");
        }
        List<Integer> returns = BenchmarkJdiCostObserver.returnOffsets(image.code());
        if (returns.isEmpty()) { throw invalid("storage save has no normal return instruction"); }
        Location entry = candidate.locationOfCodeIndex(0);
        if (entry == null || entry.codeIndex() != 0) { throw invalid("exact storage entry instruction unavailable"); }
        boundType = type; method = candidate;
        binding = new Binding(TYPE, METHOD, SIGNATURE, image.origin(), sha256(image.code()), loader.uniqueID(),
                LOADER, 0, returns);
        entryRequest = own(vm.eventRequestManager().createBreakpointRequest(entry));
        entryRequest.setSuspendPolicy(EventRequest.SUSPEND_ALL); entryRequest.enable();
    }

    private void verifyBinding() throws Exception {
        if (binding == null || method == null || method.isObsolete()
                || !Arrays.equals(image.code(), method.bytecodes())
                || !artifactSha256.equals(sha256(jar))) {
            throw invalid("storage method or immutable application provenance changed");
        }
        List<ReferenceType> loaded = vm.classesByName(TYPE);
        ClassLoaderReference loader = boundType.classLoader();
        if (loaded.size() != 1 || !loaded.getFirst().equals(boundType) || loader == null
                || loader.uniqueID() != binding.loaderIdentity() || !LOADER.equals(loader.referenceType().name())) {
            throw invalid("storage class binding disappeared or changed loader");
        }
    }

    Held awaitHeld(Duration timeout) throws Exception {
        synchronized (actionLock) {
            checkCapture();
            return await(heldResult, timeout, "matching old execution entry");
        }
    }

    Exit releaseAndAwaitRejected(Duration timeout) throws Exception {
        synchronized (actionLock) {
            checkCapture();
            validTimeout(timeout);
            EventSet release;
            synchronized (lock) {
                if (phase != Phase.ENTRY_HELD || suspendedSet == null || exitRequest == null
                        || !exitRequest.isEnabled()) { throw invalid("no exactly bound entry is held for release"); }
                verifyBinding();
                phase = Phase.RELEASED;
                release = suspendedSet; suspendedSet = null;
            }
            try { release.resume(); }
            catch (Exception | Error problem) { fail(problem); throw problem; }
            return await(exitResult, timeout, "genuine false normal return");
        }
    }

    private <T> T await(CompletableFuture<T> result, Duration timeout, String stage) throws Exception {
        validTimeout(timeout);
        try {
            T value = result.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            checkCapture();
            return value;
        } catch (TimeoutException expired) {
            AssertionError problem = invalid("timed out awaiting " + stage);
            fail(problem); throw problem;
        } catch (InterruptedException interrupted) {
            fail(interrupted); Thread.currentThread().interrupt(); throw interrupted;
        } catch (ExecutionException failed) {
            checkCapture(); throw invalid("observation future failed without capture failure");
        }
    }

    private static void validTimeout(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative() || timeout.compareTo(MAX_WAIT) > 0) {
            throw invalid("capture wait must be positive and at most two minutes");
        }
    }

    private void checkCapture() {
        AssertionError problem = failure.get();
        if (problem != null) { throw problem; }
        if (closing) { throw invalid("capture requested after debugger cleanup began"); }
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
                    if (++events > MAX_EVENTS) { throw invalid("event budget exceeded"); }
                    if (closing) { continue; }
                    if (event instanceof ClassPrepareEvent prepare) { bind(prepare.referenceType()); }
                    else if (event instanceof BreakpointEvent breakpoint) {
                        if (entry(breakpoint, set)) { retained = true; }
                    } else if (event instanceof MethodEntryEvent entered) {
                        if (!entered.request().equals(reentryRequest) || phase != Phase.RELEASED
                                || entered.thread().uniqueID() != observedThread.uniqueID()) {
                            throw invalid("method entry lost its observed write thread or request");
                        }
                        if (entered.method().equals(method)) { throw invalid("observed write recursively entered storage save"); }
                    } else if (event instanceof MethodExitEvent returned) {
                        if (exit(returned, set)) { retained = true; }
                    } else if (event instanceof ExceptionEvent thrown) { exception(thrown); }
                    else if (event instanceof ThreadDeathEvent) {
                        throw invalid("observed write thread died before cleanup");
                    } else if (event instanceof VMDeathEvent || event instanceof VMDisconnectEvent) {
                        throw invalid("owned process died or disconnected before capture cleanup");
                    } else if (!(event instanceof VMStartEvent)) { throw invalid("unmapped capture event"); }
                }
            }
        } finally {
            // Exactly one retained set transfers its suspension to releaseAndAwaitRejected or close.
            // All other event sets are resumed here, including failed or unrelated entries.
            if (!retained) { set.resume(); }
        }
    }

    private boolean entry(BreakpointEvent event, EventSet set) throws Exception {
        if (!event.location().method().equals(method) || event.location().codeIndex() != 0) {
            throw invalid("entry lost its exact storage method binding");
        }
        if (!event.request().equals(entryRequest) || phase != Phase.WAITING) {
            throw invalid("unexpected or duplicate storage entry event");
        }
        List<StackFrame> frames = frames(event.thread());
        StackFrame top = frames.getFirst();
        if (!top.location().method().equals(method)) { throw invalid("entry frame differs from event method"); }
        List<Value> arguments = top.getArgumentValues();
        if (arguments.size() != 2 || !(arguments.get(0) instanceof ObjectReference measured)
                || !(arguments.get(1) instanceof ObjectReference measuredScope)) {
            throw invalid("real storage record arguments unavailable");
        }
        ReferenceType observationType = recordType(measured, OBSERVATION);
        ReferenceType scopeType = recordType(measuredScope, SCOPE);
        String actualPipeline = string(measured.getValue(field(observationType, "pipelineId", "Ljava/lang/String;")));
        Field incarnation = field(scopeType, "pipelineIncarnationId", "Ljava/lang/String;");
        Field generation = field(scopeType, "executionGeneration", "J");
        Map<Field, Value> values = measuredScope.getValues(List.of(incarnation, generation));
        if (!(values.get(generation) instanceof LongValue actualGeneration) || actualGeneration.value() <= 0) {
            throw invalid("actual execution generation unavailable");
        }
        ObservationStore.Scope actualScope = new ObservationStore.Scope(string(values.get(incarnation)),
                actualGeneration.value());
        if (!pipelineId.equals(actualPipeline) || !expectedScope.equals(actualScope)) { return false; }
        verifyBinding();
        requireAll(set);
        ObjectReference actualReceiver = top.thisObject();
        if (actualReceiver == null || !actualReceiver.referenceType().equals(boundType)) {
            throw invalid("real storage receiver unavailable");
        }
        observedThread = event.thread(); receiver = actualReceiver; observation = measured; scope = measuredScope;
        callers = frames.subList(1, frames.size()).stream().map(frame -> frame.location().method()).toList();
        entryRequest.disable();
        exitRequest = own(vm.eventRequestManager().createMethodExitRequest());
        exitRequest.addClassFilter(boundType); exitRequest.addThreadFilter(observedThread);
        exitRequest.setSuspendPolicy(EventRequest.SUSPEND_ALL); exitRequest.enable();
        exceptionRequest = own(vm.eventRequestManager().createExceptionRequest(null, true, true));
        exceptionRequest.addThreadFilter(observedThread);
        exceptionRequest.setSuspendPolicy(EventRequest.SUSPEND_ALL); exceptionRequest.enable();
        threadDeathRequest = own(vm.eventRequestManager().createThreadDeathRequest());
        threadDeathRequest.addThreadFilter(observedThread);
        threadDeathRequest.setSuspendPolicy(EventRequest.SUSPEND_ALL); threadDeathRequest.enable();
        reentryRequest = own(vm.eventRequestManager().createMethodEntryRequest());
        reentryRequest.addClassFilter(boundType);
        reentryRequest.addThreadFilter(observedThread);
        reentryRequest.setSuspendPolicy(EventRequest.SUSPEND_ALL); reentryRequest.enable();
        held = new Held(artifactSha256, actualPipeline, actualScope, binding,
                new ThreadIdentity(observedThread.uniqueID(), observedThread.name()),
                new Entry(System.nanoTime(), 0, frames.size(), receiver.uniqueID(), observation.uniqueID(),
                        scope.uniqueID()), true);
        suspendedSet = set; phase = Phase.ENTRY_HELD;
        heldResult.complete(held);
        return true;
    }

    private boolean exit(MethodExitEvent event, EventSet set) throws Exception {
        if (!event.request().equals(exitRequest) || phase != Phase.RELEASED
                || event.thread().uniqueID() != observedThread.uniqueID()) {
            throw invalid("return lost its observed write thread or request");
        }
        if (!event.method().equals(method)) { return false; }
        verifyBinding(); requireAll(set);
        long offset = event.location().codeIndex();
        if (!event.location().method().equals(method) || offset < 0
                || !binding.normalReturnOffsets().contains(Math.toIntExact(offset))) {
            throw invalid("normal return was outside the pinned return instructions");
        }
        List<StackFrame> frames = frames(event.thread());
        int firstCaller = frames.size() - callers.size();
        if (firstCaller < 0 || firstCaller > 1
                || firstCaller == 1 && (!frames.getFirst().location().method().equals(method)
                        || frames.getFirst().thisObject() == null
                        || frames.getFirst().thisObject().uniqueID() != receiver.uniqueID())) {
            throw invalid("normal return lost its exact entry depth or receiver");
        }
        for (int i = 0; i < callers.size(); i++) {
            if (!callers.get(i).equals(frames.get(firstCaller + i).location().method())) {
                throw invalid("normal return changed the entry caller chain");
            }
        }
        Value result = event.returnValue();
        if (!(result instanceof BooleanValue actual)) { throw invalid("genuine storage return was not a Boolean value"); }
        if (actual.value()) { throw invalid("storage save genuinely returned Boolean true; expected false"); }
        exitRequest.disable(); exceptionRequest.disable(); threadDeathRequest.disable(); reentryRequest.disable();
        suspendedSet = set; phase = Phase.EXIT_HELD;
        exitResult.complete(new Exit(held, System.nanoTime(), offset, frames.size(), true, actual.value(), true, events));
        return true;
    }

    private void exception(ExceptionEvent event) throws Exception {
        if (!event.request().equals(exceptionRequest) || phase != Phase.RELEASED
                || event.thread().uniqueID() != observedThread.uniqueID()) {
            throw invalid("exception lost its observed write thread or request");
        }
        List<StackFrame> frames = frames(event.thread());
        int callIndex = frames.size() - held.entry().frameDepth();
        if (callIndex < 0 || !frames.get(callIndex).location().method().equals(method)) {
            throw invalid("observed write disappeared before normal return");
        }
        Location caught = event.catchLocation();
        if (caught == null) { throw invalid("uncaught exception escaped observed storage write"); }
        List<Integer> candidates = new ArrayList<>();
        for (int i = 0; i < frames.size(); i++) {
            if (frames.get(i).location().method().equals(caught.method())) { candidates.add(i); }
        }
        if (candidates.isEmpty() || candidates.stream().anyMatch(index -> index > callIndex)) {
            throw invalid("escaping or ambiguous write exception; type=" + safeMetadata(event.exception().referenceType().name())
                    + ", catchMethod=" + safeMetadata(caught.method().declaringType().name()) + "#"
                    + safeMetadata(caught.method().name()) + safeMetadata(caught.method().signature())
                    + ", stackDepth=" + frames.size() + ", candidateIndexes=" + candidates);
        }
    }

    private ReferenceType recordType(ObjectReference object, String expected) {
        ReferenceType type = object.referenceType();
        ClassLoaderReference loader = type.classLoader();
        if (!expected.equals(type.name()) || !type.isFinal() || loader == null
                || loader.uniqueID() != binding.loaderIdentity()) {
            throw invalid("record argument used an unexpected type or loader");
        }
        return type;
    }

    private static Field field(ReferenceType type, String name, String signature) {
        List<Field> selected = type.fields().stream().filter(value -> name.equals(value.name())).toList();
        if (selected.size() != 1) { throw invalid("record component missing or ambiguous"); }
        Field field = selected.getFirst();
        if (!field.declaringType().equals(type) || !signature.equals(field.signature()) || field.isStatic() || !field.isFinal()) {
            throw invalid("record component does not have its exact immutable binding");
        }
        return field;
    }

    private static String string(Value value) {
        if (!(value instanceof StringReference text)) { throw invalid("record string component unavailable"); }
        String actual = text.value();
        if (actual.isBlank() || actual.length() > 1024) { throw invalid("record string identity exceeded its bound"); }
        return actual;
    }

    private static List<StackFrame> frames(ThreadReference thread) throws Exception {
        List<StackFrame> frames = thread.frames();
        if (frames.isEmpty() || frames.size() > MAX_FRAMES) { throw invalid("write stack unavailable or exceeded its bound"); }
        return frames;
    }

    private static void requireAll(EventSet set) {
        if (set.suspendPolicy() != EventRequest.SUSPEND_ALL) { throw invalid("write boundary did not suspend the whole VM"); }
    }

    private void fail(Throwable problem) {
        AssertionError safe = safeFailure(problem);
        failure.compareAndSet(null, safe);
        heldResult.completeExceptionally(failure.get()); exitResult.completeExceptionally(failure.get());
        beginCleanup();
    }

    private void beginCleanup() {
        running = false;
        if (cleanupStarted.compareAndSet(false, true)) { cleanupThread.start(); }
    }

    /** This session owns the debugger only; the caller owns the live Boot process. */
    @Override public void close() throws Exception {
        synchronized (actionLock) {
            if (phase == Phase.CLOSED) { return; }
            boolean complete = phase == Phase.EXIT_HELD && exitResult.isDone() && !exitResult.isCompletedExceptionally();
            closing = true; phase = Phase.CLOSING;
            beginCleanup();
            pump.interrupt();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            try {
                cleanupThread.join(2000);
                long remaining = deadline - System.nanoTime();
                if (remaining > 0) { pump.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining))); }
            }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                cleanupFailure.compareAndSet(null, interrupted);
            }
            phase = Phase.CLOSED;
            AssertionError captured = failure.get();
            if (captured != null || cleanupFailure.get() != null || pump.isAlive() || cleanupThread.isAlive() || !complete) {
                AssertionError safe = invalid("capture close failed to preserve a clean debugger boundary");
                if (captured != null) { safe.initCause(captured); }
                if (cleanupFailure.get() != null) { safe.addSuppressed(cleanupFailure.get()); }
                if (pump.isAlive()) { safe.addSuppressed(invalid("owned event pump did not stop within its bound")); }
                if (cleanupThread.isAlive()) { safe.addSuppressed(invalid("debugger detach did not finish within its bound")); }
                if (!complete) { safe.addSuppressed(invalid("capture closed before an observed genuine false return")); }
                throw safe;
            }
        }
    }

    private void cleanup() {
        try {
            EventSet release;
            List<EventRequest> owned;
            synchronized (lock) {
                owned = List.copyOf(requests); requests.clear();
                release = suspendedSet; suspendedSet = null;
            }
            try {
                // Delete before resume so releasing a held entry cannot arm another suspension.
                vm.eventRequestManager().deleteEventRequests(owned);
            } catch (VMDisconnectedException ignored) { }
            finally {
                try { if (release != null) { release.resume(); } }
                catch (VMDisconnectedException ignored) { }
                finally { try { vm.dispose(); } catch (VMDisconnectedException ignored) { } }
            }
        } catch (Throwable problem) { cleanupFailure.compareAndSet(null, safeFailure(problem)); }
    }

    private static Image image(Path jar) throws Exception {
        Image found = null;
        String resource = TYPE.replace('.', '/') + ".class";
        try (ZipFile boot = new ZipFile(jar.toFile())) {
            ZipEntry direct = boot.getEntry("BOOT-INF/classes/" + resource);
            if (direct != null) {
                try (InputStream stream = boot.getInputStream(direct)) { found = readImage(direct.getName(), stream); }
            }
            List<? extends ZipEntry> libraries = boot.stream().filter(entry -> entry.getName().startsWith("BOOT-INF/lib/")
                    && entry.getName().endsWith(".jar")).toList();
            if (libraries.size() > 512) { throw invalid("application library count exceeded its bound"); }
            for (ZipEntry library : libraries) {
                if (library.getSize() < 0 || library.getSize() > 128L * 1024 * 1024) {
                    throw invalid("application library exceeded its bound");
                }
                try (ZipInputStream nested = new ZipInputStream(boot.getInputStream(library))) {
                    ZipEntry entry; int entries = 0;
                    while ((entry = nested.getNextEntry()) != null) {
                        if (++entries > 100_000) { throw invalid("application library entry count exceeded its bound"); }
                        if (entry.getName().equals(resource)) {
                            if (found != null) { throw invalid("immutable artifact duplicated the storage class"); }
                            found = readImage(library.getName() + "!/" + resource, nested);
                        }
                    }
                }
            }
        }
        if (found == null) { throw invalid("immutable Boot artifact lacked the exact storage class"); }
        return found;
    }

    private static Image readImage(String origin, InputStream input) throws Exception {
        byte[] bytes = input.readNBytes(MAX_CLASS_BYTES + 1);
        if (bytes.length > MAX_CLASS_BYTES) { throw invalid("storage class exceeded its bound"); }
        byte[] code = methodCode(bytes);
        if (code == null || BenchmarkJdiCostObserver.returnOffsets(code).isEmpty()) {
            throw invalid("immutable storage method missing or lacked a normal return");
        }
        return new Image(origin, code);
    }

    /** Reads the immutable Code attribute without loading, invoking or rewriting target classes. */
    private static byte[] methodCode(byte[] bytes) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (input.readInt() != 0xcafebabe) { throw invalid("invalid storage class header"); }
            input.skipNBytes(4);
            String[] utf = new String[input.readUnsignedShort()];
            for (int i = 1; i < utf.length; i++) {
                switch (input.readUnsignedByte()) {
                    case 1 -> utf[i] = input.readUTF();
                    case 3, 4, 9, 10, 11, 12, 17, 18 -> input.skipNBytes(4);
                    case 5, 6 -> { input.skipNBytes(8); i++; }
                    case 7, 8, 16, 19, 20 -> input.skipNBytes(2);
                    case 15 -> input.skipNBytes(3);
                    default -> throw invalid("unsupported storage constant-pool entry");
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
                    if (size > MAX_CLASS_BYTES) { throw invalid("storage method attribute exceeded its bound"); }
                    if ((METHOD + SIGNATURE).equals(key) && "Code".equals(name)) {
                        byte[] attribute = input.readNBytes((int) size);
                        if (attribute.length != size || found != null) { throw invalid("storage Code truncated or duplicated"); }
                        try (DataInputStream code = new DataInputStream(new ByteArrayInputStream(attribute))) {
                            code.skipNBytes(4); int length = code.readInt();
                            if (length < 1 || length > 65_535) { throw invalid("invalid storage Code length"); }
                            found = code.readNBytes(length);
                            if (found.length != length) { throw invalid("storage instructions truncated"); }
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
            if (size > MAX_CLASS_BYTES) { throw invalid("storage class attribute exceeded its bound"); }
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
    private static String safeMetadata(String value) {
        return value != null && value.length() <= 512 && value.chars().allMatch(character -> character >= 33 && character <= 126)
                ? value : "UNMAPPED";
    }
    private static AssertionError safeFailure(Throwable problem) {
        if (problem instanceof AssertionError assertion) { return assertion; }
        AssertionError safe = invalid("capture failed; type=" + safeMetadata(problem.getClass().getName()));
        safe.initCause(problem); return safe;
    }
    private static AssertionError invalid(String reason) { return new AssertionError("Invalid old execution write capture: " + reason); }
}
