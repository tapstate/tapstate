package io.tapstate.e2e;

import com.sun.jdi.*;
import com.sun.jdi.connect.Connector;
import com.sun.jdi.connect.ListeningConnector;
import com.sun.jdi.event.*;
import com.sun.jdi.request.EventRequest;
import com.sun.jdi.request.ExceptionRequest;
import com.sun.jdi.request.ThreadDeathRequest;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
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

/** Exact admission evidence from an owned immutable Boot process; never a performance measurement. */
final class ExecutionAdmissionJdiSession implements AutoCloseable {
    enum Target { ADVANCE_STANDALONE, SUBMIT_JOB }
    enum LeaseTarget { ACQUIRE, RENEW, RELEASE }
    record Counts(long entries, long normalReturns, long exceptionalExits, long inFlight) { }
    record Binding(String type, String method, String signature, String artifactOrigin,
            String methodSha256, long loaderIdentity, String loaderType, long entryOffset,
            List<Integer> normalReturnOffsets) {
        Binding { normalReturnOffsets = List.copyOf(normalReturnOffsets); }
    }
    record Boundary(long atNanos, String artifactSha256, String pipelineId,
            Map<Target, Binding> bindings, Map<Target, Counts> counts,
            long events, int openObservedCalls, boolean eventQueueDrained,
            boolean leaseObservationEnabled, Map<LeaseTarget, Binding> leaseBindings,
            Map<LeaseTarget, Counts> leaseCounts, boolean ownedVmDeath, boolean ownedVmDisconnected) {
        Boundary {
            bindings = Map.copyOf(bindings); counts = Map.copyOf(counts);
            leaseBindings = Map.copyOf(leaseBindings); leaseCounts = Map.copyOf(leaseCounts);
        }
        Boundary(long atNanos, String artifactSha256, String pipelineId,
                Map<Target, Binding> bindings, Map<Target, Counts> counts,
                long events, int openObservedCalls, boolean eventQueueDrained) {
            this(atNanos, artifactSha256, pipelineId, bindings, counts, events, openObservedCalls,
                    eventQueueDrained, false, Map.of(), Map.of(), false, false);
        }
        boolean drained() {
            return eventQueueDrained && bindings.size() == Target.values().length && openObservedCalls == 0
                    && counts.values().stream().allMatch(value -> value.inFlight() == 0
                            && value.entries() == value.normalReturns() + value.exceptionalExits())
                    && (!leaseObservationEnabled || leasesDrained());
        }
        /** Empty maps from the default mode provide no evidence about lease calls. */
        boolean leasesDrained() {
            return leaseObservationEnabled && eventQueueDrained && openObservedCalls == 0
                    && leaseBindings.size() == LeaseTarget.values().length
                    && leaseCounts.size() == LeaseTarget.values().length
                    && leaseCounts.values().stream().allMatch(value -> value.inFlight() == 0
                            && value.entries() == value.normalReturns() + value.exceptionalExits());
        }
        boolean fullyDrained() { return drained() && ownedVmDeath && ownedVmDisconnected; }
    }
    private record Key(Target target, LeaseTarget lease) {
        Key {
            if ((target == null) == (lease == null)) { throw new IllegalArgumentException("one observed method kind is required"); }
        }
        String label() { return target == null ? lease.name() : target.name(); }
    }
    private record Description(String type, String method, String signature, int arguments, int pipelineArgument) { }
    private record Image(String origin, byte[] code) { }
    private record Site(Key key, boolean entry) { }
    private static final class Totals { long entries, returns, exceptional; }
    private static final class Call {
        final Key key;
        final int depth;
        final ObjectReference receiver;
        final String pipeline;
        final boolean qualified;
        boolean escapingException;
        Call(Key key, int depth, ObjectReference receiver, String pipeline, boolean qualified) {
            this.key = key; this.depth = depth; this.receiver = receiver;
            this.pipeline = pipeline; this.qualified = qualified;
        }
    }
    private static final class ThreadState {
        final ThreadReference thread;
        final ArrayDeque<Call> calls = new ArrayDeque<>();
        final ExceptionRequest exceptions;
        final ThreadDeathRequest death;
        ThreadState(ThreadReference thread, ExceptionRequest exceptions, ThreadDeathRequest death) {
            this.thread = thread; this.exceptions = exceptions; this.death = death;
        }
    }
    private static final Map<Target, Description> DESCRIPTIONS = Map.of(
            Target.ADVANCE_STANDALONE, new Description(
                    "io.tapstate.adapters.mongostore.MongoWorkloadClaimStore", "advanceStandalone",
                    "(Ljava/lang/String;Ljava/lang/String;)Ljava/util/OptionalLong;", 2, 1),
            Target.SUBMIT_JOB, new Description("io.tapstate.runtime.engine.Engine", "submitJob",
                    "(Ljava/lang/String;Lcom/hazelcast/jet/core/DAG;)V", 2, 0));
    private static final Map<LeaseTarget, Description> LEASE_DESCRIPTIONS = Map.of(
            LeaseTarget.ACQUIRE, new Description("io.tapstate.adapters.mongostore.MongoWorkloadClaimStore", "acquire",
                    "(Lio/tapstate/spi/store/WorkloadClaimKey;Lio/tapstate/spi/store/WorkloadOwner;JLjava/time/Duration;)"
                            + "Lio/tapstate/spi/store/WorkloadClaimAttempt;", 4, -1),
            LeaseTarget.RENEW, new Description("io.tapstate.adapters.mongostore.MongoWorkloadClaimStore", "renew",
                    "(Lio/tapstate/spi/store/WorkloadClaim;Ljava/time/Duration;)Ljava/util/Optional;", 2, -1),
            LeaseTarget.RELEASE, new Description("io.tapstate.adapters.mongostore.MongoWorkloadClaimStore", "release",
                    "(Lio/tapstate/spi/store/WorkloadClaim;)Z", 1, -1));
    private static final String LOADER = "org.springframework.boot.loader.launch.LaunchedClassLoader";
    private static final int MAX_EVENTS = 50_000, MAX_THREADS = 128, MAX_CALLS = 64, MAX_FRAMES = 512;
    private static final int MAX_CLASS_BYTES = 1_048_576;
    private final Path jar;
    private final String artifactSha256, pipelineId;
    private final boolean leaseObservationEnabled;
    private final Map<Key, Description> descriptions;
    private final Map<Key, Image> images;
    private final RealProcessServer server;
    private final VirtualMachine vm;
    private final Object lock = new Object(), boundaryLock = new Object();
    private final Map<Key, Binding> bindings = new LinkedHashMap<>();
    private final Map<Key, Method> methods = new LinkedHashMap<>();
    private final Map<Key, Totals> totals = new LinkedHashMap<>();
    private final Map<Long, ThreadState> threads = new HashMap<>();
    private final AtomicReference<AssertionError> failure = new AtomicReference<>();
    private final Thread pump;
    private volatile CompletableFuture<Boundary> boundaryCommand;
    private volatile boolean running = true;
    private boolean closing, closed, disconnected, vmDeath;
    private Boundary finalBoundary;
    private ClassLoaderReference loader;
    private long events;

    private ExecutionAdmissionJdiSession(Path jar, String sha, String pipelineId, Map<Key, Description> descriptions,
            Map<Key, Image> images, boolean leaseObservationEnabled, RealProcessServer server, VirtualMachine vm) {
        this.jar = jar; this.artifactSha256 = sha; this.pipelineId = pipelineId;
        this.descriptions = descriptions; this.images = images; this.leaseObservationEnabled = leaseObservationEnabled;
        this.server = server; this.vm = vm;
        descriptions.keySet().forEach(key -> totals.put(key, new Totals()));
        for (String type : descriptions.values().stream().map(Description::type).distinct().toList()) {
            var request = vm.eventRequestManager().createClassPrepareRequest();
            request.addClassFilter(type);
            request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD); request.enable();
        }
        pump = new Thread(this::loop, "execution-admission-jdi-events");
        pump.setDaemon(true); pump.start();
    }

    static ExecutionAdmissionJdiSession start(String storeUri, String operatorDb, Path input,
            String pipelineId) throws Exception {
        return start(storeUri, operatorDb, input, pipelineId, false, List.of());
    }

    /** Lease counts cover every matching method call in this owned VM, without pipeline projection. */
    static ExecutionAdmissionJdiSession startWithLeaseObservation(String storeUri, String operatorDb, Path input,
            String pipelineId) throws Exception {
        return startWithLeaseObservation(storeUri, operatorDb, input, pipelineId, List.of());
    }

    static ExecutionAdmissionJdiSession startWithLeaseObservation(String storeUri, String operatorDb, Path input,
            String pipelineId, List<String> applicationArguments) throws Exception {
        return start(storeUri, operatorDb, input, pipelineId, true, List.copyOf(applicationArguments));
    }

    private static ExecutionAdmissionJdiSession start(String storeUri, String operatorDb, Path input,
            String pipelineId, boolean observeLeases, List<String> applicationArguments) throws Exception {
        Objects.requireNonNull(storeUri); Objects.requireNonNull(operatorDb); Objects.requireNonNull(pipelineId);
        if (pipelineId.isBlank() || pipelineId.length() > 256) { throw invalid("invalid observed pipeline identity"); }
        Path jar = input.toRealPath();
        if (!Files.isRegularFile(jar) || Files.size(jar) > 512L * 1024 * 1024) {
            throw invalid("application artifact unavailable or exceeded its bound");
        }
        String sha = sha256(jar);
        Map<Key, Description> descriptions = descriptions(observeLeases);
        Map<Key, Image> images = images(jar, descriptions);
        if (!sha.equals(sha256(jar))) { throw invalid("application artifact changed while reading method provenance"); }
        ListeningConnector connector = Bootstrap.virtualMachineManager().listeningConnectors().stream()
                .filter(value -> value.name().equals("com.sun.jdi.SocketListen")).findFirst()
                .orElseThrow(() -> invalid("loopback JDI listener unavailable"));
        Map<String, Connector.Argument> arguments = connector.defaultArguments();
        arguments.get("localAddress").setValue("127.0.0.1");
        arguments.get("port").setValue("0"); arguments.get("timeout").setValue("15000");
        String reported = connector.startListening(arguments);
        boolean listening = true;
        RealProcessServer server = null; VirtualMachine vm = null;
        ExecutionAdmissionJdiSession session = null;
        Throwable primary = null;
        try {
            String address = BenchmarkJdiCostObserver.numericLoopbackDialAddress(
                    arguments.get("localAddress").value(), reported, arguments.get("port").value());
            server = RealProcessServer.launchingWithJvmArguments(storeUri, operatorDb, jar,
                    List.of("-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=" + address), applicationArguments);
            vm = connector.accept(arguments);
            if (!vm.canGetBytecodes()) { throw invalid("target method bytecodes unavailable"); }
            connector.stopListening(arguments); listening = false;
            session = new ExecutionAdmissionJdiSession(jar, sha, pipelineId, descriptions, images,
                    observeLeases, server, vm);
            session.checkCapture();
            server.awaitHealthy();
            session.checkCapture();
            return session;
        } catch (Throwable problem) {
            primary = problem;
            if (session != null) {
                try { session.close(); }
                catch (Throwable cleanup) { if (cleanup != problem) { problem.addSuppressed(cleanup); } }
            } else {
                boolean detached = vm == null;
                if (vm != null) {
                    try { vm.dispose(); detached = true; } catch (VMDisconnectedException ignored) { detached = true; }
                    catch (Throwable cleanup) { problem.addSuppressed(invalid("failed launch could not detach its debugger")); }
                }
                if (server != null) { if (detached) { server.close(); } else { server.kill(); } }
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

    Boundary boundary() throws Exception {
        synchronized (boundaryLock) {
            checkCapture();
            synchronized (lock) {
                if (closing || closed || disconnected) { throw invalid("boundary requested after admission capture ended"); }
            }
            if (!artifactSha256.equals(sha256(jar))) { throw invalid("application artifact changed during admission capture"); }
            vm.suspend();
            try {
                vm.allThreads();
                CompletableFuture<Boundary> command = new CompletableFuture<>();
                boundaryCommand = command;
                checkCapture();
                return command.get(30, TimeUnit.SECONDS);
            } catch (Exception | Error problem) {
                fail(problem); throw problem;
            } finally {
                boundaryCommand = null;
                try { vm.resume(); } catch (VMDisconnectedException detached) { checkCapture(); }
            }
        }
    }

    private void loop() {
        try {
            while (running) {
                EventSet set = vm.eventQueue().remove(25);
                if (set != null) { handle(set); }
                CompletableFuture<Boundary> command = boundaryCommand;
                if (command != null) {
                    EventSet next;
                    while ((next = vm.eventQueue().remove(1)) != null) { handle(next); }
                    synchronized (lock) {
                        checkCapture();
                        command.complete(snapshot()); boundaryCommand = null;
                    }
                }
            }
        } catch (Throwable problem) { fail(problem); }
    }

    private void handle(EventSet set) throws Exception {
        try {
            synchronized (lock) {
                for (Event event : set) {
                    if (event instanceof ClassPrepareEvent prepare) { bind(prepare.referenceType()); }
                    else if (event instanceof BreakpointEvent breakpoint) { breakpoint(breakpoint); }
                    else if (event instanceof ExceptionEvent exception) { exception(exception); }
                    else if (event instanceof ThreadDeathEvent death) { threadDeath(death); }
                    else if (event instanceof VMDeathEvent) {
                        if (!closing || !threads.isEmpty()) { throw invalid("process exit lost admission calls or capture ownership"); }
                        vmDeath = true;
                    } else if (event instanceof VMDisconnectEvent) {
                        if (!closing || !vmDeath) { throw invalid("admission process disconnected without a normal VM death"); }
                        disconnected = true; running = false;
                    } else if (!(event instanceof VMStartEvent)) { throw invalid("unmapped admission capture event"); }
                }
            }
        } finally {
            try { set.resume(); }
            catch (VMDisconnectedException gone) { if (!closing || !vmDeath) { throw gone; } }
        }
    }

    private void bind(ReferenceType type) throws Exception {
        List<Key> requested = descriptions.entrySet().stream()
                .filter(entry -> entry.getValue().type().equals(type.name())).map(Map.Entry::getKey).toList();
        if (requested.isEmpty()) { throw invalid("unmapped prepared admission class"); }
        ClassLoaderReference actualLoader = type.classLoader();
        if (actualLoader == null || !LOADER.equals(actualLoader.referenceType().name())
                || loader != null && loader.uniqueID() != actualLoader.uniqueID()
                || requested.stream().anyMatch(bindings::containsKey)) {
            throw invalid("admission class used an unexpected or duplicate application loader");
        }
        for (Key key : requested) {
            Description description = descriptions.get(key);
            List<Method> matches = type.methodsByName(description.method(), description.signature());
            if (matches.size() != 1) { throw invalid("exact admission method missing or ambiguous"); }
            Method method = matches.getFirst();
            if (!method.declaringType().equals(type) || method.isStatic() || method.isNative() || method.isAbstract()
                    || method.isBridge() || method.isObsolete() || !Arrays.equals(images.get(key).code(), method.bytecodes())) {
                throw invalid("admission method differs from its immutable artifact provenance");
            }
            List<Integer> returns = BenchmarkJdiCostObserver.returnOffsets(images.get(key).code());
            install(method, 0, new Site(key, true));
            for (int offset : returns) { install(method, offset, new Site(key, false)); }
            methods.put(key, method);
            bindings.put(key, new Binding(type.name(), method.name(), method.signature(), images.get(key).origin(),
                    sha256(images.get(key).code()), actualLoader.uniqueID(), LOADER, 0, returns));
        }
        loader = actualLoader;
    }

    private void install(Method method, long offset, Site site) {
        Location location = method.locationOfCodeIndex(offset);
        if (location == null || location.codeIndex() != offset) { throw invalid("exact admission instruction unavailable"); }
        var request = vm.eventRequestManager().createBreakpointRequest(location);
        request.putProperty("admission-site", site);
        request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD); request.enable();
    }

    private void breakpoint(BreakpointEvent event) throws Exception {
        eventBudget();
        Object property = event.request().getProperty("admission-site");
        if (!(property instanceof Site site) || !event.location().method().equals(methods.get(site.key()))) {
            throw invalid("breakpoint lost its exact admission method binding");
        }
        List<StackFrame> frames = frames(event.thread());
        StackFrame frame = frames.getFirst();
        List<Value> arguments = frame.getArgumentValues();
        Description description = descriptions.get(site.key());
        if (arguments.size() != description.arguments()) { throw invalid("real observed method arguments unavailable"); }
        String pipeline = null;
        if (description.pipelineArgument() >= 0) {
            if (!(arguments.get(description.pipelineArgument()) instanceof StringReference actual)) {
                throw invalid("real admission pipeline argument unavailable");
            }
            pipeline = actual.value();
        }
        ObjectReference receiver = frame.thisObject();
        if (receiver == null) { throw invalid("admission receiver unavailable"); }
        long threadId = event.thread().uniqueID();
        ThreadState state = threads.get(threadId);
        if (state != null) { reconcile(state, frames, site.entry() ? site.key() : null); }
        if (site.entry()) {
            if (state == null) {
                if (threads.size() >= MAX_THREADS) { throw invalid("admission thread budget exceeded"); }
                ExceptionRequest exceptions = vm.eventRequestManager().createExceptionRequest(null, true, true);
                exceptions.addThreadFilter(event.thread()); exceptions.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
                ThreadDeathRequest death = vm.eventRequestManager().createThreadDeathRequest();
                death.addThreadFilter(event.thread()); death.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
                exceptions.enable(); death.enable();
                state = new ThreadState(event.thread(), exceptions, death); threads.put(threadId, state);
            }
            if (state.calls.size() >= MAX_CALLS || !state.calls.isEmpty() && state.calls.peek().depth >= frames.size()) {
                throw invalid("admission entry overlapped an unreturned call or exceeded its bound");
            }
            boolean qualified = site.key().lease() != null || pipelineId.equals(pipeline);
            state.calls.push(new Call(site.key(), frames.size(), receiver, pipeline, qualified));
            if (qualified) { totals.get(site.key()).entries++; }
        } else {
            Call call = state == null ? null : state.calls.peek();
            if (call == null || !call.key.equals(site.key()) || call.depth != frames.size()
                    || call.receiver.uniqueID() != receiver.uniqueID() || !Objects.equals(call.pipeline, pipeline)
                    || call.escapingException) { throw invalid("normal admission return lost its exact entry correlation"); }
            if (call.qualified) { totals.get(call.key).returns++; }
            state.calls.pop(); removeEmpty(state);
        }
    }

    private void exception(ExceptionEvent event) throws Exception {
        eventBudget();
        ThreadState state = threads.get(event.thread().uniqueID());
        if (state == null) { throw invalid("exception event lost its observed admission thread"); }
        List<StackFrame> frames = frames(event.thread());
        reconcile(state, frames, null);
        if (state.calls.isEmpty()) { return; }
        Location caught = event.catchLocation();
        List<Integer> candidates = new ArrayList<>();
        if (caught != null) {
            for (int i = 0; i < frames.size(); i++) {
                if (frames.get(i).location().method().equals(caught.method())) { candidates.add(i); }
            }
            if (candidates.isEmpty()) {
                throw invalid("exception catch frame missing; " + catchDiagnostic(event, frames, candidates, state));
            }
        } else { candidates.add(frames.size()); }
        List<Call> escaping = new ArrayList<>();
        for (Call call : state.calls) {
            int callIndex = frames.size() - call.depth;
            boolean outside = callIndex < candidates.getFirst();
            if (candidates.stream().anyMatch(index -> (callIndex < index) != outside)) {
                throw invalid("exception catch candidates disagree about admission unwind; "
                        + catchDiagnostic(event, frames, candidates, state));
            }
            if (outside) { escaping.add(call); }
        }
        escaping.forEach(call -> call.escapingException = true);
    }

    private static String catchDiagnostic(ExceptionEvent event, List<StackFrame> frames,
            List<Integer> candidates, ThreadState state) {
        return "exceptionType=" + safeMetadata(event.exception().referenceType().name())
                + ", throwMethod=" + methodMetadata(event.location().method())
                + ", catchMethod=" + (event.catchLocation() == null ? "UNCAUGHT" : methodMetadata(event.catchLocation().method()))
                + ", stackDepth=" + frames.size() + ", candidateDepths="
                + candidates.stream().map(index -> frames.size() - index).toList()
                + ", observedDepths=" + state.calls.stream().map(call -> call.key.label() + "@" + call.depth).toList();
    }

    private static String methodMetadata(Method method) {
        return safeMetadata(method.declaringType().name()) + "#" + safeMetadata(method.name())
                + safeMetadata(method.signature());
    }

    private static String safeMetadata(String value) {
        return value != null && value.length() <= 512 && value.chars().allMatch(character -> character >= 33 && character <= 126)
                ? value : "UNMAPPED";
    }

    /** An escaping exception is counted only after actual unwind, never as a normal return. */
    private void reconcile(ThreadState state, List<StackFrame> frames, Key newEntry) {
        while (!state.calls.isEmpty() && state.calls.peek().escapingException) {
            Call call = state.calls.peek();
            boolean present = present(call, frames);
            boolean reentered = call.key.equals(newEntry) && frames.size() == call.depth;
            if (present && !reentered) { break; }
            state.calls.pop();
            if (call.qualified) { totals.get(call.key).exceptional++; }
        }
        for (Call call : state.calls) {
            if (!present(call, frames)) { throw invalid("admission call disappeared without a return or escaping exception"); }
        }
        // An entry in the same event immediately reuses its exception request.
        if (newEntry == null) { removeEmpty(state); }
    }

    private boolean present(Call call, List<StackFrame> frames) {
        int index = frames.size() - call.depth;
        return index >= 0 && index < frames.size() && frames.get(index).location().method().equals(methods.get(call.key));
    }

    private List<StackFrame> frames(ThreadReference thread) throws Exception {
        List<StackFrame> frames = thread.frames();
        if (frames.isEmpty() || frames.size() > MAX_FRAMES) { throw invalid("admission stack unavailable or exceeded its bound"); }
        return frames;
    }

    private void removeEmpty(ThreadState state) {
        if (state.calls.isEmpty()) {
            vm.eventRequestManager().deleteEventRequest(state.exceptions);
            vm.eventRequestManager().deleteEventRequest(state.death);
            threads.remove(state.thread.uniqueID());
        }
    }

    private void threadDeath(ThreadDeathEvent event) {
        eventBudget();
        ThreadState state = threads.get(event.thread().uniqueID());
        if (state == null) { return; }
        for (Call call : state.calls) {
            if (!call.escapingException) { throw invalid("thread death lost an admission call without an escaping exception"); }
        }
        while (!state.calls.isEmpty()) {
            Call call = state.calls.pop();
            if (call.qualified) { totals.get(call.key).exceptional++; }
        }
        removeEmpty(state);
    }

    private Boundary snapshot() throws Exception {
        return snapshot(true);
    }

    private Boundary snapshot(boolean live) throws Exception {
        if (bindings.size() != descriptions.size()) { throw invalid("zero counts require every requested verified method binding"); }
        if (live) {
            for (Key key : descriptions.keySet()) {
                Method method = methods.get(key);
                if (method.isObsolete() || !Arrays.equals(method.bytecodes(), images.get(key).code())) {
                    throw invalid("admission method provenance changed at a boundary");
                }
            }
            for (ThreadState state : List.copyOf(threads.values())) { reconcile(state, frames(state.thread), null); }
        } else if (!vmDeath || !disconnected || !threads.isEmpty()) {
            throw invalid("terminal counts require actual VM death, disconnect and complete call drain");
        }
        Map<Target, Binding> targetBindings = new EnumMap<>(Target.class);
        Map<Target, Counts> counts = new EnumMap<>(Target.class);
        Map<LeaseTarget, Binding> leaseBindings = new EnumMap<>(LeaseTarget.class);
        Map<LeaseTarget, Counts> leaseCounts = new EnumMap<>(LeaseTarget.class);
        for (Key key : descriptions.keySet()) {
            Totals total = totals.get(key);
            long inFlight = threads.values().stream().flatMap(state -> state.calls.stream())
                    .filter(call -> call.key.equals(key) && call.qualified).count();
            if (total.entries != total.returns + total.exceptional + inFlight) {
                throw invalid("admission entry accounting lost a call");
            }
            Counts reading = new Counts(total.entries, total.returns, total.exceptional, inFlight);
            if (key.target() != null) {
                targetBindings.put(key.target(), bindings.get(key)); counts.put(key.target(), reading);
            } else {
                leaseBindings.put(key.lease(), bindings.get(key)); leaseCounts.put(key.lease(), reading);
            }
        }
        return new Boundary(System.nanoTime(), artifactSha256, pipelineId, targetBindings, counts, events,
                threads.values().stream().mapToInt(state -> state.calls.size()).sum(), true,
                leaseObservationEnabled, leaseBindings, leaseCounts, vmDeath, disconnected);
    }

    private void eventBudget() { if (++events > MAX_EVENTS) { throw invalid("admission event budget exceeded"); } }

    private void fail(Throwable problem) {
        AssertionError safe = problem instanceof AssertionError assertion ? assertion
                : invalid("admission capture failed; type=" + problem.getClass().getName());
        failure.compareAndSet(null, safe); running = false;
        CompletableFuture<Boundary> command = boundaryCommand;
        if (command != null) { command.completeExceptionally(safe); }
        detach();
    }

    private void detach() {
        try { vm.dispose(); }
        catch (VMDisconnectedException ignored) { }
        catch (Throwable cleanup) {
            AssertionError primary = failure.get();
            if (primary != null) { primary.addSuppressed(invalid("admission debugger detach failed")); }
            server.kill();
        }
    }

    /** Captures shutdown callbacks too; only actual owned VM death and disconnect seal the final counts. */
    Boundary shutdownAndFinish() throws Exception {
        synchronized (boundaryLock) {
            synchronized (lock) {
                if (closed) {
                    checkCapture();
                    if (finalBoundary == null) { throw invalid("capture ended without a final drained boundary"); }
                    return finalBoundary;
                }
            }
            Throwable primary = null;
            Boundary result = null;
            try {
                checkCapture(); boundary();
                synchronized (lock) { closing = true; }
                server.close(); pump.join(5_000); checkCapture();
                synchronized (lock) {
                    if (server.isAlive() || pump.isAlive() || !vmDeath || !disconnected || !threads.isEmpty()) {
                        throw invalid("admission capture did not fully drain on normal owned process exit");
                    }
                    if (!artifactSha256.equals(sha256(jar))) { throw invalid("application artifact changed during shutdown capture"); }
                    result = snapshot(false);
                    if (!result.fullyDrained()) { throw invalid("terminal admission evidence was not fully drained"); }
                }
            } catch (Exception | Error problem) {
                fail(problem);
                AssertionError safe = invalid("admission close failed; type=" + safeMetadata(problem.getClass().getName()));
                safe.initCause(problem); primary = safe;
                throw safe;
            }
            finally {
                running = false; detach(); server.close(); pump.interrupt();
                try { pump.join(2_000); }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    if (primary != null) { primary.addSuppressed(invalid("interrupted while stopping admission event pump")); }
                    else { throw interrupted; }
                } finally { synchronized (lock) { closed = true; } }
                if (pump.isAlive()) {
                    AssertionError stopped = invalid("owned admission event pump did not stop");
                    if (primary != null) { primary.addSuppressed(stopped); } else { throw stopped; }
                }
                if (primary instanceof InterruptedException) { Thread.currentThread().interrupt(); }
            }
            checkCapture();
            finalBoundary = result;
            return result;
        }
    }

    @Override public void close() throws Exception {
        synchronized (boundaryLock) {
            synchronized (lock) { if (closed) { return; } }
            shutdownAndFinish();
        }
    }

    private static Map<Key, Description> descriptions(boolean observeLeases) {
        Map<Key, Description> descriptions = new LinkedHashMap<>();
        for (Target target : Target.values()) { descriptions.put(new Key(target, null), DESCRIPTIONS.get(target)); }
        if (observeLeases) {
            for (LeaseTarget lease : LeaseTarget.values()) { descriptions.put(new Key(null, lease), LEASE_DESCRIPTIONS.get(lease)); }
        }
        return Map.copyOf(descriptions);
    }

    private static Map<Key, Image> images(Path jar, Map<Key, Description> descriptions) throws Exception {
        Map<Key, Image> images = new LinkedHashMap<>();
        List<String> types = descriptions.values().stream().map(Description::type).distinct().toList();
        try (ZipFile boot = new ZipFile(jar.toFile())) {
            for (String type : types) {
                String resource = type.replace('.', '/') + ".class";
                ZipEntry direct = boot.getEntry("BOOT-INF/classes/" + resource);
                if (direct != null) {
                    try (InputStream stream = boot.getInputStream(direct)) {
                        addImages(images, descriptions, type, direct.getName(), stream);
                    }
                }
            }
            List<? extends ZipEntry> libraries = boot.stream().filter(entry -> entry.getName().startsWith("BOOT-INF/lib/")
                    && entry.getName().endsWith(".jar")).toList();
            if (libraries.size() > 512) { throw invalid("application library count exceeded its bound"); }
            for (ZipEntry library : libraries) {
                if (library.getSize() < 0 || library.getSize() > 128L * 1024 * 1024) { throw invalid("application library exceeded its bound"); }
                try (ZipInputStream nested = new ZipInputStream(boot.getInputStream(library))) {
                    ZipEntry entry;
                    int entries = 0;
                    while ((entry = nested.getNextEntry()) != null) {
                        if (++entries > 100_000) { throw invalid("application library entry count exceeded its bound"); }
                        for (String type : types) {
                            String resource = type.replace('.', '/') + ".class";
                            if (entry.getName().equals(resource)) {
                                addImages(images, descriptions, type, library.getName() + "!/" + resource, nested);
                            }
                        }
                    }
                }
            }
        }
        if (images.size() != descriptions.size()) { throw invalid("application artifact lacked every requested exact method"); }
        return Map.copyOf(images);
    }

    private static void addImages(Map<Key, Image> images, Map<Key, Description> descriptions,
            String type, String origin, InputStream stream) throws Exception {
        byte[] bytes = stream.readNBytes(MAX_CLASS_BYTES + 1);
        if (bytes.length > MAX_CLASS_BYTES) { throw invalid("admission class exceeded its bound"); }
        for (Map.Entry<Key, Description> entry : descriptions.entrySet()) {
            Description description = entry.getValue();
            if (!type.equals(description.type())) { continue; }
            byte[] code = methodCode(bytes, description.method() + description.signature());
            if (code == null || BenchmarkJdiCostObserver.returnOffsets(code).isEmpty()
                    || images.putIfAbsent(entry.getKey(), new Image(origin, code)) != null) {
                throw invalid("admission method missing, duplicated or lacked a normal return");
            }
        }
    }

    /** Reads a pinned Code attribute without generating, rewriting or loading its class. */
    private static byte[] methodCode(byte[] bytes, String selected) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (input.readInt() != 0xcafebabe) { throw invalid("invalid admission class header"); }
            input.skipNBytes(4);
            String[] utf = new String[input.readUnsignedShort()];
            for (int i = 1; i < utf.length; i++) {
                switch (input.readUnsignedByte()) {
                    case 1 -> utf[i] = input.readUTF();
                    case 3, 4, 9, 10, 11, 12, 17, 18 -> input.skipNBytes(4);
                    case 5, 6 -> { input.skipNBytes(8); i++; }
                    case 7, 8, 16, 19, 20 -> input.skipNBytes(2);
                    case 15 -> input.skipNBytes(3);
                    default -> throw invalid("unsupported admission class constant-pool entry");
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
                    if (size > MAX_CLASS_BYTES) { throw invalid("admission method attribute exceeded its bound"); }
                    if (selected.equals(key) && "Code".equals(name)) {
                        byte[] attribute = input.readNBytes((int) size);
                        if (attribute.length != size || found != null) { throw invalid("admission Code attribute truncated or duplicated"); }
                        try (DataInputStream code = new DataInputStream(new ByteArrayInputStream(attribute))) {
                            code.skipNBytes(4);
                            int length = code.readInt();
                            if (length < 1 || length > 65_535) { throw invalid("invalid admission Code length"); }
                            found = code.readNBytes(length);
                            if (found.length != length) { throw invalid("admission instructions truncated"); }
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
            if (size > MAX_CLASS_BYTES) { throw invalid("admission class attribute exceeded its bound"); }
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
    private static AssertionError invalid(String reason) { return new AssertionError("Invalid execution admission capture: " + reason); }
}
