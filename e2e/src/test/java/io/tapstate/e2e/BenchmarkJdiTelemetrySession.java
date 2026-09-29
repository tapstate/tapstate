package io.tapstate.e2e;

import com.sun.jdi.*;
import com.sun.jdi.connect.Connector;
import com.sun.jdi.connect.ListeningConnector;
import com.sun.jdi.event.*;
import com.sun.jdi.request.EventRequest;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.Count;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.Namespace;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.Unit;
import static io.tapstate.e2e.BenchmarkJdiCostObserver.WireKey;

/** A separate boot-process cost capture; debugger windows are never performance samples. */
final class BenchmarkJdiTelemetrySession implements AutoCloseable {
    enum Mode { PASSIVE_JDWP, ACTIVE_CAPTURE }
    enum Feature { LATEST_PAYLOAD, CHUNKS, ROLLUPS, EVENTS, DISPATCHER, JANITOR }
    enum Origin { WRITE, READ, CLEANUP, ROLLUP_BATCH, JANITOR_BATCH }
    enum Segment { WINDOW, CARRY_IN, DRAIN_TAIL }
    enum Sink { LATEST, HISTORY, EXPORT, EVENT }
    private enum Kind { SCOPE, ENCODER, CONSTRUCTOR, CLOSE, REJECT_ASYNC }
    record CostKey(Segment segment, Origin origin, Namespace namespace, Unit unit) { }
    record WireCostKey(Segment segment, Origin origin, WireKey wire) { }
    record CallbackKey(Segment segment, Origin origin, Namespace namespace) { }
    record SinkHealth(long identity, long coalesced, long dropped, long successes,
            long failures, long timeouts, int queued, int inFlight) { }
    record Health(long identity, long epochIdentity, boolean closed, boolean abort,
            int latestPending, int latestSlots, Map<Sink, SinkHealth> sinks) {
        Health { sinks = Map.copyOf(sinks); }
        boolean quiescent() {
            return latestPending == 0 && latestSlots == 0 && sinks.values().stream()
                    .allMatch(value -> value.queued() == 0 && value.inFlight() == 0);
        }
    }
    record Delta(long coalesced, long dropped, long successes, long failures, long timeouts) { }
    record Pending(Map<CostKey, Long> costs, Map<WireCostKey, Long> commands,
            Map<CallbackKey, Long> callbacks) {
        Pending { costs = Map.copyOf(costs); commands = Map.copyOf(commands); callbacks = Map.copyOf(callbacks); }
    }
    record Boundary(long atNanos, int activeScopes, int openCalls, Health health, Pending pending) { }
    /** Counts use the invocation's entry cohort; pending boundary maps account for carry-in and drain. */
    record Evidence(BenchmarkJdiCostObserver.Arm arm, String artifactSha256, Mode mode, Set<Feature> available,
            Map<CostKey, Count> costs, Map<WireCostKey, Count> commands,
            Map<CallbackKey, Count> callbacks, Boundary begin, Boundary cutoff, Boundary shutdown,
            Map<Sink, Delta> deltas, long events, long handlingNanos, long windowBreakpointEvents,
            long drainBreakpointEvents, long drainHandlingNanos, long excludedLoaders,
            boolean fullyDrained, Set<BenchmarkJdiCostObserver.Unavailable> unavailable) {
        Evidence {
            available = Set.copyOf(available); costs = Map.copyOf(costs);
            commands = Map.copyOf(commands); callbacks = Map.copyOf(callbacks);
            deltas = Map.copyOf(deltas); unavailable = Set.copyOf(unavailable);
        }
        long entries(Unit unit) {
            if (mode != Mode.ACTIVE_CAPTURE) { throw invalid("scoped cost counts are unavailable in passive mode"); }
            return costs.entrySet().stream().filter(entry -> entry.getKey().unit() == unit)
                    .mapToLong(entry -> entry.getValue().entries()).sum();
        }
    }
    private record Spec(String type, String name, String descriptor, Kind kind,
            Namespace namespace, Origin origin, Unit unit) { }
    private record Site(Spec spec, boolean entry) { }
    private record Scope(Spec spec, int depth, BenchmarkInvocationCohorts.Invocation<CallbackKey> callback) { }
    private record Call(Spec spec, int depth, BenchmarkInvocationCohorts.Invocation<CostKey> cost,
            BenchmarkInvocationCohorts.Invocation<WireCostKey> wire,
            int requestId, long receiver) { }
    private record Template(Method method, long offset, Site site) { }
    private static final class ThreadState {
        final ThreadReference thread;
        final ArrayDeque<Scope> scopes = new ArrayDeque<>();
        final ArrayDeque<Call> calls = new ArrayDeque<>();
        final List<EventRequest> requests = new ArrayList<>();
        ThreadState(ThreadReference thread) { this.thread = thread; }
    }
    private static final String MONGO = "io.tapstate.adapters.mongostore.";
    private static final String APP = "io.tapstate.app.";
    private static final String OBS = "Lio/tapstate/core/lifecycle/Observation;";
    private static final String RATE = "Lio/tapstate/core/lifecycle/RateSample;";
    private static final String CONNECTION = "com.mongodb.internal.connection.InternalStreamConnection";
    private static final String COMMAND = "com.mongodb.internal.connection.CommandMessage";
    private static final String DISPATCHER = APP + "TelemetryDispatcher";
    private static final Set<String> STORE_METHODS = Set.of("save", "saveScoped", "read", "readStored",
            "append", "appendScoped", "upsert", "readPage", "readPageVisible", "readVisible",
            "predecessor", "predecessorVisible", "successor", "successorVisible", "readRange", "delete",
            "hasCommittedManifest",
            "scanLatestAfter", "scanManifestsAfter", "reclaimChunks", "deleteIfUnchanged",
            "deleteManifestIfUnchanged", "deleteAll", "deleteIncarnation", "deleteLegacy");
    private final BenchmarkJdiCostObserver.Artifact artifact;
    private final Mode mode;
    private final RealProcessServer server;
    private final VirtualMachine vm;
    private final Set<String> databases;
    private final List<Spec> specs = new ArrayList<>();
    private final EnumSet<Feature> available = EnumSet.noneOf(Feature.class);
    private final Object lock = new Object();
    private final Map<Long, ThreadState> threads = new HashMap<>();
    private final Map<String, ReferenceType> prepared = new HashMap<>();
    private final Set<String> verifiedWriters = new HashSet<>();
    private final List<Template> filtered = new ArrayList<>();
    private final List<EventRequest> permanent = new ArrayList<>();
    private final BenchmarkInvocationCohorts<CostKey> costs = new BenchmarkInvocationCohorts<>();
    private final BenchmarkInvocationCohorts<WireCostKey> commands = new BenchmarkInvocationCohorts<>();
    private final BenchmarkInvocationCohorts<CallbackKey> callbacks = new BenchmarkInvocationCohorts<>();
    private final AtomicReference<AssertionError> failure = new AtomicReference<>();
    private final Thread pump;
    private volatile CompletableFuture<Boundary> boundaryCommand;
    private volatile boolean running = true;
    private boolean measured, windowEnded, shuttingDown, disconnected;
    private long applicationLoader = -1, eventCount, handlingNanos, excludedLoaders;
    private long windowBreakpointEvents, drainBreakpointEvents, drainHandlingNanos;
    private ObjectReference dispatcher;
    private Boundary begin, cutoff, shutdown;

    private BenchmarkJdiTelemetrySession(BenchmarkJdiCostObserver.Artifact artifact,
            RealProcessServer server, VirtualMachine vm, String database, Mode mode) throws Exception {
        this.artifact = artifact; this.server = server; this.vm = vm; this.mode = Objects.requireNonNull(mode);
        databases = Set.of(database, "admin", "local", "config");
        manifest();
        for (String type : specs.stream().map(Spec::type).distinct().toList()) {
            var request = vm.eventRequestManager().createClassPrepareRequest();
            request.addClassFilter(type);
            request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
            request.enable(); permanent.add(request);
        }
        pump = new Thread(this::loop, "benchmark-jdi-telemetry-events");
        pump.setDaemon(true); pump.start();
    }

    static BenchmarkJdiTelemetrySession launch(BenchmarkJdiCostObserver.Artifact artifact,
            String storeUri, String database) throws Exception {
        return launch(artifact, storeUri, database, server -> { }, () -> { });
    }

    static BenchmarkJdiTelemetrySession launch(BenchmarkJdiCostObserver.Artifact artifact,
            String storeUri, String database, Consumer<RealProcessServer> serverLaunched,
            Runnable listenerStopped) throws Exception {
        return launch(artifact, storeUri, database, SharedMongo.OPERATOR_STATE_DATABASE,
                Mode.ACTIVE_CAPTURE, serverLaunched, listenerStopped);
    }

    static BenchmarkJdiTelemetrySession launch(BenchmarkJdiCostObserver.Artifact artifact,
            String storeUri, String database, String operatorStateDatabase, Mode mode) throws Exception {
        return launch(artifact, storeUri, database, operatorStateDatabase, mode, server -> { }, () -> { });
    }

    private static BenchmarkJdiTelemetrySession launch(BenchmarkJdiCostObserver.Artifact artifact,
            String storeUri, String database, String operatorStateDatabase, Mode mode,
            Consumer<RealProcessServer> serverLaunched, Runnable listenerStopped) throws Exception {
        ListeningConnector connector = Bootstrap.virtualMachineManager().listeningConnectors().stream()
                .filter(value -> value.name().equals("com.sun.jdi.SocketListen")).findFirst()
                .orElseThrow(() -> invalid("loopback JDI connector unavailable"));
        Map<String, Connector.Argument> arguments = connector.defaultArguments();
        arguments.get("localAddress").setValue("127.0.0.1");
        arguments.get("port").setValue("0"); arguments.get("timeout").setValue("15000");
        String reported = connector.startListening(arguments);
        RealProcessServer server = null; VirtualMachine vm = null;
        boolean listening = true;
        Throwable launchFailure = null;
        try {
            String address = BenchmarkJdiCostObserver.numericLoopbackDialAddress(
                    arguments.get("localAddress").value(), reported, arguments.get("port").value());
            server = RealProcessServer.launchingWithJvmArguments(storeUri, operatorStateDatabase, artifact.bootJar,
                    List.of("-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=" + address), List.of());
            serverLaunched.accept(server);
            vm = connector.accept(arguments);
            if (!vm.canGetBytecodes()) { throw invalid("target method bytecodes unavailable"); }
            connector.stopListening(arguments); listening = false;
            listenerStopped.run();
            return new BenchmarkJdiTelemetrySession(artifact, server, vm, database, mode);
        } catch (Throwable problem) {
            launchFailure = problem;
            if (vm != null) { try { vm.dispose(); } catch (Exception ignored) { } }
            if (server != null) { server.close(); }
            throw problem;
        } finally {
            if (listening) {
                try { connector.stopListening(arguments); }
                catch (Throwable cleanup) {
                    AssertionError safe = invalid("listener cleanup failed; type=" + safeType(cleanup.getClass().getName()));
                    if (launchFailure != null) { launchFailure.addSuppressed(safe); }
                    else { throw safe; }
                }
            }
        }
    }

    RealProcessServer server() { return server; }
    void checkCapture() { check(); }

    void begin() throws Exception {
        if (begin != null) { throw invalid("capture began twice"); }
        begin = boundary();
        synchronized (lock) {
            if (available.contains(Feature.DISPATCHER) && begin.health() == null) {
                throw invalid("real dispatcher constructor was not observed before begin");
            }
            costs.begin(); commands.begin(); callbacks.begin(); measured = true;
        }
        vm.resume();
    }

    void cutoff() throws Exception {
        if (begin == null || cutoff != null) { throw invalid("invalid cutoff boundary"); }
        cutoff = boundary();
        synchronized (lock) { costs.cutoff(); commands.cutoff(); callbacks.cutoff(); windowEnded = true; }
        vm.resume();
    }

    Evidence shutdownAndFinish() throws Exception {
        if (cutoff == null) { cutoff(); }
        synchronized (lock) { shuttingDown = true; }
        server.close(); pump.join(5_000); check();
        synchronized (lock) {
            if (!disconnected || !threads.isEmpty()) { throw invalid("exit left unmatched scopes or calls"); }
            costs.requireDrained(); commands.requireDrained(); callbacks.requireDrained();
            if (!costs.carryIn().equals(begin.pending().costs())
                    || !commands.carryIn().equals(begin.pending().commands())
                    || !callbacks.carryIn().equals(begin.pending().callbacks())) {
                throw invalid("begin snapshot lost pending carry-in");
            }
            if (available.contains(Feature.DISPATCHER) && (shutdown == null || shutdown.health() == null
                    || !shutdown.health().closed() || !shutdown.health().quiescent() || shutdown.health().abort())) {
                throw invalid("dispatcher shutdown retained or abandoned admitted work");
            }
            Map<Sink, Delta> deltas = deltas(begin.health(), shutdown == null ? null : shutdown.health());
            if (deltas.values().stream().anyMatch(value -> value.dropped() != 0 || value.failures() != 0
                    || value.timeouts() != 0)) { throw invalid("drain abandoned or failed admitted telemetry"); }
            var unavailable = EnumSet.of(BenchmarkJdiCostObserver.Unavailable.ASYNC_WRITE_COMPLETION,
                    BenchmarkJdiCostObserver.Unavailable.WIRE_DOCUMENT_COUNT,
                    BenchmarkJdiCostObserver.Unavailable.WIRE_BYTE_COUNT);
            if (mode == Mode.PASSIVE_JDWP) { unavailable.add(BenchmarkJdiCostObserver.Unavailable.SCOPED_COST_CAPTURE); }
            return new Evidence(artifact.arm, artifact.arm.sha256, mode, available, costs.counts(), commands.counts(),
                    callbacks.counts(), begin, cutoff, shutdown, deltas, eventCount, handlingNanos,
                    windowBreakpointEvents, drainBreakpointEvents, drainHandlingNanos, excludedLoaders,
                    mode == Mode.ACTIVE_CAPTURE, unavailable);
        }
    }

    private Boundary boundary() throws Exception {
        check(); vm.suspend(); vm.allThreads();
        CompletableFuture<Boundary> command = new CompletableFuture<>(); boundaryCommand = command;
        check(); return command.get(30, TimeUnit.SECONDS);
    }

    private void manifest() throws Exception {
        store("MongoObservationStore", Namespace.OBSERVATION);
        store("MongoRateHistoryStore", Namespace.RAW_HISTORY);
        store("MongoHistoryRollupStore", Namespace.HISTORY_ROLLUPS);
        store("MongoPipelineEventStore", Namespace.EVENTS);
        encoder(MONGO + "MongoObservationStore", "toDocument", "(" + OBS + ")Lorg/bson/Document;",
                Namespace.OBSERVATION, Unit.OBSERVATION_DOCUMENT_BUILD);
        encoder(MONGO + "MongoRateHistoryStore", "toDocument", "(" + RATE + ")Lorg/bson/Document;",
                Namespace.RAW_HISTORY, Unit.RATE_DOCUMENT_BUILD);
        encoder(MONGO + "LatestObservationPayloadCodec", "encode", "(" + OBS
                + "Lio/tapstate/adapters/mongostore/LatestObservationPayloadCodec$ChunkWriter;)"
                + "Lio/tapstate/adapters/mongostore/LatestObservationPayloadCodec$Encoded;",
                Namespace.OBSERVATION, Unit.LATEST_OBSERVATION_BINARY_ENCODER_INVOCATION);
        encoder(MONGO + "MongoHistoryRollupStore", "toDocument",
                "(Lio/tapstate/spi/store/HistoryRollupStore$Bucket;)Lorg/bson/Document;",
                Namespace.HISTORY_ROLLUPS, Unit.ROLLUP_DOCUMENT_BUILD);
        encoder(MONGO + "MongoPipelineEventStore", "toDocument",
                "(Lio/tapstate/core/lifecycle/PipelineEvent;)Lorg/bson/Document;",
                Namespace.EVENTS, Unit.EVENT_DOCUMENT_BUILD);
        encoder(MONGO + "MongoLatestObservationStorage", "chunkDocument",
                "(Lorg/bson/types/Binary;Lorg/bson/types/Binary;Ljava/lang/String;"
                + "Lio/tapstate/adapters/mongostore/LatestObservationPayloadCodec$Chunk;)Lorg/bson/Document;",
                Namespace.OBSERVATION_CHUNKS, Unit.OBSERVATION_CHUNK_DOCUMENT_BUILD);
        encoder("org.bson.codecs.DocumentCodec", "encode", "(Lorg/bson/BsonWriter;Lorg/bson/Document;"
                + "Lorg/bson/codecs/EncoderContext;)V", null, Unit.BSON_BINARY_ENCODER_INVOCATION);
        encoder("org.bson.codecs.BsonDocumentCodec", "encode", "(Lorg/bson/BsonWriter;Lorg/bson/BsonDocument;"
                + "Lorg/bson/codecs/EncoderContext;)V", null, Unit.BSON_DOCUMENT_BINARY_ENCODER_INVOCATION);
        encoder(COMMAND, "encodeMessageBody", "(Lcom/mongodb/internal/connection/ByteBufferBsonOutput;"
                + "Lcom/mongodb/internal/connection/OperationContext;)V", null, Unit.WIRE_COMMAND_BINARY_ENCODER_INVOCATION);
        encoder(CONNECTION, "sendMessage", "(Ljava/util/List;ILcom/mongodb/internal/connection/OperationContext;)V",
                null, Unit.SYNC_COMMAND_SEND);
        add(new Spec(CONNECTION, "sendMessageAsync", "(Ljava/util/List;ILcom/mongodb/internal/connection/OperationContext;"
                + "Lcom/mongodb/internal/async/SingleResultCallback;)V", Kind.REJECT_ASYNC, null, null, null));
        add(new Spec(APP + "HistoryRollupWorker", "runOneBatch", "()V", Kind.SCOPE,
                Namespace.HISTORY_ROLLUPS, Origin.ROLLUP_BATCH, null));
        add(new Spec(APP + "ObservationJanitor", "runOneBatch", "()V", Kind.SCOPE,
                Namespace.OBSERVATION, Origin.JANITOR_BATCH, null));
        add(new Spec(DISPATCHER, "<init>", "(Lio/tapstate/runtime/scheduler/ObservationPublisher;"
                + "Lio/tapstate/runtime/scheduler/RateSampler;Lio/tapstate/spi/metrics/MetricsExport;"
                + "Lio/tapstate/app/ObservationScopeRegistry;Lio/tapstate/spi/store/PipelineEventStore;"
                + "IILjava/time/Duration;)V", Kind.CONSTRUCTOR, null, null, null));
        add(new Spec(DISPATCHER, "close", "()V", Kind.CLOSE, null, null, null));
        feature(Feature.LATEST_PAYLOAD, MONGO + "LatestObservationPayloadCodec");
        feature(Feature.CHUNKS, MONGO + "MongoLatestObservationStorage");
        feature(Feature.ROLLUPS, MONGO + "MongoHistoryRollupStore");
        feature(Feature.EVENTS, MONGO + "MongoPipelineEventStore");
        feature(Feature.DISPATCHER, DISPATCHER);
        feature(Feature.JANITOR, APP + "ObservationJanitor");
        if (mode == Mode.PASSIVE_JDWP) {
            specs.removeIf(spec -> spec.kind() != Kind.CONSTRUCTOR && spec.kind() != Kind.CLOSE);
        }
    }

    private void feature(Feature feature, String type) throws Exception {
        if (artifact.image(type) != null) { available.add(feature); }
    }

    private void add(Spec spec) throws Exception {
        if (artifact.image(spec.type()) == null) { return; }
        byte[] code = artifact.methodCode(spec.type(), spec.name(), spec.descriptor());
        if (code == null || code.length == 0 || BenchmarkJdiCostObserver.returnOffsets(code).isEmpty()) {
            throw invalid("available class lacked its exact declared signature or normal return");
        }
        specs.add(spec);
    }

    private void encoder(String type, String name, String descriptor, Namespace namespace, Unit unit) throws Exception {
        add(new Spec(type, name, descriptor, Kind.ENCODER, namespace, Origin.WRITE, unit));
    }

    private void store(String simple, Namespace namespace) throws Exception {
        String type = MONGO + simple;
        var image = artifact.image(type);
        if (image == null) { return; }
        for (String key : image.methods().keySet()) {
            int split = key.indexOf('('); String name = key.substring(0, split);
            if (STORE_METHODS.contains(name)) {
                Origin origin = name.startsWith("delete") || name.startsWith("reclaim") ? Origin.CLEANUP
                        : Set.of("save", "saveScoped", "append", "appendScoped", "upsert").contains(name)
                        ? Origin.WRITE : Origin.READ;
                add(new Spec(type, name, key.substring(split), Kind.SCOPE, namespace, origin, null));
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
                        check(); command.complete(snapshot()); boundaryCommand = null;
                    }
                }
            }
        } catch (Throwable problem) {
            String type = problem.getClass().getName();
            if (type.length() > 240 || !type.matches("[A-Za-z_$][A-Za-z0-9_$.]*")) { type = "UNMAPPED"; }
            AssertionError safe = problem instanceof AssertionError assertion ? assertion
                    : invalid("collection failed before complete boot-process drain; type=" + type);
            failure.compareAndSet(null, safe);
            CompletableFuture<Boundary> command = boundaryCommand;
            if (command != null) { command.completeExceptionally(safe); }
            running = false;
            // Detach only this capture's connection so enabled breakpoints cannot orphan suspensions.
            detachOrStopOwnedProcess(vm::dispose);
        }
    }

    void detachOrStopOwnedProcess(Runnable detach) {
        try { detach.run(); }
        catch (VMDisconnectedException alreadyDetached) { }
        catch (Throwable failedDetach) {
            AssertionError primary = failure.get();
            if (primary != null) {
                primary.addSuppressed(invalid("debug detach failed; type=" + safeType(failedDetach.getClass().getName())));
            }
            // The stopped pump cannot service breakpoints reached by a graceful shutdown hook.
            server.kill();
        }
    }

    void stopEventPumpForWitness() throws InterruptedException {
        running = false; pump.join(5_000);
        if (pump.isAlive()) { throw invalid("owned event pump did not stop"); }
    }

    private void handle(EventSet set) throws Exception {
        long started = System.nanoTime();
        try {
            synchronized (lock) {
                for (Event event : set) {
                    if (event instanceof ClassPrepareEvent prepare) { prepared(prepare.referenceType()); }
                    else if (event instanceof BreakpointEvent breakpoint) { breakpoint(breakpoint); }
                    else if (event instanceof VMDeathEvent || event instanceof VMDisconnectEvent) {
                        if (!shuttingDown) { throw invalid("boot process disconnected before shutdown boundary"); }
                        disconnected = true; running = false;
                    } else if (!(event instanceof VMStartEvent)) { throw invalid("unmapped boot capture event"); }
                }
                if (measured) {
                    if (windowEnded) { drainHandlingNanos += System.nanoTime() - started; }
                    else { handlingNanos += System.nanoTime() - started; }
                }
            }
        } finally { if (!disconnected) { set.resume(); } }
    }

    private void prepared(ReferenceType type) throws Exception {
        if (type.classLoader() == null) { throw invalid("owned class used bootstrap loader"); }
        long loader = type.classLoader().uniqueID();
        if (!type.classLoader().referenceType().name().equals("org.springframework.boot.loader.launch.LaunchedClassLoader")) {
            if (type.name().startsWith("io.tapstate.")) { throw invalid("owned scope class used unexpected loader"); }
            excludedLoaders++; return;
        }
        if ((applicationLoader != -1 && applicationLoader != loader)
                || prepared.putIfAbsent(type.name(), type) != null) {
            throw invalid("owned class appeared under duplicate application loader");
        }
        applicationLoader = loader;
        for (Spec spec : specs) {
            if (!spec.type().equals(type.name())) { continue; }
            List<Method> matches = type.methodsByName(spec.name(), spec.descriptor());
            if (matches.size() != 1) { throw invalid("exact owned signature missing or ambiguous"); }
            Method method = matches.getFirst();
            byte[] code = artifact.methodCode(spec.type(), spec.name(), spec.descriptor());
            if (method.isNative() || method.isAbstract() || method.isBridge() || method.isObsolete()
                    || !Arrays.equals(code, method.bytecodes())) {
                throw invalid("owned bytecodes differ from pinned artifact");
            }
            List<Template> sites = new ArrayList<>();
            if (spec.kind() != Kind.CONSTRUCTOR && spec.kind() != Kind.CLOSE) {
                sites.add(new Template(method, 0, new Site(spec, true)));
            }
            if (spec.kind() != Kind.REJECT_ASYNC) {
                for (int offset : BenchmarkJdiCostObserver.returnOffsets(code)) {
                    sites.add(new Template(method, offset, new Site(spec, false)));
                }
            }
            for (Template site : sites) {
                if (spec.kind() == Kind.ENCODER || spec.kind() == Kind.REJECT_ASYNC) {
                    filtered.add(site);
                    for (ThreadState state : threads.values()) { request(site, state); }
                } else { request(site, null); }
            }
        }
    }

    private void request(Template template, ThreadState state) {
        var location = template.method().locationOfCodeIndex(template.offset());
        if (location == null || location.codeIndex() != template.offset()) { throw invalid("exact instruction unavailable"); }
        var request = vm.eventRequestManager().createBreakpointRequest(location);
        if (state != null) { request.addThreadFilter(state.thread); }
        request.putProperty("telemetry-site", template.site());
        request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD); request.enable();
        (state == null ? permanent : state.requests).add(request);
    }

    private void breakpoint(BreakpointEvent event) throws Exception {
        if (++eventCount > 100_000) { throw invalid("boot capture event budget exceeded"); }
        if (measured) {
            if (windowEnded) { drainBreakpointEvents++; } else { windowBreakpointEvents++; }
        }
        Object mapping = event.request().getProperty("telemetry-site");
        if (!(mapping instanceof Site site)) { throw invalid("breakpoint had no closed capture mapping"); }
        Spec spec = site.spec();
        if (spec.kind() == Kind.CONSTRUCTOR) {
            ObjectReference actual = event.thread().frame(0).thisObject();
            if (actual == null || dispatcher != null) { throw invalid("dispatcher ownership missing or duplicated"); }
            dispatcher = actual; return;
        }
        if (spec.kind() == Kind.CLOSE) {
            ObjectReference actual = event.thread().frame(0).thisObject();
            if (dispatcher == null || actual == null || actual.uniqueID() != dispatcher.uniqueID()) {
                throw invalid("dispatcher close changed ownership identity");
            }
            vm.suspend();
            try { vm.allThreads(); shutdown = snapshot(); } finally { vm.resume(); }
            return;
        }
        long id = event.thread().uniqueID(); int depth = event.thread().frameCount();
        ThreadState state = threads.get(id);
        if (spec.kind() == Kind.SCOPE) {
            if (site.entry()) {
                if (state == null) {
                    if (threads.size() >= 128) { throw invalid("scope thread budget exceeded"); }
                    state = new ThreadState(event.thread()); threads.put(id, state);
                    for (Template template : filtered) { request(template, state); }
                }
                if (state.scopes.size() >= 64) { throw invalid("scope depth budget exceeded"); }
                ThreadState enteredState = state;
                var callback = callbacks.enter(segment -> new CallbackKey(segment, originForScope(enteredState, spec), spec.namespace()));
                state.scopes.push(new Scope(spec, depth, callback));
            } else {
                Scope top = state == null ? null : state.scopes.peek();
                if (top == null || top.depth() != depth || !top.spec().equals(spec)) { throw invalid("scope return had no exact entry"); }
                callbacks.returned(top.callback());
                state.scopes.pop();
                if (state.scopes.isEmpty()) {
                    if (!state.calls.isEmpty()) { throw invalid("scope ended with unmatched encoders or sends"); }
                    vm.eventRequestManager().deleteEventRequests(state.requests); threads.remove(id);
                }
            }
            return;
        }
        if (state == null || state.scopes.isEmpty()) { throw invalid("filtered event escaped owned scope"); }
        if (spec.kind() == Kind.REJECT_ASYNC) { throw invalid("async send completion unavailable"); }
        ThreadState activeState = state;
        if (site.entry()) {
            if (state.calls.size() >= 128) { throw invalid("nested call budget exceeded"); }
            Namespace namespace = spec.namespace() == null ? state.scopes.peek().spec().namespace() : spec.namespace();
            Unit unit = spec.unit();
            List<Value> arguments = event.thread().frame(0).getArgumentValues();
            if (unit == Unit.WIRE_COMMAND_BINARY_ENCODER_INVOCATION) {
                ObjectReference message = event.thread().frame(0).thisObject();
                if (message == null) { throw invalid("root command receiver unavailable"); }
                namespace = BenchmarkJdiCostObserver.telemetryCommandKey(message, databases).namespace();
            }
            if (unit == Unit.BSON_BINARY_ENCODER_INVOCATION || unit == Unit.BSON_DOCUMENT_BINARY_ENCODER_INVOCATION) {
                if (arguments.size() != 3 || !(arguments.getFirst() instanceof ObjectReference writer)) {
                    throw invalid("typed BSON writer unavailable");
                }
                String writerType = writerType(writer);
                if (writerType.equals("org.bson.BsonDocumentWriter")) {
                    unit = Unit.BSON_REPRESENTATION_CONVERSION;
                } else if (writerType.equals("org.bson.BsonBinaryWriter")) {
                    Call command = state.calls.stream().filter(call -> call.spec().unit()
                            == Unit.WIRE_COMMAND_BINARY_ENCODER_INVOCATION).findFirst().orElse(null);
                    if (command == null) { throw invalid("binary codec lacked an exact command header scope"); }
                    namespace = command.cost().key().namespace();
                } else { throw invalid("unmapped BSON writer type=" + safeType(writerType)); }
            }
            BenchmarkInvocationCohorts.Invocation<WireCostKey> wire = null;
            int requestId = -1; long receiver = -1;
            if (unit == Unit.SYNC_COMMAND_SEND) {
                ObjectReference actual = event.thread().frame(0).thisObject();
                if (arguments.size() != 3 || !(arguments.get(1) instanceof IntegerValue number) || actual == null) {
                    throw invalid("send arguments unavailable");
                }
                requestId = number.value(); receiver = actual.uniqueID();
                WireKey key = BenchmarkJdiCostObserver.telemetryWireKey(event, requestId, receiver, databases);
                namespace = key.namespace();
                wire = commands.enter(segment -> new WireCostKey(segment, origin(activeState), key));
            }
            Namespace actualNamespace = namespace; Unit actualUnit = unit;
            var cost = costs.enter(segment -> new CostKey(segment, origin(activeState), actualNamespace, actualUnit));
            state.calls.push(new Call(spec, depth, cost, wire, requestId, receiver));
        } else {
            Call top = state.calls.peek();
            if (top == null || !top.spec().equals(spec) || top.depth() != depth) { throw invalid("call return lost its entry"); }
            if (spec.unit() == Unit.SYNC_COMMAND_SEND) {
                List<Value> arguments = event.thread().frame(0).getArgumentValues();
                ObjectReference actual = event.thread().frame(0).thisObject();
                if (actual == null || actual.uniqueID() != top.receiver()
                        || !(arguments.get(1) instanceof IntegerValue number) || number.value() != top.requestId()) {
                    throw invalid("send completion lost request correlation");
                }
            }
            costs.returned(top.cost());
            if (top.wire() != null) { commands.returned(top.wire()); }
            state.calls.pop();
        }
    }

    private String writerType(ObjectReference writer) throws Exception {
        String appending = "com.mongodb.internal.connection.BsonWriterHelper$AppendingBsonWriter";
        Set<String> wrappers = Set.of(appending, "com.mongodb.internal.connection.FieldTrackingBsonWriter",
                "com.mongodb.internal.connection.IdHoldingBsonWriter",
                "com.mongodb.internal.connection.SplittablePayloadBsonWriter");
        Set<Long> visited = new HashSet<>();
        for (int depth = 0; depth < 8; depth++) {
            if (!visited.add(writer.uniqueID())) { throw invalid("BSON writer delegate cycle"); }
            String type = writer.referenceType().name();
            if (wrappers.contains(type)) {
                verifyWriter(writer.referenceType());
                Field delegate = writer.referenceType().fieldByName("bsonWriter");
                if (delegate == null || !delegate.signature().equals("Lorg/bson/BsonWriter;")
                        || !delegate.declaringType().name()
                                .equals("com.mongodb.internal.connection.BsonWriterDecorator")) {
                    throw invalid("pinned BSON writer delegate unavailable");
                }
                verifyWriter(delegate.declaringType());
                // The decorator output is distinct from an IdHolding writer's ancillary ID buffer.
                writer = BenchmarkJdiCostObserver.object(writer.getValue(delegate));
                continue;
            }
            if (type.equals(appending + "$InternalAppendingBsonBinaryWriter")) {
                verifyWriter(writer.referenceType());
                if (!(writer.referenceType() instanceof ClassType actual)
                        || !actual.superclass().name().equals("org.bson.BsonBinaryWriter")) {
                    throw invalid("appending writer superclass differs from pinned binary writer");
                }
                return "org.bson.BsonBinaryWriter";
            }
            if (type.equals("org.bson.BsonBinaryWriter") || type.equals("org.bson.BsonDocumentWriter")) {
                verifyWriter(writer.referenceType());
            }
            return type;
        }
        throw invalid("BSON writer delegate depth exceeded its bound");
    }
    private void verifyWriter(ReferenceType type) throws Exception {
        if (type.classLoader() == null || type.classLoader().uniqueID() != applicationLoader) {
            throw invalid("BSON writer loader differs from its pinned command scope");
        }
        if (verifiedWriters.contains(type.name())) { return; }
        var image = artifact.image(type.name());
        if (image == null) { throw invalid("BSON writer missing from pinned artifact"); }
        for (var declared : image.methods().entrySet()) {
            int split = declared.getKey().indexOf('(');
            List<Method> matches = type.methodsByName(declared.getKey().substring(0, split),
                    declared.getKey().substring(split));
            if (matches.size() != 1 || !Arrays.equals(declared.getValue(), matches.getFirst().bytecodes())) {
                throw invalid("BSON writer bytecodes differ from pinned artifact");
            }
        }
        verifiedWriters.add(type.name());
    }
    private Origin origin(ThreadState state) {
        return state.scopes.stream().map(scope -> scope.spec().origin())
                .filter(value -> value == Origin.ROLLUP_BATCH || value == Origin.JANITOR_BATCH)
                .findFirst().orElse(state.scopes.peek().spec().origin());
    }
    private Origin originForScope(ThreadState state, Spec entering) {
        return state.scopes.stream().map(scope -> scope.spec().origin())
                .filter(value -> value == Origin.ROLLUP_BATCH || value == Origin.JANITOR_BATCH)
                .findFirst().orElse(entering.origin());
    }
    private Boundary snapshot() {
        return new Boundary(System.nanoTime(), threads.values().stream().mapToInt(value -> value.scopes.size()).sum(),
                threads.values().stream().mapToInt(value -> value.calls.size()).sum(), health(),
                new Pending(costs.pending(), commands.pending(), callbacks.pending()));
    }

    private Health health() {
        if (dispatcher == null) { return null; }
        EnumMap<Sink, SinkHealth> values = new EnumMap<>(Sink.class);
        ObjectReference export = objectField(dispatcher, "export");
        List<ReferenceType> types = vm.classesByName("io.tapstate.spi.metrics.MetricsExport");
        if (types.size() != 1 || types.getFirst().fieldByName("NONE") == null) { throw invalid("export wiring identity unavailable"); }
        Value none = types.getFirst().getValue(types.getFirst().fieldByName("NONE"));
        if (!(none instanceof ObjectReference noExport)) { throw invalid("export sentinel unavailable"); }
        for (Sink sink : Sink.values()) {
            boolean wired = sink == Sink.LATEST || (sink == Sink.HISTORY && value(dispatcher, "sampler") != null)
                    || (sink == Sink.EVENT && value(dispatcher, "eventWorker") != null)
                    || (sink == Sink.EXPORT && export.uniqueID() != noExport.uniqueID());
            if (!wired) { continue; }
            String prefix = sink.name().toLowerCase(Locale.ROOT);
            ObjectReference stats = objectField(dispatcher, prefix + "Stats");
            ObjectReference workers = objectField(dispatcher, sink == Sink.LATEST ? "latestWorkers" : prefix + "Worker");
            ObjectReference queue = objectField(workers, "workQueue");
            if (!queue.referenceType().name().equals("java.util.concurrent.ArrayBlockingQueue")) {
                throw invalid("queue implementation differs from declared bounded queue");
            }
            int queued = integer(value(queue, "count"));
            Value items = value(queue, "items");
            if (!(items instanceof ArrayReference array) || array.length() > 4096 || queued < 0
                    || queued > array.length() || array.getValues().stream().filter(Objects::nonNull).count() != queued) {
                throw invalid("queue boundary was incoherent or exceeded its bound");
            }
            values.put(sink, new SinkHealth(stats.uniqueID(), atomicLong(stats, "coalesced"), atomicLong(stats, "dropped"),
                    atomicLong(stats, "successes"), atomicLong(stats, "failures"), atomicLong(stats, "timeouts"),
                    queued, mapCount(objectField(stats, "inFlight"))));
        }
        return new Health(dispatcher.uniqueID(), objectField(dispatcher, "startedAt").uniqueID(),
                atomicInt(dispatcher, "closed") != 0, atomicInt(dispatcher, "abort") != 0,
                atomicInt(dispatcher, "latestPending"), mapCount(objectField(dispatcher, "latestByPipeline")), values);
    }

    private static int mapCount(ObjectReference map) {
        if (!map.referenceType().name().equals("java.util.concurrent.ConcurrentHashMap")) { throw invalid("in-flight map type drifted"); }
        long count = longNumber(value(map, "baseCount"));
        Value cells = value(map, "counterCells");
        if (cells != null) {
            if (!(cells instanceof ArrayReference array) || array.length() > 256) { throw invalid("map counter budget exceeded"); }
            for (Value cell : array.getValues()) {
                if (cell != null) { count += longNumber(value((ObjectReference) cell, "value")); }
            }
        }
        if (count < 0 || count > 4096) { throw invalid("in-flight count unavailable or unbounded"); }
        Value tableValue = value(map, "table");
        int nodes = 0;
        if (tableValue != null) {
            if (!(tableValue instanceof ArrayReference table) || table.length() > 16_384) { throw invalid("map table budget exceeded"); }
            for (Value bucket : table.getValues()) {
                if (bucket == null) { continue; }
                ObjectReference node = (ObjectReference) bucket;
                String type = node.referenceType().name();
                if (type.equals("java.util.concurrent.ConcurrentHashMap$TreeBin")) {
                    Value first = value(node, "first"); node = first == null ? null : (ObjectReference) first;
                } else if (!type.equals("java.util.concurrent.ConcurrentHashMap$Node")) {
                    throw invalid("map boundary contained a resize or unsupported node");
                }
                while (node != null) {
                    if (++nodes > 4096) { throw invalid("map node budget exceeded"); }
                    Value next = value(node, "next"); node = next == null ? null : (ObjectReference) next;
                }
            }
        }
        if (nodes != count) { throw invalid("map boundary was incoherent"); }
        return nodes;
    }

    private static Map<Sink, Delta> deltas(Health first, Health last) {
        if (first == null && last == null) { return Map.of(); }
        if (first == null || last == null || first.identity() != last.identity()
                || first.epochIdentity() != last.epochIdentity() || !first.sinks().keySet().equals(last.sinks().keySet())) {
            throw invalid("dispatcher identity, epoch or wired mask changed");
        }
        EnumMap<Sink, Delta> result = new EnumMap<>(Sink.class);
        first.sinks().forEach((sink, before) -> {
            SinkHealth after = last.sinks().get(sink);
            if (before.identity() != after.identity()) { throw invalid("sink counter identity changed"); }
            result.put(sink, new Delta(delta(before.coalesced(), after.coalesced()), delta(before.dropped(), after.dropped()),
                    delta(before.successes(), after.successes()), delta(before.failures(), after.failures()),
                    delta(before.timeouts(), after.timeouts())));
        });
        return result;
    }

    private static long delta(long before, long after) {
        if (before < 0 || after < before) { throw invalid("counter moved backward or unavailable"); }
        return after - before;
    }
    private static Value value(ObjectReference object, String field) { return BenchmarkJdiCostObserver.field(object, field); }
    private static ObjectReference objectField(ObjectReference object, String field) { return BenchmarkJdiCostObserver.object(value(object, field)); }
    private static long atomicLong(ObjectReference owner, String field) {
        ObjectReference atomic = objectField(owner, field);
        if (!atomic.referenceType().name().equals("java.util.concurrent.atomic.AtomicLong")) { throw invalid("counter type drifted"); }
        return longNumber(value(atomic, "value"));
    }
    private static int atomicInt(ObjectReference owner, String field) { return integer(value(objectField(owner, field), "value")); }
    private static int integer(Value value) {
        if (!(value instanceof IntegerValue number)) { throw invalid("integer measurement unavailable"); }
        return number.value();
    }
    private static long longNumber(Value value) {
        if (!(value instanceof LongValue number)) { throw invalid("long measurement unavailable"); }
        return number.value();
    }
    private void check() { AssertionError problem = failure.get(); if (problem != null) { throw problem; } }
    private static String safeType(String type) {
        return type.length() <= 240 && type.matches("[A-Za-z_$][A-Za-z0-9_$.]*") ? type : "UNMAPPED";
    }
    private static AssertionError invalid(String reason) { return new AssertionError("Invalid boot telemetry capture: " + reason); }

    @Override
    public void close() {
        running = false;
        try { vm.dispose(); } catch (Exception ignored) { }
        server.close(); pump.interrupt();
        try { pump.join(2000); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
    }
}
