package io.tapstate.e2e;

import com.sun.jdi.*;
import com.sun.jdi.connect.Connector;
import com.sun.jdi.connect.ListeningConnector;
import com.sun.jdi.event.*;
import com.sun.jdi.request.EventRequest;
import io.tapstate.core.common.JsonWriter;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/** One passive history entry seam; completed server queries remain separate profiler evidence. */
final class OverviewHistoryCallerJdiSession implements AutoCloseable {
    private static final String TYPE = "io.tapstate.adapters.mongostore.MongoRateHistoryStore";
    private static final String METHOD = "readPageVisible";
    private static final String DESCRIPTOR = "(Ljava/lang/String;Lio/tapstate/spi/store/RateHistoryStore$Visibility;"
            + "Ljava/time/Instant;Ljava/time/Instant;Lio/tapstate/spi/store/RateHistoryStore$Key;I)"
            + "Lio/tapstate/spi/store/RateHistoryStore$Page;";
    private static final String LOADER = "org.springframework.boot.loader.launch.LaunchedClassLoader";
    private static final int MAX_EVENTS = 50000, MAX_RECORDS = 512, MAX_FRAMES = 128;
    private static final int MAX_RECORD_BYTES = 65536, MAX_BYTES = 2 * 1024 * 1024, MAX_CLASS_BYTES = 1048576;
    private static final Set<String> WANTED = Set.of(TYPE,
            "io.tapstate.spi.store.RateHistoryStore$Visibility", "io.tapstate.spi.store.RateHistoryStore$Key",
            "com.mongodb.client.internal.MongoCollectionImpl", "com.mongodb.MongoNamespace",
            "io.tapstate.app.HistoryRollupWorker", "io.tapstate.app.HistoryRollupWorker$LevelWork",
            "io.tapstate.spi.store.HistoryRollupStore$Resolution",
            "io.tapstate.control.core.PipelineCatalogService", "io.tapstate.control.core.PipelineObservationQueryService");
    private record Image(String origin, Map<String, byte[]> methods, Map<String, String> fields) { }
    private final Path jar;
    private final String sha;
    private final Map<String, Image> images;
    private final RealProcessServer server;
    private final VirtualMachine vm;
    private final String vmVersion;
    private Map<String, Object> binding;
    private final Thread pump;
    private final Object lock = new Object(), commands = new Object();
    private final AtomicReference<AssertionError> failure = new AtomicReference<>();
    private final Set<String> layouts = new LinkedHashSet<>();
    private final List<Map<String, Object>> records = new ArrayList<>();
    private volatile CompletableFuture<Map<String, Object>> command;
    private volatile boolean running = true;
    private Method method;
    private long loader = -1, events, retainedBytes;
    private boolean closing, closed, vmDeath, disconnected;

    private OverviewHistoryCallerJdiSession(Path jar, String sha, Map<String, Image> images,
            RealProcessServer server, VirtualMachine vm) {
        this.jar = jar; this.sha = sha; this.images = images; this.server = server; this.vm = vm;
        this.vmVersion = vm.version();
        var prepare = vm.eventRequestManager().createClassPrepareRequest();
        prepare.addClassFilter(TYPE); prepare.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD); prepare.enable();
        pump = new Thread(this::loop, "overview-history-caller-events"); pump.setDaemon(true); pump.start();
    }

    static OverviewHistoryCallerJdiSession start(String uri, Path input, String sha) throws Exception {
        Path jar = input.toRealPath();
        require(sha.matches("[0-9a-f]{64}") && Files.size(jar) <= 512L * 1024 * 1024
                && sha.equals(PipelineBenchmarkLiveRunIT.sha256(jar)), "immutable JAR mismatch");
        Map<String, Image> images = images(jar);
        require(images.keySet().containsAll(WANTED) && images.get(TYPE).methods().containsKey(METHOD + DESCRIPTOR),
                "required artifact method or field classes unavailable");
        require(sha.equals(PipelineBenchmarkLiveRunIT.sha256(jar)), "JAR changed during metadata read");
        ListeningConnector connector = Bootstrap.virtualMachineManager().listeningConnectors().stream()
                .filter(value -> value.name().equals("com.sun.jdi.SocketListen")).findFirst().orElseThrow();
        Map<String, Connector.Argument> options = connector.defaultArguments();
        options.get("localAddress").setValue("127.0.0.1"); options.get("port").setValue("0");
        options.get("timeout").setValue("15000");
        String address = connector.startListening(options);
        RealProcessServer server = null;
        VirtualMachine vm = null;
        OverviewHistoryCallerJdiSession session = null;
        boolean listening = true;
        Throwable primary = null;
        try {
            String dial = BenchmarkJdiCostObserver.numericLoopbackDialAddress(
                    options.get("localAddress").value(), address, options.get("port").value());
            server = RealProcessServer.launchingWithJvmArguments(uri, jar,
                    List.of("-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=" + dial), List.of());
            vm = connector.accept(options);
            require(vm.canGetBytecodes(), "bytecode mirror capability unavailable");
            connector.stopListening(options); listening = false;
            session = new OverviewHistoryCallerJdiSession(jar, sha, images, server, vm);
            server.awaitHealthy(); session.check(); return session;
        } catch (Exception | Error problem) {
            primary = problem;
            if (session != null) { try { session.close(); } catch (Exception | Error cleanup) { suppress(problem, cleanup); } }
            else {
                if (vm != null) { try { vm.dispose(); } catch (RuntimeException cleanup) { suppress(problem, cleanup); } }
                if (server != null) { server.close(); }
            }
            throw problem;
        } finally {
            if (listening) {
                try { connector.stopListening(options); }
                catch (Exception | Error cleanup) {
                    if (primary != null) { suppress(primary, cleanup); }
                    else { if (server != null) { server.kill(); } throw cleanup; }
                }
            }
        }
    }

    ServerHandle server() { return server; }
    private void check() { AssertionError problem = failure.get(); if (problem != null) { throw problem; } }

    /** Cumulative entries preserve calls begun before a profiler window; there is no VM-wide stop. */
    Map<String, Object> boundary() throws Exception {
        synchronized (commands) {
            check(); require(!closed && !closing, "caller boundary after close");
            require(sha.equals(PipelineBenchmarkLiveRunIT.sha256(jar)), "immutable JAR changed");
            CompletableFuture<Map<String, Object>> waiting = new CompletableFuture<>();
            command = waiting;
            try { return waiting.get(30, TimeUnit.SECONDS); }
            finally { command = null; }
        }
    }

    private void loop() {
        try {
            while (running) {
                EventSet set = vm.eventQueue().remove(25);
                if (set != null) { handle(set); }
                CompletableFuture<Map<String, Object>> waiting = command;
                if (waiting != null) {
                    EventSet next; int drained = 0;
                    while ((next = vm.eventQueue().remove(1)) != null) {
                        require(++drained <= 2048, "caller event-drain budget exceeded"); handle(next);
                    }
                    synchronized (lock) { check(); waiting.complete(snapshot()); command = null; }
                }
            }
        } catch (Throwable problem) {
            AssertionError error = problem instanceof AssertionError assertion ? assertion
                    : new AssertionError("Unverified history caller capture: " + problem, problem);
            failure.compareAndSet(null, error); running = false;
            if (command != null) { command.completeExceptionally(error); }
            try { vm.dispose(); } catch (RuntimeException gone) { server.kill(); }
        }
    }

    private void handle(EventSet set) throws Exception {
        try {
            synchronized (lock) {
                for (Event event : set) {
                    require(++events <= MAX_EVENTS, "caller event budget exceeded");
                    if (event instanceof ClassPrepareEvent prepare) { bind(prepare.referenceType()); }
                    else if (event instanceof BreakpointEvent entry) { capture(entry); }
                    else if (event instanceof VMDeathEvent) { require(closing, "unowned VM death"); vmDeath = true; }
                    else if (event instanceof VMDisconnectEvent) {
                        require(closing && vmDeath, "disconnect without owned VM death"); disconnected = true; running = false;
                    } else { require(event instanceof VMStartEvent, "unexpected caller capture event"); }
                }
            }
        } finally {
            try { set.resume(); } catch (VMDisconnectedException gone) { require(closing && vmDeath, "unowned disconnect"); }
        }
    }

    private void bind(ReferenceType type) {
        require(method == null && TYPE.equals(type.name()), "unexpected or duplicate caller binding");
        require(type.classLoader() != null && LOADER.equals(type.classLoader().referenceType().name()), "unexpected loader");
        loader = type.classLoader().uniqueID(); validate(type);
        List<Method> matches = type.methodsByName(METHOD, DESCRIPTOR);
        require(matches.size() == 1, "exact history method unavailable"); method = matches.getFirst(); verify(method);
        binding = Map.of("type", TYPE, "method", METHOD, "descriptor", DESCRIPTOR,
                "codeSha256", hash(method.bytecodes()), "origin", images.get(TYPE).origin(), "loaderId", loader);
        Location entry = method.locationOfCodeIndex(0);
        require(entry != null && entry.codeIndex() == 0, "exact entry offset unavailable");
        var request = vm.eventRequestManager().createBreakpointRequest(entry);
        request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD); request.enable();
    }

    private void validate(ReferenceType type) {
        if (type.name().startsWith("java.")) { return; }
        Image image = images.get(type.name());
        require(image != null && type.classLoader() != null && type.classLoader().uniqueID() == loader,
                "decoded class provenance unavailable");
        image.fields().forEach((name, descriptor) -> {
            Field field = type.fieldByName(name);
            require(field != null && field.signature().equals(descriptor), "artifact field layout changed");
        });
    }
    private void verify(Method live) {
        Image image = images.get(live.declaringType().name());
        byte[] code = image == null ? null : image.methods().get(live.name() + live.signature());
        require(code != null && !live.isNative() && !live.isAbstract() && !live.isObsolete()
                && Arrays.equals(code, live.bytecodes()) && live.declaringType().classLoader() != null
                && live.declaringType().classLoader().uniqueID() == loader, "method provenance changed");
    }

    private void capture(BreakpointEvent event) throws Exception {
        require(event.location().method().equals(method) && event.location().codeIndex() == 0, "entry binding changed");
        verify(method);
        List<StackFrame> frames = event.thread().frames();
        require(!frames.isEmpty() && frames.size() <= MAX_FRAMES, "caller stack is unavailable or oversized");
        NativeTelemetryMirror mirror = new NativeTelemetryMirror(this::validate, layouts);
        List<Value> args = frames.getFirst().getArgumentValues(); require(args.size() == 6, "history arguments unavailable");
        ObjectReference receiver = mirror.object(frames.getFirst().thisObject());
        ObjectReference collection = mirror.object(mirror.field(receiver, "collection", "Lcom/mongodb/client/MongoCollection;"));
        ObjectReference namespace = mirror.object(mirror.field(collection, "namespace", "Lcom/mongodb/MongoNamespace;"));
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("entryOrder", events); row.put("capturedAt", Instant.now().toString());
        row.put("thread", event.thread().uniqueID()); row.put("threadName", event.thread().name());
        row.put("receiver", receiver.uniqueID()); row.put("database", mirror.text(mirror.field(namespace, "databaseName", "Ljava/lang/String;")));
        row.put("collection", mirror.text(mirror.field(namespace, "collectionName", "Ljava/lang/String;")));
        row.put("pipelineId", mirror.text(args.get(0)));
        ObjectReference visibility = mirror.object(args.get(1));
        row.put("incarnationId", mirror.scalar(mirror.field(visibility, "incarnationId", "Ljava/lang/String;")));
        row.put("includeLegacy", mirror.scalar(mirror.field(visibility, "includeLegacy", "Z")));
        row.put("from", mirror.scalar(args.get(2))); row.put("to", mirror.scalar(args.get(3)));
        row.put("fromBsonMillis", Instant.parse((String) row.get("from")).toEpochMilli());
        row.put("toBsonMillis", Instant.parse((String) row.get("to")).toEpochMilli());
        row.put("requestedLimit", mirror.integral(args.get(5)));
        if (args.get(4) == null) { row.put("after", null); }
        else {
            ObjectReference key = mirror.object(args.get(4));
            row.put("after", Map.of("observedAt", mirror.scalar(mirror.field(key, "observedAt", "Ljava/time/Instant;")),
                    "internalKey", mirror.text(mirror.field(key, "internalKey", "Ljava/lang/String;"))));
        }
        List<Map<String, Object>> stack = new ArrayList<>();
        boolean rollup = false, api = false;
        for (StackFrame frame : frames) {
            Method caller = frame.location().method(); String type = caller.declaringType().name();
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("type", type); item.put("method", caller.name()); item.put("descriptor", caller.signature());
            item.put("codeIndex", frame.location().codeIndex());
            if (images.containsKey(type)) {
                verify(caller); item.put("provenance", "EXACT_ARTIFACT_METHOD");
                item.put("codeSha256", hash(caller.bytecodes())); item.put("origin", images.get(type).origin());
                if (type.equals("io.tapstate.app.HistoryRollupWorker") && caller.name().equals("processOne")) {
                    List<Value> level = frame.getArgumentValues(); require(level.size() == 1, "actual rollup level unavailable");
                    row.put("resolution", mirror.enumName(mirror.field(mirror.object(level.getFirst()), "resolution",
                            "Lio/tapstate/spi/store/HistoryRollupStore$Resolution;")));
                    rollup = true;
                }
                if (type.equals("io.tapstate.control.core.PipelineCatalogService") && caller.name().equals("list")
                        || type.equals("io.tapstate.control.core.PipelineObservationQueryService")
                            && Set.of("status", "metrics").contains(caller.name())) { api = true; }
            } else { item.put("provenance", "UNVERIFIED_CALLER_METHOD"); }
            stack.add(Map.copyOf(item));
        }
        row.put("callerKind", rollup && !api ? "ROLLUP" : api && !rollup ? "OVERVIEW_API" : "UNATTRIBUTED");
        row.put("stack", List.copyOf(stack)); row.put("entryOnly", true);
        byte[] bytes = JsonWriter.write(row).getBytes(StandardCharsets.UTF_8);
        require(records.size() < MAX_RECORDS && bytes.length <= MAX_RECORD_BYTES && retainedBytes + bytes.length <= MAX_BYTES,
                "caller record budget exceeded"); retainedBytes += bytes.length;
        records.add(Collections.unmodifiableMap(row));
    }

    private Map<String, Object> snapshot() {
        require(method != null, "live history binding unavailable");
        if (!disconnected) { verify(method); }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("source", "PASSIVE_ARTIFACT_PINNED_HISTORY_ENTRY"); result.put("jarSha256", sha);
        result.put("binding", binding); result.put("entries", List.copyOf(records)); result.put("events", events);
        result.put("vmVersion", vmVersion); result.put("entryOnly", true);
        result.put("normalAndExceptionalReturnsObserved", false); result.put("performanceAcceptanceEligible", false);
        result.put("ownedVmDeath", vmDeath); result.put("ownedVmDisconnected", disconnected);
        return Collections.unmodifiableMap(result);
    }

    Map<String, Object> afterCloseEvidence() {
        synchronized (lock) { check(); require(closed && vmDeath && disconnected, "caller shutdown proof unavailable"); return snapshot(); }
    }

    @Override public void close() throws Exception {
        if (closed) { return; }
        Throwable primary = null;
        try {
            check(); synchronized (lock) { closing = true; } server.close(); pump.join(5000); check();
            require(!server.isAlive() && !pump.isAlive() && vmDeath && disconnected, "owned VM shutdown/drain unavailable");
            require(sha.equals(PipelineBenchmarkLiveRunIT.sha256(jar)), "JAR changed during owned run");
        } catch (Exception | Error problem) { primary = problem; throw problem; }
        finally {
            running = false;
            try { vm.dispose(); } catch (VMDisconnectedException gone) { }
            catch (RuntimeException problem) { if (primary != null) { suppress(primary, problem); } else { throw problem; } }
            server.close(); pump.interrupt(); pump.join(2000); closed = true;
        }
    }

    private static Map<String, Image> images(Path jar) throws Exception {
        Map<String, Image> found = new LinkedHashMap<>();
        try (ZipFile boot = new ZipFile(jar.toFile())) {
            for (String type : WANTED) {
                ZipEntry entry = boot.getEntry("BOOT-INF/classes/" + type.replace('.', '/') + ".class");
                if (entry != null) {
                    try (InputStream input = boot.getInputStream(entry)) { putImage(found, type, entry.getName(), input); }
                }
            }
            List<? extends ZipEntry> libraries = boot.stream().filter(entry ->
                    entry.getName().startsWith("BOOT-INF/lib/") && entry.getName().endsWith(".jar")).toList();
            require(libraries.size() <= 512, "artifact library budget exceeded");
            for (ZipEntry library : libraries) {
                require(library.getSize() >= 0 && library.getSize() <= 128L * 1024 * 1024, "artifact library size exceeded");
                try (ZipInputStream nested = new ZipInputStream(boot.getInputStream(library))) {
                    ZipEntry entry; int entries = 0;
                    while ((entry = nested.getNextEntry()) != null) {
                        require(++entries <= 100000, "artifact library entry budget exceeded");
                        if (!entry.getName().endsWith(".class")) { continue; }
                        String type = entry.getName().substring(0, entry.getName().length() - 6).replace('/', '.');
                        if (WANTED.contains(type)) { putImage(found, type, library.getName() + "!/" + entry.getName(), nested); }
                    }
                }
            }
        }
        return Map.copyOf(found);
    }

    private static void putImage(Map<String, Image> images, String type, String origin, InputStream input) throws Exception {
        byte[] bytes = input.readNBytes(MAX_CLASS_BYTES + 1);
        if (bytes.length > MAX_CLASS_BYTES || images.containsKey(type)) { throw new AssertionError("duplicate or oversized artifact class"); }
        try (DataInputStream data = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (data.readInt() != 0xcafebabe) { throw new AssertionError("invalid class header"); }
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
                    default -> throw new AssertionError("unsupported constant-pool tag");
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
                    if (length < 0 || length > MAX_CLASS_BYTES) { throw new AssertionError("class attribute budget exceeded"); }
                    byte[] value = data.readNBytes(length);
                    if (value.length != length) { throw new AssertionError("truncated class attribute"); }
                    if (name.equals("Code")) {
                        try (DataInputStream code = new DataInputStream(new ByteArrayInputStream(value))) {
                            code.skipNBytes(4); int size = code.readInt();
                            if (size < 1 || size > 65535) { throw new AssertionError("invalid method code size"); }
                            byte[] body = code.readNBytes(size);
                            if (body.length != size || methods.putIfAbsent(key, body) != null) { throw new AssertionError("truncated or duplicate method"); }
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
            if (size < 0 || size > MAX_CLASS_BYTES) { throw new AssertionError("class field attribute budget exceeded"); }
            input.skipNBytes(size);
        }
    }
    private static String hash(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (java.security.NoSuchAlgorithmException missing) { throw new AssertionError(missing); }
    }
    private static void suppress(Throwable first, Throwable next) { if (first != next) { first.addSuppressed(next); } }
    private static void require(boolean condition, String message) { if (!condition) { throw new AssertionError(message); } }
}
