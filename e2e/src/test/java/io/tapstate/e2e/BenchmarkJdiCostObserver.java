package io.tapstate.e2e;

import com.sun.jdi.Bootstrap;
import com.sun.jdi.ClassType;
import com.sun.jdi.Field;
import com.sun.jdi.IntegerValue;
import com.sun.jdi.Method;
import com.sun.jdi.ObjectReference;
import com.sun.jdi.ReferenceType;
import com.sun.jdi.StackFrame;
import com.sun.jdi.StringReference;
import com.sun.jdi.Value;
import com.sun.jdi.VirtualMachine;
import com.sun.jdi.connect.Connector;
import com.sun.jdi.connect.ListeningConnector;
import com.sun.jdi.event.BreakpointEvent;
import com.sun.jdi.event.ClassPrepareEvent;
import com.sun.jdi.event.Event;
import com.sun.jdi.event.EventSet;
import com.sun.jdi.event.VMDeathEvent;
import com.sun.jdi.event.VMDisconnectEvent;
import com.sun.jdi.event.VMStartEvent;
import com.sun.jdi.request.BreakpointRequest;
import com.sun.jdi.request.EventRequest;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Exact signature breakpoints for a separate cost witness. Debugger suspension is part of this
 * observer's intrusion and these windows must never supply throughput or latency measurements.
 * No target method is invoked by the debugger and no target bytecode is changed by this helper.
 */
final class BenchmarkJdiCostObserver {

    enum Arm {
        REFERENCE("a552744e904b680da409bbf563793c6523663174a76f748a04965b914d089fa9"),
        OBSERVABILITY("ba6029e0bd66f563b895d210ee53f05ebd665d72b7dd1a7654ab5ba7a5556c2a");

        final String sha256;

        Arm(String sha256) {
            this.sha256 = sha256;
        }
    }

    enum ArtifactSet {
        LEGACY("0.5.0", Arm.REFERENCE.sha256, Arm.OBSERVABILITY.sha256),
        COMMON_SOURCE("0.6.0", "f7b371f38fd6af3dfdf3ba2aca00612ff313de617cff53b251deb6ae77714635",
                "023f2cbf0b4ef3e72b4f992312f2d1761c270037babc63dd67e2ec76f197e9d3");

        final String moduleVersion;
        final String referenceSha;
        final String observabilitySha;
        ArtifactSet(String moduleVersion, String referenceSha, String observabilitySha) {
            this.moduleVersion = moduleVersion; this.referenceSha = referenceSha; this.observabilitySha = observabilitySha;
        }
        String sha256(Arm arm) { return arm == Arm.REFERENCE ? referenceSha : observabilitySha; }
    }

    static ArtifactSet selectedArtifactSet() {
        return ArtifactSet.valueOf(System.getProperty("tapstate.e2e.jdi-cost.artifact-set", "LEGACY"));
    }

    enum Unit {
        OBSERVATION_DOCUMENT_BUILD,
        RATE_DOCUMENT_BUILD,
        BSON_BINARY_ENCODER_INVOCATION,
        BSON_DOCUMENT_BINARY_ENCODER_INVOCATION,
        BSON_REPRESENTATION_CONVERSION,
        WIRE_COMMAND_BINARY_ENCODER_INVOCATION,
        ROLLUP_DOCUMENT_BUILD,
        EVENT_DOCUMENT_BUILD,
        OBSERVATION_CHUNK_DOCUMENT_BUILD,
        LATEST_OBSERVATION_BINARY_ENCODER_INVOCATION,
        SYNC_COMMAND_SEND
    }

    enum Unavailable {
        SCOPED_COST_CAPTURE,
        LATEST_OBSERVATION_BINARY_ENCODER,
        ASYNC_WRITE_COMPLETION,
        WIRE_DOCUMENT_COUNT,
        WIRE_BYTE_COUNT,
        BACKGROUND_BATCH_IO
    }

    enum Namespace {
        OBSERVATION, OBSERVATION_CHUNKS, RAW_HISTORY, HISTORY_ROLLUPS, EVENTS, CONTROL,
        ARTIFACTS, DESIRED, PIPELINE_STATE, WORKLOAD_CLAIMS
    }

    enum Operation {
        INSERT, UPDATE, DELETE, FIND, AGGREGATE, GET_MORE, COUNT, DISTINCT, CREATE, DROP, CONTROL
    }

    enum TargetPhase {
        ORIGINS, ENCODER_SETUP, START_BARRIER, OBSERVATION_BUILD, RATE_BUILD, BSON_ENCODING,
        UNRELATED_METHODS, FAILURE_ENCODING, DUPLICATE_LOADER, WIRE_SETUP, WIRE_COMMANDS,
        LATEST_ORIGINS, LATEST_ENCODING, STOP_BARRIER
    }

    record TargetDiagnostic(TargetPhase phase, String exceptionClass, String linkageSymbol) {
    }

    static final class TargetFailure extends AssertionError {
        private final TargetDiagnostic diagnostic;

        TargetFailure(TargetDiagnostic diagnostic) {
            super("Invalid JDI cost witness: target failed [phase=" + diagnostic.phase()
                    + ", exceptionClass=" + diagnostic.exceptionClass()
                    + ", linkageSymbol=" + (diagnostic.linkageSymbol() == null ? "-" : diagnostic.linkageSymbol()) + "]");
            this.diagnostic = diagnostic;
        }

        TargetDiagnostic diagnostic() {
            return diagnostic;
        }
    }

    record WireKey(Namespace namespace, Operation operation) {
    }

    /** A send's normal return means Stream.write returned, not that the server acknowledged it. */
    record Count(long entries, long normalReturns) {
    }

    /** Handling time is a lower bound on observer intrusion; target suspension also costs time. */
    record Summary(Arm arm, String artifactSha256, Map<Unit, Count> counts,
            Map<WireKey, Count> wireCommands, Set<Unavailable> unavailable,
            long breakpointEvents, long observerHandlingNanos, int breakpointRequests,
            int methodEntryRequests, boolean closedAndDrained) {

        Summary {
            counts = Map.copyOf(counts);
            wireCommands = Map.copyOf(wireCommands);
            unavailable = Set.copyOf(unavailable);
        }

        Count require(Unit unit) {
            Count count = counts.get(unit);
            if (count == null) {
                throw invalid("the requested cost unit was not measured");
            }
            return count;
        }
    }

    record Options(boolean dropFirstEntry, int maximumEvents, boolean failBeforeFinishPublication) {
        static final Options NORMAL = new Options(false, 10_000);

        Options(boolean dropFirstEntry, int maximumEvents) {
            this(dropFirstEntry, maximumEvents, false);
        }

        Options {
            if (maximumEvents <= 0 || maximumEvents > 10_000) {
                throw new IllegalArgumentException("event budget must be positive and bounded");
            }
        }
    }

    private static final String TARGET = "io.tapstate.adapters.mongostore.BenchmarkJdiEncoderTarget";
    private static final String LATEST_TARGET = "io.tapstate.adapters.mongostore.BenchmarkJdiLatestEncoderTarget";
    private static final String OBSERVATION = "io.tapstate.adapters.mongostore.MongoObservationStore";
    private static final String RATE = "io.tapstate.adapters.mongostore.MongoRateHistoryStore";
    private static final String LATEST = "io.tapstate.adapters.mongostore.LatestObservationPayloadCodec";
    private static final String BSON = "org.bson.codecs.DocumentCodec";
    private static final String CONNECTION = "com.mongodb.internal.connection.InternalStreamConnection";
    private static final String COMMAND = "com.mongodb.internal.connection.CommandMessage";
    private static final String OBSERVATION_ARGUMENT = "Lio/tapstate/core/lifecycle/Observation;";
    private static final String TRY_SEND_SIGNATURE = "(Lcom/mongodb/internal/connection/CommandMessage;"
            + "Lcom/mongodb/internal/connection/ByteBufferBsonOutput;"
            + "Lcom/mongodb/internal/connection/OperationContext;)V";
    private static final List<Signature> ENCODERS = List.of(
            new Signature(OBSERVATION, "toDocument", "(" + OBSERVATION_ARGUMENT + ")Lorg/bson/Document;",
                    Unit.OBSERVATION_DOCUMENT_BUILD, false),
            new Signature(RATE, "toDocument", "(Lio/tapstate/core/lifecycle/RateSample;)Lorg/bson/Document;",
                    Unit.RATE_DOCUMENT_BUILD, false),
            new Signature(BSON, "encode", "(Lorg/bson/BsonWriter;Lorg/bson/Document;"
                    + "Lorg/bson/codecs/EncoderContext;)V", Unit.BSON_BINARY_ENCODER_INVOCATION, false));
    private static final Signature LATEST_ENCODER = new Signature(LATEST, "encode",
            "(" + OBSERVATION_ARGUMENT
                    + "Lio/tapstate/adapters/mongostore/LatestObservationPayloadCodec$ChunkWriter;)"
                    + "Lio/tapstate/adapters/mongostore/LatestObservationPayloadCodec$Encoded;",
            Unit.LATEST_OBSERVATION_BINARY_ENCODER_INVOCATION, false);
    private static final Signature SYNC_SEND = new Signature(CONNECTION, "sendMessage",
            "(Ljava/util/List;ILcom/mongodb/internal/connection/OperationContext;)V",
            Unit.SYNC_COMMAND_SEND, false);
    private static final Signature ASYNC_SEND = new Signature(CONNECTION, "sendMessageAsync",
            "(Ljava/util/List;ILcom/mongodb/internal/connection/OperationContext;"
                    + "Lcom/mongodb/internal/async/SingleResultCallback;)V", null, true);
    private static final List<String> LIBRARIES = List.of(
            "adapter-mongo-store-0.5.0.jar", "spi-store-0.5.0.jar", "core-common-0.5.0.jar", "core-lifecycle-0.5.0.jar",
            "core-model-0.5.0.jar", "core-event-0.5.0.jar", "spi-metrics-0.5.0.jar", "bson-5.8.0.jar",
            "bson-record-codec-5.8.0.jar", "mongodb-driver-core-5.8.0.jar",
            "mongodb-driver-sync-5.8.0.jar", "slf4j-api-2.0.18.jar");
    private static final List<String> TARGET_CLASSES = List.of(TARGET, LATEST_TARGET,
            LATEST_TARGET + "$Chunks", LATEST_TARGET + "$ChunkFixture");
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private BenchmarkJdiCostObserver() {
    }

    static final class Artifact implements AutoCloseable {
        final Arm arm;
        final ArtifactSet artifactSet;
        final String artifactSha256;
        boolean reactorBuild;
        final Path bootJar;
        final Path directory;
        final List<Path> libraries;
        final Path targets;
        final Map<String, ClassImage> images = new ConcurrentHashMap<>();

        private Artifact(Arm arm, ArtifactSet artifactSet, String artifactSha256,
                Path bootJar, Path directory, List<Path> libraries, Path targets) {
            this.arm = arm;
            this.artifactSet = artifactSet;
            this.artifactSha256 = artifactSha256;
            this.bootJar = bootJar;
            this.directory = directory;
            this.libraries = List.copyOf(libraries);
            this.targets = targets;
        }

        static Artifact open(Path input, Arm arm) throws Exception {
            return open(input, arm, ArtifactSet.LEGACY);
        }

        static Artifact open(Path input, Arm arm, ArtifactSet set) throws Exception {
            if (!Files.isRegularFile(input) || !sha256(Files.newInputStream(input)).equals(set.sha256(arm))) {
                throw invalid("immutable artifact hash did not match its selected arm");
            }
            return extract(input, arm, set, set.sha256(arm));
        }

        /** A separate build-local gate: selected module bytes must be exactly this reactor's outputs. */
        static Artifact openReactor(Path input, Path root) throws Exception {
            Path actual = input.toRealPath();
            if (!actual.getParent().equals(root.resolve("app/target").toRealPath())
                    || !actual.getFileName().toString().equals("app-0.6.0-boot.jar")) {
                throw invalid("reactor encoder gate requires the current app target artifact");
            }
            String hash = sha256(Files.newInputStream(actual));
            Artifact artifact = extract(actual, Arm.OBSERVABILITY, ArtifactSet.COMMON_SOURCE, hash);
            try {
                for (Path library : artifact.libraries) {
                    String name = library.getFileName().toString();
                    if (!name.endsWith("-0.6.0.jar")) { continue; }
                    String module = name.substring(0, name.length() - "-0.6.0.jar".length());
                    String directory = module.startsWith("adapter-") ? "adapters/" + module
                            : module.startsWith("spi-") ? "spi/" + module : "core/" + module;
                    Path built = root.resolve(directory).resolve("target").resolve(name);
                    if (!Files.isRegularFile(built) || !sha256(Files.newInputStream(built))
                            .equals(sha256(Files.newInputStream(library)))) {
                        throw invalid("reactor boot library differs from its current module output");
                    }
                }
                artifact.reactorBuild = true;
                return artifact;
            } catch (Throwable failure) { artifact.close(); throw failure; }
        }

        private static Artifact extract(Path input, Arm arm, ArtifactSet set, String expectedHash) throws Exception {
            Path directory = Files.createTempDirectory("benchmark-jdi-cost-");
            Artifact result = new Artifact(arm, set, expectedHash, input.toRealPath(), directory, LIBRARIES.stream()
                    .map(name -> name.replace("-0.5.0.jar", "-" + set.moduleVersion + ".jar"))
                    .map(directory::resolve).toList(), directory.resolve("targets"));
            try {
                try (ZipFile boot = new ZipFile(input.toFile())) {
                    for (Path output : result.libraries) {
                        ZipEntry entry = boot.getEntry("BOOT-INF/lib/" + output.getFileName());
                        if (entry == null || entry.getSize() < 0 || entry.getSize() > 32 * 1024 * 1024L) {
                            throw invalid("a pinned library was missing or exceeded the extraction budget");
                        }
                        try (InputStream stream = boot.getInputStream(entry)) {
                            Files.copy(stream, output);
                        }
                    }
                }
                if (!sha256(Files.newInputStream(input)).equals(expectedHash)) {
                    throw invalid("immutable artifact changed during extraction");
                }
                for (String target : TARGET_CLASSES) {
                    String resource = target.replace('.', '/') + ".class";
                    Path output = result.targets.resolve(resource);
                    Files.createDirectories(output.getParent());
                    try (InputStream stream = BenchmarkJdiCostObserver.class.getClassLoader()
                            .getResourceAsStream(resource)) {
                        if (stream == null) {
                            throw invalid("a compiled proof target was missing");
                        }
                        Files.copy(stream, output);
                    }
                }
                String driverHash = sha256(Files.newInputStream(directory.resolve("mongodb-driver-core-5.8.0.jar")));
                String bsonHash = sha256(Files.newInputStream(directory.resolve("bson-5.8.0.jar")));
                if (!driverHash.equals("d25b85c134560a5e30d82a385106a32cdbecdc4766f5c1c831a97c6a0a36f2e2")
                        || !bsonHash.equals("056c512ba9900d5f7d1ac06b9947175d8728e1de619588d00b3a115fc96f4b81")) {
                    throw invalid("the pinned driver or BSON library changed");
                }
                for (Signature signature : ENCODERS) {
                    result.code(signature);
                }
                result.code(SYNC_SEND);
                result.code(ASYNC_SEND);
                boolean latest = result.image(LATEST) != null;
                if (latest != (arm == Arm.OBSERVABILITY)) {
                    throw invalid("the selected arm's latest encoder capability changed");
                }
                if (latest) {
                    result.code(LATEST_ENCODER);
                }
                return result;
            } catch (Throwable failure) {
                result.close();
                throw failure;
            }
        }

        ClassImage image(String type) throws Exception {
            ClassImage cached = images.get(type);
            if (cached != null) {
                return cached;
            }
            String resource = type.replace('.', '/') + ".class";
            ClassImage found = null;
            try (ZipFile boot = new ZipFile(bootJar.toFile())) {
                ZipEntry entry = boot.getEntry("BOOT-INF/classes/" + resource);
                if (entry != null) {
                    if (entry.getSize() < 0 || entry.getSize() > 1_048_576) {
                        throw invalid("an oversized whitelisted boot class was found");
                    }
                    try (InputStream stream = boot.getInputStream(entry)) {
                        found = new ClassImage(bootJar, parseMethods(stream.readAllBytes()));
                    }
                }
            }
            for (Path library : libraries) {
                try (ZipFile jar = new ZipFile(library.toFile())) {
                    ZipEntry entry = jar.getEntry(resource);
                    if (entry != null) {
                        if (found != null || entry.getSize() < 0 || entry.getSize() > 1_048_576) {
                            throw invalid("duplicate or oversized whitelisted class in artifact libraries");
                        }
                        try (InputStream stream = jar.getInputStream(entry)) {
                            found = new ClassImage(library.toRealPath(), parseMethods(stream.readAllBytes()));
                        }
                    }
                }
            }
            if (found != null) {
                images.put(type, found);
            }
            return found;
        }

        byte[] code(Signature signature) throws Exception {
            ClassImage image = image(signature.type());
            byte[] code = image == null ? null : image.methods().get(signature.method() + signature.descriptor());
            if (code == null || code.length == 0 || returnOffsets(code).isEmpty()) {
                throw invalid("a whitelisted exact signature or normal return was missing");
            }
            return code;
        }

        boolean latestAvailable() throws Exception {
            return image(LATEST) != null;
        }

        byte[] methodCode(String type, String method, String descriptor) throws Exception {
            ClassImage image = image(type);
            return image == null ? null : image.methods().get(method + descriptor);
        }

        String classpath() {
            List<String> parts = new ArrayList<>();
            parts.add(targets.toString());
            libraries.forEach(path -> parts.add(path.toString()));
            return String.join(java.io.File.pathSeparator, parts);
        }

        @Override
        public void close() throws IOException {
            try (var paths = Files.walk(directory)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(path);
                }
            }
        }
    }

    static Summary run(Artifact artifact, String mode, Options options, String mongoUri) throws Exception {
        boolean latest = mode.equals("latest") || mode.equals("latest-chunk");
        boolean wire = mode.startsWith("wire");
        boolean store = mode.startsWith("store-raw");
        if (latest && !artifact.latestAvailable()) {
            throw invalid("latest binary encoder is unavailable in the selected arm");
        }
        List<Signature> signatures = latest ? List.of(LATEST_ENCODER)
                : wire ? List.of(SYNC_SEND, ASYNC_SEND)
                : store ? List.of(ENCODERS.get(1), ENCODERS.get(2)) : ENCODERS;
        ListeningConnector connector = Bootstrap.virtualMachineManager().listeningConnectors().stream()
                .filter(item -> item.name().equals("com.sun.jdi.SocketListen"))
                .findFirst().orElseThrow(() -> invalid("the loopback JDI listening connector is unavailable"));
        Map<String, Connector.Argument> arguments = connector.defaultArguments();
        arguments.get("localAddress").setValue("127.0.0.1");
        arguments.get("port").setValue("0");
        arguments.get("timeout").setValue("10000");
        String reportedAddress = connector.startListening(arguments);
        String address;
        try {
            address = numericLoopbackDialAddress(arguments.get("localAddress").value(),
                    reportedAddress, arguments.get("port").value());
        } catch (AssertionError failure) {
            connector.stopListening(arguments);
            throw failure;
        }
        Process child = null;
        VirtualMachine vm = null;
        Pump pump = null;
        Protocol protocol = null;
        try {
            Path java = Path.of(System.getProperty("java.home"), "bin", "java");
            ProcessBuilder builder = new ProcessBuilder(java.toString(),
                    "-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=" + address,
                    "-cp", artifact.classpath(), latest ? LATEST_TARGET : TARGET, mode);
            for (String variable : List.of("CLASSPATH", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS")) {
                builder.environment().remove(variable);
            }
            builder.environment().remove("TAPSTATE_JDI_WITNESS_MONGO_URI");
            if ((wire || store) && mongoUri != null) {
                builder.environment().put("TAPSTATE_JDI_WITNESS_MONGO_URI", mongoUri);
            }
            child = builder.start();
            protocol = new Protocol(child);
            vm = connector.accept(arguments);
            connector.stopListening(arguments);
            if (!vm.canGetBytecodes()) {
                throw invalid("the target VM cannot expose exact method bytecodes");
            }
            pump = new Pump(vm, artifact, signatures, options, store);
            pump.start();
            protocol.await("READY", pump, artifact, latest, wire);
            pump.begin();
            protocol.send("START");
            protocol.await("DONE", pump, artifact, latest, wire);
            Summary summary = pump.finish();
            protocol.send("STOP");
            if (!child.waitFor(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS) || child.exitValue() != 0) {
                throw invalid("the target did not complete its stop barrier successfully");
            }
            protocol.verifyExitedAndDrained();
            return summary;
        } finally {
            try {
                connector.stopListening(arguments);
            } catch (Exception ignored) {
                // The owned listener may already have stopped after accept.
            }
            if (pump != null) {
                pump.close();
            } else if (vm != null) {
                try {
                    vm.dispose();
                } catch (Exception ignored) {
                    // A failed target can already be disconnected.
                }
            }
            if (child != null && child.isAlive()) {
                child.destroyForcibly();
                child.waitFor(5, TimeUnit.SECONDS);
            }
            if (protocol != null) {
                protocol.close();
            }
        }
    }

    static String numericLoopbackDialAddress(String configuredBind, String reported, String boundPort) {
        String metadata = " [reported=" + safeListenerMetadata(reported)
                + ", boundPort=" + safeListenerMetadata(boundPort) + "]";
        if (!"127.0.0.1".equals(configuredBind) || reported == null || reported.length() > 128) {
            throw invalid("JDI did not use the required numeric loopback bind" + metadata);
        }
        int separator = reported.lastIndexOf(':');
        if (separator <= 0) {
            throw invalid("JDI returned an invalid listener address" + metadata);
        }
        String host = reported.substring(0, separator);
        String portText = reported.substring(separator + 1);
        if (!portText.matches("[0-9]{1,5}") || !portText.equals(boundPort)
                || Integer.parseInt(portText) == 0 || Integer.parseInt(portText) > 65_535) {
            throw invalid("JDI returned an invalid or mismatched bound port" + metadata);
        }
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        } else if (host.indexOf(':') >= 0) {
            throw invalid("JDI returned an unbracketed listener address" + metadata);
        }
        if (!host.matches("[A-Za-z0-9_.:-]+")) {
            throw invalid("JDI returned an invalid listener host" + metadata);
        }
        try {
            InetAddress[] aliases = InetAddress.getAllByName(host);
            if (aliases.length == 0 || Arrays.stream(aliases).anyMatch(alias -> !alias.isLoopbackAddress())) {
                throw invalid("JDI returned a non-loopback listener alias" + metadata);
            }
        } catch (UnknownHostException failure) {
            throw invalid("JDI returned an unresolved listener alias" + metadata);
        }
        // SocketListen may reverse-resolve its numeric bind for display. The child always dials
        // the configured IPv4 loopback, regardless of that display alias or its address family.
        return "127.0.0.1:" + portText;
    }

    private static String safeListenerMetadata(String value) {
        return value == null ? "<null>" : value.length() <= 128 && value.matches("[A-Za-z0-9_.:\\[\\]-]+")
                ? value : "<invalid>";
    }

    static TargetFailure targetFailure(String protocol) {
        String[] fields = protocol.split("\t", -1);
        if (fields.length != 4 || !fields[0].equals("TARGET_FAILED") || !binaryClassSymbol(fields[2])) {
            throw invalid("the target failure protocol had an invalid shape");
        }
        TargetPhase phase;
        try {
            phase = TargetPhase.valueOf(fields[1]);
        } catch (IllegalArgumentException failure) {
            throw invalid("the target failure protocol used an unknown phase");
        }
        String linkage = fields[3].equals("-") ? null : fields[3];
        if (linkage != null && (!binaryClassSymbol(linkage) || !Set.of("java.lang.LinkageError",
                "java.lang.NoClassDefFoundError", "java.lang.NoSuchMethodError", "java.lang.NoSuchFieldError",
                "java.lang.AbstractMethodError", "java.lang.IncompatibleClassChangeError",
                "java.lang.UnsatisfiedLinkError", "java.lang.IllegalAccessError", "java.lang.ClassFormatError",
                "java.lang.ExceptionInInitializerError", "java.lang.BootstrapMethodError",
                "java.lang.UnsupportedClassVersionError", "java.lang.VerifyError").contains(fields[2]))) {
            throw invalid("the target failure protocol included unsupported linkage metadata");
        }
        return new TargetFailure(new TargetDiagnostic(phase, fields[2], linkage));
    }

    private static boolean binaryClassSymbol(String value) {
        return value.length() <= 240
                && value.matches("[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)+");
    }

    private record Signature(String type, String method, String descriptor, Unit unit, boolean rejectAsync) {
    }

    record ClassImage(Path origin, Map<String, byte[]> methods) {
    }

    private record Site(Signature signature, boolean entry) {
    }

    private record Pending(Signature signature, int frameCount, long connectionId, int requestId, WireKey wire) {
    }

    private static final class MutableCount {
        long entries;
        long returns;

        Count snapshot() {
            return new Count(entries, returns);
        }
    }

    private static final class Pump implements AutoCloseable {
        private final VirtualMachine vm;
        private final Artifact artifact;
        private final List<Signature> signatures;
        private final boolean productionStore;
        private final Set<String> verifiedWriterTypes = new HashSet<>();
        private final Options options;
        private final Object state = new Object();
        private final EnumMap<Unit, MutableCount> counts = new EnumMap<>(Unit.class);
        private final Map<WireKey, MutableCount> wireCounts = new HashMap<>();
        private final Map<Long, ArrayDeque<Pending>> pending = new HashMap<>();
        private final Map<String, Long> prepared = new HashMap<>();
        private final List<EventRequest> requests = new ArrayList<>();
        private final AtomicReference<AssertionError> failure = new AtomicReference<>();
        private final Thread thread;
        private volatile CompletableFuture<Summary> finish;
        private volatile CompletableFuture<Void> terminalFailureRequested;
        private volatile boolean running = true;
        private boolean window;
        private boolean closed;
        private boolean dropped;
        private long loaderId = -1;
        private long events;
        private long handlingNanos;
        private int breakpointRequests;

        Pump(VirtualMachine vm, Artifact artifact, List<Signature> signatures, Options options, boolean productionStore) {
            this.vm = vm;
            this.artifact = artifact;
            this.signatures = signatures;
            this.productionStore = productionStore;
            this.options = options;
            signatures.stream().filter(item -> item.unit() != null)
                    .forEach(item -> counts.put(item.unit(), new MutableCount()));
            for (String type : signatures.stream().map(Signature::type).distinct().toList()) {
                var request = vm.eventRequestManager().createClassPrepareRequest();
                request.addClassFilter(type);
                request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
                request.enable();
                requests.add(request);
            }
            thread = new Thread(this::loop, "benchmark-jdi-cost-events");
            thread.setDaemon(true);
        }

        void start() {
            thread.start();
        }

        void begin() {
            check();
            synchronized (state) {
                if (window || closed || prepared.size() != signatures.stream().map(Signature::type).distinct().count()) {
                    List<String> missing = signatures.stream().map(Signature::type).distinct()
                            .filter(type -> !prepared.containsKey(type)).sorted().toList();
                    throw invalid("the complete exact breakpoint roster was not prepared before start"
                            + " [missing=" + String.join(",", missing) + "]");
                }
                window = true;
            }
        }

        void check() {
            AssertionError failed = failure.get();
            if (failed != null) {
                throw failed;
            }
        }

        Summary finish() throws Exception {
            check();
            vm.suspend();
            // A synchronous protocol DONE follows all resumed workload breakpoints. This command
            // also completes after VM suspension; the pump drains queued sets before closing scope.
            vm.allThreads();
            if (options.failBeforeFinishPublication()) {
                // The controlled witness acknowledges the real pump's terminal failure before
                // publication. A short future deadline makes the defective handoff bounded.
                CompletableFuture<Void> acknowledged = new CompletableFuture<>();
                terminalFailureRequested = acknowledged;
                acknowledged.get(1, TimeUnit.SECONDS);
            }
            CompletableFuture<Summary> command = new CompletableFuture<>();
            finish = command;
            // If the pump failed before publication it could not complete this future. A failure
            // after publication sees the volatile future and completes it in the terminal catch.
            check();
            Summary summary;
            try {
                summary = command.get(options.failBeforeFinishPublication() ? 250 : TIMEOUT.toMillis(),
                        TimeUnit.MILLISECONDS);
            } catch (ExecutionException problem) {
                if (problem.getCause() instanceof AssertionError invalid) {
                    throw invalid;
                }
                throw invalid("the drain barrier failed");
            }
            check();
            vm.resume();
            return summary;
        }

        private void loop() {
            try {
                while (running) {
                    EventSet set = vm.eventQueue().remove(50);
                    if (set != null) {
                        handle(set);
                    }
                    if (terminalFailureRequested != null) {
                        throw invalid("JDI collection failed before a complete drained window");
                    }
                    CompletableFuture<Summary> command = finish;
                    if (command != null) {
                        EventSet queued;
                        while ((queued = vm.eventQueue().remove(1)) != null) {
                            handle(queued);
                        }
                        synchronized (state) {
                            check();
                            if (!window || !pending.isEmpty()) {
                                throw invalid("the measured window retained an unmatched entry or failed send");
                            }
                            requests.forEach(EventRequest::disable);
                            window = false;
                            closed = true;
                            command.complete(summary());
                            finish = null;
                        }
                    }
                }
            } catch (Throwable problem) {
                AssertionError safe = problem instanceof AssertionError assertion ? assertion
                        : invalid("JDI collection failed before a complete drained window");
                failure.compareAndSet(null, safe);
                CompletableFuture<Summary> command = finish;
                if (command != null) {
                    command.completeExceptionally(safe);
                }
                CompletableFuture<Void> acknowledged = terminalFailureRequested;
                if (acknowledged != null) {
                    acknowledged.complete(null);
                }
            }
        }

        private void handle(EventSet set) throws Exception {
            long started = System.nanoTime();
            try {
                synchronized (state) {
                    for (Event event : set) {
                        if (event instanceof ClassPrepareEvent preparedEvent) {
                            prepare(preparedEvent.referenceType());
                        } else if (event instanceof BreakpointEvent breakpoint) {
                            if (window) {
                                breakpoint(breakpoint);
                            }
                        } else if (event instanceof VMDeathEvent || event instanceof VMDisconnectEvent) {
                            if (!closed) {
                                throw invalid("the target disconnected before the measured window drained");
                            }
                            running = false;
                        } else if (!(event instanceof VMStartEvent)) {
                            throw invalid("an event outside the exact request roster was received");
                        }
                    }
                    if (window) {
                        handlingNanos += System.nanoTime() - started;
                    }
                }
            } finally {
                set.resume();
            }
        }

        private void prepare(ReferenceType type) throws Exception {
            if (type.classLoader() == null) {
                throw invalid("a whitelisted artifact class used an unexpected bootstrap loader");
            }
            long actualLoader = type.classLoader().uniqueID();
            if (prepared.putIfAbsent(type.name(), actualLoader) != null
                    || (loaderId != -1 && loaderId != actualLoader)) {
                throw invalid("a whitelisted class appeared under a duplicate or unexpected loader");
            }
            loaderId = actualLoader;
            for (Signature signature : signatures) {
                if (!signature.type().equals(type.name())) {
                    continue;
                }
                List<Method> methods = type.methodsByName(signature.method(), signature.descriptor());
                if (methods.size() != 1) {
                    throw invalid("the prepared exact method signature was missing or ambiguous");
                }
                Method method = methods.getFirst();
                byte[] expected = artifact.code(signature);
                if (method.isNative() || method.isAbstract() || method.isBridge() || method.isObsolete()
                        || !Arrays.equals(method.bytecodes(), expected)
                        || method.locationOfCodeIndex(0) == null
                        || method.locationOfCodeIndex(0).codeIndex() != 0) {
                    throw invalid("the exact first instruction or artifact method bytecodes did not match");
                }
                request(method, 0, new Site(signature, true));
                if (!signature.rejectAsync()) {
                    for (int offset : returnOffsets(expected)) {
                        request(method, offset, new Site(signature, false));
                    }
                }
            }
        }

        private void request(Method method, long offset, Site site) {
            var location = method.locationOfCodeIndex(offset);
            if (location == null || location.codeIndex() != offset) {
                throw invalid("a verified return instruction could not be addressed exactly");
            }
            BreakpointRequest request = vm.eventRequestManager().createBreakpointRequest(location);
            request.putProperty("cost-site", site);
            request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
            request.enable();
            requests.add(request);
            breakpointRequests++;
        }

        private void breakpoint(BreakpointEvent event) throws Exception {
            if (++events > options.maximumEvents()) {
                throw invalid("the bounded JDI event budget was exceeded");
            }
            Object property = event.request().getProperty("cost-site");
            if (!(property instanceof Site site)) {
                throw invalid("a breakpoint lacked its closed cost-unit mapping");
            }
            Signature signature = site.signature();
            if (signature.rejectAsync()) {
                throw invalid("asynchronous write completion is unsupported by this witness");
            }
            long threadId = event.thread().uniqueID();
            int frameCount = event.thread().frameCount();
            if (site.entry()) {
                if (options.dropFirstEntry() && !dropped) {
                    dropped = true;
                    return;
                }
                if (pending.size() >= 128 && !pending.containsKey(threadId)) {
                    throw invalid("the bounded JDI in-flight thread budget was exceeded");
                }
                ArrayDeque<Pending> stack = pending.computeIfAbsent(threadId, ignored -> new ArrayDeque<>());
                if (stack.size() >= 64) {
                    throw invalid("the bounded JDI in-flight depth was exceeded");
                }
                StackFrame frame = event.thread().frame(0);
                List<Value> arguments = frame.getArgumentValues();
                WireKey wire = null;
                long connectionId = -1;
                int requestId = -1;
                if (signature.unit() == Unit.SYNC_COMMAND_SEND) {
                    if (arguments.size() != 3 || !(arguments.get(1) instanceof IntegerValue id)
                            || frame.thisObject() == null) {
                        throw invalid("the exact synchronous send arguments were unavailable");
                    }
                    requestId = id.value();
                    connectionId = frame.thisObject().uniqueID();
                    wire = wireKey(event, requestId, connectionId,
                            Set.of("jdi_cost_witness", "admin", "local", "config"));
                    wireCounts.computeIfAbsent(wire, ignored -> new MutableCount()).entries++;
                } else {
                    encoderArguments(signature.unit(), arguments);
                }
                stack.push(new Pending(signature, frameCount, connectionId, requestId, wire));
                counts.get(signature.unit()).entries++;
            } else {
                ArrayDeque<Pending> stack = pending.get(threadId);
                Pending entry = stack == null || stack.isEmpty() ? null : stack.peek();
                if (entry == null || !entry.signature().equals(signature) || entry.frameCount() != frameCount) {
                    throw invalid("a normal return had no matching exact entry event");
                }
                if (signature.unit() == Unit.SYNC_COMMAND_SEND) {
                    StackFrame frame = event.thread().frame(0);
                    List<Value> arguments = frame.getArgumentValues();
                    if (frame.thisObject() == null || frame.thisObject().uniqueID() != entry.connectionId()
                            || !(arguments.get(1) instanceof IntegerValue id) || id.value() != entry.requestId()) {
                        throw invalid("the completed synchronous send lost its request correlation");
                    }
                    wireCounts.get(entry.wire()).returns++;
                }
                stack.pop();
                if (stack.isEmpty()) {
                    pending.remove(threadId);
                }
                counts.get(signature.unit()).returns++;
            }
        }

        private void encoderArguments(Unit unit, List<Value> arguments) throws Exception {
            String expected = unit == Unit.RATE_DOCUMENT_BUILD ? "io.tapstate.core.lifecycle.RateSample"
                    : unit == Unit.BSON_BINARY_ENCODER_INVOCATION ? "org.bson.BsonBinaryWriter"
                    : "io.tapstate.core.lifecycle.Observation";
            int size = unit == Unit.BSON_BINARY_ENCODER_INVOCATION ? 3
                    : unit == Unit.LATEST_OBSERVATION_BINARY_ENCODER_INVOCATION ? 2 : 1;
            // A null writer is allowed through the entry so the missing normal return is observable.
            if (productionStore && unit == Unit.BSON_BINARY_ENCODER_INVOCATION && arguments.size() == 3
                    && arguments.getFirst() instanceof ObjectReference writer) {
                requireBinaryWriter(writer);
                return;
            }
            if (arguments.size() != size || (arguments.getFirst() == null
                    && unit != Unit.BSON_BINARY_ENCODER_INVOCATION)
                    || (arguments.getFirst() != null && (!(arguments.getFirst() instanceof ObjectReference object)
                    || !object.referenceType().name().equals(expected)))) {
                throw invalid("the typed encoder arguments did not match the closed cost unit");
            }
        }

        private void requireBinaryWriter(ObjectReference writer) throws Exception {
            String appending = "com.mongodb.internal.connection.BsonWriterHelper$AppendingBsonWriter";
            Set<String> wrappers = Set.of(appending, "com.mongodb.internal.connection.FieldTrackingBsonWriter",
                    "com.mongodb.internal.connection.IdHoldingBsonWriter", "com.mongodb.internal.connection.SplittablePayloadBsonWriter");
            for (int depth = 0; depth < 8; depth++) {
                ReferenceType type = writer.referenceType();
                verifyWriter(type);
                if (type.name().equals("org.bson.BsonBinaryWriter")) { return; }
                if (type.name().equals(appending + "$InternalAppendingBsonBinaryWriter")
                        && type instanceof ClassType concrete && concrete.superclass().name().equals("org.bson.BsonBinaryWriter")) {
                    verifyWriter(concrete.superclass()); return;
                }
                if (!wrappers.contains(type.name())) { throw invalid("raw publication used an unmapped binary writer"); }
                Field delegate = type.fieldByName("bsonWriter");
                if (delegate == null || delegate.isStatic() || !delegate.isFinal()
                        || !delegate.signature().equals("Lorg/bson/BsonWriter;")
                        || !delegate.declaringType().name().equals("com.mongodb.internal.connection.BsonWriterDecorator")) {
                    throw invalid("raw publication writer delegate differs from the pinned driver");
                }
                verifyWriter(delegate.declaringType()); writer = object(writer.getValue(delegate));
            }
            throw invalid("raw publication binary writer exceeded its bounded decorator depth");
        }

        private void verifyWriter(ReferenceType type) throws Exception {
            if (type.classLoader() == null || type.classLoader().uniqueID() != loaderId) {
                throw invalid("raw publication writer escaped its pinned application loader");
            }
            if (verifiedWriterTypes.contains(type.name())) { return; }
            ClassImage image = artifact.image(type.name());
            if (image == null) { throw invalid("raw publication writer is absent from the selected artifact"); }
            for (var entry : image.methods().entrySet()) {
                int split = entry.getKey().indexOf('(');
                List<Method> methods = type.methodsByName(entry.getKey().substring(0, split), entry.getKey().substring(split));
                if (methods.size() != 1 || !Arrays.equals(entry.getValue(), methods.getFirst().bytecodes())) {
                    throw invalid("raw publication writer bytecodes differ from the selected artifact");
                }
            }
            verifiedWriterTypes.add(type.name());
        }

        private static WireKey wireKey(BreakpointEvent event, int requestId, long connectionId,
                Set<String> databases) throws Exception {
            ObjectReference message = null;
            int count = Math.min(event.thread().frameCount() - 1, 63);
            if (count <= 0) { throw invalid("a synchronous send lacked its exact command ancestor"); }
            for (StackFrame ancestor : event.thread().frames(1, count)) {
                Method method = ancestor.location().method();
                if (method.declaringType().name().equals(CONNECTION) && method.name().equals("trySendMessage")
                        && method.signature().equals(TRY_SEND_SIGNATURE)) {
                    List<Value> arguments = ancestor.getArgumentValues();
                    if (message != null || arguments.size() != 3
                            || !(arguments.getFirst() instanceof ObjectReference candidate)
                            || !candidate.referenceType().name().equals(COMMAND)
                            || ancestor.thisObject() == null || ancestor.thisObject().uniqueID() != connectionId
                            || !(field(candidate, "id") instanceof IntegerValue id) || id.value() != requestId) {
                        throw invalid("the wire command ancestor did not match the send request");
                    }
                    message = candidate;
                }
            }
            if (message == null) {
                throw invalid("a synchronous send lacked its exact command ancestor");
            }
            return messageKey(message, databases);
        }

        private static WireKey messageKey(ObjectReference message, Set<String> databases) {
            if (!message.referenceType().name().equals(COMMAND)) {
                throw invalid("a command header used an unmapped exact type");
            }
            String database = string(field(message, "database"));
            if (!databases.contains(database)) {
                throw invalid("a wire command used an unmapped database");
            }
            ObjectReference document = object(field(message, "command"));
            if (document.referenceType().name().equals("org.bson.BsonDocumentWrapper")) {
                document = object(field(document, "unwrapped"));
            }
            if (!document.referenceType().name().equals("org.bson.BsonDocument")) {
                throw invalid("an unsupported lazy wire command shape was observed");
            }
            ObjectReference map = object(field(document, "map"));
            if (!map.referenceType().name().equals("java.util.LinkedHashMap")) {
                throw invalid("the wire command map implementation was unsupported");
            }
            ObjectReference first = object(field(map, "head"));
            String name = string(field(first, "key"));
            if (Set.of("hello", "isMaster", "ismaster", "ping", "endSessions", "killCursors",
                    "commitTransaction", "abortTransaction").contains(name)) {
                return new WireKey(Namespace.CONTROL, Operation.CONTROL);
            }
            Operation operation = switch (name) {
                case "insert" -> Operation.INSERT;
                case "update" -> Operation.UPDATE;
                case "delete" -> Operation.DELETE;
                case "find" -> Operation.FIND;
                case "aggregate" -> Operation.AGGREGATE;
                case "getMore" -> Operation.GET_MORE;
                case "count" -> Operation.COUNT;
                case "distinct" -> Operation.DISTINCT;
                case "create" -> Operation.CREATE;
                case "drop" -> Operation.DROP;
                case "findAndModify" -> Operation.UPDATE;
                default -> throw invalid("a wire command used an unmapped operation");
            };
            Value collectionValue = field(first, "value");
            if (operation == Operation.GET_MORE) {
                collectionValue = null;
                ObjectReference entry = first;
                for (int i = 0; entry != null && i < 64; i++) {
                    if (string(field(entry, "key")).equals("collection")) {
                        collectionValue = field(entry, "value");
                        break;
                    }
                    Value next = field(entry, "after");
                    entry = next == null ? null : object(next);
                }
            }
            ObjectReference collection = object(collectionValue);
            if (!collection.referenceType().name().equals("org.bson.BsonString")) {
                throw invalid("a wire collection field lacked its exact string type");
            }
            String collectionName = string(field(collection, "value"));
            Namespace namespace = switch (collectionName) {
                case "pipeline_observation" -> Namespace.OBSERVATION;
                case "pipeline_observation_chunks" -> Namespace.OBSERVATION_CHUNKS;
                case "pipeline_rate_history" -> Namespace.RAW_HISTORY;
                case "pipeline_history_rollups" -> Namespace.HISTORY_ROLLUPS;
                case "pipeline_events" -> Namespace.EVENTS;
                case "artifacts" -> Namespace.ARTIFACTS;
                case "pipeline_desired" -> Namespace.DESIRED;
                case "pipeline_state" -> Namespace.PIPELINE_STATE;
                case "workload_claims" -> Namespace.WORKLOAD_CLAIMS;
                default -> throw invalid("a wire command used an unmapped namespace [collection="
                        + (collectionName.length() <= 128 && collectionName.matches("[A-Za-z_][A-Za-z0-9_.-]*")
                                ? collectionName : "UNAVAILABLE") + ", operation=" + operation + "]");
            };
            return new WireKey(namespace, operation);
        }

        private Summary summary() {
            EnumMap<Unit, Count> snapshot = new EnumMap<>(Unit.class);
            counts.forEach((unit, count) -> snapshot.put(unit, count.snapshot()));
            Map<WireKey, Count> wires = new HashMap<>();
            wireCounts.forEach((key, count) -> wires.put(key, count.snapshot()));
            EnumSet<Unavailable> unavailable = EnumSet.of(Unavailable.ASYNC_WRITE_COMPLETION,
                    Unavailable.WIRE_DOCUMENT_COUNT, Unavailable.WIRE_BYTE_COUNT, Unavailable.BACKGROUND_BATCH_IO);
            if (artifact.arm == Arm.REFERENCE) {
                unavailable.add(Unavailable.LATEST_OBSERVATION_BINARY_ENCODER);
            }
            return new Summary(artifact.arm, artifact.artifactSha256, snapshot, wires, unavailable,
                    events, handlingNanos, breakpointRequests,
                    vm.eventRequestManager().methodEntryRequests().size(), closed);
        }

        @Override
        public void close() throws InterruptedException {
            running = false;
            try {
                vm.dispose();
            } catch (Exception ignored) {
                // A rejected witness can have lost its connection already.
            }
            thread.interrupt();
            thread.join(2_000);
        }
    }

    private static final class Protocol implements AutoCloseable {
        private final Process child;
        private final ArrayBlockingQueue<String> lines = new ArrayBlockingQueue<>(64);
        private final AtomicReference<AssertionError> failure = new AtomicReference<>();
        private final Thread output;
        private final Thread errors;
        private final OutputStreamWriter input;
        private final Set<String> origins = new HashSet<>();

        Protocol(Process child) {
            this.child = child;
            input = new OutputStreamWriter(child.getOutputStream(), StandardCharsets.UTF_8);
            output = new Thread(() -> read(child.getInputStream(), true), "benchmark-jdi-cost-protocol");
            errors = new Thread(() -> read(child.getErrorStream(), false), "benchmark-jdi-cost-errors");
            output.setDaemon(true);
            errors.setDaemon(true);
            output.start();
            errors.start();
        }

        private void read(InputStream stream, boolean protocol) {
            try (InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
                char[] buffer = new char[256];
                StringBuilder line = protocol ? new StringBuilder(4_096) : null;
                int count = 0;
                int length = 0;
                boolean previousCr = false;
                boolean discard = false;
                int read;
                while ((read = reader.read(buffer)) != -1) {
                    for (int i = 0; i < read; i++) {
                        if (discard) {
                            continue;
                        }
                        char character = buffer[i];
                        if (character == '\n' && previousCr) {
                            previousCr = false;
                            continue;
                        }
                        previousCr = character == '\r';
                        if (character == '\n' || character == '\r') {
                            if (++count > 256 || (protocol && !lines.offer(line.toString()))) {
                                outputBudgetExceeded();
                                discard = true;
                            }
                            length = 0;
                            if (line != null) {
                                line.setLength(0);
                            }
                        } else if (++length > 4_096) {
                            outputBudgetExceeded();
                            discard = true;
                            if (line != null) {
                                line.setLength(0);
                            }
                        } else if (line != null) {
                            line.append(character);
                        }
                    }
                }
                if (!discard && length > 0 && (++count > 256 || (protocol && !lines.offer(line.toString())))) {
                    outputBudgetExceeded();
                }
                // Stderr is never retained. After a failure both streams are drained and discarded
                // with constant storage until the controlling thread rejects and ends its child.
            } catch (IOException ignored) {
                failure.compareAndSet(null, invalid("the target protocol stream closed unexpectedly"));
            }
        }

        private void outputBudgetExceeded() {
            failure.compareAndSet(null, invalid("the bounded target output budget was exceeded"));
        }

        private void check() {
            AssertionError problem = failure.get();
            if (problem != null) {
                throw problem;
            }
        }

        void await(String expected, Pump pump, Artifact artifact, boolean latest, boolean wire) throws Exception {
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                check();
                pump.check();
                String line = lines.poll(50, TimeUnit.MILLISECONDS);
                if (line == null) {
                    if (!child.isAlive()) {
                        throw invalid("the target exited before its protocol barrier");
                    }
                    continue;
                }
                if (line.startsWith("ORIGIN\t")) {
                    String[] fields = line.split("\t", -1);
                    ClassImage image = fields.length == 3 ? artifact.image(fields[1]) : null;
                    if (image == null || !image.origin().equals(Path.of(fields[2]).toRealPath())
                            || !origins.add(fields[1])) {
                        throw invalid("a target class did not come from its pinned artifact code source");
                    }
                } else if (line.startsWith("TARGET_FAILED\t")) {
                    throw targetFailure(line);
                } else if (line.equals(expected)) {
                    Set<String> required = latest
                            ? Set.of(LATEST, "io.tapstate.core.lifecycle.Observation")
                            : wire ? Set.of(OBSERVATION, RATE, BSON, CONNECTION,
                                    "io.tapstate.core.lifecycle.Observation")
                            : Set.of(OBSERVATION, RATE, BSON, "io.tapstate.core.lifecycle.Observation");
                    if (!origins.equals(required)) {
                        throw invalid("the complete pinned code-source roster was not reported");
                    }
                    check();
                    return;
                } else {
                    throw invalid("the target produced an unexpected protocol value");
                }
            }
            throw invalid("the target protocol barrier timed out");
        }

        void send(String command) throws IOException {
            input.write(command + "\n");
            input.flush();
        }

        void verifyExitedAndDrained() throws InterruptedException {
            output.join(2_000);
            errors.join(2_000);
            check();
            if (output.isAlive() || errors.isAlive()) {
                throw invalid("the target protocol readers did not drain after child exit");
            }
            if (!lines.isEmpty()) {
                throw invalid("forbidden post-DONE target output remained queued");
            }
        }

        @Override
        public void close() {
            try {
                input.close();
            } catch (IOException ignored) {
                // Cleanup is not a success check and must preserve the original collection failure.
            }
            try {
                output.join(2_000);
                errors.join(2_000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static WireKey telemetryWireKey(BreakpointEvent event, int requestId, long connectionId,
            Set<String> databases) throws Exception {
        return Pump.wireKey(event, requestId, connectionId, databases);
    }

    static WireKey telemetryCommandKey(ObjectReference message, Set<String> databases) {
        return Pump.messageKey(message, databases);
    }

    static Value field(ObjectReference object, String name) {
        Field field = object.referenceType().fieldByName(name);
        if (field == null || field.isStatic()) {
            throw invalid("a whitelisted wire metadata field was unavailable");
        }
        return object.getValue(field);
    }

    static ObjectReference object(Value value) {
        if (!(value instanceof ObjectReference object)) {
            throw invalid("a whitelisted wire metadata object was unavailable");
        }
        return object;
    }

    static String string(Value value) {
        if (!(value instanceof StringReference string) || string.value().length() > 128) {
            throw invalid("a whitelisted wire metadata string was unavailable or oversized");
        }
        return string.value();
    }

    private static String sha256(InputStream input) throws Exception {
        try (input) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[16_384];
            int read;
            while ((read = input.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
            return java.util.HexFormat.of().formatHex(digest.digest());
        }
    }

    /** Reads Code attributes without rewriting, generating, or executing any bytecode. */
    private static Map<String, byte[]> parseMethods(byte[] bytes) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (input.readInt() != 0xcafebabe) {
                throw invalid("the artifact class file header was invalid");
            }
            input.readUnsignedShort();
            input.readUnsignedShort();
            String[] utf = new String[input.readUnsignedShort()];
            for (int i = 1; i < utf.length; i++) {
                int tag = input.readUnsignedByte();
                switch (tag) {
                    case 1 -> utf[i] = input.readUTF();
                    case 3, 4, 9, 10, 11, 12, 17, 18 -> input.skipNBytes(4);
                    case 5, 6 -> {
                        input.skipNBytes(8);
                        i++;
                    }
                    case 7, 8, 16, 19, 20 -> input.skipNBytes(2);
                    case 15 -> input.skipNBytes(3);
                    default -> throw invalid("an unsupported artifact constant-pool entry was found");
                }
            }
            input.skipNBytes(6);
            input.skipNBytes(input.readUnsignedShort() * 2L);
            int fields = input.readUnsignedShort();
            for (int i = 0; i < fields; i++) {
                input.skipNBytes(6);
                skipAttributes(input);
            }
            Map<String, byte[]> methods = new LinkedHashMap<>();
            int methodCount = input.readUnsignedShort();
            for (int i = 0; i < methodCount; i++) {
                input.readUnsignedShort();
                String key = utf[input.readUnsignedShort()] + utf[input.readUnsignedShort()];
                int attributes = input.readUnsignedShort();
                for (int j = 0; j < attributes; j++) {
                    String name = utf[input.readUnsignedShort()];
                    long size = Integer.toUnsignedLong(input.readInt());
                    if (size > 1_048_576) {
                        throw invalid("an artifact method attribute exceeded its budget");
                    }
                    if (name.equals("Code")) {
                        byte[] attribute = input.readNBytes((int) size);
                        try (DataInputStream code = new DataInputStream(new ByteArrayInputStream(attribute))) {
                            code.skipNBytes(4);
                            int codeSize = code.readInt();
                            if (codeSize <= 0 || codeSize > 65_535) {
                                throw invalid("an artifact method Code attribute was invalid");
                            }
                            byte[] instructions = code.readNBytes(codeSize);
                            if (instructions.length != codeSize || methods.putIfAbsent(key, instructions) != null) {
                                throw invalid("an artifact method Code attribute was truncated or duplicated");
                            }
                        }
                    } else {
                        input.skipNBytes(size);
                    }
                }
            }
            return methods;
        }
    }

    private static void skipAttributes(DataInputStream input) throws IOException {
        int attributes = input.readUnsignedShort();
        for (int i = 0; i < attributes; i++) {
            input.skipNBytes(2);
            input.skipNBytes(Integer.toUnsignedLong(input.readInt()));
        }
    }

    /** Instruction decoding avoids mistaking an operand byte for a RETURN opcode. */
    static List<Integer> returnOffsets(byte[] code) {
        List<Integer> returns = new ArrayList<>();
        for (int pc = 0; pc < code.length;) {
            int opcode = Byte.toUnsignedInt(code[pc]);
            if (opcode >= 0xac && opcode <= 0xb1) {
                returns.add(pc);
            }
            int length = switch (opcode) {
                case 0x10, 0x12, 0x15, 0x16, 0x17, 0x18, 0x19, 0x36, 0x37, 0x38, 0x39,
                        0x3a, 0xa9, 0xbc -> 2;
                case 0x11, 0x13, 0x14, 0x84, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e,
                        0x9f, 0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8,
                        0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8, 0xbb, 0xbd, 0xc0,
                        0xc1, 0xc6, 0xc7 -> 3;
                case 0xb9, 0xba, 0xc8, 0xc9 -> 5;
                case 0xc5 -> 4;
                case 0xc4 -> pc + 1 < code.length && Byte.toUnsignedInt(code[pc + 1]) == 0x84 ? 6 : 4;
                case 0xaa, 0xab -> switchLength(code, pc, opcode);
                default -> 1;
            };
            if (length <= 0 || pc + length > code.length) {
                throw invalid("an artifact instruction was truncated");
            }
            pc += length;
        }
        return List.copyOf(returns);
    }

    private static int switchLength(byte[] code, int pc, int opcode) {
        int aligned = (pc + 4) & ~3;
        if (aligned + (opcode == 0xaa ? 12 : 8) > code.length) {
            throw invalid("an artifact switch instruction was truncated");
        }
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(code);
        long size = opcode == 0xaa
                ? 12L + 4L * (buffer.getInt(aligned + 8) - (long) buffer.getInt(aligned + 4) + 1)
                : 8L + 8L * buffer.getInt(aligned + 4);
        if (size < 8 || size > code.length - aligned) {
            throw invalid("an artifact switch instruction exceeded its method");
        }
        return Math.toIntExact(aligned - pc + size);
    }

    private static AssertionError invalid(String reason) {
        return new AssertionError("Invalid JDI cost witness: " + reason);
    }
}
