package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.TransactionBeginFunction;
import io.tapdata.pdk.apis.functions.connector.target.TransactionCommitFunction;
import io.tapdata.pdk.apis.functions.connector.target.TransactionRollbackFunction;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Optional benchmark evidence about an unchanged, explicitly pinned connector implementation. */
final class PdkMongoWriteScope {
    private static final String MONGO_CONNECTOR = "io.tapdata.mongodb.MongodbConnector";
    private static final String PINNED_JAR =
            "7fbdbf1ef2965053c9c5e6d28c0c21c3aa2818938ab758b965cc278d61e0c7b4";
    private static final int MAX_INSPECTORS = 128;
    private static final int MAX_THREAD_NAMES = 128;
    private static final int MAX_THREAD_NAME_LENGTH = 256;
    private static final Map<PdkConnector, WeakReference<PdkMongoWriteScope>> INSPECTORS = new WeakHashMap<>();
    private static final PdkMongoWriteScope DISABLED = new PdkMongoWriteScope("DISABLED");

    private final String unavailable;
    private final Access access;
    private final Transactions transactions;
    private PdkWriteReturnWorkBudget workBudget;
    private final ThreadOwners threadOwners = new ThreadOwners();

    private PdkMongoWriteScope(String unavailable) {
        this.unavailable = unavailable;
        this.access = null;
        this.transactions = null;
    }

    private PdkMongoWriteScope(Access access, Transactions transactions) {
        this.unavailable = null;
        this.access = access;
        this.transactions = transactions;
    }

    static PdkMongoWriteScope disabled() { return DISABLED; }

    /** Called only by the explicitly enabled probe, after connector initialization and before writes. */
    static PdkMongoWriteScope enabled(PdkConnector connector) {
        return enabled(connector, null);
    }

    static PdkMongoWriteScope enabled(PdkConnector connector, PdkWriteReturnWorkBudget budget) {
        if (budget == null) return enabledObserved(connector, null);
        try (var span = budget.enter(PdkWriteReturnWorkBudget.Activity.SCOPE_INSTALL)) {
            PdkMongoWriteScope result = enabledObserved(connector, budget); span.success(); return result;
        }
    }

    private static PdkMongoWriteScope enabledObserved(PdkConnector connector, PdkWriteReturnWorkBudget budget) {
        if (connector == null) return new PdkMongoWriteScope("CONNECTOR_UNAVAILABLE");
        synchronized (INSPECTORS) {
            WeakReference<PdkMongoWriteScope> existing = INSPECTORS.get(connector);
            PdkMongoWriteScope cached = existing == null ? null : existing.get();
            if (cached != null) {
                if (cached.workBudget != budget && budget != null) budget.gap("SCOPE_LIFETIME_BINDING_CHANGED");
                return cached;
            }
            INSPECTORS.entrySet().removeIf(entry -> {
                if (budget != null) budget.observe(PdkWriteReturnWorkBudget.Activity.SCOPE_CACHE_ENTRY);
                return entry.getValue().get() == null;
            });
            if (INSPECTORS.size() >= MAX_INSPECTORS) return new PdkMongoWriteScope("INSPECTOR_LIMIT");
            PdkMongoWriteScope created;
            try {
                Access access = Access.open(connector.connector(), budget);
                created = new PdkMongoWriteScope(access, Transactions.install(connector.functions(), budget));
            } catch (UnsupportedShape rejected) {
                created = new PdkMongoWriteScope(rejected.reason);
            } catch (ReflectiveOperationException | IOException | RuntimeException | LinkageError unavailable) {
                created = new PdkMongoWriteScope("SCOPE_INSPECTION_UNAVAILABLE");
            }
            created.workBudget = budget;
            INSPECTORS.put(connector, new WeakReference<>(created));
            return created;
        }
    }

    /** The token is tied to this inspector and the actual calling thread, never to a remote JMX reader. */
    static final class Call {
        private final PdkMongoWriteScope owner;
        private final long threadId;
        private final String threadName;
        private final Observation before;
        private final String rejection;

        private Call(PdkMongoWriteScope owner, long threadId, String threadName,
                Observation before, String rejection) {
            this.owner = owner;
            this.threadId = threadId;
            this.threadName = threadName;
            this.before = before;
            this.rejection = rejection;
        }
    }

    Call beforeWrite() {
        if (workBudget == null) return beforeObserved();
        try (var span = workBudget.enter(PdkWriteReturnWorkBudget.Activity.SCOPE_BEFORE)) {
            Call result = beforeObserved(); span.success(); return result;
        }
    }

    private Call beforeObserved() {
        Thread thread = Thread.currentThread();
        String name = thread.getName();
        String rejection = unavailable;
        if (rejection == null) rejection = threadOwners.claim(name, thread.threadId());
        if (rejection == null) rejection = transactions.rejection();
        Observation before = null;
        if (rejection == null) {
            try {
                before = access.read(name, false);
                rejection = before.rejection;
            } catch (ReflectiveOperationException | RuntimeException | LinkageError failed) {
                rejection = "SCOPE_READ_FAILED";
            }
        }
        return new Call(this, thread.threadId(), name, before, rejection);
    }

    /** Bounded ASCII only; no namespace, URI, record, thread name or exception text is published. */
    String afterWrite(Call began) {
        if (workBudget == null) return afterObserved(began);
        try (var span = workBudget.enter(PdkWriteReturnWorkBudget.Activity.SCOPE_AFTER)) {
            String result = afterObserved(began); span.success(); return result;
        }
    }

    private String afterObserved(Call began) {
        if (began == null || began.owner != this) return unknown("CALL_TOKEN_MISMATCH");
        if (began.rejection != null) {
            return "DISABLED".equals(began.rejection)
                    ? receipt("DISABLED", "DISABLED", "UNKNOWN") : unknown(began.rejection);
        }
        Thread thread = Thread.currentThread();
        if (thread.threadId() != began.threadId || !thread.getName().equals(began.threadName)) {
            return unknown("CALL_THREAD_CHANGED");
        }
        String rejected = threadOwners.claim(began.threadName, began.threadId);
        if (rejected == null) rejected = transactions.rejection();
        if (rejected != null) return unknown(rejected);
        try {
            Observation after = access.read(began.threadName, true);
            if (after.rejection != null) return unknown(after.rejection);
            if (!began.before.sameScope(after)) return unknown("RUNTIME_SCOPE_CHANGED");
            // The wrappers record entry before the original call, including a begin/commit pair
            // that could otherwise make the session map look empty at both snapshot boundaries.
            rejected = transactions.rejection();
            if (rejected != null) return unknown(rejected);
            return receipt("ORDINARY_ACKNOWLEDGED", "PINNED_RUNTIME_SCOPE", after.concern);
        } catch (ReflectiveOperationException | RuntimeException | LinkageError failed) {
            return unknown("SCOPE_READ_FAILED");
        }
    }

    static final class ThreadOwners {
        private final Map<String, Long> owners = new HashMap<>();
        private String failure;

        synchronized String claim(String name, long id) {
            if (failure != null) return failure;
            if (name == null || name.length() > MAX_THREAD_NAME_LENGTH) return failure = "THREAD_NAME_LIMIT";
            Long owner = owners.get(name);
            if (owner != null && owner != id) return failure = "THREAD_NAME_REUSED";
            if (owner == null) {
                if (owners.size() >= MAX_THREAD_NAMES) return failure = "THREAD_OWNER_LIMIT";
                owners.put(name, id);
            }
            return null;
        }
    }

    private static String unknown(String reason) { return receipt("UNKNOWN", reason, "UNKNOWN"); }

    private static String receipt(String state, String reason, String concern) {
        return "state=" + state + ";reason=" + reason + ";concern=" + concern;
    }

    /** Nonstandard tag names are hashed, so concern equality is comparable without publishing them. */
    static String concern(Object w, Boolean journal, Integer timeout) {
        String write;
        if (w == null) write = "DEFAULT";
        else if (w instanceof Integer number) {
            if (number < 0) return null;
            write = number.toString();
        }
        else if (w instanceof String tag) {
            if (tag.length() > 4096) return null;
            write = "majority".equals(tag) ? "MAJORITY"
                    : "TAG_SHA256_" + sha256(tag.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } else return null;
        if (timeout != null && timeout < 0) return null;
        return "w:" + write + ",j:" + (journal == null ? "DEFAULT" : journal)
                + ",timeoutMs:" + (timeout == null ? "DEFAULT" : timeout);
    }

    /** Same-behavior wrappers are installed once per enabled connector, and never on the disabled path. */
    static final class Transactions {
        private static final Map<ConnectorFunctions, WeakReference<Transactions>> OBSERVERS = new WeakHashMap<>();
        private final ConnectorFunctions functions;
        private final AtomicLong calls = new AtomicLong();
        private final PdkWriteReturnWorkBudget workBudget;
        private boolean limit;
        private TransactionBeginFunction begin;
        private TransactionCommitFunction commit;
        private TransactionRollbackFunction rollback;

        private Transactions(ConnectorFunctions functions, PdkWriteReturnWorkBudget budget) {
            this.functions = functions; this.workBudget = budget;
        }

        static Transactions install(ConnectorFunctions functions) {
            return install(functions, null);
        }

        static Transactions install(ConnectorFunctions functions, PdkWriteReturnWorkBudget budget) {
            synchronized (OBSERVERS) {
                WeakReference<Transactions> reference = OBSERVERS.get(functions);
                Transactions existing = reference == null ? null : reference.get();
                if (existing != null) {
                    if (existing.workBudget != budget && budget != null) budget.gap("TRANSACTION_LIFETIME_BINDING_CHANGED");
                    return existing;
                }
                OBSERVERS.entrySet().removeIf(entry -> {
                    if (budget != null) budget.observe(PdkWriteReturnWorkBudget.Activity.SCOPE_CACHE_ENTRY);
                    return entry.getValue().get() == null;
                });
                Transactions observed = new Transactions(functions, budget);
                if (OBSERVERS.size() >= MAX_INSPECTORS) {
                    observed.limit = true;
                    return observed;
                }
                observed.wrap();
                OBSERVERS.put(functions, new WeakReference<>(observed));
                return observed;
            }
        }

        private void wrap() {
            TransactionBeginFunction originalBegin = functions.getTransactionBeginFunction();
            TransactionCommitFunction originalCommit = functions.getTransactionCommitFunction();
            TransactionRollbackFunction originalRollback = functions.getTransactionRollbackFunction();
            if (originalBegin != null) {
                begin = context -> {
                    var span = transaction();
                    try { enter(); originalBegin.begin(context); if (span != null) span.success(); }
                    finally { if (span != null) span.close(); }
                };
                functions.supportTransactionBeginFunction(begin);
            }
            if (originalCommit != null) {
                commit = context -> {
                    var span = transaction();
                    try { enter(); originalCommit.commit(context); if (span != null) span.success(); }
                    finally { if (span != null) span.close(); }
                };
                functions.supportTransactionCommitFunction(commit);
            }
            if (originalRollback != null) {
                rollback = context -> {
                    var span = transaction();
                    try { enter(); originalRollback.rollback(context); if (span != null) span.success(); }
                    finally { if (span != null) span.close(); }
                };
                functions.supportTransactionRollbackFunction(rollback);
            }
        }

        private void enter() {
            calls.updateAndGet(value -> value == Long.MAX_VALUE ? value : value + 1);
        }

        private PdkWriteReturnWorkBudget.Span transaction() {
            return workBudget == null ? null : workBudget.enter(PdkWriteReturnWorkBudget.Activity.TRANSACTION_WRAPPER);
        }

        String rejection() {
            if (limit) return "TRANSACTION_OBSERVER_LIMIT";
            if (calls.get() != 0) return "TRANSACTION_FUNCTION_INVOKED";
            if (begin == null || commit == null || rollback == null) return "TRANSACTION_OBSERVATION_UNAVAILABLE";
            if (functions.getTransactionBeginFunction() != begin
                    || functions.getTransactionCommitFunction() != commit
                    || functions.getTransactionRollbackFunction() != rollback) return "TRANSACTION_WRAPPER_CHANGED";
            return null;
        }
    }

    private record Observation(Object config, Object client, Object database, Object writer,
            Map<?, ?> writers, Map<?, ?> sessions, String concern, String rejection) {
        boolean sameScope(Observation after) {
            return config == after.config && client == after.client && writers == after.writers
                    && sessions == after.sessions && Objects.equals(concern, after.concern)
                    && (writer == null || writer == after.writer)
                    && (writer == null || database == after.database);
        }
    }

    /** All class discovery, digesting and reflective handle lookup stays outside the measured call. */
    private static final class Access {
        private final Object delegate;
        private final Class<?> configType;
        private final Class<?> clientType;
        private final Class<?> databaseType;
        private final Class<?> writerType;
        private final Class<?> concernType;
        private final Field config;
        private final Field client;
        private final Field database;
        private final Field writers;
        private final Field sessions;
        private final Field writerConfig;
        private final Field writerClient;
        private final Field writerDatabase;
        private final Field writerSessions;
        private final Method doubleActive;
        private final Method databaseConcern;
        private final Method acknowledged;
        private final Method w;
        private final Method journal;
        private final Method timeout;

        private Access(Object delegate, Class<?>[] types) throws ReflectiveOperationException {
            this.delegate = delegate;
            Class<?> connectorType = types[0];
            configType = types[1]; writerType = types[2]; clientType = types[3];
            databaseType = types[4]; concernType = types[5];
            config = field(connectorType, "mongoConfig"); client = field(connectorType, "mongoClient");
            database = field(connectorType, "mongoDatabase"); writers = field(connectorType, "writerMap");
            sessions = field(connectorType, "transactionSessionMap");
            writerConfig = field(writerType, "mongodbConfig"); writerClient = field(writerType, "mongoClient");
            writerDatabase = field(writerType, "mongoDatabase"); writerSessions = field(writerType, "sessionMap");
            doubleActive = configType.getMethod("getDoubleActive");
            databaseConcern = databaseType.getMethod("getWriteConcern");
            acknowledged = concernType.getMethod("isAcknowledged"); w = concernType.getMethod("getWObject");
            journal = concernType.getMethod("getJournal"); timeout = concernType.getMethod("getWTimeout", TimeUnit.class);
        }

        static Access open(Object delegate, PdkWriteReturnWorkBudget budget) throws ReflectiveOperationException, IOException {
            if (delegate == null || !MONGO_CONNECTOR.equals(delegate.getClass().getName())) {
                throw new UnsupportedShape("UNSUPPORTED_CONNECTOR");
            }
            Path artifact = artifact(delegate.getClass());
            if (!PINNED_JAR.equals(fileDigest(artifact, budget))) throw new UnsupportedShape("CONNECTOR_BYTES_UNSUPPORTED");
            if (hasAgent(ManagementFactory.getRuntimeMXBean().getInputArguments(), budget)) {
                throw new UnsupportedShape("TRANSFORMED_CLASSES_UNPROVEN");
            }
            String[] names = { MONGO_CONNECTOR, "io.tapdata.mongodb.entity.MongodbConfig",
                    "io.tapdata.mongodb.writer.MongodbWriter", "com.mongodb.client.internal.MongoClientImpl",
                    "com.mongodb.client.internal.MongoDatabaseImpl", "com.mongodb.WriteConcern",
                    "com.mongodb.client.internal.MongoCollectionImpl", "io.tapdata.mongodb.MongodbUtil",
                    "io.tapdata.common.CommonDbConfig", "com.mongodb.MongoClientSettings$Builder" };
            Class<?>[] types = new Class<?>[names.length];
            for (int index = 0; index < names.length; index++) {
                types[index] = Class.forName(names[index], false, delegate.getClass().getClassLoader());
                if (!artifact.equals(artifact(types[index]))) throw new UnsupportedShape("CLASS_ORIGIN_MISMATCH");
            }
            return new Access(delegate, types);
        }

        Observation read(String name, boolean requireWriter) throws ReflectiveOperationException {
            Object actualConfig = config.get(delegate), actualClient = client.get(delegate);
            Object actualDatabase = database.get(delegate);
            Object rawWriters = writers.get(delegate), rawSessions = sessions.get(delegate);
            if (actualConfig == null || actualConfig.getClass() != configType
                    || actualClient == null || actualClient.getClass() != clientType
                    || !(rawWriters instanceof Map<?, ?> writerMap) || !(rawSessions instanceof Map<?, ?> sessionMap)
                    || rawWriters.getClass() != java.util.concurrent.ConcurrentHashMap.class
                    || rawSessions.getClass() != java.util.concurrent.ConcurrentHashMap.class) {
                return rejected("RUNTIME_OBJECT_UNSUPPORTED");
            }
            if (!Boolean.FALSE.equals(invoke(doubleActive, actualConfig))) return rejected("DOUBLE_ACTIVE_OR_UNKNOWN");
            if (sessionMap.get(name) != null) return rejected("EXTERNAL_SESSION_PRESENT");
            Object writer = writerMap.get(name);
            if (writer != null) {
                if (writer.getClass() != writerType || writerConfig.get(writer) != actualConfig
                        || writerClient.get(writer) != actualClient || writerSessions.get(writer) != sessionMap) {
                    return rejected("WRITER_SCOPE_UNSUPPORTED");
                }
                actualDatabase = writerDatabase.get(writer);
            } else if (requireWriter) return rejected("ACTUAL_WRITER_UNAVAILABLE");
            if (actualDatabase == null || actualDatabase.getClass() != databaseType) return rejected("DATABASE_SCOPE_UNSUPPORTED");
            Object effective = invoke(databaseConcern, actualDatabase);
            if (effective == null || effective.getClass() != concernType) return rejected("WRITE_CONCERN_UNAVAILABLE");
            if (!Boolean.TRUE.equals(invoke(acknowledged, effective))) return rejected("UNACKNOWLEDGED_WRITE_CONCERN");
            Object rawJournal = invoke(journal, effective), rawTimeout = invoke(timeout, effective, TimeUnit.MILLISECONDS);
            if ((rawJournal != null && !(rawJournal instanceof Boolean))
                    || (rawTimeout != null && !(rawTimeout instanceof Integer))) return rejected("WRITE_CONCERN_UNSUPPORTED");
            String evidence = concern(invoke(w, effective), (Boolean) rawJournal, (Integer) rawTimeout);
            if (evidence == null) return rejected("WRITE_CONCERN_UNSUPPORTED");
            return new Observation(actualConfig, actualClient, actualDatabase, writer, writerMap, sessionMap, evidence, null);
        }

        private static Observation rejected(String reason) {
            return new Observation(null, null, null, null, null, null, null, reason);
        }
    }

    private static Field field(Class<?> type, String name) throws NoSuchFieldException {
        Field found = type.getDeclaredField(name);
        if (!found.trySetAccessible()) throw new UnsupportedShape("REFLECTION_UNAVAILABLE");
        return found;
    }

    private static Object invoke(Method method, Object instance, Object... arguments) throws ReflectiveOperationException {
        try {
            return method.invoke(instance, arguments);
        } catch (InvocationTargetException failed) {
            if (failed.getCause() instanceof VirtualMachineError fatal) throw fatal;
            throw failed;
        }
    }

    private static Path artifact(Class<?> type) throws IOException {
        var source = type.getProtectionDomain().getCodeSource();
        if (source == null || source.getLocation() == null || !"file".equals(source.getLocation().getProtocol())) {
            throw new UnsupportedShape("CLASS_ORIGIN_UNAVAILABLE");
        }
        try {
            Path path = Path.of(source.getLocation().toURI()).toRealPath();
            if (!Files.isRegularFile(path)) throw new UnsupportedShape("CLASS_ORIGIN_UNSUPPORTED");
            return path;
        } catch (java.net.URISyntaxException failure) {
            throw new UnsupportedShape("CLASS_ORIGIN_UNSUPPORTED");
        }
    }

    private static String fileDigest(Path path, PdkWriteReturnWorkBudget budget) throws IOException {
        long limit = budget == null ? Long.MAX_VALUE : budget.connectorHashLimit();
        if (budget != null && (limit < 1 || Files.size(path) > limit)) {
            budget.gap("CONNECTOR_HASH_MATERIAL_BOUND"); throw new UnsupportedShape("CONNECTOR_BYTES_UNSUPPORTED");
        }
        MessageDigest digest = digest();
        try (var in = Files.newInputStream(path)) {
            byte[] buffer = new byte[16384];
            long total = 0;
            for (;;) {
                var reading = budget == null ? null : budget.enter(PdkWriteReturnWorkBudget.Activity.HASH_READ);
                int read;
                try { read = in.read(buffer); if (reading != null) reading.success(); }
                finally { if (reading != null) reading.close(); }
                if (read == -1) break;
                if (budget != null) {
                    budget.material(4, read);
                    budget.observe(PdkWriteReturnWorkBudget.Activity.HASH_MATERIAL, read);
                }
                if (read == 0 || read > limit - total) {
                    if (budget != null) budget.gap("CONNECTOR_HASH_PROGRESS_OR_GROWTH");
                    throw new IOException("Connector material exceeds its admitted bound");
                }
                total += read; digest.update(buffer, 0, read);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static boolean hasAgent(java.util.List<String> arguments, PdkWriteReturnWorkBudget budget) {
        if (budget == null) return arguments.stream().anyMatch(argument -> argument.startsWith("-javaagent:")
                || argument.startsWith("-agentpath:") || argument.startsWith("-agentlib:"));
        if (arguments == null || arguments.size() > budget.argumentLimit()) {
            budget.gap("RUNTIME_ARGUMENT_ROSTER_BOUND"); throw new UnsupportedShape("TRANSFORMED_CLASSES_UNPROVEN");
        }
        long material = 0;
        int count = arguments.size();
        for (int index = 0; index < count; index++) {
            String argument = arguments.get(index);
            budget.observe(PdkWriteReturnWorkBudget.Activity.ARGUMENT);
            if (argument == null || argument.length() > 16384) {
                budget.gap("RUNTIME_ARGUMENT_MATERIAL_BOUND"); throw new UnsupportedShape("TRANSFORMED_CLASSES_UNPROVEN");
            }
            budget.observe(PdkWriteReturnWorkBudget.Activity.ARGUMENT_MATERIAL, argument.length());
            int bytes = argument.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            if (bytes > budget.argumentBytesLimit() - material) {
                budget.gap("RUNTIME_ARGUMENT_MATERIAL_BOUND"); throw new UnsupportedShape("TRANSFORMED_CLASSES_UNPROVEN");
            }
            material += bytes; budget.material(4, bytes);
            if (argument.startsWith("-javaagent:") || argument.startsWith("-agentpath:") || argument.startsWith("-agentlib:")) return true;
        }
        return false;
    }

    private static String sha256(byte[] bytes) { return HexFormat.of().formatHex(digest().digest(bytes)); }

    private static MessageDigest digest() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }

    private static final class UnsupportedShape extends RuntimeException {
        private final String reason;
        private UnsupportedShape(String reason) { this.reason = reason; }
    }
}
