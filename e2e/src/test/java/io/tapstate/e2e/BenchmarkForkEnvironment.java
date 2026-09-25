package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import org.bson.Document;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Owns one real connector, source database, Mongo databases and application process per fork. */
final class BenchmarkForkEnvironment implements AutoCloseable {

    private static final Duration PIPELINE_WAIT = Duration.ofMinutes(3);
    private static final Duration TARGET_WAIT = Duration.ofMinutes(5);
    private static final Duration TARGET_POLL = Duration.ofMillis(300);

    @FunctionalInterface
    interface BatchHook {
        void beforeBatch(BenchmarkWorkloadDefinitions.Phase phase, int batchIndex,
                         long issuedAtNanos, List<String> sql) throws Exception;
    }

    record BatchResult(int index, long issuedAtNanos, long completedAtNanos) {}

    record TargetResult(BenchmarkWorkloadDefinitions.TargetExpectation expectation,
                        long rows, String checksum) {
        boolean matches() {
            return rows == expectation.rows() && expectation.checksum().equals(checksum);
        }
    }

    record PhaseResult(BenchmarkWorkloadDefinitions.Phase phase, long startedAtNanos,
                       long completedAtNanos, List<BatchResult> batches,
                       List<TargetResult> targets) {
        PhaseResult {
            batches = List.copyOf(batches);
            targets = List.copyOf(targets);
        }
    }

    private final BenchmarkWorkloadDefinitions.Workload workload;
    private final String forkId;
    private final Map<String, Object> sourceSettings;
    private final String storeUri;
    private final String externalTargetUri;
    private final String managedViewsUri;
    private final String operatorStateUri;
    private final RealProcessServer server;
    private final ControlPlane control;
    private final Connection source;
    private final MongoClient mongo;

    private int nextPhase;
    private boolean closed;

    private BenchmarkForkEnvironment(BenchmarkWorkloadDefinitions.Workload workload, String forkId,
            Map<String, Object> sourceSettings, String storeUri, String externalTargetUri,
            String managedViewsUri, String operatorStateUri, RealProcessServer server,
            ControlPlane control, Connection source, MongoClient mongo) {
        this.workload = workload;
        this.forkId = forkId;
        this.sourceSettings = Map.copyOf(sourceSettings);
        this.storeUri = storeUri;
        this.externalTargetUri = externalTargetUri;
        this.managedViewsUri = managedViewsUri;
        this.operatorStateUri = operatorStateUri;
        this.server = server;
        this.control = control;
        this.source = source;
        this.mongo = mongo;
    }

    /** Opens and starts one fork, with all source rows present before the first pipeline starts. */
    static BenchmarkForkEnvironment open(BenchmarkWorkloadDefinitions.Workload workload,
            Path applicationJar, String forkId) throws Exception {
        Objects.requireNonNull(workload, "workload");
        Objects.requireNonNull(applicationJar, "applicationJar");
        if (!Files.isRegularFile(applicationJar) || forkId == null || forkId.isBlank()) {
            throw new IllegalArgumentException("a fork needs an application JAR and a nonempty id");
        }
        String namespace = "bench_" + workload.id() + "_"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        Map<String, Object> sourceSettings = workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                ? SharedMySql.settings(namespace + "_src")
                : SharedPostgres.settings(namespace + "_src");
        String storeUri = SharedMongo.replicaSetUrl(namespace + "_control");
        String externalTargetUri = SharedMongo.replicaSetUrl(namespace + "_target");
        String managedViewsUri = SharedMongo.replicaSetUrl("views");
        String operatorDatabase = namespace + "_operator";
        String operatorStateUri = SharedMongo.replicaSetUrl(operatorDatabase);

        Connection source = workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                ? SharedMySql.connect(sourceSettings) : SharedPostgres.connect(sourceSettings);
        RealProcessServer server = null;
        MongoClient mongo = null;
        try {
            execute(source, workload.setupSql());
            // The view database is product-wide on this replica set. Forks run serially, and the
            // preceding application process is closed before these shared collections are dropped.
            workload.resetTargets(externalTargetUri, managedViewsUri, operatorStateUri);
            server = RealProcessServer.start(storeUri, operatorDatabase, applicationJar);
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("benchmark", "benchmark-password");
            String sourceConnector = workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                    ? "mysql" : "postgres";
            control.registerConnector(sourceConnector, ConnectorJars.bytesFor(sourceConnector));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            Map<String, String> resources = workload.resources(sourceSettings, externalTargetUri);
            Map<String, String> sourceResources = new LinkedHashMap<>();
            for (BenchmarkWorkloadDefinitions.SourceChain chain : workload.sourceChains()) {
                String file = chain.sourceId() + ".tap.yml";
                String sourceYaml = resources.get(file);
                if (sourceYaml == null) {
                    throw new IllegalStateException("benchmark source resource is missing: " + file);
                }
                sourceResources.put(file, sourceYaml);
            }
            control.apply(sourceResources);
            Map<String, Object> connectorConfig = workload.connectorConfig(sourceSettings);
            for (BenchmarkWorkloadDefinitions.SourceChain chain : workload.sourceChains()) {
                control.discoverSchema(chain.sourceId(), sourceConnector, connectorConfig);
            }
            // Apply validates references within the submitted workspace, so the discovered sources
            // travel with their dependents again even though their content is unchanged.
            control.apply(resources);
            for (String pipeline : workload.pipelineIds()) {
                control.lifecycle(pipeline, LifecycleVerb.START);
            }
            for (String pipeline : workload.pipelineIds()) {
                awaitRunning(control, pipeline);
            }
            mongo = MongoClients.create(storeUri);
            return new BenchmarkForkEnvironment(workload, forkId, sourceSettings, storeUri,
                    externalTargetUri, managedViewsUri, operatorStateUri, server, control, source, mongo);
        } catch (Exception | Error failure) {
            if (server != null) {
                server.close();
            }
            if (mongo != null) {
                mongo.close();
            }
            source.close();
            throw failure;
        }
    }

    private static void awaitRunning(ControlPlane control, String pipeline) {
        Await.until(pipeline + " to run", PIPELINE_WAIT,
                () -> control.state(pipeline).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(pipeline)));
    }

    BenchmarkWorkloadDefinitions.Workload workload() {
        return workload;
    }

    String forkId() {
        return forkId;
    }

    Map<String, Object> sourceSettings() {
        return sourceSettings;
    }

    String storeUri() {
        return storeUri;
    }

    String externalTargetUri() {
        return externalTargetUri;
    }

    String managedViewsUri() {
        return managedViewsUri;
    }

    String operatorStateUri() {
        return operatorStateUri;
    }

    ControlPlane control() {
        return control;
    }

    RealProcessServer server() {
        return server;
    }

    /** Issues a source-side preflight statement without advancing the frozen phase sequence. */
    void executeSource(String sql) throws Exception {
        requireOpen();
        Objects.requireNonNull(sql, "sql");
        execute(source, List.of(sql));
    }

    /** A guarded benchmark boundary write must affect its one chosen source row. */
    void executeOneSourceUpdate(String sql) throws Exception {
        requireOpen();
        Objects.requireNonNull(sql, "sql");
        try (Statement statement = source.createStatement()) {
            int affected = statement.executeUpdate(sql);
            if (affected != 1) {
                throw new AssertionError("benchmark boundary update affected " + affected + " rows");
            }
        }
    }

    /** Recheck the last completed phase after the unmeasured source boundary has been ACKed. */
    List<TargetResult> verifyCurrentTargets(BenchmarkWorkloadDefinitions.Phase phase) throws Exception {
        requireOpen();
        if (nextPhase == 0 || !workload.phases().get(nextPhase - 1).equals(phase)) {
            throw new IllegalArgumentException("target verification requires the last completed phase");
        }
        return awaitTargets(phase);
    }

    /** Runs the next declared phase; the hook can register target expectations before SQL begins. */
    PhaseResult runPhase(BenchmarkWorkloadDefinitions.Phase phase, boolean paced,
            BatchHook beforeBatch) throws Exception {
        requireOpen();
        Objects.requireNonNull(phase, "phase");
        Objects.requireNonNull(beforeBatch, "beforeBatch");
        if (nextPhase >= workload.phases().size() || !workload.phases().get(nextPhase).equals(phase)) {
            throw new IllegalArgumentException("benchmark phases must run in declared order");
        }
        long started = System.nanoTime();
        long nextBatchStart = started;
        List<BatchResult> batches = new ArrayList<>();
        int batchIndex = 0;
        for (List<String> sql : phase.batches()) {
            if (paced) {
                long remaining = nextBatchStart - System.nanoTime();
                if (remaining > 0) {
                    TimeUnit.NANOSECONDS.sleep(remaining);
                }
            }
            long issuedAt = System.nanoTime();
            beforeBatch.beforeBatch(phase, batchIndex, issuedAt, sql);
            execute(source, sql);
            long completedAt = System.nanoTime();
            batches.add(new BatchResult(batchIndex, issuedAt, completedAt));
            nextBatchStart = issuedAt + phase.batchInterval().toNanos();
            batchIndex++;
        }
        List<TargetResult> targets = awaitTargets(phase);
        nextPhase++;
        return new PhaseResult(phase, started, System.nanoTime(), batches, targets);
    }

    PhaseResult runPhase(BenchmarkWorkloadDefinitions.Phase phase, boolean paced) throws Exception {
        return runPhase(phase, paced, (ignoredPhase, ignoredIndex, ignoredAt, ignoredSql) -> {});
    }

    List<PhaseResult> runAllPhases(boolean paced) throws Exception {
        List<PhaseResult> results = new ArrayList<>();
        while (nextPhase < workload.phases().size()) {
            results.add(runPhase(workload.phases().get(nextPhase), paced));
        }
        return List.copyOf(results);
    }

    private List<TargetResult> awaitTargets(BenchmarkWorkloadDefinitions.Phase phase) throws Exception {
        long deadline = System.nanoTime() + TARGET_WAIT.toNanos();
        List<TargetResult> latest;
        do {
            latest = phase.targets().stream().map(this::readTarget).toList();
            if (latest.stream().allMatch(TargetResult::matches)) {
                return latest;
            }
            if (System.nanoTime() >= deadline) {
                String timeout = "fork " + forkId + " phase " + phase.id()
                        + " did not reach target count/checksum: " + latest;
                if (workload.id().equals("stateful")
                        && phase.stage() == BenchmarkWorkloadDefinitions.Stage.COLD_READ) {
                    try {
                        timeout += "; " + nestColdReadDiagnostics(phase);
                    } catch (RuntimeException | AssertionError diagnosticFailure) {
                        timeout += "; nest diagnostic unavailable: " + diagnosticFailure;
                    }
                }
                throw new AssertionError(timeout);
            }
            TimeUnit.NANOSECONDS.sleep(TARGET_POLL.toNanos());
        } while (true);
    }

    /** Read only on timeout: distinguish absent cold children from a wrong value on complete roots. */
    private String nestColdReadDiagnostics(BenchmarkWorkloadDefinitions.Phase phase) {
        BenchmarkWorkloadDefinitions.TargetExpectation nest = phase.targets().stream()
                .filter(target -> target.projection() == BenchmarkWorkloadDefinitions.Projection.NEST)
                .findFirst().orElseThrow(() -> new IllegalStateException("cold-read has no nest target"));
        String target = diagnosticRead(() -> nestTargetDiagnostics(nest));
        String state = diagnosticRead(() -> String.valueOf(control.state(nest.pipelineId())));
        String metrics = diagnosticRead(() -> control.metrics(nest.pipelineId()));
        return "nestColdRead{target=" + target + ",state=" + state
                + ",metrics=" + abbreviate(metrics, 4096) + "}";
    }

    private String nestTargetDiagnostics(BenchmarkWorkloadDefinitions.TargetExpectation nest) {
        String database = Objects.requireNonNull(new ConnectionString(externalTargetUri).getDatabase(),
                "nest target database");
        MongoCollection<Document> collection = mongo.getDatabase(database).getCollection(nest.table());
        long rootCount = 0;
        long childCount = 0;
        long missingCold = 0;
        long unreadableRoots = 0;
        List<String> examples = new ArrayList<>(5);
        List<String> firstRoots = new ArrayList<>(5);
        for (Document root : collection.find().maxTime(15, TimeUnit.SECONDS)) {
            rootCount++;
            addExample(firstRoots, root);
            Object rawItems = root.get("items");
            if (!(rawItems instanceof List<?> items)) {
                unreadableRoots++;
                addExample(examples, root);
                continue;
            }
            childCount += items.size();
            if (!(root.get("id") instanceof Number rootId)) {
                unreadableRoots++;
                addExample(examples, root);
                continue;
            }
            long expectedColdId = 300_000L + rootId.longValue();
            boolean found = false;
            boolean unreadableChild = false;
            for (Object rawItem : items) {
                if (!(rawItem instanceof Map<?, ?> item) || !(item.get("id") instanceof Number id)) {
                    unreadableChild = true;
                } else if (id.longValue() == expectedColdId) {
                    found = true;
                }
            }
            if (unreadableChild) {
                unreadableRoots++;
                addExample(examples, root);
            } else if (!found) {
                missingCold++;
                addExample(examples, root);
            }
        }
        if (examples.isEmpty()) {
            examples.addAll(firstRoots);
        }
        return "roots=" + rootCount + ",children=" + childCount
                + ",rootsMissingExpectedColdItem=" + missingCold
                + ",unreadableRoots=" + unreadableRoots + ",examples=" + examples;
    }

    private static void addExample(List<String> examples, Document root) {
        if (examples.size() < 5) {
            examples.add(abbreviate(root.toJson(), 4096));
        }
    }

    private static String diagnosticRead(java.util.function.Supplier<String> read) {
        try {
            return read.get();
        } catch (RuntimeException | AssertionError unavailable) {
            return "UNAVAILABLE(" + unavailable + ")";
        }
    }

    private static String abbreviate(String value, int limit) {
        return value.length() <= limit ? value : value.substring(0, limit) + "...(truncated)";
    }

    private TargetResult readTarget(BenchmarkWorkloadDefinitions.TargetExpectation expectation) {
        String uri = expectation.location() == BenchmarkWorkloadDefinitions.TargetLocation.MANAGED_VIEW
                ? managedViewsUri : externalTargetUri;
        String database = Objects.requireNonNull(new ConnectionString(uri).getDatabase(), "target database");
        MongoCollection<Document> collection = mongo.getDatabase(database).getCollection(expectation.table());
        long count = collection.countDocuments();
        if (count != expectation.rows()) {
            return new TargetResult(expectation, count, null);
        }
        List<Document> documents = collection.find().into(new ArrayList<>());
        try {
            return new TargetResult(expectation, documents.size(),
                    BenchmarkWorkloadDefinitions.checksumOf(expectation, documents));
        } catch (RuntimeException | AssertionError incomplete) {
            return new TargetResult(expectation, documents.size(), "UNREADABLE: " + incomplete);
        }
    }

    private static void execute(Connection connection, List<String> sql) throws Exception {
        if (sql.isEmpty()) {
            return;
        }
        try (Statement statement = connection.createStatement()) {
            for (String command : sql) {
                statement.execute(command);
            }
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("benchmark fork is closed");
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        try {
            server.close();
        } finally {
            try {
                source.close();
            } finally {
                mongo.close();
            }
        }
    }
}
