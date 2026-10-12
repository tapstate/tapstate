package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;

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
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Owns one real connector, source database, Mongo databases and application process per fork. */
final class BenchmarkForkEnvironment implements AutoCloseable {

    private static final Duration PIPELINE_WAIT = Duration.ofMinutes(3);
    private static final Duration TARGET_WAIT = Duration.ofMinutes(5);
    private final BenchmarkReturnWorkBudget.Scope workScope = BenchmarkReturnWorkBudget.current();
    private static final Duration TARGET_POLL = Duration.ofSeconds(2);

    @FunctionalInterface
    interface BootLauncher {
        OwnedBoot launch(String storeUri, String operatorStateDatabase, Path applicationJar) throws Exception;
    }

    record OwnedBoot(RealProcessServer server, BenchmarkJdiTelemetrySession telemetry,
            ExecutionAdmissionJdiSession admission) implements AutoCloseable {
        OwnedBoot(RealProcessServer server, BenchmarkJdiTelemetrySession telemetry) {
            this(server, telemetry, null);
        }
        OwnedBoot {
            Objects.requireNonNull(server, "owned server");
            if (telemetry != null && telemetry.server() != server) {
                throw new IllegalArgumentException("telemetry must own this exact server");
            }
            if (admission != null && (admission.server() != server || telemetry != null)) {
                throw new IllegalArgumentException("admission must exclusively own this exact server");
            }
        }
        static OwnedBoot plain(String storeUri, String operatorStateDatabase, Path applicationJar) {
            return new OwnedBoot(RealProcessServer.start(storeUri, operatorStateDatabase, applicationJar), null);
        }
        void check() {
            if (telemetry != null) { telemetry.checkCapture(); }
            if (admission != null) { admission.checkCapture(); }
        }
        void begin() throws Exception { if (telemetry != null) { telemetry.begin(); } }
        void cutoff() throws Exception { if (telemetry != null) { telemetry.cutoff(); } }
        Optional<BenchmarkJdiTelemetrySession.Evidence> finish() throws Exception {
            return telemetry == null ? Optional.empty() : Optional.of(telemetry.shutdownAndFinish());
        }
        boolean terminated() { return server.terminated(); }
        @Override public void close() throws Exception {
            BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.CLEANUP,
                    BenchmarkSharedSupportSourceWork.Kind.CLOSING, () -> {
                        if (BenchmarkReturnWorkBudget.current() != null && (admission != null || telemetry != null)) {
                            BenchmarkReturnWorkBudget.gap(BenchmarkReturnWorkBudget.current(), BenchmarkReturnWorkBudget.Activity.CLEANUP,
                                    "OWNED_CLOSE_OUTSIDE_REGISTERED_PLAIN_BODY");
                        }
                        if (admission != null) { admission.close(); }
                        else if (telemetry != null) { telemetry.close(); }
                        else { server.close(); }
                        return null;
                    });
        }
    }

    @FunctionalInterface
    interface BeforeStart {
        void accept(OwnedBoot boot) throws Exception;
    }

    @FunctionalInterface
    interface BatchHook {
        void beforeBatch(BenchmarkWorkloadDefinitions.Phase phase, int batchIndex,
                         long issuedAtNanos, List<String> sql) throws Exception;
    }

    record BatchResult(int index, long issuedAtNanos, long completedAtNanos) {
        long durationNanos() {
            return completedAtNanos - issuedAtNanos;
        }
    }

    /** The bracket bounds this clock read's uncertainty, not later clock adjustments or UTC accuracy. */
    record ClockAnchor(Instant utc, long beforeNanos, long afterNanos) {
        ClockAnchor {
            Objects.requireNonNull(utc, "UTC anchor");
            if (afterNanos - beforeNanos < 0) {
                throw new IllegalArgumentException("clock anchor bracket moved backward");
            }
        }

        static ClockAnchor capture() {
            long before = BenchmarkReturnWorkBudget.rootNanoTime();
            Instant utc = Instant.now();
            return new ClockAnchor(utc, before, BenchmarkReturnWorkBudget.rootNanoTime());
        }

        long uncertaintyNanos() {
            return afterNanos - beforeNanos;
        }

        Instant earliestUtc(long monotonicNanos) {
            return utc.plusNanos(monotonicNanos - afterNanos);
        }

        Instant latestUtc(long monotonicNanos) {
            return utc.plusNanos(monotonicNanos - beforeNanos);
        }
    }

    record PhaseIssue(BenchmarkWorkloadDefinitions.Phase phase, long startedAtNanos,
                      long sourceCompletedAtNanos, List<BatchResult> batches, ClockAnchor clockAnchor) {
        PhaseIssue {
            batches = List.copyOf(batches);
            Objects.requireNonNull(clockAnchor, "phase clock anchor");
        }
    }

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
    private final OwnedBoot boot;
    private final BenchmarkOwnedProcessReceipt ownedProcessReceipt;
    private final ControlPlane control;
    private final Connection source;
    private final MongoClient mongo;
    private final SharedPostgres.Fixture postgresFixture;
    private final SharedPostgres.Closing bootClosing;

    private int nextPhase;
    private BenchmarkWorkloadDefinitions.Phase pendingPhase;
    private boolean closed;

    private BenchmarkForkEnvironment(BenchmarkWorkloadDefinitions.Workload workload, String forkId,
            Map<String, Object> sourceSettings, String storeUri, String externalTargetUri,
            String managedViewsUri, String operatorStateUri, OwnedBoot boot, BenchmarkOwnedProcessReceipt ownedProcessReceipt,
            ControlPlane control, Connection source, MongoClient mongo,
            SharedPostgres.Fixture postgresFixture, SharedPostgres.Closing bootClosing) {
        this.workload = workload;
        this.forkId = forkId;
        this.sourceSettings = Map.copyOf(sourceSettings);
        this.storeUri = storeUri;
        this.externalTargetUri = externalTargetUri;
        this.managedViewsUri = managedViewsUri;
        this.operatorStateUri = operatorStateUri;
        this.boot = Objects.requireNonNull(boot); this.server = boot.server();
        this.ownedProcessReceipt = Objects.requireNonNull(ownedProcessReceipt);
        this.control = control;
        this.source = source;
        this.mongo = mongo; this.postgresFixture = postgresFixture; this.bootClosing = bootClosing;
    }

    /** Opens and starts one fork, with all source rows present before the first pipeline starts. */
    static BenchmarkForkEnvironment open(BenchmarkWorkloadDefinitions.Workload workload,
            Path applicationJar, String forkId) throws Exception {
        return open(workload, applicationJar, forkId, OwnedBoot::plain);
    }

    static BenchmarkForkEnvironment open(BenchmarkWorkloadDefinitions.Workload workload,
            Path applicationJar, String forkId, BootLauncher launcher) throws Exception {
        return open(workload, applicationJar, forkId, launcher, boot -> { });
    }

    /** Allows a diagnostic baseline after setup while the real first START is still pending. */
    static BenchmarkForkEnvironment open(BenchmarkWorkloadDefinitions.Workload workload,
            Path applicationJar, String forkId, BootLauncher launcher, BeforeStart beforeStart) throws Exception {
        var scope = BenchmarkReturnWorkBudget.current();
        if (scope != null) {
            BenchmarkSharedSupportSourceWork.originalWorkload(workload);
            BenchmarkSharedSupportSourceWork.scalar(forkId, BenchmarkSharedSupportSourceWork.NAME);
        }
        var opening = BenchmarkReturnWorkBudget.begin(scope, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, BenchmarkReturnWorkBudget.Work.call());
        Objects.requireNonNull(workload, "workload");
        Objects.requireNonNull(applicationJar, "applicationJar");
        if (!Files.isRegularFile(applicationJar) || forkId == null || forkId.isBlank()) {
            throw new IllegalArgumentException("a fork needs an application JAR and a nonempty id");
        }
        String namespace = "bench_" + workload.id() + "_"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        SharedPostgres.Fixture postgresFixture = SharedPostgres.fixture();
        SharedPostgres.Closing bootClosing = postgresFixture.pending("owned benchmark boot");
        Connection source = null;
        OwnedBoot boot = null;
        BenchmarkOwnedProcessReceipt ownedProcessReceipt = null;
        MongoClient mongo = null;
        try {
            Map<String, Object> sourceSettings = workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                    ? SharedMySql.settings(namespace + "_src")
                    : postgresFixture.settings(namespace + "_src");
            String storeUri = SharedMongo.replicaSetUrl(namespace + "_control");
            String externalTargetUri = SharedMongo.replicaSetUrl(namespace + "_target");
            String managedViewsUri = SharedMongo.replicaSetUrl("views");
            String operatorDatabase = namespace + "_operator";
            String operatorStateUri = SharedMongo.replicaSetUrl(operatorDatabase);

            source = workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                    ? SharedMySql.connect(sourceSettings) : SharedPostgres.connect(sourceSettings);
            execute(source, workload.setupSql());
            // The view database is product-wide on this replica set. Forks run serially, and the
            // preceding application process is closed before these shared collections are dropped.
            resetTargets(workload, externalTargetUri, managedViewsUri, operatorStateUri);
            if (Boolean.getBoolean(BenchmarkMongoDeliveryObserver.CLOCK_REJECTION_EVIDENCE_PROPERTY)) {
                Map<String, Object> identity = SharedMongo.diagnosticIdentity();
                System.out.println("benchmark-owned-mongo-clock-fixture=" + JsonWriter.write(identity));
                if (!"COMPLETE".equals(identity.get("status"))) {
                    throw new AssertionError("clock rejection evidence requires an actual owned Mongo build identity");
                }
            }
            boot = Objects.requireNonNull(BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS,
                    () -> launcher.launch(storeUri, operatorDatabase, applicationJar)));
            RealProcessServer server = boot.server();
            OwnedBoot ownedBoot = boot;
            postgresFixture.bind(bootClosing, ownedBoot, ownedBoot::terminated);
            Map<String, Object> bindings = new LinkedHashMap<>();
            bindings.put("namespace", namespace); bindings.put("forkId", forkId); bindings.put("workload", workload.id());
            bindings.put("sourceDatabaseKind", workload.database().name());
            Map<String, Object> sourceBinding = new LinkedHashMap<>();
            for (String name : List.of("host", "port", "database", "schema")) {
                if (sourceSettings.get(name) != null) { sourceBinding.put(name, sourceSettings.get(name)); }
            }
            bindings.put("sourceSettings", Map.copyOf(sourceBinding)); bindings.put("storeUri", storeUri);
            bindings.put("externalTargetUri", externalTargetUri); bindings.put("managedViewsUri", managedViewsUri);
            bindings.put("operatorDatabase", operatorDatabase); bindings.put("operatorStateUri", operatorStateUri);
            bindings.put("captureBoundary", "AFTER_SOURCE_FIXTURE_PREPARATION_BEFORE_PIPELINE_CONFIGURATION_AND_START");
            ownedProcessReceipt = BenchmarkOwnedProcessReceipt.capture(server, applicationJar, bindings);
            ControlPlane readiness = new ControlPlane(server.baseUrl());
            Await.until("benchmark owned boot readiness", () -> { ownedBoot.check(); return BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.CONTROL_RPC, readiness::healthy); },
                    () -> "owned process alive=" + server.isAlive());
            ControlPlane control = new ControlPlane(server.baseUrl());
            BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.CONTROL_RPC,
                    () -> { control.bootstrapAndLogin("benchmark", "benchmark-password"); return null; });
            String sourceConnector = workload.database() == BenchmarkWorkloadDefinitions.Database.MYSQL
                    ? "mysql" : "postgres";
            byte[] sourceJar = BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                    () -> ConnectorJars.bytesFor(sourceConnector));
            BenchmarkReturnWorkBudget.connectorBytes(scope, BenchmarkReturnWorkBudget.Activity.CONTROL_RPC, sourceJar);
            BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.CONTROL_RPC,
                    () -> { control.registerConnector(sourceConnector, sourceJar); return null; });
            byte[] targetJar = BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                    () -> ConnectorJars.bytesFor("mongodb"));
            BenchmarkReturnWorkBudget.connectorBytes(scope, BenchmarkReturnWorkBudget.Activity.CONTROL_RPC, targetJar);
            BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.CONTROL_RPC,
                    () -> { control.registerConnector("mongodb", targetJar); return null; });
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
            BenchmarkReturnWorkBudget.requireLocalMaterial(scope, BenchmarkReturnWorkBudget.Activity.CONTROL_RPC, sourceResources);
            BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.CONTROL_RPC, () -> { control.apply(sourceResources); return null; });
            Map<String, Object> connectorConfig = workload.connectorConfig(sourceSettings);
            for (BenchmarkWorkloadDefinitions.SourceChain chain : workload.sourceChains()) {
                BenchmarkReturnWorkBudget.requireLocalMaterial(scope, BenchmarkReturnWorkBudget.Activity.CONTROL_RPC, connectorConfig);
                BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.CONTROL_RPC,
                        () -> { control.discoverSchema(chain.sourceId(), sourceConnector, connectorConfig); return null; });
            }
            // Apply validates references within the submitted workspace, so the discovered sources
            // travel with their dependents again even though their content is unchanged.
            BenchmarkReturnWorkBudget.requireLocalMaterial(scope, BenchmarkReturnWorkBudget.Activity.CONTROL_RPC, resources);
            BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.CONTROL_RPC, () -> { control.apply(resources); return null; });
            Objects.requireNonNull(beforeStart, "beforeStart").accept(boot);
            for (String pipeline : workload.pipelineIds()) {
                BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.CONTROL_RPC,
                        () -> { control.lifecycle(pipeline, LifecycleVerb.START); return null; });
            }
            for (String pipeline : workload.pipelineIds()) {
                awaitRunning(control, pipeline);
            }
            mongo = BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, () -> MongoClients.create(storeUri));
            BenchmarkReturnWorkBudget.done(opening);
            return new BenchmarkForkEnvironment(workload, forkId, sourceSettings, storeUri,
                    externalTargetUri, managedViewsUri, operatorStateUri, boot, ownedProcessReceipt,
                    control, source, mongo, postgresFixture, bootClosing);
        } catch (Exception | Error failure) {
            BenchmarkReturnWorkBudget.failed(opening, failure);
            if (ownedProcessReceipt != null) {
                try { ownedProcessReceipt.verifyBeforeClose(); }
                catch (Exception | Error recording) { if (recording != failure) { failure.addSuppressed(recording); } }
            }
            closeAfterFailure(bootClosing, failure);
            closeAfterFailure(mongo, failure);
            closeAfterFailure(source, failure);
            closeAfterFailure(postgresFixture, failure);
            if (ownedProcessReceipt != null) {
                try {
                    ownedProcessReceipt.finishAfterClose();
                    System.out.println("benchmark-owned-boot-at-failure=" + JsonWriter.write(ownedProcessReceipt.evidence()));
                } catch (Exception | Error recording) { if (recording != failure) { failure.addSuppressed(recording); } }
            }
            throw failure;
        }
    }

    private static void closeAfterFailure(AutoCloseable resource, Throwable failure) {
        if (resource == null) { return; }
        try { BenchmarkReturnWorkBudget.external(BenchmarkReturnWorkBudget.current(), BenchmarkReturnWorkBudget.Activity.CLEANUP, () -> { resource.close(); return null; }); }
        catch (Exception | Error cleanupFailure) {
            if (cleanupFailure != failure) { failure.addSuppressed(cleanupFailure); }
        }
    }

    /** Ends only the owned application, retaining this fork's live SQL and Mongo fixtures. */
    void closeBoot() throws Exception {
        BenchmarkReturnWorkBudget.external(workScope, BenchmarkReturnWorkBudget.Activity.CLEANUP, () -> { boot.close(); return null; });
    }

    /**
     * Stops every pipeline after the measured, ACK and oracle windows have closed, so the product closes its
     * sink writers before the owned server shuts down instead of racing the producer's bounded final seal.
     */
    void stopPipelines() {
        requireOpen();
        stopAll(control, workload.pipelineIds(), PIPELINE_WAIT);
    }

    static void stopAll(ControlPlane control, List<String> pipelines, Duration wait) {
        for (String pipeline : pipelines) {
            BenchmarkReturnWorkBudget.opaqueRequest(BenchmarkReturnWorkBudget.current(), BenchmarkReturnWorkBudget.Activity.CONTROL_RPC,
                    () -> { control.stop(pipeline, false); return null; });
        }
        for (String pipeline : pipelines) {
            Await.until(pipeline + " to stop", wait,
                    () -> BenchmarkReturnWorkBudget.opaqueRequest(BenchmarkReturnWorkBudget.current(), BenchmarkReturnWorkBudget.Activity.CONTROL_RPC,
                            () -> control.state(pipeline)).filter(PipelineState.STOPPED::equals).isPresent(),
                    () -> String.valueOf(control.state(pipeline)));
        }
    }

    private static void awaitRunning(ControlPlane control, String pipeline) {
        Await.until(pipeline + " to run", PIPELINE_WAIT,
                () -> BenchmarkReturnWorkBudget.opaqueRequest(BenchmarkReturnWorkBudget.current(), BenchmarkReturnWorkBudget.Activity.CONTROL_RPC,
                        () -> control.state(pipeline)).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(pipeline)));
    }

    BenchmarkWorkloadDefinitions.Workload workload() {
        return workload;
    }

    String forkId() {
        return forkId;
    }

    /** A real external reader without a terminal receipt keeps the same PostgreSQL database owned. */
    void registerUnconfirmedExternalPostgresBorrower(String label) {
        if (workload.database() != BenchmarkWorkloadDefinitions.Database.MYSQL) {
            postgresFixture.pending("external PostgreSQL borrower: " + Objects.requireNonNull(label));
        }
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

    BenchmarkOwnedProcessReceipt ownedProcessReceipt() { return ownedProcessReceipt; }

    void beginTelemetryCapture() throws Exception { boot.begin(); }
    void cutoffTelemetryCapture() throws Exception { boot.cutoff(); }
    Optional<BenchmarkJdiTelemetrySession.Evidence> finishTelemetryCapture() throws Exception { return boot.finish(); }

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
        executeOneSourceUpdate(source, sql);
    }

    static void executeOneSourceUpdate(Connection source, String sql) throws Exception {
        if (BenchmarkReturnWorkBudget.current() == null) {
            try (Statement statement = source.createStatement()) {
                int affected = statement.executeUpdate(sql);
                if (affected != 1) throw new AssertionError("benchmark boundary update affected " + affected + " rows");
            }
            return;
        }
        BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                BenchmarkSharedSupportSourceWork.Kind.SQL_UPDATE, () -> {
                    BenchmarkSharedSupportSourceWork.scalar(sql, BenchmarkSharedSupportSourceWork.TEXT);
                    var scope = BenchmarkReturnWorkBudget.current();
                    Statement statement = BenchmarkReturnWorkBudget.external(scope,
                            BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, source::createStatement);
                    Throwable primary = null;
                    try {
                        int affected = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                                () -> statement.executeUpdate(sql));
                        if (affected != 1) throw new AssertionError("benchmark boundary update affected " + affected + " rows");
                    } catch (Exception | Error failure) { primary = failure; throw failure; }
                    finally { closeSqlStatement(statement, primary); }
                    return null;
                });
    }

    /** Recheck the last completed phase after the unmeasured source boundary has been ACKed. */
    List<TargetResult> verifyCurrentTargets(BenchmarkWorkloadDefinitions.Phase phase) throws Exception {
        requireOpen();
        if (nextPhase == 0 || !workload.phases().get(nextPhase - 1).equals(phase)) {
            throw new IllegalArgumentException("target verification requires the last completed phase");
        }
        return awaitTargets(phase);
    }

    /** Issues the next phase without charging target-oracle scans to its ACK timing window. */
    PhaseIssue issuePhase(BenchmarkWorkloadDefinitions.Phase phase, boolean paced,
            BatchHook beforeBatch) throws Exception {
        requireOpen();
        Objects.requireNonNull(phase, "phase");
        Objects.requireNonNull(beforeBatch, "beforeBatch");
        if (pendingPhase != null || nextPhase >= workload.phases().size()
                || !workload.phases().get(nextPhase).equals(phase)) {
            throw new IllegalArgumentException("benchmark phases must run in declared order");
        }
        BenchmarkSharedSupportSourceWork.phaseSql(phase);
        ClockAnchor clockAnchor = ClockAnchor.capture();
        long started = BenchmarkReturnWorkBudget.rootNanoTime();
        long nextBatchStart = started, grid = started;
        List<BatchResult> batches = new ArrayList<>();
        int batchIndex = 0;
        for (List<String> sql : phase.batches()) {
            BenchmarkReturnWorkBudget.add(workScope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                    new BenchmarkReturnWorkBudget.Work(1, 0, 0, 0, 0, 1, sql.size(), 0));
            if (paced) {
                long remaining = nextBatchStart - BenchmarkReturnWorkBudget.rootNanoTime();
                if (remaining > 0) {
                    TimeUnit.NANOSECONDS.sleep(remaining);
                }
            }
            long issuedAt = BenchmarkReturnWorkBudget.rootNanoTime();
            beforeBatch.beforeBatch(phase, batchIndex, issuedAt, sql);
            execute(source, sql);
            long completedAt = BenchmarkReturnWorkBudget.rootNanoTime();
            batches.add(new BatchResult(batchIndex, issuedAt, completedAt));
            grid = Math.addExact(grid, phase.batchInterval().toNanos());
            nextBatchStart = nextBatchStart(grid, issuedAt, phase.batchInterval().toNanos());
            batchIndex++;
        }
        pendingPhase = phase;
        return new PhaseIssue(phase, started, BenchmarkReturnWorkBudget.rootNanoTime(), batches, clockAnchor);
    }

    /**
     * Holds the declared offer on a fixed grid so neither timer overshoot nor a slow statement lowers the average
     * rate. A batch behind its grid point follows the previous one after four fifths of an interval, so lost time
     * is repaid at no more than 1.25 times the declared rate, never in a burst.
     */
    static long nextBatchStart(long grid, long issuedAt, long interval) {
        return Math.max(grid, Math.addExact(issuedAt, interval - interval / 5));
    }

    /** Verifies final target content after the external ACK window has ended. */
    List<TargetResult> completePhase(BenchmarkWorkloadDefinitions.Phase phase) throws Exception {
        requireOpen();
        if (pendingPhase == null || !pendingPhase.equals(phase)) {
            throw new IllegalArgumentException("benchmark phase was not issued");
        }
        List<TargetResult> targets = awaitTargets(phase);
        pendingPhase = null;
        nextPhase++;
        return targets;
    }

    /** Runs a correctness phase and waits for its frozen target answer. */
    PhaseResult runPhase(BenchmarkWorkloadDefinitions.Phase phase, boolean paced,
            BatchHook beforeBatch) throws Exception {
        PhaseIssue issued = issuePhase(phase, paced, beforeBatch);
        List<TargetResult> targets = completePhase(phase);
        return new PhaseResult(phase, issued.startedAtNanos(), BenchmarkReturnWorkBudget.rootNanoTime(), issued.batches(), targets);
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

    /** Only larger non-measured setup scales with its fixed source population. */
    static Duration targetWait(BenchmarkWorkloadDefinitions.Workload workload,
            BenchmarkWorkloadDefinitions.Phase phase) {
        if (phase.measured() || (phase.stage() != BenchmarkWorkloadDefinitions.Stage.SNAPSHOT
                && phase.stage() != BenchmarkWorkloadDefinitions.Stage.WARM_UP)) {
            return TARGET_WAIT;
        }
        long populations = Math.max(1L, (workload.rows() + BenchmarkWorkloadDefinitions.SNAPSHOT_ROWS - 1L)
                / BenchmarkWorkloadDefinitions.SNAPSHOT_ROWS);
        return TARGET_WAIT.multipliedBy(populations);
    }

    private List<TargetResult> awaitTargets(BenchmarkWorkloadDefinitions.Phase phase) throws Exception {
        Duration budget = targetWait(workload, phase);
        long started = BenchmarkReturnWorkBudget.rootNanoTime();
        long deadline = started + budget.toNanos();
        int rounds = 0;
        boolean matched = false;
        List<TargetResult> latest;
        try {
            do {
                rounds++;
                latest = phase.targets().stream().map(this::readTarget).toList();
                BenchmarkReturnWorkBudget.add(workScope, BenchmarkReturnWorkBudget.Activity.ACK_POLL, BenchmarkReturnWorkBudget.Work.call());
                if (latest.stream().allMatch(TargetResult::matches)) {
                    matched = true;
                    return latest;
                }
                if (BenchmarkReturnWorkBudget.rootNanoTime() >= deadline) {
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
        } finally {
            if (!phase.measured() && (phase.stage() == BenchmarkWorkloadDefinitions.Stage.SNAPSHOT
                    || phase.stage() == BenchmarkWorkloadDefinitions.Stage.WARM_UP)) {
                // This is a polling budget between complete reads, not a server command timeout.
                System.out.println("benchmark-setup-target-wait=" + io.tapstate.core.common.JsonWriter.write(Map.of(
                        "fork", forkId, "phase", phase.id(), "rows", workload.rows(),
                        "pollingBudgetNanos", budget.toNanos(), "elapsedNanos", BenchmarkReturnWorkBudget.rootNanoTime() - started,
                        "targetReadRounds", rounds, "matched", matched,
                        "performanceAcceptanceEligible", false)));
            }
        }
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
        BenchmarkReturnWorkBudget.gap(workScope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, "NEST_COLD_DIAGNOSTIC_CURSOR_PRIOR_CARDINALITY_UNBOUND");
        for (Document root : collection.find().maxTime(15, TimeUnit.SECONDS)) {
            BenchmarkReturnWorkBudget.requireLocalMaterial(workScope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, root);
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
            var scope = BenchmarkReturnWorkBudget.current();
            BenchmarkReturnWorkBudget.requireLocalMaterial(scope, BenchmarkReturnWorkBudget.Activity.REPORT_SERIALIZE, root);
            examples.add(abbreviate(BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.REPORT_SERIALIZE, root::toJson), 4096));
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
        long count = BenchmarkReturnWorkBudget.request(workScope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, collection::countDocuments);
        if (count != expectation.rows()) {
            return new TargetResult(expectation, count, null);
        }
        var work = BenchmarkReturnWorkBudget.begin(workScope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, BenchmarkReturnWorkBudget.Work.call());
        List<Document> documents;
        try { BenchmarkReturnWorkBudget.issued(work); documents = collection.find().into(new ArrayList<>()); BenchmarkReturnWorkBudget.done(work); }
        catch (RuntimeException | Error failure) { BenchmarkReturnWorkBudget.failed(work, failure); throw failure; }
        if (workScope != null) {
            if (documents.size() > expectation.rows()) { BenchmarkReturnWorkBudget.gap(workScope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, "TARGET_CARDINALITY_PRIOR_BOUND_EXCEEDED"); }
            for (Document document : documents) { BenchmarkReturnWorkBudget.requireLocalMaterial(workScope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, document); }
            BenchmarkReturnWorkBudget.add(workScope, BenchmarkReturnWorkBudget.Activity.ASSOCIATION, BenchmarkReturnWorkBudget.Work.records(documents.size()));
        }
        try {
            return new TargetResult(expectation, documents.size(),
                    BenchmarkWorkloadDefinitions.checksumOf(expectation, documents));
        } catch (RuntimeException | AssertionError incomplete) {
            return new TargetResult(expectation, documents.size(), "UNREADABLE: " + incomplete);
        }
    }

    static void execute(Connection connection, List<String> sql) throws Exception {
        BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                BenchmarkSharedSupportSourceWork.Kind.SQL_PARENT, () -> {
                    executeBounded(connection, sql);
                    return null;
                });
    }

    private static void executeBounded(Connection connection, List<String> sql) throws Exception {
        var scope = BenchmarkReturnWorkBudget.current();
        // Fixed 193/194/581 setup and one/two-statement batches. Reject prior shape before the
        // first traversal or provider request; scalar guards also precede createStatement.
        BenchmarkSharedSupportSourceWork.sqlRoster(sql, BenchmarkSharedSupportSourceWork.SQL_STATEMENTS);
        if (sql.isEmpty()) return;
        Statement statement = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, connection::createStatement);
        Throwable primary = null;
        try {
            for (String command : sql) {
                BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                        BenchmarkSharedSupportSourceWork.Kind.SQL_COMMAND, () -> {
                            BenchmarkReturnWorkBudget.requireLocalMaterial(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, command);
                            BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, () -> statement.execute(command));
                            return null;
                        });
            }
        } catch (Exception | Error failure) { primary = failure; throw failure; }
        finally { closeSqlStatement(statement, primary); }
    }

    private static void closeSqlStatement(Statement statement, Throwable primary) throws Exception {
        try {
            BenchmarkReturnWorkBudget.external(BenchmarkReturnWorkBudget.current(), BenchmarkReturnWorkBudget.Activity.CLEANUP,
                    () -> { statement.close(); return null; });
        } catch (Exception | Error closeFailure) {
            if (primary == null) throw closeFailure;
            if (closeFailure != primary) primary.addSuppressed(closeFailure);
        }
    }

    /** The unscoped method keeps the original workload route; only the explicit budget path uses
     * the same target/state reset sequence with complete child and close accounting. */
    static void resetTargets(BenchmarkWorkloadDefinitions.Workload workload, String externalTargetUri,
            String managedViewsUri, String operatorStateUri) {
        if (BenchmarkReturnWorkBudget.current() == null) {
            workload.resetTargets(externalTargetUri, managedViewsUri, operatorStateUri); return;
        }
        resetTargetsUsing(workload, externalTargetUri, managedViewsUri, operatorStateUri, MongoClients::create);
    }

    static void resetTargetsUsing(BenchmarkWorkloadDefinitions.Workload workload, String externalTargetUri,
            String managedViewsUri, String operatorStateUri,
            java.util.function.Function<ConnectionString, MongoClient> clients) {
        if (BenchmarkReturnWorkBudget.current() == null) {
            workload.resetTargetsUsing(externalTargetUri, managedViewsUri, operatorStateUri, clients); return;
        }
        BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                BenchmarkSharedSupportSourceWork.Kind.TARGET_RESET, () -> {
                    BenchmarkSharedSupportSourceWork.originalWorkload(workload);
                    BenchmarkSharedSupportSourceWork.uri(externalTargetUri);
                    BenchmarkSharedSupportSourceWork.uri(managedViewsUri);
                    BenchmarkSharedSupportSourceWork.uri(operatorStateUri);
                    for (var target : workload.targetResets()) {
                        BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                                BenchmarkSharedSupportSourceWork.Kind.TARGET_RESET_ROW, () -> {
                                    String uri = target.location() == BenchmarkWorkloadDefinitions.TargetLocation.MANAGED_VIEW
                                            ? managedViewsUri : externalTargetUri;
                                    resetTarget(target, uri, clients); return null;
                                });
                    }
                    if (workload.id().equals("stateful")) {
                        BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                                BenchmarkSharedSupportSourceWork.Kind.STATE_RESET, () -> {
                                    resetState(operatorStateUri, clients); return null;
                                });
                    }
                    return null;
                });
    }

    private static void resetTarget(BenchmarkWorkloadDefinitions.TargetExpectation target, String uri,
            java.util.function.Function<ConnectionString, MongoClient> clients) {
        BenchmarkSharedSupportSourceWork.scalar(target.table(), BenchmarkSharedSupportSourceWork.NAME);
        var scope = BenchmarkReturnWorkBudget.current();
        ConnectionString address = BenchmarkReturnWorkBudget.opaqueRequest(scope,
                BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, () -> new ConnectionString(uri));
        String database = Objects.requireNonNull(BenchmarkReturnWorkBudget.opaqueRequest(scope,
                BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, address::getDatabase), "target URI has no database");
        BenchmarkSharedSupportSourceWork.scalar(database, BenchmarkSharedSupportSourceWork.URI);
        MongoClient client = BenchmarkReturnWorkBudget.opaqueRequest(scope,
                BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, () -> clients.apply(address));
        Throwable primary = null;
        try {
            var actualDatabase = BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                    () -> client.getDatabase(database));
            var collection = BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                    () -> actualDatabase.getCollection(target.table()));
            BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                    () -> { collection.drop(); return null; });
        } catch (RuntimeException | Error failure) { primary = failure; throw failure; }
        finally { closeResetClient(client, primary); }
    }

    private static void resetState(String uri, java.util.function.Function<ConnectionString, MongoClient> clients) {
        var scope = BenchmarkReturnWorkBudget.current();
        ConnectionString address = BenchmarkReturnWorkBudget.opaqueRequest(scope,
                BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, () -> new ConnectionString(uri));
        String database = Objects.requireNonNull(BenchmarkReturnWorkBudget.opaqueRequest(scope,
                BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, address::getDatabase), "state URI has no database");
        BenchmarkSharedSupportSourceWork.scalar(database, BenchmarkSharedSupportSourceWork.URI);
        MongoClient client = BenchmarkReturnWorkBudget.opaqueRequest(scope,
                BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, () -> clients.apply(address));
        Throwable primary = null;
        try {
            var actualDatabase = BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                    () -> client.getDatabase(database));
            BenchmarkReturnWorkBudget.opaqueRequest(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                    () -> { actualDatabase.drop(); return null; });
        } catch (RuntimeException | Error failure) { primary = failure; throw failure; }
        finally { closeResetClient(client, primary); }
    }

    private static void closeResetClient(MongoClient client, Throwable primary) {
        try {
            BenchmarkReturnWorkBudget.opaqueRequest(BenchmarkReturnWorkBudget.current(), BenchmarkReturnWorkBudget.Activity.CLEANUP,
                    () -> { client.close(); return null; });
        } catch (RuntimeException | Error closeFailure) {
            if (primary == null) throw closeFailure;
            if (closeFailure != primary) primary.addSuppressed(closeFailure);
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("benchmark fork is closed");
        }
    }

    @Override
    public void close() throws Exception {
        BenchmarkReturnWorkBudget.add(workScope, BenchmarkReturnWorkBudget.Activity.CLEANUP, BenchmarkReturnWorkBudget.Work.call());
        if (closed) {
            return;
        }
        closed = true;
        Throwable primary = null;
        for (AutoCloseable resource : List.of(bootClosing, source, mongo, postgresFixture)) {
            try { BenchmarkReturnWorkBudget.external(BenchmarkReturnWorkBudget.current(), BenchmarkReturnWorkBudget.Activity.CLEANUP, () -> { resource.close(); return null; }); }
            catch (Exception | Error failure) {
                if (primary == null) { primary = failure; } else if (primary != failure) { primary.addSuppressed(failure); }
            }
        }
        if (primary instanceof Exception failure) { throw failure; }
        if (primary instanceof Error failure) { throw failure; }
    }
}
