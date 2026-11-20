package io.tapstate.e2e;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator;
import io.tapstate.e2e.benchmark.WorkloadRecipe;
import io.tapstate.testsupport.RequiresDocker;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/** Executes an explicit, version-bound benchmark request with real OP processes and locked connectors. */
@RequiresDocker
@EnabledIfSystemProperty(named = "tapstate.benchmarks.request", matches = ".+")
class BenchmarkRunIT {
    private static final String PIPELINE = "perf_copy";
    private static final String RECORDS = "records";
    private static final String HANDOFF = "copy.live.handoff.standard";
    private static final int HANDOFF_WRITE_HOLD_MILLIS = 5_000;
    private static final String DUMMY = "dummy_orders";
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void executeRequestedBatch() throws Exception {
        var request = BsonDocument.parse(Files.readString(Path.of(System.getProperty("tapstate.benchmarks.request"))));
        Path output = Path.of(text(request, "output"));
        Path connectors = Path.of(text(request, "connector_dir"));
        var lockPath = checkout().resolve("deploy/cloud/connectors.lock.json");
        var lock = BsonDocument.parse(Files.readString(lockPath));
        Map<String, Path> artifacts = new HashMap<>();
        var failures = new ArrayList<String>();
        for (var value : request.getArray("entries")) {
            var entry = value.asDocument();
            String connector = text(entry.getDocument("case"), "source");
            if (!artifacts.containsKey(connector)) {
                artifacts.put(connector, verified(lock, connectors, connector));
            }
        }
        for (var value : request.getArray("entries")) {
            var entry = value.asDocument();
            var definition = entry.getDocument("case");
            String id = text(definition, "id");
            Path artifact = artifacts.get(text(definition, "source"));
            for (int repeat = 1; repeat <= number(entry, "repeats"); repeat++) {
                Path folder = Files.createDirectories(output.resolve(id).resolve(Integer.toString(repeat)));
                var result = run(request, entry, artifact, repeat, folder);
                Path pending = folder.resolve("result.pending.json");
                JSON.writerWithDefaultPrettyPrinter().writeValue(pending.toFile(), result);
                Files.move(pending, folder.resolve("result.json"), StandardCopyOption.ATOMIC_MOVE);
                if (!"completed".equals(result.get("status"))) {
                    failures.add(id + " repeat " + repeat + " " + result.get("reason"));
                }
            }
        }
        if (!failures.isEmpty()) {
            throw new AssertionError(String.join(System.lineSeparator(), failures));
        }
    }

    private static Map<String, Object> run(BsonDocument request, BsonDocument entry, Path artifact, int repeat, Path folder)
            throws Exception {
        var definition = entry.getDocument("case");
        String mode = text(definition, "mode");
        String connector = text(definition, "source");
        var evidence = new Evidence(request, definition, repeat, mode, artifact, entry);
        long start = System.nanoTime();
        evidence.result.put("started_epoch_ms", System.currentTimeMillis());
        try {
            String scenario = definition.getString("spec").getValue();
            boolean slowTarget = scenario.equals("specs/copy-slow-target.json");
            BenchmarkOperators operators = text(definition, "group").equals("copy")
                    ? null : new BenchmarkOperators(definition, number(entry, "rows"), false);
            if (connector.equals("dummy")) {
                evidence.stage = "snapshot";
                runDummy(entry, artifact, folder, evidence, false);
                evidence.stage = "content-witness";
                runDummy(entry, artifact, folder, evidence, true);
            } else if (slowTarget || operators != null && operators.paired()) {
                var arms = new LinkedHashMap<String, Map<String, Object>>();
                evidence.result.put("paired_arms", arms);
                evidence.stage = "paired-operator";
                var failures = new ArrayList<String>();
                boolean fanout = operators != null && operators.fanout != null;
                var names = slowTarget ? List.of("unlimited", "limited")
                        : fanout ? List.of("ordinary", "high-fanout") : List.of("uniform", "hot");
                var baseLengths = new LinkedHashMap<String, Integer>();
                for (String arm : names) {
                    var armEvidence = new Evidence(request, definition, repeat, mode, artifact, entry);
                    arms.put(arm, armEvidence.result);
                    long armStart = System.nanoTime();
                    armEvidence.result.put("started_epoch_ms", System.currentTimeMillis());
                    try {
                        runMongo(entry, definition, artifact, Files.createDirectory(folder.resolve(arm)), armEvidence,
                                arm.equals("high-fanout")
                                        ? new BenchmarkOperators(definition, number(entry, "rows"), true) : operators,
                                arm.equals("hot") ? new MongoWorkloadGenerator.HotKeys(25, 70) : null,
                                baseLengths, slowTarget, arm.equals("limited"));
                        armEvidence.result.put("status", "completed");
                    } catch (RuntimeException | AssertionError | IOException failure) {
                        recordFailure(armEvidence, failure);
                        failures.add(arm + " at " + armEvidence.stage + " " + failure.getMessage());
                    }
                    armEvidence.result.put("last_stage", armEvidence.stage);
                    armEvidence.result.put("total_nanos", System.nanoTime() - armStart);
                    armEvidence.result.put("finished_epoch_ms", System.currentTimeMillis());
                }
                if (!failures.isEmpty()) {
                    throw new Unverified(String.join(System.lineSeparator(), failures));
                }
                if (slowTarget) {
                    verifyBackpressure(arms.get("unlimited"), arms.get("limited"), evidence);
                }
            } else {
                runMongo(entry, definition, artifact, folder, evidence, operators, null, new LinkedHashMap<>(), false, false);
            }
            evidence.result.put("status", "completed");
        } catch (RuntimeException | AssertionError | IOException | URISyntaxException failure) {
            recordFailure(evidence, failure);
        }
        evidence.result.put("last_stage", evidence.stage);
        evidence.result.put("total_nanos", System.nanoTime() - start);
        evidence.result.put("finished_epoch_ms", System.currentTimeMillis());
        return evidence.result;
    }

    private static void verifyBackpressure(Map<String, Object> unlimited, Map<String, Object> limited, Evidence evidence) {
        long unrestricted = (long) unlimited.get("target_majority_visible_before_drain");
        long restricted = (long) limited.get("target_majority_visible_before_drain");
        var sourceUnlimited = JSON.valueToTree(unlimited.get("source_window"));
        var sourceLimited = JSON.valueToTree(limited.get("source_window"));
        var first = sourceUnlimited.path(RECORDS);
        var second = sourceLimited.path(RECORDS);
        if (!first.path("sequence_sha256").equals(second.path("sequence_sha256"))
                || !first.path("acknowledged").equals(second.path("acknowledged"))
                || restricted >= unrestricted) {
            throw new Unverified("The matched slow-target pair did not show reduced target confirmations under identical input");
        }
        evidence.result.put("backpressure_pair", Map.of("same_source_sequence", true,
                "unlimited_target_majority_visible_at_drain", unrestricted, "limited_target_majority_visible_at_drain", restricted,
                "source_rate_unlimited", sourceUnlimited.path("acknowledged_per_second"),
                "source_rate_limited", sourceLimited.path("acknowledged_per_second")));
    }

    private static void recordFailure(Evidence evidence, Throwable failure) {
        if (failure instanceof Limit) {
            evidence.result.put("status", "timed-out");
        } else if (failure instanceof Unverified) {
            evidence.result.put("status", "measurement-incomplete");
        } else {
            evidence.result.put("status", "failed");
        }
        evidence.result.put("reason", failure.toString());
        if (failure.getSuppressed().length > 0) {
            evidence.result.put("secondary_failures", java.util.Arrays.stream(failure.getSuppressed())
                    .map(Throwable::toString).toList());
        }
    }

    private static void runDummy(BsonDocument entry, Path artifact, Path folder, Evidence evidence, boolean diagnostic)
            throws IOException, URISyntaxException {
        Path seeds = Files.createDirectory(folder.resolve(diagnostic ? "witness-input" : "runtime-input"));
        Path witness = folder.resolve("dummy-content.tsv");
        Path jar = seeds.resolve("dummy-connector.jar");
        if (diagnostic) {
            Files.write(jar, ObservedDummyConnectorJar.build(Files.readAllBytes(artifact), witness));
        } else {
            Files.copy(artifact, jar);
        }
        int rows = number(entry, "rows");
        long startup = System.nanoTime();
        try (var storage = PreparedNativeStore.create();
                var server = RealProcessServer.startPrepared(storage.configurationUri(), "127.0.0.1",
                        ignored -> List.of("--tapstate.connectors.seed-dir=" + seeds))) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            try (var peaks = new BenchmarkResources(server.pid())) {
                var src = dummyConfig(rows);
                var tgt = dummyConfig(1);
                var workspace = new LinkedHashMap<>(Map.of("source.tap.yml", resource("src_perf", "dummy", src, DUMMY),
                        "target.tap.yml", resource("tgt_perf", "dummy", tgt, null)));
                control.apply(workspace);
                discover(control, "src_perf", "dummy", src);
                discover(control, "tgt_perf", "dummy", tgt);
                workspace.put("pipeline.tap.yml", pipeline("snapshot_only", "clear"));
                control.apply(workspace);
                if (!diagnostic) {
                    evidence.phase("startup", startup);
                }
                long measurement = System.nanoTime();
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                waitFor(entry, "Dummy snapshot completion",
                        () -> snapshotLanded(control, DUMMY, rows));
                if (diagnostic) {
                    verifyDummy(witness, rows);
                    evidence.result.put("content_witness", Map.of("rows", rows, "file", witness.getFileName().toString(),
                            "scope", "separate-observer-run"));
                    evidence.phase("content_witness", startup);
                } else {
                    evidence.measure(measurement, rows, rows);
                    evidence.result.put("snapshot_landed", control.initialLoad(PIPELINE).orElseThrow());
                    evidence.result.put("runtime_metrics", control.metrics(PIPELINE));
                    evidence.result.put("op_rss_peak_kib", peaks.peakKiB());
                }
            } finally {
                saveProcess(server, folder.resolve(diagnostic ? "dummy-witness.log" : "op.log"));
            }
        }
    }

    private static void verifyDummy(Path witness, int count) throws IOException {
        var contents = new ArrayList<String>();
        long confirmed = 0;
        for (String line : Files.readAllLines(witness)) {
            String[] row = line.split("\\t", -1);
            if (row[0].equals("WRITE") && row[3].equals("I")) {
                contents.add(row[5]);
            } else if (row[0].equals("ACK")) {
                confirmed += Long.parseLong(row[1]);
            }
        }
        var expected = IntStream.rangeClosed(1, count)
                .mapToObj(key -> "{id=" + key + ", payload=fixed123}").sorted().toList();
        if (!contents.stream().sorted().toList().equals(expected) || confirmed != count) {
            throw new AssertionError("Dummy content or independent consumption acknowledgement differed");
        }
    }

    private static Map<String, Object> dummyConfig(int rows) {
        return Map.ofEntries(Map.entry("table_name", DUMMY),
                Map.entry("table_fields", List.of(Map.of("name", "id", "type", "serial(1,1)", "pri", true),
                        Map.of("name", "payload", "type", "string(8)", "def", "fixed123"))),
                Map.entry("initial_totals", rows), Map.entry("write_interval_totals", 0),
                Map.entry("write_log", false));
    }

    private static void runMongo(BsonDocument entry, BsonDocument definition, Path artifact, Path folder,
            Evidence evidence, BenchmarkOperators operators, MongoWorkloadGenerator.HotKeys hot,
            Map<String, Integer> baseLengths, boolean slowTarget, boolean limited) throws Exception {
        String namespace = "perf_" + UUID.randomUUID().toString().replace("-", "");
        String sourceName = namespace + "_src";
        String targetName = namespace + "_tgt";
        Path seeds = Files.createDirectory(folder.resolve("runtime-input"));
        Files.copy(artifact, seeds.resolve("mongodb-connector.jar"));
        boolean requiresTestCommands = slowTarget || text(definition, "id").equals(HANDOFF);
        try (var owned = requiresTestCommands ? BenchmarkTargetLimit.newReplicaSet() : null) {
            if (owned != null) {
                owned.start();
            }
            String sourceUri = owned == null ? SharedMongo.replicaSetUrl(sourceName) : owned.getReplicaSetUrl(sourceName);
            String targetUri = owned == null ? SharedMongo.replicaSetUrl(targetName) : owned.getReplicaSetUrl(targetName);
            evidence.result.put("mongo_topology", requiresTestCommands ? "owned shared replica set with test commands"
                    : "shared replica set");
            try (var client = MongoClients.create(sourceUri)) {
                var sourceDb = client.getDatabase(sourceName).withWriteConcern(WriteConcern.MAJORITY);
                var targetDb = client.getDatabase(targetName).withReadConcern(ReadConcern.MAJORITY);
                runMongo(entry, definition, evidence, folder, operators, hot, baseLengths, seeds, sourceUri, targetUri,
                        sourceDb, targetDb, client.getDatabase("admin"), limited);
            }
        }
    }

    private static void runMongo(BsonDocument entry, BsonDocument definition, Evidence evidence, Path folder,
            BenchmarkOperators operators, MongoWorkloadGenerator.HotKeys hot, Map<String, Integer> baseLengths,
            Path seeds, String sourceUri, String targetUri, MongoDatabase sourceDb, MongoDatabase targetDb,
            MongoDatabase admin, boolean limited) throws Exception {
        var memoryProfile = definition.getDocument("workload_spec").getDocument("memory_profile", null);
        boolean memory = memoryProfile != null;
        boolean steady = operators != null && operators.scenario.equals("join-steady-state");
        boolean lifecycle = operators != null && operators.scenario.equals("join-lifecycle-release");
        try (java.io.Closeable cleanup = () -> dropBenchmarkDatabases(sourceDb, targetDb, evidence)) {
            evidence.stage = "prepare";
            long preparing = System.nanoTime();
            var rowCounts = operators == null ? Map.of(RECORDS, number(entry, "rows")) : operators.rows;
            var recipe = WorkloadRecipe.fromCatalog(definition, rowCounts, operators == null ? null : operators.fanout);
            evidence.result.put("effective_input_rows", rowCounts);
            var generator = new MongoWorkloadGenerator(sourceDb, recipe, text(evidence.request, "run_id"), 32);
            if (baseLengths.isEmpty()) {
                baseLengths.putAll(generator.calibrateStringLengths());
            }
            var prepared = generator.prepare(baseLengths);
            evidence.result.put("source_data", prepared.preparation());
            var settings = prepared.preparation().source();
            evidence.result.put("database_conditions", Map.of("mongo_version", settings.mongoVersion(),
                    "replica_set", settings.replicaSet(), "write_concern", settings.writeConcern(),
                    "pre_images", settings.preImages(), "batch_size", settings.batchSize(),
                    "string_base_lengths", settings.stringBaseLengths()));
            evidence.phase("prepare", preparing);
            var source = sourceDb.getCollection(RECORDS, BsonDocument.class);
            var target = targetDb.getCollection(RECORDS, BsonDocument.class);
            long startup = System.nanoTime();
            evidence.stage = "startup";
            int heapLimit = memory ? number(memoryProfile, "op_heap_limit_mib") : 768;
            var jvmOptions = new ArrayList<>(List.of("-Xmx" + heapLimit + "m", "-XX:ActiveProcessorCount=2"));
            if (memory) {
                jvmOptions.add("-Xlog:gc:file=" + folder.resolve("gc.log") + ":timemillis,uptime,level,tags");
            }
            try (var storage = PreparedNativeStore.create()) {
                var server = RealProcessServer.startPreparedInJvm(storage.configurationUri(), jvmOptions,
                        ignored -> List.of("--tapstate.connectors.seed-dir=" + seeds));
                var log = folder.resolve("op.log");
                try (java.io.Closeable observeExit = () -> observeTeardown(server, log, memory, evidence);
                        java.io.Closeable copyLog = () -> saveProcess(server, log);
                        server) {
                    server.awaitReady();
                    var control = new ControlPlane(server.baseUrl());
                    storage.loginAndAwaitBusiness(control);
                    try (var peaks = memory ? new BenchmarkResources(server.pid(), folder.resolve("op-resource.jsonl"),
                            number(memoryProfile, "op_rss_sample_interval_ms"), number(memoryProfile, "minimum_samples_per_window"),
                            memoryProfile.getNumber("minimum_coverage_fraction").doubleValue())
                            : new BenchmarkResources(server.pid())) {
                        if (memory) {
                            evidence.resources = peaks;
                        }
                        var src = Map.<String, Object>of("uri", sourceUri, "preImage", true);
                        String targetApplication = targetDb.getName() + "_writer";
                        var tgt = Map.<String, Object>of("uri", targetUri + (targetUri.contains("?") ? "&" : "?")
                                + "w=majority&appName=" + targetApplication);
                        String input = operators == null ? RECORDS : String.join(", ", operators.sources());
                        var workspace = new LinkedHashMap<>(Map.of(
                                "source.tap.yml", resource("src_perf", "mongodb", src, input),
                                "target.tap.yml", resource("tgt_perf", "mongodb", tgt, null)));
                        control.apply(workspace);
                        discover(control, "src_perf", "mongodb", src);
                        boolean snapshot = text(definition, "mode").equals("snapshot") && !lifecycle;
                        String yaml = operators == null ? pipeline(snapshot ? "snapshot_only" : "snapshot_and_cdc", "clear")
                                : JSON.writerWithDefaultPrettyPrinter().writeValueAsString(operators.pipeline(PIPELINE, snapshot, sourceDb));
                        Files.writeString(folder.resolve("pipeline.tap.yml"), yaml);
                        workspace.put("pipeline.tap.yml", yaml);
                        control.apply(workspace);
                        evidence.phase("startup", startup);
                        try {
                            if (operators != null) {
                                operatorRun(entry, prepared, control, sourceDb, targetDb, operators, evidence, hot);
                            } else {
                                if (snapshot) {
                                    mongoSnapshot(entry, control, evidence);
                                } else {
                                    int delay = limited ? number(definition.getDocument("workload_spec")
                                            .getDocument("scenario").getDocument("parameters"),
                                            "trial_target_command_delay_ms") : 0;
                                    boolean slowTarget = definition.getString("spec").getValue().equals("specs/copy-slow-target.json");
                                    var handoff = text(definition, "id").equals(HANDOFF)
                                            ? new BenchmarkTargetLimit(admin, targetApplication,
                                                    target.getNamespace().getFullName(), HANDOFF_WRITE_HOLD_MILLIS)
                                            : null;
                                    try (handoff; var limit = limited ? new BenchmarkTargetLimit(admin, targetApplication,
                                            target.getNamespace().getFullName(), delay) : null;
                                            var window = slowTarget ? new BenchmarkTargetLimit.TargetWindow(targetDb, target) : null) {
                                        mongoCdc(entry, prepared, control, source, target, evidence, text(definition, "mode"),
                                                limit, delay, window, handoff);
                                    }
                                }
                                evidence.stage = "readback";
                                long verify = System.nanoTime();
                                evidence.result.put("readback_bson_type_differences", verifySame(source, target));
                                evidence.phase("readback", verify);
                                evidence.result.put("target_readback_rows", target.countDocuments());
                            }
                            if (memory && !lifecycle) {
                                evidence.window("idle");
                                observeIdleWindow(server, number(memoryProfile, "idle_seconds"));
                            }
                        } finally {
                            if (memory) {
                                var resourceReport = peaks.report(steady ? number(memoryProfile, "steady_windows") : 0);
                                resourceReport.put("op_heap_limit_mib", heapLimit);
                                evidence.result.put("resources", resourceReport);
                                evidence.diagnostic("target_bson_storage", () -> {
                                    var collections = operators == null ? List.of(RECORDS) : operators.targets();
                                    var sizes = new LinkedHashMap<String, Object>();
                                    for (var collection : collections) {
                                        var reading = targetDb.runCommand(new Document("collStats", collection));
                                        sizes.put(collection, Map.of("documents", reading.get("count", Number.class),
                                                "logical_bson_bytes", reading.get("size", Number.class)));
                                    }
                                    return sizes;
                                });
                            }
                            evidence.diagnostic("runtime_metrics", () -> control.metrics(PIPELINE));
                            evidence.diagnostic("runtime_position", () -> control.positionReading(PIPELINE));
                            evidence.result.put("op_rss_peak_kib", peaks.peakKiB());
                            evidence.diagnostic("mongo_shared_instance_resident_mib", () ->
                                    sourceDb.runCommand(new org.bson.Document("serverStatus", 1))
                                            .get("mem", org.bson.Document.class).getInteger("resident"));
                        }
                    }
                }
            }
        }
    }

    private static void observeTeardown(RealProcessServer server, Path log, boolean memory, Evidence evidence)
            throws IOException {
        boolean unexpected = server.exitedBeforeCloseRequest();
        evidence.result.put("op_alive_before_teardown", !unexpected);
        evidence.diagnostic("op_exit_code", server::exitValue);
        if (memory && Files.isRegularFile(log)) {
            try (var lines = Files.lines(log)) {
                if (lines.anyMatch(line -> line.stripLeading().startsWith("java.lang.OutOfMemoryError:")
                        || line.contains("Caused by: java.lang.OutOfMemoryError:"))) {
                    evidence.result.put("exit_attribution", "observed-java-out-of-memory");
                    throw new AssertionError("OP reported OutOfMemoryError");
                }
            }
        }
        if (unexpected) {
            evidence.result.put("exit_attribution", "unattributed-process-exit");
            throw new Unverified("OP exited before teardown");
        }
    }

    private static void dropBenchmarkDatabases(MongoDatabase source, MongoDatabase target, Evidence evidence)
            throws IOException {
        evidence.stageBeforeCleanup();
        long cleanup = System.nanoTime();
        try (java.io.Closeable dropTarget = () -> target.withTimeout(10, java.util.concurrent.TimeUnit.SECONDS).drop()) {
            source.withTimeout(10, java.util.concurrent.TimeUnit.SECONDS).drop();
        } finally {
            evidence.phase("cleanup", cleanup);
        }
    }

    private static void mongoSnapshot(BsonDocument entry, ControlPlane control, Evidence evidence) {
        evidence.stage = "snapshot";
        evidence.window("snapshot");
        int rows = number(entry, "rows");
        long start = System.nanoTime();
        control.lifecycle(PIPELINE, LifecycleVerb.START);
        waitFor(entry, "the MongoDB snapshot and settled writes",
                () -> snapshotLanded(control, RECORDS, rows));
        evidence.measure(start, rows, rows);
        evidence.result.put("snapshot_landed", control.initialLoad(PIPELINE).orElseThrow());
        evidence.window("readback");
    }

    private static boolean snapshotLanded(ControlPlane control, String table, int rows) {
        return control.state(PIPELINE).filter(PipelineState.COMPLETED::equals).isPresent()
                && control.snapshotRowsRead(PIPELINE).getOrDefault(table, 0L) == rows
                && control.initialLoad(PIPELINE).filter(load -> "COMPLETED".equals(load.get("phase"))
                        && load.get("landedTables") instanceof Number landed && landed.longValue() == 1).isPresent();
    }

    private static void mongoCdc(BsonDocument entry, MongoWorkloadGenerator.Prepared prepared, ControlPlane control,
            MongoCollection<BsonDocument> source, MongoCollection<BsonDocument> target, Evidence evidence, String mode,
            BenchmarkTargetLimit limit, int delay, BenchmarkTargetLimit.TargetWindow targetWindow,
            BenchmarkTargetLimit handoffLimit) {
        evidence.stage = "initial-state";
        evidence.window("load");
        int initial = number(entry, "rows");
        long prepareState = System.nanoTime();
        boolean handoff = handoffLimit != null;
        String preAnchorToken;
        long preAnchorSettled;
        var anchorValue = new BsonString(handoff ? "perf-handoff" : "perf-anchor");
        var key = new BsonDocument("_id", new BsonInt64(1));
        if (handoff) {
            changeAtSnapshotHandoff(entry, source, target, control, handoffLimit, initial, key, anchorValue, evidence);
            preAnchorToken = null;
            preAnchorSettled = initial;
        } else {
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            waitFor(entry, "settled snapshot state at the target", () -> target.countDocuments() == initial
                    && control.snapshotRowsRead(PIPELINE).getOrDefault(RECORDS, 0L) == initial
                    && settled(control) >= initial && control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent());
            verifySame(source, target);
            preAnchorToken = control.resumePoint(PIPELINE).get("token");
            preAnchorSettled = settled(control);
            writeAnchor(source, key, anchorValue);
        }
        waitFor(entry, "confirmed initial-state anchor", () -> {
            var row = target.find(key).first();
            String current = control.resumePoint(PIPELINE).get("token");
            long changes = capturedChanges(control).getOrDefault(RECORDS, 0L);
            long acknowledged = settled(control);
            return row != null && anchorValue.equals(row.get("s0"))
                    && acknowledged > preAnchorSettled && changes > 0 && acknowledged == initial + changes
                    && (!handoff || control.snapshotRowsRead(PIPELINE).getOrDefault(RECORDS, 0L) == initial
                            && control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent())
                    && current != null && !current.equals(preAnchorToken);
        });
        verifySame(source, target);
        evidence.result.put("initial_settled_target_writes", settled(control));
        evidence.result.put("initial_captured_changes_including_anchor", capturedChanges(control).getOrDefault(RECORDS, 0L));
        evidence.phase("initial_state", prepareState);
        var mix = mix(entry);
        if (mode.equals("backlog-throughput")) {
            var resume = control.resumePoint(PIPELINE);
            control.stop(PIPELINE, false);
            waitFor(entry, "pipeline stopped before prewrite",
                    () -> control.state(PIPELINE).filter(PipelineState.STOPPED::equals).isPresent());
            var retained = target.find().sort(Sorts.ascending("_id")).into(new ArrayList<>());
            evidence.stage = "prewrite";
            long prewrite = System.nanoTime();
            var backlog = prepared.prewrite(number(entry, "backlog_events"), mix, null);
            evidence.result.put("source_batch", phase(backlog.batch()));
            evidence.phase("prewrite", prewrite);
            if (!backlog.batch().complete()) {
                throw new Unverified("The source backlog did not match the submitted history");
            }
            if (!retained.equals(target.find().sort(Sorts.ascending("_id")).into(new ArrayList<>()))) {
                throw new AssertionError("Stopped pipeline changed target state during backlog prewrite");
            }
            int events = backlog.batch().streams().get(RECORDS).observed().total();
            evidence.stage = "resume-position";
            var retainedPosition = control.resumePoint(PIPELINE);
            if (!resume.equals(retainedPosition)) {
                control.writeBackPosition(PIPELINE, resume.get("chainId"), resume.get("token"));
            }
            if (!resume.equals(control.resumePoint(PIPELINE))) {
                throw new Unverified("The confirmed pre-batch position was not restored");
            }
            evidence.result.put("resumed_from_retained_position", resume.equals(retainedPosition));
            evidence.stage = "catch-up";
            long catchup = System.nanoTime();
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            settleExpected(entry, control, events);
            waitFor(entry, "zero snapshot rows in the catch-up run",
                    () -> Long.valueOf(0).equals(control.snapshotRowsRead(PIPELINE).get(RECORDS)));
            evidence.measure(catchup, events, events);
            evidence.result.put("source_tail_token_directly_comparable", false);
            evidence.result.put("measurement_status", "insufficient-source-end-evidence");
            evidence.result.put("retained_initial_state", true);
        } else {
            evidence.stage = "live";
            long baseline = settled(control);
            long[] observedWarm = new long[1];
            long[] observedBeforeDrain = new long[1];
            int rate = number(entry, "rate");
            int warmup = number(entry, "warmup_seconds");
            int measure = number(entry, "measurement_seconds");
            evidence.window("warmup");
            var result = prepared.inject(new MongoWorkloadGenerator.LiveWindow(rate, Duration.ofSeconds(warmup),
                    Duration.ofSeconds(measure)), mix, null, () -> {
                        settleExpected(entry, control, baseline + (long) rate * warmup);
                        observedWarm[0] = settled(control);
                        if (targetWindow != null) {
                            targetWindow.begin();
                        }
                        if (limit != null) {
                            limit.arm();
                        }
                        evidence.window("measurement");
                    }, () -> {
                        evidence.window(targetWindow == null ? "drain" : "target-frontier");
                        observedBeforeDrain[0] = settled(control) - observedWarm[0];
                        try (BenchmarkTargetLimit cleanup = limit) {
                            if (targetWindow != null) {
                                evidence.result.put("target_frontier_requested_epoch_ms", System.currentTimeMillis());
                                var observed = targetWindow.countThroughBoundary().orElseThrow(() ->
                                        new Unverified("Target change stream did not reach its majority-visible frontier"));
                                evidence.result.put("target_majority_visible_before_drain", observed);
                            }
                            if (limit != null) {
                                evidence.result.put("target_delay_millis", delay);
                                evidence.result.put("target_commands_delayed", limit.release());
                                evidence.result.put("target_limit_released_epoch_ms", System.currentTimeMillis());
                            }
                        }
                        evidence.window("drain");
                    });
            evidence.result.put("warmup", phase(result.warmup()));
            evidence.result.put("source_window", phase(result.measured()));
            if (!result.measured().complete()) {
                throw new Unverified("Live source window was incomplete");
            }
            long measured = result.measured().streams().get(RECORDS).observed().total();
            long drain = System.nanoTime();
            long acknowledged = observedBeforeDrain[0];
            if (limit != null) {
                long delayed = (long) evidence.result.get("target_commands_delayed");
                long majorityVisible = (long) evidence.result.get("target_majority_visible_before_drain");
                evidence.result.put("source_minus_target_at_frontier_marker", measured - majorityVisible);
                if (delayed <= 0 || majorityVisible >= measured) {
                    throw new Unverified("The target-only delay did not prove an acknowledgement deficit");
                }
            }
            evidence.result.put("target_settled_before_drain", acknowledged);
            settleExpected(entry, control, observedWarm[0] + measured);
            evidence.phase("ack_wait", drain);
            var threshold = entry.getDocument("case").getDocument("workload_spec").getDocument("measurements")
                    .getDocument("live-latency").getDocument("trial").getDocument("p99");
            evidence.result.put("live_latency", LiveLatencyEligibility.assess(Math.toIntExact(measured), List.of(), false,
                    number(threshold, "minimum_valid_samples"),
                    threshold.getNumber("minimum_confirmation_coverage_percent").doubleValue() / 100));
            evidence.result.put("measurement_status", "insufficient-event-correlation");
            evidence.result.put("diagnostic_delivery", "The aggregate histogram includes events outside the measured window.");
            evidence.result.put("window_source_events", measured);
            evidence.result.put("window_settled_delta", settled(control) - observedWarm[0]);
        }
    }

    private static void changeAtSnapshotHandoff(BsonDocument entry,
            MongoCollection<BsonDocument> source, MongoCollection<BsonDocument> target, ControlPlane control,
            BenchmarkTargetLimit hold, int initial, BsonDocument key, BsonString anchorValue, Evidence evidence) {
        evidence.stage = "snapshot-handoff";
        BsonDocument original = source.find(key).first();
        var match = new BsonDocument("$match", new BsonDocument("documentKey._id", key.get("_id")));
        var boundedTarget = target.withTimeout(2, java.util.concurrent.TimeUnit.SECONDS);
        var witness = new LinkedHashMap<String, Object>();
        witness.put("repeated_key", key.toJson());
        witness.put("expected_snapshot_rows", initial);
        evidence.result.put("snapshot_handoff", witness);
        try (var history = boundedTarget.watch(List.of(match))
                .fullDocument(com.mongodb.client.model.changestream.FullDocument.UPDATE_LOOKUP)
                .maxAwaitTime(100, java.util.concurrent.TimeUnit.MILLISECONDS).cursor()) {
            hold.holdSnapshotWrites();
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            hold.awaitHeldWrite(Duration.ofSeconds(number(entry, "timeout_seconds")));
            witness.put("initial_target_write_held", true);
            writeAnchor(source, key, anchorValue);
            witness.put("source_update_acknowledged", true);
            long visible = boundedTarget.countDocuments();
            witness.put("target_snapshot_rows_visible_after_update", visible);
            long held = hold.release();
            witness.put("target_snapshot_commands_held", held);
            if (visible >= initial || held < 1) {
                throw new Unverified("A target snapshot write was not held across the handoff update");
            }
            waitFor(entry, "the handoff update to land", () -> {
                var row = boundedTarget.find(key).first();
                return row != null && anchorValue.equals(row.get("s0"));
            });
            var writes = new ArrayList<com.mongodb.client.model.changestream.ChangeStreamDocument<BsonDocument>>();
            waitFor(entry, "the repeated key's snapshot and CDC writes", () -> {
                var change = history.tryNext();
                if (change != null) writes.add(change);
                return writes.size() == 2;
            });
            if (!BenchmarkOperatorOracle.equivalent(original, writes.getFirst().getFullDocument())) {
                throw new Unverified("The first target write of the handoff key did not carry its snapshot value");
            }
            if (writes.getLast().getFullDocument() == null
                    || !anchorValue.equals(writes.getLast().getFullDocument().get("s0"))) {
                throw new Unverified("The subsequent target write of the handoff key did not carry its updated value");
            }
            witness.put("repeated_key_target_writes", writes.size());
        }
    }

    private static void writeAnchor(MongoCollection<BsonDocument> source, BsonDocument key, BsonString value) {
        if (source.updateOne(key, new BsonDocument("$set", new BsonDocument("s0", value))).getModifiedCount() != 1) {
            throw new Unverified("The readiness key did not change: " + key);
        }
    }

    private static void operatorRun(BsonDocument entry, MongoWorkloadGenerator.Prepared prepared, ControlPlane control,
            MongoDatabase source, MongoDatabase target, BenchmarkOperators operators, Evidence evidence,
            MongoWorkloadGenerator.HotKeys hot) {
        boolean lifecycle = operators.scenario.equals("join-lifecycle-release");
        String mode = lifecycle ? "live-latency" : text(entry.getDocument("case"), "mode");
        evidence.stage = "initial-state";
        evidence.window(mode.equals("snapshot") ? "snapshot" : "load");
        var initial = operators.expected(source);
        long initialStart = System.nanoTime();
        control.lifecycle(PIPELINE, LifecycleVerb.START);
        waitFor(entry, "operator snapshot input and landing", () -> operatorInitialLanded(control, operators, mode));
        if (mode.equals("snapshot")) {
            long input = operators.rows.values().stream().mapToLong(Integer::longValue).sum();
            evidence.measure(initialStart, input, initial.size());
            evidence.window("readback");
            verifyOperator(operators, target, initial, evidence);
            return;
        }

        awaitContent(entry, operators, target, initial, "operator initial state");
        waitFor(entry, "target writer settled the initial state", () -> settled(control) >= initial.size());
        var previousAcks = operatorPositions(control, operators.mutableInputs(), "targetAcked");
        var inputBeforeAnchor = capturedChanges(control);
        long targetBeforeAnchor = settled(control);
        for (String name : operators.mutableInputs()) {
            anchor(source, operators.scenario, name);
        }
        var anchor = operators.expected(source);
        if (anchor.equals(initial)) {
            throw new Unverified("The readiness change does not reach the expected target projection");
        }
        try {
            waitFor(entry, "confirmed operator readiness changes", () -> {
                var captured = capturedChanges(control);
                return operators.mutableInputs().stream().allMatch(name -> captured.getOrDefault(name, 0L)
                                >= inputBeforeAnchor.getOrDefault(name, 0L) + 1)
                        && settled(control) > targetBeforeAnchor
                        && BenchmarkOperatorOracle.difference(operators, target, anchor) == null;
            });
        } catch (Limit failure) {
            var current = operatorPositions(control, operators.mutableInputs(), "targetAcked");
            throw new Limit(failure.getMessage() + ": captured " + capturedChanges(control) + ", settled "
                    + targetBeforeAnchor + " -> " + settled(control) + ", target position advanced "
                    + (!current.isEmpty() && !current.equals(previousAcks)) + ": "
                    + BenchmarkOperatorOracle.difference(operators, target, anchor));
        }
        var confirmedAcks = operatorPositions(control, operators.mutableInputs(), "targetAcked");
        evidence.result.put("initial_target_position_advanced", !confirmedAcks.isEmpty() && !confirmedAcks.equals(previousAcks));
        evidence.phase("initial_state", initialStart);
        if (lifecycle) {
            lifecycleCycles(entry, prepared, control, source, target, operators, evidence);
            return;
        }
        var mix = operators.paired() ? new WorkloadRecipe.Mix(0, 70, 30) : mix(entry);
        if (mode.equals("backlog-throughput")) {
            operatorBacklog(entry, prepared, control, source, target, operators, evidence, anchor, mix);
        } else {
            operatorLive(entry, prepared, control, source, target, operators, evidence, mix, hot);
        }
    }

    private static void lifecycleCycles(BsonDocument entry, MongoWorkloadGenerator.Prepared prepared, ControlPlane control,
            MongoDatabase source, MongoDatabase target, BenchmarkOperators operators, Evidence evidence) {
        int cycles = number(entry.getDocument("case").getDocument("workload_spec").getDocument("scenario")
                .getDocument("parameters"), "same_jvm_cycles_per_repeat");
        int idle = number(entry.getDocument("case").getDocument("workload_spec").getDocument("memory_profile"),
                "idle_seconds");
        var observations = new ArrayList<Map<String, Object>>();
        evidence.result.put("lifecycle_cycles", observations);
        Map<String, String> previousPositions = Map.of();
        for (int cycle = 1; cycle <= cycles; cycle++) {
            evidence.stage = "lifecycle-cycle-" + cycle;
            evidence.windowPrefix = evidence.stage + "/";
            if (cycle > 1) {
                evidence.window("start");
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                waitFor(entry, "Join resumed", () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent());
                recordLifecycleResume(control, operators, observations.getLast(), previousPositions);
            }
            operatorLive(entry, prepared, control, source, target, operators, evidence, null, null);
            var warmup = evidence.result.get("warmup");
            var measurement = evidence.result.get("source_window");
            evidence.window("stop");
            previousPositions = readDurablePositions(control, operators.mutableInputs());
            control.stop(PIPELINE, false);
            waitFor(entry, "Join stopped", () -> control.state(PIPELINE).filter(PipelineState.STOPPED::equals).isPresent());
            var output = target.getCollection("facts", BsonDocument.class);
            try (var activity = output.watch().maxAwaitTime(200, java.util.concurrent.TimeUnit.MILLISECONDS).cursor()) {
                evidence.window("idle");
                long until = System.nanoTime() + Duration.ofSeconds(idle).toNanos();
                while (System.nanoTime() < until) {
                    if (activity.tryNext() != null) {
                        throw new Unverified("The target collection changed in the stopped window");
                    }
                }
            }
            verifyOperator(operators, target, operators.expected(source), evidence);
            observations.add(new LinkedHashMap<>(Map.of("cycle", cycle, "state", "STOPPED", "target_changes_while_idle", 0,
                    "idle_seconds", idle, "target_readback", "verified", "durable_state", "restart-not-observed",
                    "durable_positions_before_stop", previousPositions,
                    "queue_release", "unavailable from runtime read face",
                    "warmup", warmup, "measurement", measurement)));
        }
        evidence.windowPrefix = "";
    }

    private static void recordLifecycleResume(ControlPlane control, BenchmarkOperators operators,
            Map<String, Object> observation, Map<String, String> before) {
        Map<String, Long> snapshot;
        try {
            snapshot = control.awaitSnapshotRowsRead(PIPELINE, java.util.Set.copyOf(operators.sources()));
        } catch (AssertionError missing) {
            observation.put("durable_state", "snapshot-observation-unavailable");
            observation.put("snapshot_observation_error", missing.toString());
            return;
        }
        observation.put("snapshot_rows_after_restart", snapshot);
        var after = readDurablePositions(control, operators.mutableInputs());
        observation.put("durable_positions_after_restart", after);
        if (operators.sources().stream().anyMatch(name -> snapshot.get(name) != 0)) {
            observation.put("durable_state", "snapshot-read-observed");
        } else if (!before.keySet().containsAll(operators.mutableInputs())
                || !after.keySet().containsAll(operators.mutableInputs())) {
            observation.put("durable_state", "position-unavailable");
        } else {
            observation.put("durable_state", before.equals(after) ? "retained" : "position-changed");
        }
    }

    private static Map<String, String> readDurablePositions(ControlPlane control, List<String> inputs) {
        var positions = new LinkedHashMap<String, String>();
        for (String input : inputs) {
            control.durablePosition(PIPELINE, input).ifPresent(position -> positions.put(input, position));
        }
        return positions;
    }

    private static void observeIdleWindow(RealProcessServer server, int seconds) {
        try {
            if (server.exitsWithin(Duration.ofSeconds(seconds))) {
                throw new Unverified("OP exited before teardown");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new Unverified("Interrupted in the idle window");
        }
    }

    private static boolean operatorInitialLanded(ControlPlane control, BenchmarkOperators operators, String mode) {
        var state = control.state(PIPELINE);
        if (state.filter(PipelineState.FAILED::equals).isPresent()) {
            throw new AssertionError("Operator pipeline failed during initial load: " + control.logs(PIPELINE));
        }
        boolean snapshot = mode.equals("snapshot");
        var readyState = snapshot ? PipelineState.COMPLETED : PipelineState.RUNNING;
        if (state.filter(readyState::equals).isEmpty()) {
            return false;
        }
        var counts = control.snapshotRowsRead(PIPELINE);
        boolean inputsRead = operators.rows.entrySet().stream().allMatch(row ->
                counts.getOrDefault(row.getKey(), 0L) == row.getValue().longValue());
        return inputsRead && (!snapshot || control.initialLoad(PIPELINE)
                .filter(load -> "COMPLETED".equals(load.get("phase"))).isPresent());
    }

    private static void anchor(MongoDatabase source, String scenario, String name) {
        var rows = source.getCollection(name, BsonDocument.class);
        BsonDocument chosen = null;
        if (scenario.equals("join-hot-update") && name.equals("dimensions")) {
            var fact = source.getCollection("facts", BsonDocument.class)
                    .find(com.mongodb.client.model.Filters.gt("dimension_id", 0)).first();
            if (fact != null) {
                chosen = rows.find(new BsonDocument("_id", fact.get("dimension_id"))).first();
            }
        } else {
            try (var cursor = rows.find().sort(Sorts.ascending("_id")).cursor()) {
                while (cursor.hasNext()) {
                    var candidate = cursor.next();
                    if (scenario.equals("filter-basic") && candidate.getNumber("_id").longValue() % 2 != 0
                            || name.equals("facts") && candidate.getNumber("dimension_id").longValue() < 0) {
                        continue;
                    }
                    chosen = candidate;
                    break;
                }
            }
        }
        if (chosen == null) {
            throw new Unverified("No target-visible readiness key in " + name);
        }
        String numeric = name.equals("dimensions") ? "score" : "value";
        BsonDocument changes = chosen.containsKey(numeric)
                ? new BsonDocument(numeric, new BsonInt32(chosen.getNumber(numeric).intValue() + 1))
                : new BsonDocument(name.equals("facts") ? "payload" : "s0", new BsonString("benchmark-ready"));
        if (rows.updateOne(new BsonDocument("_id", chosen.get("_id")), new BsonDocument("$set", changes))
                .getModifiedCount() != 1) {
            throw new AssertionError("The readiness key did not change in " + name);
        }
    }

    private static void operatorBacklog(BsonDocument entry, MongoWorkloadGenerator.Prepared prepared, ControlPlane control,
            MongoDatabase source, MongoDatabase target, BenchmarkOperators operators, Evidence evidence,
            Map<String, BsonDocument> initial, WorkloadRecipe.Mix mix) {
        control.stop(PIPELINE, false);
        waitFor(entry, "operator stopped before prewrite",
                () -> control.state(PIPELINE).filter(PipelineState.STOPPED::equals).isPresent());
        var resume = operatorPositions(control, operators.mutableInputs(), "resumeFrom");
        if (resume.isEmpty()) {
            throw new Unverified("The stopped operator has no persisted pre-batch position");
        }
        evidence.stage = "prewrite";
        long prewrite = System.nanoTime();
        var backlog = prepared.prewrite(number(entry, "backlog_events"), mix, null);
        evidence.phase("prewrite", prewrite);
        evidence.result.put("source_batch", phase(backlog.batch()));
        if (!backlog.batch().complete()) {
            throw new Unverified("The source operator backlog did not match its declared history");
        }
        String unconsumed = BenchmarkOperatorOracle.difference(operators, target, initial);
        if (unconsumed != null) {
            throw new AssertionError("Stopped operator changed the pre-batch target state: " + unconsumed);
        }
        var expected = operators.expected(source);
        var retained = operatorPositions(control, operators.mutableInputs(), "resumeFrom");
        for (var chain : resume.entrySet()) {
            if (!chain.getValue().equals(retained.get(chain.getKey()))) {
                control.writeBackPosition(PIPELINE, chain.getKey(), chain.getValue());
            }
        }
        if (!resume.equals(operatorPositions(control, operators.mutableInputs(), "resumeFrom"))) {
            throw new Unverified("The operator did not retain the confirmed pre-batch position");
        }

        evidence.stage = "catch-up";
        long start = System.nanoTime();
        control.lifecycle(PIPELINE, LifecycleVerb.START);
        awaitOperatorPhase(entry, control, target, operators, expected, Map.of(), backlog.batch(),
                initial.equals(expected) ? -1 : 0);
        evidence.phase("catch_up_observed", start);
        var snapshot = control.awaitSnapshotRowsRead(PIPELINE, java.util.Set.copyOf(operators.sources()));
        if (operators.sources().stream().anyMatch(name -> snapshot.get(name) != 0)) {
            throw new Unverified("A resumed operator input read a snapshot: " + snapshot);
        }
        evidence.result.put("retained_initial_state", true);
        evidence.result.put("measurement_status", "insufficient-source-end-evidence");
        evidence.result.put("target_settled_units", settled(control));
        evidence.result.put("source_captured_changes", capturedChanges(control));
        verifyOperator(operators, target, expected, evidence);
    }

    private static void operatorLive(BsonDocument entry, MongoWorkloadGenerator.Prepared prepared, ControlPlane control,
            MongoDatabase source, MongoDatabase target, BenchmarkOperators operators, Evidence evidence,
            WorkloadRecipe.Mix mix, MongoWorkloadGenerator.HotKeys hot) {
        evidence.stage = "live";
        int rate = number(entry, "rate");
        int warmup = number(entry, "warmup_seconds");
        int measurement = number(entry, "measurement_seconds");
        var baseline = capturedChanges(control);
        var warm = new LinkedHashMap<String, Long>();
        var warmRows = new LinkedHashMap<String, BsonDocument>();
        long[] warmOutput = new long[1];
        evidence.window("warmup");
        var windowSize = new MongoWorkloadGenerator.LiveWindow(rate, Duration.ofSeconds(warmup),
                Duration.ofSeconds(measurement));
        Runnable afterWarmup = () -> {
            var expected = operators.expected(source);
            waitFor(entry, "operator warm-up seen at both endpoints",
                    () -> capturedChanges(control).values().stream().mapToLong(Long::longValue).sum()
                            >= baseline.values().stream().mapToLong(Long::longValue).sum() + (long) rate * warmup
                            && BenchmarkOperatorOracle.difference(operators, target, expected) == null);
            warmRows.putAll(expected);
            warm.putAll(capturedChanges(control));
            warmOutput[0] = settled(control);
            evidence.window("measurement");
        };
        Runnable afterWrites = () -> evidence.window("drain");
        var run = operators.scenario.equals("join-lifecycle-release")
                ? prepared.injectSteadyCycle(windowSize, afterWarmup, afterWrites)
                : prepared.inject(windowSize, mix, hot, afterWarmup, afterWrites);
        evidence.result.put("warmup", phase(run.warmup()));
        evidence.result.put("source_window", phase(run.measured()));
        if (!run.measured().complete()) {
            throw new Unverified("The operator live source window was incomplete");
        }
        long drained = System.nanoTime();
        var expected = operators.expected(source);
        awaitOperatorPhase(entry, control, target, operators, expected, warm, run.measured(),
                warmRows.equals(expected) ? -1 : warmOutput[0]);
        evidence.phase("ack_wait_with_readback", drained);
        evidence.result.put("target_settled_delta", settled(control) - warmOutput[0]);
        var actual = capturedChanges(control);
        var window = new LinkedHashMap<String, Long>();
        run.measured().streams().keySet().forEach(name ->
                window.put(name, actual.getOrDefault(name, 0L) - warm.getOrDefault(name, 0L)));
        evidence.result.put("source_captured_changes", window);
        evidence.result.put("measurement_status", "insufficient-event-correlation");
        evidence.window("readback");
        verifyOperator(operators, target, expected, evidence);
        if (operators.scenario.equals("join-steady-state")) {
            var factRows = source.getCollection("facts").countDocuments();
            var dimensionRows = source.getCollection("dimensions").countDocuments();
            if (factRows != operators.rows.get("facts") || dimensionRows != operators.rows.get("dimensions")
                    || expected.size() != warmRows.size() || !expected.keySet().equals(warmRows.keySet())) {
                throw new AssertionError("Steady-state workload changed the logical key space");
            }
            evidence.result.put("steady_state_logical_rows", Map.of("facts", factRows, "dimensions", dimensionRows,
                    "joined", expected.size()));
        }
    }

    private static void awaitOperatorPhase(BsonDocument entry, ControlPlane control, MongoDatabase target,
            BenchmarkOperators operators, Map<String, BsonDocument> expected, Map<String, Long> before,
            MongoWorkloadGenerator.PhaseReport phase, long minimumTargetExclusive) {
        var expectedInputs = new LinkedHashMap<String, Long>();
        phase.streams().forEach((name, stream) -> expectedInputs.put(name,
                before.getOrDefault(name, 0L) + stream.observed().total()));
        try {
            waitFor(entry, "per-input operator phase and independent target rows", () ->
                    capturedInputsMatch(expectedInputs, capturedChanges(control))
                    && (minimumTargetExclusive < 0 || settled(control) > minimumTargetExclusive)
                    && BenchmarkOperatorOracle.difference(operators, target, expected) == null);
        } catch (Limit failure) {
            throw new Limit(failure.getMessage() + ": input " + capturedChanges(control)
                    + ": " + BenchmarkOperatorOracle.difference(operators, target, expected));
        }
    }

    static boolean capturedInputsMatch(Map<String, Long> expected, Map<String, Long> current) {
        boolean matched = true;
        for (var input : expected.entrySet()) {
            long expectedCount = input.getValue();
            long actualCount = current.getOrDefault(input.getKey(), 0L);
            if (actualCount > expectedCount) {
                throw new Unverified("Captured extra changes for " + input.getKey() + ": expected "
                        + expectedCount + " and observed " + actualCount);
            }
            matched &= actualCount == expectedCount;
        }
        return matched;
    }

    private static Map<String, String> operatorPositions(ControlPlane control, List<String> inputs, String field) {
        String reading = control.positionReading(PIPELINE);
        if (!reading.startsWith("200 ")) {
            throw new AssertionError("Cannot read the operator positions: " + reading);
        }
        var result = new LinkedHashMap<String, String>();
        var covered = new java.util.HashSet<String>();
        for (var item : BsonDocument.parse(reading.substring(4)).getArray("chains")) {
            var chain = item.asDocument();
            var selected = chain.getArray("tables").stream().map(value -> value.asString().getValue())
                    .filter(inputs::contains).toList();
            var point = chain.get(field);
            if (!selected.isEmpty() && point != null && point.isDocument()
                    && point.asDocument().get("token") instanceof BsonString token) {
                covered.addAll(selected);
                result.put(text(chain, "chainId"), token.getValue());
            }
        }
        return covered.containsAll(inputs) ? result : Map.of();
    }

    private static Map<String, Long> capturedChanges(ControlPlane control) {
        var result = new LinkedHashMap<String, Long>();
        for (var fact : control.metricFacts(PIPELINE)) {
            if (!"tapstate.pipeline.records".equals(fact.get("name"))) {
                continue;
            }
            for (var value : (List<?>) fact.get("points")) {
                var point = (Map<?, ?>) value;
                var attributes = (Map<?, ?>) point.get("attributes");
                if ("in".equals(attributes.get("direction")) && !"read".equals(attributes.get("op"))
                        && point.get("value") instanceof Number count) {
                    result.merge(String.valueOf(attributes.get("tapstate.table.id")), count.longValue(), Long::sum);
                }
            }
        }
        return result;
    }

    private static void awaitContent(BsonDocument entry, BenchmarkOperators operators, MongoDatabase target,
            Map<String, BsonDocument> expected, String stage) {
        try {
            waitFor(entry, stage, () -> BenchmarkOperatorOracle.difference(operators, target, expected) == null);
        } catch (Limit failure) {
            throw new Limit(failure.getMessage() + ": " + BenchmarkOperatorOracle.difference(operators, target, expected));
        }
    }

    private static void verifyOperator(BenchmarkOperators operators, MongoDatabase target, Map<String, BsonDocument> expected,
            Evidence evidence) {
        evidence.stage = "readback";
        long started = System.nanoTime();
        String difference = BenchmarkOperatorOracle.difference(operators, target, expected);
        if (difference != null) {
            throw new AssertionError(difference);
        }
        evidence.phase("readback", started);
        evidence.result.put("target_readback_rows", expected.size());
        evidence.result.put("target_collections_expected", operators.targets());
        evidence.result.put("target_collections", target.listCollectionNames().into(new ArrayList<>()));
    }

    private static Map<String, Object> phase(MongoWorkloadGenerator.PhaseReport report) {
        var result = new LinkedHashMap<String, Object>();
        result.put("complete", report.complete());
        result.put("started_at", report.startedAt() == null ? null : report.startedAt().toString());
        result.put("writes_finished_at", report.writesFinishedAt() == null ? null : report.writesFinishedAt().toString());
        result.put("generator_process_cpu_nanos", report.generatorProcessCpuNanos());
        result.put("write_nanos", report.writeNanos());
        result.put("history_nanos", report.historyNanos());
        result.put("submitted_per_second", report.actualSubmittedEventsPerSecond());
        result.put("acknowledged_per_second", report.actualAcknowledgedEventsPerSecond());
        report.streams().forEach((name, stream) -> {
            var reading = new LinkedHashMap<String, Object>();
            reading.put("planned", stream.planned());
            reading.put("submitted", stream.submittedOperations());
            reading.put("observed", stream.observed());
            reading.put("acknowledged", stream.acknowledged());
            reading.put("start_exclusive", token(stream.startExclusive()));
            reading.put("first", token(stream.first()));
            reading.put("last", token(stream.last()));
            reading.put("history_boundary", token(stream.historyBoundary()));
            reading.put("sequence_sha256", stream.sourceSequenceSha256());
            reading.put("hot_key_selections", stream.hotKeySelections());
            reading.put("error", stream.error());
            result.put(name, reading);
        });
        return result;
    }

    private static String token(MongoWorkloadGenerator.Position position) {
        return position == null ? null : position.resumeToken().toJson();
    }

    private static void settleExpected(BsonDocument entry, ControlPlane control, long count) {
        waitFor(entry, "settled target writes through " + count,
                () -> settled(control) >= count && control.durablePosition(PIPELINE, RECORDS).isPresent());
        if (settled(control) != count) {
            throw new Unverified("Unexpected target writes beyond the declared boundary " + count);
        }
    }

    private static Map<String, Long> verifySame(MongoCollection<BsonDocument> source, MongoCollection<BsonDocument> target) {
        var typeDifferences = new java.util.TreeMap<String, Long>();
        try (var left = source.find().sort(Sorts.ascending("_id")).cursor();
                var right = target.find().sort(Sorts.ascending("_id")).cursor()) {
            while (left.hasNext() && right.hasNext()) {
                var expected = left.next();
                var actual = right.next();
                if (!sameDocument(expected, actual)) {
                    throw new AssertionError("Target readback differed at " + expected.get("_id")
                            + " expected " + expected.toJson() + " actual " + actual.toJson());
                }
                for (String field : expected.keySet()) {
                    var expectedType = expected.get(field).getBsonType();
                    var actualType = actual.get(field).getBsonType();
                    if (expectedType != actualType) {
                        typeDifferences.merge(field + " " + expectedType + " to " + actualType, 1L, Long::sum);
                    }
                }
            }
            if (left.hasNext() || right.hasNext()) {
                throw new AssertionError("Target readback had a different key cardinality");
            }
        }
        return typeDifferences;
    }

    private static boolean sameDocument(BsonDocument expected, BsonDocument actual) {
        if (!expected.keySet().equals(actual.keySet())) {
            return false;
        }
        for (String field : expected.keySet()) {
            BsonValue left = expected.get(field);
            BsonValue right = actual.get(field);
            if (left.isNumber() && right.isNumber()) {
                if (left.asNumber().decimal128Value().bigDecimalValue()
                        .compareTo(right.asNumber().decimal128Value().bigDecimalValue()) != 0) {
                    return false;
                }
            } else if (!left.equals(right)) {
                return false;
            }
        }
        return true;
    }

    private static String resource(String id, String connector, Map<String, Object> config, String table)
            throws IOException {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                %sconfig: %s
                """.formatted(id, connector, table == null ? "" : "mode: cdc\ntables: [" + table + "]\n",
                JSON.writeValueAsString(config));
    }

    private static String pipeline(String mode, String onLoad) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: perf_copy
                source: src_perf
                settings: {read_mode: %s}
                serve:
                  from: /.*/
                  sync:
                    - {id: output, source: tgt_perf, write_mode: upsert, on_full_load: %s}
                """.formatted(mode, onLoad);
    }

    private static void discover(ControlPlane control, String id, String connector, Map<String, Object> config) {
        control.discoverSchema(id, connector, config);
    }

    private static void waitFor(BsonDocument entry, String stage, BooleanSupplier condition) {
        long end = System.nanoTime() + Duration.ofSeconds(number(entry, "timeout_seconds")).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= end) {
                throw new Limit("Timed out waiting for " + stage);
            }
            try {
                Await.pauseBeforeNextPoll(Duration.ofMillis(200));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new Limit("Interrupted waiting for " + stage);
            }
        }
    }

    private static WorkloadRecipe.Mix mix(BsonDocument entry) {
        if (entry.get("mix").isNull()) {
            return null;
        }
        var values = entry.getArray("mix");
        return new WorkloadRecipe.Mix(values.get(0).asInt32().getValue(), values.get(1).asInt32().getValue(),
                values.get(2).asInt32().getValue());
    }

    private static long settled(ControlPlane control) {
        return control.metricsNamed(PIPELINE, "records.out").getOrDefault("records.out", 0L);
    }

    private static void saveProcess(RealProcessServer server, Path destination) throws IOException {
        Files.copy(server.output(), destination);
    }

    private static Path checkout() {
        Path root = Path.of("").toAbsolutePath();
        while (!Files.isRegularFile(root.resolve("deploy/cloud/connectors.lock.json"))) {
            root = root.getParent();
            if (root == null) {
                throw new IllegalStateException("Could not find the connector lock");
            }
        }
        return root;
    }

    private static Path verified(BsonDocument lock, Path folder, String id) throws Exception {
        var entry = lock.getArray("connectors").stream().map(BsonValue::asDocument)
                .filter(row -> id.equals(text(row, "id"))).findFirst().orElseThrow();
        Path artifact = folder.resolve(id + "-connector.jar");
        if (Files.size(artifact) != number(entry, "bytes") || !digest(artifact).equals(text(entry, "sha256"))) {
            throw new IllegalArgumentException("Connector does not match the lock: " + id);
        }
        return artifact;
    }

    private static String digest(Path file) throws Exception {
        var digest = MessageDigest.getInstance("SHA-256");
        try (var stream = Files.newInputStream(file)) {
            var bytes = new byte[65536];
            for (int length; (length = stream.read(bytes)) != -1;) {
                digest.update(bytes, 0, length);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static String text(BsonDocument doc, String name) {
        return doc.getString(name).getValue();
    }

    private static int number(BsonDocument doc, String name) {
        var value = doc.get(name);
        if (value == null || !(value.isInt32() || value.isInt64())) {
            throw new IllegalArgumentException(name + " must be a BSON integer");
        }
        return Math.toIntExact(value.asNumber().longValue());
    }

    private static final class Limit extends RuntimeException {
        Limit(String message) { super(message); }
    }

    private static final class Unverified extends RuntimeException {
        Unverified(String message) { super(message); }
    }

    private static final class Evidence {
        final BsonDocument request;
        final Map<String, Object> result = new LinkedHashMap<>();
        final Map<String, Long> phases = new LinkedHashMap<>();
        String stage = "prepare";
        BenchmarkResources resources;
        String windowPrefix = "";

        Evidence(BsonDocument request, BsonDocument definition, int repeat, String mode, Path artifact, BsonDocument entry)
                throws Exception {
            this.request = request;
            result.put("run_id", text(request, "run_id"));
            result.put("case_id", text(definition, "id"));
            result.put("case_version", number(definition, "case_version"));
            result.put("definition_sha256", text(definition, "definition_sha256"));
            result.put("code_sha", text(request.getDocument("identity"), "code_sha"));
            result.put("repeat", repeat);
            result.put("mode", mode);
            result.put("scale", text(entry, "scale"));
            var effective = entry.clone();
            effective.remove("case");
            effective.remove("repeats");
            result.put("effective_load", JSON.readTree(effective.toJson()));
            result.put("connector_sha256", digest(artifact));
            result.put("op_boot_sha256", digest(Path.of(System.getProperty("tapstate.e2e.boot-jar"))));
            result.put("environment", Map.of("os", System.getProperty("os.name"), "arch", System.getProperty("os.arch"),
                    "jdk", System.getProperty("java.runtime.version"), "cpu_count", Runtime.getRuntime().availableProcessors()));
            result.put("phase_nanos", phases);
        }

        void measure(long start, long input, long confirmed) {
            phase("completion_observed", start);
            result.put("input_units", input);
            result.put("confirmed_output_units", confirmed);
            result.put("completion_clock", "monotonic observed upper bound including telemetry publication and polling");
            result.put("measurement_status", "bounded-observation");
        }

        void diagnostic(String name, Supplier<?> reading) {
            try {
                result.put(name, reading.get());
            } catch (RuntimeException | AssertionError error) {
                result.put(name + "_error", error.toString());
            }
        }

        void phase(String name, long start) {
            phases.put(windowPrefix + name, System.nanoTime() - start);
        }

        void window(String name) {
            if (resources != null) {
                resources.phase(windowPrefix + name);
            }
        }

        void stageBeforeCleanup() {
            result.put("stage_before_cleanup", stage);
        }
    }
}
