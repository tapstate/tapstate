package io.tapstate.e2e;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
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
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/** Executes an explicit, version-bound benchmark request with real OP processes and locked connectors. */
@RequiresDocker
@EnabledIfSystemProperty(named = "tapstate.benchmarks.request", matches = ".+")
class BenchmarkRunIT {
    private static final String PIPELINE = "perf_copy";
    private static final String RECORDS = "records";
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
        try {
            if (connector.equals("dummy")) {
                evidence.stage = "snapshot";
                runDummy(entry, artifact, folder, evidence, false);
                evidence.stage = "content-witness";
                runDummy(entry, artifact, folder, evidence, true);
            } else {
                runMongo(entry, definition, artifact, folder, evidence);
            }
            evidence.result.put("status", "completed");
        } catch (Limit failure) {
            evidence.result.put("status", "timed-out");
            evidence.result.put("reason", failure.getMessage());
        } catch (Unverified failure) {
            evidence.result.put("status", "measurement-incomplete");
            evidence.result.put("reason", failure.getMessage());
        } catch (RuntimeException | AssertionError | IOException | URISyntaxException failure) {
            evidence.result.put("status", "failed");
            evidence.result.put("reason", failure.toString());
        }
        evidence.result.put("last_stage", evidence.stage);
        evidence.result.put("total_nanos", System.nanoTime() - start);
        return evidence.result;
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
                var workspace = new LinkedHashMap<>(Map.of("source.tap.yml", resource("src_perf", "dummy", src, null),
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
            Evidence evidence) throws Exception {
        String namespace = "perf_" + UUID.randomUUID().toString().replace("-", "");
        String sourceName = namespace + "_src";
        String targetName = namespace + "_tgt";
        String sourceUri = SharedMongo.replicaSetUrl(sourceName);
        String targetUri = SharedMongo.replicaSetUrl(targetName);
        Path seeds = Files.createDirectory(folder.resolve("runtime-input"));
        Files.copy(artifact, seeds.resolve("mongodb-connector.jar"));
        try (var client = MongoClients.create(sourceUri)) {
            var sourceDb = client.getDatabase(sourceName).withWriteConcern(WriteConcern.MAJORITY);
            var targetDb = client.getDatabase(targetName).withReadConcern(ReadConcern.MAJORITY);
            try {
                evidence.stage = "prepare";
                long preparing = System.nanoTime();
                var recipe = WorkloadRecipe.fromCatalog(definition, Map.of(RECORDS, number(entry, "rows")), null);
                var generator = new MongoWorkloadGenerator(sourceDb, recipe, text(evidence.request, "run_id"), 32);
                var prepared = generator.prepare(generator.calibrateStringLengths());
                evidence.result.put("source_data", prepared.preparation());
                evidence.phase("prepare", preparing);
                var source = sourceDb.getCollection(RECORDS, BsonDocument.class);
                var target = targetDb.getCollection(RECORDS, BsonDocument.class);
                long startup = System.nanoTime();
                evidence.stage = "startup";
                try (var storage = PreparedNativeStore.create();
                        var server = RealProcessServer.startPrepared(storage.configurationUri(), "127.0.0.1",
                                ignored -> List.of("--tapstate.connectors.seed-dir=" + seeds))) {
                    var control = new ControlPlane(server.baseUrl());
                    storage.loginAndAwaitBusiness(control);
                    try (var peaks = new BenchmarkResources(server.pid())) {
                        var src = Map.<String, Object>of("uri", sourceUri, "preImage", true);
                        var tgt = Map.<String, Object>of("uri", targetUri + (targetUri.contains("?") ? "&" : "?") + "w=majority");
                        var workspace = new LinkedHashMap<>(Map.of(
                                "source.tap.yml", resource("src_perf", "mongodb", src, RECORDS),
                                "target.tap.yml", resource("tgt_perf", "mongodb", tgt, null)));
                        control.apply(workspace);
                        discover(control, "src_perf", "mongodb", src);
                        boolean snapshot = text(definition, "mode").equals("snapshot");
                        workspace.put("pipeline.tap.yml", pipeline(snapshot ? "snapshot_only" : "snapshot_and_cdc", "clear"));
                        control.apply(workspace);
                        evidence.phase("startup", startup);
                        try {
                            if (snapshot) {
                                mongoSnapshot(entry, control, evidence);
                            } else {
                                mongoCdc(entry, prepared, control, source, target, evidence, text(definition, "mode"));
                            }
                            evidence.stage = "readback";
                            long verify = System.nanoTime();
                            evidence.result.put("readback_bson_type_differences", verifySame(source, target));
                            evidence.phase("readback", verify);
                            evidence.result.put("target_readback_rows", target.countDocuments());
                        } finally {
                            evidence.diagnostic("runtime_metrics", () -> control.metrics(PIPELINE));
                            evidence.diagnostic("runtime_position", () -> control.positionReading(PIPELINE));
                            evidence.result.put("op_rss_peak_kib", peaks.peakKiB());
                            evidence.diagnostic("mongo_shared_instance_resident_mib", () ->
                                    sourceDb.runCommand(new org.bson.Document("serverStatus", 1))
                                            .get("mem", org.bson.Document.class).getInteger("resident"));
                        }
                    } finally {
                        saveProcess(server, folder.resolve("op.log"));
                    }
                }
            } finally {
                evidence.stageBeforeCleanup();
                long cleanup = System.nanoTime();
                try {
                    sourceDb.drop();
                } finally {
                    targetDb.drop();
                    evidence.phase("cleanup", cleanup);
                }
            }
        }
    }

    private static void mongoSnapshot(BsonDocument entry, ControlPlane control, Evidence evidence) {
        evidence.stage = "snapshot";
        int rows = number(entry, "rows");
        long start = System.nanoTime();
        control.lifecycle(PIPELINE, LifecycleVerb.START);
        waitFor(entry, "the MongoDB snapshot and settled writes",
                () -> snapshotLanded(control, RECORDS, rows));
        evidence.measure(start, rows, rows);
        evidence.result.put("snapshot_landed", control.initialLoad(PIPELINE).orElseThrow());
    }

    private static boolean snapshotLanded(ControlPlane control, String table, int rows) {
        return control.state(PIPELINE).filter(PipelineState.COMPLETED::equals).isPresent()
                && control.snapshotRowsRead(PIPELINE).getOrDefault(table, 0L) == rows
                && control.initialLoad(PIPELINE).filter(load -> "COMPLETED".equals(load.get("phase"))
                        && load.get("landedTables") instanceof Number landed && landed.longValue() == 1).isPresent();
    }

    private static void mongoCdc(BsonDocument entry, MongoWorkloadGenerator.Prepared prepared, ControlPlane control,
            MongoCollection<BsonDocument> source, MongoCollection<BsonDocument> target, Evidence evidence, String mode) {
        evidence.stage = "initial-state";
        int initial = number(entry, "rows");
        long prepareState = System.nanoTime();
        control.lifecycle(PIPELINE, LifecycleVerb.START);
        waitFor(entry, "settled snapshot state at the target", () -> target.countDocuments() == initial
                && settled(control) == initial && control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent());
        verifySame(source, target);
        // Updating an existing key establishes a normal runtime frontier before either CDC measurement.
        String preAnchorToken = control.resumePoint(PIPELINE).get("token");
        var key = new BsonDocument("_id", new BsonInt64(1));
        source.updateOne(key, new BsonDocument("$set", new BsonDocument("s0", new BsonString("perf-anchor"))));
        waitFor(entry, "confirmed initial-state anchor", () -> {
            var row = target.find(key).first();
            String current = control.resumePoint(PIPELINE).get("token");
            return row != null && new BsonString("perf-anchor").equals(row.get("s0"))
                    && settled(control) == initial + 1 && current != null && !current.equals(preAnchorToken);
        });
        verifySame(source, target);
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
            control.writeBackPosition(PIPELINE, resume.get("chainId"), resume.get("token"));
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
            int rate = number(entry, "rate");
            int warmup = number(entry, "warmup_seconds");
            int measure = number(entry, "measurement_seconds");
            var result = prepared.inject(new MongoWorkloadGenerator.LiveWindow(rate, Duration.ofSeconds(warmup),
                    Duration.ofSeconds(measure)), mix, null, () -> {
                        settleExpected(entry, control, baseline + (long) rate * warmup);
                        observedWarm[0] = settled(control);
                    });
            evidence.result.put("warmup", phase(result.warmup()));
            evidence.result.put("source_window", phase(result.measured()));
            if (!result.measured().complete()) {
                throw new Unverified("Live source window was incomplete");
            }
            long measured = result.measured().streams().get(RECORDS).observed().total();
            long drain = System.nanoTime();
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

    private static Map<String, Object> phase(MongoWorkloadGenerator.PhaseReport report) {
        var result = new LinkedHashMap<String, Object>();
        result.put("complete", report.complete());
        result.put("started_at", report.startedAt() == null ? null : report.startedAt().toString());
        result.put("write_nanos", report.writeNanos());
        result.put("history_nanos", report.historyNanos());
        result.put("submitted_per_second", report.actualSubmittedEventsPerSecond());
        result.put("acknowledged_per_second", report.actualAcknowledgedEventsPerSecond());
        report.streams().forEach((name, stream) -> {
            var reading = new LinkedHashMap<String, Object>();
            reading.put("planned", stream.planned());
            reading.put("observed", stream.observed());
            reading.put("acknowledged", stream.acknowledged());
            reading.put("start_exclusive", token(stream.startExclusive()));
            reading.put("first", token(stream.first()));
            reading.put("last", token(stream.last()));
            reading.put("sequence_sha256", stream.sourceSequenceSha256());
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
        return doc.getNumber(name).intValue();
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
            phases.put(name, System.nanoTime() - start);
        }

        void stageBeforeCleanup() {
            result.put("stage_before_cleanup", stage);
        }
    }
}
