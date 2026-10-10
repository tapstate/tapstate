package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.PdkSinkPort;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.testsupport.DockerGate;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Cold scope and transport control; no pipeline workload or formal performance result is produced. */
@RequiresDocker
class BenchmarkWriteReturnProbeIT {
    @Test
    void coldScopeAndClockPointsComeFromTheActualOwnedWriter(@TempDir Path temporary) throws Exception {
        runCold(temporary, false);
    }

    @Test
    void collectedWindowUsesActualOwnedGetterAndTerminalPages(@TempDir Path temporary) throws Exception {
        runCold(temporary, true);
    }

    private static void runCold(Path temporary, boolean collected) throws Exception {
        DockerGate.require(); RealConnectorGate.require("mongodb");
        Path jar = ConnectorJars.pathFor("mongodb");
        String database = "write_return_" + UUID.randomUUID().toString().replace("-", "");
        String uri = SharedMongo.replicaSetUrl(database);
        Path log = temporary.resolve("owned-writer.log");
        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process child = new ProcessBuilder(java.toString(),
                "--add-opens=java.base/java.lang=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "--add-opens=java.management/sun.management=ALL-UNNAMED",
                "--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED",
                "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
                "-Dtapstate.benchmark.write-return=true",
                "-Dapp_type=DAAS", "-cp", classpath, Child.class.getName(), jar.toString(), uri, database)
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        OwnedOutput output = new OwnedOutput(log);
        Throwable primary = null;
        try (var mongo = MongoClients.create(uri)) {
            output.awaitLine(child, "WRITE_RETURN_READY");
            try (var changes = mongo.getDatabase(database).getCollection("orders").watch()
                    .fullDocument(com.mongodb.client.model.changestream.FullDocument.UPDATE_LOOKUP)
                    .maxAwaitTime(100, TimeUnit.MILLISECONDS).cursor();
                 var resources = BenchmarkProcessProbe.open(child.pid())) {
                var runtime = resources.runtimeEvidence();
                assertThat(runtime.get("status")).isEqualTo("COMPLETE");
                assertThat(child.isAlive()).as("the original owned child remains alive after attach").isTrue();
                var probe = resources.writeReturnReader();
                BenchmarkWriteReturnCapture capture = collected
                        ? BenchmarkWriteReturnCapture.open(probe, "cold-scope-control") : null;
                try {
                    if (capture == null) { assertThat(probe.start("cold-scope-control")).isTrue(); }
                    List<BenchmarkCausalClock.Sample> samples = new ArrayList<>();
                    if (capture == null) { samples.add(probe.clockSample(0)); }
                    for (int step = 1; step <= 3; step++) {
                        child.getOutputStream().write(("WRITE " + step + "\n").getBytes(StandardCharsets.US_ASCII));
                        child.getOutputStream().flush();
                        output.awaitLine(child, "WRITE_RETURN_DONE " + step);
                        if (capture == null) { samples.add(probe.clockSample(step)); }
                    }
                    if (capture == null) { assertThat(probe.stop()).isTrue(); }
                    else {
                        var result = capture.finish();
                        assertThat(result.calls()).hasSize(3);
                        assertThat(result.summary().completedCalls()).isEqualTo(3);
                        assertThat(result.summary().reportedRecords()).isEqualTo(6);
                        assertThat(result.summary().openCalls()).isZero();
                        assertThat(result.summary().failedCalls()).isZero();
                        assertThat(result.samples().size()).isBetween(2, 512);
                        samples.addAll(result.samples());
                    }
                    byte[] raw = probe.page(0);
                    var page = BenchmarkWriteReturnLedger.decode(raw);
                    assertThat(page.window()).isEqualTo("cold-scope-control");
                    assertThat(page.state()).isEqualTo("RECORDED_SCOPE_UNQUALIFIED");
                    assertThat(page.cursor()).isZero(); assertThat(page.nextCursor()).isEqualTo(3);
                    assertThat(page.totalFrames()).isEqualTo(3);
                    assertThat(page.calls()).extracting(BenchmarkWriteReturnLedger.Call::sequence)
                            .containsExactly(1L, 2L, 3L);
                    assertThat(page.calls()).allSatisfy(call -> {
                        assertThat(call.returnedNormally()).isTrue(); assertThat(call.errors()).isZero();
                        assertThat(call.callbackCount()).isPositive(); assertThat(call.writer()).isEqualTo(1);
                        assertThat(call.lastCallbackExitNanos()).isBetween(call.beganNanos(), call.observedNanos());
                        assertThat(call.target()).isEqualTo("orders"); assertThat(call.keyFields()).containsExactly("id");
                        assertThat(call.scope()).startsWith("state=ORDINARY_ACKNOWLEDGED;reason=PINNED_RUNTIME_SCOPE;")
                                .contains("concern=w:1,");
                    });
                    assertThat(page.calls().stream().mapToLong(call ->
                            call.inserted() + call.modified() + call.removed()).sum()).isEqualTo(6);
                    assertThat(page.calls().stream().flatMap(call -> call.rows().stream())
                            .map(BenchmarkWriteReturnLedger.Row::keys).toList())
                            .containsExactly(List.of(1), List.of(2), List.of(3), List.of(1), List.of(2), List.of(3));
                    assertThat(page.calls().stream().flatMap(call -> call.rows().stream())
                            .map(BenchmarkWriteReturnLedger.Row::kind).toList()).containsExactly(1, 1, 1, 2, 2, 3);
                    var clock = new BenchmarkCausalClock(samples.getFirst().identity(), samples);
                    List<Map<String, Object>> bounds = new ArrayList<>();
                    for (var call : page.calls()) {
                        var point = clock.map(samples.getFirst().identity(), call.observedNanos());
                        var exited = clock.map(samples.getFirst().identity(), call.lastCallbackExitNanos());
                        assertThat(point.widthNanos()).isPositive();
                        bounds.add(Map.of("sequence", call.sequence(), "capturedPointBounds", List.of(point.lowerNanos(), point.upperNanos()),
                                "literalReturnBounds", List.of(exited.lowerNanos(), point.upperNanos()),
                                "scope", call.scope()));
                    }
                    Map<Object, Long> insertedKeys = new java.util.HashMap<>();
                    List<String> coverage = new ArrayList<>();
                    String barrier = UUID.randomUUID().toString();
                    mongo.getDatabase(database).getCollection("orders")
                            .insertOne(new Document("_write_return_barrier", barrier));
                    long oracleDeadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                    org.bson.BsonTimestamp previous = null;
                    boolean barrierSeen = false;
                    while (!barrierSeen && System.nanoTime() < oracleDeadline) {
                        var change = changes.tryNext();
                        if (change == null) { continue; }
                        assertThat(change.getClusterTime()).isNotNull();
                        if (previous != null) { assertThat(change.getClusterTime().compareTo(previous)).isNotNegative(); }
                        previous = change.getClusterTime();
                        var operation = change.getOperationType();
                        if (operation == com.mongodb.client.model.changestream.OperationType.INSERT
                                && barrier.equals(change.getFullDocument().getString("_write_return_barrier"))) {
                            barrierSeen = true; continue;
                        }
                        if (coverage.size() >= 32) { throw new AssertionError("cold target event roster exceeded its bound"); }
                        if (operation == com.mongodb.client.model.changestream.OperationType.INSERT) {
                            long key = ((Number) change.getFullDocument().get("id")).longValue();
                            insertedKeys.put(change.getDocumentKey().get("_id"), key); coverage.add("1:" + key);
                        } else if (operation == com.mongodb.client.model.changestream.OperationType.UPDATE
                                || operation == com.mongodb.client.model.changestream.OperationType.REPLACE) {
                            coverage.add("2:" + ((Number) change.getFullDocument().get("id")).longValue());
                        } else if (operation == com.mongodb.client.model.changestream.OperationType.DELETE) {
                            coverage.add("3:" + insertedKeys.get(change.getDocumentKey().get("_id")));
                        } else { throw new AssertionError("unexpected cold target event: " + operation); }
                    }
                    assertThat(barrierSeen).as("owned post-write target barrier").isTrue();
                    assertThat(coverage).containsExactlyInAnyOrder("1:1", "1:2", "1:3", "2:1", "2:2", "3:3");
                    mongo.getDatabase(database).getCollection("orders")
                            .deleteOne(new Document("_write_return_barrier", barrier));
                    List<Document> rows = mongo.getDatabase(database).getCollection("orders")
                            .find().projection(new Document("_id", 0)).sort(new Document("id", 1)).into(new ArrayList<>());
                    assertThat(rows).containsExactly(new Document("id", 1L).append("seq", 21L),
                            new Document("id", 2L).append("seq", 22L));
                    System.out.println("benchmark-write-return-cold-evidence=" + JsonWriter.write(Map.of(
                            "purpose", collected ? "COLD_CAPTURE_SAMPLER_ASSEMBLY_CONTROL" : "COLD_ACK_SCOPE_AND_CAUSAL_CLOCK_CONTROL", "runtime", runtime,
                            "samples", samples.stream().map(sample -> Map.of("sequence", sample.sequence(),
                                    "pid", sample.identity().pid(), "jvmStartTimeMillis", sample.identity().jvmStartTimeMillis(),
                                    "driverBeforeNanos", sample.driverBeforeNanos(), "driverAfterNanos", sample.driverAfterNanos(),
                                    "ownedNanos", sample.ownedNanos())).toList(),
                            "bounds", bounds, "receiptBase64", Base64.getEncoder().encodeToString(raw),
                            "targetRows", rows, "targetEventCoverage", coverage, "performanceAcceptanceEligible", false,
                            "returnCaptureDelayQualified", false, "samplingCostQualified", false)));
                } finally { if (capture != null) { capture.close(); } }
            }
            child.getOutputStream().write("EXIT\n".getBytes(StandardCharsets.US_ASCII));
            child.getOutputStream().flush();
            assertThat(child.waitFor(10, TimeUnit.SECONDS)).as("owned writer terminated").isTrue();
            assertThat(child.exitValue()).isZero();
        } catch (Exception | Error failure) {
            primary = failure; throw failure;
        } finally {
            try {
                if (child.isAlive()) { child.destroyForcibly(); child.waitFor(5, TimeUnit.SECONDS); }
                if (child.isAlive()) { throw new AssertionError("owned cold writer remains alive; database retained"); }
                try (var cleanup = MongoClients.create(uri)) { cleanup.getDatabase(database).drop(); }
            } catch (Exception | Error failure) {
                if (primary != null) { primary.addSuppressed(failure); } else { throw failure; }
            }
        }
    }

    private static final class OwnedOutput {
        private final Path file;
        private final List<String> retained = new ArrayList<>();
        private int consumed;
        OwnedOutput(Path file) { this.file = file; }
        void awaitLine(Process child, String expected) throws Exception {
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                if (Files.size(file) > 2L * 1024 * 1024) { throw new AssertionError("owned writer log exceeded its bound"); }
                byte[] bytes;
                try (var input = Files.newInputStream(file)) { bytes = input.readNBytes(2 * 1024 * 1024 + 1); }
                if (bytes.length < consumed || bytes.length > 2 * 1024 * 1024) {
                    throw new AssertionError("owned writer log changed identity or exceeded its bound");
                }
                int complete = bytes.length;
                while (complete > consumed && bytes[complete - 1] != '\n') { complete--; }
                if (complete > consumed) {
                    String text = new String(bytes, consumed, complete - consumed, StandardCharsets.UTF_8);
                    consumed = complete;
                    boolean found = false;
                    for (String line : text.split("\n")) {
                        if (retained.size() < 128) { retained.add(line); }
                        if (line.equals(expected)) { found = true; }
                    }
                    if (found) { return; }
                }
                if (!child.isAlive()) { break; }
                Thread.sleep(10);
            }
            throw new AssertionError("owned write-return child did not reach " + expected + ": " + retained);
        }
    }

    public static final class Child {
        public static void main(String[] arguments) throws Exception {
            Path jar = Path.of(arguments[0]);
            var inspected = new ConnectorIntrospector().introspect(List.of(jar));
            var ref = new ConnectorRef(List.of(jar), inspected.className(), inspected.pdkApiVersion(), null, inspected.spec());
            var target = new TargetTable("orders", List.of(
                    new TargetField("id", "source_integer", true, TapstateType.INT64),
                    new TargetField("seq", "source_integer", false, TapstateType.INT64)));
            var config = new SinkConfig("mongodb", Map.of("uri", arguments[1], "database", arguments[2]),
                    WriteMode.UPSERT, DdlPolicy.FAIL, target, new PipelineNode("write_return_smoke", "sink"));
            try (var writer = new PdkSinkPort(id -> ref).open(config);
                 var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.US_ASCII))) {
                System.out.println("WRITE_RETURN_READY"); System.out.flush();
                for (String line; (line = input.readLine()) != null && !line.equals("EXIT");) {
                    int step = Integer.parseInt(line.substring("WRITE ".length()));
                    List<Envelope> events = switch (step) {
                        case 1 -> List.of(insert(1), insert(2), insert(3));
                        case 2 -> List.of(update(1), update(2));
                        case 3 -> List.of(Envelope.delete(1, "orders", Map.of("id", 3L, "seq", 13L), null));
                        default -> throw new AssertionError("unknown cold write step");
                    };
                    long written = writer.write(events).toCompletableFuture().get(15, TimeUnit.SECONDS).written();
                    if (written != events.size()) { throw new AssertionError("cold write reported partial success"); }
                    System.out.println("WRITE_RETURN_DONE " + step); System.out.flush();
                }
            }
            System.exit(0);
        }
        private static Envelope insert(long key) {
            return Envelope.insert(1, "orders", Map.of("id", key, "seq", key + 10), null);
        }
        private static Envelope update(long key) {
            return Envelope.update(1, "orders", Map.of("id", key, "seq", key + 10),
                    Map.of("id", key, "seq", key + 20), null);
        }
    }
}
