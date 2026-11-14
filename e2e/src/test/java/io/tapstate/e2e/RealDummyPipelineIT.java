package io.tapstate.e2e;

import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The release-backed process and CLI witnesses require -Dtapstate.dummy.jar.
 * The MongoDB backlog witness also requires -Dtapstate.e2e.managed-mongo-jar or -Dtapstate.e2e.connectors-dir.
 */
@RequiresDocker
class RealDummyPipelineIT {
    private static final String SHA256 = "fb8bb372cd1fedc0ec43e27dd839b91feee36a70a89adfaae6b6fd788c3352f3";
    private static final String SNAPSHOT = "dummy_op_smoke";

    @TempDir Path work;

    @BeforeEach
    void requirePublishedArtifact() {
        Assumptions.assumeTrue(System.getProperty("tapstate.dummy.jar") != null,
                "no -Dtapstate.dummy.jar: the published Dummy bytes were not supplied");
    }

    @Test
    void publishedDummyRunsThroughTheCli() throws Exception {
        Path published = published();
        Path seeds = Files.createDirectory(work.resolve("seed"));
        Files.copy(published, seeds.resolve("dummy-connector.jar"));
        Path example = Path.of(System.getProperty("tapstate.e2e.dummy-example"));
        for (String file : List.of("src_dummy.tap.yml", "tgt_dummy.tap.yml", "pipeline.tap.yml")) {
            Files.copy(example.resolve(file), work.resolve(file));
        }
        try (var storage = PreparedNativeStore.create(); var server = start(storage, seeds)) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            applyAndStart(server, storage, SNAPSHOT);
            awaitState(control, SNAPSHOT, PipelineState.COMPLETED);
            String observed = cli(server, storage, "status dummy_op_smoke\nmetrics dummy_op_smoke\nexit\n");
            assertThat(observed).contains("dummy_op_smoke  completed", "stage=LANDED",
                    "snapshot.rows.read.dummy_orders  32");
            assertThat(control.initialLoad(SNAPSHOT)).hasValueSatisfying(load -> {
                assertThat(load.get("phase")).isEqualTo("COMPLETED");
                assertThat(((Number) load.get("landedTables")).intValue()).isEqualTo(1);
            });
            assertThat(control.metricsNamed(SNAPSHOT, "snapshot.rows.read")).containsEntry(
                    "snapshot.rows.read.dummy_orders", 32L);
            assertThat(control.errorCount(SNAPSHOT)).contains(0L);
            System.out.println(control.metrics(SNAPSHOT));
            String cdc = "dummy_op_cdc";
            Files.writeString(work.resolve("pipeline.tap.yml"), Files.readString(example.resolve("pipeline.tap.yml"))
                    .replace(SNAPSHOT, cdc).replace("snapshot_only", "cdc_only"));
            cli(server, storage, "apply\nstart " + cdc + "\nexit\n");
            Await.until("target-confirmed Dummy CDC progress", Duration.ofSeconds(25),
                    () -> recordsOut(control, cdc) > 0 && control.durablePosition(cdc, "dummy_orders").isPresent(),
                    () -> control.state(cdc) + " " + control.metrics(cdc));
            assertThat(control.state(cdc)).contains(PipelineState.RUNNING);
            long first = recordsOut(control, cdc);
            assertThat(cli(server, storage, "status " + cdc + "\nmetrics " + cdc + "\nexit\n"))
                    .contains("dummy_op_cdc  running", "records.out  ", "targetAckedPosition.dummy_orders  ");
            Await.until("target-confirmed output to increase", Duration.ofSeconds(12),
                    () -> recordsOut(control, cdc) > first, () -> control.metrics(cdc));
            assertThat(control.errorCount(cdc)).contains(0L);
            System.out.printf("Raw Dummy CDC records.out: %d -> %d%n", first, recordsOut(control, cdc));
            cli(server, storage, "metrics " + cdc + "\nexit\n");

            String failing = "dummy_op_failure";
            Files.writeString(work.resolve("src_failure.tap.yml"),
                    source("src_failure", 32, true, "[1]") + "  batch_timeouts: bad-interval\n");
            Files.writeString(work.resolve("pipeline.tap.yml"),
                    pipeline(failing, "snapshot_only").replace("source: src_dummy", "source: src_failure"));
            cli(server, storage, """
                    apply src_failure.tap.yml
                    discover-schema src_failure
                    apply
                    start dummy_op_failure
                    exit
                    """);
            awaitState(control, failing, PipelineState.FAILED);
            assertThat(control.failureCode(failing)).contains(ConnectorError.CAPTURE_FAILED.code());
            assertThat(control.logs(failing)).contains("bad-interval");
            cli(server, storage, "status " + failing + "\nexit\n");

            long afterFailure = recordsOut(control, cdc);
            Await.until("the independent CDC source to advance after the failing source", Duration.ofSeconds(12),
                    () -> recordsOut(control, cdc) > afterFailure, () -> control.metrics(cdc));
            assertThat(control.state(cdc)).contains(PipelineState.RUNNING);
            assertThat(control.errorCount(cdc)).contains(0L);
            cli(server, storage, "metrics " + cdc + "\nposition " + cdc + "\nstop " + cdc + " --keep-state\nexit\n");
            awaitState(control, cdc, PipelineState.STOPPED);
            assertThat(cli(server, storage, "status " + cdc + "\nexit\n")).contains("dummy_op_cdc  stopped");
            System.out.println("OP server log: " + server.output());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"snapshot_only", "cdc_only", "snapshot_and_cdc"})
    void observesContentAndRealConsumptionSeparately(String mode) throws Exception {
        Path seeds = Files.createDirectory(work.resolve("seed"));
        Path witness = work.resolve("witness.tsv");
        Files.write(seeds.resolve("dummy-connector.jar"),
                ObservedDummyConnectorJar.build(Files.readAllBytes(published()), witness));
        boolean snapshotOnly = mode.equals("snapshot_only");
        int snapshotRows = snapshotOnly ? 32 : 3;
        String id = "dummy_" + mode;
        writeWorkspace(id, mode, snapshotRows, snapshotOnly ? "[1]" : "[1, 2, 3]");
        try (var storage = PreparedNativeStore.create(); var server = start(storage, seeds)) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            try {
                applyAndStart(server, storage, id);
                if (snapshotOnly) {
                    awaitState(control, id, PipelineState.COMPLETED);
                    List<String[]> written = rows(witness, "WRITE");
                    List<String> expected = IntStream.rangeClosed(1, 32).mapToObj(i -> "{id=" + i + "}").toList();
                    assertSnapshotRows(written, expected);
                    assertThat(acked(witness)).containsExactly(32L, 0L, 0L);
                    assertThat(rows(witness, "CDC")).isEmpty();
                    var wrong = new ArrayList<>(expected);
                    wrong.set(31, "{id=999}");
                    assertThatThrownBy(() -> assertSnapshotRows(written, wrong)).isInstanceOf(AssertionError.class);
                } else {
                    if (mode.equals("cdc_only")) assertDeliveryTiming(control, id, witness);
                    Await.until("Dummy to acknowledge I/U/D for two CDC keys", Duration.ofSeconds(25),
                            () -> acked(witness)[2] >= 2 || control.state(id).filter(PipelineState.FAILED::equals).isPresent(),
                            () -> control.state(id) + " " + control.metrics(id));
                    assertThat(control.state(id)).contains(PipelineState.RUNNING);
                    String first = control.metrics(id);
                    if (!mode.equals("cdc_only")) {
                        assertThat(control.initialLoad(id)).hasValueSatisfying(load -> {
                            assertThat(load.get("phase")).isEqualTo("STREAMING");
                            assertThat(((Number) load.get("landedTables")).intValue()).isEqualTo(1);
                        });
                    }
                    Await.until("another Dummy delete acknowledgement", Duration.ofSeconds(10),
                            () -> acked(witness)[2] >= 3, () -> control.metrics(id));
                    Files.writeString(witness, "STOP_REQUEST\t" + System.currentTimeMillis() + "\n",
                            java.nio.file.StandardOpenOption.APPEND);
                    cli(server, storage, "metrics " + id + "\nposition " + id + "\nstop " + id + " --keep-state\nexit\n");
                    awaitState(control, id, PipelineState.STOPPED);
                    Await.until("the real Dummy stream function to return", Duration.ofSeconds(10),
                            () -> rows(witness, "TAIL_END").size() == 1, () -> control.logs(id));
                    long[] stable = acked(witness);
                    long stoppedAt = System.nanoTime();
                    Await.until("target acknowledgements to stay unchanged after stop", Duration.ofSeconds(10), () -> {
                        assertThat(acked(witness)).containsExactly(stable);
                        return System.nanoTime() - stoppedAt >= Duration.ofMillis(3200).toNanos();
                    }, () -> control.logs(id));
                    System.out.println(first);
                    assertCdc(witness, mode.equals("cdc_only") ? 0 : snapshotRows);
                }
            } finally {
                if (Files.exists(witness)) System.out.println(Files.readString(witness));
                System.out.println("OP server log: " + server.output());
            }
        }
    }

    @Test
    void fixedDummyInputsRepeatAcrossIndependentRuns() throws Exception {
        var first = qualifyInputs(1);
        var second = qualifyInputs(2);
        assertThat(second.left()).isEqualTo(first.left());
        assertThat(second.right()).isEqualTo(first.right());
        assertThat(second.inserts()).isEqualTo(first.inserts());
        assertThat(second.firstEventTime()).isGreaterThan(first.firstEventTime());
        assertThat(second.randomValues()).doesNotContainAnyElementsOf(first.randomValues());
    }

    private InputRun qualifyInputs(int run) throws Exception {
        Path seeds = Files.createDirectory(work.resolve("seed-" + run));
        Path witness = work.resolve("inputs-" + run + ".tsv");
        Files.write(seeds.resolve("dummy-connector.jar"),
                ObservedDummyConnectorJar.build(Files.readAllBytes(published()), witness));
        try (var storage = PreparedNativeStore.create(); var server = start(storage, seeds)) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            try {
                control.apply(Map.of("target.tap.yml", source("tgt_dummy", 1, false, "[1]")));
                discoverDummyTarget(control);
                var left = inputSnapshot(control, witness, "left", 1, false);
                var right = inputSnapshot(control, witness, "right", 2, false);
                assertThat(left).filteredOn(right::contains).containsExactly(2, 3);
                assertThat(left).filteredOn(key -> !right.contains(key)).containsExactly(1);
                assertThat(right).filteredOn(key -> !left.contains(key)).containsExactly(4);
                int randomStart = rows(witness, "WRITE").size();
                inputSnapshot(control, witness, "random", 1, true);
                var randomValues = rows(witness, "WRITE").subList(randomStart, randomStart + 3)
                        .stream().map(row -> row[5].substring(row[5].indexOf("sample=") + 7, row[5].length() - 1))
                        .toList();
                assertThat(randomValues).allSatisfy(value -> assertThat(value).hasSize(36)
                        .matches("[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}")).doesNotHaveDuplicates();

                String id = "fixed_inserts";
                control.apply(inputWorkspace(id, "cdc_only", "left_source", inputFields(1, false)));
                control.lifecycle(id, LifecycleVerb.START);
                Await.until("the first three generated Dummy inserts to be acknowledged", Duration.ofSeconds(25),
                        () -> confirmedInputCohort(witness), () -> control.logs(id));
                assertThat(control.errorCount(id)).contains(0L);
                control.stop(id, false);
                awaitState(control, id, PipelineState.STOPPED);
                var cdc = rows(witness, "CDC").subList(0, 3);
                var written = inputCohortWrites(witness, cdc);
                var expected = fixedRows(1);
                assertFixedRows(cdc, expected);
                assertSnapshotRows(written, expected);
                var stamps = cdc.stream().map(row -> Long.parseLong(row[6])).toList();
                assertThat(stamps).allSatisfy(stamp -> assertThat(stamp).isPositive()).isSorted().doesNotHaveDuplicates();
                assertThat(written).extracting(row -> List.of(row[5], row[6]))
                        .containsExactlyInAnyOrderElementsOf(cdc.stream().map(row -> List.of(row[5], row[6])).toList());
                assertThat(written).allSatisfy(row -> assertThat(rows(witness, "ACK"))
                        .anySatisfy(ack -> assertThat(ack[5]).isEqualTo(row[7])));
                return new InputRun(left, right, cdc.stream().map(row -> row[5]).toList(),
                        stamps.getFirst(), randomValues);
            } finally {
                if (Files.exists(witness)) System.out.println(Files.readString(witness));
                System.out.println("OP server log: " + server.output());
            }
        }
    }

    private static boolean confirmedInputCohort(Path witness) {
        var first = rows(witness, "CDC").stream().limit(3).toList();
        if (first.size() < 3) return false;
        var writes = inputCohortWrites(witness, first);
        var acknowledgements = rows(witness, "ACK");
        return first.stream().allMatch(event -> writes.stream().anyMatch(write ->
                write[5].equals(event[5]) && write[6].equals(event[6])
                        && acknowledgements.stream().anyMatch(ack -> ack[5].equals(write[7]))));
    }

    private static List<String[]> inputCohortWrites(Path witness, List<String[]> events) {
        var stamps = events.stream().map(row -> row[6]).toList();
        return rows(witness, "WRITE").stream().filter(row -> stamps.contains(row[6])).toList();
    }

    private static List<Integer> inputSnapshot(
            ControlPlane control, Path witness, String id, int first, boolean random) {
        String sourceId = id + "_source";
        int start = rows(witness, "WRITE").size();
        int sourceStart = rows(witness, "SNAPSHOT").size();
        long acknowledgedInserts = acked(witness)[0];
        var fields = inputFields(first, random);
        var workspace = inputWorkspace(id, "snapshot_only", sourceId, fields);
        control.apply(Map.of("input.tap.yml", workspace.get("input.tap.yml")));
        control.discoverSchema(sourceId, "dummy", Map.of("table_name", "dummy_orders", "table_fields", fields));
        assertThat(control.sourceSchemaTables(sourceId)).containsExactly("dummy_orders");
        assertThat(control.sourceSchemaFields(sourceId, "dummy_orders")).containsExactlyElementsOf(
                fields.stream().map(field -> (String) field.get("name")).toList());
        control.apply(workspace);
        control.lifecycle(id, LifecycleVerb.START);
        awaitState(control, id, PipelineState.COMPLETED);
        var written = rows(witness, "WRITE").subList(start, rows(witness, "WRITE").size());
        assertThat(acked(witness)).containsExactly(acknowledgedInserts + 3, 0L, 0L);
        if (random) {
            assertThat(written).hasSize(3).allSatisfy(row -> assertThat(row[5]).contains("sample="));
        } else {
            assertFixedRows(rows(witness, "SNAPSHOT").subList(sourceStart, sourceStart + 3), fixedRows(first));
            assertSnapshotRows(written, fixedRows(first));
            if (first == 2) {
                assertThatThrownBy(() -> assertSnapshotRows(written, fixedRows(1)))
                        .isInstanceOf(AssertionError.class);
            }
        }
        return written.stream().map(row -> Integer.parseInt(row[5].substring(4, row[5].indexOf(',')))).sorted().toList();
    }

    private static List<Map<String, Object>> inputFields(int first, boolean random) {
        var fields = new ArrayList<Map<String, Object>>();
        fields.add(Map.of("name", "id", "type", "serial(" + first + ",1)", "pri", true));
        fields.add(Map.of("name", "payload", "type", "string(8)", "def", "fixed123"));
        if (random) fields.add(Map.of("name", "sample", "type", "uuid"));
        return fields;
    }

    private static Map<String, String> inputWorkspace(
            String pipelineId, String mode, String sourceId, List<Map<String, Object>> fields) {
        var input = source(sourceId, 3, true, "[1]").replace(
                "table_fields:\n    - {name: id, type: serial, pri: true}",
                "table_fields: " + JsonWriter.write(fields));
        return Map.of("input.tap.yml", input, "target.tap.yml", source("tgt_dummy", 1, false, "[1]"),
                "pipeline.tap.yml", pipeline(pipelineId, mode).replace("source: src_dummy", "source: " + sourceId));
    }

    private static void discoverDummyTarget(ControlPlane control) {
        control.discoverSchema("tgt_dummy", "dummy", Map.of("table_name", "dummy_orders",
                "table_fields", List.of(Map.of("name", "id", "type", "serial", "pri", true)),
                "initial_totals", 1, "write_interval_totals", 0, "write_log", false));
    }

    private static List<String> fixedRows(int first) {
        return IntStream.range(first, first + 3).mapToObj(key -> "{id=" + key + ", payload=fixed123}").toList();
    }

    private static void assertFixedRows(List<String[]> rows, List<String> expected) {
        assertThat(rows).allSatisfy(row -> {
            assertThat(row[2]).isEqualTo("dummy_orders");
            assertThat(row[3]).isEqualTo("I");
            assertThat(row[4]).isEqualTo("null");
        }).extracting(row -> row[5]).containsExactlyElementsOf(expected);
    }

    private record InputRun(List<Integer> left, List<Integer> right, List<String> inserts,
            long firstEventTime, List<String> randomValues) {}

    @Test
    void qualifiesSourceAndSinkLimitsWithoutAnObserver() throws Exception {
        Path seeds = Files.createDirectory(work.resolve("rate-seed"));
        Files.copy(published(), seeds.resolve("dummy-connector.jar"));
        try (var storage = PreparedNativeStore.create(); var documents = new NativeSrsDocuments(storage);
                var server = start(storage, seeds)) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            try {
                var limited = sampleCdcRate(control, documents, "source_limited", 10, 0);
                var unrestricted = sampleCdcRate(control, documents, "source_unrestricted", 0, 0);
                var sinkLimited = sampleCdcRate(control, documents, "sink_limited", 10, 1);
                assertThat(limited.sourcePerSecond()).isBetween(5.0, 15.0);
                // An unrestricted source can fill the bounded direct channel during warm-up.
                assertThat(unrestricted.sourceFrom()).isGreaterThan(2 * limited.sourceFrom());
                assertThat(unrestricted.outputPerSecond()).isGreaterThan(2 * limited.outputPerSecond());
                assertThat(sinkLimited.sourcePerSecond()).isBetween(5.0, 15.0);
                assertThat(sinkLimited.outputPerSecond()).isLessThan(limited.outputPerSecond());
                assertThat(sinkLimited.unconfirmedGrowth()).as("extra rows waiting for target confirmation")
                        .isPositive();
            } finally {
                System.out.println("OP server log: " + server.output());
            }
        }
    }

    private static CdcRate sampleCdcRate(ControlPlane control, NativeSrsDocuments documents,
            String id, int sourceLimit, int sinkLimit) {
        applyRatePipeline(control, id, sourceLimit, sinkLimit);
        long startRequested = System.nanoTime();
        control.lifecycle(id, LifecycleVerb.START);
        var from = Await.answered(id + " warm-up with acknowledged output", Duration.ofSeconds(20),
                () -> rateReading(control, id));
        long sourceFrom = capturedRows(documents, id);
        long warmupMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startRequested);
        // The observation publisher can re-publish the same underlying job counters.
        var to = Await.answered(id + " fresh counters after the measurement window", Duration.ofSeconds(15),
                () -> rateReading(control, id).filter(reading ->
                        !reading.at().isBefore(from.at().plusSeconds(3)) && reading.output() > from.output()));
        var rate = new CdcRate(from, to, sourceFrom, capturedRows(documents, id));
        long stopRequested = System.nanoTime();
        control.stop(id, false);
        awaitState(control, id, PipelineState.STOPPED);
        long stopMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stopRequested);
        assertThat(control.errorCount(id)).contains(0L);
        System.out.printf(Locale.ROOT,
                "%s: source quota %d/1000 ms, sink quota %d/250 ms, source flush 250 ms, "
                        + "one sink writer with batches of up to 8 rows, from %s to %s, "
                        + "warm-up %d ms, source %d -> %d (%.2f/s), input %.2f/s, driven %.2f/s, confirmed %.2f/s, "
                        + "unconfirmed growth %d, average source-to-confirmation %.1f ms, stop %d ms, errors 0%n",
                id, sourceLimit, sinkLimit, rate.from(), rate.to(), warmupMillis, rate.sourceFrom(), rate.sourceTo(),
                rate.sourcePerSecond(), rate.inputPerSecond(), rate.perSecond(rate.to().driven() - rate.from().driven()),
                rate.outputPerSecond(), rate.unconfirmedGrowth(), to.delivery().averageMillisSince(from.delivery()),
                stopMillis);
        System.out.printf(Locale.ROOT, "%s stages: transform %.3f ms/row, sink processor %.3f ms/call%n", id,
                to.transform().averageMillisSince(from.transform()), to.target().averageMillisSince(from.target()));
        return rate;
    }

    private static long capturedRows(NativeSrsDocuments documents, String id) {
        var window = documents.directChannel(id, id + "_source").get("current", Document.class);
        long lastSequence = window.get("lastBoundary", Document.class).getLong("sequence");
        // The direct capture numbers rows and batch boundaries consecutively, starting at zero.
        return lastSequence + 1 - window.getLong("recordedThrough");
    }

    private static Optional<RateReading> rateReading(ControlPlane control, String id) {
        var state = control.state(id);
        if (state.filter(PipelineState.FAILED::equals).isPresent()) {
            throw new AssertionError(id + " failed while measuring rates: " + control.logs(id));
        }
        var facts = control.metricFacts(id);
        var movement = metricPoints(facts, "tapstate.pipeline.records");
        var drivenPoints = metricPoints(facts, "tapstate.pipeline.records.driven");
        var deliveries = metricPoints(facts, "tapstate.pipeline.record.delivery.duration");
        var stages = metricPoints(facts, "tapstate.pipeline.process.duration");
        long input = movement.stream().filter(point -> "in".equals(metricAttribute(point, "direction")))
                .mapToLong(point -> ((Number) point.get("value")).longValue()).sum();
        long output = movement.stream().filter(point -> "out".equals(metricAttribute(point, "direction")))
                .mapToLong(point -> ((Number) point.get("value")).longValue()).sum();
        if (input <= 0 || output <= 0 || drivenPoints.isEmpty() || deliveries.isEmpty()) {
            return Optional.empty();
        }
        var driven = drivenPoints.getFirst();
        return Optional.of(new RateReading(Instant.parse((String) driven.get("observedAt")), input,
                ((Number) driven.get("value")).longValue(), output, Timing.of(deliveries.getFirst()),
                stageTiming(stages, "transform"), stageTiming(stages, "sink")));
    }

    private static Object metricAttribute(Map<?, ?> point, String name) {
        return ((Map<?, ?>) point.get("attributes")).get(name);
    }

    private static Timing stageTiming(List<Map<?, ?>> stages, String name) {
        return stages.stream().filter(point -> name.equals(metricAttribute(point, "stage")))
                .map(Timing::of).findFirst().orElseThrow(() -> new AssertionError("Missing stage timing: " + name));
    }

    private static List<Map<?, ?>> metricPoints(List<Map<String, Object>> facts, String name) {
        return facts.stream().filter(fact -> name.equals(fact.get("name")))
                .flatMap(fact -> ((List<?>) fact.get("points")).stream())
                .<Map<?, ?>>map(point -> (Map<?, ?>) point).toList();
    }

    private record Timing(long count, double seconds) {
        static Timing of(Map<?, ?> point) {
            return new Timing(((Number) point.get("count")).longValue(), ((Number) point.get("sum")).doubleValue());
        }

        double averageMillisSince(Timing earlier) {
            assertThat(count).isGreaterThan(earlier.count);
            return 1_000 * (seconds - earlier.seconds) / (count - earlier.count);
        }
    }

    private record RateReading(Instant at, long input, long driven, long output,
            Timing delivery, Timing transform, Timing target) {}

    private record CdcRate(RateReading from, RateReading to, long sourceFrom, long sourceTo) {
        double sourcePerSecond() { return perSecond(sourceTo - sourceFrom); }
        double inputPerSecond() { return perSecond(to.input() - from.input()); }
        double outputPerSecond() { return perSecond(to.output() - from.output()); }
        long unconfirmedGrowth() { return (to.input() - from.input()) - (to.output() - from.output()); }
        double perSecond(long records) { return 1_000.0 * records / Duration.between(from.at(), to.at()).toMillis(); }
    }

    private static void applyRatePipeline(ControlPlane control, String id, int sourceLimit, int sinkLimit) {
        String sourceId = id + "_source";
        String targetId = id + "_target";
        var input = source(sourceId, 32, true, "[1]")
                .replace("tables: [dummy_orders]\n", "tables: [dummy_orders]\nsrs: {enabled: false}\n")
                .replace("incremental_interval_totals: 1", "incremental_interval_totals: " + sourceLimit)
                + "  batch_timeouts: '250'\n";
        var target = source(targetId, 1, false, "[1]")
                .replace("write_interval: 1000", "write_interval: 250")
                .replace("write_interval_totals: 0", "write_interval_totals: " + sinkLimit);
        var sources = Map.of("source.tap.yml", input, "target.tap.yml", target);
        control.apply(sources);
        var fields = List.of(Map.of("name", "id", "type", "serial", "pri", true));
        var schema = Map.<String, Object>of("table_name", "dummy_orders", "table_fields", fields);
        control.discoverSchema(sourceId, "dummy", schema);
        control.discoverSchema(targetId, "dummy", schema);
        control.apply(Map.of("source.tap.yml", input, "target.tap.yml", target,
                "pipeline.tap.yml", pipeline(id, "cdc_only")
                .replace("source: src_dummy", "source: " + sourceId)
                .replace("source: tgt_dummy", "source: " + targetId
                        + "\n      execution: {parallelism: 1, batch: {max_records: 8}}")));
    }

    @Test
    void replaysTheSamePrewrittenMongoBatchThroughRealDummy() throws Exception {
        String managedMongoJar = System.getProperty("tapstate.e2e.managed-mongo-jar");
        Path mongoJar;
        if (managedMongoJar == null || managedMongoJar.isBlank()) {
            RealConnectorGate.require("mongodb");
            mongoJar = ConnectorJars.pathFor("mongodb");
        } else {
            mongoJar = Path.of(managedMongoJar);
        }
        System.out.printf("MongoDB source artifact: %s SHA-256 %s%n", mongoJar.getFileName(), digest(mongoJar));
        String database = "dummy_backlog_" + UUID.randomUUID().toString().replace("-", "");
        String uri = SharedMongo.replicaSetUrl(database);
        try (var client = MongoClients.create(uri)) {
            var db = client.getDatabase(database).withWriteConcern(WriteConcern.MAJORITY);
            String table = "dummy_orders";
            try {
                db.createCollection(table);
                db.runCommand(new Document("collMod", table)
                        .append("changeStreamPreAndPostImages", new Document("enabled", true)));
                var orders = db.getCollection(table);
                try (var changes = orders.watch().maxAwaitTime(200, TimeUnit.MILLISECONDS).cursor();
                        var session = client.startSession()) {
                    orders.insertOne(new Document("_id", 1).append("payload", "outside"));
                    var earlier = readChanges(changes, 1).getFirst();
                    int earlierSecond = earlier.getClusterTime().getTime();
                    Await.until("MongoDB server time past the earlier controls", Duration.ofSeconds(5),
                            () -> db.runCommand(new Document("hello", 1)).getDate("localTime").toInstant()
                                    .getEpochSecond() > earlierSecond, () -> "earlier second " + earlierSecond);

                    long prewriteStart = System.nanoTime();
                    session.withTransaction(() -> {
                        orders.insertOne(session, new Document("_id", 2).append("payload", "inserted"));
                        orders.updateOne(session, new Document("_id", 2),
                                new Document("$set", new Document("payload", "after")));
                        orders.deleteOne(session, new Document("_id", 2));
                        return null;
                    }, TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).build());
                    long prewriteMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - prewriteStart);
                    var batch = readChanges(changes, 3);
                    var batchTime = batch.getFirst().getClusterTime();
                    assertThat(batchTime.getTime()).isGreaterThan(earlierSecond);
                    assertThat(batch).extracting(ChangeStreamDocument::getClusterTime).containsOnly(batchTime);
                    assertThat(batch).extracting(change -> change.getOperationType().getValue())
                            .containsExactly("insert", "update", "delete");
                    assertThat(batch).extracting(change -> change.getDocumentKey().getInt32("_id").getValue())
                            .containsExactly(2, 2, 2);
                    assertThat(batch).extracting(ChangeStreamDocument::getResumeToken).doesNotHaveDuplicates();
                    Instant cutoff = Instant.ofEpochSecond(batchTime.getTime());
                    System.out.printf("MongoDB earlier control: %s; cutoff: %s; prewrite: %d ms%n",
                            earlier.getClusterTime(), cutoff, prewriteMillis);
                    batch.forEach(change -> System.out.printf("MongoDB event: %s %s %s token=%s%n",
                            change.getOperationType(), change.getDocumentKey(), change.getClusterTime(),
                            change.getResumeToken()));

                    replayMongoBatch(uri, cutoff, 1, mongoJar);
                    replayMongoBatch(uri, cutoff, 2, mongoJar);
                }
            } finally {
                db.drop();
            }
        }
    }

    private void replayMongoBatch(String uri, Instant cutoff, int run, Path mongoJar) throws Exception {
        Path seeds = Files.createDirectory(work.resolve("mongo-seed-" + run));
        Path witness = work.resolve("mongo-backlog-" + run + ".tsv");
        Files.write(seeds.resolve("dummy-connector.jar"),
                ObservedDummyConnectorJar.build(Files.readAllBytes(published()), witness));
        String id = "dummy_backlog";
        try (var storage = PreparedNativeStore.create(); var server = start(storage, seeds)) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector("mongodb", Files.readAllBytes(mongoJar));
            try {
                // Direct capture positions against MongoDB's history. A fresh shared ring has no history.
                var source = """
                        version: tapstate/v1
                        kind: source
                        id: src_mongo
                        connector: mongodb
                        mode: cdc
                        tables: [dummy_orders]
                        srs: {enabled: false}
                        config: { uri: "%s", preImage: true }
                        """.formatted(uri);
                var target = source("tgt_dummy", 1, false, "[1]");
                control.apply(Map.of("mongo.tap.yml", source, "target.tap.yml", target));
                control.discoverSchema("src_mongo", "mongodb", Map.of("uri", uri, "preImage", true));
                discoverDummyTarget(control);
                var pipeline = pipeline(id, "cdc_only").replace("source: src_dummy", "source: src_mongo")
                        .replace("read_mode: cdc_only}", "read_mode: cdc_only, start_from: '" + cutoff + "'}");
                control.apply(Map.of("mongo.tap.yml", source, "target.tap.yml", target, "pipeline.tap.yml", pipeline));
                long startMillis = System.currentTimeMillis();
                control.lifecycle(id, LifecycleVerb.START);
                Await.until("the prewritten MongoDB delete acknowledgement", Duration.ofSeconds(30),
                        () -> acked(witness)[2] > 0 || control.state(id).filter(PipelineState.FAILED::equals).isPresent(),
                        () -> control.state(id) + " " + control.logs(id));
                assertThat(control.state(id)).as(control.logs(id)).contains(PipelineState.RUNNING);
                assertThat(control.errorCount(id)).contains(0L);
                String position = Await.answered("MongoDB backlog durable target position", Duration.ofSeconds(10),
                        () -> control.durablePosition(id, "dummy_orders"));
                control.stop(id, false);
                awaitState(control, id, PipelineState.STOPPED);
                var written = rows(witness, "WRITE");
                assertThat(written).allSatisfy(row -> {
                    assertThat(row[1]).isEqualTo("dummy_orders");
                    assertThat(row[2]).isEqualTo("dummy_orders");
                    assertThat(Long.parseLong(row[6])).isEqualTo(cutoff.toEpochMilli());
                }).extracting(row -> List.of(row[3], row[4], row[5])).containsExactly(
                        List.of("I", "null", "{_id=2, payload=inserted}"),
                        List.of("U", "{_id=2, payload=inserted}", "{_id=2, payload=after}"),
                        List.of("D", "{_id=2, payload=after}", "null"));
                assertThat(acked(witness)).containsExactly(1L, 1L, 1L);
                var acks = rows(witness, "ACK");
                assertThat(written).allSatisfy(row -> assertThat(acks)
                        .anySatisfy(ack -> assertThat(ack[5]).isEqualTo(row[7])));
                var lastAck = acks.stream().filter(ack -> ack[5].equals(written.getLast()[7])).findFirst().orElseThrow();
                assertThat(lastAck[3]).isEqualTo("1");
                long catchupMillis = Long.parseLong(lastAck[4]) - startMillis;
                assertThat(catchupMillis).isPositive();
                System.out.printf("MongoDB backlog run %d: %d rows, start-to-last-ack %d ms, position %s%n",
                        run, written.size(), catchupMillis, position);
            } finally {
                if (Files.exists(witness)) System.out.println(Files.readString(witness));
                System.out.println("OP server log: " + server.output());
            }
        }
    }

    private static List<ChangeStreamDocument<Document>> readChanges(
            MongoCursor<ChangeStreamDocument<Document>> cursor, int count) {
        var changes = new ArrayList<ChangeStreamDocument<Document>>();
        Await.until(count + " MongoDB change stream events", Duration.ofSeconds(10), () -> {
            var change = cursor.tryNext();
            if (change != null) changes.add(change);
            return changes.size() == count;
        }, changes::toString);
        return changes;
    }

    private static void assertSnapshotRows(List<String[]> written, List<String> expected) {
        assertThat(written).allSatisfy(row -> {
            assertThat(row[1]).isEqualTo("dummy_orders");
            assertThat(row[2]).isEqualTo("dummy_orders");
            assertThat(row[3]).isEqualTo("I");
            assertThat(row[4]).isEqualTo("null");
        }).extracting(row -> row[5]).containsExactlyInAnyOrderElementsOf(expected);
    }

    private static void assertCdc(Path witness, int snapshotRows) {
        var source = rows(witness, "CDC");
        var written = rows(witness, "WRITE");
        var snapshot = rows(witness, "SNAPSHOT");
        assertThat(snapshot).hasSize(snapshotRows);
        assertThat(source).hasSizeGreaterThanOrEqualTo(9);
        assertThat(rows(witness, "TAIL_START")).singleElement().satisfies(line ->
                assertThat(line[1]).isEqualTo("DummyOffset"));
        for (int index = 0; index < 9; index += 3) {
            String key = "{id=" + (1 + index / 3) + "}";
            assertThat(source.get(index)).containsSequence("I", "null", key);
            assertThat(source.get(index + 1)).containsSequence("U", key, key);
            assertThat(source.get(index + 2)).containsSequence("D", key, "null");
        }
        var cohort = new ArrayList<>(snapshot);
        cohort.addAll(source.subList(0, 9));
        var target = new ArrayList<String[]>();
        for (String[] row : cohort) {
            assertThat(Long.parseLong(row[6])).isPositive();
            var matches = written.stream().filter(write -> write[6].equals(row[6]) && write[3].equals(row[3])
                    && write[4].equals(row[4]) && write[5].equals(row[5])).toList();
            assertThat(matches).singleElement().satisfies(write -> {
                assertThat(write[1]).isEqualTo("dummy_orders");
                assertThat(write[2]).isEqualTo("dummy_orders");
            });
            target.add(matches.getFirst());
        }
        if (snapshotRows > 0) {
            assertSnapshotRows(written.subList(0, snapshotRows),
                    IntStream.rangeClosed(1, snapshotRows).mapToObj(i -> "{id=" + i + "}").toList());
            assertThat(written.subList(0, snapshotRows)).containsExactlyInAnyOrderElementsOf(
                    target.subList(0, snapshotRows));
        }
        long stopRequested = Long.parseLong(rows(witness, "STOP_REQUEST").getFirst()[1]);
        assertThat(source).filteredOn(row -> Long.parseLong(row[8]) < stopRequested)
                .extracting(row -> List.of(row[3], row[4], row[5], row[6])).doesNotHaveDuplicates();
        assertThat(written).extracting(row -> List.of(row[3], row[4], row[5], row[6])).doesNotHaveDuplicates();
        for (String[] ack : rows(witness, "ACK")) {
            var batch = written.stream().filter(row -> row[7].equals(ack[5])).toList();
            assertThat(java.util.Arrays.copyOfRange(ack, 1, 4)).containsExactly(
                    String.valueOf(batch.stream().filter(row -> row[3].equals("I")).count()),
                    String.valueOf(batch.stream().filter(row -> row[3].equals("U")).count()),
                    String.valueOf(batch.stream().filter(row -> row[3].equals("D")).count()));
        }
        assertThat(target).allSatisfy(row -> assertThat(rows(witness, "ACK"))
                .anySatisfy(ack -> assertThat(ack[5]).isEqualTo(row[7])));
    }

    private static void assertDeliveryTiming(ControlPlane control, String id, Path witness) {
        var timing = Await.answered("a Dummy delivery duration histogram", Duration.ofSeconds(25),
                () -> control.metricFacts(id).stream()
                        .filter(fact -> fact.get("name").equals("tapstate.pipeline.record.delivery.duration"))
                        .flatMap(fact -> ((List<?>) fact.get("points")).stream())
                        .map(point -> (Map<?, ?>) point).findFirst());
        long samples = ((Number) timing.get("count")).longValue();
        double measuredMillis = ((Number) timing.get("sum")).doubleValue() * 1000;
        long sourceToAck = 0;
        long sourceToEmission = 0;
        long matched = 0;
        var written = rows(witness, "WRITE");
        var source = rows(witness, "CDC");
        for (String[] ack : rows(witness, "ACK")) {
            var batch = written.stream().filter(write -> write[7].equals(ack[5])).toList();
            for (String[] write : batch) {
                var original = source.stream().filter(row -> row[3].equals(write[3]) && row[5].equals(write[5])
                        && row[4].equals(write[4]) && row[6].equals(write[6])).findFirst().orElseThrow();
                long stamp = Long.parseLong(original[6]);
                assertThat(stamp).isPositive();
                sourceToAck += Long.parseLong(ack[4]) - stamp;
                sourceToEmission += Long.parseLong(original[8]) - stamp;
                matched++;
            }
            if (matched >= samples) break;
        }
        assertThat(matched).isPositive().isEqualTo(samples);
        assertThat(sourceToEmission).as("source batching wait in the observed sample").isGreaterThan(1000);
        assertThat(measuredMillis).as("histogram uses source stamp through real acknowledgement")
                .isBetween(sourceToAck - 5.0, sourceToAck + 500.0);
        System.out.printf("CDC timing: %d samples, %.0f ms histogram, %d ms stamp-to-ack, "
                + "%d ms stamp-to-PDK-emission%n", samples, measuredMillis, sourceToAck, sourceToEmission);
    }

    private static long recordsOut(ControlPlane control, String pipeline) {
        return control.metricsNamed(pipeline, "records.out").getOrDefault("records.out", 0L);
    }

    private static List<String[]> rows(Path witness, String kind) {
        try {
            if (!Files.exists(witness)) return List.of();
            return Files.readAllLines(witness).stream()
                    .filter(line -> line.equals(kind) || line.startsWith(kind + "\t"))
                    .map(line -> line.split("\t")).toList();
        } catch (java.io.IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }

    private static long[] acked(Path witness) {
        long[] counts = new long[3];
        for (String[] result : rows(witness, "ACK")) {
            for (int i = 0; i < counts.length; i++) counts[i] += Long.parseLong(result[i + 1]);
        }
        return counts;
    }

    private static Path published() throws Exception {
        Path published = Path.of(System.getProperty("tapstate.dummy.jar"));
        assertThat(digest(published)).isEqualTo(SHA256);
        return published;
    }

    private static String digest(Path artifact) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(artifact)));
    }

    private void writeWorkspace(String id, String mode, int snapshotRows, String operations) throws Exception {
        Files.writeString(work.resolve("src_dummy.tap.yml"), source("src_dummy", snapshotRows, true, operations));
        Files.writeString(work.resolve("tgt_dummy.tap.yml"), source("tgt_dummy", 1, false, "[1]"));
        Files.writeString(work.resolve("pipeline.tap.yml"), pipeline(id, mode));
    }

    private static RealProcessServer start(PreparedNativeStore storage, Path seeds) {
        return RealProcessServer.startPrepared(storage.configurationUri(), "127.0.0.1",
                port -> List.of("--tapstate.connectors.seed-dir=" + seeds));
    }

    private void applyAndStart(ServerHandle server, PreparedNativeStore storage, String id) {
        String applied = cli(server, storage, """
                apply src_dummy.tap.yml
                apply tgt_dummy.tap.yml
                discover-schema src_dummy -o json
                discover-schema tgt_dummy -o json
                apply
                start %s
                exit
                """.formatted(id));
        assertThat(applied).contains("dummy_orders", "primaryKey", "serial", "created  pipeline  " + id);
    }

    private static void awaitState(ControlPlane control, String id, PipelineState state) {
        Await.until(id + " to be " + state, Duration.ofSeconds(25),
                () -> control.state(id).filter(state::equals).isPresent(),
                () -> control.state(id) + " " + control.metrics(id) + " " + control.logs(id));
    }

    private String cli(ServerHandle server, PreparedNativeStore storage, String script) {
        var run = CliOnce.runSession(storage.password(), script, "-w", work.toString(),
                "-c", server.baseUrl().toString(), "-u", storage.username());
        System.out.println(run.stdout() + run.stderr());
        assertThat(run.exitCode()).as("CLI script: %s", script).isZero();
        return run.stdout();
    }

    private static String source(String id, int rows, boolean capture, String operations) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: dummy
                %sconfig:
                  table_name: dummy_orders
                  table_fields:
                    - {name: id, type: serial, pri: true}
                  initial_totals: %d
                  incremental_interval: 1000
                  incremental_interval_totals: 1
                  incremental_types: %s
                  write_interval: 1000
                  write_interval_totals: 0
                  write_log: false
                """.formatted(id, capture ? "mode: cdc\ntables: [dummy_orders]\n" : "", rows, operations);
    }

    private static String pipeline(String id, String mode) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: src_dummy
                settings: {read_mode: %s}
                transforms:
                  - {id: pass_rows, from: [dummy_orders], type: filter, expr: "true"}
                serve:
                  from: pass_rows
                  sync:
                    - source: tgt_dummy
                """.formatted(id, mode);
    }
}
