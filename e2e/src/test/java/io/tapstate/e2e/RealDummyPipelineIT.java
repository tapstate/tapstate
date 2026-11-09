package io.tapstate.e2e;

import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
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
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The release-backed process and CLI witness is opt-in via -Dtapstate.dummy.jar. */
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
        writeWorkspace(SNAPSHOT, "snapshot_only", 32, "[1]");
        try (var storage = PreparedNativeStore.create(); var server = start(storage, seeds)) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            applyAndStart(server, storage, SNAPSHOT);
            awaitState(control, SNAPSHOT, PipelineState.COMPLETED);
            String observed = cli(server, storage, "status dummy_op_smoke\nmetrics dummy_op_smoke\nexit\n");
            assertThat(observed).contains("dummy_op_smoke  completed", "snapshot.rowsDone = 32");
            assertThat(control.initialLoad(SNAPSHOT)).hasValueSatisfying(load -> {
                assertThat(load.get("phase")).isEqualTo("COMPLETED");
                assertThat(((Number) load.get("landedTables")).intValue()).isEqualTo(1);
            });
            assertThat(control.metricsNamed(SNAPSHOT, "snapshot.rows.read")).containsEntry(
                    "snapshot.rows.read.dummy_orders", 32L);
            assertThat(control.errorCount(SNAPSHOT)).contains(0L);
            System.out.println(control.metrics(SNAPSHOT));
            String cdc = "dummy_op_cdc";
            Files.writeString(work.resolve("pipeline.tap.yml"), pipeline(cdc, "cdc_only"));
            cli(server, storage, "apply\nstart " + cdc + "\nexit\n");
            Await.until("target-confirmed Dummy CDC progress", Duration.ofSeconds(25),
                    () -> recordsOut(control, cdc) > 0 && control.metrics(cdc).contains("targetAckedPosition"),
                    () -> control.state(cdc) + " " + control.metrics(cdc));
            assertThat(control.state(cdc)).contains(PipelineState.RUNNING);
            long first = recordsOut(control, cdc);
            Await.until("target-confirmed output to increase", Duration.ofSeconds(12),
                    () -> recordsOut(control, cdc) > first, () -> control.metrics(cdc));
            assertThat(control.errorCount(cdc)).contains(0L);
            System.out.printf("Raw Dummy CDC records.out: %d -> %d%n", first, recordsOut(control, cdc));

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
                    Thread.sleep(3200);
                    assertThat(acked(witness)).containsExactly(stable);
                    System.out.println(first);
                    assertCdc(witness, mode.equals("cdc_only") ? 0 : snapshotRows);
                }
            } finally {
                if (Files.exists(witness)) System.out.println(Files.readString(witness));
                System.out.println("OP server log: " + server.output());
            }
        }
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
        assertThat(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(Files.readAllBytes(published)))).isEqualTo(SHA256);
        return published;
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
