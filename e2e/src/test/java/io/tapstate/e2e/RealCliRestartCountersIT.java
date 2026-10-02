package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual healthy CLI composites keep or reset counters on an immutable owned application. */
@RequiresDocker
class RealCliRestartCountersIT {
    private static final String PREFIX = "tapstate.e2e.cli-restart.";
    private static final Duration WAIT = Duration.ofMinutes(2);

    @TempDir
    Path cliHome;

    @Test
    void healthyPlainRestartContinuesAndAnActualRerunStartsFreshCounters() throws Exception {
        Assumptions.assumeTrue(List.of("jar", "jar-sha256", "output").stream()
                .anyMatch(name -> System.getProperty(PREFIX + name) != null),
                "no CLI restart properties supplied; the immutable real-process witness is opt-in");
        Path jar = Path.of(required("jar")).toRealPath();
        BenchmarkCaptureCalibrationLiveRunIT.requireConnectors();
        Map<String, Object> application = PipelineBenchmarkLiveRunIT.artifact(jar);
        String expectedSha = required("jar-sha256");
        assertThat(expectedSha).matches("[0-9a-f]{64}");
        assertThat(application.get("sha256")).as("the selected immutable application").isEqualTo(expectedSha);
        Map<String, Map<String, Object>> connectors = new LinkedHashMap<>();
        for (String id : List.of("mysql", "postgres", "mongodb")) {
            var artifact = PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor(id));
            connectors.put(id, artifact);
        }
        Map<String, Object> cli = cliProvenance();
        Path output = Path.of(required("output"));
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, PipelineBenchmarkLiveRunIT.harnessRoot());
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        try {
            report.begin(Map.of("purpose", "ACTUAL_HEALTHY_CLI_RESTART_COUNTERS", "application", application,
                            "connectors", connectors, "cli", cli, "scope", "HEALTHY_RUNNING_ONLY"),
                    PipelineBenchmarkLiveRunIT.environment(), List.of());
            var workload = BenchmarkWorkloadDefinitions.byId("copy");
            String pipeline = workload.pipelineIds().getFirst();
            try (var fork = BenchmarkForkEnvironment.open(workload, jar, "cli-restart-counters");
                    var client = MongoClients.create(fork.storeUri())) {
                String databaseName = new ConnectionString(fork.storeUri()).getDatabase();
                assertThat(databaseName).matches("bench_copy_[0-9a-f]{12}_control");
                MongoDatabase database = client.getDatabase(databaseName);
                var latest = new MongoObservationStore(client,
                        database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                        database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
                fork.runPhase(workload.phases().getFirst(), true);
                fork.runPhase(workload.phases().get(1), true);
                long baselineMinimum = workload.phases().getFirst().expectedLogicalOutputChanges()
                        + workload.phases().get(1).expectedLogicalOutputChanges();
                long firstGeneration = generation(database, pipeline);
                var before = reading(latest, pipeline, firstGeneration, value -> counter(value).value() >= baselineMinimum);
                var scope = before.scope().orElseThrow();
                Counter baseline = counter(before);
                Set<String> oldEvents = eventIds(database, pipeline);
                assertThat(fork.control().state(pipeline)).contains(PipelineState.RUNNING);

                CliOnce.Run plain = cli(fork, pipeline, false);
                assertThat(plain.exitCode()).as("plain CLI restart stderr: %s", plain.stderr()).isZero();
                assertThat(plain.stdout()).contains(pipeline + "  paused", pipeline + "  running");
                long plainAmount = amount(client.getDatabase(new ConnectionString(fork.externalTargetUri()).getDatabase())) + 1;
                fork.executeOneSourceUpdate("UPDATE bench_copy_orders SET amount=amount+1 WHERE id=1");
                delivered(client.getDatabase(new ConnectionString(fork.externalTargetUri()).getDatabase()), plainAmount);
                var continued = reading(latest, pipeline, firstGeneration, value -> value.scope().equals(before.scope())
                        && value.observation().observedAt().isAfter(before.observation().observedAt())
                        && counter(value).value() > baseline.value());
                assertThat(counter(continued).start()).isEqualTo(baseline.start());
                assertThat(generation(database, pipeline)).isEqualTo(firstGeneration);
                List<Document> cycle = Await.answered("the actual CLI pause and resume to produce real state changes", WAIT, () -> {
                    List<Document> events = events(database, pipeline).stream()
                            .filter(event -> !oldEvents.contains(event.getString("_id")))
                            .filter(event -> scope.pipelineIncarnationId().equals(event.getString("pipelineIncarnationId"))
                                    && event.get("executionGeneration") instanceof Number number
                                    && number.longValue() == firstGeneration).toList();
                    return transition(events, "RUNNING", "PAUSED") && transition(events, "PAUSED", "RUNNING")
                            ? java.util.Optional.of(events) : java.util.Optional.empty();
                });
                report.addFork(evidence("PLAIN_RESTART", fork, databaseName, plain, before, continued,
                        Map.of("targetAmount", plainAmount, "stateChangeEventIds", cycle.stream()
                                .map(event -> event.getString("_id")).toList())));

                assertThat(fork.control().state(pipeline)).contains(PipelineState.RUNNING);
                Counter old = counter(continued);
                CliOnce.Run rerun = cli(fork, pipeline, true);
                assertThat(rerun.exitCode()).as("rerun CLI stderr: %s", rerun.stderr()).isZero();
                assertThat(rerun.stdout()).contains(pipeline + "  stopped", pipeline + "  running");
                long nextGeneration = Math.incrementExact(firstGeneration);
                var fresh = reading(latest, pipeline, nextGeneration, value -> value.scope().orElseThrow()
                        .pipelineIncarnationId().equals(scope.pipelineIncarnationId())
                        && counter(value).start().isAfter(old.start())
                        && counter(value).value() > 0 && counter(value).value() < old.value());
                // Observe the fresh account before the next write; an inherited old total cannot satisfy this.
                assertThat(generation(database, pipeline)).isEqualTo(nextGeneration);
                Counter reset = counter(fresh);
                long rerunAmount = amount(client.getDatabase(new ConnectionString(fork.externalTargetUri()).getDatabase())) + 1;
                fork.executeOneSourceUpdate("UPDATE bench_copy_orders SET amount=amount+1 WHERE id=1");
                delivered(client.getDatabase(new ConnectionString(fork.externalTargetUri()).getDatabase()), rerunAmount);
                var progressing = reading(latest, pipeline, nextGeneration, value -> value.scope().equals(fresh.scope())
                        && counter(value).start().equals(reset.start()) && counter(value).value() > reset.value());
                assertThat(generation(database, pipeline)).isEqualTo(nextGeneration);
                report.addFork(evidence("RERUN_RESTART", fork, databaseName, rerun, continued, progressing,
                        Map.of("firstFreshCounter", reset.value(), "firstFreshCounterStart", reset.start().toString(),
                                "targetAmount", rerunAmount)));
            }
            assertThat(PipelineBenchmarkLiveRunIT.artifact(jar)).isEqualTo(application);
            assertThat(cliProvenance()).isEqualTo(cli);
            for (String id : connectors.keySet()) {
                assertThat(PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor(id))).isEqualTo(connectors.get(id));
            }
            report.completeDiagnostic(Map.of("correctness", "ACTUAL_CLI_CONTINUATION_AND_RESET", "performanceAcceptanceEligible", false,
                    "concreteJobIdCaptured", false, "transactionCommitProven", false,
                    "unverified", List.of("PHYSICAL_COMMAND_COUNTS", "PENDING_PUBLIC_OBSERVATION_WINDOW", "ALL_TELEMETRY_IDENTITIES")));
        } catch (Exception | Error failure) {
            try { report.fail(failure); } catch (RuntimeException recording) { failure.addSuppressed(recording); }
            throw failure;
        }
    }

    private CliOnce.Run cli(BenchmarkForkEnvironment fork, String pipeline, boolean rerun) {
        List<String> arguments = new ArrayList<>(List.of("-c", fork.server().baseUrl().toString(), "-u", "benchmark", "restart", pipeline));
        if (rerun) { arguments.addAll(List.of("--rerun", "-y")); }
        return CliOnce.runWithPassword("benchmark-password", List.of("-Duser.home=" + cliHome), arguments.toArray(String[]::new));
    }

    private record Counter(long value, Instant start) { }

    private static Counter counter(ObservationStore.Stored stored) {
        List<MetricPoint> points = stored.observation().facts().stream()
                .filter(fact -> fact.name().endsWith(".pipeline.records")).flatMap(fact -> fact.points().stream())
                .filter(point -> "out".equals(point.attributes().get("direction"))).toList();
        assertThat(points).as("actual output counter points").isNotEmpty();
        var starts = points.stream().map(MetricPoint::startTime).distinct().toList();
        assertThat(starts).hasSize(1).doesNotContainNull();
        return new Counter(points.stream().mapToLong(MetricPoint::value).sum(), starts.getFirst());
    }

    private static ObservationStore.Stored reading(MongoObservationStore store, String pipeline, long generation,
            java.util.function.Predicate<ObservationStore.Stored> extra) {
        return Await.answered("current RUNNING counter for " + pipeline + " generation " + generation, WAIT,
                () -> store.readStored(pipeline).filter(value -> value.scope().isPresent()
                        && value.scope().orElseThrow().executionGeneration() == generation
                        && value.observation().state() == PipelineState.RUNNING
                        && value.observation().facts().stream()
                                .filter(fact -> fact.name().endsWith(".pipeline.records"))
                                .flatMap(fact -> fact.points().stream())
                                .anyMatch(point -> "out".equals(point.attributes().get("direction")))
                        && extra.test(value)));
    }

    private static long generation(MongoDatabase database, String pipeline) {
        List<Document> rows = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                .find(new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", pipeline))
                .limit(2).into(new ArrayList<>());
        assertThat(rows).hasSize(1);
        Document claim = rows.getFirst();
        assertThat(claim).doesNotContainKeys("ownerNodeId", "ownerBootId", "claimGeneration", "leaseUntil");
        assertThat(claim.get("executionGeneration")).isInstanceOfAny(Integer.class, Long.class);
        long value = ((Number) claim.get("executionGeneration")).longValue();
        assertThat(value).isPositive();
        return value;
    }

    private static long amount(MongoDatabase database) {
        Document row = database.getCollection("bench_copy_orders").find(new Document("id", 1L)).first();
        assertThat(row).isNotNull();
        assertThat(row.get("amount")).isInstanceOf(Number.class);
        return ((Number) row.get("amount")).longValue();
    }

    private static void delivered(MongoDatabase database, long expected) {
        Await.until("a change after the actual CLI command reaches the target", WAIT,
                () -> amount(database) == expected, () -> "targetAmount=" + amount(database) + ", expected=" + expected);
    }

    private static List<Document> events(MongoDatabase database, String pipeline) {
        List<Document> events = database.getCollection(MongoStorePort.PIPELINE_EVENTS)
                .find(new Document("pipelineId", pipeline)).limit(129).into(new ArrayList<>());
        assertThat(events).as("bounded owned state-change trace").hasSizeLessThanOrEqualTo(128);
        return events;
    }

    private static Set<String> eventIds(MongoDatabase database, String pipeline) {
        return events(database, pipeline).stream().map(event -> event.getString("_id")).collect(Collectors.toSet());
    }

    private static boolean transition(List<Document> events, String from, String to) {
        return events.stream().anyMatch(event -> "STATE_CHANGED".equals(event.getString("kind"))
                && from.equals(event.getString("beforeState")) && to.equals(event.getString("afterState")));
    }

    private static Map<String, Object> evidence(String action, BenchmarkForkEnvironment fork, String database,
            CliOnce.Run cli, ObservationStore.Stored before, ObservationStore.Stored after, Map<String, Object> detail) {
        Map<String, Object> result = new LinkedHashMap<>(detail);
        result.put("action", action);
        result.put("database", database);
        result.put("serverOutput", fork.server().output().toString());
        result.put("cli", Map.of("exitCode", cli.exitCode(), "stdout", cli.stdout(), "stderr", cli.stderr()));
        result.put("before", Map.of("incarnation", before.scope().orElseThrow().pipelineIncarnationId(),
                "generation", before.scope().orElseThrow().executionGeneration(), "counterStart", counter(before).start().toString(),
                "recordsOut", counter(before).value()));
        result.put("after", Map.of("incarnation", after.scope().orElseThrow().pipelineIncarnationId(),
                "generation", after.scope().orElseThrow().executionGeneration(), "counterStart", counter(after).start().toString(),
                "recordsOut", counter(after).value()));
        return result;
    }

    private static Map<String, Object> cliProvenance() throws Exception {
        String value = System.getProperty("tapstate.e2e.cli-classpath");
        assertThat(value).as("the actual CLI launch classpath file").isNotBlank();
        Path file = Path.of(value).toRealPath();
        assertThat(Files.size(file)).isLessThanOrEqualTo(262144L);
        String[] entries = Files.readString(file).trim().split(Pattern.quote(File.pathSeparator));
        assertThat(entries.length).isLessThanOrEqualTo(512);
        List<Map<String, Object>> origins = new ArrayList<>();
        for (String entry : entries) {
            Path path = Path.of(entry);
            byte[] cli = null, repl = null;
            if (Files.isDirectory(path)) {
                Path cliFile = path.resolve("io/tapstate/cli/Cli.class"), replFile = path.resolve("io/tapstate/cli/Repl.class");
                if (Files.isRegularFile(cliFile) && Files.isRegularFile(replFile)) {
                    assertThat(Files.size(cliFile)).isBetween(1L, 1_048_576L);
                    assertThat(Files.size(replFile)).isBetween(1L, 1_048_576L);
                    cli = Files.readAllBytes(cliFile); repl = Files.readAllBytes(replFile);
                }
            } else if (Files.isRegularFile(path) && entry.endsWith(".jar")) {
                try (ZipFile zip = new ZipFile(path.toFile())) {
                    var cliEntry = zip.getEntry("io/tapstate/cli/Cli.class");
                    var replEntry = zip.getEntry("io/tapstate/cli/Repl.class");
                    if (cliEntry != null && replEntry != null) {
                        try (var input = zip.getInputStream(cliEntry)) { cli = input.readNBytes(1_048_577); }
                        try (var input = zip.getInputStream(replEntry)) { repl = input.readNBytes(1_048_577); }
                    }
                }
            }
            if (cli != null) {
                assertThat(cli.length).isBetween(1, 1_048_576);
                assertThat(repl.length).isBetween(1, 1_048_576);
                origins.add(Map.of("origin", path.toRealPath().toString(), "cliSha256", sha(cli), "replSha256", sha(repl)));
            }
        }
        assertThat(origins).as("exactly one CLI implementation is reachable by its launch classpath").hasSize(1);
        return Map.of("classpathFile", PipelineBenchmarkLiveRunIT.artifact(file), "implementation", origins.getFirst());
    }

    private static String sha(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static String required(String name) {
        String value = System.getProperty(PREFIX + name);
        if (value == null || value.isBlank()) { throw new IllegalArgumentException("CLI restart witness requires " + PREFIX + name); }
        return value;
    }
}
