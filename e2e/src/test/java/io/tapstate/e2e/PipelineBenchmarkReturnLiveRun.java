package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.tapstate.e2e.BenchmarkReturnExternalAcquirer.*;

/** Executes the predeclared original alternating forks and acquires each closed method independently. */
final class PipelineBenchmarkReturnLiveRun {
    private PipelineBenchmarkReturnLiveRun() { }
    static final int MAX_SLOTS = 30, MAX_PROPERTIES = 128;

    record Slot(String workload, PipelineBenchmarkComparison.Arm arm, int ordinal,
                Path registration, String registrationSha256, Path report, Path outcomeXml) { }
    record Config(Path repository, Path maven, Path javaHome, Path a, Path b,
                  BenchmarkReturnWorkBudgetBindings.Blob aBytes, BenchmarkReturnWorkBudgetBindings.Blob bBytes, List<Slot> slots) {
        Config { slots = List.copyOf(slots); }
    }

    public static void main(String[] args) throws Exception {
        require(args.length == 3, "expected frozen execution plan, its SHA-256 and a new evidence directory");
        Path input = path(args[0]), output = path(args[2]);
        require(output.getParent().toRealPath().equals(output.getParent()) && !Files.exists(output, LinkOption.NOFOLLOW_LINKS),
                "execution evidence directory must be new under a canonical existing parent");
        Files.createDirectory(output);
        var reads = new ArrayList<Map<String, Object>>();
        var plan = acquire(input, INPUT_BYTES, null, reads, item -> Files.newInputStream(item, LinkOption.NOFOLLOW_LINKS));
        require(plan.blob().sha256().equals(args[1]), "execution plan differs from independent frozen SHA-256");
        var config = config(json(plan.bytes(), new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(65536, 16, 65536, INPUT_BYTES, 256, 24)));
        Files.write(output.resolve("execution-plan.acquired.json"), plan.bytes(), StandardOpenOption.CREATE_NEW);
        var originals = BenchmarkWorkloadDefinitions.all().stream().map(workload -> BenchmarkWorkloadDefinitions.steadyPilot(workload.id())).toList();
        var completed = new ArrayList<Map<String, Object>>(); int[] next = {0};
        try {
            var result = PipelineBenchmarkReturnHarness.run(PipelineBenchmarkHarness.Gate.OBSERVABILITY_COST,
                    config.a(), config.b(), originals, (workload, arm, ordinal, app) -> {
                int at = next[0]++;
                require(at < config.slots().size(), "execution exceeds its predeclared original slots");
                var slot = config.slots().get(at);
                require(slot.workload().equals(workload.id()) && slot.arm() == arm && slot.ordinal() == ordinal
                        && app.equals(arm == PipelineBenchmarkComparison.Arm.A ? config.a() : config.b()),
                        "executed slot differs from original alternating schedule");
                Path evidence = output.resolve(String.format(java.util.Locale.ROOT, "%02d-%s-%s-%d", at + 1, workload.id(), arm, ordinal));
                Files.createDirectory(evidence);
                var acquired = execute(config, slot, evidence);
                var fork = BenchmarkReturnCompletedMethod.verify(acquired);
                completed.add(fork.evidence());
                write(output.resolve(String.format(java.util.Locale.ROOT, "completed-%02d.json", at + 1)), fork.evidence());
                return fork;
            }, config.aBytes(), config.bBytes());
            result.requireCompletedMethodCost();
            require(next[0] == MAX_SLOTS && completed.size() == MAX_SLOTS, "original thirty slots are incomplete");
            var finalReport = new LinkedHashMap<String, Object>();
            finalReport.put("method", BenchmarkReturnMethodAdmission.METHOD); finalReport.put("gate", "OBSERVABILITY_COST");
            finalReport.put("state", result.intervalEvaluation().state().name());
            finalReport.put("completedForks", List.copyOf(completed));
            finalReport.put("evaluation", evaluation(result.intervalEvaluation()));
            finalReport.put("aggregateScope", "EXTERNAL_ORIGINAL_SCHEDULE_AND_COMPLETED_FORK_CONSUMER");
            finalReport.put("externalParentSourceWork", result.externalParentWork());
            finalReport.put("executionPlanSha256", plan.blob().sha256());
            BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> finalReport.put(flag, false));
            write(output.resolve("comparison.json"), finalReport);
            if (result.intervalEvaluation().state() != BenchmarkReturnIntervalComparison.State.PASS)
                throw new AssertionError("original return observability gate " + result.intervalEvaluation().state()
                        + ": " + result.intervalEvaluation().failures() + "; " + result.intervalEvaluation().unresolved());
        } catch (Exception | Error primary) {
            var refusal = new LinkedHashMap<String, Object>();
            refusal.put("state", "REFUSED_OR_INCOMPLETE"); refusal.put("failure", throwable(primary));
            refusal.put("enteredSlots", next[0]); refusal.put("completedForks", List.copyOf(completed));
            refusal.put("executionPlanSha256", plan.blob().sha256());
            refusal.put("retryPolicy", "NO_SLOT_RETRY_REFILL_OR_FAVORABLE_SELECTION");
            BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> refusal.put(flag, false));
            try { write(output.resolve("refusal.json"), refusal); }
            catch (Exception | Error reporting) { if (reporting != primary) primary.addSuppressed(reporting); }
            throw primary;
        }
    }

    private static AcquiredCompletion execute(Config config, Slot slot, Path evidence) throws Exception {
        require(BenchmarkReturnWorkBudget.current() == null && BenchmarkReturnWorkBudgetRun.current() == null,
                "external dispatch must not hold an original Run");
        Path attempt = Path.of(slot.registration() + ".attempt.json");
        require(!Files.exists(attempt, LinkOption.NOFOLLOW_LINKS)
                && !Files.exists(slot.report(), LinkOption.NOFOLLOW_LINKS) && !Files.exists(slot.outcomeXml(), LinkOption.NOFOLLOW_LINKS),
                "an ended or occupied original slot cannot be dispatched");
        var input = acquire(slot.registration(), INPUT_BYTES, null, new ArrayList<>(), file -> Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS));
        require(input.blob().sha256().equals(slot.registrationSha256()), "slot registration differs from frozen plan");
        var registration = json(input.bytes(), new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(65536, 16, 65536, INPUT_BYTES, 256, 24));
        var flags = registeredFlags(registration);
        var properties = object(registration.get("properties"));
        require(properties.size() <= MAX_PROPERTIES && flags.arm().equals(slot.arm().name())
                && slot.workload().equals(registration.get("workloadId"))
                && Integer.toString(slot.ordinal()).equals(properties.get("tapstate.e2e.benchmark-smoke.fork"))
                && slot.report().toString().equals(registration.get("reportPath"))
                && config.repository().toString().equals(registration.get("sourceRoot")), "slot registration identity differs");
        var command = new ArrayList<String>(List.of(config.maven().toString(), "-o", "-B", "-pl", "e2e", "-am", "verify",
                "-Dtest=NoUnitTestsForReturnMethodFork", "-Dsurefire.failIfNoSpecifiedTests=false",
                "-Dfailsafe.failIfNoSpecifiedTests=false", "-Dapi.version=1.44",
                "-Dit.test=RealBenchmarkForkDriverIT#" + text(object(registration.get("expectedCase")).get("name"), 256)));
        for (var entry : properties.entrySet()) {
            String key = text(entry.getKey(), 512);
            requireSlotProperty(key);
            command.add("-D" + key + "=" + text(entry.getValue(), 16384));
        }
        require((slot.arm() == PipelineBenchmarkComparison.Arm.A ? config.a() : config.b()).toString()
                .equals(properties.get("tapstate.e2e.benchmark-smoke.jar")), "slot artifact differs from fixed arm");
        var dispatch = new LinkedHashMap<String, Object>(); dispatch.put("command", List.copyOf(command));
        dispatch.put("workingDirectory", config.repository().toString()); dispatch.put("registrationSha256", input.blob().sha256());
        dispatch.put("reportPath", slot.report().toString()); dispatch.put("outcomeXmlPath", slot.outcomeXml().toString());
        write(evidence.resolve("dispatch.json"), dispatch);
        var docker = new ProcessBuilder("docker", "info", "--format", "{{.ServerVersion}}")
                .redirectErrorStream(true).redirectOutput(evidence.resolve("docker-info.log").toFile()).start();
        int dockerExit = awaitOwned(docker, 30, TimeUnit.SECONDS);
        write(evidence.resolve("docker-info-exit.json"), Map.of("exitCode", dockerExit));
        require(dockerExit == 0, "this original fork needs Docker; the daemon is unavailable and will not be started");
        write(attempt, Map.of("state", "ORIGINAL_SLOT_ATTEMPT_RESERVED", "registrationSha256", input.blob().sha256(),
                "workload", slot.workload(), "arm", slot.arm().name(), "ordinal", slot.ordinal(), "evidenceDirectory", evidence.toString()));
        var builder = new ProcessBuilder(command).directory(config.repository().toFile()).redirectErrorStream(true)
                .redirectOutput(evidence.resolve("maven.log").toFile());
        builder.environment().put("JAVA_HOME", config.javaHome().toString());
        builder.environment().put("PATH", config.javaHome().resolve("bin") + java.io.File.pathSeparator
                + builder.environment().getOrDefault("PATH", ""));
        int exit = awaitOwned(builder.start(), 40, TimeUnit.MINUTES);
        write(evidence.resolve("process-exit.json"), Map.of("exitCode", exit, "normalWaitReturned", true));
        require(exit == 0, "original Maven/Failsafe process failed; slot remains ended without retry");
        Path originalXml = config.repository().resolve("e2e/target/failsafe-reports/TEST-io.tapstate.e2e.RealBenchmarkForkDriverIT.xml");
        var outcome = acquire(originalXml, BenchmarkReturnAfterReceipt.XML_BYTES, null, new ArrayList<>(),
                file -> Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS));
        Files.write(slot.outcomeXml(), outcome.bytes(), StandardOpenOption.CREATE_NEW);
        write(evidence.resolve("outcome-copy.json"), Map.of("sourcePath", originalXml.toString(), "archivedPath", slot.outcomeXml().toString(),
                "sha256", outcome.blob().sha256(), "bytes", outcome.blob().bytes(), "originalMavenExitCode", exit));
        return acquireCompletion(slot.registration(), slot.registrationSha256(), slot.report(), slot.outcomeXml(), evidence.resolve("acquired"));
    }

    static Config config(Map<String, Object> input) {
        require(number(input, "schemaVersion") == 1 && BenchmarkReturnMethodAdmission.METHOD.equals(input.get("method"))
                && "OBSERVABILITY_COST".equals(input.get("gate")), "explicit original return execution method or gate differs");
        var rows = list(input.get("slots"), MAX_SLOTS); require(rows.size() == MAX_SLOTS, "all original thirty slots are required");
        var slots = new ArrayList<Slot>(); var paths = new java.util.HashSet<Path>();
        int at = 0;
        for (String workload : List.of("copy", "stateless", "stateful")) {
            int a = 0, b = 0;
            for (var arm : PipelineBenchmarkComparison.schedule()) {
                int ordinal = arm == PipelineBenchmarkComparison.Arm.A ? ++a : ++b;
                var row = object(rows.get(at++));
                require(workload.equals(row.get("workload")) && arm.name().equals(row.get("arm"))
                        && ordinal == number(row, "ordinal"), "frozen slot order differs from original workload/arm schedule");
                Path registration = path(text(row.get("registrationPath"), 4096)), report = path(text(row.get("reportPath"), 4096)),
                        xml = path(text(row.get("outcomeXmlPath"), 4096));
                require(paths.add(registration) && paths.add(report) && paths.add(xml), "original slot path is repeated");
                String sha = text(row.get("registrationSha256"), 64); require(sha.matches("[a-f0-9]{64}"), "slot digest differs");
                slots.add(new Slot(workload, arm, ordinal, registration, sha, report, xml));
            }
        }
        return new Config(path(text(input.get("repository"), 4096)), path(text(input.get("maven"), 4096)),
                path(text(input.get("javaHome"), 4096)), path(text(input.get("baselineJar"), 4096)),
                path(text(input.get("candidateJar"), 4096)), blob(object(input.get("baselineJarBytes"))),
                blob(object(input.get("candidateJarBytes"))), slots);
    }
    static void requireSlotProperty(String key) {
        require(key != null && key.length() <= 512
                && (key.startsWith("tapstate.e2e.") || key.equals(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY))
                && key.matches("[A-Za-z0-9._-]+"), "slot property is outside the benchmark namespace");
    }
    private static Map<String, Object> evaluation(BenchmarkReturnIntervalComparison.Evaluation evaluation) {
        var workloads = new EnumMap<PipelineBenchmarkComparison.Workload, Object>(PipelineBenchmarkComparison.Workload.class);
        for (var entry : evaluation.workloads().entrySet()) {
            var pair = entry.getValue();
            workloads.put(entry.getKey(), Map.of("baseline", summary(pair.baseline()), "candidate", summary(pair.candidate())));
        }
        var named = new LinkedHashMap<String, Object>(); workloads.forEach((key, value) -> named.put(key.name(), value));
        return Map.of("state", evaluation.state().name(), "workloads", Map.copyOf(named),
                "failures", evaluation.failures(), "unresolved", evaluation.unresolved());
    }
    private static Map<String, Object> summary(BenchmarkReturnIntervalComparison.Summary summary) {
        return Map.of("throughput", statistics(summary.throughput()), "p99", statistics(summary.p99()),
                "maximumPeakHeapBytes", summary.peakHeapBytes(), "maximumPeakRssBytes", summary.peakRssBytes());
    }
    private static Map<String, Object> statistics(BenchmarkReturnIntervalComparison.Statistics value) {
        return Map.of("median", bounds(value.median()), "relativeMad", bounds(value.relativeMad()));
    }
    private static Map<String, Object> bounds(BenchmarkReturnIntervalComparison.Bounds value) {
        return Map.of("lower", BenchmarkReturnForkBridge.decimal(value.lower()), "upper", BenchmarkReturnForkBridge.decimal(value.upper()));
    }
    static int awaitOwned(Process process, long limit, TimeUnit unit) throws InterruptedException {
        require(process != null && limit > 0 && unit != null, "owned process wait is missing");
        var owned = new LinkedHashMap<Long, ProcessHandle>(); owned.put(process.pid(), process.toHandle());
        long duration = unit.toNanos(limit), started = System.nanoTime(); Throwable primary = null;
        try {
            while (true) {
                process.descendants().forEach(handle -> owned.putIfAbsent(handle.pid(), handle));
                long remaining = duration - (System.nanoTime() - started);
                if (remaining <= 0) throw new AssertionError("owned external process exceeded its dispatch timeout");
                if (process.waitFor(Math.min(remaining, TimeUnit.SECONDS.toNanos(1)), TimeUnit.NANOSECONDS)) {
                    return process.exitValue();
                }
            }
        } catch (InterruptedException | RuntimeException | Error failure) { primary = failure; throw failure;
        } finally {
            // Keep the exact handles observed under this owned process; never target a guessed PID.
            try {
                process.descendants().forEach(handle -> owned.putIfAbsent(handle.pid(), handle));
                var descendants = new ArrayList<>(owned.values()); java.util.Collections.reverse(descendants);
                for (var handle : descendants) if (handle.isAlive()) handle.destroy();
                boolean interrupted = Thread.interrupted();
                try {
                    for (var handle : descendants) if (handle.isAlive()) {
                        try { handle.onExit().get(2, TimeUnit.SECONDS); }
                        catch (java.util.concurrent.TimeoutException timeout) { handle.destroyForcibly(); }
                        catch (java.util.concurrent.ExecutionException failure) { throw new AssertionError("owned process cleanup failed", failure); }
                    }
                } finally { if (interrupted) Thread.currentThread().interrupt(); }
            } catch (InterruptedException | RuntimeException | Error cleanup) {
                if (primary == null) throw cleanup;
                if (cleanup != primary) primary.addSuppressed(cleanup);
            }
        }
    }
}
