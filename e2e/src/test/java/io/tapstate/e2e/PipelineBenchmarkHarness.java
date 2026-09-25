package io.tapstate.e2e;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Drives the same frozen workloads against two separately built application jars. */
final class PipelineBenchmarkHarness {

    private PipelineBenchmarkHarness() {
    }

    enum Gate {
        OBSERVABILITY_COST,
        BUSINESS_OPTIMIZATION
    }

    /** One fork runs a fresh source, store, target, and application process. */
    @FunctionalInterface
    interface ForkDriver {
        ForkResult run(BenchmarkWorkloadDefinitions.Workload workload,
                       PipelineBenchmarkComparison.Arm arm, int armFork, Path applicationJar) throws Exception;
    }

    record ForkResult(PipelineBenchmarkComparison.Fork measurement, BenchmarkAckOracle.Fork correctness) {
        ForkResult {
            Objects.requireNonNull(measurement, "measurement");
            Objects.requireNonNull(correctness, "correctness");
        }
    }

    record Report(Path baselineJar, Path candidateJar,
                  Map<PipelineBenchmarkComparison.Workload, List<ForkResult>> forks,
                  PipelineBenchmarkComparison.Evaluation evaluation) {
        Report {
            forks = Map.copyOf(forks);
        }
    }

    static Report run(Gate gate, Path baselineJar, Path candidateJar,
                      PipelineBenchmarkComparison.Workload target,
                      PipelineBenchmarkComparison.PrimaryMetric primary, ForkDriver driver) throws Exception {
        Objects.requireNonNull(gate, "gate");
        Objects.requireNonNull(driver, "driver");
        Path baseline = requireJar(baselineJar);
        Path candidate = requireJar(candidateJar);
        if (gate == Gate.BUSINESS_OPTIMIZATION && (target == null || primary == null)) {
            throw new IllegalArgumentException("an optimization run must freeze its target and primary metric");
        }

        Map<PipelineBenchmarkComparison.Workload, List<ForkResult>> results =
                new EnumMap<>(PipelineBenchmarkComparison.Workload.class);
        Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> measurements =
                new EnumMap<>(PipelineBenchmarkComparison.Workload.class);
        for (BenchmarkWorkloadDefinitions.Workload workload : BenchmarkWorkloadDefinitions.all()) {
            PipelineBenchmarkComparison.Workload kind = kindOf(workload.id());
            List<ForkResult> forks = new ArrayList<>();
            int baselineFork = 0;
            int candidateFork = 0;
            for (PipelineBenchmarkComparison.Arm arm : PipelineBenchmarkComparison.schedule()) {
                int armFork = arm == PipelineBenchmarkComparison.Arm.A ? ++baselineFork : ++candidateFork;
                ForkResult result = driver.run(workload, arm, armFork,
                        arm == PipelineBenchmarkComparison.Arm.A ? baseline : candidate);
                if (result.measurement().arm() != arm) {
                    throw new AssertionError(workload.id() + " fork " + armFork + " reported the wrong arm");
                }
                BenchmarkAckOracle.verify(List.of(result.correctness()));
                forks.add(result);
            }
            BenchmarkAckOracle.verify(forks.stream().map(ForkResult::correctness).toList());
            results.put(kind, List.copyOf(forks));
            measurements.put(kind, forks.stream().map(ForkResult::measurement).toList());
        }

        PipelineBenchmarkComparison.Evaluation evaluation = gate == Gate.OBSERVABILITY_COST
                ? PipelineBenchmarkComparison.evaluateObservabilityCost(measurements)
                : PipelineBenchmarkComparison.evaluate(measurements, target, primary);
        return new Report(baseline, candidate, results, evaluation);
    }

    private static Path requireJar(Path jar) {
        if (jar == null || !Files.isRegularFile(jar)) {
            throw new IllegalArgumentException("benchmark application jar is missing: " + jar);
        }
        return jar.toAbsolutePath().normalize();
    }

    private static PipelineBenchmarkComparison.Workload kindOf(String id) {
        return switch (id) {
            case "copy" -> PipelineBenchmarkComparison.Workload.COPY;
            case "stateless" -> PipelineBenchmarkComparison.Workload.STATELESS;
            case "stateful" -> PipelineBenchmarkComparison.Workload.STATEFUL;
            default -> throw new IllegalArgumentException("unknown benchmark workload: " + id);
        };
    }
}
