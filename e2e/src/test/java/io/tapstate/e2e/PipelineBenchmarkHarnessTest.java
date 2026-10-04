package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The driver must use both correctness and performance evidence from every fixed fork. */
class PipelineBenchmarkHarnessTest {

    @TempDir
    Path directory;

    @Test
    void everyWorkloadRunsBothJarsInTheFrozenOrder() throws Exception {
        Path baseline = jar("baseline.jar");
        Path candidate = jar("candidate.jar");
        List<String> calls = new ArrayList<>();

        PipelineBenchmarkHarness.Report report = PipelineBenchmarkHarness.run(
                PipelineBenchmarkHarness.Gate.OBSERVABILITY_COST, baseline, candidate,
                null, null, (workload, arm, armFork, applicationJar) -> {
                    calls.add(workload.id() + ":" + arm + ":" + armFork + ":" + applicationJar.getFileName());
                    return fork(workload, arm, armFork, 100, false);
                });

        assertThat(report.evaluation().passed()).isTrue();
        assertThat(report.forks()).hasSize(3);
        for (BenchmarkWorkloadDefinitions.Workload workload : BenchmarkWorkloadDefinitions.all()) {
            assertThat(calls.stream().filter(call -> call.startsWith(workload.id() + ":")).toList())
                    .containsExactly(
                            workload.id() + ":A:1:baseline.jar",
                            workload.id() + ":B:1:candidate.jar",
                            workload.id() + ":B:2:candidate.jar",
                            workload.id() + ":A:2:baseline.jar",
                            workload.id() + ":A:3:baseline.jar",
                            workload.id() + ":B:3:candidate.jar",
                            workload.id() + ":B:4:candidate.jar",
                            workload.id() + ":A:4:baseline.jar",
                            workload.id() + ":A:5:baseline.jar",
                            workload.id() + ":B:5:candidate.jar");
        }
    }

    @Test
    void oneMissingSourceChainAckRejectsAnOtherwiseValidComparison() throws Exception {
        Path baseline = jar("baseline.jar");
        Path candidate = jar("candidate.jar");

        assertThatThrownBy(() -> PipelineBenchmarkHarness.run(
                PipelineBenchmarkHarness.Gate.OBSERVABILITY_COST, baseline, candidate,
                null, null, (workload, arm, armFork, applicationJar) ->
                        fork(workload, arm, armFork, 100,
                                workload.id().equals("stateful")
                                        && arm == PipelineBenchmarkComparison.Arm.B && armFork == 5)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("no authoritative target ACK covering");
    }

    @Test
    void omittingTheSameFrozenSourceChainFromEveryForkRejectsTheComparison() throws Exception {
        Path baseline = jar("baseline.jar");
        Path candidate = jar("candidate.jar");

        assertThatThrownBy(() -> PipelineBenchmarkHarness.run(
                PipelineBenchmarkHarness.Gate.OBSERVABILITY_COST, baseline, candidate,
                null, null, (workload, arm, armFork, applicationJar) -> {
                    PipelineBenchmarkHarness.ForkResult complete = fork(workload, arm, armFork, 100, false);
                    if (!workload.id().equals("stateful")) {
                        return complete;
                    }
                    assertThat(workload.sourceChains()).hasSizeGreaterThanOrEqualTo(2);
                    BenchmarkWorkloadDefinitions.SourceChain omitted = workload.sourceChains().getLast();
                    BenchmarkAckOracle.Fork original = complete.correctness();
                    List<BenchmarkAckOracle.SourceChain> remaining = original.chains().stream()
                            .filter(chain -> !chain.id().equals(omitted.id())).toList();
                    Map<String, Long> remainingCoverage = new LinkedHashMap<>(original.logicalCoverage());
                    remainingCoverage.remove(omitted.terminalLogicalId());
                    BenchmarkAckOracle.Fork incomplete = new BenchmarkAckOracle.Fork(
                            original.id(), remaining, remainingCoverage, original.checksum(), original.errorTotal());
                    return new PipelineBenchmarkHarness.ForkResult(complete.measurement(), incomplete);
                }))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("source chain");
    }

    @Test
    void addingTheSameUnfrozenSourceChainToEveryForkRejectsTheComparison() throws Exception {
        Path baseline = jar("baseline.jar");
        Path candidate = jar("candidate.jar");

        assertThatThrownBy(() -> PipelineBenchmarkHarness.run(
                PipelineBenchmarkHarness.Gate.OBSERVABILITY_COST, baseline, candidate,
                null, null, (workload, arm, armFork, applicationJar) -> {
                    PipelineBenchmarkHarness.ForkResult complete = fork(workload, arm, armFork, 100, false);
                    if (!workload.id().equals("stateful")) {
                        return complete;
                    }
                    BenchmarkAckOracle.Fork original = complete.correctness();
                    List<BenchmarkAckOracle.SourceChain> extra = new ArrayList<>(original.chains());
                    String token = "unexpected-source-" + arm + "-" + armFork;
                    extra.add(new BenchmarkAckOracle.SourceChain("unexpected-source",
                            List.of(new BenchmarkAckOracle.TerminalEvent("unexpected-terminal", token)),
                            token, String::equals));
                    Map<String, Long> extraCoverage = new LinkedHashMap<>(original.logicalCoverage());
                    extraCoverage.put("unexpected-terminal", 1L);
                    BenchmarkAckOracle.Fork expanded = new BenchmarkAckOracle.Fork(
                            original.id(), extra, extraCoverage, original.checksum(), original.errorTotal());
                    return new PipelineBenchmarkHarness.ForkResult(complete.measurement(), expanded);
                }))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("source chains or terminal identities differ from frozen workload");
    }

    @Test
    void rewritingTheSameFrozenTerminalIdentityInEveryForkRejectsTheComparison() throws Exception {
        Path baseline = jar("baseline.jar");
        Path candidate = jar("candidate.jar");

        assertThatThrownBy(() -> PipelineBenchmarkHarness.run(
                PipelineBenchmarkHarness.Gate.OBSERVABILITY_COST, baseline, candidate,
                null, null, (workload, arm, armFork, applicationJar) -> {
                    PipelineBenchmarkHarness.ForkResult complete = fork(workload, arm, armFork, 100, false);
                    if (!workload.id().equals("stateful")) {
                        return complete;
                    }
                    BenchmarkWorkloadDefinitions.SourceChain changed = workload.sourceChains().getLast();
                    String rewrittenId = changed.terminalLogicalId() + "-rewritten";
                    BenchmarkAckOracle.Fork original = complete.correctness();
                    List<BenchmarkAckOracle.SourceChain> rewritten = original.chains().stream().map(chain -> {
                        if (!chain.id().equals(changed.id())) {
                            return chain;
                        }
                        BenchmarkAckOracle.TerminalEvent terminal = chain.sourceTerminals().getFirst();
                        return new BenchmarkAckOracle.SourceChain(chain.id(),
                                List.of(new BenchmarkAckOracle.TerminalEvent(rewrittenId, terminal.sourcePosition())),
                                chain.authoritativeTargetAck(), chain.positionCoverage());
                    }).toList();
                    Map<String, Long> rewrittenCoverage = new LinkedHashMap<>(original.logicalCoverage());
                    rewrittenCoverage.put(rewrittenId, rewrittenCoverage.remove(changed.terminalLogicalId()));
                    BenchmarkAckOracle.Fork incorrect = new BenchmarkAckOracle.Fork(
                            original.id(), rewritten, rewrittenCoverage, original.checksum(), original.errorTotal());
                    return new PipelineBenchmarkHarness.ForkResult(complete.measurement(), incorrect);
                }))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("source chains or terminal identities differ from frozen workload");
    }

    @Test
    void optimizationRequiresAFrozenTargetAndUsesTheSeparateGainGate() throws Exception {
        Path baseline = jar("baseline.jar");
        Path candidate = jar("candidate.jar");
        assertThatThrownBy(() -> PipelineBenchmarkHarness.run(
                PipelineBenchmarkHarness.Gate.BUSINESS_OPTIMIZATION, baseline, candidate,
                null, null, (workload, arm, armFork, applicationJar) -> fork(workload, arm, armFork, 100, false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("freeze its target");

        PipelineBenchmarkHarness.Report report = PipelineBenchmarkHarness.run(
                PipelineBenchmarkHarness.Gate.BUSINESS_OPTIMIZATION, baseline, candidate,
                PipelineBenchmarkComparison.Workload.COPY,
                PipelineBenchmarkComparison.PrimaryMetric.THROUGHPUT,
                (workload, arm, armFork, applicationJar) -> fork(workload, arm, armFork,
                        workload.id().equals("copy") && arm == PipelineBenchmarkComparison.Arm.B ? 112 : 100,
                        false));
        assertThat(report.evaluation().passed()).isTrue();
    }

    private Path jar(String name) throws Exception {
        return Files.writeString(directory.resolve(name), "test jar identity");
    }

    private static PipelineBenchmarkHarness.ForkResult fork(
            BenchmarkWorkloadDefinitions.Workload workload, PipelineBenchmarkComparison.Arm arm,
            int armFork, double throughput, boolean missingAck) {
        long[] deliveries = new long[10_000];
        Arrays.fill(deliveries, 10_000_000L);
        PipelineBenchmarkComparison.Fork measurement =
                new PipelineBenchmarkComparison.Fork(arm, throughput, deliveries, 1_000, 1_000);

        List<BenchmarkAckOracle.SourceChain> chains = new ArrayList<>();
        Map<String, Long> coverage = new LinkedHashMap<>();
        for (BenchmarkWorkloadDefinitions.SourceChain source : workload.sourceChains()) {
            String token = workload.id() + "-" + source.id() + "-" + arm + "-" + armFork;
            String ack = missingAck && source == workload.sourceChains().getLast() ? null : token;
            chains.add(new BenchmarkAckOracle.SourceChain(source.id(),
                    List.of(new BenchmarkAckOracle.TerminalEvent(source.terminalLogicalId(), token)),
                    ack, String::equals));
            coverage.put(source.terminalLogicalId(), 1L);
        }
        coverage.put(workload.id() + "-measured", 12_000L);
        BenchmarkAckOracle.Fork correctness = new BenchmarkAckOracle.Fork(
                workload.id() + "-" + arm + "-" + armFork, chains, coverage,
                workload.id() + "-checksum", 0);
        return new PipelineBenchmarkHarness.ForkResult(measurement, correctness);
    }
}
