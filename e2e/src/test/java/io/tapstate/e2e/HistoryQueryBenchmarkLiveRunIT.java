package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Opt-in five-pair query acceptance using the same frozen fixture as the functional run. */
@RequiresDocker
class HistoryQueryBenchmarkLiveRunIT {
    private static final String PREFIX = "tapstate.e2e.history-live.";

    @Test
    void fiveInterleavedPairsKeepAllEvidenceAndEnforceRequestedWindowBenefits() throws Exception {
        boolean requested = List.of("jar", "output", "anchor").stream()
                .anyMatch(name -> System.getProperty(PREFIX + name) != null);
        Assumptions.assumeTrue(requested, "no history live-run properties supplied; five-pair run is opt-in");
        Path jar = Path.of(required("jar")).toAbsolutePath().normalize();
        if (!Files.isRegularFile(jar)) {
            throw new IllegalArgumentException("history live-run jar is not a regular file");
        }
        Path output = Path.of(required("output"));
        Path root = PipelineBenchmarkLiveRunIT.harnessRoot();
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, root);
        Map<String, Object> revision = PipelineBenchmarkLiveRunIT.harnessRevision(root);
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        try {
            String configured = System.getProperty(PREFIX + "anchor");
            Instant anchor = configured == null || configured.isBlank()
                    ? Instant.now().truncatedTo(ChronoUnit.MINUTES).minusSeconds(180).plusSeconds(17)
                    : Instant.parse(configured);
            String sha = PipelineBenchmarkLiveRunIT.sha256(jar);
            report.begin(Map.of("purpose", "FORMAL_HISTORY_QUERY_COMPARISON",
                    "applicationJar", jar.toString(), "artifactSha256", sha, "harness", revision,
                    "anchor", anchor.toString(), "forksPerArm", HistoryQueryBenchmarkComparison.FORKS,
                    "warmupReads", HistoryQueryBenchmarkComparison.WARMUP_READS,
                    "hotReads", HistoryQueryBenchmarkComparison.HOT_READS,
                    "schedule", List.of("RAW", "CACHED", "CACHED", "RAW", "RAW", "CACHED",
                            "CACHED", "RAW", "RAW", "CACHED"), "concurrency", 1),
                    PipelineBenchmarkLiveRunIT.environment(),
                    HistoryQueryBenchmarkComparison.WINDOWS.stream()
                            .map(window -> Map.<String, Object>of("requestedWindow", window)).toList());
            List<HistoryQueryBenchmarkComparison.Pair> pairs = new ArrayList<>();
            for (int fork = 1; fork <= HistoryQueryBenchmarkComparison.FORKS; fork++) {
                int pairNumber = fork;
                boolean cachedFirst = HistoryQueryBenchmarkComparison.cachedFirst(fork);
                var settings = new HistoryQueryBenchmarkIT.Settings(jar, anchor,
                        HistoryQueryBenchmarkComparison.WARMUP_READS,
                        HistoryQueryBenchmarkComparison.HOT_READS, false, cachedFirst);
                if (!sha.equals(PipelineBenchmarkLiveRunIT.sha256(jar))) {
                    throw new AssertionError("query artifact changed before the next pair");
                }
                HistoryQueryBenchmarkIT.PairRun completed = HistoryQueryBenchmarkIT.runPair(settings, window -> {
                    var evidence = new java.util.LinkedHashMap<String, Object>(window);
                    evidence.put("recordType", "COMPLETED_QUERY_WINDOW");
                    evidence.put("pairNumber", pairNumber);
                    evidence.put("cachedFirst", cachedFirst);
                    report.addFork(evidence);
                });
                pairs.add(HistoryQueryBenchmarkComparison.from(fork, completed));
                report.addFork(Map.of("recordType", "COMPLETED_QUERY_PAIR", "pairNumber", fork,
                        "artifactSha256", completed.artifactSha256(), "cachedFirst", cachedFirst,
                        "fixture", HistoryQueryBenchmarkIT.fixtureEvidence(completed.fixture()),
                        "correctness", "ALL_COLD_WARMUP_HOT_AND_ARM_RESPONSES_MATCHED"));
            }
            var evaluation = HistoryQueryBenchmarkComparison.evaluate(sha, anchor, pairs);
            report.finish(HistoryQueryBenchmarkComparison.evidence(evaluation), evaluation.passed());
            if (!evaluation.passed()) {
                throw new AssertionError("history query performance gate failed: " + evaluation.failures()
                        + "; evidence: " + report.output());
            }
        } catch (Exception | Error failure) {
            try {
                report.fail(failure);
            } catch (RuntimeException writeFailure) {
                failure.addSuppressed(writeFailure);
            }
            throw failure;
        }
    }

    private static String required(String suffix) {
        String value = System.getProperty(PREFIX + suffix);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("history live-run requires -D" + PREFIX + suffix);
        }
        return value;
    }
}
