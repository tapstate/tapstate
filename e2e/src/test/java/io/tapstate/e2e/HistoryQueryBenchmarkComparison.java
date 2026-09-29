package io.tapstate.e2e;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Reduces the original requested query windows without pooling individual read timings. */
final class HistoryQueryBenchmarkComparison {
    static final int FORKS = 5;
    static final int WARMUP_READS = 5;
    static final int HOT_READS = 40;
    static final List<String> WINDOWS = List.of("1h", "1d", "15d");

    record Pair(int fork, String artifactSha256, Instant anchor, int warmupReads, int hotReads,
            boolean cachedFirst, Map<String, List<Long>> raw, Map<String, List<Long>> cached) {
        Pair {
            raw = copy(raw);
            cached = copy(cached);
        }

        private static Map<String, List<Long>> copy(Map<String, List<Long>> readings) {
            Map<String, List<Long>> result = new LinkedHashMap<>();
            readings.forEach((window, values) -> result.put(window, List.copyOf(values)));
            return Map.copyOf(result);
        }
    }

    record Summary(List<Long> forkP50Nanos, List<Long> forkP95Nanos,
            long p50MedianNanos, long p95MedianNanos, double p95RelativeMad) {}
    record WindowComparison(Summary raw, Summary cached, double improvement,
            double measurementNoise) {}
    record Evaluation(boolean passed, List<String> failures, Map<String, WindowComparison> windows) {}

    static boolean cachedFirst(int fork) {
        if (fork < 1 || fork > FORKS) {
            throw new IllegalArgumentException("query fork number must be between one and five");
        }
        return fork % 2 == 0;
    }

    static Pair from(int fork, HistoryQueryBenchmarkIT.PairRun run) {
        var fixture = run.fixture();
        Instant minute = fixture.anchor().truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
        Instant missing = Instant.ofEpochSecond(Math.floorDiv(
                fixture.anchor().minusSeconds(1_800).getEpochSecond(), 1_800) * 1_800);
        if (fixture.seededSamples() != 21_561 || !fixture.firstSample().equals(minute.minusSeconds(15 * 86_400 - 600))
                || !fixture.lastSample().equals(minute) || !fixture.missingBucket().equals(missing)) {
            throw new AssertionError("query fixture samples, missing range, or bounds changed");
        }
        Map<String, List<Long>> raw = timings(run.raw().frozen());
        Map<String, List<Long>> cached = timings(run.cached().frozen());
        return new Pair(fork, run.artifactSha256(), run.fixture().anchor(), run.warmupReads(),
                run.hotReads(), run.cachedFirst(), raw, cached);
    }

    private static Map<String, List<Long>> timings(List<HistoryQueryBenchmarkIT.WindowRun> runs) {
        Map<String, List<Long>> result = new LinkedHashMap<>();
        for (var run : runs) {
            String window = run.window().name();
            String resolution = switch (window) {
                case "1h" -> "raw";
                case "1d" -> "PT30M";
                case "15d" -> "PT6H";
                default -> throw new AssertionError("supplemental window cannot enter requested query comparison");
            };
            if (!run.window().resolution().equals(resolution)
                    || run.window().span().getSeconds() != switch (window) {
                        case "1h" -> 3_600;
                        case "1d" -> 86_400;
                        default -> 15 * 86_400;
                    } || run.warmup().size() != WARMUP_READS
                    || result.put(window, run.hot().stream()
                            .map(HistoryQueryBenchmarkIT.Reading::elapsedNanos).toList()) != null) {
                throw new AssertionError("requested query fixture or warmup sequence changed");
            }
            if (run.resources() == null || run.resources().sampleCount() < 2
                    || run.resources().peakHeapBytes() <= 0 || run.resources().peakRssBytes() <= 0) {
                throw new AssertionError("requested query resource evidence is incomplete");
            }
        }
        return result;
    }

    static Evaluation evaluate(String artifactSha256, Instant anchor, List<Pair> pairs) {
        if (artifactSha256 == null || !artifactSha256.matches("[0-9a-f]{64}") || anchor == null
                || pairs.size() != FORKS) {
            throw new AssertionError("query acceptance requires one exact artifact, anchor, and five complete pairs");
        }
        for (int index = 0; index < pairs.size(); index++) {
            Pair pair = pairs.get(index);
            if (pair.fork() != index + 1 || pair.cachedFirst() != cachedFirst(index + 1)
                    || !artifactSha256.equals(pair.artifactSha256()) || !anchor.equals(pair.anchor())
                    || pair.warmupReads() != WARMUP_READS || pair.hotReads() != HOT_READS
                    || !pair.raw().keySet().equals(java.util.Set.copyOf(WINDOWS))
                    || !pair.cached().keySet().equals(java.util.Set.copyOf(WINDOWS))) {
                throw new AssertionError("query pair identity, schedule, fixture, or sampling configuration changed");
            }
            for (String window : WINDOWS) {
                validate(pair.raw().get(window));
                validate(pair.cached().get(window));
            }
        }
        Map<String, WindowComparison> windows = new LinkedHashMap<>();
        List<String> failures = new ArrayList<>();
        for (String window : WINDOWS) {
            Summary raw = summary(pairs.stream().map(pair -> pair.raw().get(window)).toList());
            Summary cached = summary(pairs.stream().map(pair -> pair.cached().get(window)).toList());
            double improvement = (raw.p95MedianNanos() - (double) cached.p95MedianNanos())
                    / raw.p95MedianNanos();
            double noise = Math.max(raw.p95RelativeMad(), cached.p95RelativeMad());
            windows.put(window, new WindowComparison(raw, cached, improvement, noise));
            if (window.equals("1h")) {
                if ((double) cached.p95MedianNanos() / raw.p95MedianNanos() > 1.10) {
                    failures.add("1h requested raw path regressed by more than ten percent");
                }
            } else {
                if (improvement < 0.20) {
                    failures.add(window + " requested p95 improvement is below twenty percent");
                }
                if (improvement <= 2 * noise) {
                    failures.add(window + " requested p95 improvement did not exceed twice measurement noise");
                }
            }
        }
        return new Evaluation(failures.isEmpty(), List.copyOf(failures), Map.copyOf(windows));
    }

    private static void validate(List<Long> values) {
        if (values == null || values.size() != HOT_READS
                || values.stream().anyMatch(value -> value == null || value <= 0)) {
            throw new AssertionError("a formal query fork requires forty positive completed hot readings");
        }
    }

    private static Summary summary(List<List<Long>> forks) {
        List<Long> p50 = forks.stream().map(values -> percentile(values, 0.50)).toList();
        List<Long> p95 = forks.stream().map(values -> percentile(values, 0.95)).toList();
        long p95Median = percentile(p95, 0.50);
        long[] deviations = p95.stream().mapToLong(value -> Math.abs(value - p95Median)).toArray();
        Arrays.sort(deviations);
        return new Summary(p50, p95, percentile(p50, 0.50), p95Median,
                (double) deviations[deviations.length / 2] / p95Median);
    }

    private static long percentile(List<Long> values, double fraction) {
        List<Long> sorted = values.stream().sorted().toList();
        return sorted.get((int) Math.ceil(sorted.size() * fraction) - 1);
    }

    static Map<String, Object> evidence(Evaluation evaluation) {
        Map<String, Object> windows = new LinkedHashMap<>();
        for (String name : WINDOWS) {
            WindowComparison window = evaluation.windows().get(name);
            windows.put(name, Map.of("raw", evidence(window.raw()), "cached", evidence(window.cached()),
                    "improvement", window.improvement(), "measurementNoise", window.measurementNoise()));
        }
        return Map.of("passed", evaluation.passed(), "failures", evaluation.failures(),
                "comparisonScope", "ORIGINAL_REQUESTED_WINDOWS", "windows", windows,
                "measurementNoiseDefinition", "MAX_OF_RAW_AND_CACHED_FORK_P95_RELATIVE_MAD");
    }

    private static Map<String, Object> evidence(Summary summary) {
        return Map.of("forkP50Nanos", summary.forkP50Nanos(), "forkP95Nanos", summary.forkP95Nanos(),
                "p50MedianNanos", summary.p50MedianNanos(), "p95MedianNanos", summary.p95MedianNanos(),
                "p95RelativeMad", summary.p95RelativeMad());
    }
}
