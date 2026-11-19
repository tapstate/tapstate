package io.tapstate.e2e;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Out-of-process RSS and thread sampling of the OP JVM for benchmark runs. */
final class BenchmarkResources implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern GC = Pattern.compile(
            "\\[(\\d+)ms].*GC\\(\\d+\\) (Pause .+?) (\\d+)([KMG])->(\\d+)([KMG])\\(");
    private static final boolean PROCFS = Files.isDirectory(Path.of("/proc", "self"));
    private static final String SOURCE = PROCFS ? "procfs process status" : "ps process rss";
    private final ScheduledExecutorService timer;
    private final List<Sample> samples = new ArrayList<>();
    private final List<Window> windows = new ArrayList<>();
    private final int intervalMillis;
    private final long pid;
    private final int minimumSamples;
    private final double minimumCoverage;
    private final Path output;
    private final long started = System.currentTimeMillis();
    private String phase = "startup";
    private long phaseStarted = started;
    private int missing;
    private Long lifetimeRssPeak;

    BenchmarkResources(long pid) {
        this(pid, null, 200, 2, 0.5);
    }

    BenchmarkResources(long pid, Path output, int intervalMillis, int minimumSamples, double minimumCoverage) {
        this.pid = pid;
        this.output = output;
        this.intervalMillis = intervalMillis;
        this.minimumSamples = minimumSamples;
        this.minimumCoverage = minimumCoverage;
        timer = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());
        timer.scheduleWithFixedDelay(this::sample, 0, intervalMillis, TimeUnit.MILLISECONDS);
    }

    synchronized void phase(String name) {
        if (!phase.equals(name)) {
            long now = System.currentTimeMillis();
            windows.add(new Window(phase, phaseStarted, now));
            phase = name;
            phaseStarted = now;
        }
    }

    synchronized Long peakKiB() {
        return lifetimeRssPeak;
    }

    private void sample() {
        long timestamp = System.currentTimeMillis();
        try {
            long rss;
            long highWater;
            Long threads = null;
            if (PROCFS) {
                var lines = Files.readAllLines(Path.of("/proc", Long.toString(pid), "status"));
                rss = number(lines, "VmRSS:");
                highWater = number(lines, "VmHWM:");
                threads = number(lines, "Threads:");
            } else {
                var ps = new ProcessBuilder("ps", "-o", "rss=", "-p", Long.toString(pid)).start();
                if (!ps.waitFor(2, TimeUnit.SECONDS)) {
                    ps.destroyForcibly();
                    throw new IOException("ps timed out");
                }
                rss = Long.parseLong(new String(ps.getInputStream().readAllBytes(),
                        java.nio.charset.StandardCharsets.UTF_8).trim());
                highWater = rss;
            }
            synchronized (this) {
                lifetimeRssPeak = lifetimeRssPeak == null ? highWater : Math.max(highWater, lifetimeRssPeak);
                if (output == null) {
                    return;
                }
                var sample = new Sample(timestamp, rss, threads);
                Files.writeString(output, JSON.writeValueAsString(sample) + "\n",
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                samples.add(sample);
            }
        } catch (IOException | IllegalArgumentException ignored) {
            synchronized (this) {
                missing++;
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static long number(List<String> lines, String field) {
        return lines.stream().filter(line -> line.startsWith(field)).findFirst()
                .map(line -> Long.parseLong(line.substring(field.length()).trim().split("\\s+")[0]))
                .orElseThrow(() -> new IllegalArgumentException("Missing process field " + field));
    }

    synchronized Map<String, Object> report(int steadyWindows) {
        var gc = readGc();
        var completed = new ArrayList<>(windows);
        completed.add(new Window(phase, phaseStarted, System.currentTimeMillis()));
        var observations = new LinkedHashMap<String, Map<String, Object>>();
        for (Window window : completed) {
            observations.put(window.name(), summarize(window, gc));
            if (steadyWindows > 0 && window.name().equals("measurement")) {
                long duration = window.end() - window.start();
                for (int i = 0; i < steadyWindows; i++) {
                    observations.put("steady-" + (i + 1), summarize(new Window("steady",
                            window.start() + i * duration / steadyWindows,
                            window.start() + (i + 1) * duration / steadyWindows), gc));
                }
            }
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", samples.isEmpty() ? "unavailable" : "observed");
        result.put("threshold_status", "pending-calibration");
        result.put("scope", "OP process");
        result.put("pid", pid);
        result.put("sampling_interval_ms", intervalMillis);
        result.put("missing_samples", missing);
        result.put("windows", observations);
        result.put("natural_gc_status", gc.isEmpty() ? "missing-identifiable-post-gc-samples" : "observed");
        if (steadyWindows > 0) {
            var early = observations.get("steady-1");
            var late = observations.get("steady-" + steadyWindows);
            if (early != null && late != null && Boolean.TRUE.equals(early.get("covered"))
                    && Boolean.TRUE.equals(late.get("covered"))) {
                double first = (double) early.get("rss_median_kib");
                double last = (double) late.get("rss_median_kib");
                double elapsed = (((long) late.get("started_epoch_ms")) - ((long) early.get("started_epoch_ms"))) / 1000.0;
                result.put("steady_rss_growth_kib_per_second", (last - first) / elapsed);
            }
        }
        return result;
    }

    private Map<String, Object> summarize(Window window, List<GcSample> gc) {
        var inWindow = samples.stream().filter(s -> s.epochMillis() >= window.start()
                && s.epochMillis() < window.end()).toList();
        long expected = Math.max(1, (window.end() - window.start()) / intervalMillis);
        var sorted = inWindow.stream().map(Sample::rssKiB).sorted().toList();
        var result = new LinkedHashMap<String, Object>();
        result.put("source", SOURCE);
        result.put("started_epoch_ms", window.start());
        result.put("ended_epoch_ms", window.end());
        result.put("samples", inWindow.size());
        double coverage = Math.min(1.0, (double) inWindow.size() / expected);
        result.put("coverage", coverage);
        result.put("covered", inWindow.size() >= minimumSamples && coverage >= minimumCoverage);
        if (!sorted.isEmpty()) {
            result.put("rss_peak_kib", sorted.getLast());
            int midpoint = sorted.size() / 2;
            result.put("rss_median_kib", (sorted.get(midpoint) + sorted.get((sorted.size() - 1) / 2)) / 2.0);
            var threadPeak = inWindow.stream().map(Sample::threads).filter(java.util.Objects::nonNull)
                    .max(Long::compareTo);
            threadPeak.ifPresent(value -> result.put("threads_peak", value));
        }
        var postGc = gc.stream().filter(sample -> sample.epochMillis() >= window.start()
                && sample.epochMillis() < window.end()).collect(java.util.stream.Collectors.groupingBy(GcSample::kind,
                java.util.TreeMap::new, java.util.stream.Collectors.mapping(GcSample::postHeapKiB,
                        java.util.stream.Collectors.toList())));
        result.put("gc_post_heap_kib_by_pause_type", postGc);
        return result;
    }

    private List<GcSample> readGc() {
        if (output == null) {
            return List.of();
        }
        Path log = output.resolveSibling("gc.log");
        try (var lines = Files.lines(log)) {
            var readings = new ArrayList<GcSample>();
            lines.forEach(line -> {
                var match = GC.matcher(line);
                if (match.find()) {
                    long value = Long.parseLong(match.group(5));
                    long multiplier = switch (match.group(6)) {
                        case "G" -> 1024L * 1024L;
                        case "M" -> 1024L;
                        default -> 1;
                    };
                    readings.add(new GcSample(Long.parseLong(match.group(1)), match.group(2).trim(),
                            value * multiplier));
                }
            });
            return readings;
        } catch (IOException ignored) {
            return List.of();
        }
    }

    @Override
    public void close() {
        timer.shutdownNow();
    }

    record Sample(long epochMillis, long rssKiB, Long threads) {}
    private record GcSample(long epochMillis, String kind, long postHeapKiB) {}
    private record Window(String name, long start, long end) {}
}
