package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The published facts around each measured phase: read before the phase runs, sampled while it runs, read
 * again once its own ACK has settled.
 *
 * <p>Under a fixed offer the phase runs as issued. In a drain, the pipelines are paused and confirmed paused
 * before the first statement is issued, so no part of the phase is processed at the issuing rate; the first
 * read follows the whole backlog and precedes resume. A pause suspends the data job while capture keeps
 * reading behind the ring's headroom, so the drain measures capture, processing and target writes together.
 * Gauges are sampled on a fixed period so a saturated queue or sink is visible while it is saturated rather
 * than after it emptied.
 */
final class BenchmarkPhaseFacts {

    /** Long enough for the slowest workload's largest backlog at its lowest measured completion rate. */
    static final Duration DRAIN_WAIT = Duration.ofMinutes(25);
    private static final Duration STATE_WAIT = Duration.ofMinutes(3);
    private static final Duration FRESH_WAIT = Duration.ofMinutes(2);
    private static final Duration SAMPLE_PERIOD = Duration.ofSeconds(2);
    private static final int MAX_SAMPLES = 1_200;
    private static final String FLAT_RECORDS_OUT = "records.out";
    private static final List<String> SAMPLED_GAUGES = List.of(
            "tapstate.pipeline.process.active", "tapstate.pipeline.work.active",
            "tapstate.pipeline.queue.depth", "tapstate.pipeline.stage.queue.depth",
            "tapstate.pipeline.sink.batch.pending", "tapstate.pipeline.sink.batch.limit",
            "tapstate.pipeline.sink.backpressured", "tapstate.pipeline.lag");

    private final BenchmarkWorkloadDefinitions.Workload workload;
    private final ControlPlane control;
    private final OptionalInt processFactsPort;
    private final boolean draining;
    private final Map<String, BenchmarkPipelineFacts.Snapshot> held = new LinkedHashMap<>();
    private final List<Map<String, Object>> samples = new ArrayList<>();
    private BenchmarkProcessFacts.Scrape processHeld;
    private BenchmarkProcessFacts.Scrape processAcknowledged;
    private final List<Map<String, Object>> phases = new ArrayList<>();
    private ScheduledExecutorService sampler;
    private volatile Throwable samplingFailure;
    private long pausedAtNanos;
    private long heldAtNanos;
    private long ackObservedAtNanos;

    BenchmarkPhaseFacts(BenchmarkWorkloadDefinitions.Workload workload, ControlPlane control, OptionalInt processFactsPort,
            boolean draining) {
        this.workload = workload;
        this.control = control;
        this.processFactsPort = processFactsPort;
        this.draining = draining;
    }

    boolean draining() {
        return draining;
    }

    /**
     * Before the first statement. A drain holds every pipeline and returns once each has published that it is
     * paused; a fixed offer reads the facts now and starts sampling, because its phase begins with the issue.
     */
    void beforeIssue() throws InterruptedException {
        if (!draining) {
            readHeldAndSample();
            return;
        }
        for (String pipeline : workload.pipelineIds()) { control.lifecycle(pipeline, LifecycleVerb.PAUSE); }
        for (String pipeline : workload.pipelineIds()) { awaitState(pipeline, PipelineState.PAUSED); }
        pausedAtNanos = BenchmarkReturnWorkBudget.rootNanoTime();
    }

    /** After the last statement. A drain reads the held facts over the whole backlog, then resumes. */
    void afterIssue() throws InterruptedException {
        if (!draining) { return; }
        readHeldAndSample();
        for (String pipeline : workload.pipelineIds()) { control.lifecycle(pipeline, LifecycleVerb.RESUME); }
        for (String pipeline : workload.pipelineIds()) { awaitState(pipeline, PipelineState.RUNNING); }
    }

    private void readHeldAndSample() throws InterruptedException {
        Instant now = Instant.now();
        for (String pipeline : workload.pipelineIds()) { held.put(pipeline, freshSnapshot(pipeline, now)); }
        processHeld = processFactsPort.isPresent() ? BenchmarkProcessFacts.scrape(processFactsPort.getAsInt()) : null;
        heldAtNanos = BenchmarkReturnWorkBudget.rootNanoTime();
        sampler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "benchmark-phase-fact-sampler");
            thread.setDaemon(true);
            return thread;
        });
        sampler.scheduleAtFixedRate(this::sample, 0, SAMPLE_PERIOD.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Stops sampling and reads the process facts once the phase's own target ACK has been observed. */
    void acknowledged(long completedAckAtNanos) throws InterruptedException {
        ackObservedAtNanos = completedAckAtNanos;
        stopSampling();
        processAcknowledged = processHeld == null ? null : BenchmarkProcessFacts.scrape(processFactsPort.getAsInt());
        if (samplingFailure != null) { throw new AssertionError("phase fact sampling failed", samplingFailure); }
    }

    /** Reads the facts after the phase settled and retains this phase's fact evidence. */
    void settle(String phaseId) throws InterruptedException {
        Instant settled = Instant.now();
        Map<String, Object> pipelines = new LinkedHashMap<>();
        Map<String, BenchmarkPipelineFacts.Snapshot> settledFacts = new LinkedHashMap<>();
        Map<String, List<BenchmarkPipelineFacts.Change>> changed = new LinkedHashMap<>();
        long flatRecordsOut = 0;
        for (String pipeline : workload.pipelineIds()) {
            BenchmarkPipelineFacts.Snapshot after = freshSnapshot(pipeline, settled);
            List<BenchmarkPipelineFacts.Change> changes = BenchmarkPipelineFacts.changes(held.get(pipeline), after);
            settledFacts.put(pipeline, after);
            changed.put(pipeline, changes);
            flatRecordsOut += after.flat().getOrDefault(FLAT_RECORDS_OUT, 0L)
                    - held.get(pipeline).flat().getOrDefault(FLAT_RECORDS_OUT, 0L);
            Map<String, Object> facts = new LinkedHashMap<>();
            facts.put("heldReadAtNanos", held.get(pipeline).readAtNanos());
            facts.put("settledReadAtNanos", after.readAtNanos());
            facts.put("heldFlat", held.get(pipeline).flat());
            facts.put("settledFlat", after.flat());
            facts.put("changes", changes.stream().map(BenchmarkPipelineFacts.Change::evidence).toList());
            pipelines.put(pipeline, facts);
        }
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("phase", phaseId);
        evidence.put("regime", draining ? "BACKLOG_DRAIN" : "FIXED_OFFER");
        evidence.put("pausedAtNanos", pausedAtNanos);
        evidence.put("factsHeldAtNanos", heldAtNanos);
        evidence.put("ackObservedAtNanos", ackObservedAtNanos);
        evidence.put("pipelines", pipelines);
        java.util.Set<String> active = new java.util.TreeSet<>();
        for (String chain : workload.phase(phaseId).expectedLogicalCoverage().keySet()) {
            workload.pipelineIds().stream().filter(pipeline -> chain.startsWith(pipeline + "/")).forEach(active::add);
        }
        try {
            evidence.put("factsContract", Map.of("state", "PASS", "activePipelines", List.copyOf(active),
                    "checked", BenchmarkPerformanceFacts.verify(workload.id(), active, settledFacts, changed, flatRecordsOut)));
        } catch (AssertionError violated) {
            evidence.put("factsContract", Map.of("state", "FAIL", "violations", String.valueOf(violated.getMessage())));
        }
        evidence.put("bottleneckSignals", BenchmarkPerformanceFacts.signals(changed, ackObservedAtNanos - heldAtNanos));
        evidence.put("gaugeSamplePeriodMillis", SAMPLE_PERIOD.toMillis());
        synchronized (samples) { evidence.put("gaugeSamples", List.copyOf(samples)); }
        if (processHeld != null && processAcknowledged != null) {
            evidence.put("processFacts", BenchmarkProcessFacts.changes(processHeld, processAcknowledged));
            evidence.put("processFactsWindowNanos", processAcknowledged.readAtNanos() - processHeld.readAtNanos());
        } else {
            evidence.put("processFacts", Map.of("state", "UNAVAILABLE", "reason", "NO_PROCESS_FACT_EXPORT"));
        }
        phases.add(Map.copyOf(evidence));
        held.clear();
        synchronized (samples) { samples.clear(); }
        processHeld = null;
        processAcknowledged = null;
    }

    /** Every settled phase's fact evidence, in phase order. */
    List<Map<String, Object>> evidence() {
        return List.copyOf(phases);
    }

    /** Stops the sampler on any exit path; a failure here never replaces the phase's own failure. */
    void abandon() {
        try { stopSampling(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
    }

    private void stopSampling() throws InterruptedException {
        if (sampler == null) { return; }
        sampler.shutdownNow();
        if (!sampler.awaitTermination(30, TimeUnit.SECONDS)) {
            throw new AssertionError("phase fact sampler did not stop");
        }
        sampler = null;
    }

    private void sample() {
        try {
            synchronized (samples) {
                if (samples.size() >= MAX_SAMPLES) { return; }
            }
            Map<String, Object> reading = new LinkedHashMap<>();
            reading.put("readAtNanos", BenchmarkReturnWorkBudget.rootNanoTime());
            for (String pipeline : workload.pipelineIds()) {
                Optional<BenchmarkPipelineFacts.Snapshot> snapshot = read(pipeline);
                Map<String, Object> gauges = new LinkedHashMap<>();
                snapshot.ifPresent(read -> SAMPLED_GAUGES.forEach(name -> read.fact(name).ifPresent(fact ->
                        gauges.put(name, fact.points().stream().map(point -> {
                            Map<String, Object> value = new LinkedHashMap<>();
                            value.put("attributes", point.attributes());
                            value.put("observedAt", point.observedAt());
                            value.put("value", point.value());
                            return value;
                        }).toList()))));
                reading.put(pipeline, gauges);
            }
            synchronized (samples) { samples.add(reading); }
        } catch (RuntimeException | Error failure) {
            samplingFailure = failure;
            throw failure;
        }
    }

    private BenchmarkPipelineFacts.Snapshot freshSnapshot(String pipeline, Instant after) {
        Await.until("an observation of " + pipeline + " after " + after, FRESH_WAIT,
                () -> control.statusObservedAt(pipeline).isAfter(after), () -> String.valueOf(control.statusObservedAt(pipeline)));
        return read(pipeline).orElseThrow(() -> new AssertionError("a fresh observation of " + pipeline + " has no metrics"));
    }

    /** One read; an answer that cannot be read is printed whole before the refusal, so it can be diagnosed. */
    private Optional<BenchmarkPipelineFacts.Snapshot> read(String pipeline) {
        var answer = control.metricsAnswer(pipeline);
        try {
            return BenchmarkPipelineFacts.parse(pipeline, BenchmarkReturnWorkBudget.rootNanoTime(), answer.status(), answer.body());
        } catch (AssertionError unreadable) {
            System.out.println("benchmark-phase-facts-unreadable=" + io.tapstate.core.common.JsonWriter.write(
                    Map.of("pipeline", pipeline, "status", answer.status(), "body", answer.body())));
            throw unreadable;
        }
    }

    private void awaitState(String pipeline, PipelineState expected) {
        Await.until(pipeline + " to publish " + expected, STATE_WAIT,
                () -> control.state(pipeline).filter(expected::equals).isPresent(), () -> String.valueOf(control.state(pipeline)));
    }
}
