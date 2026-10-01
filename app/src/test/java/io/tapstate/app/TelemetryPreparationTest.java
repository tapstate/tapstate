package io.tapstate.app;

import io.tapstate.core.lifecycle.CaptureReading;
import io.tapstate.core.lifecycle.DeliveryReading;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.lifecycle.FrontierStallPressure;
import io.tapstate.core.lifecycle.NestColdLayerPressure;
import io.tapstate.runtime.scheduler.FrontierStallAlert;
import io.tapstate.runtime.scheduler.FrontierStallWatch;
import io.tapstate.runtime.scheduler.NestColdLayerAlert;
import io.tapstate.runtime.scheduler.NestColdLayerWatch;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.List;
import java.util.Collection;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static io.tapstate.core.lifecycle.PipelineState.RUNNING;
import static io.tapstate.core.lifecycle.PipelineState.FAILED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class TelemetryPreparationTest {

    @Test
    void aBlockedNativeCollectorLeavesTheSchedulerAndAnotherPipelinesLatestMoving() throws Exception {
        InMemoryDesiredStore desired = new InMemoryDesiredStore();
        InMemoryStateStore state = new InMemoryStateStore();
        Instant since = Instant.now().minusSeconds(1);
        for (String id : new String[] {"slow", "fast"}) {
            desired.save(new DesiredState(id, RUNNING, "rev-1"));
            state.create(id, StateJson.of(RUNNING), since);
        }
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        scopes.begin("slow", "inc-slow", 1);
        scopes.begin("fast", "inc-fast", 1);
        Map<String, Observation> latest = new ConcurrentHashMap<>();
        ObservationStore store = accepting(latest);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger slowCollections = new AtomicInteger();
        AtomicInteger fastRows = new AtomicInteger(1);
        ObservationPublisher publisher = new ObservationPublisher(state, store,
                id -> OptionalLong.empty(), id -> Map.of(), id -> SnapshotReading.NONE,
                id -> Map.of(), id -> {
                    if (id.equals("slow")) {
                        slowCollections.incrementAndGet();
                        entered.countDown();
                        awaitRelease(release);
                    }
                    return Map.of();
                }, new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, NestColdLayerAlert.NONE),
                id -> Map.of(), new FrontierStallWatch(FrontierStallPressure.DEFAULT, FrontierStallAlert.NONE),
                id -> Map.of(), id -> Map.of(), id -> Map.of(), id -> CaptureReading.NONE,
                id -> id.equals("fast") ? new DeliveryReading(
                        Map.of("orders", Map.of("u", (long) fastRows.get())), Map.of(), Map.of(), since)
                        : DeliveryReading.NONE, Clock.systemUTC());
        ExecutorService scheduler = Executors.newSingleThreadExecutor();
        try (TelemetryDispatcher telemetry = new TelemetryDispatcher(
                publisher, null, MetricsExport.none(), scopes, 2, 2, Duration.ofSeconds(30))) {
            ConvergenceDriver driver = new ConvergenceDriver(
                    new PipelineConverger(desired, state, new NoOpActuator(), Clock.systemUTC()),
                    desired, publisher, null, MetricsExport.none(), () -> true,
                    PipelineActuationOwnership.single(), LifecycleWorkDispatcher.inline(), scopes, telemetry);
            try {
                var first = scheduler.submit(driver::reconcile);
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatCode(() -> first.get(1, TimeUnit.SECONDS))
                        .as("the scheduler returns while the native collector remains blocked")
                        .doesNotThrowAnyException();
                await(() -> latest.containsKey("fast"));
                Observation before = latest.get("fast");
                assertThat(before.metrics()).containsEntry("records.out", 1L);
                fastRows.set(2);
                for (int at = 0; at < 20; at++) {
                    scheduler.submit(driver::reconcile).get(1, TimeUnit.SECONDS);
                }
                await(() -> latest.get("fast").observedAt().isAfter(before.observedAt())
                        && latest.get("fast").metrics().get("records.out").longValue() == 2);
                assertThat(release.getCount()).isEqualTo(1);
                assertThat(slowCollections.get()).as("one collector per pipeline stays in flight").isEqualTo(1);
                var health = telemetry.health().get(TelemetryDispatcher.Sink.LATEST);
                assertThat(health.inFlight()).isBetween(1, 2);
                assertThat(health.queueDepth()).isBetween(0, 4);
                assertThat(health.highWater()).isLessThanOrEqualTo(4);
                assertThat(health.coalesced()).isPositive();
                assertThat(health.dropped()).isZero();
            } finally {
                release.countDown();
            }
        } finally {
            release.countDown();
            scheduler.shutdownNow();
            assertThat(scheduler.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void aSupersededQueuedScopeNeverCollectsNativeMetrics() throws Exception {
        InMemoryStateStore state = new InMemoryStateStore();
        state.create("slow", StateJson.of(RUNNING), Instant.now());
        state.create("orders", StateJson.of(RUNNING), Instant.now());
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        var slow = scopes.begin("slow", "inc-slow", 1);
        var old = scopes.begin("orders", "inc-old", 41);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger ordersCollections = new AtomicInteger();
        Map<String, Observation> latest = new ConcurrentHashMap<>();
        ObservationPublisher publisher = new ObservationPublisher(state, accepting(latest), id -> {
            if (id.equals("slow")) {
                entered.countDown();
                awaitRelease(release);
            } else {
                ordersCollections.incrementAndGet();
            }
            return OptionalLong.empty();
        }, id -> Map.of());
        try (TelemetryDispatcher telemetry = new TelemetryDispatcher(
                publisher, null, MetricsExport.none(), scopes, 1, 2)) {
            try {
                telemetry.offerPreparation("slow", null, slow, () -> true);
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                telemetry.offerPreparation("orders", null, old, () -> true);
                scopes.begin("orders", "inc-new", 42);
                release.countDown();
                await(() -> telemetry.health().get(TelemetryDispatcher.Sink.LATEST).successes() == 1
                        && telemetry.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0
                        && telemetry.health().get(TelemetryDispatcher.Sink.LATEST).queueDepth() == 0);
            } finally {
                release.countDown();
            }
        }
        assertThat(ordersCollections.get()).isZero();
        assertThat(latest).doesNotContainKey("orders");
    }

    @Test
    void scopeOrOwnerLossDuringCollectionCannotPublishOrProject() throws Exception {
        for (boolean replaceScope : new boolean[] {true, false}) {
            InMemoryStateStore state = new InMemoryStateStore();
            state.create("orders", StateJson.of(RUNNING), Instant.now());
            ObservationScopeRegistry scopes = new ObservationScopeRegistry();
            var scope = scopes.begin("orders", "inc-a", 41);
            AtomicBoolean owner = new AtomicBoolean(true);
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            Map<String, Observation> latest = new ConcurrentHashMap<>();
            List<List<MetricFact>> exported = new CopyOnWriteArrayList<>();
            InMemoryRateHistoryStore history = new InMemoryRateHistoryStore();
            ObservationPublisher publisher = new ObservationPublisher(state, accepting(latest), id -> {
                entered.countDown();
                awaitRelease(release);
                return OptionalLong.of(7);
            }, id -> Map.of());
            try (TelemetryDispatcher telemetry = new TelemetryDispatcher(publisher,
                    new RateSampler(history, Duration.ofMillis(1)), recordingExport(exported), scopes, 1, 2)) {
                try {
                    telemetry.offerPreparation("orders", null, scope, owner::get);
                    assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                    if (replaceScope) {
                        scopes.begin("orders", "inc-b", 42);
                    } else {
                        owner.set(false);
                    }
                    release.countDown();
                    await(() -> telemetry.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0);
                    assertThat(telemetry.health().get(TelemetryDispatcher.Sink.LATEST).successes()).isZero();
                } finally {
                    release.countDown();
                }
            }
            assertThat(latest).isEmpty();
            assertThat(exported).isEmpty();
            assertThat(history.readPage("orders", Instant.EPOCH, Instant.now().plusSeconds(1), null, 10).entries())
                    .isEmpty();
        }
    }

    @Test
    void historyAndExportUseOneMeasurementOnlyAfterItsLatestCommit() throws Exception {
        InMemoryStateStore state = new InMemoryStateStore();
        Instant since = Instant.now().minusSeconds(1);
        state.create("orders", StateJson.of(RUNNING), since);
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        var scope = scopes.begin("orders", "inc-a", 41);
        CountDownLatch commitEntered = new CountDownLatch(1);
        CountDownLatch releaseCommit = new CountDownLatch(1);
        Map<String, Observation> latest = new ConcurrentHashMap<>();
        ObservationStore store = new ObservationStore() {
            @Override public void save(Observation observation) { latest.put(observation.pipelineId(), observation); }
            @Override public boolean saveScoped(Observation observation, Scope scope) {
                commitEntered.countDown();
                awaitRelease(releaseCommit);
                save(observation);
                return true;
            }
            @Override public Optional<Observation> read(String id) { return Optional.ofNullable(latest.get(id)); }
            @Override public void delete(String id) { latest.remove(id); }
        };
        AtomicInteger collections = new AtomicInteger();
        ObservationPublisher publisher = movementPublisher(state, store, since, () -> {
            collections.incrementAndGet();
            return 7;
        });
        List<List<MetricFact>> exported = new CopyOnWriteArrayList<>();
        InMemoryRateHistoryStore history = new InMemoryRateHistoryStore();
        try (TelemetryDispatcher telemetry = new TelemetryDispatcher(publisher,
                new RateSampler(history, Duration.ofMillis(1)), recordingExport(exported), scopes, 1, 2)) {
            try {
                telemetry.offerPreparation("orders", null, scope, () -> true);
                assertThat(commitEntered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(exported).isEmpty();
                assertThat(history.readPage("orders", since, Instant.now().plusSeconds(1), null, 10).entries())
                        .isEmpty();
                releaseCommit.countDown();
                await(() -> latest.containsKey("orders") && exported.size() == 1
                        && telemetry.health().get(TelemetryDispatcher.Sink.HISTORY).successes() == 1);
                assertThat(collections.get()).isEqualTo(1);
                Observation written = latest.get("orders");
                assertThat(written.metrics()).containsEntry("records.out", 7L);
                assertThat(exported.getFirst()).isEqualTo(written.facts());
                var samples = history.readPage("orders", since, Instant.now().plusSeconds(1), null, 10).entries();
                assertThat(samples).hasSize(1);
                assertThat(samples.getFirst().sample().counters()).containsEntry("records.out", 7L);
                assertThat(samples.getFirst().sample().observedAt()).isEqualTo(written.observedAt());
            } finally {
                releaseCommit.countDown();
            }
        }
    }

    @Test
    void aNeutralPendingRequestCannotEraseTheOneWitnessedFailureCause() throws Exception {
        InMemoryStateStore state = new InMemoryStateStore();
        state.create("slow", StateJson.of(RUNNING), Instant.now());
        state.create("orders", StateJson.of(FAILED), Instant.now());
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        var slow = scopes.begin("slow", "inc-slow", 1);
        var scope = scopes.begin("orders", "inc-a", 41);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Map<String, Observation> latest = new ConcurrentHashMap<>();
        ObservationPublisher publisher = new ObservationPublisher(state, accepting(latest), id -> {
            if (id.equals("slow")) {
                entered.countDown();
                awaitRelease(release);
            }
            return OptionalLong.empty();
        }, id -> Map.of());
        var failure = PipelineFailures.of("orders", new IllegalStateException("native job failed"));
        try (TelemetryDispatcher telemetry = new TelemetryDispatcher(
                publisher, null, MetricsExport.none(), scopes, 1, 2)) {
            try {
                telemetry.offerPreparation("slow", null, slow, () -> true);
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                telemetry.offerPreparation("orders", failure, scope, () -> true);
                telemetry.offerPreparation("orders", null, scope, () -> true);
                release.countDown();
                await(() -> latest.containsKey("orders"));
                assertThat(latest.get("orders").failure()).isEqualTo(failure);
                assertThat(latest.get("orders").metrics()).containsEntry("errors.engine.job-failed", 1L);
                telemetry.offerPreparation("orders", null, scope, () -> true);
                await(() -> telemetry.health().get(TelemetryDispatcher.Sink.LATEST).successes() == 3);
                assertThat(latest.get("orders").metrics()).containsEntry("errors.engine.job-failed", 1L);
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void failedPreparationKeepsTheOldLatestAndMarksARealHistoryGapUntilRecovery() throws Exception {
        InMemoryStateStore state = new InMemoryStateStore();
        Instant since = Instant.now().minusSeconds(1);
        state.create("orders", StateJson.of(RUNNING), since);
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        var scope = scopes.begin("orders", "inc-a", 41);
        Map<String, Observation> latest = new ConcurrentHashMap<>();
        AtomicInteger collections = new AtomicInteger();
        AtomicBoolean fail = new AtomicBoolean();
        ObservationPublisher publisher = movementPublisher(state, accepting(latest), since, () -> {
            int rows = collections.incrementAndGet();
            if (fail.get()) {
                throw new IllegalStateException("native metrics unavailable");
            }
            return rows;
        });
        InMemoryRateHistoryStore history = new InMemoryRateHistoryStore();
        RateSampler sampler = new RateSampler(history, Duration.ofMillis(1));
        Observation before = publisher.publishScoped("orders", null, scope).orElseThrow();
        sampler.appendIfDue(before, scope);
        await(() -> Instant.now().isAfter(before.observedAt().plusMillis(1)));
        List<List<MetricFact>> exported = new CopyOnWriteArrayList<>();
        List<PipelineEvent> events = new CopyOnWriteArrayList<>();
        try (TelemetryDispatcher telemetry = new TelemetryDispatcher(publisher, sampler, recordingExport(exported),
                scopes, TelemetryBoundaryDispatchTest.eventStore(events), 1, 2)) {
            fail.set(true);
            telemetry.offerPreparation("orders", null, scope, () -> true);
            await(() -> telemetry.health().get(TelemetryDispatcher.Sink.LATEST).failures() == 1
                    && events.size() == 1);
            assertThat(latest.get("orders")).isSameAs(before);
            assertThat(exported).isEmpty();
            assertThat(history.readPage("orders", since, Instant.now().plusSeconds(1), null, 10).entries())
                    .hasSize(1);
            assertThat(sampler.gapHealth().open()).isEqualTo(1);
            assertThat(telemetry.health().get(TelemetryDispatcher.Sink.HISTORY).degraded()).isTrue();
            assertThat(events).extracting(PipelineEvent::kind).containsExactly(PipelineEvent.Kind.TELEMETRY_DEGRADED);

            fail.set(false);
            telemetry.offerPreparation("orders", null, scope, () -> true);
            await(() -> telemetry.health().get(TelemetryDispatcher.Sink.LATEST).successes() == 1
                    && telemetry.health().get(TelemetryDispatcher.Sink.HISTORY).successes() == 1
                    && exported.size() == 1 && events.size() == 2);
            assertThat(collections.get()).isEqualTo(3);
            assertThat(latest.get("orders").metrics()).containsEntry("records.out", 3L);
            assertThat(exported.getFirst()).isEqualTo(latest.get("orders").facts());
            var entries = history.readPage("orders", since, Instant.now().plusSeconds(1), null, 10).entries();
            assertThat(entries).hasSize(2);
            assertThat(entries.getLast().gapFrom()).isAfter(before.observedAt())
                    .isBeforeOrEqualTo(entries.getLast().sample().observedAt());
            assertThat(sampler.gapHealth().open()).isZero();
            assertThat(sampler.gapHealth().opened()).isEqualTo(1);
            assertThat(sampler.gapHealth().closed()).isEqualTo(1);
            assertThat(events).extracting(PipelineEvent::kind).containsExactly(
                    PipelineEvent.Kind.TELEMETRY_DEGRADED, PipelineEvent.Kind.TELEMETRY_RESTORED);
        }
    }

    @Test
    void aTimedOutPreparationReportsDegradedWhileBlockedAndNeedsALaterFrameToRestore() throws Exception {
        InMemoryStateStore state = new InMemoryStateStore();
        Instant since = Instant.now().minusSeconds(1);
        state.create("orders", StateJson.of(RUNNING), since);
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        var scope = scopes.begin("orders", "inc-a", 41);
        Map<String, Observation> latest = new ConcurrentHashMap<>();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger collections = new AtomicInteger();
        ObservationPublisher publisher = movementPublisher(state, accepting(latest), since, () -> {
            int count = collections.incrementAndGet();
            if (count == 2) {
                entered.countDown();
                awaitRelease(release);
            }
            return count;
        });
        InMemoryRateHistoryStore history = new InMemoryRateHistoryStore();
        RateSampler sampler = new RateSampler(history, Duration.ofMillis(1));
        Observation before = publisher.publishScoped("orders", null, scope).orElseThrow();
        sampler.appendIfDue(before, scope);
        await(() -> Instant.now().isAfter(before.observedAt().plusMillis(1)));
        List<List<MetricFact>> exported = new CopyOnWriteArrayList<>();
        List<PipelineEvent> events = new CopyOnWriteArrayList<>();
        try (TelemetryDispatcher telemetry = new TelemetryDispatcher(publisher, sampler, recordingExport(exported),
                scopes, TelemetryBoundaryDispatchTest.eventStore(events), 1, 2, Duration.ofMillis(80))) {
            try {
                telemetry.offerPreparation("orders", null, scope, () -> true);
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                await(() -> telemetry.health().get(TelemetryDispatcher.Sink.LATEST).timeouts() == 1
                        && events.size() == 1);
                assertThat(release.getCount()).isEqualTo(1);
                assertThat(latest.get("orders")).isSameAs(before);
                assertThat(exported).isEmpty();
                assertThat(sampler.gapHealth().open()).isEqualTo(1);
                assertThat(events).extracting(PipelineEvent::kind)
                        .containsExactly(PipelineEvent.Kind.TELEMETRY_DEGRADED);
                release.countDown();
                await(() -> telemetry.health().get(TelemetryDispatcher.Sink.LATEST).failures() == 1
                        && telemetry.health().get(TelemetryDispatcher.Sink.HISTORY).successes() == 1
                        && exported.size() == 1);
                assertThat(telemetry.health().get(TelemetryDispatcher.Sink.LATEST).successes()).isZero();
                assertThat(telemetry.health().get(TelemetryDispatcher.Sink.LATEST).degraded()).isTrue();
                assertThat(latest.get("orders").metrics()).containsEntry("records.out", 2L);
                assertThat(events).extracting(PipelineEvent::kind)
                        .containsExactly(PipelineEvent.Kind.TELEMETRY_DEGRADED);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (telemetry.health().get(TelemetryDispatcher.Sink.LATEST).successes() == 0
                        && System.nanoTime() - deadline < 0) {
                    telemetry.offerPreparation("orders", null, scope, () -> true);
                    TimeUnit.MILLISECONDS.sleep(10);
                }
                await(() -> telemetry.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0
                        && telemetry.health().get(TelemetryDispatcher.Sink.LATEST).queueDepth() == 0);
                await(() -> events.size() == 2);
                var recovered = telemetry.health().get(TelemetryDispatcher.Sink.LATEST);
                assertThat(recovered.successes()).isPositive();
                assertThat(collections.get()).isEqualTo(Math.toIntExact(recovered.successes() + 2));
                assertThat(recovered.timeouts()).isEqualTo(1);
                assertThat(recovered.degraded()).isFalse();
                assertThat(events).extracting(PipelineEvent::kind).containsExactly(
                        PipelineEvent.Kind.TELEMETRY_DEGRADED, PipelineEvent.Kind.TELEMETRY_RESTORED);
            } finally {
                release.countDown();
            }
        }
    }

    private static ObservationPublisher movementPublisher(InMemoryStateStore state, ObservationStore store,
            Instant since, java.util.function.IntSupplier rows) {
        return new ObservationPublisher(state, store, id -> OptionalLong.empty(), id -> Map.of(),
                id -> SnapshotReading.NONE, id -> Map.of(), id -> Map.of(),
                new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, NestColdLayerAlert.NONE),
                id -> Map.of(), new FrontierStallWatch(FrontierStallPressure.DEFAULT, FrontierStallAlert.NONE),
                id -> Map.of(), id -> Map.of(), id -> Map.of(), id -> CaptureReading.NONE,
                id -> new DeliveryReading(Map.of("orders", Map.of("u", (long) rows.getAsInt())),
                        Map.of(), Map.of(), since), Clock.systemUTC());
    }

    private static MetricsExport recordingExport(List<List<MetricFact>> exported) {
        return new MetricsExport() {
            @Override public void offer(String id, PipelineState state, Instant at, List<MetricFact> facts) {
                exported.add(List.copyOf(facts));
            }
            @Override public void forgetPipelinesOutside(Collection<String> ids) { }
        };
    }

    private static ObservationStore accepting(Map<String, Observation> latest) {
        return new ObservationStore() {
            @Override public void save(Observation observation) { latest.put(observation.pipelineId(), observation); }
            @Override public boolean saveScoped(Observation observation, Scope scope) {
                save(observation);
                return true;
            }
            @Override public Optional<Observation> read(String id) { return Optional.ofNullable(latest.get(id)); }
            @Override public void delete(String id) { latest.remove(id); }
        };
    }

    private static void awaitRelease(CountDownLatch release) {
        try {
            if (!release.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("native collector was not released");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("native collector was interrupted", interrupted);
        }
    }

    private static void await(BooleanSupplier ready) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!ready.getAsBoolean() && System.nanoTime() - deadline < 0) {
            TimeUnit.MILLISECONDS.sleep(5);
        }
        assertThat(ready.getAsBoolean()).isTrue();
    }
}
