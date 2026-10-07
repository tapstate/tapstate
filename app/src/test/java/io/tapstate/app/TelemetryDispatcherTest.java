package io.tapstate.app;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineEventStore;
import io.tapstate.spi.store.RateHistoryStore;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryDispatcherTest {

    @Test
    void scopedExportUsesTheCurrentOwnerAndADeletedOwnerIsImmediatelyAbsent() throws Exception {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        AtomicReference<java.util.function.Function<String, Optional<MetricsExport.ScopeToken>>> current =
                new AtomicReference<>();
        AtomicReference<MetricsExport.ScopeToken> offered = new AtomicReference<>();
        MetricsExport export = new MetricsExport() {
            @Override public void offer(String id, PipelineState state, Instant at, List<MetricFact> facts) { }
            @Override public void offerFoldedScoped(String id, ScopeToken scope, PipelineState state,
                    Instant at, List<MetricFact> facts) { offered.set(scope); }
            @Override public void bindCurrentScopes(
                    java.util.function.Function<String, Optional<ScopeToken>> resolver) {
                current.set(resolver);
            }
            @Override public void forgetPipelinesOutside(java.util.Collection<String> ids) { }
        };
        ObservationStore store = new ObservationStore() {
            @Override public void save(Observation observation) { }
            @Override public boolean saveScoped(Observation observation, Scope scope) { return true; }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), store), null, export, scopes, 1, 2)) {
            ObservationStore.Scope old = scopes.begin("orders", "inc-old", 41);
            MetricsExport.ScopeToken oldToken = new MetricsExport.ScopeToken("inc-old", 41);
            assertThat(current.get().apply("orders")).contains(oldToken);
            dispatcher.offer(frame(1), old);
            await(() -> oldToken.equals(offered.get()));

            scopes.forgetIncarnation("orders", "inc-old");
            assertThat(current.get().apply("orders")).isEmpty();
            ObservationStore.Scope next = scopes.begin("orders", "inc-new", 42);
            dispatcher.offer(frame(2), next);
            MetricsExport.ScopeToken nextToken = new MetricsExport.ScopeToken("inc-new", 42);
            await(() -> nextToken.equals(offered.get()));
            assertThat(current.get().apply("orders")).contains(nextToken);
        }
    }

    @Test
    void rejectedHistoryQueueFrameMarksTheNextRetainedSampleAsAGap() throws Exception {
        Instant at = Instant.parse("2026-09-27T10:00:00Z");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch recoveryEntered = new CountDownLatch(1);
        CountDownLatch releaseRecovery = new CountDownLatch(1);
        InMemoryRateHistoryStore retained = new InMemoryRateHistoryStore();
        RateHistoryStore blocked = new RateHistoryStore() {
            @Override public void append(RateSample sample) { append(sample, null); }
            @Override public void append(RateSample sample, Instant gapFrom) {
                appendOwned(sample, null, gapFrom);
            }
            @Override public void appendScoped(RateSample sample, ObservationStore.Scope scope, Instant gapFrom) {
                appendOwned(sample, scope, gapFrom);
            }
            private void appendOwned(RateSample sample, ObservationStore.Scope scope, Instant gapFrom) {
                if (sample.observedAt().equals(at.plusSeconds(60))) {
                    entered.countDown();
                    try {
                        if (!release.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("blocked append was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                } else if (sample.observedAt().equals(at.plusSeconds(240))) {
                    recoveryEntered.countDown();
                    try {
                        if (!releaseRecovery.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("recovery append was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
                if (scope == null) {
                    retained.append(sample, gapFrom);
                } else {
                    retained.appendScoped(sample, scope, gapFrom);
                }
            }
            @Override public Page readPage(String id, Instant from, Instant to, Key after, int limit) {
                return retained.readPage(id, from, to, after, limit);
            }
            @Override public Optional<Entry> predecessor(String id, Instant time) {
                return retained.predecessor(id, time);
            }
            @Override public Optional<Entry> read(String id, Key key) { return retained.read(id, key); }
            @Override public Optional<Entry> successor(String id, Instant time) {
                return retained.successor(id, time);
            }
            @Override public void deleteAll(String id) { retained.deleteAll(id); }
            @Override public Duration retention() { return retained.retention(); }
        };
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        ObservationStore.Scope scope = scopes.begin("orders", "inc-a", 41);
        List<PipelineEvent> boundaryEvents = new java.util.concurrent.CopyOnWriteArrayList<>();
        RateSampler sampler = new RateSampler(blocked, Duration.ofSeconds(60));
        sampler.appendIfDue(historyFrame(at, 100).observation(), scope);
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), TelemetryBoundaryDispatchTest.acceptingLatest()),
                sampler, MetricsExport.none(), scopes, TelemetryBoundaryDispatchTest.eventStore(boundaryEvents), 1, 1)) {
            dispatcher.offer(historyFrame(at.plusSeconds(60), 160), scope);
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            dispatcher.offer(historyFrame(at.plusSeconds(120), 220), scope);
            dispatcher.offer(historyFrame(at.plusSeconds(180), 280), scope);
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.HISTORY).dropped()).isEqualTo(1);
            assertHistoryGapFacts(dispatcher, sampler, 1, 1, 0);
            await(() -> boundaryEvents.size() == 1);
            release.countDown();
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.HISTORY).successes() >= 2);
            dispatcher.offer(historyFrame(at.plusSeconds(240), 340), scope);
            assertThat(recoveryEntered.await(5, TimeUnit.SECONDS)).isTrue();
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.EVENT).queueDepth() == 0);
            // FIFO barrier: all boundaries from the older due sample are persisted before this event.
            dispatcher.offerEvent(event("history-boundary-barrier", Instant.now()));
            await(() -> boundaryEvents.stream().anyMatch(event -> event.id().equals("history-boundary-barrier")));
            assertThat(boundaryEvents.stream().filter(event -> event.kind() != PipelineEvent.Kind.STATE_CHANGED))
                    .extracting(PipelineEvent::kind)
                    .containsExactly(PipelineEvent.Kind.TELEMETRY_DEGRADED);
            releaseRecovery.countDown();
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.HISTORY).successes() >= 3);
            assertHistoryGapFacts(dispatcher, sampler, 0, 1, 1);
            await(() -> boundaryEvents.size() == 3);
            assertThat(boundaryEvents.stream().filter(event -> event.kind() != PipelineEvent.Kind.STATE_CHANGED))
                    .extracting(PipelineEvent::kind).containsExactly(
                    PipelineEvent.Kind.TELEMETRY_DEGRADED, PipelineEvent.Kind.TELEMETRY_RESTORED);

            assertThat(retained.readPage("orders", at, at.plusSeconds(300), null, 10).entries())
                    .extracting(RateHistoryStore.Entry::gapFrom)
                    .containsExactly(null, null, null, at.plusSeconds(180));
        } finally {
            release.countDown();
            releaseRecovery.countDown();
        }
    }

    @Test
    void queuedOldHistoryAndExportFramesAreSkippedAfterSameGenerationResourceReplacement() throws Exception {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        var blocker = scopes.begin("blocker", "inc-blocker", 1);
        var old = scopes.begin("orders", "inc-old", 41);
        CountDownLatch historyEntered = new CountDownLatch(1), exportEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<Map<String, Object>> appended = new java.util.concurrent.CopyOnWriteArrayList<>();
        List<Map<String, Object>> exported = new java.util.concurrent.CopyOnWriteArrayList<>();
        RateHistoryStore history = new RateHistoryStore() {
            @Override public void append(RateSample sample) { throw new AssertionError("unscoped history"); }
            @Override public void appendScoped(RateSample sample, ObservationStore.Scope scope, Instant gapFrom) {
                if (sample.pipelineId().equals("blocker")) {
                    historyEntered.countDown(); awaitProjectionRelease(release);
                }
                appended.add(Map.of("pipeline", sample.pipelineId(), "scope", scope,
                        "out", sample.counters().get("records.out")));
            }
            @Override public Page readPage(String id, Instant from, Instant to, Key after, int limit) { return new Page(List.of(), false); }
            @Override public Optional<Entry> read(String id, Key key) { return Optional.empty(); }
            @Override public Optional<Entry> predecessor(String id, Instant time) { return Optional.empty(); }
            @Override public Optional<Entry> successor(String id, Instant time) { return Optional.empty(); }
            @Override public void deleteAll(String id) { }
            @Override public Duration retention() { return Duration.ofDays(15); }
        };
        MetricsExport export = new MetricsExport() {
            @Override public void offer(String id, PipelineState state, Instant at, List<MetricFact> facts) { throw new AssertionError("unscoped export"); }
            @Override public void offerFoldedScoped(String id, ScopeToken scope, PipelineState state, Instant at, List<MetricFact> facts) {
                if (id.equals("blocker")) { exportEntered.countDown(); awaitProjectionRelease(release); }
                exported.add(Map.of("pipeline", id, "scope", scope, "at", at));
            }
            @Override public void forgetPipelinesOutside(java.util.Collection<String> ids) { }
        };
        ObservationStore latest = new ObservationStore() {
            @Override public void save(Observation observation) { throw new AssertionError("unscoped latest"); }
            @Override public boolean saveScoped(Observation observation, Scope scope) { return true; }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(new ObservationPublisher(new InMemoryStateStore(), latest),
                new RateSampler(history, Duration.ofMillis(1)), export, scopes, 1, 4, Duration.ofSeconds(5))) {
            dispatcher.offer(projectionFrame("blocker", 1), blocker);
            assertThat(historyEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(exportEntered.await(5, TimeUnit.SECONDS)).isTrue();
            dispatcher.offer(projectionFrame("orders", 2), old);
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.HISTORY).queueDepth() == 1
                    && dispatcher.health().get(TelemetryDispatcher.Sink.EXPORT).queueDepth() == 1);
            var current = scopes.begin("orders", "inc-new", 41);
            dispatcher.offer(projectionFrame("orders", 3), new ObservationStore.Scope("inc-foreign", 41));
            dispatcher.offer(projectionFrame("orders", 4), null);
            dispatcher.offer(projectionFrame("orders", 5), current);
            release.countDown();
            await(() -> appended.size() == 2 && exported.size() == 2
                    && dispatcher.health().get(TelemetryDispatcher.Sink.HISTORY).inFlight() == 0
                    && dispatcher.health().get(TelemetryDispatcher.Sink.EXPORT).inFlight() == 0);
            assertThat(appended).containsExactly(Map.of("pipeline", "blocker", "scope", blocker, "out", 1L),
                    Map.of("pipeline", "orders", "scope", current, "out", 5L));
            assertThat(exported).containsExactly(
                    Map.of("pipeline", "blocker", "scope", new MetricsExport.ScopeToken("inc-blocker", 1),
                            "at", projectionFrame("blocker", 1).observation().observedAt()),
                    Map.of("pipeline", "orders", "scope", new MetricsExport.ScopeToken("inc-new", 41),
                            "at", projectionFrame("orders", 5).observation().observedAt()));
        } finally { release.countDown(); }
    }

    private static void awaitProjectionRelease(CountDownLatch release) {
        try { assertThat(release.await(5, TimeUnit.SECONDS)).as("the owned projection blocker is released").isTrue(); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); throw new AssertionError("projection blocker interrupted", interrupted);
        }
    }

    private static ObservationPublisher.Prepared projectionFrame(String pipeline, long count) {
        Instant at = Instant.parse("2026-09-26T10:00:00Z").plusMillis(count);
        return new ObservationPublisher.Prepared(new Observation(pipeline, PipelineState.RUNNING,
                Map.of("records.out", count), Map.of(), Map.of(), null, at), false, Map.of(), Map.of(), Map.of());
    }

    private static void assertHistoryGapFacts(TelemetryDispatcher dispatcher, RateSampler sampler,
            long open, long opened, long closed) {
        Instant sampledAt = Instant.now();
        List<MetricFact> facts = TelemetryProcessFacts.snapshot(dispatcher.health(),
                java.util.Set.of(TelemetryDispatcher.Sink.HISTORY), sampledAt, sampledAt);
        for (var expected : Map.of("open", open, "opened", opened, "closed", closed).entrySet()) {
            MetricFact fact = facts.stream().filter(item -> item.name().equals(
                    "tapstate.process.telemetry.gap." + expected.getKey())).findFirst().orElseThrow();
            assertThat(fact.unit()).isEqualTo("{gap}");
            assertThat(fact.type()).isEqualTo(expected.getKey().equals("open")
                    ? io.tapstate.core.lifecycle.MetricType.GAUGE : io.tapstate.core.lifecycle.MetricType.COUNTER);
            assertThat(fact.points()).singleElement().satisfies(point -> {
                assertThat(point.attributes()).containsExactlyEntriesOf(Map.of("sink", "history"));
                assertThat(point.value()).isEqualTo(expected.getValue());
                if (!expected.getKey().equals("open")) {
                    assertThat(point.startTime()).isEqualTo(sampler.gapHealth().startedAt());
                }
            });
        }
        assertThat(facts).noneMatch(fact -> fact.name().equals(
                "tapstate.process.telemetry.restoration.pending"));
    }

    private static ObservationPublisher.Prepared historyFrame(Instant at, long out) {
        Observation observation = new Observation("orders", PipelineState.RUNNING,
                Map.of("records.out", out), Map.of(), Map.of(), null, at);
        return new ObservationPublisher.Prepared(observation, false, Map.of(), Map.of(), Map.of());
    }

    @Test
    void aBlockedExportResetCannotHoldAnotherPipelinesObservationOffer() throws Exception {
        CountDownLatch resetEntered = new CountDownLatch(1);
        CountDownLatch releaseReset = new CountDownLatch(1);
        AtomicInteger fastSaved = new AtomicInteger();
        MetricsExport export = new MetricsExport() {
            @Override public void offer(String id, PipelineState state, Instant at, List<MetricFact> facts) { }
            @Override public void forgetPipeline(String id) {
                if (id.equals("orders")) {
                    resetEntered.countDown();
                    try {
                        if (!releaseReset.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("export reset was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
            }
            @Override public void forgetPipelinesOutside(java.util.Collection<String> ids) { }
        };
        ObservationStore store = new ObservationStore() {
            @Override public void save(Observation observation) { }
            @Override public boolean saveScoped(Observation observation, Scope scope) {
                if (observation.pipelineId().equals("fast")) {
                    fastSaved.incrementAndGet();
                }
                return true;
            }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), store), null,
                export, scopes, 1, 2)) {
            ObservationStore.Scope slowScope = scopes.begin("orders", "inc-a", 1);
            dispatcher.offer(frame(1), slowScope);
            assertThat(resetEntered.await(5, TimeUnit.SECONDS)).isTrue();

            ObservationStore.Scope fastScope = scopes.begin("fast", "inc-b", 2);
            Observation fastObservation = new Observation("fast", PipelineState.RUNNING,
                    Map.of("sequence", 2L), Map.of(), Map.of(), null,
                    Instant.parse("2026-09-27T10:00:00Z"));
            ObservationPublisher.Prepared fast = new ObservationPublisher.Prepared(
                    fastObservation, false, Map.of(), Map.of(), Map.of());
            java.util.concurrent.CompletableFuture.runAsync(() -> dispatcher.offer(fast, fastScope))
                    .get(1, TimeUnit.SECONDS);
            await(() -> fastSaved.get() > 0);
            releaseReset.countDown();
        } finally {
            releaseReset.countDown();
        }
    }

    @Test
    void freshExecutionsForgetOnlyTheirOldExportSeriesWhileRebuildingResumeKeepsItsNames()
            throws Exception {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        List<String> actions = java.util.Collections.synchronizedList(new ArrayList<>());
        MetricsExport export = new MetricsExport() {
            @Override public void offer(String id, PipelineState state, Instant at, List<MetricFact> facts) {
                actions.add("offer");
            }
            @Override public void forgetPipeline(String id) { actions.add("forget"); }
            @Override public void forgetPipelinesOutside(java.util.Collection<String> ids) { }
        };
        ObservationStore store = new ObservationStore() {
            @Override public void save(Observation observation) { }
            @Override public boolean saveScoped(Observation observation, Scope scope) { return true; }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), store), null,
                export, scopes, 1, 2)) {
            ObservationStore.Scope first = scopes.begin("orders", "inc-a", 1);
            dispatcher.offer(frame(1), first);
            await(() -> actions.stream().filter("offer"::equals).count() == 1);
            ObservationStore.Scope restarted = scopes.begin("orders", "inc-a", 2);
            dispatcher.offer(frame(2), restarted);
            await(() -> actions.stream().filter("offer"::equals).count() == 2);
            scopes.prepareRebuildingResume("orders", Optional.empty());
            ObservationStore.Scope continued = scopes.begin("orders", "inc-a", 3);
            dispatcher.offer(frame(3), continued);
            await(() -> actions.stream().filter("offer"::equals).count() == 3);
            ObservationStore.Scope recreated = scopes.begin("orders", "inc-b", 4);
            dispatcher.offer(frame(4), recreated);
            await(() -> actions.stream().filter("offer"::equals).count() == 4);

            assertThat(actions).containsExactly("forget", "offer", "forget", "offer", "offer",
                    "forget", "offer");
        }
    }

    @Test
    void aFailedLocalExportResetNeverStopsLatestAndIsRetried() throws Exception {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        AtomicInteger resets = new AtomicInteger();
        AtomicInteger writes = new AtomicInteger();
        MetricsExport export = new MetricsExport() {
            @Override public void offer(String id, PipelineState state, Instant at, List<MetricFact> facts) { }
            @Override public void forgetPipeline(String id) {
                if (resets.incrementAndGet() == 1) {
                    throw new IllegalStateException("exporter unavailable");
                }
            }
            @Override public void forgetPipelinesOutside(java.util.Collection<String> ids) { }
        };
        ObservationStore store = new ObservationStore() {
            @Override public void save(Observation observation) { }
            @Override public boolean saveScoped(Observation observation, Scope scope) {
                writes.incrementAndGet();
                return true;
            }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), store), null, export, scopes, 1, 2)) {
            ObservationStore.Scope scope = scopes.begin("orders", "inc-a", 1);
            dispatcher.offer(frame(1), scope);
            dispatcher.offer(frame(2), scope);
            await(() -> resets.get() == 2);
            await(() -> writes.get() > 0);

            assertThat(resets).hasValue(2);
            assertThat(writes.get()).isPositive();
        }
    }


    @Test
    void anUnwiredEventSinkDoesNotTurnAStateTransitionIntoAConvergenceFailure() {
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), new InMemoryObservationStore()),
                null, MetricsExport.none(), 1, 1)) {
            dispatcher.offerEvent(event("transition", Instant.now()));
            assertThat(dispatcher.health()).doesNotContainKey(TelemetryDispatcher.Sink.EVENT);
        }
    }

    @Test
    void distinctPipelineLossesNeverGrowPastTheGlobalGapBudget() throws Exception {
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        PipelineEventStore store = new PipelineEventStore() {
            @Override public void append(PipelineEvent event) {
                if (event.id().equals("first")) {
                    firstEntered.countDown();
                    try {
                        if (!releaseFirst.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("first event was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
                if (event.kind() == PipelineEvent.Kind.TELEMETRY_GAP) {
                    throw new IllegalStateException("marker store unavailable");
                }
            }
            @Override public Page readPage(String pipelineId, String incarnationId, Instant from,
                    Instant to, Key after, int limit) { return new Page(List.of(), false); }
            @Override public void deleteIncarnation(String pipelineId, String incarnationId) { }
            @Override public Duration retention() { return Duration.ofDays(15); }
        };
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), new InMemoryObservationStore()),
                null, MetricsExport.none(), null, store, 1, 1, Duration.ofSeconds(5))) {
            dispatcher.offerEvent(event("first", Instant.now()));
            assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
            dispatcher.offerEvent(event("queued", Instant.now()));
            for (int index = 0; index < 20; index++) {
                dispatcher.offerEvent(new PipelineEvent("lost-" + index, "pipeline-" + index,
                        "inc-a", 41, PipelineEvent.Kind.STATE_CHANGED, Instant.now(),
                        PipelineState.NEW, PipelineState.RUNNING, null, null, null));
            }
            assertThat(dispatcher.openEventGaps()).isEqualTo(2);
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.EVENT).dropped()).isEqualTo(20);
            releaseFirst.countDown();
        } finally {
            releaseFirst.countDown();
        }
    }

    @Test
    void shutdownAbandonsQueuedEventsWithinTheFlushBudgetAndKeepsLossVisible() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        PipelineEventStore store = new PipelineEventStore() {
            @Override public void append(PipelineEvent event) {
                if (event.id().equals("first")) {
                    entered.countDown();
                    while (release.getCount() > 0) {
                        try {
                            release.await();
                        } catch (InterruptedException ignored) {
                            // A blocked storage driver may not honor interruption during shutdown.
                        }
                    }
                }
            }
            @Override public Page readPage(String pipelineId, String incarnationId, Instant from,
                    Instant to, Key after, int limit) { return new Page(List.of(), false); }
            @Override public void deleteIncarnation(String pipelineId, String incarnationId) { }
            @Override public Duration retention() { return Duration.ofDays(15); }
        };
        TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), new InMemoryObservationStore()),
                null, MetricsExport.none(), null, store, 1, 1, Duration.ofSeconds(5));
        try {
            dispatcher.offerEvent(event("first", Instant.now()));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            dispatcher.offerEvent(event("queued", Instant.now()));
            dispatcher.offerEvent(event("overflow", Instant.now()));

            long started = System.nanoTime();
            dispatcher.close();

            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)).isLessThan(4_000);
            TelemetryDispatcher.Health health = dispatcher.health().get(TelemetryDispatcher.Sink.EVENT);
            assertThat(health.timeouts()).isEqualTo(1);
            assertThat(health.dropped()).isGreaterThanOrEqualTo(3);
            assertThat(health.openGaps()).isEqualTo(1);
            assertThat(health.degraded()).isTrue();
        } finally {
            release.countDown();
        }
    }

    @Test
    void ambiguousMarkerAcknowledgementRetriesTheSameIdentityWithWiderBounds() throws Exception {
        CountDownLatch markerWritten = new CountDownLatch(1);
        CountDownLatch releaseAcknowledgement = new CountDownLatch(1);
        List<PipelineEvent> markerAttempts = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<PipelineEvent> durableMarker = new AtomicReference<>();
        AtomicInteger markerCalls = new AtomicInteger();
        List<PipelineEvent> persisted = Collections.synchronizedList(new ArrayList<>());
        PipelineEventStore store = new PipelineEventStore() {
            @Override public void append(PipelineEvent event) {
                if (event.id().equals("failed")) {
                    throw new IllegalStateException("normal event append failed");
                }
                if (event.kind() == PipelineEvent.Kind.TELEMETRY_GAP) {
                    markerAttempts.add(event);
                    durableMarker.set(event);
                    if (markerCalls.incrementAndGet() == 1) {
                        markerWritten.countDown();
                        try {
                            if (!releaseAcknowledgement.await(5, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("marker acknowledgement was not released");
                            }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(interrupted);
                        }
                        throw new IllegalStateException("acknowledgement was lost after persistence");
                    }
                }
                persisted.add(event);
            }
            @Override public Page readPage(String pipelineId, String incarnationId, Instant from,
                    Instant to, Key after, int limit) { return new Page(List.of(), false); }
            @Override public void deleteIncarnation(String pipelineId, String incarnationId) { }
            @Override public Duration retention() { return Duration.ofDays(15); }
        };
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), new InMemoryObservationStore()),
                null, MetricsExport.none(), null, store, 1, 1, Duration.ofSeconds(5))) {
            dispatcher.offerEvent(event("failed", Instant.now()));
            assertThat(markerWritten.await(5, TimeUnit.SECONDS)).isTrue();
            PipelineEvent firstMarker = durableMarker.get();
            assertThat(firstMarker).isNotNull();
            dispatcher.offerEvent(event("queued", Instant.now()));
            dispatcher.offerEvent(event("lost", Instant.now()));
            assertThat(dispatcher.openEventGaps()).isEqualTo(1);
            releaseAcknowledgement.countDown();
            await(() -> persisted.stream().anyMatch(event ->
                    event.kind() == PipelineEvent.Kind.TELEMETRY_RESTORED));

            PipelineEvent finalMarker = durableMarker.get();
            assertThat(markerAttempts.size()).isGreaterThanOrEqualTo(2);
            assertThat(markerAttempts).extracting(PipelineEvent::id).containsOnly(firstMarker.id());
            assertThat(markerAttempts).extracting(PipelineEvent::occurredAt)
                    .containsOnly(firstMarker.occurredAt());
            assertThat(finalMarker.gap().from()).isEqualTo(firstMarker.gap().from());
            assertThat(finalMarker.gap().to()).isAfter(firstMarker.gap().to());
            assertThat(finalMarker.gap().reasons()).containsExactly(
                    PipelineEvent.GapReason.QUEUE_FULL, PipelineEvent.GapReason.WRITE_FAILURE);
            assertThat(dispatcher.openEventGaps()).isZero();
        } finally {
            releaseAcknowledgement.countDown();
        }
    }

    @Test
    void aPersistedGapWithFailedRestorationStaysDegradedUntilRestorationIsWritten() throws Exception {
        java.util.concurrent.atomic.AtomicBoolean failRestoration = new java.util.concurrent.atomic.AtomicBoolean(true);
        AtomicInteger restorationAttempts = new AtomicInteger();
        List<PipelineEvent> persisted = Collections.synchronizedList(new ArrayList<>());
        PipelineEventStore store = new PipelineEventStore() {
            @Override public void append(PipelineEvent event) {
                if (event.id().equals("failed")) {
                    throw new IllegalStateException("event store unavailable");
                }
                if (event.kind() == PipelineEvent.Kind.TELEMETRY_RESTORED) {
                    restorationAttempts.incrementAndGet();
                    if (failRestoration.get()) {
                        throw new IllegalStateException("restoration write unavailable");
                    }
                }
                persisted.add(event);
            }
            @Override public Page readPage(String pipelineId, String incarnationId, Instant from,
                    Instant to, Key after, int limit) { return new Page(List.of(), false); }
            @Override public void deleteIncarnation(String pipelineId, String incarnationId) { }
            @Override public Duration retention() { return Duration.ofDays(15); }
        };
        AtomicReference<Supplier<List<MetricFact>>> process = new AtomicReference<>();
        MetricsExport export = new MetricsExport() {
            @Override public void observeProcess(Supplier<List<MetricFact>> facts) {
                process.set(facts);
            }
            @Override public void offer(String id, PipelineState state, Instant at, List<MetricFact> facts) { }
            @Override public void forgetPipelinesOutside(java.util.Collection<String> ids) { }
        };
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), new InMemoryObservationStore()),
                null, export, null, store, 1, 2, Duration.ofSeconds(5))) {
            dispatcher.offerEvent(event("failed", Instant.now()));
            await(() -> restorationAttempts.get() > 0);
            assertThat(dispatcher.openEventGaps()).isZero();
            assertThat(dispatcher.pendingEventRestorations()).isEqualTo(1);
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.EVENT).degraded()).isTrue();
            assertThat(process.get()).isNotNull();
            assertThat(processMetric(process.get().get(), "tapstate.process.telemetry.restoration.pending"))
                    .isEqualTo(1);
            assertThat(processMetric(process.get().get(), "tapstate.process.telemetry.degraded"))
                    .isEqualTo(1);
            assertThat(persisted).extracting(PipelineEvent::kind)
                    .contains(PipelineEvent.Kind.TELEMETRY_GAP)
                    .doesNotContain(PipelineEvent.Kind.TELEMETRY_RESTORED);

            failRestoration.set(false);
            await(() -> dispatcher.pendingEventRestorations() == 0);
            assertThat(persisted).extracting(PipelineEvent::kind).containsSubsequence(
                    PipelineEvent.Kind.TELEMETRY_GAP, PipelineEvent.Kind.TELEMETRY_RESTORED);
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.EVENT).degraded()).isFalse();
            assertThat(processMetric(process.get().get(), "tapstate.process.telemetry.restoration.pending"))
                    .isZero();
        }
    }

    private static long processMetric(List<MetricFact> facts, String name) {
        return facts.stream().filter(fact -> fact.name().equals(name))
                .flatMap(fact -> fact.points().stream())
                .filter(point -> "event".equals(point.attributes().get(MetricAttributes.TELEMETRY_SINK)))
                .mapToLong(io.tapstate.core.lifecycle.MetricPoint::value).findFirst().orElseThrow();
    }

    @Test
    void eventOverflowKeepsOneGapAndRetriesItsMarkerBeforeRestored() throws Exception {
        Instant at = Instant.parse("2026-09-27T10:00:00Z");
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicBoolean failMarkers = new java.util.concurrent.atomic.AtomicBoolean(true);
        List<PipelineEvent> persisted = Collections.synchronizedList(new ArrayList<>());
        List<PipelineEvent> attemptedMarkers = Collections.synchronizedList(new ArrayList<>());
        PipelineEventStore store = new PipelineEventStore() {
            @Override public void append(PipelineEvent event) {
                if (event.id().equals("first")) {
                    firstEntered.countDown();
                    try {
                        if (!releaseFirst.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("first event was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
                if (event.kind() == PipelineEvent.Kind.TELEMETRY_GAP) {
                    attemptedMarkers.add(event);
                    if (failMarkers.get()) {
                        throw new IllegalStateException("marker write failed");
                    }
                }
                persisted.add(event);
            }
            @Override public Page readPage(String pipelineId, String incarnationId, Instant from,
                    Instant to, Key after, int limit) { return new Page(List.of(), false); }
            @Override public void deleteIncarnation(String pipelineId, String incarnationId) { }
            @Override public Duration retention() { return Duration.ofDays(15); }
        };
        ObservationStore latest = new InMemoryObservationStore();
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), latest), null,
                MetricsExport.none(), null, store, 1, 1, Duration.ofSeconds(5))) {
            dispatcher.offerEvent(event("first", at));
            assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
            dispatcher.offerEvent(event("second", at.plusMillis(1)));
            long started = System.nanoTime();
            Instant firstLossNoEarlierThan = Instant.now();
            dispatcher.offerEvent(event("lost-1", at.plusMillis(2)));
            dispatcher.offerEvent(event("lost-2", at.plusMillis(3)));
            Instant initialLossNoLaterThan = Instant.now();
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)).isLessThan(200);
            releaseFirst.countDown();

            await(() -> !attemptedMarkers.isEmpty());
            assertThat(dispatcher.openEventGaps()).isEqualTo(1);
            assertThat(persisted).noneMatch(event -> event.kind() == PipelineEvent.Kind.TELEMETRY_RESTORED);
            failMarkers.set(false);
            await(() -> persisted.stream().anyMatch(event ->
                    event.kind() == PipelineEvent.Kind.TELEMETRY_RESTORED));

            assertThat(dispatcher.openEventGaps()).isZero();
            Instant gapFrom = attemptedMarkers.getFirst().gap().from();
            assertThat(gapFrom).isBetween(firstLossNoEarlierThan.truncatedTo(java.time.temporal.ChronoUnit.MILLIS),
                    initialLossNoLaterThan.truncatedTo(java.time.temporal.ChronoUnit.MILLIS));
            assertThat(attemptedMarkers).extracting(PipelineEvent::id).containsOnly(
                    PipelineEvent.gapId("orders", "inc-a", 41, gapFrom));
            assertThat(attemptedMarkers).extracting(PipelineEvent::occurredAt).containsOnly(gapFrom);
            assertThat(persisted).extracting(PipelineEvent::kind).containsSubsequence(
                    PipelineEvent.Kind.TELEMETRY_GAP, PipelineEvent.Kind.TELEMETRY_RESTORED);
            PipelineEvent marker = persisted.stream().filter(event ->
                    event.kind() == PipelineEvent.Kind.TELEMETRY_GAP).findFirst().orElseThrow();
            assertThat(marker.gap().from()).isEqualTo(gapFrom);
            assertThat(marker.gap().to()).isAfterOrEqualTo(gapFrom);
            assertThat(marker.gap().reasons()).containsExactly(
                    PipelineEvent.GapReason.QUEUE_FULL, PipelineEvent.GapReason.WRITE_FAILURE);
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.EVENT).dropped()).isGreaterThanOrEqualTo(3);
        } finally {
            releaseFirst.countDown();
        }
    }

    private static PipelineEvent event(String id, Instant at) {
        return new PipelineEvent(id, "orders", "inc-a", 41, PipelineEvent.Kind.STATE_CHANGED,
                at, PipelineState.NEW, PipelineState.RUNNING, null, null, null);
    }

    private static void await(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() - deadline < 0) {
            TimeUnit.MILLISECONDS.sleep(5);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    @Test
    void shutdownStopsWaitingAfterItsFlushBudgetAndReportsTheTimeout() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<Long> saved = java.util.Collections.synchronizedList(new ArrayList<>());
        ObservationStore store = new ObservationStore() {
            @Override public void save(Observation observation) {
                entered.countDown();
                while (release.getCount() > 0) {
                    try {
                        release.await();
                    } catch (InterruptedException ignored) {
                        // A blocked storage driver may not honor interruption during shutdown.
                    }
                }
                saved.add(observation.metrics().get("sequence"));
            }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), store), null,
                MetricsExport.none(), null, 1, 1, java.time.Duration.ofSeconds(5));
        try {
            dispatcher.offer(frame(1), null);
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            dispatcher.offer(frame(2), null);

            long started = System.nanoTime();
            dispatcher.close();

            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)).isLessThan(4_000);
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).timeouts()).isEqualTo(1);
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).degraded()).isTrue();
            release.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() > 0
                    && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).dropped()).isEqualTo(1);
            assertThat(saved).containsExactly(1L);
        } finally {
            release.countDown();
        }
    }

    @Test
    void processHealthRemainsReadableWhenTheLatestStoreFails() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        ObservationStore store = new ObservationStore() {
            @Override public void save(Observation observation) {
                throw new IllegalStateException("latest store unavailable");
            }
            @Override public Optional<Observation> read(String id) {
                reads.incrementAndGet();
                return Optional.empty();
            }
            @Override public void delete(String id) { }
        };
        AtomicReference<Supplier<List<MetricFact>>> exportedProcess = new AtomicReference<>();
        MetricsExport export = new MetricsExport() {
            @Override public void observeProcess(Supplier<List<MetricFact>> facts) {
                exportedProcess.set(facts);
            }
            @Override public void offer(String id, PipelineState state, Instant at, List<MetricFact> facts) { }
            @Override public void forgetPipelinesOutside(java.util.Collection<String> ids) { }
        };
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), store), null, export, 1, 2)) {
            assertThat(exportedProcess.get()).isNotNull();
            assertThat(exportedProcess.get().get()).as("wired sinks report zero readings before work")
                    .anyMatch(fact -> fact.name().equals("tapstate.process.telemetry.degraded")
                            && fact.points().stream().anyMatch(point ->
                                    "latest".equals(point.attributes().get(MetricAttributes.TELEMETRY_SINK))
                                            && point.value() == 0));
            dispatcher.offer(frame(1), null);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).failures() == 0
                    && System.nanoTime() - deadline < 0) {
                Thread.sleep(5);
            }
            List<MetricFact> health = exportedProcess.get().get();
            MetricFact degraded = health.stream()
                    .filter(fact -> fact.name().equals("tapstate.process.telemetry.degraded"))
                    .findFirst().orElseThrow();
            assertThat(degraded.points()).anySatisfy(point -> {
                assertThat(point.attributes()).containsEntry(MetricAttributes.TELEMETRY_SINK, "latest");
                assertThat(point.value()).isEqualTo(1L);
            });
            assertThat(health.stream().flatMap(fact -> fact.points().stream())
                    .map(point -> point.attributes().get(MetricAttributes.TELEMETRY_SINK)))
                    .doesNotContain("history");
            assertThat(health.stream().filter(fact -> fact.name().equals(
                    "tapstate.process.telemetry.last_success.age"))
                    .flatMap(fact -> fact.points().stream())
                    .map(point -> point.attributes().get(MetricAttributes.TELEMETRY_SINK)))
                    .doesNotContain("latest");
            assertThat(reads).hasValue(0);
        }
    }

    @Test
    void historyWorkerRetainsTheExecutionScopeOfTheOfferedFrame() throws Exception {
        InMemoryRateHistoryStore history = new InMemoryRateHistoryStore();
        ObservationStore latest = new ObservationStore() {
            @Override public void save(Observation observation) { }
            @Override public boolean saveScoped(Observation observation, Scope scope) { return true; }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        ObservationStore.Scope owner = scopes.begin("orders", "inc-current", 42);
        Observation observation = new Observation("orders", PipelineState.RUNNING,
                Map.of("records.out", 7L), Map.of(), Map.of(), null,
                Instant.parse("2026-09-26T10:00:00Z"));
        ObservationPublisher.Prepared prepared = new ObservationPublisher.Prepared(observation, false,
                Map.of(), Map.of(), Map.of());

        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                new ObservationPublisher(new InMemoryStateStore(), latest),
                new RateSampler(history, Duration.ofMinutes(1)), MetricsExport.none(), scopes, 1, 4)) {
            dispatcher.offer(prepared, owner);
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dispatcher.health().get(TelemetryDispatcher.Sink.HISTORY).successes() == 0
                    && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.HISTORY).successes()).isEqualTo(1);
            assertThat(history.readPageVisible("orders", new io.tapstate.spi.store.RateHistoryStore.Visibility(
                    "inc-current", false), observation.observedAt(),
                    observation.observedAt().plusSeconds(1), null, 10).entries()).hasSize(1);
            assertThat(history.readPageVisible("orders", new io.tapstate.spi.store.RateHistoryStore.Visibility(
                    null, true), observation.observedAt(), observation.observedAt().plusSeconds(1),
                    null, 10).entries()).isEmpty();
        }
    }

    @Test
    void aTimedOutWriteOpensTheBreakerWithoutStartingUnboundedReplacementWorkers() throws Exception {
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        List<Long> saved = new ArrayList<>();
        ObservationStore store = new ObservationStore() {
            @Override public void save(Observation observation) {
                long sequence = observation.metrics().get("sequence");
                if (sequence == 1) {
                    firstEntered.countDown();
                    try {
                        if (!releaseFirst.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("first latest write was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
                synchronized (saved) {
                    saved.add(sequence);
                }
            }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        ObservationPublisher publisher = new ObservationPublisher(new InMemoryStateStore(), store);
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                publisher, null, MetricsExport.none(), null, 1, 1,
                java.time.Duration.ofMillis(80))) {
            dispatcher.offer(frame(1), null);
            assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).timeouts() == 0
                    && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            TelemetryDispatcher.Health timedOut = dispatcher.health().get(TelemetryDispatcher.Sink.LATEST);
            assertThat(timedOut.timeouts()).isEqualTo(1);
            assertThat(timedOut.inFlight()).isEqualTo(1);
            assertThat(timedOut.degraded()).isTrue();
            dispatcher.offer(frame(2), null);
            releaseFirst.countDown();
            until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).dropped() == 0
                    && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).dropped()).isGreaterThanOrEqualTo(1);
            until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).successes() == 0
                    && System.nanoTime() < until) {
                dispatcher.offer(frame(3), null);
                TimeUnit.MILLISECONDS.sleep(10);
            }
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).successes()).isGreaterThanOrEqualTo(1);
            synchronized (saved) {
                assertThat(saved).doesNotContain(2L);
            }
        } finally {
            releaseFirst.countDown();
        }
    }

    @Test
    void aFailedLatestWriteMarksLocalHealthDegradedUntilANewerFrameSucceeds() throws Exception {
        AtomicInteger writes = new AtomicInteger();
        CountDownLatch recovered = new CountDownLatch(1);
        ObservationStore store = new ObservationStore() {
            @Override public void save(Observation observation) {
                if (writes.incrementAndGet() == 1) {
                    throw new IllegalStateException("store unavailable");
                }
                recovered.countDown();
            }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        ObservationPublisher publisher = new ObservationPublisher(new InMemoryStateStore(), store);
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                publisher, null, MetricsExport.none(), 1, 1)) {
            dispatcher.offer(frame(1), null);
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).failures() == 0
                    && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            TelemetryDispatcher.Health failed = dispatcher.health().get(TelemetryDispatcher.Sink.LATEST);
            assertThat(failed.failures()).isEqualTo(1);
            assertThat(failed.degraded()).isTrue();
            assertThat(failed.lastSuccessAgeMillis()).isEmpty();

            dispatcher.offer(frame(2), null);
            assertThat(recovered.await(5, TimeUnit.SECONDS)).isTrue();
            until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).successes() == 0
                    && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            TelemetryDispatcher.Health healthy = dispatcher.health().get(TelemetryDispatcher.Sink.LATEST);
            assertThat(healthy.successes()).isEqualTo(1);
            assertThat(healthy.degraded()).isFalse();
            assertThat(healthy.lastSuccessAgeMillis()).isPresent();
        }
    }

    @Test
    void aBlockedLatestWriteKeepsOnlyTheNewestPendingFrameForThatPipeline() throws Exception {
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch lastSaved = new CountDownLatch(1);
        List<Long> saved = new ArrayList<>();
        ObservationStore store = new ObservationStore() {
            @Override public void save(Observation observation) {
                long sequence = observation.metrics().get("sequence");
                if (sequence == 1) {
                    firstEntered.countDown();
                    try {
                        if (!releaseFirst.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("first latest write was not released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                }
                synchronized (saved) {
                    saved.add(sequence);
                }
                if (sequence == 3) {
                    lastSaved.countDown();
                }
            }
            @Override public Optional<Observation> read(String id) { return Optional.empty(); }
            @Override public void delete(String id) { }
        };
        ObservationPublisher publisher = new ObservationPublisher(new InMemoryStateStore(), store);
        try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(
                publisher, null, MetricsExport.none(), 1, 1)) {
            dispatcher.offer(frame(1), null);
            assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
            dispatcher.offer(frame(2), null);
            dispatcher.offer(frame(3), null);
            TelemetryDispatcher.Health blocked = dispatcher.health().get(TelemetryDispatcher.Sink.LATEST);
            assertThat(blocked.queueDepth()).isEqualTo(1);
            assertThat(blocked.highWater()).isEqualTo(1);
            assertThat(blocked.coalesced()).isEqualTo(1);
            assertThat(blocked.successes()).isZero();
            releaseFirst.countDown();
            assertThat(lastSaved.await(5, TimeUnit.SECONDS)).isTrue();
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).successes() < 2
                    && System.nanoTime() < until) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            synchronized (saved) {
                assertThat(saved).containsExactly(1L, 3L);
            }
            TelemetryDispatcher.Health completed = dispatcher.health().get(TelemetryDispatcher.Sink.LATEST);
            assertThat(completed.successes()).isEqualTo(2);
            assertThat(completed.failures()).isZero();
            assertThat(completed.dropped()).isZero();
            assertThat(completed.lastSuccessAgeMillis()).isPresent();
            assertThat(completed.degraded()).isFalse();
        } finally {
            releaseFirst.countDown();
        }
    }

    private static ObservationPublisher.Prepared frame(long sequence) {
        Observation observation = new Observation("orders", PipelineState.RUNNING,
                Map.of("sequence", sequence), Map.of(), Map.of(), null,
                Instant.parse("2026-09-26T10:00:00Z").plusMillis(sequence));
        return new ObservationPublisher.Prepared(observation, false, Map.of(), Map.of(), Map.of());
    }
}
