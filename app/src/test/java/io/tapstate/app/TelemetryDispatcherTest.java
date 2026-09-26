package io.tapstate.app;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineEventStore;
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
