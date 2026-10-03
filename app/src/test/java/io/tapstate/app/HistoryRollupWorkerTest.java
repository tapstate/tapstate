package io.tapstate.app;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.HistoryRollupStore;
import io.tapstate.spi.store.HistoryRollupStore.Bucket;
import io.tapstate.spi.store.HistoryRollupStore.Key;
import io.tapstate.spi.store.HistoryRollupStore.Resolution;
import io.tapstate.spi.store.HistoryRollupStore.Scope;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.RateHistoryStore.Visibility;
import io.tapstate.core.model.Resource;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class HistoryRollupWorkerTest {

    private static final String ID = "orders";
    private static final Instant TEN = Instant.parse("2026-09-27T10:00:00Z");
    private static final Instant START = TEN.minus(Duration.ofHours(1));
    private static final Scope SCOPED = Scope.incarnation("inc-a");
    private static final Key FIRST = new Key(ID, SCOPED, Resolution.PT5M, TEN);

    @Test
    void aPipelineWithNoRetainedSamplesDoesNotPublishZeroRollupSeries() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(6)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        MemoryRollups rollups = new MemoryRollups();
        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 1,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            worker.runOneBatch();
            assertThat(HistoryRollupFacts.snapshot(worker.health(), START, clock.instant())).isEmpty();
        }
    }

    @Test
    void processHealthReportsFiveClosedResolutionsAndRecoveryWithoutIdentityLabels() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(6)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN.plusSeconds(60), 10, 1), owner(1));
        MemoryRollups rollups = new MemoryRollups();
        rollups.failNextUpsert = true;

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 1,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            assertThat(HistoryRollupFacts.snapshot(worker.health(), START, clock.instant())).isEmpty();

            worker.runOneBatch();
            HistoryRollupWorker.Health failedHealth = worker.health();
            assertThat(failedHealth.degraded()).isTrue();
            assertThat(failedHealth.levels().get(Resolution.PT5M).failed()).isEqualTo(1);
            assertThat(failedHealth.levels().get(Resolution.PT5M).closedThroughAgeMillis()).isEmpty();

            worker.runOneBatch();
            HistoryRollupWorker.Health recovered = worker.health();
            assertThat(recovered.degraded()).isFalse();
            assertThat(recovered.levels()).containsOnlyKeys(Resolution.PT5M);
            assertThat(recovered.levels().get(Resolution.PT5M).computed()).isEqualTo(1);
            assertThat(recovered.levels().get(Resolution.PT5M).retried()).isEqualTo(1);
            assertThat(recovered.levels().get(Resolution.PT5M).closedThroughAgeMillis())
                    .hasValue(Duration.ofMinutes(1).toMillis());
            assertThat(recovered.maxBatchDurationMillis()).isPresent();

            List<MetricFact> facts = HistoryRollupFacts.snapshot(recovered, START, clock.instant());
            assertThat(facts).extracting(MetricFact::name).contains(
                    "tapstate.process.rollup.closed_through.age",
                    "tapstate.process.rollup.bucket.computed",
                    "tapstate.process.rollup.bucket.retried",
                    "tapstate.process.rollup.bucket.failed",
                    "tapstate.process.rollup.build.raw_fallback",
                    "tapstate.process.rollup.batch.duration.max",
                    "tapstate.process.rollup.degraded");
            assertThat(facts.stream().flatMap(fact -> fact.points().stream()))
                    .allSatisfy(point -> assertThat(point.attributes().keySet())
                            .isSubsetOf(MetricAttributes.ROLLUP_RESOLUTION));
            assertThat(facts.stream().flatMap(fact -> fact.points().stream())
                    .map(point -> point.attributes().get(MetricAttributes.ROLLUP_RESOLUTION))
                    .filter(java.util.Objects::nonNull).distinct())
                    .containsExactly("5m");
        }
    }

    @Test
    void blockedRollupStoreCannotBlockHealthScrapeOrBoundedRefreshOffer() throws Exception {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(6)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN.plusSeconds(60), 10, 1), owner(1));
        MemoryRollups rollups = new MemoryRollups();
        rollups.readEntered = new CountDownLatch(1);
        rollups.releaseRead = new CountDownLatch(1);
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        MetricsExportProperties properties = new MetricsExportProperties();
        properties.getPrometheus().setPort(port);
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try (HistoryRollupWorker worker = new HistoryRollupWorker(raw, rollups, clock,
                Duration.ofMinutes(1), 1, Duration.ofMillis(20),
                () -> List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true, false);
             MetricsExport export = RuntimeConvergenceConfiguration.metricsExportFor(properties)) {
            export.observeProcess("rollup", () -> HistoryRollupFacts.snapshot(
                    worker.health(), START, clock.instant()));
            Future<?> blocked = threads.submit(worker::runOneBatch);
            assertThat(rollups.readEntered.await(5, TimeUnit.SECONDS)).isTrue();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (!worker.health().degraded() && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            Future<HttpResponse<String>> scrape = threads.submit(() -> {
                HistoryRollupWorker.Health health = worker.health();
                assertThat(health.degraded()).isTrue();
                assertThat(health.inFlightAgeMillis()).isPresent();
                for (int index = 0; index < HistoryRollupWorker.MAX_REFRESH_HINTS; index++) {
                    assertThat(worker.requestRefresh(new Key(ID, SCOPED, Resolution.PT5M,
                            TEN.minus(Duration.ofMinutes(5L * index))))).isTrue();
                }
                assertThat(worker.requestRefresh(FIRST)).isTrue();
                assertThat(worker.requestRefresh(new Key(ID, SCOPED, Resolution.PT5M,
                        TEN.minus(Duration.ofMinutes(5L * HistoryRollupWorker.MAX_REFRESH_HINTS)))))
                        .isFalse();
                return HttpClient.newHttpClient().send(
                        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/metrics"))
                                .GET().build(), HttpResponse.BodyHandlers.ofString());
            });
            HttpResponse<String> response = scrape.get(2, TimeUnit.SECONDS);
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).containsPattern("tapstate_process_rollup_degraded\\{[^}]*\\} 1")
                    .contains("tapstate_process_rollup_batch_in_flight_age_milliseconds")
                    .doesNotContain("tapstate_pipeline_id=")
                    .doesNotContain("bucketStart=");
            rollups.releaseRead.countDown();
            blocked.get(5, TimeUnit.SECONDS);
            assertThat(worker.health().degraded()).isFalse();
        } finally {
            rollups.releaseRead.countDown();
            threads.shutdownNow();
        }
    }

    @Test
    void closedFiveMinuteBucketUsesRawAggregatorAndKeepsIncarnationsSeparate() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(6)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.append(sample(TEN, 900, 90), null);
        raw.appendScoped(sample(TEN.plusSeconds(60), 10, 2), owner(1));
        raw.appendScoped(sample(TEN.plusSeconds(120), 40, 7), owner(1));
        raw.appendScoped(sample(TEN.plusSeconds(180), 50, 3), owner(2));
        raw.appendScoped(sample(TEN.plusSeconds(240), 60, 1), owner(2));
        raw.appendScoped(sample(TEN.plusSeconds(300), 70, 1), owner(2));
        MemoryRollups rollups = new MemoryRollups();

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 1,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            worker.runOneBatch();
        }

        Bucket bucket = rollups.read(FIRST).orElseThrow();
        assertThat(bucket.key().scope()).isEqualTo(SCOPED);
        assertThat(bucket.inputReadStartedAt()).isEqualTo(clock.instant());
        assertThat(bucket.validUntil()).isEqualTo(clock.instant().plus(Duration.ofMinutes(5)));
        assertThat(bucket.requiresFinerResolution()).isFalse();
        assertThat(bucket.fragments()).hasSize(2);
        assertThat(bucket.fragments().getFirst().recordsOut().delta())
                .isEqualByComparingTo(new BigDecimal("30"));
        assertThat(bucket.fragments().getFirst().lag()).singleElement()
                .satisfies(lag -> assertThat(lag.max()).isEqualTo(7));
        assertThat(bucket.fragments().get(1).startReason())
                .isEqualTo(HistoryRollupStore.StartReason.CONTINUATION);
        assertThat(bucket.fragments().get(1).recordsOut().delta())
                .isEqualByComparingTo(new BigDecimal("20"));
        assertThat(rollups.read(new Key(ID, Scope.legacy(), Resolution.PT5M, TEN))).isEmpty();
    }

    @Test
    void failedUpsertRetainsTheContiguousPositionAndRestartSkipsCompletedBuckets() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(11)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN.plusSeconds(60), 1, 1), owner(1));
        raw.appendScoped(sample(TEN.plusSeconds(360), 2, 2), owner(1));
        MemoryRollups rollups = new MemoryRollups();
        rollups.failNextUpsert = true;
        List<HistoryRollupWorker.Work> work = List.of(new HistoryRollupWorker.Work(ID, SCOPED));

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 1, work, ignored -> true)) {
            worker.runOneBatch();
            assertThat(rollups.read(FIRST)).isEmpty();
            worker.runOneBatch();
            assertThat(rollups.read(FIRST)).isPresent();
        }
        try (HistoryRollupWorker restarted = worker(raw, rollups, clock, 2, work, ignored -> true)) {
            restarted.runOneBatch();
        }

        assertThat(rollups.attempts).containsExactly(FIRST, FIRST,
                new Key(ID, SCOPED, Resolution.PT5M, TEN.plus(Duration.ofMinutes(5))));
        assertThat(rollups.rows).hasSize(2);
    }

    @Test
    void oneFailedBucketIsAttemptedOnlyOncePerColdPass() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(6)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN.plusSeconds(60), 1, 1), owner(1));
        MemoryRollups rollups = new MemoryRollups();
        rollups.alwaysFail = true;

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 16,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            worker.runOneBatch();
        }

        assertThat(rollups.attempts).containsExactly(FIRST);
    }


    @Test
    void ownerDenialDoesNotReadRawOrWriteAndLateRefreshIsBoundedAndExplicit() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(6)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN.plusSeconds(60), 10, 1), owner(1));
        raw.appendScoped(sample(TEN.plusSeconds(120), 20, 2), owner(1));
        MemoryRollups rollups = new MemoryRollups();
        AtomicBoolean owner = new AtomicBoolean(false);
        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 1,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> owner.get())) {
            worker.runOneBatch();
            assertThat(rollups.rows).isEmpty();
            owner.set(true);
            worker.runOneBatch();
            Bucket first = rollups.read(FIRST).orElseThrow();

            clock.at = TEN.plus(Duration.ofMinutes(12));
            raw.appendScoped(sample(TEN.plusSeconds(180), 30, 8), owner(1));
            worker.runOneBatch();
            assertThat(rollups.read(FIRST).orElseThrow()).isEqualTo(first);
            assertThat(worker.requestRefresh(FIRST)).isTrue();
            assertThat(worker.requestRefresh(FIRST)).isTrue();
            worker.runOneBatch();
            Bucket refreshed = rollups.read(FIRST).orElseThrow();
            assertThat(refreshed.validUntil()).isEqualTo(clock.instant().plus(Duration.ofMinutes(5)));
            assertThat(refreshed.fragments().getFirst().lag().getFirst().max()).isEqualTo(8);
        }
        assertThat(rollups.attempts.stream().filter(FIRST::equals).count()).isEqualTo(2);
    }

    @Test
    void excessiveResetFragmentsUseFallbackMarkerInsteadOfUnboundedDocument() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(6)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        for (int second = 0; second < 75; second++) {
            Instant at = TEN.plusSeconds(second);
            raw.appendScoped(new RateSample(ID, at, Map.of("records.out", (long) second),
                    Map.of("orders", (long) second), at), owner(1));
        }
        MemoryRollups rollups = new MemoryRollups();
        try (HistoryRollupWorker worker = new HistoryRollupWorker(raw, rollups, clock,
                Duration.ofSeconds(1), 1, Duration.ofSeconds(5),
                () -> List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true, false)) {
            worker.runOneBatch();
        }
        Bucket bucket = rollups.read(FIRST).orElseThrow();
        assertThat(bucket.requiresFinerResolution()).isTrue();
        assertThat(bucket.fragments()).isEmpty();
        assertThat(bucket.gaps()).isEmpty();
    }

    @Test
    void aStoppedPipelineDoesNotAccumulateEmptyFutureBuckets() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofHours(2)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN.plusSeconds(60), 10, 1), owner(1));
        MemoryRollups rollups = new MemoryRollups();
        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 16,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            worker.runOneBatch();
            worker.runOneBatch();
        }

        assertThat(rollups.rows.keySet()).extracting(Key::resolution)
                .containsExactlyInAnyOrder(Resolution.values());
        assertThat(rollups.rows.keySet()).allSatisfy(key -> {
            assertThat(key.bucketStart()).isBeforeOrEqualTo(TEN.plusSeconds(60));
            assertThat(key.bucketEnd()).isAfter(TEN.plusSeconds(60));
        });
    }

    @Test
    void resetAndGapBoundariesMatchTheRawAggregationRules() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(6)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN, 0, 1), owner(1));
        raw.appendScoped(sample(TEN.plusSeconds(60), 10, 4), owner(1));
        Instant restarted = START.plusSeconds(1);
        raw.appendScoped(new RateSample(ID, TEN.plusSeconds(120), Map.of("records.out", 0L),
                Map.of("orders", 2L), restarted), owner(2));
        raw.appendScoped(new RateSample(ID, TEN.plusSeconds(180), Map.of("records.out", 5L),
                Map.of("orders", 7L), restarted), owner(2));
        raw.appendScoped(new RateSample(ID, TEN.plusSeconds(300), Map.of("records.out", 6L),
                Map.of("orders", 1L), restarted), owner(2));
        MemoryRollups rollups = new MemoryRollups();

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 1,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            worker.runOneBatch();
        }

        Bucket bucket = rollups.read(FIRST).orElseThrow();
        assertThat(bucket.requiresFinerResolution()).isFalse();
        assertThat(bucket.fragments()).hasSize(2);
        assertThat(bucket.fragments().getFirst().recordsOut().delta()).isEqualByComparingTo("10");
        assertThat(bucket.fragments().get(1).startReason())
                .isEqualTo(HistoryRollupStore.StartReason.COUNTER_RESET);
        assertThat(bucket.fragments().get(1).recordsOut().delta()).isEqualByComparingTo("5");
        assertThat(bucket.fragments().get(1).lag().getFirst().max()).isEqualTo(7);
        assertThat(bucket.gaps()).singleElement().satisfies(gap -> {
            assertThat(gap.intervalStart()).isEqualTo(TEN.plusSeconds(180));
            assertThat(gap.intervalEnd()).isEqualTo(TEN.plusSeconds(300));
        });
    }

    @Test
    void expiryIsAnchoredToInputReadRatherThanComputationCompletion() {
        TickingClock clock = new TickingClock(TEN.plus(Duration.ofMinutes(6)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN.plusSeconds(60), 10, 1), owner(1));
        MemoryRollups rollups = new MemoryRollups();

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 1,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            worker.runOneBatch();
        }

        Bucket bucket = rollups.read(FIRST).orElseThrow();
        assertThat(bucket.computedAt()).isAfter(bucket.inputReadStartedAt());
        assertThat(bucket.validUntil()).isEqualTo(bucket.inputReadStartedAt().plus(Duration.ofMinutes(5)));
        assertThat(bucket.validUntil()).isBefore(bucket.computedAt().plus(Duration.ofMinutes(5)));
    }

    @Test
    void wiringKeepsLegacyAndCurrentIncarnationAsDistinctWorkAndRejectsStaleOwner() {
        Map<String, ArtifactStore.HistoryOwner> owners = Map.of(
                "legacy", new ArtifactStore.HistoryOwner(new Visibility(null, true)),
                "upgraded", new ArtifactStore.HistoryOwner(new Visibility("inc-a", true)),
                "new", new ArtifactStore.HistoryOwner(new Visibility("inc-b", false)));
        ArtifactStore artifacts = new ArtifactStore() {
            @Override public void saveAll(List<Resource> resources) { throw new UnsupportedOperationException(); }
            @Override public Optional<Resource> get(String id) { return Optional.empty(); }
            @Override public List<Resource> list() { return List.of(); }
            @Override public Optional<HistoryOwner> pipelineHistoryOwner(String id) {
                return Optional.ofNullable(owners.get(id));
            }
        };

        assertThat(HistoryRollupConfiguration.currentWork(
                List.of("legacy", "upgraded", "new", "deleted"), artifacts)).containsExactly(
                new HistoryRollupWorker.Work("legacy", Scope.legacy()),
                new HistoryRollupWorker.Work("upgraded", Scope.legacy()),
                new HistoryRollupWorker.Work("upgraded", Scope.incarnation("inc-a")),
                new HistoryRollupWorker.Work("new", Scope.incarnation("inc-b")));
        assertThat(HistoryRollupConfiguration.currentOwner(artifacts,
                new HistoryRollupWorker.Work("upgraded", Scope.incarnation("inc-old")))).isFalse();
        assertThat(HistoryRollupConfiguration.currentOwner(artifacts,
                new HistoryRollupWorker.Work("new", Scope.legacy()))).isFalse();
    }

    @Test
    void onePassSharesItsFixedBatchAcrossPipelines() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(16)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN.plusSeconds(60), 1, 1), owner(1));
        raw.appendScoped(sample(TEN.plusSeconds(360), 2, 2), owner(1));
        raw.appendScoped(new RateSample("other", TEN.plusSeconds(60), Map.of("records.out", 1L),
                Map.of(), START), new ObservationStore.Scope("inc-b", 1));
        raw.appendScoped(new RateSample("other", TEN.plusSeconds(360), Map.of("records.out", 2L),
                Map.of(), START), new ObservationStore.Scope("inc-b", 1));
        MemoryRollups rollups = new MemoryRollups();
        List<HistoryRollupWorker.Work> work = List.of(new HistoryRollupWorker.Work(ID, SCOPED),
                new HistoryRollupWorker.Work("other", Scope.incarnation("inc-b")));

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 2, work, ignored -> true)) {
            worker.runOneBatch();
        }

        assertThat(rollups.attempts).containsExactly(
                FIRST, new Key("other", Scope.incarnation("inc-b"), Resolution.PT5M, TEN));
    }

    @Test
    void everyCoarseLevelInheritsTheEarliestChildDeadlineAndExactCounterCoverage() {
        Instant start = Instant.parse("2026-09-27T06:00:00Z");
        for (Resolution resolution : List.of(Resolution.PT30M, Resolution.PT1H,
                Resolution.PT3H, Resolution.PT6H)) {
            Key target = new Key(ID, SCOPED, resolution, start);
            MutableClock clock = new MutableClock(target.bucketEnd().plus(Duration.ofMinutes(1)));
            InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
            raw.appendScoped(sample(start.plusSeconds(60), 1, 1), owner(1));
            MemoryRollups rollups = new MemoryRollups();
            Resolution finer = switch (resolution) {
                case PT30M -> Resolution.PT5M;
                case PT1H -> Resolution.PT30M;
                case PT3H -> Resolution.PT1H;
                case PT6H -> Resolution.PT3H;
                default -> throw new AssertionError(resolution);
            };
            int childIndex = 0;
            for (Instant at = start; at.isBefore(target.bucketEnd()); at = at.plus(finer.duration())) {
                rollups.upsert(child(new Key(ID, SCOPED, finer, at), clock.instant(), childIndex++));
            }
            try (HistoryRollupWorker worker = worker(raw, rollups, clock, 16,
                    List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
                assertThat(worker.requestRefresh(target)).isTrue();
                worker.runOneBatch();
            }

            Bucket coarse = rollups.read(target).orElseThrow();
            assertThat(coarse.inputReadStartedAt()).isEqualTo(clock.instant().minusSeconds(60));
            assertThat(coarse.validUntil()).isEqualTo(clock.instant().plusSeconds(120));
            assertThat(coarse.inWindowSamples()).isEqualTo(resolution.duration().toMinutes());
            assertThat(coarse.fragments()).singleElement().satisfies(fragment -> {
                assertThat(fragment.recordsOut().delta())
                        .isEqualByComparingTo(BigDecimal.valueOf(resolution.duration().toSeconds()));
                assertThat(fragment.recordsOut().averageRate()).isEqualByComparingTo("1");
                assertThat(fragment.recordsOutStats().coveredNanos())
                        .isEqualTo(resolution.duration().toNanos());
                assertThat(fragment.resumeAt()).isEqualTo(target.bucketEnd());
            });
        }
    }

    @Test
    void cascadedThirtyMinuteBucketMatchesRawForIrregularIntervalsAndLagFrames() {
        Key target = new Key(ID, SCOPED, Resolution.PT30M, TEN);
        MutableClock clock = new MutableClock(target.bucketEnd().plus(Duration.ofMinutes(1)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        for (int minute = 0; minute <= 30; minute++) {
            Instant at = TEN.plusSeconds(minute * 60L + minute % 3 * 5L);
            raw.appendScoped(sample(at, minute * 3L, minute % 7), owner(1));
        }
        MemoryRollups rollups = new MemoryRollups();
        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 16,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            for (int pass = 0; pass < 8; pass++) {
                worker.runOneBatch();
            }
            Bucket direct = rollups.read(target).orElseThrow();
            assertThat(rollups.readRange(ID, SCOPED, Resolution.PT5M,
                    TEN, target.bucketEnd(), 6)).hasSize(6);
            rollups.rows.remove(target);

            assertThat(worker.requestRefresh(target)).isTrue();
            worker.runOneBatch();

            Bucket cascaded = rollups.read(target).orElseThrow();
            assertThat(cascaded.inputReadStartedAt()).isEqualTo(direct.inputReadStartedAt());
            assertThat(cascaded.validUntil()).isEqualTo(direct.validUntil());
            assertThat(cascaded.inWindowSamples()).isEqualTo(direct.inWindowSamples());
            assertThat(cascaded.fragments()).isEqualTo(direct.fragments());
        }
    }

    @Test
    void expiredChildIsRebuiltFromRawWithoutPublishingAnExpiredCoarseBucket() {
        Key target = new Key(ID, SCOPED, Resolution.PT30M, TEN);
        MutableClock clock = new MutableClock(target.bucketEnd().plus(Duration.ofMinutes(1)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN.plusSeconds(60), 1, 1), owner(1));
        MemoryRollups rollups = new MemoryRollups();
        int childIndex = 0;
        for (Instant at = TEN; at.isBefore(target.bucketEnd()); at = at.plus(Duration.ofMinutes(5))) {
            rollups.upsert(child(new Key(ID, SCOPED, Resolution.PT5M, at), clock.instant(), childIndex++));
        }
        Key firstChild = new Key(ID, SCOPED, Resolution.PT5M, TEN);
        Bucket first = rollups.read(firstChild).orElseThrow();
        rollups.upsert(new Bucket(first.key(), first.computedAt(), first.inputReadStartedAt(),
                clock.instant(), false, first.fragments(), first.gaps(), first.inWindowSamples()));

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 16,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            assertThat(worker.requestRefresh(target)).isTrue();
            worker.runOneBatch();
        }

        Bucket coarse = rollups.read(target).orElseThrow();
        assertThat(coarse.inputReadStartedAt()).isEqualTo(clock.instant());
        assertThat(coarse.validUntil()).isEqualTo(clock.instant().plus(Duration.ofMinutes(5)));
        assertThat(coarse.inWindowSamples()).isEqualTo(1);
    }

    @Test
    void childGapForcesRawReaggregationSoTheCoarseGapIsPreserved() {
        Key target = new Key(ID, SCOPED, Resolution.PT30M, TEN);
        MutableClock clock = new MutableClock(target.bucketEnd().plus(Duration.ofMinutes(1)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(sample(TEN, 0, 1), owner(1));
        raw.appendScoped(sample(TEN.plusSeconds(60), 10, 2), owner(1));
        raw.appendScoped(sample(TEN.plusSeconds(300), 20, 3), owner(1));
        raw.appendScoped(sample(TEN.plusSeconds(360), 30, 4), owner(1));
        MemoryRollups rollups = new MemoryRollups();
        int childIndex = 0;
        for (Instant at = TEN; at.isBefore(target.bucketEnd()); at = at.plus(Duration.ofMinutes(5))) {
            rollups.upsert(child(new Key(ID, SCOPED, Resolution.PT5M, at), clock.instant(), childIndex++));
        }
        Key firstChild = new Key(ID, SCOPED, Resolution.PT5M, TEN);
        Bucket first = rollups.read(firstChild).orElseThrow();
        rollups.upsert(new Bucket(first.key(), first.computedAt(), first.inputReadStartedAt(),
                first.validUntil(), false, first.fragments(),
                List.of(new HistoryRollupStore.Gap(1, TEN.plusSeconds(60),
                        TEN.plusSeconds(300), HistoryRollupStore.GapReason.SAMPLE_GAP)),
                first.inWindowSamples()));

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 16,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            assertThat(worker.requestRefresh(target)).isTrue();
            worker.runOneBatch();
        }

        Bucket coarse = rollups.read(target).orElseThrow();
        assertThat(coarse.inputReadStartedAt()).isEqualTo(clock.instant());
        assertThat(coarse.gaps()).singleElement().satisfies(gap -> {
            assertThat(gap.intervalStart()).isEqualTo(TEN.plusSeconds(60));
            assertThat(gap.intervalEnd()).isEqualTo(TEN.plusSeconds(300));
        });
    }

    @Test
    void rawBucketRestoresResetEvidenceBeforeAnUnknownPredecessor() {
        InMemoryRateHistoryStore raw = unknownPredecessor(false, false);
        Bucket bucket = refreshRaw(raw);

        assertThat(bucket.requiresFinerResolution()).isFalse();
        assertThat(bucket.inWindowSamples()).isEqualTo(2);
        assertThat(bucket.fragments()).singleElement().satisfies(fragment -> {
            assertThat(fragment.startReason()).isEqualTo(HistoryRollupStore.StartReason.COUNTER_RESET);
            assertThat(fragment.intervalStart()).isEqualTo(TEN);
            assertThat(fragment.recordsOut().delta()).isEqualByComparingTo("1");
            assertThat(fragment.bytesOut().delta()).isEqualByComparingTo("10");
            assertThat(fragment.recordsOutStats().coveredNanos()).isEqualTo(Duration.ofMinutes(1).toNanos());
        });
        assertThat(bucket.gaps()).isEmpty();
    }

    @Test
    void rawBucketKeepsTheSameStartAcrossAnUnknownPredecessor() {
        Bucket bucket = refreshRaw(unknownPredecessor(true, false));

        assertThat(bucket.requiresFinerResolution()).isFalse();
        assertThat(bucket.fragments()).singleElement().satisfies(fragment -> {
            assertThat(fragment.startReason()).isEqualTo(HistoryRollupStore.StartReason.CONTINUATION);
            assertThat(fragment.recordsOut().delta()).isEqualByComparingTo("1");
            assertThat(fragment.bytesOut().delta()).isEqualByComparingTo("10");
            assertThat(fragment.recordsOutStats().coveredNanos()).isEqualTo(Duration.ofMinutes(1).toNanos());
        });
        assertThat(bucket.gaps()).isEmpty();
    }

    @Test
    void equalTimeUnknownPredecessorReplaysTheEarlierStableKey() {
        Bucket bucket = refreshRaw(unknownPredecessor(false, true));

        assertThat(bucket.requiresFinerResolution()).isFalse();
        assertThat(bucket.fragments()).singleElement().satisfies(fragment -> {
            assertThat(fragment.startReason()).isEqualTo(HistoryRollupStore.StartReason.COUNTER_RESET);
            assertThat(fragment.recordsOut().delta()).isEqualByComparingTo("1");
            assertThat(fragment.bytesOut().delta()).isEqualByComparingTo("10");
        });
        assertThat(bucket.gaps()).isEmpty();
    }

    @Test
    void aGapBeforeAnUnknownPredecessorEndsOldResetEvidence() {
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(outputs(TEN.minusSeconds(120), 3, START), owner(1));
        raw.appendScoped(new RateSample(ID, TEN.minusSeconds(60), Map.of(), Map.of(), null), owner(2),
                TEN.minusSeconds(120));
        raw.appendScoped(outputs(TEN, 1, START.plusSeconds(1)), owner(2));
        raw.appendScoped(outputs(TEN.plusSeconds(60), 2, START.plusSeconds(1)), owner(2));
        Bucket bucket = refreshRaw(raw);

        assertThat(bucket.fragments()).singleElement().satisfies(fragment -> {
            assertThat(fragment.startReason()).isEqualTo(HistoryRollupStore.StartReason.CONTINUATION);
            assertThat(fragment.recordsOut().delta()).isEqualByComparingTo("1");
        });
        assertThat(bucket.gaps()).isEmpty();
    }

    @Test
    void aFreshAmbiguousBucketIsRebuiltBeforeReuse() {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(6)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(outputs(TEN, 3, START), owner(1));
        raw.appendScoped(new RateSample(ID, TEN.plusSeconds(60), Map.of(), Map.of(), null), owner(2));
        raw.appendScoped(outputs(TEN.plusSeconds(120), 1, START.plusSeconds(1)), owner(2));
        raw.appendScoped(outputs(TEN.plusSeconds(180), 61, START.plusSeconds(1)), owner(2));
        var entries = raw.readPageVisible(ID, new Visibility("inc-a", false),
                TEN, FIRST.bucketEnd(), null, 10).entries();
        var first = new HistoryRollupStore.Fragment(0, HistoryRollupStore.StartReason.WINDOW_START,
                TEN, TEN, null, null, List.of(), null, null, entries.getFirst().key(), TEN);
        var continued = staleContinuation(TEN.plusSeconds(60), TEN.plusSeconds(180),
                entries.getLast().key());
        MemoryRollups rollups = new MemoryRollups();
        rollups.upsert(new Bucket(FIRST, clock.instant(), clock.instant(),
                clock.instant().plus(Duration.ofMinutes(5)), false,
                List.of(first, continued), List.of(), 4));
        rollups.attempts.clear();

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 1,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            worker.runOneBatch();
        }

        Bucket rebuilt = rollups.read(FIRST).orElseThrow();
        assertThat(rollups.attempts).containsExactly(FIRST);
        assertThat(rebuilt.fragments()).filteredOn(fragment ->
                fragment.startReason() == HistoryRollupStore.StartReason.COUNTER_RESET)
                .singleElement().satisfies(fragment -> {
                    assertThat(fragment.intervalStart()).isEqualTo(TEN.plusSeconds(120));
                    assertThat(fragment.recordsOut().delta()).isEqualByComparingTo("60");
                });
        assertThat(rebuilt.gaps()).isEmpty();
    }

    @Test
    void cascadeRejectsAChildThatLostAResetBehindAnUnknownSample() {
        Key target = new Key(ID, SCOPED, Resolution.PT30M, TEN);
        MutableClock clock = new MutableClock(target.bucketEnd().plus(Duration.ofMinutes(1)));
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(outputs(TEN.minusSeconds(60), 3, START), owner(1));
        raw.appendScoped(new RateSample(ID, TEN, Map.of(), Map.of(), null), owner(2));
        raw.appendScoped(outputs(TEN.plusSeconds(60), 1, START.plusSeconds(1)), owner(2));
        raw.appendScoped(outputs(TEN.plusSeconds(120), 61, START.plusSeconds(1)), owner(2));
        var last = raw.readPageVisible(ID, new Visibility("inc-a", false), TEN, FIRST.bucketEnd(),
                null, 10).entries().getLast().key();
        MemoryRollups rollups = new MemoryRollups();
        for (Instant at = TEN; at.isBefore(target.bucketEnd()); at = at.plus(Duration.ofMinutes(5))) {
            Key childKey = new Key(ID, SCOPED, Resolution.PT5M, at);
            List<HistoryRollupStore.Fragment> fragments = at.equals(TEN)
                    ? List.of(staleContinuation(TEN, TEN.plusSeconds(120), last)) : List.of();
            rollups.upsert(new Bucket(childKey, clock.instant(), clock.instant(),
                    clock.instant().plus(Duration.ofMinutes(5)), false, fragments, List.of(),
                    at.equals(TEN) ? 3 : 0));
        }
        rollups.attempts.clear();

        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 1,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            assertThat(worker.requestRefresh(target)).isTrue();
            worker.runOneBatch();
            worker.runOneBatch();
        }

        Bucket rebuilt = rollups.read(target).orElseThrow();
        assertThat(rebuilt.requiresFinerResolution()).isFalse();
        assertThat(rebuilt.inWindowSamples()).isEqualTo(3);
        assertThat(rebuilt.fragments()).filteredOn(fragment ->
                fragment.startReason() == HistoryRollupStore.StartReason.COUNTER_RESET)
                .singleElement().satisfies(fragment -> {
                    assertThat(fragment.intervalStart()).isEqualTo(TEN.plusSeconds(60));
                    assertThat(fragment.recordsOut().delta()).isEqualByComparingTo("60");
                });
        assertThat(rebuilt.gaps()).isEmpty();
    }

    @Test
    void aPrefixThatExhaustsTheExistingRawBudgetUsesTheFallbackMarker() {
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        for (int before = HistoryRollupWorker.MAX_RAW_ENTRIES_PER_BUCKET + 1; before > 0; before--) {
            raw.appendScoped(new RateSample(ID, TEN.minusSeconds(before), Map.of(), Map.of(), null), owner(1));
        }
        raw.appendScoped(outputs(TEN, 1, START), owner(1));
        raw.appendScoped(outputs(TEN.plusSeconds(60), 2, START), owner(1));
        Bucket bucket = refreshRaw(raw);

        assertThat(bucket.requiresFinerResolution()).isTrue();
        assertThat(bucket.fragments()).isEmpty();
        assertThat(bucket.gaps()).isEmpty();
    }

    private static InMemoryRateHistoryStore unknownPredecessor(boolean continuing, boolean equalTime) {
        InMemoryRateHistoryStore raw = new InMemoryRateHistoryStore();
        raw.appendScoped(outputs(TEN.minusSeconds(equalTime ? 60 : 120), 3, START), owner(1));
        raw.appendScoped(new RateSample(ID, TEN.minusSeconds(60), Map.of(), Map.of(), null), owner(2));
        Instant nextStart = continuing ? START : START.plusSeconds(1);
        long nextValue = continuing ? 4 : 1;
        raw.appendScoped(outputs(TEN, nextValue, nextStart), owner(2));
        raw.appendScoped(outputs(TEN.plusSeconds(60), nextValue + 1, nextStart), owner(2));
        raw.appendScoped(outputs(TEN.minusSeconds(30), 900, START.plusSeconds(2)),
                new ObservationStore.Scope("other-incarnation", 1));
        raw.append(outputs(TEN.minusSeconds(30), 800, START.plusSeconds(3)));
        return raw;
    }

    private static Bucket refreshRaw(InMemoryRateHistoryStore raw) {
        MutableClock clock = new MutableClock(TEN.plus(Duration.ofMinutes(6)));
        MemoryRollups rollups = new MemoryRollups();
        try (HistoryRollupWorker worker = worker(raw, rollups, clock, 1,
                List.of(new HistoryRollupWorker.Work(ID, SCOPED)), ignored -> true)) {
            assertThat(worker.requestRefresh(FIRST)).isTrue();
            worker.runOneBatch();
        }
        return rollups.read(FIRST).orElseThrow();
    }

    private static RateSample outputs(Instant at, long records, Instant start) {
        return new RateSample(ID, at, Map.of("records.out", records, "bytes.out", records * 10),
                Map.of(), start);
    }

    private static HistoryRollupStore.Fragment staleContinuation(Instant start, Instant end,
            io.tapstate.spi.store.RateHistoryStore.Key last) {
        var rate = new HistoryRollupStore.Rate(BigDecimal.valueOf(60), BigDecimal.ONE, BigDecimal.ONE);
        var stats = new HistoryRollupStore.CounterStats(BigDecimal.valueOf(60),
                Duration.ofMinutes(1).toNanos(), BigDecimal.ONE);
        var byteRate = new HistoryRollupStore.Rate(BigDecimal.valueOf(600), BigDecimal.TEN, BigDecimal.TEN);
        var byteStats = new HistoryRollupStore.CounterStats(BigDecimal.valueOf(600),
                Duration.ofMinutes(1).toNanos(), BigDecimal.TEN);
        return new HistoryRollupStore.Fragment(0, HistoryRollupStore.StartReason.CONTINUATION,
                start, end, rate, byteRate, List.of(), stats, byteStats, last, end);
    }

    private static Bucket child(Key key, Instant now, int childIndex) {
        Instant readAt = now.minusSeconds(60);
        Instant validUntil = now.plusSeconds(childIndex == 0 ? 120 : 240);
        long seconds = key.resolution().duration().toSeconds();
        HistoryRollupStore.CounterStats stats = new HistoryRollupStore.CounterStats(
                BigDecimal.valueOf(seconds), key.resolution().duration().toNanos(), BigDecimal.ONE);
        HistoryRollupStore.Fragment fragment = new HistoryRollupStore.Fragment(0,
                childIndex == 0 ? HistoryRollupStore.StartReason.WINDOW_START
                        : HistoryRollupStore.StartReason.CONTINUATION,
                key.bucketStart(), key.bucketEnd(),
                new HistoryRollupStore.Rate(BigDecimal.valueOf(seconds), BigDecimal.ONE, BigDecimal.ONE),
                null, List.of(), stats, null,
                new io.tapstate.spi.store.RateHistoryStore.Key(
                        key.bucketEnd().minusSeconds(60), "child-" + childIndex), key.bucketEnd());
        return new Bucket(key, now.minusSeconds(30), readAt, validUntil, false,
                List.of(fragment), List.of(), (int) key.resolution().duration().toMinutes());
    }

    private static HistoryRollupWorker worker(InMemoryRateHistoryStore raw,
            MemoryRollups rollups, Clock clock, int batchSize, List<HistoryRollupWorker.Work> work,
            java.util.function.Predicate<HistoryRollupWorker.Work> allowed) {
        return new HistoryRollupWorker(raw, rollups, clock, Duration.ofMinutes(1), batchSize,
                Duration.ofSeconds(5), () -> work, allowed, false);
    }

    private static ObservationStore.Scope owner(long generation) {
        return new ObservationStore.Scope("inc-a", generation);
    }

    private static RateSample sample(Instant at, long records, long lag) {
        return new RateSample(ID, at, Map.of("records.out", records), Map.of("orders", lag), START);
    }

    private static final class MutableClock extends Clock {
        private Instant at;

        private MutableClock(Instant at) { this.at = at; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return at; }
    }

    private static final class TickingClock extends Clock {
        private Instant next;

        private TickingClock(Instant at) { next = at; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() {
            Instant current = next;
            next = next.plusSeconds(1);
            return current;
        }
    }

    private static final class MemoryRollups implements HistoryRollupStore {
        private final Map<Key, Bucket> rows = new HashMap<>();
        private final List<Key> attempts = new ArrayList<>();
        private boolean failNextUpsert;
        private boolean alwaysFail;
        private CountDownLatch readEntered;
        private CountDownLatch releaseRead;

        @Override public void upsert(Bucket bucket) {
            attempts.add(bucket.key());
            if (failNextUpsert || alwaysFail) {
                failNextUpsert = false;
                throw new IllegalStateException("injected rollup write failure");
            }
            rows.put(bucket.key(), bucket);
        }
        @Override public Optional<Bucket> read(Key key) {
            if (readEntered != null) {
                readEntered.countDown();
                try {
                    if (!releaseRead.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("injected rollup read did not resume");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("injected rollup read was interrupted", interrupted);
                }
            }
            return Optional.ofNullable(rows.get(key));
        }
        @Override public List<Bucket> readRange(String pipelineId, Scope scope, Resolution resolution,
                Instant from, Instant to, int limit) {
            return rows.values().stream().filter(bucket -> bucket.key().pipelineId().equals(pipelineId))
                    .filter(bucket -> bucket.key().scope().equals(scope))
                    .filter(bucket -> bucket.key().resolution() == resolution)
                    .filter(bucket -> !bucket.key().bucketStart().isBefore(from)
                            && bucket.key().bucketStart().isBefore(to))
                    .sorted(java.util.Comparator.comparing(bucket -> bucket.key().bucketStart()))
                    .limit(limit).toList();
        }
        @Override public void deleteIncarnation(String pipelineId, String incarnationId) { }
        @Override public void deleteLegacy(String pipelineId) { }
        @Override public Duration retention() { return Duration.ofDays(15); }
    }
}
