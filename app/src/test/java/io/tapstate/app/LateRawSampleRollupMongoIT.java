package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.HistoryCursorCodec;
import io.tapstate.control.core.HistoryResolution;
import io.tapstate.control.core.PipelineHistoryQuery;
import io.tapstate.control.core.PipelineHistoryQueryService;
import io.tapstate.control.core.PipelineMetricsHistory;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.HistoryRollupStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** A late raw sample cannot remain hidden by an expired, persisted history bucket. */
@RequiresDocker
class LateRawSampleRollupMongoIT {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void lateLagPeakAppearsAfterExpiryAndCoarseWorkerRecomputesFromRaw() {
        Instant start = Instant.parse("2026-09-27T10:00:00Z");
        MutableClock clock = new MutableClock(start.plus(Duration.ofMinutes(6)));
        try (MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                MONGO.getReplicaSetUrl("late_raw_rollup_it"), null, Duration.ofSeconds(5)))) {
            connection.verify();
            MongoStorePort store = new MongoStorePort(connection, "operator_late_raw_it");
            store.artifacts().saveAll(List.<Resource>of(new PipelineResource("orders", null,
                    List.of(), null, null, null, null, null)));
            String incarnation = store.artifacts().pipelineIncarnationId("orders").orElseThrow();
            ObservationStore.Scope owner = new ObservationStore.Scope(incarnation, 1);
            store.rateHistory().appendScoped(sample(start, 0, 1), owner);
            store.rateHistory().appendScoped(sample(start.plusSeconds(60), 10, 2), owner);
            store.rateHistory().appendScoped(sample(start.plusSeconds(120), 20, 3), owner);
            store.rateHistory().appendScoped(sample(start.plusSeconds(180), 30, 4), owner);
            HistoryRollupStore.Scope scope = HistoryRollupStore.Scope.incarnation(incarnation);
            HistoryRollupStore.Key five = new HistoryRollupStore.Key("orders", scope,
                    HistoryRollupStore.Resolution.PT5M, start);
            try (HistoryRollupWorker worker = worker(store, clock, scope, 1)) {
                worker.runOneBatch();
            }
            HistoryRollupStore.Bucket original = store.historyRollups().read(five).orElseThrow();
            assertThat(original.validUntil()).isEqualTo(clock.instant().plus(Duration.ofMinutes(5)));

            PipelineHistoryQueryService query = new PipelineHistoryQueryService(
                    new ArtifactQueryService(store.artifacts()), store.rateHistory(), store.historyRollups(),
                    Duration.ofMinutes(1), clock,
                    new HistoryCursorCodec("late-raw-test-secret".getBytes(StandardCharsets.UTF_8), clock));
            PipelineHistoryQuery firstWindow = new PipelineHistoryQuery("orders", start,
                    start.plus(Duration.ofMinutes(5)), HistoryResolution.PT5M, 100,
                    List.of("orders"), null);
            assertThat(maxLag(query.query(firstWindow))).isEqualTo(4);

            store.rateHistory().appendScoped(sample(start.plusSeconds(150), 25, 999), owner);
            assertThat(maxLag(query.query(firstWindow))).isEqualTo(4);

            clock.at = original.validUntil().plusMillis(1);
            assertThat(maxLag(query.query(firstWindow))).isEqualTo(999);

            clock.at = start.plus(Duration.ofMinutes(31));
            try (HistoryRollupWorker worker = worker(store, clock, scope, 16)) {
                worker.runOneBatch();
            }
            HistoryRollupStore.Key thirty = new HistoryRollupStore.Key("orders", scope,
                    HistoryRollupStore.Resolution.PT30M, start);
            HistoryRollupStore.Bucket coarse = store.historyRollups().read(thirty).orElseThrow();
            assertThat(coarse.usableAt(clock.instant())).isTrue();
            assertThat(coarse.inputReadStartedAt()).isEqualTo(clock.instant());
            assertThat(maxLag(query.query(new PipelineHistoryQuery("orders", start,
                    start.plus(Duration.ofMinutes(30)), HistoryResolution.PT30M,
                    100, List.of("orders"), null)))).isEqualTo(999);
        }
    }

    private static HistoryRollupWorker worker(MongoStorePort store, Clock clock,
            HistoryRollupStore.Scope scope, int batchSize) {
        return new HistoryRollupWorker(store.rateHistory(), store.historyRollups(), clock,
                Duration.ofMinutes(1), batchSize, Duration.ofSeconds(5),
                () -> List.of(new HistoryRollupWorker.Work("orders", scope)), ignored -> true, false);
    }

    private static RateSample sample(Instant at, long records, long lag) {
        return new RateSample("orders", at, Map.of("records.out", records), Map.of("orders", lag),
                Instant.parse("2026-09-27T09:00:00Z"));
    }

    private static long maxLag(PipelineMetricsHistory response) {
        return response.segments().stream().flatMap(segment -> segment.points().stream())
                .flatMap(point -> point.lag().stream()).mapToLong(PipelineMetricsHistory.Lag::max)
                .max().orElseThrow();
    }

    private static final class MutableClock extends Clock {
        private volatile Instant at;

        private MutableClock(Instant at) {
            this.at = at;
        }

        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return at; }
    }
}
