package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.HistoryRollupStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Real-store witness for the cold worker and its persisted incarnation-scoped bucket. */
@RequiresDocker
class HistoryRollupWorkerMongoIT {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void coldWorkerPersistsOnlyCurrentIncarnationAndRestartSkipsCompletedBucket() {
        Instant bucketStart = Instant.parse("2026-09-27T10:00:00Z");
        HistoryRollupStore.Scope current = HistoryRollupStore.Scope.incarnation("inc-current");
        HistoryRollupStore.Key key = new HistoryRollupStore.Key("orders", current,
                HistoryRollupStore.Resolution.PT5M, bucketStart);
        Clock clock = Clock.fixed(bucketStart.plus(Duration.ofMinutes(6)), ZoneOffset.UTC);
        try (MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                MONGO.getReplicaSetUrl("history_rollup_worker_it"), null, Duration.ofSeconds(5)))) {
            connection.verify();
            MongoStorePort store = new MongoStorePort(connection, "operator_rollup_worker_it");
            store.rateHistory().appendScoped(sample(bucketStart, 0),
                    new ObservationStore.Scope("inc-current", 1));
            store.rateHistory().appendScoped(sample(bucketStart.plusSeconds(60), 10),
                    new ObservationStore.Scope("inc-current", 1));
            store.rateHistory().appendScoped(sample(bucketStart.plusSeconds(120), 20),
                    new ObservationStore.Scope("inc-current", 1));
            store.rateHistory().appendScoped(sample(bucketStart.plusSeconds(180), 900),
                    new ObservationStore.Scope("inc-old", 1));
            List<HistoryRollupWorker.Work> work = List.of(new HistoryRollupWorker.Work("orders", current));

            try (HistoryRollupWorker worker = worker(store, clock, work)) {
                worker.runOneBatch();
            }
            HistoryRollupStore.Bucket persisted = store.historyRollups().read(key).orElseThrow();
            assertThat(persisted.requiresFinerResolution()).isFalse();
            assertThat(persisted.fragments()).isNotEmpty();
            assertThat(persisted.fragments().stream()
                    .map(fragment -> fragment.recordsOut() == null ? BigDecimal.ZERO
                            : fragment.recordsOut().delta())
                    .reduce(BigDecimal.ZERO, BigDecimal::add)).isEqualByComparingTo("20");
            assertThat(store.historyRollups().read(new HistoryRollupStore.Key("orders",
                    HistoryRollupStore.Scope.incarnation("inc-old"),
                    HistoryRollupStore.Resolution.PT5M, bucketStart))).isEmpty();

            try (HistoryRollupWorker restarted = worker(store, clock, work)) {
                restarted.runOneBatch();
            }
            assertThat(store.historyRollups().read(key)).contains(persisted);
        }
    }

    private static HistoryRollupWorker worker(MongoStorePort store, Clock clock,
            List<HistoryRollupWorker.Work> work) {
        return new HistoryRollupWorker(store.rateHistory(), store.historyRollups(), clock,
                Duration.ofMinutes(1), 1, Duration.ofSeconds(5), () -> work, ignored -> true, false);
    }

    private static RateSample sample(Instant at, long records) {
        return new RateSample("orders", at, Map.of("records.out", records), Map.of(),
                Instant.parse("2026-09-27T09:00:00Z"));
    }
}
