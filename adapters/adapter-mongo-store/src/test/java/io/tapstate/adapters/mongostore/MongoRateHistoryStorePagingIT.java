package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.RateHistoryStore.Entry;
import io.tapstate.spi.store.RateHistoryStore.Page;
import io.tapstate.spi.store.RateHistoryStore.Visibility;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Real-store witness for the stable, bounded keyset contract. */
@RequiresDocker
class MongoRateHistoryStorePagingIT {

    @Test
    void newHistoryScopesSurviveRecreateAndOldCleanupCannotDeleteTheNewRun() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("history_scope_it");
            database.drop();
            MongoRateHistoryStore history = new MongoRateHistoryStore(database,
                    SystemCollections.PIPELINE_RATE_HISTORY.on(database), Duration.ofDays(15));
            history.append(sample("orders", T0, 0));
            history.appendScoped(sample("orders", T0.plusSeconds(60), 1),
                    new ObservationStore.Scope("inc-a", 41));
            history.appendScoped(sample("orders", T0.plusSeconds(120), 2),
                    new ObservationStore.Scope("inc-a", 42));
            history.appendScoped(sample("orders", T0.plusSeconds(180), 3),
                    new ObservationStore.Scope("inc-b", 43));

            Page firstIncarnation = history.readPageVisible("orders", new Visibility("inc-a", true),
                    T0, T0.plusSeconds(240), null, 10);
            assertThat(firstIncarnation.entries())
                    .extracting(entry -> entry.sample().counters().get("records.out"))
                    .containsExactly(0L, 1L, 2L);
            assertThat(firstIncarnation.entries()).extracting(Entry::scope)
                    .containsExactly(java.util.Optional.empty(),
                            java.util.Optional.of(new ObservationStore.Scope("inc-a", 41)),
                            java.util.Optional.of(new ObservationStore.Scope("inc-a", 42)));
            assertThat(history.readVisible("orders", new Visibility("inc-b", false),
                    firstIncarnation.entries().get(1).key())).isEmpty();
            assertThat(history.predecessorVisible("orders", new Visibility("inc-b", false),
                    T0.plusSeconds(240))).get()
                    .extracting(entry -> entry.sample().counters().get("records.out")).isEqualTo(3L);
            assertThat(history.successorVisible("orders", new Visibility("inc-a", false),
                    T0.plusSeconds(90))).get()
                    .extracting(entry -> entry.sample().counters().get("records.out")).isEqualTo(2L);
            assertThat(history.readPageVisible("orders", new Visibility(null, true),
                    T0, T0.plusSeconds(240), null, 10).entries())
                    .extracting(entry -> entry.sample().counters().get("records.out"))
                    .containsExactly(0L);
            assertThat(history.readPageVisible("orders", new Visibility("inc-b", false),
                    T0, T0.plusSeconds(240), null, 10).entries())
                    .extracting(entry -> entry.sample().counters().get("records.out"))
                    .containsExactly(3L);

            history.deleteIncarnation("orders", "inc-a");
            assertThat(history.readPageVisible("orders", new Visibility("inc-a", true),
                    T0, T0.plusSeconds(240), null, 10).entries())
                    .extracting(entry -> entry.sample().counters().get("records.out"))
                    .containsExactly(0L);
            history.deleteLegacy("orders");
            assertThat(history.readPageVisible("orders", new Visibility("inc-b", false),
                    T0, T0.plusSeconds(240), null, 10).entries())
                    .extracting(entry -> entry.sample().counters().get("records.out"))
                    .containsExactly(3L);
        }
    }

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static final Instant T0 = Instant.parse("2026-09-21T10:00:00Z");

    @Test
    void equalTimestampsPageWithoutDuplicatesAndBoundariesStayHalfOpen() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("history_keyset_it");
            database.drop();
            MongoRateHistoryStore history = new MongoRateHistoryStore(database,
                    SystemCollections.PIPELINE_RATE_HISTORY.on(database), Duration.ofDays(15));

            history.append(sample("orders", T0.minusSeconds(60), 0));
            history.append(sample("orders", T0, 1));
            history.append(sample("orders", T0, 2));
            history.append(sample("other", T0, 99));
            history.append(sample("orders", T0, 3));
            history.append(sample("orders", T0.plusSeconds(60), 4));
            history.append(sample("orders", T0.plusSeconds(120), 5));

            Page first = history.readPage("orders", T0, T0.plusSeconds(120), null, 2);
            Page second = history.readPage("orders", T0, T0.plusSeconds(120), first.lastKey().orElseThrow(), 2);
            Page finished = history.readPage("orders", T0, T0.plusSeconds(120), second.lastKey().orElseThrow(), 2);

            assertThat(first.hasMore()).isTrue();
            assertThat(first.entries()).extracting(entry -> entry.sample().counters().get("records.out"))
                    .containsExactly(1L, 2L);
            assertThat(first.entries()).extracting(Entry::key).doesNotHaveDuplicates();
            assertThat(second.hasMore()).isFalse();
            assertThat(second.entries()).extracting(entry -> entry.sample().counters().get("records.out"))
                    .containsExactly(3L, 4L);
            assertThat(finished.entries()).isEmpty();

            List<Entry> walked = List.of(first, second).stream().flatMap(page -> page.entries().stream()).toList();
            assertThat(walked).extracting(Entry::key).doesNotHaveDuplicates();
            assertThat(walked).extracting(entry -> entry.sample().pipelineId()).containsOnly("orders");
            assertThat(walked).noneMatch(entry -> entry.sample().observedAt().equals(T0.plusSeconds(120)));

            assertThat(history.predecessor("orders", T0)).get()
                    .extracting(entry -> entry.sample().counters().get("records.out")).isEqualTo(0L);
            assertThat(history.successor("orders", T0.plusSeconds(120))).get()
                    .extracting(entry -> entry.sample().counters().get("records.out")).isEqualTo(5L);
        }
    }

    private static RateSample sample(String pipelineId, Instant at, long records) {
        return new RateSample(pipelineId, at, Map.of("records.out", records), Map.of(), T0.minusSeconds(3600));
    }
}
