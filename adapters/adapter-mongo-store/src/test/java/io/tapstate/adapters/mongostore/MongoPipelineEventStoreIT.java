package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.PipelineEventStore;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real Mongo witness for idempotent writes, one-incarnation pages, and age-bound storage. */
@RequiresDocker
class MongoPipelineEventStoreIT {

    @Container
    private static final MongoDBContainer REPLICA_SET =
            new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void aRetryKeepsOneEventButAReusedIdWithDifferentContentFails() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("pipeline_event_identity_it");
            database.drop();
            MongoPipelineEventStore events = store(database);
            Instant at = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            PipelineEvent first = event("ev-1", "orders", "inc-a", 41, at, PipelineEvent.Kind.STATE_CHANGED);

            events.append(first);
            events.append(first);
            assertThat(SystemCollections.PIPELINE_EVENTS.on(database).countDocuments()).isEqualTo(1);
            assertThatThrownBy(() -> events.append(event("ev-1", "orders", "inc-a", 41,
                    at, PipelineEvent.Kind.EXECUTION_RESTARTED)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("conflicting content");
            assertThatThrownBy(() -> events.append(event("ev-1", "orders", "inc-b", 42,
                    at, PipelineEvent.Kind.STATE_CHANGED)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("conflicting content");
            assertThat(SystemCollections.PIPELINE_EVENTS.on(database).countDocuments()).isEqualTo(1);
        }
    }

    @Test
    void ambiguousGapMarkerRetryCanWidenItsKnownLossWithoutMakingASecondMarker() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("pipeline_event_gap_retry_it");
            database.drop();
            MongoPipelineEventStore events = store(database);
            Instant at = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            PipelineEvent first = gap(at, at.minusSeconds(20), at.minusSeconds(10),
                    List.of(PipelineEvent.GapReason.QUEUE_FULL));
            PipelineEvent widened = gap(at, at.minusSeconds(20), at.minusSeconds(1),
                    List.of(PipelineEvent.GapReason.QUEUE_FULL, PipelineEvent.GapReason.WRITE_FAILURE));

            events.append(first);
            events.append(widened);
            events.append(first);

            assertThat(SystemCollections.PIPELINE_EVENTS.on(database).countDocuments()).isEqualTo(1);
            assertThat(events.readPage("orders", "inc-a", at, at.plusSeconds(1), null, 10).events())
                    .containsExactly(widened);
        }
    }

    private static PipelineEvent gap(Instant occurredAt, Instant from, Instant to,
            List<PipelineEvent.GapReason> reasons) {
        PipelineEvent.Gap gap = new PipelineEvent.Gap(from, to, reasons);
        return new PipelineEvent(PipelineEvent.gapId("orders", "inc-a", 41, from),
                "orders", "inc-a", 41, PipelineEvent.Kind.TELEMETRY_GAP,
                occurredAt, null, null, null, null, gap);
    }

    @Test
    void pagesStayWithinOneIncarnationAcrossExecutionsAndOldCleanupKeepsNewEvents() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("pipeline_event_page_it");
            database.drop();
            MongoPipelineEventStore events = store(database);
            Instant at = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            events.append(event("ev-b", "orders", "inc-a", 41, at, PipelineEvent.Kind.STATE_CHANGED));
            events.append(event("ev-a", "orders", "inc-a", 41, at, PipelineEvent.Kind.STATE_CHANGED));
            events.append(event("ev-c", "orders", "inc-a", 42, at.plusSeconds(1),
                    PipelineEvent.Kind.EXECUTION_RESTARTED));
            events.append(event("ev-other", "other", "inc-a", 41, at, PipelineEvent.Kind.STATE_CHANGED));
            events.append(event("ev-new", "orders", "inc-b", 43, at, PipelineEvent.Kind.STATE_CHANGED));
            PipelineEvent.Gap gap = new PipelineEvent.Gap(at.minusSeconds(20), at.minusSeconds(1),
                    List.of(PipelineEvent.GapReason.WRITE_FAILURE, PipelineEvent.GapReason.QUEUE_FULL));
            PipelineEvent marker = new PipelineEvent(PipelineEvent.gapId("orders", "inc-a", 42, gap.from()),
                    "orders", "inc-a", 42, PipelineEvent.Kind.TELEMETRY_GAP,
                    at.plusSeconds(2), null, null, null, null, gap);
            events.append(marker);

            PipelineEventStore.Page first = events.readPage("orders", "inc-a",
                    at, at.plusSeconds(3), null, 2);
            PipelineEventStore.Page second = events.readPage("orders", "inc-a",
                    at, at.plusSeconds(3), first.lastKey().orElseThrow(), 2);

            assertThat(first.hasMore()).isTrue();
            assertThat(first.events()).extracting(PipelineEvent::id).containsExactly("ev-a", "ev-b");
            assertThat(second.hasMore()).isFalse();
            assertThat(second.events()).extracting(PipelineEvent::id)
                    .containsExactly("ev-c", marker.id());
            assertThat(second.events().get(1)).isEqualTo(marker);
            assertThat(events.readPage("orders", "inc-b", at, at.plusSeconds(3), null, 10).events())
                    .extracting(PipelineEvent::id).containsExactly("ev-new");

            events.deleteIncarnation("orders", "inc-a");
            events.deleteIncarnation("orders", "inc-a");
            assertThat(events.readPage("orders", "inc-a", at, at.plusSeconds(3), null, 10).events())
                    .isEmpty();
            assertThat(events.readPage("orders", "inc-b", at, at.plusSeconds(3), null, 10).events())
                    .extracting(PipelineEvent::id).containsExactly("ev-new");
        }
    }

    @Test
    void collectionHasFifteenDayDateExpiryAndScopedRangeIndex() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("pipeline_event_indexes_it");
            database.drop();
            MongoPipelineEventStore events = store(database);

            List<Document> indexes = SystemCollections.PIPELINE_EVENTS.on(database).listIndexes().into(new java.util.ArrayList<>());
            assertThat(events.retention()).isEqualTo(MongoPipelineEventStore.DEFAULT_RETENTION);
            assertThat(indexes).anySatisfy(index -> {
                assertThat(index.getString("name")).isEqualTo("occurredAt_idx");
                assertThat(index.get("expireAfterSeconds", Number.class).longValue())
                        .isEqualTo(MongoPipelineEventStore.DEFAULT_RETENTION.toSeconds());
                assertThat(index.get("key", Document.class)).containsEntry("occurredAt", 1);
            });
            assertThat(indexes).anySatisfy(index -> assertThat(index.getString("name"))
                    .isEqualTo("pipelineId_pipelineIncarnationId_occurredAt__id_idx"));
        }
    }

    @Test
    void fractionalQueryBoundsKeepThePublicHalfOpenIntervalExact() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("pipeline_event_fractional_bounds_it");
            database.drop();
            MongoPipelineEventStore events = store(database);
            Instant at = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            events.append(event("ev-boundary", "orders", "inc-a", 41,
                    at, PipelineEvent.Kind.STATE_CHANGED));

            assertThat(events.readPage("orders", "inc-a", at.plusNanos(1),
                    at.plusSeconds(1), null, 10).events()).isEmpty();
            assertThat(events.readPage("orders", "inc-a", at.minusSeconds(1),
                    at.plusNanos(1), null, 10).events())
                    .extracting(PipelineEvent::id).containsExactly("ev-boundary");
        }
    }

    private static MongoPipelineEventStore store(MongoDatabase database) {
        return new MongoPipelineEventStore(database, SystemCollections.PIPELINE_EVENTS.on(database),
                MongoPipelineEventStore.DEFAULT_RETENTION);
    }

    private static PipelineEvent event(String id, String pipelineId, String incarnationId,
            long generation, Instant at, PipelineEvent.Kind kind) {
        return new PipelineEvent(id, pipelineId, incarnationId, generation, kind, at,
                PipelineState.NEW, PipelineState.RUNNING, null, null, null);
    }
}
