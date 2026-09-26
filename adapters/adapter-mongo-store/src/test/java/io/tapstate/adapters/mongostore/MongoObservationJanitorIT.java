package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises bounded cold scans and exact conditional deletion against a real MongoDB server. */
@RequiresDocker
class MongoObservationJanitorIT {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    private static final Instant FIRST = Instant.parse("2026-09-27T10:00:00Z");

    @Test
    void scanUsesBoundedIdKeysetAndReturnsOnlyOwnerAndObservedTime() {
        List<BsonDocument> finds = new CopyOnWriteArrayList<>();
        CommandListener listener = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                if (event.getCommandName().equals("find")) {
                    finds.add(event.getCommand().clone());
                }
            }
        };
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                .addCommandListener(listener).build();
        try (MongoClient client = MongoClients.create(settings)) {
            MongoDatabase database = client.getDatabase("observation_janitor_scan_it");
            database.drop();
            MongoCollection<Document> collection = database.getCollection(MongoStorePort.PIPELINE_OBSERVATION);
            MongoObservationStore store = new MongoObservationStore(collection);
            ObservationStore.Scope owner = new ObservationStore.Scope("inc-a", 17);
            store.save(observation("a", null));
            store.save(observation("b", FIRST));
            store.saveScoped(observation("c", FIRST.plusSeconds(1)), owner);
            store.saveScoped(observation("d", FIRST.plusSeconds(2)), owner);
            store.saveScoped(observation("e", FIRST.plusSeconds(3)), owner);

            List<ObservationStore.LatestSnapshot> first = store.scanLatestAfter(Optional.empty(), 2);
            List<ObservationStore.LatestSnapshot> second = store.scanLatestAfter(Optional.of("b"), 2);
            List<ObservationStore.LatestSnapshot> third = store.scanLatestAfter(Optional.of("d"), 2);
            assertThat(ids(first)).containsExactly("a", "b");
            assertThat(ids(second)).containsExactly("c", "d");
            assertThat(ids(third)).containsExactly("e");
            assertThat(first.get(0).scope()).isEmpty();
            assertThat(first.get(0).observedAt()).isEmpty();
            assertThat(first.get(1).observedAt()).contains(FIRST);
            assertThat(second.get(0).scope()).contains(owner);
            assertThat(second.get(0).observedAt()).contains(FIRST.plusSeconds(1));

            assertThat(finds).hasSize(3);
            for (BsonDocument find : finds) {
                assertThat(find.getInt32("limit").getValue()).isEqualTo(2);
                assertThat(find.getDocument("sort").getInt32("_id").getValue()).isEqualTo(1);
                BsonDocument projection = find.getDocument("projection");
                assertThat(projection.keySet()).containsExactlyInAnyOrder(
                        "_id", "pipelineIncarnationId", "executionGeneration", "observedAt");
            }
            assertThat(finds.get(0).getDocument("filter")).isEmpty();
            assertThat(finds.get(1).getDocument("filter").getDocument("_id")
                    .getString("$gt").getValue()).isEqualTo("b");
        }
    }

    @Test
    void scannedScopedDocumentCannotDeleteARecreatedOrRepublishedObservation() {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("observation_janitor_scoped_it");
            database.drop();
            MongoObservationStore store = new MongoObservationStore(
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION));
            ObservationStore.Scope old = new ObservationStore.Scope("inc-old", 41);
            ObservationStore.Scope current = new ObservationStore.Scope("inc-new", 42);
            assertThat(store.saveScoped(observation("flow", FIRST), old)).isTrue();
            ObservationStore.LatestSnapshot scannedOld = only(store);

            assertThat(store.saveScoped(observation("flow", FIRST.plusSeconds(1)), current)).isTrue();
            assertThat(store.deleteIfUnchanged(scannedOld)).isFalse();
            assertThat(store.readStored("flow").orElseThrow().scope()).contains(current);

            ObservationStore.LatestSnapshot scannedCurrent = only(store);
            assertThat(store.saveScoped(observation("flow", FIRST.plusSeconds(2)), current)).isTrue();
            assertThat(store.deleteIfUnchanged(scannedCurrent)).isFalse();
            assertThat(store.readStored("flow").orElseThrow().observation().observedAt())
                    .isEqualTo(FIRST.plusSeconds(2));

            assertThat(store.deleteIfUnchanged(only(store))).isTrue();
            assertThat(store.deleteIfUnchanged(scannedCurrent)).isFalse();
            assertThat(store.readStored("flow")).isEmpty();
        }
    }

    @Test
    void legacyDeletionRequiresBothOwnerFieldsAndTheExactObservedTimeIncludingAbsence() {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("observation_janitor_legacy_it");
            database.drop();
            MongoObservationStore store = new MongoObservationStore(
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION));
            store.save(observation("legacy", FIRST));
            ObservationStore.LatestSnapshot timedLegacy = only(store);
            ObservationStore.Scope newOwner = new ObservationStore.Scope("inc-new", 1);
            assertThat(store.saveScoped(observation("legacy", FIRST.plusSeconds(1)), newOwner)).isTrue();
            assertThat(store.deleteIfUnchanged(timedLegacy)).isFalse();
            assertThat(store.readStored("legacy").orElseThrow().scope()).contains(newOwner);

            store.save(observation("legacy", null));
            ObservationStore.LatestSnapshot missingTime = only(store);
            assertThat(missingTime.observedAt()).isEmpty();
            store.save(observation("legacy", FIRST.plusSeconds(2)));
            assertThat(store.deleteIfUnchanged(missingTime)).isFalse();
            assertThat(store.read("legacy").orElseThrow().observedAt()).isEqualTo(FIRST.plusSeconds(2));

            assertThat(store.deleteIfUnchanged(only(store))).isTrue();
            store.save(observation("legacy", null));
            assertThat(store.deleteIfUnchanged(only(store))).isTrue();
            assertThat(store.read("legacy")).isEmpty();
        }
    }

    @Test
    void scanRejectsUnboundedOrInvalidBatchRequests() {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("observation_janitor_limits_it");
            database.drop();
            MongoObservationStore store = new MongoObservationStore(
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION));
            assertThatThrownBy(() -> store.scanLatestAfter(Optional.empty(), 0))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> store.scanLatestAfter(Optional.empty(), -1))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> store.scanLatestAfter(Optional.empty(),
                    ObservationStore.MAX_LATEST_SCAN_BATCH + 1)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> store.scanLatestAfter(Optional.of(""), 1))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static ObservationStore.LatestSnapshot only(MongoObservationStore store) {
        return store.scanLatestAfter(Optional.empty(), 1).get(0);
    }

    private static List<String> ids(List<ObservationStore.LatestSnapshot> snapshots) {
        List<String> ids = new ArrayList<>();
        for (ObservationStore.LatestSnapshot snapshot : snapshots) {
            ids.add(snapshot.pipelineId());
        }
        return ids;
    }

    private static Observation observation(String id, Instant observedAt) {
        return new Observation(id, PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, observedAt);
    }
}
