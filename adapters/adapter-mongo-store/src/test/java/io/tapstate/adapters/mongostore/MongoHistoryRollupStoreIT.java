package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.migration.MigrationRunner;
import io.tapstate.spi.store.HistoryRollupStore;
import io.tapstate.spi.store.HistoryRollupStore.Bucket;
import io.tapstate.spi.store.HistoryRollupStore.Key;
import io.tapstate.spi.store.HistoryRollupStore.Resolution;
import io.tapstate.spi.store.HistoryRollupStore.Scope;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@RequiresDocker
class MongoHistoryRollupStoreIT {

    @Container
    private static final MongoDBContainer REPLICA_SET =
            new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void deterministicUpsertRangeAndCapturedOwnerCleanupUseOnlyTheirOwnBuckets() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("rollup_store_it");
            MongoCollection<Document> collection = SystemCollections.PIPELINE_HISTORY_ROLLUPS.on(database);
            HistoryRollupStore store = new MongoHistoryRollupStore(database, collection, Duration.ofDays(15));
            Bucket old = MongoHistoryRollupStoreTest.bucket(Scope.incarnation("inc-old"));
            Bucket current = MongoHistoryRollupStoreTest.bucket(Scope.incarnation("inc-current"));
            Bucket legacy = MongoHistoryRollupStoreTest.bucket(Scope.legacy());
            Instant nextRead = current.inputReadStartedAt().plus(Duration.ofMinutes(30));
            Bucket next = new Bucket(new Key("flow", current.key().scope(), Resolution.PT30M,
                    current.key().bucketEnd()), nextRead.plusSeconds(1), nextRead,
                    nextRead.plus(Duration.ofMinutes(5)),
                    false, List.of(), List.of());

            store.upsert(old);
            store.upsert(current);
            store.upsert(legacy);
            store.upsert(next);
            store.upsert(current);
            assertThat(collection.countDocuments()).isEqualTo(4);
            assertThat(store.read(current.key())).contains(current);
            assertThat(store.readRange("flow", current.key().scope(), Resolution.PT30M,
                    current.key().bucketStart(), next.key().bucketEnd(), 1)).containsExactly(current);
            assertThat(store.readRange("flow", current.key().scope(), Resolution.PT30M,
                    current.key().bucketStart(), next.key().bucketEnd(), 2)).containsExactly(current, next);
            assertThat(store.readRange("flow", current.key().scope(), Resolution.PT30M,
                    current.key().bucketStart().plusNanos(1), next.key().bucketStart().plusNanos(1), 2))
                    .containsExactly(next);

            store.deleteIncarnation("flow", "inc-old");
            assertThat(store.read(old.key())).isEmpty();
            assertThat(store.read(current.key())).contains(current);
            assertThat(store.read(legacy.key())).contains(legacy);
            store.deleteLegacy("flow");
            assertThat(store.read(legacy.key())).isEmpty();
            assertThat(store.read(current.key())).contains(current);
            assertThat(collection.countDocuments()).isEqualTo(2);

            List<Document> indexes = collection.listIndexes().into(new ArrayList<>());
            assertThat(indexes).anySatisfy(index -> {
                assertThat(index.getString("name")).isEqualTo("bucketStart_idx");
                assertThat(index.get("expireAfterSeconds", Number.class).longValue())
                        .isEqualTo(Duration.ofDays(15).toSeconds());
            });
            assertThat(indexes).extracting(index -> index.getString("name"))
                    .contains("pipelineId_scopeKey_resolution_bucketStart_idx");
        }
    }

    @Test
    void anExistingStoreAtVersionTwelveReceivesTheRollupIndexes() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("rollup_migration_it");
            SystemCollections.SYSTEM_META.on(database).insertOne(
                    new Document("_id", "schema").append("installedVersion", 12));

            MigrationRunner.migrate(database);

            Document schema = SystemCollections.SYSTEM_META.on(database)
                    .find(new Document("_id", "schema")).first();
            assertThat(schema).isNotNull();
            assertThat(schema.getInteger("installedVersion")).isEqualTo(MigrationRunner.SUPPORTED_VERSION);
            List<String> indexes = new ArrayList<>();
            SystemCollections.PIPELINE_HISTORY_ROLLUPS.on(database).listIndexes()
                    .forEach(index -> indexes.add(index.getString("name")));
            assertThat(indexes).contains("bucketStart_idx",
                    "pipelineId_scopeKey_resolution_bucketStart_idx");
        }
    }
}
