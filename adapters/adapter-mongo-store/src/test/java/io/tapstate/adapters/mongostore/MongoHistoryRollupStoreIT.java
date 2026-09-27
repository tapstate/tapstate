package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.migration.MigrationRunner;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.HistoryRollupStore;
import io.tapstate.spi.store.HistoryRollupStore.Bucket;
import io.tapstate.spi.store.HistoryRollupStore.Key;
import io.tapstate.spi.store.HistoryRollupStore.Resolution;
import io.tapstate.spi.store.HistoryRollupStore.Scope;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    void fifteenDayScheduleKeepsFiveRollupLevelsWithinTwentySixPercentOfRawDocuments() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("rollup_fifteen_day_capacity_it");
            MongoCollection<Document> raw = SystemCollections.PIPELINE_RATE_HISTORY.on(database);
            MongoCollection<Document> collection = SystemCollections.PIPELINE_HISTORY_ROLLUPS.on(database);
            Duration retention = Duration.ofDays(15);
            new MongoRateHistoryStore(database, raw, retention);
            HistoryRollupStore store = new MongoHistoryRollupStore(database, collection, retention);

            // Start fourteen days ago so the TTL monitor cannot remove the first fixture documents.
            long sixHours = Duration.ofHours(6).toSeconds();
            Instant first = Instant.ofEpochSecond(Math.floorDiv(
                    Instant.now().minus(Duration.ofDays(14)).getEpochSecond(), sixHours) * sixHours);
            Instant end = first.plus(retention);
            List<Document> batch = new ArrayList<>(1_000);
            long rawStarted = System.nanoTime();
            long minute = 0;
            for (Instant at = first; at.isBefore(end); at = at.plus(Duration.ofMinutes(1)), minute++) {
                RateSample sample = new RateSample("capacity", at,
                        Map.of("records.out", minute * 60, "bytes.out", minute * 600),
                        Map.of("orders", minute % 11), first);
                batch.add(MongoRateHistoryStore.toDocument(sample).append("_id", new ObjectId()));
                if (batch.size() == 1_000) {
                    raw.insertMany(batch);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                raw.insertMany(batch);
                batch.clear();
            }
            long rawInsertNanos = System.nanoTime() - rawStarted;

            Map<Resolution, Integer> expected = Map.of(
                    Resolution.PT5M, 4_320,
                    Resolution.PT30M, 720,
                    Resolution.PT1H, 360,
                    Resolution.PT3H, 120,
                    Resolution.PT6H, 60);
            Instant computedAt = end;
            long rollupStarted = System.nanoTime();
            for (Resolution resolution : Resolution.values()) {
                for (Instant at = first; at.isBefore(end); at = at.plus(resolution.duration())) {
                    Bucket bucket = representativeBucket(resolution, at, computedAt);
                    batch.add(MongoHistoryRollupStore.toDocument(bucket));
                    if (batch.size() == 1_000) {
                        collection.insertMany(batch);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    collection.insertMany(batch);
                    batch.clear();
                }
                assertThat(collection.countDocuments(new Document("resolution", resolution.name())))
                        .as("one physical document for each closed %s bucket", resolution)
                        .isEqualTo(expected.get(resolution).longValue());
            }
            long rollupInsertNanos = System.nanoTime() - rollupStarted;

            long rawDocuments = raw.countDocuments();
            long rollupDocuments = collection.countDocuments();
            assertThat(rawDocuments).isEqualTo(21_600);
            assertThat(rollupDocuments).isEqualTo(5_580)
                    .isLessThanOrEqualTo(rawDocuments * 26 / 100);
            for (Resolution resolution : Resolution.values()) {
                store.upsert(representativeBucket(resolution, first, computedAt));
            }
            assertThat(collection.countDocuments()).as("refresh replaces each bucket in place")
                    .isEqualTo(rollupDocuments);

            Document rawStats = database.runCommand(new Document("collStats", raw.getNamespace().getCollectionName()));
            Document rollupStats = database.runCommand(
                    new Document("collStats", collection.getNamespace().getCollectionName()));
            System.out.printf("history-rollup-capacity fixture=one-fragment rawDocs=%d"
                            + " rollupDocs=%d ratioPercent=%.3f"
                            + " rawLogicalBytes=%d rollupLogicalBytes=%d"
                            + " rawBulkSeedMs=%.3f rollupBulkSeedMs=%.3f%n",
                    rawDocuments, rollupDocuments, rollupDocuments * 100d / rawDocuments,
                    number(rawStats, "size"), number(rollupStats, "size"),
                    rawInsertNanos / 1_000_000d, rollupInsertNanos / 1_000_000d);
        }
    }

    private static Bucket representativeBucket(Resolution resolution, Instant at, Instant computedAt) {
        Instant end = at.plus(resolution.duration());
        long minutes = resolution.duration().toMinutes();
        HistoryRollupStore.Rate records = new HistoryRollupStore.Rate(
                BigDecimal.valueOf(minutes * 60), BigDecimal.ONE, BigDecimal.ONE);
        HistoryRollupStore.Rate bytes = new HistoryRollupStore.Rate(
                BigDecimal.valueOf(minutes * 600), BigDecimal.TEN, BigDecimal.TEN);
        HistoryRollupStore.Fragment fragment = new HistoryRollupStore.Fragment(0,
                HistoryRollupStore.StartReason.CONTINUATION, at, end, records, bytes,
                List.of(new HistoryRollupStore.Lag("orders", end.minusSeconds(60), 7, 10)),
                new HistoryRollupStore.CounterStats(records.delta(), resolution.duration().toNanos(),
                        records.maxRate()),
                new HistoryRollupStore.CounterStats(bytes.delta(), resolution.duration().toNanos(),
                        bytes.maxRate()),
                new RateHistoryStore.Key(end.minusSeconds(60), new ObjectId().toHexString()), end);
        return new Bucket(new Key("capacity", Scope.incarnation("capacity-owner"), resolution, at),
                computedAt, computedAt, computedAt.plus(HistoryRollupStore.MAX_CACHE_AGE),
                false, List.of(fragment), List.of(), Math.toIntExact(minutes));
    }

    private static long number(Document source, String name) {
        return source.get(name, Number.class).longValue();
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
