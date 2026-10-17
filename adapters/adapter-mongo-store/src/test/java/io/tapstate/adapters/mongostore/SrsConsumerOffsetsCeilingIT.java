package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.tapstate.adapters.mongostore.StoredBytes.DOCUMENT_CEILING;
import static io.tapstate.adapters.mongostore.StoredBytes.bsonSize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Witnesses the other unbounded part of one chain's coordination record: every consumer pipeline keeps
 * a per-table cursor and snapshot-completion set in the same document, for as long as that pipeline is
 * attached. Enough pipelines sharing a wide chain therefore leave no room for even the chain's own source
 * position, and MongoDB refuses that progress write at its 16 MiB document ceiling.
 *
 * <p>The drive inserts the store's own mapped document in one operation instead of issuing hundreds of
 * successively larger consumer updates. That changes only the cost of reaching the state: every consumer
 * sub-document and both table collections are produced by the mapping used by the real mutators, and the
 * case derives the last fitting consumer and table counts from their encoded bytes. The write under
 * witness goes through the real source-offset mutator.
 */
@RequiresDocker
class SrsConsumerOffsetsCeilingIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final String CHAIN = "orders@mysql-1";
    private static final int TABLES_PER_CONSUMER = 1_000;
    private static final Instant WRITTEN_AT = Instant.parse("2026-09-20T08:00:00Z");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void consumerOffsetsCannotFillTheRecordUntilTheChainCanNoLongerRecordSourceProgress() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> roots =
                    client.getDatabase("tapstate").getCollection("srs_meta");
            MongoCollection<Document> consumerOffsets =
                    client.getDatabase("tapstate").getCollection("srs_consumer_offsets");
            roots.drop();
            consumerOffsets.drop();
            MongoSrsMetaStore store = new MongoSrsMetaStore(client,
                    roots, consumerOffsets, Clock.fixed(WRITTEN_AT, ZoneOffset.UTC));
            List<ConsumerOffset> consumers = consumersFillingTheRecord();
            Document record = recordDocument(consumers, null);
            roots.insertOne(record);

            ChainPosition next = new ChainPosition(new SourceOrder(1L, 1L), "gtid:aaa-1:1");
            long stored = storedBytes(roots);
            long withProgress = bsonSize(recordDocument(consumers, next));
            assertThat(stored)
                    .as("the endpoint stores the exact consumer-only shape the mapping measured")
                    .isEqualTo(bsonSize(record));
            assertThat(stored)
                    .as("the consumer offsets fit before the progress write")
                    .isLessThanOrEqualTo(DOCUMENT_CEILING);
            assertThat(withProgress)
                    .as("adding the chain's source position needs %d bytes, past the %d-byte ceiling",
                            withProgress - stored, DOCUMENT_CEILING)
                    .isGreaterThan(DOCUMENT_CEILING);

            assertThatCode(() -> store.advanceSourceReadOffset(CHAIN, next))
                    .as("a chain filled only by retained consumer cursors must still be able to record "
                            + "that it read its source")
                    .doesNotThrowAnyException();
            assertThat(store.read(CHAIN).orElseThrow().sourceRead()).isEqualTo(next);
            Document migratedRoot = roots.find(new Document("_id", CHAIN)).first();
            assertThat(migratedRoot).isNotNull();
            assertThat(migratedRoot.get("consumerOffsets", Document.class)).isEmpty();
            assertThat(consumerOffsets.countDocuments(new Document("miningChainId", CHAIN)))
                    .isEqualTo(consumers.size());
        }
    }

    /**
     * Builds the largest mapped consumer set that fits in one document. Full-width consumers establish
     * the scale, then one final consumer uses as many table pairs as the remaining bytes hold, leaving
     * less than one pair between the record and the endpoint ceiling.
     */
    private static List<ConsumerOffset> consumersFillingTheRecord() {
        List<ConsumerOffset> candidates = new ArrayList<>();
        int lower = 0;
        int upper = 1;
        while (true) {
            addFullConsumersThrough(candidates, upper);
            if (storedBytes(candidates.subList(0, upper)) > DOCUMENT_CEILING) {
                break;
            }
            lower = upper;
            upper *= 2;
        }
        while (lower + 1 < upper) {
            int middle = lower + (upper - lower) / 2;
            if (storedBytes(candidates.subList(0, middle)) <= DOCUMENT_CEILING) {
                lower = middle;
            } else {
                upper = middle;
            }
        }

        List<ConsumerOffset> fitting = new ArrayList<>(candidates.subList(0, lower));
        int lastTables = largestFittingPartialConsumer(fitting, lower);
        if (lastTables >= 0) {
            fitting.add(consumer(lower, lastTables));
        }
        return List.copyOf(fitting);
    }

    /** Adds full-width consumers until the list contains {@code count} candidates. */
    private static void addFullConsumersThrough(List<ConsumerOffset> consumers, int count) {
        while (consumers.size() < count) {
            consumers.add(consumer(consumers.size(), TABLES_PER_CONSUMER));
        }
    }

    /**
     * Finds how many table pairs a final consumer can add. Minus one means even its empty wrapper does
     * not fit; {@value #TABLES_PER_CONSUMER} is already known not to fit from the full-consumer search.
     */
    private static int largestFittingPartialConsumer(List<ConsumerOffset> fullConsumers, int pipeline) {
        int lower = -1;
        int upper = TABLES_PER_CONSUMER;
        while (lower + 1 < upper) {
            int middle = lower + (upper - lower) / 2;
            List<ConsumerOffset> candidate = new ArrayList<>(fullConsumers);
            candidate.add(consumer(pipeline, middle));
            if (storedBytes(candidate) <= DOCUMENT_CEILING) {
                lower = middle;
            } else {
                upper = middle;
            }
        }
        return lower;
    }

    /** One consumer in the shape the two production mutators leave: a cursor and a completion mark. */
    private static ConsumerOffset consumer(int pipeline, int tables) {
        Map<String, Long> perTableSeq = new LinkedHashMap<>();
        List<String> snapshotCompletedTables = new ArrayList<>();
        for (int table = 0; table < tables; table++) {
            String name = "table_" + table;
            perTableSeq.put(name, (long) table);
            snapshotCompletedTables.add(name);
        }
        return new ConsumerOffset(
                "pipeline-" + pipeline, perTableSeq, null, snapshotCompletedTables);
    }

    /** The stored record produced by the store's mapping, optionally carrying its next source position. */
    private static Document recordDocument(List<ConsumerOffset> consumers, ChainPosition sourceRead) {
        return MongoSrsMetaStore.toDocument(new SrsMeta(
                CHAIN,
                sourceRead,
                consumers,
                List.of(),
                null,
                0L,
                sourceRead == null ? null : WRITTEN_AT));
    }

    /** The mapped record's BSON size, without a database round trip during the ceiling search. */
    private static long storedBytes(List<ConsumerOffset> consumers) {
        return bsonSize(recordDocument(consumers, null));
    }

    /** The endpoint's own count of the record bytes it accepted. */
    private static long storedBytes(MongoCollection<Document> collection) {
        Document sized = collection.aggregate(List.of(
                new Document("$match", new Document("_id", CHAIN)),
                new Document("$project", new Document("bytes", new Document("$bsonSize", "$$ROOT"))))).first();
        if (sized == null) {
            throw new IllegalStateException("the consumer-filled chain was not stored: " + CHAIN);
        }
        return ((Number) sized.get("bytes")).longValue();
    }
}
