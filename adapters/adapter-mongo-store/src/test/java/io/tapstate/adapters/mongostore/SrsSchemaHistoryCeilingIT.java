package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.tapstate.adapters.mongostore.StoredBytes.DOCUMENT_CEILING;
import static io.tapstate.adapters.mongostore.StoredBytes.bsonSize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Witnesses where the coordination record's append-only schema history ends up: a chain whose history
 * has filled its document to the ceiling can no longer record a schema change at all.
 *
 * <p>The record carries one entry per DDL the source has ever emitted, and the append under witness
 * now trims that array back to the newest entries its budget holds — so the state this case holds it
 * in is not one a chain arrives at any more: it is the state a chain was left in before that trim,
 * which is the state the trim exists to rescue. The whole record is one MongoDB document, and growing
 * that document is the store's only way to record a schema change, so once it is at the ceiling there
 * is no write left that can land. That is the difference this case exists to hold down: the failure is
 * not slow reads, it is a chain that can no longer record that its source's schema moved.
 *
 * <p><strong>How the record is driven there.</strong> The record is seeded by the store, and every
 * history entry written into it is one the store's own mapping serialised — but the drive lays them
 * down in batches of one {@code $push} carrying many entries, where production pushes one DDL at a
 * time. That is a cost decision, not a change of subject. Each single append rewrites the whole record
 * server-side, so walking there one DDL at a time would rewrite roughly a hundred gigabytes to arrive
 * at the same bytes this reaches in a score of writes; what the record ends up carrying is identical
 * either way, because the array and every entry in it are the store's own. What is under witness is
 * the write that comes *after* the drive — that one goes through the real mutator and nothing else.
 *
 * <p>The ceiling is MongoDB's, not this case's: a document may not exceed 16 MiB, on insert and on the
 * result of an update alike, and the entry count that reaches it is derived from the record's own
 * measured bytes rather than written down, so the case keeps meaning what it says if the entry shape or
 * the mapping ever moves.
 */
@RequiresDocker
class SrsSchemaHistoryCeilingIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    /** The width of the table whose schema each history entry carries. */
    private static final int COLUMNS = 20;

    /** Entries per batched write of the drive; large enough to be few writes, small enough to send. */
    private static final int CHUNK = 2_000;

    private static final String CHAIN = "orders@mysql-1";

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void aChainAtTheDocumentCeilingCanStillRecordItsNextSchema() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> collection =
                    client.getDatabase("tapstate").getCollection("srs_meta");
            MongoSrsMetaStore store = new MongoSrsMetaStore(collection);

            store.create(CHAIN, null);
            int entries = driveHistoryToTheCeiling(collection);

            long atCeiling = storedBytes(collection);
            long mappedBytes = bsonSize(recordDocument(entries));
            long nextEntryBytes = elementBytes(entries);
            assertThat(elementBytes(0))
                    .as("the element arithmetic the drive steers by is the mapping's own: what the first "
                            + "entry adds to the record, counted, is what it adds, measured")
                    .isEqualTo(bsonSize(recordDocument(1)) - bsonSize(recordDocument(0)));
            assertThat(atCeiling)
                    .as("the record under witness is the one the store's mapping describes: %d entries "
                            + "stored in %d bytes", entries, atCeiling)
                    .isEqualTo(mappedBytes);
            assertThat(atCeiling + nextEntryBytes)
                    .as("and the next schema entry -- %d bytes -- no longer fits: %d of the record plus "
                            + "%d of the entry is past the %d-byte ceiling",
                            nextEntryBytes, atCeiling, nextEntryBytes, DOCUMENT_CEILING)
                    .isGreaterThan(DOCUMENT_CEILING);

            SchemaVersion next = new SchemaVersion(entries + 1L, columns(), entries + 1L);
            assertThatCode(() -> store.appendSchemaVersion(CHAIN, next))
                    .as("recording the next schema change, which is the only write that can record one "
                            + "and which has nowhere to go on a record already at the ceiling. Refusing it "
                            + "does not defer the change: the chain simply stops being able to say that its "
                            + "source's schema moved")
                    .doesNotThrowAnyException();

            assertThat(store.read(CHAIN).orElseThrow().schemaHistory())
                    .as("and the change is recorded, not merely not refused")
                    .extracting(SchemaVersion::version)
                    .contains(next.version());
        }
    }

    /**
     * Grows the record's schema history to the largest number of entries that still fits inside the
     * ceiling, and returns that count, so the single append under witness is the one that crosses it.
     *
     * <p>The growth operator is a {@code $push} of the store's own serialised entries — the one
     * production grew this array with before the change under witness; only the count per write
     * differs. Batches are used while a whole batch certainly fits, then entries go in one at a time
     * while the record's own stored bytes still leave room — which is what lands the record exactly
     * where the next write cannot go, instead of at a number this case would have had to guess at.
     *
     * <p>Both numbers the loops steer by are cheap on purpose, because the record on the way to 16 MiB is
     * the most expensive thing in this file to touch. The bytes so far are counted by the endpoint, which
     * answers in a score of bytes rather than carrying the record back to be decoded and encoded again; and
     * what the next entry adds is counted rather than measured by rebuilding the whole history around it.
     * The two are cross-checked against the mapping's own document in the case above.
     */
    private static int driveHistoryToTheCeiling(MongoCollection<Document> collection) {
        int entries = 0;
        long stored = storedBytes(collection);
        while (true) {
            long room = DOCUMENT_CEILING - stored;
            // Sized by the widest key in the batch, so a batch can only undershoot the room it has.
            int batch = (int) Math.min(CHUNK, room / elementBytes(entries + CHUNK));
            if (batch == 0) {
                break;
            }
            pushEntries(collection, entries, batch);
            entries += batch;
            stored = storedBytes(collection);
        }
        while (stored + elementBytes(entries) <= DOCUMENT_CEILING) {
            pushEntries(collection, entries, 1);
            stored += elementBytes(entries);
            entries++;
        }
        return entries;
    }

    /** Lays down {@code count} history entries from the given index on, in one write. */
    private static void pushEntries(MongoCollection<Document> collection, int from, int count) {
        collection.updateOne(new Document("_id", CHAIN), new Document("$push",
                new Document("schemaHistory", new Document("$each", schemaEntries(from, count)))));
    }

    /**
     * What the entry landing at this index adds to the record: the array carries each element's index as
     * its key, so the entry's own bytes, its type byte, and that key with its terminator is the whole of
     * it. Exact rather than a bound, because the case asserts that the next entry does not fit, and a
     * drive that stopped a byte early would leave one that does.
     */
    private static long elementBytes(int index) {
        return entryBytes() + 2 + Integer.toString(index).length();
    }

    /**
     * The stored bytes one history entry has on its own, before the array wraps it — measured, not
     * assumed.
     */
    private static long entryBytes() {
        return bsonSize(schemaEntries(0, 1).get(0));
    }

    /**
     * One batch of history entries as the store serialises them: taken out of the document the store's
     * own mapping builds for a record carrying that history, so the drive writes the bytes a real append
     * writes rather than a copy of them written out again here.
     */
    private static List<Document> schemaEntries(int from, int count) {
        List<SchemaVersion> versions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            versions.add(new SchemaVersion(from + i + 1L, columns(), from + i + 1L));
        }
        return recordDocument(versions).getList("schemaHistory", Document.class);
    }

    /** The record the store's own mapping would write for a chain carrying that many history entries. */
    private static Document recordDocument(int entries) {
        List<SchemaVersion> history = new ArrayList<>();
        for (int i = 0; i < entries; i++) {
            history.add(new SchemaVersion(i + 1L, columns(), i + 1L));
        }
        return recordDocument(history);
    }

    /** The same, for a history that is already built. */
    private static Document recordDocument(List<SchemaVersion> history) {
        return MongoSrsMetaStore.toDocument(
                new SrsMeta(CHAIN, null, List.of(), null, history, null, 0L, 0L, null));
    }

    /**
     * The stored document's size in bytes, as the endpoint counts it against the ceiling — asked of the
     * endpoint rather than worked out here, so a record on its way to 16 MiB is never carried back over the
     * wire to be measured.
     */
    private static long storedBytes(MongoCollection<Document> collection) {
        Document sized = collection.aggregate(List.of(
                new Document("$match", new Document("_id", CHAIN)),
                new Document("$project", new Document("bytes", new Document("$bsonSize", "$$ROOT"))))).first();
        if (sized == null) {
            throw new IllegalStateException("the chain just seeded was not there: " + CHAIN);
        }
        return ((Number) sized.get("bytes")).longValue();
    }

    /**
     * A schema of the size a real table's carries — the field-per-column shape the cost instrument
     * measures, so the count that reaches the ceiling is the one this product's records reach.
     */
    private static Map<String, Object> columns() {
        return StoredBytes.schemaOfWidth(COLUMNS);
    }
}
