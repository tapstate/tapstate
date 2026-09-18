package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoRateHistoryStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import org.bson.Document;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A reader of the store's own documents, opened alongside the product rather than through it.
 *
 * <p>It exists because the bookkeeping a removal reclaims has no read face. A pipeline's intent, its
 * checkpoint and a mining chain's consumer cursors are internal to the product; nothing over the wire
 * says whether they are still there, and "still there" is exactly what the reclaim rules are about. The
 * one that does surface - the published observation - is asserted over the wire by the cases that can,
 * and the point of reading it here as well is that residue and reclaim must agree on every document, not
 * only the visible one.
 *
 * <p>Reading the store directly is the same choice the file and Mongo endpoint drivers make: a reading
 * taken through the product's own code would agree with the product by construction. The collection
 * names come from the adapter's published constants rather than being spelled again here, so a rename
 * breaks the compile instead of quietly turning every assertion below into "no document, therefore
 * reclaimed".
 */
final class StoreDocuments implements AutoCloseable {

    private final MongoClient client;
    private final MongoDatabase database;

    private StoreDocuments(MongoClient client, MongoDatabase database) {
        this.client = client;
        this.database = database;
    }

    /** Opens a reader on the same database the product was launched against. */
    static StoreDocuments at(String storeUri) {
        ConnectionString connection = new ConnectionString(storeUri);
        String name = connection.getDatabase();
        if (name == null) {
            throw new AssertionError("the store url names no database, so there is nothing to read: " + storeUri);
        }
        MongoClient client = MongoClients.create(connection);
        return new StoreDocuments(client, client.getDatabase(name));
    }

    /** Whether {@code collection} holds a document under {@code id}. */
    boolean holds(String collection, String id) {
        return database.getCollection(collection).find(new Document("_id", id)).first() != null;
    }

    /**
     * How many samples of {@code pipelineId} the rate history holds. A history is one document per sample
     * under the pipeline's id rather than one document per pipeline, so "still there" is a count, and the
     * count is taken off the collection because the history has no read face of its own.
     */
    long rateSamplesOf(String pipelineId) {
        return database.getCollection(MongoStorePort.PIPELINE_RATE_HISTORY)
                .countDocuments(new Document(MongoRateHistoryStore.PIPELINE_ID, pipelineId));
    }

    /** Every {@code _id} in {@code collection} - the reconciliation set a converger would work from. */
    Set<String> ids(String collection) {
        return StreamSupport.stream(
                        database.getCollection(collection).find().projection(new Document("_id", 1)).spliterator(),
                        false)
                .map(document -> document.getString("_id"))
                .collect(Collectors.toSet());
    }

    /**
     * The pipeline ids carrying a cursor on {@code miningChainId}, or none when the chain is gone. A chain
     * that is absent and a chain that is present with no consumers are told apart by {@link #holds}, and
     * the difference matters: the first is the shared record this design forbids removing.
     */
    Set<String> consumersOf(String miningChainId) {
        Document chain = database.getCollection(MongoStorePort.SRS_META)
                .find(new Document("_id", miningChainId))
                .first();
        if (chain == null || !(chain.get("consumerOffsets") instanceof Document consumers)) {
            return Set.of();
        }
        return Set.copyOf(consumers.keySet());
    }

    /** The whole consumer cursor of one pipeline on one chain, for comparing it byte for byte across a removal. */
    Document consumerOffset(String miningChainId, String pipelineId) {
        Document chain = database.getCollection(MongoStorePort.SRS_META)
                .find(new Document("_id", miningChainId))
                .first();
        if (chain == null || !(chain.get("consumerOffsets") instanceof Document consumers)) {
            return null;
        }
        return consumers.get(pipelineId) instanceof Document offset ? offset : null;
    }

    /** Every mining chain id the store holds. */
    Set<String> miningChainIds() {
        return ids(MongoStorePort.SRS_META);
    }

    /** The whole chain record, for reporting what a chain actually held when an assertion about it fails. */
    Document chain(String miningChainId) {
        return database.getCollection(MongoStorePort.SRS_META)
                .find(new Document("_id", miningChainId))
                .first();
    }

    /**
     * The chain's durable read offset - how far the capture may forget, held back to the slowest consumer.
     *
     * <p>Absent until the first change is mined: it is derived from the consumers' acknowledged sink
     * positions, and before any cdc has flowed there are none to take a minimum over. A case reading it has
     * to allow for that rather than treat absent as stalled.
     */
    String sourceReadOffset(String miningChainId) {
        Document chain = chain(miningChainId);
        return chain == null ? null : chain.getString("sourceReadOffset");
    }

    /**
     * How far one consumer has read a table on the chain, or {@code -1} when it holds no such cursor.
     *
     * <p>This is the reading the cdc write gate takes its headroom from: the capture may not run further
     * ahead of the slowest consumer than the ring can hold. So a departed consumer whose cursor was left
     * attached freezes this minimum, the gate closes, and every surviving pipeline stops receiving changes
     * with nothing logged anywhere - which is why a case watches this rather than the pipeline's own state.
     */
    long consumerReadSeq(String miningChainId, String pipelineId, String table) {
        Document offset = consumerOffset(miningChainId, pipelineId);
        if (offset == null || !(offset.get("perTableSeq") instanceof Document perTable)
                || !(perTable.get(table) instanceof Number seq)) {
            return -1L;
        }
        return seq.longValue();
    }

    @Override
    public void close() {
        client.close();
    }
}
