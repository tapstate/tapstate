package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;

/**
 * Where the store keeps what a connector wrote for itself while reading a shared change stream: under the
 * mining chain the pipeline reads through rather than under the pipeline, so the notes outlive whichever
 * pipeline happened to open the stream.
 */
final class ChainNotes {

    private ChainNotes() {
    }

    /** The chain {@code pipeline} holds a cursor on, read from the store's consumer records. */
    static String chainOf(String storeUri, String pipeline) {
        try (MongoClient client = MongoClients.create(storeUri)) {
            Document cursor = client.getDatabase(new ConnectionString(storeUri).getDatabase())
                    .getCollection("srs_consumer_offsets")
                    .find(new Document("_id.pipeline", pipeline))
                    .first();
            assertThat(cursor).as("%s has a cursor on a chain", pipeline).isNotNull();
            return cursor.get("_id", Document.class).getString("chain");
        }
    }

    /** The namespace the notes of the chain {@code pipeline} reads through are kept under. */
    static String namespaceOf(String storeUri, String pipeline) {
        return "pdk.chain." + chainOf(storeUri, pipeline);
    }
}
