package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** MongoDB singleton implementation of the stable cluster-identity port. */
public final class MongoClusterIdentityStore implements ClusterIdentityStore {

    private static final String DOCUMENT_ID = "cluster";
    private final MongoCollection<Document> collection;

    public MongoClusterIdentityStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public Optional<ClusterIdentity> find() {
        Document found = StoreIo.call(() -> collection.find(new Document("_id", DOCUMENT_ID)).first());
        return found == null ? Optional.empty() : Optional.of(read(found));
    }

    @Override
    public ClusterIdentity createIfAbsent(ClusterIdentity proposed) {
        Objects.requireNonNull(proposed, "proposed");
        Document stored = StoreIo.call(() -> collection.findOneAndUpdate(
                new Document("_id", DOCUMENT_ID),
                Updates.setOnInsert("clusterId", proposed.clusterId()),
                new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER)));
        return read(stored);
    }

    private static ClusterIdentity read(Document document) {
        Object id = document == null ? DOCUMENT_ID : document.get("_id");
        if (document == null) {
            throw new TapstateException(
                    IoError.DOCUMENT_UNREADABLE, Map.of("id", String.valueOf(id)), null);
        }
        try {
            return new ClusterIdentity(document.getString("clusterId"));
        } catch (RuntimeException invalid) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", String.valueOf(id)), invalid);
        }
    }
}
