package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.ConnectorArtifactHoldings;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

final class MongoCacheConnectorArtifactHoldings implements ConnectorArtifactHoldings {
    private static final SystemCollections TABLE = SystemCollections.CACHE_CONNECTOR_ARTIFACT_HOLDINGS;
    private static final TransactionOptions DDL = TransactionOptions.builder().readConcern(ReadConcern.LOCAL)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoCacheMetadataAccess access;

    MongoCacheConnectorArtifactHoldings(MongoCacheMetadataAccess access) {
        this.access = access;
    }

    /** A new or rebuilt derived facet is prepared without adopting or replacing the existing CACHE. */
    static void prepare(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable active) {
        StoreIo.run(() -> {
            active.run();
            if (exists(database)) return;
            try (var session = client.startSession()) {
                session.withTransaction(() -> {
                    active.run();
                    new MongoCacheBusinessIdentity(database, scope).admitMetadataWrite(session);
                    if (!exists(database)) TABLE.prepareCollectionOn(database, session);
                    active.run();
                    return null;
                }, DDL);
            }
            active.run();
        });
    }

    private static boolean exists(MongoDatabase database) {
        return !MongoCollectionMetadata.matching(database, Set.of(TABLE.collectionName()), false).isEmpty();
    }

    @Override public void publish(String nodeId, Held artifact) {
        Document key = key(nodeId, artifact.sha256());
        Document row = access.stamp(new Document("_id", key)
                .append("connectorId", artifact.connectorId()).append("version", artifact.version()));
        access.write(session -> {
            access.collection.replaceOne(session, new Document("_id", key), row, new ReplaceOptions().upsert(true));
            return null;
        });
    }

    @Override public List<Held> forNode(String nodeId) {
        Objects.requireNonNull(nodeId, "nodeId");
        return access.read(session -> {
            var result = new ArrayList<Held>();
            Document filter = access.stamp(new Document("_id.nodeId", nodeId));
            try (var cursor = access.collection.find(session, filter).iterator()) {
                while (cursor.hasNext()) {
                    Document row = cursor.next();
                    try {
                        String sha256 = row.get("_id", Document.class).getString("sha256");
                        result.add(new Held(sha256, row.getString("connectorId"), row.getString("version")));
                    } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) {
                        throw MongoCacheMetadataAccess.unreadable(nodeId, TABLE.collectionName());
                    }
                }
            }
            return List.copyOf(result);
        });
    }

    @Override public void withdraw(String nodeId, String sha256) {
        Document key = key(nodeId, sha256);
        access.write(session -> {
            access.collection.deleteOne(session, new Document("_id", key));
            return null;
        });
    }

    private Document key(String nodeId, String sha256) {
        return access.stamp(new Document("nodeId", Objects.requireNonNull(nodeId))
                .append("sha256", Objects.requireNonNull(sha256)));
    }
}
