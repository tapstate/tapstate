package io.tapstate.adapters.mongostore;

import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Byte-exact spec sources, with their content hash and original CACHE scope verified on full reads. */
final class MongoCacheConnectorSpecStore implements ConnectorSpecStore {
    private final MongoCacheMetadataAccess access;
    MongoCacheConnectorSpecStore(MongoCacheMetadataAccess access) { this.access = access; }
    @Override public void put(String contentHash, byte[] spec) {
        Objects.requireNonNull(contentHash, "contentHash"); byte[] captured = Objects.requireNonNull(spec, "spec").clone();
        if (!contentHash.equals(ContentHash.of(captured))) throw new TapstateException(IoError.DOCUMENT_INVALID, Map.of("id", contentHash, "field", "spec-hash"), null);
        Document row = access.stamp(MongoConnectorSpecStore.toDocument(contentHash, captured));
        access.write(session -> { access.collection.replaceOne(session, new Document("_id", contentHash), row, new ReplaceOptions().upsert(true)); return null; });
    }
    @Override public Optional<byte[]> get(String contentHash) {
        Objects.requireNonNull(contentHash, "contentHash");
        return access.read(session -> {
            Document row = access.collection.find(session, new Document("_id", contentHash)).first();
            if (row == null || !access.inScope(row, contentHash)) return Optional.empty();
            Document payload = access.payload(row); byte[] bytes = MongoConnectorSpecStore.toSpec(payload, contentHash);
            if (!contentHash.equals(ContentHash.of(bytes)) || !payload.equals(MongoConnectorSpecStore.toDocument(contentHash, bytes)))
                throw MongoCacheMetadataAccess.unreadable(contentHash, "spec");
            return Optional.of(bytes.clone());
        });
    }
    @Override public boolean has(String contentHash) {
        Objects.requireNonNull(contentHash, "contentHash");
        return access.read(session -> {
            Document row = access.collection.find(session, new Document("_id", contentHash)).projection(new Document("_id", 1).append("cacheScope", 1)).first();
            return row != null && access.inScope(row, contentHash);
        });
    }
}
