package io.tapstate.adapters.mongostore;

import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResultStore;
import org.bson.Document;

import java.util.Objects;
import java.util.Optional;

/** Latest diagnostics in one original CACHE scope; diagnostic presence never grants setup authority. */
final class MongoCacheConnectionTestResultStore implements ConnectionTestResultStore {
    private final MongoCacheMetadataAccess access;
    MongoCacheConnectionTestResultStore(MongoCacheMetadataAccess access) { this.access = access; }
    @Override public void save(ConnectionTestResult result) {
        Document captured = access.stamp(MongoConnectionTestResultStore.toDocument(Objects.requireNonNull(result, "result")));
        access.write(session -> { access.collection.replaceOne(session, new Document("_id", result.connectionId()), captured, new ReplaceOptions().upsert(true)); return null; });
    }
    @Override public Optional<ConnectionTestResult> find(String connectionId) {
        Objects.requireNonNull(connectionId, "connectionId");
        return access.read(session -> {
            Document row = access.collection.find(session, new Document("_id", connectionId)).first();
            if (row == null || !access.inScope(row, connectionId)) return Optional.empty();
            try {
                Document payload = access.payload(row); var result = MongoConnectionTestResultStore.toResult(payload);
                if (!payload.equals(MongoConnectionTestResultStore.toDocument(result))) throw MongoCacheMetadataAccess.unreadable(connectionId, "result");
                return Optional.of(result);
            } catch (TapstateException | NullPointerException | ClassCastException | IllegalArgumentException invalid) {
                throw MongoCacheMetadataAccess.unreadable(connectionId, "result");
            }
        });
    }
}
