package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.FirstAdminStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.spi.store.StorageSetupStateStore;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.bson.Document;

/** Reads and advances only the existing setup authority; no capability secret is stored here. */
final class MongoStorageSetupStateStore implements StorageSetupStateStore {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final MongoStorageBindingStore bindings;
    private final Runnable active;

    MongoStorageSetupStateStore(MongoClient client, MongoDatabase database, MongoStorageBindingStore bindings, Runnable active) {
        this.client = client; this.database = database; this.bindings = bindings; this.active = active;
    }
    @Override public StorageSetupStateStore guardedBy(Runnable guard) {
        Objects.requireNonNull(guard, "guard");
        return new MongoStorageSetupStateStore(client, database, bindings, () -> { active.run(); guard.run(); });
    }
    @Override public Status load() { return transaction(this::load); }

    @Override public void requireCurrent(StorageSetupScope expected, Purpose purpose) {
        Objects.requireNonNull(expected, "expected"); Objects.requireNonNull(purpose, "purpose");
        transaction(session -> { require(session, load(session), expected, purpose); return null; });
    }
    @Override public Status rotate(StorageSetupScope expected, Purpose purpose) {
        Objects.requireNonNull(expected, "expected"); Objects.requireNonNull(purpose, "purpose");
        return transaction(session -> {
            Status original = load(session);
            require(session, original, expected, purpose);
            if (expected.generation() >= Long.MAX_VALUE - 1) throw refused();
            ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database), client);
            Document filter = filter(expected).append("phase", original.phase().name());
            if (SystemCollections.STORAGE_SETUP.on(database).updateOne(session, filter,
                    new Document("$inc", new Document("generation", 1L))).getMatchedCount() != 1) throw refused();
            if (purpose == Purpose.FIRST_ADMIN && SystemCollections.OP_BOOTSTRAP.on(database).updateOne(session,
                    filter(expected).append("phase", FirstAdminStore.Phase.PENDING.name()),
                    new Document("$inc", new Document("generation", 1L))).getMatchedCount() != 1) throw refused();
            return load(session);
        });
    }

    private Status load(ClientSession session) {
        var binding = bindings.loadForTransaction(session);
        Document row = SystemCollections.STORAGE_SETUP.on(database).find(session, new Document("_id", "cluster")).first();
        try {
            var scope = new StorageSetupScope(row.getString("clusterId"), row.getString("installationId"),
                    row.getString("operationId"), row.getLong("generation"));
            Phase phase = Phase.valueOf(row.getString("phase"));
            long version = binding.map(StorageBinding::bindingVersion).orElse(0L);
            return new Status(scope, phase, version);
        } catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "storage-setup", "field", "phase"), null);
        }
    }
    private void require(ClientSession session, Status status, StorageSetupScope expected, Purpose purpose) {
        if (!status.scope().equals(expected)) throw refused();
        if (purpose == Purpose.SETUP) {
            if (status.phase() == Phase.COMPLETE) throw refused();
        } else {
            if (status.phase() != Phase.COMPLETE) throw refused();
            var admin = new MongoFirstAdminStore(client, database, bindings).load(session).orElseThrow(MongoStorageSetupStateStore::refused);
            if (admin.phase() != FirstAdminStore.Phase.PENDING || !admin.scope().equals(expected)) throw refused();
        }
    }
    private static Document filter(StorageSetupScope scope) {
        return new Document("_id", "cluster").append("clusterId", scope.clusterId())
                .append("installationId", scope.installationId()).append("operationId", scope.operationId())
                .append("generation", scope.generation());
    }
    private <T> T transaction(Function<ClientSession,T> body) {
        return StoreIo.call(() -> {
            try (var session = client.startSession()) {
                return session.withTransaction(() -> { active.run(); T result = body.apply(session); active.run(); return result; }, TRANSACTION);
            }
        });
    }
    private static TapstateException refused() { return new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null); }
}
