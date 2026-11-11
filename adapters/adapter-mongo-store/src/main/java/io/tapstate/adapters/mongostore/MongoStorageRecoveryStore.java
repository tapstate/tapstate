package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CachePreparation;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageBinding;
import org.bson.Document;

import java.util.Map;
import java.util.UUID;

/** The bounded configuration authority for original binding and minimum durable authentication recovery. */
final class MongoStorageRecoveryStore {
    private static final String SINGLETON_ID = "cluster";
    private static final String PHASE = "phase";

    private static final String INITIAL = "INITIAL_PREPARATION";
    static final String AUTH_PREPARING = "AUTH_RECOVERY_PREPARING";
    static final String AUTH_READY = "AUTH_RECOVERY_READY";
    record Authentication(CachePreparation preparation, boolean pending) {}
    record Stored(Document row, CachePreparation preparation) {
        Authentication authentication() { return new Authentication(preparation, AUTH_PREPARING.equals(row.get(PHASE))); }
    }

    private MongoStorageRecoveryStore() {}

    static void requireAbsent(ClientSession session, MongoDatabase database) {
        if (SystemCollections.STORAGE_RECOVERY.on(database).find(session).limit(1).first() != null) throw unreadable();
    }

    static void allocate(ClientSession session, MongoDatabase database, StorageBinding binding, String incarnation) {
        SystemCollections.STORAGE_RECOVERY.on(database).insertOne(session, new Document("_id", SINGLETON_ID)
                .append("formatVersion", 1).append("layoutId", MongoConfigurationDomain.LAYOUT)
                .append("clusterId", binding.setup().clusterId()).append("bindingVersion", binding.bindingVersion())
                .append("recoveryId", binding.setup().operationId()).append(PHASE, INITIAL)
                .append("revision", 0L).append("authGeneration", 1L).append("cacheIncarnation", incarnation));
    }

    static CachePreparation load(ClientSession session, MongoDatabase database, StorageBinding binding) {
        return read(session, database, binding).preparation();
    }
    static Authentication authentication(ClientSession session, MongoDatabase database, StorageBinding binding) {
        var value = read(session, database, binding);
        return value.authentication();
    }
    static Authentication reserve(ClientSession session, MongoDatabase database, StorageBinding binding, CachePreparation expected,
            String operation, String incarnation) {
        var current = read(session, database, binding);
        var held = current.preparation(); var scope = held.authScope();
        if (!scope.clusterId().equals(expected.authScope().clusterId()) || scope.bindingVersion() != expected.authScope().bindingVersion()
                || scope.mode() != expected.authScope().mode()) throw fenced();
        if (AUTH_PREPARING.equals(current.row().get(PHASE)) || !held.equals(expected))
            return new Authentication(held, AUTH_PREPARING.equals(current.row().get(PHASE)));
        if (scope.authGeneration() >= Long.MAX_VALUE - 1 || current.row().getLong("revision") >= Long.MAX_VALUE - 2) throw fenced();
        var next = new CachePreparation(operation, new CacheAuthScope(scope.clusterId(), scope.bindingVersion(), scope.mode(),
                incarnation, scope.authGeneration() + 1));
        ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database), database);
        var values = new Document("recoveryId", operation).append(PHASE, AUTH_PREPARING)
                .append("revision", current.row().getLong("revision") + 1).append("authGeneration", next.authScope().authGeneration())
                .append("cacheIncarnation", incarnation);
        if (SystemCollections.STORAGE_RECOVERY.on(database).updateOne(session, filter(current.row()), new Document("$set", values)).getMatchedCount() != 1) throw fenced();
        return new Authentication(next, true);
    }
    static CachePreparation publish(ClientSession session, MongoDatabase database, StorageBinding binding, CachePreparation expected) {
        var current = read(session, database, binding);
        if (!current.preparation().equals(expected)) throw fenced();
        if (AUTH_READY.equals(current.row().get(PHASE))) return expected;
        if (!AUTH_PREPARING.equals(current.row().get(PHASE)) || current.row().getLong("revision") >= Long.MAX_VALUE - 1) throw fenced();
        ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database), database);
        if (SystemCollections.STORAGE_RECOVERY.on(database).updateOne(session, filter(current.row()),
                new Document("$set", new Document(PHASE, AUTH_READY).append("revision", current.row().getLong("revision") + 1))).getMatchedCount() != 1) throw fenced();
        return expected;
    }
    private static Document filter(Document row) {
        return new Document("_id", SINGLETON_ID).append("revision", row.get("revision")).append(PHASE, row.get(PHASE))
                .append("recoveryId", row.get("recoveryId")).append("authGeneration", row.get("authGeneration"))
                .append("cacheIncarnation", row.get("cacheIncarnation"));
    }
    private static Stored read(ClientSession session, MongoDatabase database, StorageBinding binding) {
        return read(session, database, binding, () -> SystemCollections.STORAGE_SETUP.on(database)
                .find(session, new Document("_id", SINGLETON_ID)).first());
    }
    static Stored read(ClientSession session, MongoDatabase database, StorageBinding binding, Document setup) {
        return read(session, database, binding, () -> setup);
    }
    private static Stored read(ClientSession session, MongoDatabase database, StorageBinding binding,
            java.util.function.Supplier<Document> setup) {
        var rows = SystemCollections.STORAGE_RECOVERY.on(database).find(session).limit(2).into(new java.util.ArrayList<>());
        if (rows.size() != 1) throw unreadable();
        Document row = rows.getFirst();
        if (!SINGLETON_ID.equals(row.get("_id")) || !Integer.valueOf(1).equals(row.get("formatVersion"))
                || !MongoConfigurationDomain.LAYOUT.equals(row.get("layoutId"))
                || !binding.setup().clusterId().equals(row.get("clusterId"))
                || !Long.valueOf(binding.bindingVersion()).equals(row.get("bindingVersion"))
                || !(row.get("revision") instanceof Long revision) || revision < 0 || revision == Long.MAX_VALUE
                || !(row.get("authGeneration") instanceof Long generation) || generation < 1 || generation == Long.MAX_VALUE
                || !(row.get("recoveryId") instanceof String operation) || operation.isBlank()
                || !(row.get("cacheIncarnation") instanceof String incarnation)) throw unreadable();
        try {
            if (!UUID.fromString(incarnation).toString().equals(incarnation)) throw unreadable();
            requireRecoveryPhase(row, binding, setup, operation, revision, generation);
            var scope = new CacheAuthScope(binding.setup().clusterId(), binding.bindingVersion(), binding.mode(), incarnation, generation);
            requirePendingScope(row, scope);
            return new Stored(row, new CachePreparation(operation, scope));
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) {
            throw unreadable();
        }
    }

    private static void requireRecoveryPhase(Document row, StorageBinding binding,
            java.util.function.Supplier<Document> setup, String operation, long revision, long generation) {
        if (INITIAL.equals(row.get(PHASE))) {
            if (!binding.setup().operationId().equals(operation) || revision != 0 || generation != 1) throw unreadable();
        } else {
            if (!java.util.Set.of(AUTH_PREPARING, AUTH_READY).contains(row.get(PHASE)) || generation < 2 || revision < 1
                    || !UUID.fromString(operation).toString().equals(operation)) throw unreadable();
            Document stage = setup.get();
            if (stage == null || !("COMPLETE".equals(stage.get(PHASE))
                    || binding.mode() == StorageBinding.Mode.CLOUD && "STORAGE_BOUND".equals(stage.get(PHASE)))) throw unreadable();
        }
    }

    private static void requirePendingScope(Document row, CacheAuthScope scope) {
        var pending = MongoStorageRecoveryCommands.pending(row);
        if (pending.isPresent()) {
            var original = pending.orElseThrow().originalScope();
            if (original.mode() != scope.mode() || original.authGeneration() > scope.authGeneration()
                    || original.authGeneration() == scope.authGeneration() && !original.equals(scope)
                    || original.authGeneration() < scope.authGeneration()
                        && original.cacheIncarnation().equals(scope.cacheIncarnation())) throw unreadable();
        }
    }
    private static TapstateException fenced() { return new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null); }

    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", "storage-recovery", "field", "cache-identity"), null);
    }
}
