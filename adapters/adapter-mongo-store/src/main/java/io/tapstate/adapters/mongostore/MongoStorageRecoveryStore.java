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
    private static final String INITIAL = "INITIAL_PREPARATION";
    static final String AUTH_PREPARING = "AUTH_RECOVERY_PREPARING";
    static final String AUTH_READY = "AUTH_RECOVERY_READY";
    record Authentication(CachePreparation preparation, boolean pending) {}
    private record Stored(Document row, CachePreparation preparation) {}

    private MongoStorageRecoveryStore() {}

    static void requireAbsent(ClientSession session, MongoDatabase database) {
        if (SystemCollections.STORAGE_RECOVERY.on(database).find(session).limit(1).first() != null) throw unreadable();
    }

    static void allocate(ClientSession session, MongoDatabase database, StorageBinding binding, String incarnation) {
        SystemCollections.STORAGE_RECOVERY.on(database).insertOne(session, new Document("_id", "cluster")
                .append("formatVersion", 1).append("layoutId", MongoConfigurationDomain.LAYOUT)
                .append("clusterId", binding.setup().clusterId()).append("bindingVersion", binding.bindingVersion())
                .append("recoveryId", binding.setup().operationId()).append("phase", INITIAL)
                .append("revision", 0L).append("authGeneration", 1L).append("cacheIncarnation", incarnation));
    }

    static CachePreparation load(ClientSession session, MongoDatabase database, StorageBinding binding) {
        return read(session, database, binding).preparation();
    }
    static Authentication authentication(ClientSession session, MongoDatabase database, StorageBinding binding) {
        var value = read(session, database, binding);
        return new Authentication(value.preparation(), AUTH_PREPARING.equals(value.row().get("phase")));
    }
    static Authentication reserve(ClientSession session, MongoDatabase database, StorageBinding binding, CachePreparation expected,
            String operation, String incarnation) {
        var current = read(session, database, binding);
        var held = current.preparation(); var scope = held.authScope();
        if (!scope.clusterId().equals(expected.authScope().clusterId()) || scope.bindingVersion() != expected.authScope().bindingVersion()
                || scope.mode() != expected.authScope().mode()) throw fenced();
        if (AUTH_PREPARING.equals(current.row().get("phase")) || !held.equals(expected))
            return new Authentication(held, AUTH_PREPARING.equals(current.row().get("phase")));
        if (scope.authGeneration() >= Long.MAX_VALUE - 1 || current.row().getLong("revision") >= Long.MAX_VALUE - 2) throw fenced();
        var next = new CachePreparation(operation, new CacheAuthScope(scope.clusterId(), scope.bindingVersion(), scope.mode(),
                incarnation, scope.authGeneration() + 1));
        ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database));
        var values = new Document("recoveryId", operation).append("phase", AUTH_PREPARING)
                .append("revision", current.row().getLong("revision") + 1).append("authGeneration", next.authScope().authGeneration())
                .append("cacheIncarnation", incarnation);
        if (SystemCollections.STORAGE_RECOVERY.on(database).updateOne(session, filter(current.row()), new Document("$set", values)).getMatchedCount() != 1) throw fenced();
        return new Authentication(next, true);
    }
    static CachePreparation publish(ClientSession session, MongoDatabase database, StorageBinding binding, CachePreparation expected) {
        var current = read(session, database, binding);
        if (!current.preparation().equals(expected)) throw fenced();
        if (AUTH_READY.equals(current.row().get("phase"))) return expected;
        if (!AUTH_PREPARING.equals(current.row().get("phase")) || current.row().getLong("revision") >= Long.MAX_VALUE - 1) throw fenced();
        ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database));
        if (SystemCollections.STORAGE_RECOVERY.on(database).updateOne(session, filter(current.row()),
                new Document("$set", new Document("phase", AUTH_READY).append("revision", current.row().getLong("revision") + 1))).getMatchedCount() != 1) throw fenced();
        return expected;
    }
    private static Document filter(Document row) {
        return new Document("_id", "cluster").append("revision", row.get("revision")).append("phase", row.get("phase"))
                .append("recoveryId", row.get("recoveryId")).append("authGeneration", row.get("authGeneration"))
                .append("cacheIncarnation", row.get("cacheIncarnation"));
    }
    private static Stored read(ClientSession session, MongoDatabase database, StorageBinding binding) {
        var rows = SystemCollections.STORAGE_RECOVERY.on(database).find(session).limit(2).into(new java.util.ArrayList<>());
        if (rows.size() != 1) throw unreadable();
        Document row = rows.getFirst();
        if (!"cluster".equals(row.get("_id")) || !Integer.valueOf(1).equals(row.get("formatVersion"))
                || !MongoConfigurationDomain.LAYOUT.equals(row.get("layoutId"))
                || !binding.setup().clusterId().equals(row.get("clusterId"))
                || !Long.valueOf(binding.bindingVersion()).equals(row.get("bindingVersion"))
                || !(row.get("revision") instanceof Long revision) || revision < 0 || revision == Long.MAX_VALUE
                || !(row.get("authGeneration") instanceof Long generation) || generation < 1 || generation == Long.MAX_VALUE
                || !(row.get("recoveryId") instanceof String operation) || operation.isBlank()
                || !(row.get("cacheIncarnation") instanceof String incarnation)) throw unreadable();
        try {
            if (!UUID.fromString(incarnation).toString().equals(incarnation)) throw unreadable();
            if (INITIAL.equals(row.get("phase"))) {
                if (!binding.setup().operationId().equals(operation) || revision != 0 || generation != 1) throw unreadable();
            } else {
                if (!java.util.Set.of(AUTH_PREPARING, AUTH_READY).contains(row.get("phase")) || generation < 2 || revision < 1
                        || !UUID.fromString(operation).toString().equals(operation)) throw unreadable();
                Document setup = SystemCollections.STORAGE_SETUP.on(database).find(session, new Document("_id", "cluster")).first();
                if (setup == null || !"COMPLETE".equals(setup.get("phase"))) throw unreadable();
            }
            var scope = new CacheAuthScope(binding.setup().clusterId(), binding.bindingVersion(), binding.mode(), incarnation, generation);
            var pending = MongoStorageRecoveryCommands.pending(row);
            if (pending.isPresent()) {
                var original = pending.orElseThrow().originalScope();
                if (original.mode() != binding.mode() || original.authGeneration() > generation
                        || original.authGeneration() == generation && !original.equals(scope)
                        || original.authGeneration() < generation && original.cacheIncarnation().equals(incarnation)) throw unreadable();
            }
            return new Stored(row, new CachePreparation(operation, scope));
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) {
            throw unreadable();
        }
    }
    private static TapstateException fenced() { return new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null); }

    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", "storage-recovery", "field", "cache-identity"), null);
    }
}
