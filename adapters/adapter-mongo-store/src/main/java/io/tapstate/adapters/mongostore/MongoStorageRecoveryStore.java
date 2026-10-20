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

/** The small configuration authority for the cache identities allocated by initial storage binding. */
final class MongoStorageRecoveryStore {
    private static final String INITIAL = "INITIAL_PREPARATION";

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

    static CachePreparation loadInitial(ClientSession session, MongoDatabase database, StorageBinding binding) {
        var rows = SystemCollections.STORAGE_RECOVERY.on(database).find(session).limit(2).into(new java.util.ArrayList<>());
        if (rows.size() != 1) throw unreadable();
        Document row = rows.getFirst();
        var pending = MongoStorageRecoveryCommands.pending(row);
        if (pending.isPresent()) {
            var scope = pending.orElseThrow().originalScope();
            if (scope.mode() != binding.mode() || !scope.cacheIncarnation().equals(row.get("cacheIncarnation"))
                    || !Long.valueOf(scope.authGeneration()).equals(row.get("authGeneration"))) throw unreadable();
        }
        if (!"cluster".equals(row.get("_id")) || !Integer.valueOf(1).equals(row.get("formatVersion"))
                || !MongoConfigurationDomain.LAYOUT.equals(row.get("layoutId"))
                || !binding.setup().clusterId().equals(row.get("clusterId"))
                || !Long.valueOf(binding.bindingVersion()).equals(row.get("bindingVersion"))
                || !binding.setup().operationId().equals(row.get("recoveryId")) || !INITIAL.equals(row.get("phase"))
                || !Long.valueOf(0).equals(row.get("revision")) || !Long.valueOf(1).equals(row.get("authGeneration"))
                || !(row.get("cacheIncarnation") instanceof String incarnation)) throw unreadable();
        try {
            if (!UUID.fromString(incarnation).toString().equals(incarnation)) throw unreadable();
            return new CachePreparation(binding.setup().operationId(), new CacheAuthScope(binding.setup().clusterId(),
                    binding.bindingVersion(), binding.mode(), incarnation, 1));
        } catch (IllegalArgumentException invalid) {
            throw unreadable();
        }
    }

    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", "storage-recovery", "field", "cache-identity"), null);
    }
}
