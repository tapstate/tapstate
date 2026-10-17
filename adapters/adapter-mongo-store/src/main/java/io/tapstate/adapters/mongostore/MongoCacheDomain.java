package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageBinding;
import org.bson.Document;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** One cache client owner, with authentication preparation independent from business-domain assembly. */
public final class MongoCacheDomain implements AutoCloseable {
    private final MongoConnection connection;

    private MongoCacheDomain(MongoConnection connection) { this.connection = connection; }

    /** Connectivity alone creates no identity, schema, session, membership or business resource. */
    public static MongoCacheDomain connect(StorageBinding.Connection cache, Duration timeout, String processId) {
        MongoConnection connection = MongoConnection.forCache(cache, timeout, processId);
        try {
            connection.verifyConnectivity();
            return new MongoCacheDomain(connection);
        } catch (RuntimeException | Error failure) {
            connection.close(); throw failure;
        }
    }

    /** Ordinary load verifies only the original caller-supplied authentication identity and schema. */
    public CacheAuthScope loadAuthScope(CacheAuthScope expected) {
        var facts = facts(expected);
        facts.transaction(session -> { facts.requireReady(session); return null; });
        verifyAuthCollections(expected.mode());
        return expected;
    }

    /** The scope was durably allocated by the original configuration setup or recovery operation. */
    public CacheAuthScope prepareAuth(CacheAuthScope scope, String operationId, Runnable requireActive) {
        Objects.requireNonNull(requireActive, "requireActive");
        requireActive.run();
        boolean emptyTarget = StoreIo.call(() -> {
            try (var names = connection.database().listCollectionNames().iterator()) { return !names.hasNext(); }
        });
        var facts = facts(scope);
        facts.beginPreparation(operationId, emptyTarget, requireActive);
        facts.prepareStructure(operationId, requireActive, session -> {
            for (var table : authTables(scope.mode())) {
                requireActive.run();
                table.prepareCollectionOn(connection.database(), session);
                for (var index : table.indexes()) IndexEnsure.createInTransaction(session, table.on(connection.database()), index);
            }
        });
        verifyAuthCollections(scope.mode());
        return loadAuthScope(scope);
    }

    /** An authorized reset publishes one next auth generation on the same cache business incarnation. */
    public CacheAuthScope advanceAuthGeneration(CacheAuthScope original, long nextGeneration, String operationId, Runnable requireActive) {
        Objects.requireNonNull(requireActive, "requireActive");
        requireActive.run();
        verifyAuthCollections(original.mode());
        var next = facts(original).advanceAuth(nextGeneration, operationId, requireActive);
        return loadAuthScope(next);
    }

    private MongoCacheIdentity facts(CacheAuthScope scope) {
        return new MongoCacheIdentity(connection.client(), connection.database(), Objects.requireNonNull(scope, "scope"));
    }

    private void verifyAuthCollections(StorageBinding.Mode mode) {
        StoreIo.run(() -> {
            var tables = new ArrayList<>(authTables(mode));
            tables.add(SystemCollections.CACHE_IDENTITY); tables.add(SystemCollections.CACHE_SCHEMA_MIGRATIONS);
            Set<String> required = tables.stream().map(SystemCollections::collectionName).collect(java.util.stream.Collectors.toSet());
            var actual = connection.database().listCollections().filter(new Document("name", new Document("$in", new ArrayList<>(required)))).into(new ArrayList<>());
            if (actual.size() != required.size() || actual.stream().anyMatch(row -> !"collection".equals(row.get("type")))) throw unreadableIndex();
            for (var table : authTables(mode)) {
                var existing = table.on(connection.database()).listIndexes().into(new ArrayList<>());
                if (existing.stream().anyMatch(index -> index.containsKey("expireAfterSeconds"))) throw unreadableIndex();
                for (var declared : table.indexes()) {
                    var found = existing.stream().filter(index -> declared.indexName().equals(index.get("name"))).findFirst().orElse(null);
                    Document keys = new Document(); declared.keys().forEach(key -> keys.put(key, 1));
                    if (found == null || !keys.equals(found.get("key")) || Boolean.TRUE.equals(found.get("unique"))
                            || Boolean.TRUE.equals(found.get("sparse")) || Boolean.TRUE.equals(found.get("hidden"))
                            || found.containsKey("partialFilterExpression")) throw unreadableIndex();
                }
            }
        });
    }

    private static List<SystemCollections> authTables(StorageBinding.Mode mode) {
        return mode == StorageBinding.Mode.OP ? List.of(SystemCollections.OP_SESSIONS)
                : List.of(SystemCollections.CLOUD_SESSIONS, SystemCollections.CLOUD_REVOCATIONS);
    }
    private static TapstateException unreadableIndex() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "cache-auth", "field", "index"), null);
    }
    @Override public void close() { connection.close(); }
}
