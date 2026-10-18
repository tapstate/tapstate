package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Explicit metadata schema DDL, separate from ordinary facet opening and business READY publication. */
final class MongoCacheMetadataStructure {
    private static final List<SystemCollections> TABLES = List.of(SystemCollections.CACHE_SOURCE_SCHEMAS,
            SystemCollections.CACHE_CONNECTOR_CATALOG, SystemCollections.CACHE_CONNECTOR_SPECS,
            SystemCollections.CACHE_CONNECTION_TEST_RESULTS, SystemCollections.CACHE_CONNECTOR_ARTIFACTS,
            SystemCollections.CACHE_DERIVED_SCHEMAS, SystemCollections.CACHE_PIPELINE_RATE_HISTORY);
    private static final SystemCollections.IndexSpec FILE_NAMES = new SystemCollections.IndexSpec(
            List.of("filename", "uploadDate"), false, "filename_1_uploadDate_1");
    private static final SystemCollections.IndexSpec CHUNK_ORDER = new SystemCollections.IndexSpec(
            List.of("files_id", "n"), true, "files_id_1_n_1");
    private final MongoDatabase database;
    private final Duration retention;

    MongoCacheMetadataStructure(MongoDatabase database, Duration retention) {
        this.database = Objects.requireNonNull(database, "database"); this.retention = requireRetention(retention);
    }

    static Duration requireRetention(Duration retention) {
        if (retention == null || retention.isNegative() || retention.isZero() || retention.getNano() != 0)
            throw new TapstateException(StoreError.INVALID_HISTORY_RETENTION, Map.of(), null);
        return retention;
    }

    /**
     * Called only by controlled business preparation in its original LOCAL-read DDL transaction.
     * The caller owns authorization, same-client session, commit and READY publication. Existing complete
     * structures are only verified; a partial set is unknown and is never repaired by this helper.
     */
    void prepareInTransaction(ClientSession session, Runnable requireActive) {
        Objects.requireNonNull(session, "session"); Objects.requireNonNull(requireActive, "requireActive");
        if (!session.hasActiveTransaction()) throw new IllegalStateException("metadata preparation requires its owner's transaction");
        requireActive.run();
        var existing = collections();
        if (!existing.isEmpty()) { verify(); requireActive.run(); return; }
        for (var table : TABLES) {
            requireActive.run();
            if (table == SystemCollections.CACHE_CONNECTOR_ARTIFACTS) {
                table.prepareGridFsCollectionsOn(database, session);
                IndexEnsure.createInTransaction(session, table.indexTargetOn(database), FILE_NAMES);
                IndexEnsure.createInTransaction(session, table.chunksOn(database), CHUNK_ORDER);
            } else table.prepareCollectionOn(database, session);
            for (var index : indexes(table)) IndexEnsure.createInTransaction(session, table.indexTargetOn(database), index);
        }
        requireActive.run();
    }

    /** Load-only validation: neither missing collections nor missing or changed indexes cause DDL. */
    void verify() { verify(TABLES); }

    void verifyFacet(SystemCollections table) {
        if (Objects.requireNonNull(table, "table").database() != SystemCollections.Database.CACHE) throw new IllegalArgumentException("not a CACHE resource");
        verify(List.of(table));
    }

    private void verify(List<SystemCollections> tables) {
        StoreIo.run(() -> {
            var required = names(tables);
            var existing = collections(required);
            if (!existing.keySet().equals(required)) throw unreadable("collections");
            for (var table : tables) {
                for (String name : table.physicalNames()) {
                    Document description = existing.get(name);
                    if (!"collection".equals(description.get("type"))) throw unreadable("collection-type");
                    Document options = description.get("options", Document.class);
                    if (options != null && (Boolean.TRUE.equals(options.get("capped")) || options.containsKey("timeseries"))) throw unreadable("collection-options");
                }
                var rows = table.indexTargetOn(database).listIndexes().into(new ArrayList<>());
                verifyId(rows);
                if (table != SystemCollections.CACHE_PIPELINE_RATE_HISTORY && rows.stream().anyMatch(row -> row.containsKey("expireAfterSeconds"))) throw unreadable("ttl-index");
                for (var index : indexes(table)) verifyIndex(rows, index);
                if (table == SystemCollections.CACHE_PIPELINE_RATE_HISTORY && rows.stream().anyMatch(row -> row.containsKey("expireAfterSeconds")
                        && !indexes(table).getFirst().indexName().equals(row.get("name")))) throw unreadable("ttl-index");
                if (table == SystemCollections.CACHE_CONNECTOR_ARTIFACTS) {
                    verifyIndex(rows, FILE_NAMES);
                    var chunks = table.chunksOn(database).listIndexes().into(new ArrayList<>());
                    verifyId(chunks); verifyIndex(chunks, CHUNK_ORDER);
                    if (chunks.stream().anyMatch(row -> row.containsKey("expireAfterSeconds"))) throw unreadable("ttl-index");
                }
            }
        });
    }

    private Map<String, Document> collections() { return collections(names()); }

    private Map<String, Document> collections(Set<String> required) {
        var result = new java.util.LinkedHashMap<String, Document>();
        for (Document row : database.listCollections().filter(new Document("name", new Document("$in", new ArrayList<>(required))))) {
            String name = row.getString("name");
            if (result.put(name, row) != null) throw unreadable("collections");
        }
        return result;
    }
    private static Set<String> names() { return names(TABLES); }
    private static Set<String> names(List<SystemCollections> tables) {
        var names = new LinkedHashSet<String>(); tables.forEach(table -> names.addAll(table.physicalNames())); return names;
    }
    private List<SystemCollections.IndexSpec> indexes(SystemCollections table) {
        if (table == SystemCollections.CACHE_PIPELINE_RATE_HISTORY) return List.of(
                new SystemCollections.IndexSpec(List.of("observedAt"), false, retention.toSeconds()), table.indexes().get(1));
        return table.indexes();
    }
    private static void verifyId(List<Document> rows) {
        Document id = rows.stream().filter(row -> "_id_".equals(row.get("name"))).findFirst().orElseThrow(() -> unreadable("id-index"));
        if (!new Document("_id", 1).equals(id.get("key")) || Boolean.TRUE.equals(id.get("hidden")) || Boolean.TRUE.equals(id.get("sparse"))
                || id.containsKey("partialFilterExpression") || id.containsKey("expireAfterSeconds")) throw unreadable("id-index");
    }
    private static void verifyIndex(List<Document> rows, SystemCollections.IndexSpec index) {
        Document found = rows.stream().filter(row -> index.indexName().equals(row.get("name"))).findFirst().orElseThrow(() -> unreadable("index"));
        var keys = new Document(); index.keys().forEach(key -> keys.append(key, 1));
        Object ttl = found.get("expireAfterSeconds");
        if (!keys.equals(found.get("key")) || Boolean.TRUE.equals(found.get("unique")) != index.unique()
                || Boolean.TRUE.equals(found.get("hidden")) || Boolean.TRUE.equals(found.get("sparse")) || found.containsKey("partialFilterExpression")) throw unreadable("index");
        if (index.expireAfterSeconds() == null) { if (ttl != null) throw unreadable("index"); }
        else {
            if (!(ttl instanceof Long || ttl instanceof Integer)) throw unreadable("index");
            if (((Number) ttl).longValue() != index.expireAfterSeconds()) throw new TapstateException(StoreError.HISTORY_RETENTION_MISMATCH,
                    Map.of("configuredSeconds", index.expireAfterSeconds(), "storedSeconds", ((Number) ttl).longValue()), null);
        }
    }
    private static TapstateException unreadable(String field) { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "cache-metadata", "field", field), null); }
}
