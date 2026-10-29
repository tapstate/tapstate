package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.CacheBusinessScope;
import org.bson.Document;
import java.util.*;

/** Structure checks shared by explicit preparation and load-only validation access. */
final class MongoValidationSchema {
    static final int VERSION = 2;
    static final int COMPOSED_CONFIGURATION_VERSION = 3;
    static final List<SystemCollections> CONFIG = List.of(SystemCollections.VALIDATION_TASKS,
            SystemCollections.VALIDATION_REPORTS, SystemCollections.VALIDATION_DIFFERENCES);
    static final List<SystemCollections> CACHE = List.of(SystemCollections.VALIDATION_RUNS);
    private MongoValidationSchema() {}

    static Document marker(String operation, String phase, long initialized, CacheBusinessScope scope) {
        var result = new Document("formatVersion", 1).append("operationId", operation).append("phase", phase).append("initializedPipelines", initialized)
                .append("scope", MongoValidationControlStore.scope(scope));
        validateMarker(result); return result;
    }

    static Document validateMarker(Document value) {
        if (value == null || !value.keySet().equals(Set.of("formatVersion", "operationId", "phase", "initializedPipelines", "scope"))
                || !Integer.valueOf(1).equals(value.get("formatVersion")) || !(value.get("operationId") instanceof String operation)
                || operation.isBlank() || operation.length() > 512 || !(value.get("phase") instanceof String phase)
                || !Set.of("PREPARING", "READY").contains(phase) || !(value.get("initializedPipelines") instanceof Long count)
                || count < 0 || count == Long.MAX_VALUE) throw unreadable("marker");
        scope(value); MongoValidationPreparationFacts.bounded(value); return value;
    }

    static CacheBusinessScope scope(Document marker) {
        try {
            var value = marker.get("scope", Document.class);
            if (!(value.get("bindingVersion") instanceof Long version)) throw unreadable("scope");
            var result = new CacheBusinessScope(value.getString("clusterId"), version, value.getString("cacheIncarnation"));
            if (!MongoValidationControlStore.scope(result).equals(value)) throw unreadable("scope"); return result;
        } catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadable("scope"); }
    }

    static Document requireVersionTwo(Document schema, String phase) {
        if (schema == null || !Integer.valueOf(VERSION).equals(schema.get("installedVersion"))) throw unreadable("version");
        if (!(schema.get("validation") instanceof Document value)) throw unreadable("marker");
        var marker = validateMarker(value);
        if (!phase.equals(marker.get("phase"))) throw refused(); return marker;
    }

    /** Version-two components have distinct positive structures; absence never selects a fallback. */
    static boolean configurationHasValidation(MongoDatabase database, Document schema) {
        if (schema == null || !(schema.get("installedVersion") instanceof Integer version)) throw unreadable("version");
        if (version == 1) {
            if (schema.containsKey("validation")) throw unreadable("marker");
            return false;
        }
        if (version == VERSION && !schema.containsKey("validation")) {
            MongoConfigurationAiProviderSettingsStore.verifyStructure(database);
            requireAbsent(database, CONFIG);
            return false;
        }
        configurationMarker(schema);
        if (version == VERSION) requireAbsent(database, List.of(SystemCollections.CONFIG_AI_PROVIDER_SETTINGS));
        else MongoConfigurationAiProviderSettingsStore.verifyStructure(database);
        verify(database, CONFIG);
        return true;
    }

    static Document configurationMarker(Document schema) {
        if (schema == null || !(schema.get("installedVersion") instanceof Integer version)
                || version != VERSION && version != COMPOSED_CONFIGURATION_VERSION) throw unreadable("version");
        if (!(schema.get("validation") instanceof Document marker)) throw unreadable("marker");
        return validateMarker(marker);
    }

    static Document requireConfiguration(Document schema, String phase) {
        var marker = configurationMarker(schema);
        if (!phase.equals(marker.get("phase"))) throw refused();
        return marker;
    }

    static int preparedConfigurationVersion(Document original) {
        if (Integer.valueOf(1).equals(original.get("installedVersion"))) return VERSION;
        if (Integer.valueOf(VERSION).equals(original.get("installedVersion")) && !original.containsKey("validation"))
            return COMPOSED_CONFIGURATION_VERSION;
        throw refused();
    }

    static void requireReady(MongoDatabase database, boolean configuration) {
        var table = configuration ? SystemCollections.SCHEMA_MIGRATIONS : SystemCollections.CACHE_SCHEMA_MIGRATIONS;
        var schema = table.on(database).find(new Document("_id", configuration ? "config" : "business")).first();
        if (configuration && !configurationHasValidation(database, schema)) throw unreadable("version");
        var marker = configuration ? requireConfiguration(schema, "READY") : requireVersionTwo(schema, "READY");
        var identity = (configuration ? SystemCollections.CONFIG_CLUSTER_IDENTITY : SystemCollections.CACHE_IDENTITY)
                .on(database).find(new Document("_id", "cluster")).first();
        var original = scope(marker);
        if (identity == null || !original.clusterId().equals(identity.get("clusterId"))
                || !MongoConfigurationDomain.LAYOUT.equals(identity.get("layoutId"))
                || !configuration && (!Long.valueOf(original.bindingVersion()).equals(identity.get("bindingVersion"))
                || !original.cacheIncarnation().equals(identity.get("cacheIncarnation")))) throw unreadable("identity");
        if (!configuration) verify(database, CACHE);
    }

    static void requireAbsent(MongoDatabase database, List<SystemCollections> tables) {
        var names = new HashSet<>(database.listCollectionNames().into(new ArrayList<>()));
        for (var table : tables) if (names.contains(table.collectionName())) throw refused();
    }

    static void create(ClientSession session, MongoDatabase database, List<SystemCollections> tables) {
        if (!session.hasActiveTransaction()) throw new IllegalStateException("validation schema needs its controlled transaction");
        for (var table : tables) {
            table.prepareCollectionOn(database, session);
            for (var index : table.indexes()) IndexEnsure.createInTransaction(session, table.on(database), index);
        }
    }

    static void verify(MongoDatabase database, List<SystemCollections> tables) {
        for (var table : tables) {
            var description = database.listCollections().filter(new Document("name", table.collectionName())).first();
            if (description == null || !"collection".equals(description.get("type"))) throw unreadable("collection");
            var options = description.get("options", Document.class);
            if (options != null && (Boolean.TRUE.equals(options.get("capped")) || options.containsKey("timeseries"))) throw unreadable("collection-options");
            var indexes = table.on(database).listIndexes().into(new ArrayList<>());
            var expected = new HashMap<String, SystemCollections.IndexSpec>();
            for (var index : table.indexes()) expected.put(index.indexName(), index);
            if (indexes.size() != expected.size() + 1) throw unreadable("indexes");
            for (var index : indexes) {
                var declared = expected.get(index.getString("name"));
                var keys = new Document();
                if ("_id_".equals(index.get("name"))) keys.put("_id", 1);
                else if (declared != null) declared.keys().forEach(key -> keys.put(key, 1));
                else throw unreadable("index");
                if (!keys.equals(index.get("key")) || Boolean.TRUE.equals(index.get("hidden")) || Boolean.TRUE.equals(index.get("sparse"))
                        || index.containsKey("expireAfterSeconds") || Boolean.TRUE.equals(index.get("unique")) != (declared != null && declared.unique())
                        || !Objects.equals(index.get("partialFilterExpression"), declared == null || declared.partialFilterJson() == null
                        ? null : Document.parse(declared.partialFilterJson()))) throw unreadable("index");
            }
        }
    }

    static TapstateException refused() { return new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null); }
    static TapstateException unreadable(String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "validation-schema", "field", field), null);
    }
}
