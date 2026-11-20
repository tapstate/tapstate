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
    static final String QUERY_INDEX_VERSION = "queryIndexVersion";
    static final int CURRENT_QUERY_INDEX_VERSION = 1;
    private static final Set<String> QUERY_INDEX_NAMES = Set.of("validation_tasks_pipeline_task",
            "validation_reports_history", "validation_differences_page");
    static final List<SystemCollections> CONFIG = List.of(SystemCollections.VALIDATION_TASKS,
            SystemCollections.VALIDATION_REPORTS, SystemCollections.VALIDATION_DIFFERENCES);
    static final List<SystemCollections> CACHE = List.of(SystemCollections.VALIDATION_RUNS);
    private static final ThreadLocal<IdentityHashMap<MongoDatabase, VerifiedStructure>> STARTUP = new ThreadLocal<>();

    private record VerifiedStructure(boolean configuration, Document marker, Document identity) {}

    private MongoValidationSchema() {}

    /** Reuses physical structure checks only inside one synchronous startup phase. */
    static <T> T duringStartup(java.util.function.Supplier<T> work) {
        if (STARTUP.get() != null) return work.get();
        STARTUP.set(new IdentityHashMap<>());
        try {
            return work.get();
        } finally {
            STARTUP.remove();
        }
    }

    static Document marker(String operation, String phase, long initialized, CacheBusinessScope scope) {
        var result = new Document("formatVersion", 1).append("operationId", operation).append("phase", phase).append("initializedPipelines", initialized)
                .append("scope", MongoValidationControlStore.scope(scope));
        validateMarker(result); return result;
    }

    /** Logical migration marker validation for an already opened configuration domain. */
    static boolean configurationDeclaresValidation(Document schema) {
        if (schema == null || !(schema.get("installedVersion") instanceof Integer version)) throw unreadable("version");
        if (version == 1 || version == VERSION && !schema.containsKey("validation")) {
            if (schema.containsKey("validation")) throw unreadable("marker");
            return false;
        }
        configurationMarker(schema);
        return true;
    }

    static Document configurationMarker(String operation, String phase, long initialized, CacheBusinessScope scope) {
        var result = marker(operation, phase, initialized, scope);
        result.put("formatVersion", 2);
        result.put("scheduledAdmission", scheduledMarker(operation, "READY", null, 0));
        result.put(QUERY_INDEX_VERSION, CURRENT_QUERY_INDEX_VERSION);
        return validateMarker(result);
    }
    static Document recoveredMarker(Document previous, String operation, long initialized, CacheBusinessScope scope) {
        var result = marker(operation, "READY", initialized, scope);
        if (Integer.valueOf(2).equals(previous.get("formatVersion"))) {
            result.put("formatVersion", 2); result.put("scheduledAdmission", previous.get("scheduledAdmission"));
        }
        if (previous.containsKey(QUERY_INDEX_VERSION)) result.put(QUERY_INDEX_VERSION, queryIndexVersion(previous));
        return validateMarker(result);
    }
    static Document scheduledMarker(String operation, String phase, Document cursor, long initialized) {
        return new Document("formatVersion", 1).append("operationId", operation).append("phase", phase).append("cursor", cursor).append("initializedTasks", initialized);
    }
    static boolean scheduledReady(MongoDatabase database, ClientSession session) {
        return scheduledReady(SystemCollections.SCHEMA_MIGRATIONS.on(database).find(session, new Document("_id", "config")).first());
    }
    private static boolean scheduledReady(Document schema) {
        if (schema == null || !schema.containsKey("validation")) return false;
        var marker = configurationMarker(schema);
        if (!Integer.valueOf(2).equals(marker.get("formatVersion"))) return false;
        return "READY".equals(marker.get("scheduledAdmission", Document.class).get("phase"));
    }
    static void requireTaskCoverage(MongoDatabase database, ClientSession session, Document task) {
        requireTaskCoverage(scheduledReady(database, session), task);
    }
    static void requireTaskCoverage(Document schema, Document task) {
        requireTaskCoverage(scheduledReady(schema), task);
    }
    static void requireTaskCoverage(boolean covered, Document task) {
        if (covered != Integer.valueOf(2).equals(task.get("formatVersion"))) throw unreadable("scheduled-task-coverage");
    }
    static void requireMutationReady(MongoDatabase database, ClientSession session) {
        requireMutationReady(SystemCollections.SCHEMA_MIGRATIONS.on(database).find(session, new Document("_id", "config")).first());
    }
    static void requireMutationReady(Document schema) {
        var marker = requireConfiguration(schema, "READY");
        if (Integer.valueOf(2).equals(marker.get("formatVersion")) && !"READY".equals(marker.get("scheduledAdmission", Document.class).get("phase"))) throw refused();
    }

    static Document validateMarker(Document value) {
        boolean covered = value != null && Integer.valueOf(2).equals(value.get("formatVersion"));
        var fields = new HashSet<>(Set.of("formatVersion", "operationId", "phase", "initializedPipelines", "scope"));
        if (covered) fields.add("scheduledAdmission");
        if (value != null && value.containsKey(QUERY_INDEX_VERSION)) fields.add(QUERY_INDEX_VERSION);
        if (value == null || !value.keySet().equals(fields)
                || !covered && !Integer.valueOf(1).equals(value.get("formatVersion")) || !(value.get("operationId") instanceof String operation)
                || operation.isBlank() || operation.length() > 512 || !(value.get("phase") instanceof String phase)
                || !Set.of("PREPARING", "READY").contains(phase) || !(value.get("initializedPipelines") instanceof Long count)
                || count < 0 || count == Long.MAX_VALUE) throw unreadable("marker");
        if (covered) {
            if (!(value.get("scheduledAdmission") instanceof Document watermark)) throw unreadable("scheduled-marker");
            if (watermark == null || !watermark.keySet().equals(Set.of("formatVersion", "operationId", "phase", "cursor", "initializedTasks"))
                    || !Integer.valueOf(1).equals(watermark.get("formatVersion")) || !(watermark.get("operationId") instanceof String upgrade)
                    || upgrade.isBlank() || upgrade.length() > 512 || !(watermark.get("phase") instanceof String scheduledPhase)
                    || !Set.of("PREPARING", "READY").contains(scheduledPhase)
                    || !(watermark.get("initializedTasks") instanceof Long tasks) || tasks < 0 || tasks == Long.MAX_VALUE
                    || watermark.get("cursor") != null && !(watermark.get("cursor") instanceof Document)) throw unreadable("scheduled-marker");
            var cursor = watermark.get("cursor", Document.class);
            if (cursor != null && (!cursor.keySet().equals(Set.of("pipelineId", "taskId")) || !(cursor.get("pipelineId") instanceof String pipeline)
                    || pipeline.isBlank() || pipeline.length() > 512 || !(cursor.get("taskId") instanceof String task) || task.isBlank() || task.length() > 512)) throw unreadable("scheduled-cursor");
            if (MongoValidationTaskCodec.bsonBytes(watermark) > 4096 || MongoValidationTaskCodec.bsonBytes(value) > 8192) throw unreadable("scheduled-marker-bytes");
        }
        queryIndexVersion(value); scope(value); MongoValidationPreparationFacts.bounded(value); return value;
    }

    static int queryIndexVersion(Document marker) {
        if (!marker.containsKey(QUERY_INDEX_VERSION)) return 0;
        if (!(marker.get(QUERY_INDEX_VERSION) instanceof Integer version) || version != CURRENT_QUERY_INDEX_VERSION)
            throw unreadable("query-index-version");
        return version;
    }

    static boolean queryIndex(SystemCollections.IndexSpec index) {
        return QUERY_INDEX_NAMES.contains(index.indexName());
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
        if (!Integer.valueOf(1).equals(marker.get("formatVersion"))) throw unreadable("cache-marker-format");
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
        var marker = configurationMarker(schema);
        if (version == VERSION) requireAbsent(database, List.of(SystemCollections.CONFIG_AI_PROVIDER_SETTINGS));
        else MongoConfigurationAiProviderSettingsStore.verifyStructure(database);
        verify(database, CONFIG, queryIndexVersion(marker) == CURRENT_QUERY_INDEX_VERSION);
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
        var startup = STARTUP.get();
        var previous = startup == null ? null : startup.get(database);
        if (previous != null && (configuration != previous.configuration() || !previous.marker().equals(schema))) {
            startup.remove(database);
            throw refused();
        }
        if (configuration && previous == null && !configurationHasValidation(database, schema)) throw unreadable("version");
        var marker = configuration ? requireConfiguration(schema, "READY") : requireVersionTwo(schema, "READY");
        if (configuration && Integer.valueOf(2).equals(marker.get("formatVersion")) && !"READY".equals(marker.get("scheduledAdmission", Document.class).get("phase"))) throw refused();
        var identity = (configuration ? SystemCollections.CONFIG_CLUSTER_IDENTITY : SystemCollections.CACHE_IDENTITY)
                .on(database).find(new Document("_id", "cluster")).first();
        var original = scope(marker);
        if (identity == null || !original.clusterId().equals(identity.get("clusterId"))
                || !MongoConfigurationDomain.LAYOUT.equals(identity.get("layoutId"))
                || !configuration && (!Long.valueOf(original.bindingVersion()).equals(identity.get("bindingVersion"))
                || !original.cacheIncarnation().equals(identity.get("cacheIncarnation")))) throw unreadable("identity");
        if (startup == null) {
            if (!configuration) verify(database, CACHE);
            return;
        }
        var scopeIdentity = new Document("clusterId", identity.get("clusterId"))
                .append("layoutId", identity.get("layoutId")).append("bindingVersion", identity.get("bindingVersion"))
                .append("cacheIncarnation", identity.get("cacheIncarnation"));
        if (previous != null && !previous.identity().equals(scopeIdentity)) {
            startup.remove(database);
            throw refused();
        }
        if (previous == null) {
            if (!configuration) verify(database, CACHE);
            startup.put(database, new VerifiedStructure(configuration, new Document(schema), scopeIdentity));
        }
    }

    static boolean startupQueryIndexes(MongoDatabase database) {
        var startup = STARTUP.get();
        var proof = startup == null ? null : startup.get(database);
        return proof != null && proof.configuration()
                && queryIndexVersion(configurationMarker(proof.marker())) == CURRENT_QUERY_INDEX_VERSION;
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
        verify(database, tables, false);
    }

    static void verifyQueryIndexes(MongoDatabase database) {
        verify(database, CONFIG, true);
    }

    private static void verify(MongoDatabase database, List<SystemCollections> tables, boolean requireQueryIndexes) {
        for (var table : tables) {
            var description = database.listCollections().filter(new Document("name", table.collectionName())).first();
            if (description == null || !"collection".equals(description.get("type"))) throw unreadable("collection");
            var options = description.get("options", Document.class);
            if (options != null && (Boolean.TRUE.equals(options.get("capped")) || options.containsKey("timeseries"))) throw unreadable("collection-options");
            var indexes = table.on(database).listIndexes().into(new ArrayList<>());
            var expected = new HashMap<String, SystemCollections.IndexSpec>();
            for (var index : table.indexes()) expected.put(index.indexName(), index);
            var required = new HashSet<String>(); required.add("_id_");
            for (var declared : table.indexes()) if (requireQueryIndexes || !queryIndex(declared)) required.add(declared.indexName());
            var actualNames = new HashSet<String>();
            for (var index : indexes) actualNames.add(index.getString("name"));
            if (!actualNames.containsAll(required)) throw unreadable("indexes");
            for (var index : indexes) {
                var declared = expected.get(index.getString("name"));
                var keys = new Document();
                if ("_id_".equals(index.get("name"))) keys.put("_id", 1);
                else if (declared != null) keys.putAll(declared.keyDocument());
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
