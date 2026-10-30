package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.bson.Document;

/** One fixed feature-version fact in the existing CONFIG schema aggregate, independent of other features. */
final class MongoAlertStorageSchema {
    static final int VERSION = 7;
    static final int DOMAIN_VERSION = 6;
    static final int NODE_VERSION = 5;
    static final int OPERATIONAL_VERSION = 4;
    private static final int BASE_QUERY_COUNT = 5;
    static final int QUERY_VERSION = 3;
    static final int AUTHORITY_VERSION = 2;
    static final int LEGACY_VERSION = 1;
    static final String FIELD = "alerting";
    private static final List<SystemCollections> BASE_COLLECTIONS = Stream.of(MongoAlertConfigurationStore.COLLECTIONS,
            MongoAlertStateStore.COLLECTIONS, MongoAlertNotificationStore.COLLECTIONS).flatMap(List::stream).toList();
    static final List<SystemCollections> COLLECTIONS = Stream.concat(BASE_COLLECTIONS.stream(),Stream.of(SystemCollections.ALERT_NODE_LIFECYCLES)).toList();
    private static final Set<String> LEGACY_FIELDS = Set.of("installedVersion", "clusterId", "layoutId", "keyId", "operationId");
    private static final Set<String> MARKER_FIELDS = Set.of("installedVersion", "clusterId", "layoutId", "keyId", "operationId",
            "authorityPreparationOperationId");
    private static final Set<String> QUERY_FIELDS = Set.of("installedVersion", "clusterId", "layoutId", "keyId", "operationId",
            "authorityPreparationOperationId", "queryPreparation");
    private static final String QUERY = "queryPreparation";
    private enum Structures { LEGACY, PREPARING, READY }
    record QueryIndex(SystemCollections collection, SystemCollections.IndexSpec index) { }
    static final List<QueryIndex> QUERY_INDEXES = List.of(query(SystemCollections.ALERT_INSTANCES, "alert_query_active"),
            query(SystemCollections.ALERT_EPISODES, "alert_query_ended"), query(SystemCollections.ALERT_NOTIFICATION_SCHEDULES, "alert_query_schedule_due"),
            query(SystemCollections.ALERT_DELIVERY_INTENTS, "alert_query_delivery_due"), query(SystemCollections.ALERT_DELIVERY_INTENTS, "alert_query_test_window"),query(SystemCollections.ALERT_NODE_LIFECYCLES,"alert_query_node_lifecycle"));
    private final MongoClient client;
    private final MongoDatabase database;
    private final String clusterId;
    private final String keyId;
    private final Runnable requireActive;

    MongoAlertStorageSchema(MongoClient client, MongoDatabase database, String clusterId,
            String keyId, Runnable requireActive) {
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId"); this.keyId = Objects.requireNonNull(keyId, "keyId");
        this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
    }

    /** Load-only: absent means unprepared only when the entire feature set is absent as well. */
    int load(MongoReadBudget budget) {
        Objects.requireNonNull(budget, "budget");
        return StoreIo.call(() -> {
            requireActive.run();
            Document marker = verifiedMarker(budget);
            requireActive.run(); budget.remainingMillis(); return marker == null ? 0 : marker.getInteger("installedVersion");
        });
    }

    java.util.Optional<io.tapstate.spi.store.AlertQueryPreparation> progress(MongoReadBudget budget) {
        return StoreIo.call(() -> {
            requireActive.run(); Document marker = verifiedMarker(budget);
            if (marker == null || !marker.containsKey(QUERY)) { return java.util.Optional.empty(); }
            Document query = marker.get(QUERY, Document.class);
            var result = new io.tapstate.spi.store.AlertQueryPreparation(marker.getInteger("installedVersion"), query.getString("operationId"),
                    "READY".equals(query.getString("phase")) ? io.tapstate.spi.store.AlertQueryPreparation.Phase.READY
                            : io.tapstate.spi.store.AlertQueryPreparation.Phase.PREPARING,
                    query.getInteger("nextIndex"), query.getInteger("indexContractVersion"));
            requireActive.run(); budget.remainingMillis(); return java.util.Optional.of(result);
        });
    }

    /** Controlled maintenance only; ordinary opening never advances these component capabilities. */
    int prepare(String operationId,MongoReadBudget budget) {
        return StoreIo.call(()->prepareAll(operationId,budget));
    }
    private int prepareAll(String operationId,MongoReadBudget budget) {
        prepareOperational(operationId,budget);
        prepareNodes(budget);
        budget.transaction(client,session->{
            Document original=marker(session,budget);
            if(original==null)throw unreadable("missing-node-preparation");
            int version=original.getInteger("installedVersion");
            if(version>=DOMAIN_VERSION)return null;
            if(version!=NODE_VERSION)throw unreadable("operation-domain-preparation");
            replaceMarker(session,original,new Document(original).append("installedVersion",DOMAIN_VERSION));
            return null;
        });
        budget.transaction(client,session->{
            Document original=marker(session,budget);
            if(original==null)throw unreadable("missing-rule-binding-preparation");
            int version=original.getInteger("installedVersion");
            if(version==VERSION)return null;
            if(version!=DOMAIN_VERSION)throw unreadable("rule-binding-preparation");
            replaceMarker(session,original,new Document(original).append("installedVersion",VERSION));
            return null;
        });
        return load(budget);
    }

    /** Explicit preparation; an already installed feature is verified without rewriting its original operation. */
    private int prepareOperational(String operationId, MongoReadBudget budget) {
        requireOperation(operationId);
        Objects.requireNonNull(budget, "budget");
        return StoreIo.call(() -> {
            requireActive.run();
            Document existing = verifiedMarker(budget);
            if (existing != null && existing.getInteger("installedVersion")>=OPERATIONAL_VERSION) { return existing.getInteger("installedVersion"); }
            try (var session = client.startSession()) {
                session.withTransaction(() -> {
                    requireActive.run();
                    Document original = marker(session, budget);
                    if (original != null && original.getInteger("installedVersion")>=OPERATIONAL_VERSION) { return null; }
                    if (original != null && Integer.valueOf(QUERY_VERSION).equals(original.get("installedVersion"))) {
                        Document upgraded = new Document(original).append("installedVersion", OPERATIONAL_VERSION);
                        if (SystemCollections.SCHEMA_MIGRATIONS.on(database).updateOne(session,
                                new Document("_id","config").append(FIELD,original),
                                new Document("$set",new Document(FIELD,upgraded))).getModifiedCount()!=1) throw unreadable("operational-preparation-fence");
                        return null;
                    }
                    if (original != null && original.containsKey(QUERY)) { requireQueryOperation(original, operationId); return null; }
                    var schemas = SystemCollections.SCHEMA_MIGRATIONS.on(database);
                    var filter = new Document("_id", "config").append(FIELD,
                            original == null ? new Document("$exists", false) : original);
                    Document installed;
                    if (original == null) { installed = initialMarker(clusterId, keyId, operationId); }
                    else {
                        installed = new Document(original).append("installedVersion", AUTHORITY_VERSION)
                                .append(QUERY, queryProgress(operationId, "PREPARING", 0));
                        if (Integer.valueOf(LEGACY_VERSION).equals(original.get("installedVersion"))) {
                            installed.append("authorityPreparationOperationId", operationId);
                        }
                    }
                    if (MongoAlertConfigurationCodec.size(installed) > 2048) { throw unreadable("marker-size"); }
                    if (original == null || Integer.valueOf(LEGACY_VERSION).equals(original.get("installedVersion"))) {
                        ConfigurationVersion.prepareAlertFence(session, SystemCollections.CONFIGURATION_VERSION.on(database));
                    }
                    if (schemas.updateOne(session, filter, new Document("$set",
                            new Document(FIELD, installed))).getModifiedCount() != 1) {
                        throw unreadable("preparation-fence");
                    }
                    if (original == null) { prepareCollections(session, database); }
                    requireActive.run(); budget.remainingMillis(); return null;
                }, TransactionOptions.builder().readConcern(ReadConcern.LOCAL)
                        .writeConcern(WriteConcern.MAJORITY.withJournal(true))
                        .timeout(budget.remainingMillis(), TimeUnit.MILLISECONDS).build());
            }
            prepareQueries(operationId, budget);
            int installed = load(budget); requireActive.run(); budget.remainingMillis(); return installed;
        });
    }

    private void prepareNodes(MongoReadBudget budget) {
        Document retained=verifiedMarker(budget);
        if(retained==null)throw unreadable("missing-query-preparation");
        if(retained.getInteger("installedVersion")>=NODE_VERSION)return;
        budget.transaction(client,session->{
            Document original=marker(session,budget);
            if(original==null || original.getInteger("installedVersion")<OPERATIONAL_VERSION)throw unreadable("original-query-capability");
            if(original.getInteger("installedVersion")>=NODE_VERSION)return null;
            Document query=original.get(QUERY,Document.class);
            if(query.getInteger("indexContractVersion")==2)return null;
            if(query.getInteger("indexContractVersion")!=1 || !"READY".equals(query.getString("phase"))
                    || query.getInteger("nextIndex")!=BASE_QUERY_COUNT)throw unreadable("original-query-preparation");
            Document next=new Document(query).append("phase","PREPARING").append("indexContractVersion",2);
            replaceMarker(session,original,new Document(original).append(QUERY,next));return null;
        });
        for(int attempt=0;attempt<3;attempt++) {
            Document current=verifiedMarker(budget);
            if(current.getInteger("installedVersion")>=NODE_VERSION)return;
            Document progress=current.get(QUERY,Document.class);int cursor=progress.getInteger("nextIndex");
            if(cursor==BASE_QUERY_COUNT) {
                MongoAlertNodeLifecycleStore.prepareNamespace(budget.database(database).withWriteConcern(WriteConcern.MAJORITY.withJournal(true)));
            } else if(cursor==QUERY_INDEXES.size())verify(budget,Structures.READY,2);
            else throw unreadable("node-query-cursor");
            budget.transaction(client,session->{
                Document original=marker(session,budget);
                if(original.getInteger("installedVersion")>=NODE_VERSION)return null;
                Document query=original.get(QUERY,Document.class);
                if(!query.getString("operationId").equals(progress.getString("operationId")))throw unreadable("node-query-operation");
                if(query.getInteger("nextIndex")>cursor)return null;
                if(query.getInteger("nextIndex")!=cursor)throw unreadable("node-query-cursor");
                Document next=new Document(query).append("nextIndex",QUERY_INDEXES.size());
                Document updated=new Document(original).append(QUERY,next);
                if(cursor==QUERY_INDEXES.size()){next.put("phase","READY");updated.put("installedVersion",NODE_VERSION);}
                replaceMarker(session,original,updated);return null;
            });
        }
        throw unreadable("node-query-progress");
    }
    private void replaceMarker(ClientSession session,Document original,Document updated) {
        if(MongoAlertConfigurationCodec.size(updated)>2048)throw unreadable("marker-size");
        if(SystemCollections.SCHEMA_MIGRATIONS.on(database).updateOne(session,new Document("_id","config").append(FIELD,original),
                new Document("$set",new Document(FIELD,updated))).getModifiedCount()!=1)throw unreadable("component-preparation-fence");
    }

    static Document initialMarker(String clusterId, String keyId, String operationId) {
        requireOperation(operationId);
        Document result = new Document("installedVersion", OPERATIONAL_VERSION).append("clusterId", clusterId)
                .append("layoutId", MongoConfigurationDomain.LAYOUT).append("keyId", keyId).append("operationId", operationId)
                .append("authorityPreparationOperationId", operationId).append(QUERY, queryProgress(operationId, "READY", BASE_QUERY_COUNT));
        if (MongoAlertConfigurationCodec.size(result) > 2048) { throw unreadable("marker-size"); }
        return result;
    }

    private static QueryIndex query(SystemCollections collection, String name) {
        return new QueryIndex(collection, collection.indexes().stream().filter(index -> index.indexName().equals(name)).findFirst().orElseThrow());
    }
    private static Document queryProgress(String operation, String phase, int next) {
        return new Document("operationId", operation).append("phase", phase).append("nextIndex", next).append("indexContractVersion", 1);
    }
    private static void requireQueryOperation(Document marker, String expected) {
        if (!expected.equals(marker.get(QUERY, Document.class).getString("operationId"))) { throw unreadable("query-operation"); }
    }

    private void prepareQueries(String operation, MongoReadBudget budget) {
        for (int step = 0; step <= BASE_QUERY_COUNT; step++) {
            Document current = budget.transaction(client, session -> marker(session, budget));
            if (current == null) { throw unreadable("missing-query-operation"); }
            if (current.getInteger("installedVersion")>=OPERATIONAL_VERSION) { return; }
            requireQueryOperation(current, operation);
            int cursor = current.get(QUERY, Document.class).getInteger("nextIndex");
            verify(budget, Structures.PREPARING,1);
            if (cursor == BASE_QUERY_COUNT) {
                verify(budget, Structures.READY,1);
                advanceQuery(operation, cursor, true, budget); return;
            }
            QueryIndex next = QUERY_INDEXES.get(cursor);
            MongoDatabase ddl = budget.database(database).withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
            requireActive.run();
            if (MongoAlertStateStore.COLLECTIONS.contains(next.collection())) {
                MongoAlertStateStore.prepareQueryIndex(ddl, next.collection(), next.index());
            } else { MongoAlertNotificationStore.prepareQueryIndex(ddl, next.collection(), next.index()); }
            requireActive.run(); budget.remainingMillis();
            advanceQuery(operation, cursor, false, budget);
        }
        throw unreadable("query-progress");
    }

    private void advanceQuery(String operation, int cursor, boolean ready, MongoReadBudget budget) {
        budget.transaction(client, session -> {
            Document original = marker(session, budget);
            if (original == null) { throw unreadable("missing-query-operation"); }
            if (original.getInteger("installedVersion")>=OPERATIONAL_VERSION) { return null; }
            requireQueryOperation(original, operation);
            int current = original.get(QUERY, Document.class).getInteger("nextIndex");
            if (current > cursor) { return null; }
            if (current != cursor || ready && cursor != BASE_QUERY_COUNT) { throw unreadable("query-progress"); }
            Document updated = new Document(original).append(QUERY, queryProgress(operation, ready ? "READY" : "PREPARING", ready ? cursor : cursor + 1));
            if (ready) { updated.put("installedVersion", OPERATIONAL_VERSION); }
            if (MongoAlertConfigurationCodec.size(updated) > 2048) { throw unreadable("marker-size"); }
            if (SystemCollections.SCHEMA_MIGRATIONS.on(database).updateOne(session, new Document("_id", "config").append(FIELD, original),
                    new Document("$set", new Document(FIELD, updated))).getModifiedCount() != 1) { throw unreadable("query-progress-fence"); }
            return null;
        });
    }

    private Document verifiedMarker(MongoReadBudget budget) {
        Document original = budget.transaction(client, session -> marker(session, budget));
        try { verifyMarkerStructures(original, budget); return original; }
        catch (TapstateException changed) {
            Document refreshed = budget.transaction(client, session -> marker(session, budget));
            if (Objects.equals(original, refreshed)) { throw changed; }
            verifyMarkerStructures(refreshed, budget); return refreshed;
        }
    }
    private void verifyMarkerStructures(Document marker, MongoReadBudget budget) {
        if (marker == null) { if (!structures(budget).isEmpty()) { throw unreadable("unmarked-collections"); } return; }
        int contract=marker.containsKey(QUERY)?marker.get(QUERY,Document.class).getInteger("indexContractVersion"):1;
        Structures mode=marker.containsKey(QUERY) && "PREPARING".equals(marker.get(QUERY,Document.class).getString("phase"))
                ? Structures.PREPARING : marker.getInteger("installedVersion")>=QUERY_VERSION ? Structures.READY : Structures.LEGACY;
        verify(budget,mode,contract);
    }

    static void prepareCollections(ClientSession session, MongoDatabase database) {
        MongoAlertConfigurationStore.prepare(session, database);
        MongoAlertStateStore.prepare(session, database);
        MongoAlertNotificationStore.prepare(session, database);
    }

    private Document marker(ClientSession session, MongoReadBudget budget) {
        requireActive.run();
        // The transaction owns CSOT; overriding a database or collection timeout inside it is invalid.
        budget.remainingMillis();
        MongoDatabase reads = database;
        Document identity = SystemCollections.CONFIG_CLUSTER_IDENTITY.on(reads)
                .find(session, new Document("_id", "cluster")).first();
        if (identity == null || !clusterId.equals(identity.get("clusterId"))
                || !MongoConfigurationDomain.LAYOUT.equals(identity.get("layoutId"))) { throw unreadable("identity"); }
        Document keys = SystemCollections.CLUSTER_KEYS.on(reads).find(session, new Document("_id", "cluster"))
                .projection(new Document("keyId", 1)).first();
        if (keys == null || !keyId.equals(keys.get("keyId"))) { throw unreadable("key-binding"); }
        Document schema = SystemCollections.SCHEMA_MIGRATIONS.on(reads).find(session, new Document("_id", "config")).first();
        if (schema == null || !(schema.get("installedVersion") instanceof Integer installed)
                || installed < 1 || installed > MongoConfigurationDomain.SCHEMA_VERSION) { throw unreadable("config-version"); }
        Document counter = SystemCollections.CONFIGURATION_VERSION.on(reads)
                .find(session, new Document("_id", ConfigurationVersion.ID)).first();
        return verifyAuthorityState(schema, counter, clusterId, keyId);
    }

    /** Load-only validation also runs on ordinary CONFIG reopen; it never reenrolls missing facts. */
    static Document verifyAuthorityState(Document schema, Document counter, String clusterId, String keyId) {
        if (!schema.containsKey(FIELD)) { ConfigurationVersion.validateAlertState(counter, false); return null; }
        if (!(schema.get(FIELD) instanceof Document value)
                || !(value.get("installedVersion") instanceof Integer installed)
                || (installed < LEGACY_VERSION || installed > VERSION)
                || !value.keySet().equals(installed == LEGACY_VERSION ? LEGACY_FIELDS : value.containsKey(QUERY) ? QUERY_FIELDS : MARKER_FIELDS)
                || !clusterId.equals(value.get("clusterId")) || !keyId.equals(value.get("keyId"))
                || !MongoConfigurationDomain.LAYOUT.equals(value.get("layoutId"))
                || !(value.get("operationId") instanceof String operation)) { throw unreadable("feature-version"); }
        requireStoredOperation(operation);
        if (installed >= AUTHORITY_VERSION) {
            if (!(value.get("authorityPreparationOperationId") instanceof String authority)) { throw unreadable("authority-operation"); }
            requireStoredOperation(authority);
        }
        if(value.containsKey(QUERY)) {
            if(!(value.get(QUERY) instanceof Document query) || !query.keySet().equals(Set.of("operationId","phase","nextIndex","indexContractVersion"))
                    || !(query.get("operationId") instanceof String id) || !(query.get("nextIndex") instanceof Integer next)
                    || !(query.get("indexContractVersion") instanceof Integer contract))throw unreadable("query-preparation");
            boolean supported=contract==1 && next>=0 && next<=BASE_QUERY_COUNT
                    && (installed==AUTHORITY_VERSION && "PREPARING".equals(query.get("phase"))
                    || (installed==QUERY_VERSION || installed==OPERATIONAL_VERSION) && "READY".equals(query.get("phase")) && next==BASE_QUERY_COUNT)
                    || contract==2 && next>=BASE_QUERY_COUNT && next<=QUERY_INDEXES.size()
                    && (installed==OPERATIONAL_VERSION && "PREPARING".equals(query.get("phase"))
                    || (installed==NODE_VERSION || installed==DOMAIN_VERSION || installed==VERSION) && "READY".equals(query.get("phase")) && next==QUERY_INDEXES.size());
            if(!supported)throw unreadable("query-preparation");requireStoredOperation(id);
        }else if(installed>=QUERY_VERSION)throw unreadable("query-preparation");
        ConfigurationVersion.validateAlertState(counter, installed >= AUTHORITY_VERSION);
        if (MongoAlertConfigurationCodec.size(value) > 2048) { throw unreadable("marker-size"); }
        return value;
    }

    private Map<String, Document> structures(MongoReadBudget budget) {
        List<String> names = COLLECTIONS.stream().map(SystemCollections::collectionName).toList();
        var found = new HashMap<String, Document>();
        try (var cursor = budget.database(database).listCollections().filter(new Document("name", new Document("$in", names)))
                .batchSize(COLLECTIONS.size()).iterator()) {
            while (cursor.hasNext()) {
                budget.remainingMillis(); Document row = cursor.next();
                if (!(row.get("name") instanceof String name) || !names.contains(name) || found.put(name, row) != null) {
                    throw unreadable("collection-metadata");
                }
            }
        }
        return found;
    }

    private void verify(MongoReadBudget budget, Structures mode,int contract) {
        Map<String, Document> found = structures(budget);
        if(contract==1 && found.containsKey(SystemCollections.ALERT_NODE_LIFECYCLES.collectionName()))throw unreadable("unmarked-node-namespace");
        for (SystemCollections collection : contract==1?BASE_COLLECTIONS:COLLECTIONS) {
            Structures structureMode=contract==2 && collection!=SystemCollections.ALERT_NODE_LIFECYCLES?Structures.READY:mode;
            Document row = found.get(collection.collectionName());
            if(row==null && contract==2 && mode==Structures.PREPARING && collection==SystemCollections.ALERT_NODE_LIFECYCLES)continue;
            if (row == null || !"collection".equals(row.get("type"))
                    || !(row.get("options") instanceof Document options) || !options.isEmpty()) {
                throw unreadable("collection-structure");
            }
            MongoCollection<Document> handle = collection.on(budget.database(database));
            var indexes = new HashMap<String, Document>();
            try (var cursor = handle.listIndexes().iterator()) {
                while (cursor.hasNext()) {
                    budget.remainingMillis(); Document index = cursor.next();
                    if (!(index.get("name") instanceof String name) || indexes.put(name, index) != null
                            || index.containsKey("expireAfterSeconds") || index.containsKey("prepareUnique")) {
                        throw unreadable("index-structure");
                    }
                }
            }
            var expectedNames = new HashSet<String>(); var legacyNames = new HashSet<String>();
            expectedNames.add("_id_"); legacyNames.add("_id_");
            collection.indexes().forEach(index -> {
                expectedNames.add(index.indexName()); if (!index.indexName().startsWith("alert_query_")) { legacyNames.add(index.indexName()); }
            });
            if (structureMode == Structures.READY ? !indexes.keySet().equals(expectedNames)
                    : structureMode == Structures.LEGACY ? !indexes.keySet().equals(legacyNames)
                    : !indexes.keySet().containsAll(legacyNames) || !expectedNames.containsAll(indexes.keySet())) { throw unreadable("index-structure"); }
            if (!(indexes.get("_id_").get("key") instanceof Document primary)
                    || !primary.keySet().equals(Set.of("_id")) || !(primary.get("_id") instanceof Number order)
                    || order.doubleValue() != 1) { throw unreadable("index-structure"); }
            for (SystemCollections.IndexSpec expected : collection.indexes()) {
                Document index = indexes.get(expected.indexName());
                if (structureMode != Structures.READY && expected.indexName().startsWith("alert_query_") && index == null) { continue; }
                if (index == null || !(index.get("key") instanceof Document keys)
                        || !new ArrayList<>(keys.keySet()).equals(expected.keys())
                        || keys.values().stream().anyMatch(value -> !(value instanceof Number n) || n.doubleValue() != 1)
                        || index.get("unique") != null && !(index.get("unique") instanceof Boolean)
                        || Boolean.TRUE.equals(index.get("unique")) != expected.unique()
                        || index.containsKey("partialFilterExpression") || index.containsKey("collation")
                        || Boolean.TRUE.equals(index.get("sparse"))
                        || structureMode == Structures.READY && (index.containsKey("buildUUID") || Boolean.FALSE.equals(index.get("ready")))) { throw unreadable("index-structure"); }
            }
        }
    }

    private static void requireOperation(String operation) {
        if (!validOperation(operation)) { throw new IllegalArgumentException("schema operation identity must be bounded non-control text"); }
    }
    private static void requireStoredOperation(String operation) {
        if (!validOperation(operation)) { throw unreadable("operation"); }
    }
    private static boolean validOperation(String value) {
        if (value == null || value.isBlank() || value.length() > 256
                || value.getBytes(StandardCharsets.UTF_8).length > 256 || value.codePoints().anyMatch(Character::isISOControl)) { return false; }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (Character.isHighSurrogate(character)) {
                if (index + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(index + 1))) { return false; }
                index++;
            } else if (Character.isLowSurrogate(character)) { return false; }
        }
        return true;
    }
    private static TapstateException unreadable(String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "alert-schema", "field", field), null);
    }
}
