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
    static final int VERSION = 1;
    static final String FIELD = "alerting";
    static final List<SystemCollections> COLLECTIONS = Stream.of(MongoAlertConfigurationStore.COLLECTIONS,
            MongoAlertStateStore.COLLECTIONS, MongoAlertNotificationStore.COLLECTIONS).flatMap(List::stream).toList();
    private static final Set<String> MARKER_FIELDS = Set.of("installedVersion", "clusterId", "layoutId", "keyId", "operationId");
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
            Document marker = budget.transaction(client, session -> marker(session, budget));
            if (marker == null) {
                if (!structures(budget).isEmpty()) { throw unreadable("unmarked-collections"); }
                requireActive.run(); budget.remainingMillis(); return 0;
            }
            verify(budget); requireActive.run(); budget.remainingMillis(); return VERSION;
        });
    }

    /** Explicit preparation; an already installed feature is verified without rewriting its original operation. */
    int prepare(String operationId, MongoReadBudget budget) {
        requireOperation(operationId);
        Objects.requireNonNull(budget, "budget");
        return StoreIo.call(() -> {
            requireActive.run();
            Document existing = budget.transaction(client, session -> marker(session, budget));
            if (existing != null) { verify(budget); requireActive.run(); return VERSION; }
            if (!structures(budget).isEmpty()) { throw unreadable("unmarked-collections"); }
            try (var session = client.startSession()) {
                session.withTransaction(() -> {
                    requireActive.run();
                    if (marker(session, budget) != null) { return null; }
                    var schemas = SystemCollections.SCHEMA_MIGRATIONS.on(database);
                    var filter = new Document("_id", "config").append(FIELD, new Document("$exists", false));
                    if (schemas.updateOne(session, filter, new Document("$set",
                            new Document(FIELD, initialMarker(clusterId, keyId, operationId)))).getMatchedCount() != 1) {
                        throw unreadable("preparation-fence");
                    }
                    prepareCollections(session, database);
                    requireActive.run(); budget.remainingMillis(); return null;
                }, TransactionOptions.builder().readConcern(ReadConcern.LOCAL)
                        .writeConcern(WriteConcern.MAJORITY.withJournal(true))
                        .timeout(budget.remainingMillis(), TimeUnit.MILLISECONDS).build());
            }
            // Never return a preparation receipt for a partial or vanished namespace set.
            verify(budget); requireActive.run(); budget.remainingMillis(); return VERSION;
        });
    }

    static Document initialMarker(String clusterId, String keyId, String operationId) {
        requireOperation(operationId);
        return new Document("installedVersion", VERSION).append("clusterId", clusterId)
                .append("layoutId", MongoConfigurationDomain.LAYOUT).append("keyId", keyId).append("operationId", operationId);
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
        if (!schema.containsKey(FIELD)) { return null; }
        if (!(schema.get(FIELD) instanceof Document value) || !value.keySet().equals(MARKER_FIELDS)
                || !Integer.valueOf(VERSION).equals(value.get("installedVersion"))
                || !clusterId.equals(value.get("clusterId")) || !keyId.equals(value.get("keyId"))
                || !MongoConfigurationDomain.LAYOUT.equals(value.get("layoutId"))
                || !(value.get("operationId") instanceof String operation)) { throw unreadable("feature-version"); }
        requireStoredOperation(operation);
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

    private void verify(MongoReadBudget budget) {
        Map<String, Document> found = structures(budget);
        for (SystemCollections collection : COLLECTIONS) {
            Document row = found.get(collection.collectionName());
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
            var expectedNames = new HashSet<String>();
            expectedNames.add("_id_");
            collection.indexes().forEach(index -> expectedNames.add(index.indexName()));
            if (!indexes.keySet().equals(expectedNames)) { throw unreadable("index-structure"); }
            if (!(indexes.get("_id_").get("key") instanceof Document primary)
                    || !primary.keySet().equals(Set.of("_id")) || !(primary.get("_id") instanceof Number order)
                    || order.doubleValue() != 1) { throw unreadable("index-structure"); }
            for (SystemCollections.IndexSpec expected : collection.indexes()) {
                Document index = indexes.get(expected.indexName());
                if (index == null || !(index.get("key") instanceof Document keys)
                        || !new ArrayList<>(keys.keySet()).equals(expected.keys())
                        || keys.values().stream().anyMatch(value -> !(value instanceof Number n) || n.doubleValue() != 1)
                        || index.get("unique") != null && !(index.get("unique") instanceof Boolean)
                        || Boolean.TRUE.equals(index.get("unique")) != expected.unique()
                        || index.containsKey("partialFilterExpression") || index.containsKey("collation")
                        || Boolean.TRUE.equals(index.get("sparse"))) { throw unreadable("index-structure"); }
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
