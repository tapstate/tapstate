package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import org.bson.Document;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

/**
 * Prepares only the identity, initial key and configuration schema of an explicitly authorized new
 * installation. The caller owns installation-file handoff and authorization. This is neither storage
 * binding completion nor first-administrator creation, and ordinary starts must use the load-only path.
 */
public final class MongoConfigurationBootstrap {
    static final String PREPARED = "CONFIG_PREPARED";
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();

    private MongoConfigurationBootstrap() {}

    /** Repeating the same installation operation loads its winner; a different operation never resets it. */
    public static MongoConfigurationDomain prepare(MongoConnectionSettings settings, String clusterId,
            String installationId, String operationId, long generation) {
        requireText(clusterId);
        requireText(installationId);
        requireText(operationId);
        if (generation < 1) throw new IllegalArgumentException("bootstrap generation must be positive");
        MongoConnection connection = MongoConnection.forConfiguration(settings);
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        try {
            connection.verifyConnectivity();
            MongoDatabase database = connection.database();
            // A namespace inventory is a refusal guard only. The transaction's identity/setup writes
            // arbitrate concurrent supported initializers; old/new binaries must never share a target.
            boolean emptyTarget = StoreIo.call(() -> {
                try (var namespaces = database.listCollectionNames().iterator()) {
                    return !namespaces.hasNext();
                }
            });
            StoreIo.run(() -> {
                for (int attempt = 0; ; attempt++) {
                    try (var session = connection.client().startSession()) {
                        session.withTransaction(() -> {
                            Document existing = SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database)
                                    .find(session, new Document("_id", "cluster")).first();
                            if (existing != null) {
                                if (!clusterId.equals(existing.get("clusterId"))
                                        || !MongoConfigurationDomain.LAYOUT.equals(existing.get("layoutId"))) {
                                    throw refused();
                                }
                                Document setup = SystemCollections.STORAGE_SETUP.on(database)
                                        .find(session, new Document("_id", "cluster")).first();
                                if (setup == null || !installationId.equals(setup.get("installationId"))
                                        || !operationId.equals(setup.get("operationId"))
                                        || !Long.valueOf(generation).equals(setup.get("generation"))
                                        || !PREPARED.equals(setup.get("phase"))) throw refused();
                                return null;
                            }
                            if (!emptyTarget) throw refused();
                            SystemCollections.STORAGE_SETUP.on(database).insertOne(session,
                                    new Document("_id", "cluster").append("clusterId", clusterId)
                                            .append("installationId", installationId).append("operationId", operationId)
                                            .append("generation", generation).append("phase", PREPARED));
                            SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database).insertOne(session,
                                    new Document("_id", "cluster").append("clusterId", clusterId)
                                            .append("layoutId", MongoConfigurationDomain.LAYOUT)
                                            .append("bindingVersion", 0L));
                            SystemCollections.CLUSTER_KEYS.on(database).insertOne(session,
                                    new Document("_id", "cluster").append("clusterId", clusterId)
                                            .append("formatVersion", 1).append("keyId", SourceConfigCipher.fingerprint(key))
                                            .append("material", Base64.getUrlEncoder().withoutPadding().encodeToString(key)));
                            SystemCollections.SCHEMA_MIGRATIONS.on(database).insertOne(session,
                                    new Document("_id", "config").append("installedVersion", MongoConfigurationDomain.SCHEMA_VERSION));
                            SystemCollections.CONFIGURATION_VERSION.on(database).insertOne(session,
                                    new Document("_id", ConfigurationVersion.ID).append("revision", 0L));
                            return null;
                        }, TRANSACTION);
                        break;
                    } catch (MongoException concurrent) {
                        // Concurrent first inserts can surface a duplicate id instead of a retry label.
                        // Re-evaluate the original operation, never overwrite or mint another identity.
                        if (attempt >= 2 || ErrorCategory.fromErrorCode(concurrent.getCode())
                                != ErrorCategory.DUPLICATE_KEY) throw concurrent;
                    }
                }
            });
            MongoStorageBindingStore.prepareIndex(database);
            MongoRequiredNoteStore.prepareIndexes(database);
            MongoConfigurationDraftStore.prepareIndexes(database);
            return new MongoConfigurationDomain(connection, clusterId);
        } catch (RuntimeException | Error failure) {
            connection.close();
            throw failure;
        } finally {
            Arrays.fill(key, (byte) 0);
        }
    }

    private static void requireText(String value) {
        Objects.requireNonNull(value, "bootstrap identity");
        if (value.isBlank()) throw new IllegalArgumentException("bootstrap identity must not be blank");
    }

    private static TapstateException refused() {
        return new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
    }
}
