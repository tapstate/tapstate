package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.TokenStore;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.FirstAdminStore;
import io.tapstate.spi.store.UserStore;
import io.tapstate.spi.store.CatalogStore;
import io.tapstate.spi.store.PipelineLayoutStore;
import io.tapstate.spi.store.PipelineDraftStore;
import io.tapstate.spi.store.PipelineStateMappings;
import io.tapstate.spi.store.RequiredNoteStore;
import io.tapstate.spi.store.StorageBindingStore;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

/**
 * Opens an already initialized configuration domain without opening any cache or business resource.
 * Bootstrap owns creation of identity, schema and key facts. Ordinary opens only verify them and never
 * run the old shared-database migrations or infer a new installation from missing records.
 */
public final class MongoConfigurationDomain implements AutoCloseable {
    static final String LAYOUT = "three-layer-v1";
    static final int SCHEMA_VERSION = 1;
    private final MongoConnection connection;
    private final MongoConfigurationArtifactStore artifacts;
    private final MongoConfigurationCatalogStore catalog;
    private final PipelineLayoutStore layouts;
    private final PipelineStateMappings pipelineStates;
    private final RequiredNoteStore requiredNotes;
    private final MongoStorageBindingStore bindings;
    private final MongoConfigurationSecurityStores security;
    private final MongoFirstAdminStore firstAdmin;
    private final MongoConfigurationDraftStore drafts;
    private final String clusterId;
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();

    MongoConfigurationDomain(MongoConnection connection, String clusterId) {
        this.connection = connection;
        this.clusterId = clusterId;
        MongoDatabase database = connection.database();
        Document identity = StoreIo.call(() -> durable(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database))
                .find(new Document("_id", "cluster")).first());
        if (identity == null || !clusterId.equals(identity.get("clusterId"))
                || !LAYOUT.equals(identity.get("layoutId"))
                || !(identity.get("bindingVersion") instanceof Long binding) || binding < 0) {
            throw unreadable("cluster", "identity");
        }
        Document schema = StoreIo.call(() -> durable(SystemCollections.SCHEMA_MIGRATIONS.on(database))
                .find(new Document("_id", "config")).first());
        if (schema == null || !(schema.get("installedVersion") instanceof Integer installed) || installed < 1) {
            throw unreadable("config", "installedVersion");
        }
        if (installed > SCHEMA_VERSION) {
            throw new TapstateException(MigrationError.DATA_NEWER_THAN_BINARY,
                    Map.of("installed", String.valueOf(installed), "supported", String.valueOf(SCHEMA_VERSION)), null);
        }
        Document version = StoreIo.call(() -> durable(SystemCollections.CONFIGURATION_VERSION.on(database))
                .find(new Document("_id", ConfigurationVersion.ID)).first());
        if (version == null || !(version.get("revision") instanceof Long revision)
                || revision < 0 || revision == Long.MAX_VALUE) {
            throw unreadable(ConfigurationVersion.ID, "revision");
        }
        PersistentKey key = new PersistentKey(database, clusterId);
        this.artifacts = new MongoConfigurationArtifactStore(connection.client(), database, key);
        this.requiredNotes = new MongoRequiredNoteStore(connection.client(), database, key);
        this.catalog = new MongoConfigurationCatalogStore(connection.client(), database, key);
        artifacts.verifySourceEnvelopes();
        catalog.verifyEnvelopes();
        this.pipelineStates = new MongoPipelineStateMappings(connection.client(), database);
        this.bindings = new MongoStorageBindingStore(connection.client(), database, key);
        bindings.load();
        this.security = new MongoConfigurationSecurityStores(connection.client(), database, clusterId);
        security.verify();
        this.firstAdmin = new MongoFirstAdminStore(connection.client(), database, bindings);
        firstAdmin.load();
        this.layouts = new MongoPipelineLayoutStore(connection.client(),
                SystemCollections.CONFIG_PIPELINE_LAYOUTS.on(database),
                SystemCollections.CONFIGURATION_VERSION.on(database));
        this.drafts = new MongoConfigurationDraftStore(connection.client(), database, key, artifacts);
        drafts.verify();
        new MongoRequiredDeadLetterStore(connection.client(), database, clusterId).verify();
    }

    /** The caller supplies the trusted Cluster identity from its deployment or installation context. */
    public static MongoConfigurationDomain open(MongoConnectionSettings settings, String expectedClusterId) {
        Objects.requireNonNull(expectedClusterId, "expectedClusterId");
        if (expectedClusterId.isBlank()) throw new IllegalArgumentException("expected Cluster identity must not be blank");
        MongoConnection connection = MongoConnection.forConfiguration(settings);
        try {
            connection.verifyConnectivity();
            return new MongoConfigurationDomain(connection, expectedClusterId);
        } catch (RuntimeException | Error failure) {
            connection.close();
            throw failure;
        }
    }

    public ArtifactStore artifacts() {
        return artifacts;
    }

    public CatalogStore catalog() {
        return catalog;
    }

    public PipelineLayoutStore layouts() {
        return layouts;
    }

    public RequiredNoteStore requiredNotes() {
        return requiredNotes;
    }

    public PipelineStateMappings pipelineStates() {
        return pipelineStates;
    }

    public io.tapstate.spi.store.PipelineStatePreparations preparations() {
        requireOpen();
        return new MongoPipelineStatePreparations(connection.client(), connection.database(), clusterId, bindings, this::requireOpen);
    }

    public StorageBindingStore bindings() {
        return bindings;
    }

    public io.tapstate.spi.store.PipelineIntentStore intents() {
        requireOpen();
        return new MongoPipelineIntentStore(connection.client(), connection.database(), clusterId, bindings).guardedBy(this::requireOpen);
    }

    public PipelineDraftStore drafts() { return drafts; }

    public io.tapstate.spi.store.RequiredDeadLetterStore requiredDeadLetters() {
        return new MongoRequiredDeadLetterStore(connection.client(), connection.database(), clusterId);
    }

    public FirstAdminStore firstAdmin() { return firstAdmin; }

    public UserStore users() { return firstAdmin.users(); }

    public TokenStore tokens() { return security.tokens(); }

    public AuditStore audit() { return security.audit(); }

    public ClusterIdentityStore clusterIdentity() { return security.clusterIdentity(); }

    @Override
    public void close() {
        closed.set(true);
        connection.close();
    }

    private void requireOpen() {
        if (closed.get()) throw new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
    }

    private static MongoCollection<Document> durable(MongoCollection<Document> collection) {
        return collection.withReadPreference(ReadPreference.primary()).withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }

    private static TapstateException unreadable(String id, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id, "field", field), null);
    }

    /** One persistent key, with no node-session ACK, rotation state or cache access. */
    private static final class PersistentKey implements SourceConfigCipherProvider {
        private final MongoCollection<Document> keys;
        private final String clusterId;
        private final SourceConfigCipher cipher;

        private PersistentKey(MongoDatabase database, String clusterId) {
            this.keys = durable(SystemCollections.CLUSTER_KEYS.on(database));
            this.clusterId = clusterId;
            this.cipher = StoreIo.call(() -> decode(keys.find(new Document("_id", "cluster")).first()));
        }

        @Override
        public SourceConfigCipher current() {
            return cipher;
        }

        @Override
        public SourceConfigCipher refresh() {
            SourceConfigCipher observed = StoreIo.call(() -> decode(keys.find(new Document("_id", "cluster")).first()));
            if (!cipher.activeKeyId().equals(observed.activeKeyId())) throw invalidKey();
            return observed;
        }

        @Override
        public boolean requiresWriteFence() {
            return true;
        }

        @Override
        public boolean fenceWrite(SourceConfigWriteScope scope, String keyId) {
            SourceConfigCipher observed = decode(keys.find(scope.session(), new Document("_id", "cluster")).first());
            if (!cipher.activeKeyId().equals(keyId) || !keyId.equals(observed.activeKeyId())) throw invalidKey();
            return keys.updateOne(scope.session(), new Document("_id", "cluster").append("keyId", keyId),
                    new Document("$inc", new Document("sourceWriteFence", 1L))).getMatchedCount() == 1;
        }

        private SourceConfigCipher decode(Document document) {
            if (document == null || !clusterId.equals(document.get("clusterId"))
                    || !Integer.valueOf(1).equals(document.get("formatVersion"))
                    || !(document.get("keyId") instanceof String keyId)
                    || !(document.get("material") instanceof String material)) throw invalidKey();
            byte[] bytes;
            try {
                bytes = Base64.getUrlDecoder().decode(material);
            } catch (IllegalArgumentException invalid) {
                throw invalidKey();
            }
            try {
                if (bytes.length != 32 || !keyId.equals(SourceConfigCipher.fingerprint(bytes))) throw invalidKey();
                return new SourceConfigCipher(bytes);
            } finally {
                Arrays.fill(bytes, (byte) 0);
            }
        }

        private static TapstateException invalidKey() {
            return new TapstateException(StoreError.SOURCE_CONFIG_KEYRING_INVALID, Map.of(), null);
        }
    }
}
