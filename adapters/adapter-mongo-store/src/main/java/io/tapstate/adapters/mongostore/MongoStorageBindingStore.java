package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageBindingStore;
import io.tapstate.spi.store.StorageSetupScope;
import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/** Configuration-only binding publication; it neither opens a cache client nor enables a running member. */
public final class MongoStorageBindingStore implements StorageBindingStore {
    static final String BOUND = "STORAGE_BOUND";
    static final String DEFAULT_ROLE = "DEFAULT_TARGET";
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final MongoCollection<Document> bindings;
    private final MongoCollection<Document> identity;
    private final MongoCollection<Document> setup;
    private final MongoCollection<Document> sources;
    private final MongoCollection<Document> pipelines;
    private final MongoCollection<Document> versions;
    private final MongoCollection<Document> noteScopes;
    private final SourceConfigCipherProvider ciphers;
    private final EncryptedArtifactCodec codec;
    private final Runnable requireActive;

    MongoStorageBindingStore(MongoClient client, MongoDatabase database, SourceConfigCipherProvider ciphers) {
        this.client = Objects.requireNonNull(client, "client");
        this.database = Objects.requireNonNull(database, "database");
        this.bindings = durable(SystemCollections.STORAGE_BINDING.on(database));
        this.identity = durable(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database));
        this.setup = durable(SystemCollections.STORAGE_SETUP.on(database));
        this.sources = durable(SystemCollections.SOURCES.on(database));
        this.pipelines = durable(SystemCollections.PIPELINES.on(database));
        this.versions = durable(SystemCollections.CONFIGURATION_VERSION.on(database));
        this.noteScopes = durable(SystemCollections.PDK_NOTE_SCOPES.on(database));
        this.ciphers = Objects.requireNonNull(ciphers, "ciphers");
        this.codec = new EncryptedArtifactCodec(ciphers);
        this.requireActive = () -> {};
    }

    private MongoStorageBindingStore(MongoStorageBindingStore original, Runnable guard) {
        this.client = original.client;
        this.database = original.database;
        this.bindings = original.bindings;
        this.identity = original.identity;
        this.setup = original.setup;
        this.sources = original.sources;
        this.pipelines = original.pipelines;
        this.versions = original.versions;
        this.noteScopes = original.noteScopes;
        this.ciphers = original.ciphers;
        this.codec = original.codec;
        this.requireActive = () -> { original.requireActive.run(); guard.run(); };
    }

    @Override public StorageBindingStore guardedBy(Runnable guard) {
        return new MongoStorageBindingStore(this, Objects.requireNonNull(guard, "guard"));
    }

    /** Install only on the controlled setup path; normal opens never create or repair indexes. */
    static void prepareIndex(MongoDatabase database) {
        StoreIo.run(() -> IndexEnsure.createIfAbsent(SystemCollections.SOURCES.on(database),
                SystemCollections.SOURCES.indexes().getFirst()));
    }

    private void verifyIndex() {
        var declared = SystemCollections.SOURCES.indexes().getFirst();
        Document index = IndexEnsure.existing(sources, declared);
        if (index == null || !Boolean.TRUE.equals(index.get("unique"))
                || !new Document("systemRole", 1).equals(index.get("key"))
                || !Document.parse(declared.partialFilterJson()).equals(index.get("partialFilterExpression"))) {
            throw unreadable();
        }
    }

    @Override public Optional<StorageBinding> load() {
        return StoreIo.call(() -> {
            requireActive.run();
            verifyIndex();
            return transaction(session -> load(session).map(Loaded::binding));
        });
    }

    @Override public StorageBinding commit(StorageSetupScope expected, StorageBinding.Mode mode,
            StorageBinding.Connection cache, SourceResource defaultSource) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(cache, "cache");
        Objects.requireNonNull(defaultSource, "defaultSource");
        // Capture the nested connector config once, before a retry can invoke external code again.
        SourceResource captured = (SourceResource) new DslParser().parse(new CanonicalWriter().write(defaultSource));
        if (!captured.id().equals(StorageBinding.defaultSourceId(mode)) || captured.mode() != null
                || captured.tables() != null || captured.srs() != null || captured.execution() != null) throw refused();
        String cacheJson = encodeConnection(cache);
        String candidateHash = fingerprint(mode, cacheJson, CanonicalHash.of(captured));
        return StoreIo.call(() -> {
            requireActive.run();
            verifyIndex();
            return transaction(session -> {
                Optional<Loaded> previous = load(session);
                if (previous.isPresent()) {
                    Loaded loaded = previous.get();
                    if (!expected.equals(loaded.binding().setup()) || mode != loaded.binding().mode()
                            || !candidateHash.equals(loaded.candidateHash())) throw refused();
                    return loaded.binding();
                }
                Document state = setup.find(session, new Document("_id", "cluster")).first();
                requireSetup(state, expected, MongoConfigurationBootstrap.PREPARED);
                Document cluster = identity.find(session, new Document("_id", "cluster")).first();
                if (!expected.clusterId().equals(cluster.get("clusterId"))) throw refused();
                if (sources.find(session, new Document("_id", captured.id())).first() != null
                        || pipelines.find(session, new Document("_id", captured.id())).first() != null
                        || sources.find(session, new Document("systemRole", DEFAULT_ROLE)).first() != null) throw refused();
                new MongoPipelineStateMappings(client, database).requireNoRetainedIdentity(session, captured.id());
                ConfigurationVersion.advance(session, versions);
                SourceConfigCipher cipher = cipherForWrite(session);
                StorageBinding binding = new StorageBinding(expected, mode, 1L, captured.id(), cache);
                sources.insertOne(session, codec.encode(captured, session).append("systemRole", DEFAULT_ROLE));
                bindings.insertOne(session, new Document("_id", "cluster")
                        .append("clusterId", expected.clusterId()).append("layoutId", MongoConfigurationDomain.LAYOUT)
                        .append("bindingVersion", 1L).append("mode", mode.name())
                        .append("installationId", expected.installationId()).append("operationId", expected.operationId())
                        .append("generation", expected.generation()).append("defaultSourceId", captured.id())
                        .append("sourceHash", CanonicalHash.of(captured)).append("candidateHash", candidateHash)
                        .append("cache", cipher.encrypt(bindingContext(expected.clusterId()), "cache-binding", cacheJson)));
                if (identity.updateOne(session, new Document("_id", "cluster").append("bindingVersion", 0L),
                        new Document("$set", new Document("bindingVersion", 1L))).getMatchedCount() != 1) throw refused();
                if (setup.updateOne(session, setupFilter(expected, MongoConfigurationBootstrap.PREPARED),
                        new Document("$set", new Document("phase", BOUND).append("bindingVersion", 1L)
                                .append("defaultSourceId", captured.id()).append("candidateHash", candidateHash)))
                        .getMatchedCount() != 1) throw refused();
                // Binding owns this Cluster-wide scope; pipeline teardown never initializes or clears it.
                noteScopes.insertOne(session, new Document("_id", new Document("bindingVersion", 1L)
                        .append("namespace", "pdk.global-state")).append("ownerKind", "GLOBAL")
                        .append("ownerId", expected.clusterId()).append("generation", 1L).append("writeFence", 0L)
                        .append("outgoingCarries", 0L).append("incomingCarries", 0L));
                return binding;
            });
        });
    }

    private Optional<Loaded> load(ClientSession session) {
        Document cluster = identity.find(session, new Document("_id", "cluster")).first();
        if (cluster == null || !(cluster.get("clusterId") instanceof String clusterId) || clusterId.isBlank()
                || !MongoConfigurationDomain.LAYOUT.equals(cluster.get("layoutId"))
                || !(cluster.get("bindingVersion") instanceof Long version) || version < 0 || version > 1) throw unreadable();
        Document stored = bindings.find(session, new Document("_id", "cluster")).first();
        Document state = setup.find(session, new Document("_id", "cluster")).first();
        if (state == null || !clusterId.equals(state.get("clusterId"))
                || !(state.get("installationId") instanceof String installationId) || installationId.isBlank()
                || !(state.get("operationId") instanceof String operationId) || operationId.isBlank()
                || !(state.get("generation") instanceof Long currentGeneration) || currentGeneration < 1) throw unreadable();
        if (version == 0) {
            if (!MongoConfigurationBootstrap.PREPARED.equals(state.get("phase"))
                    || (state.containsKey("bindingVersion") && !Long.valueOf(0).equals(state.get("bindingVersion")))
                    || state.containsKey("defaultSourceId") || state.containsKey("candidateHash")) throw unreadable();
            if (stored != null || sources.find(session, new Document("systemRole", DEFAULT_ROLE)).first() != null) throw unreadable();
            return Optional.empty();
        }
        try {
            if (stored == null || !clusterId.equals(stored.get("clusterId"))
                    || !MongoConfigurationDomain.LAYOUT.equals(stored.get("layoutId"))
                    || !Long.valueOf(version).equals(stored.get("bindingVersion"))) throw unreadable();
            StorageSetupScope scope = new StorageSetupScope(clusterId, stored.getString("installationId"),
                    stored.getString("operationId"), stored.getLong("generation"));
            StorageBinding.Mode mode = StorageBinding.Mode.valueOf(stored.getString("mode"));
            String defaultId = stored.getString("defaultSourceId");
            if (currentGeneration < scope.generation() || !(BOUND.equals(state.get("phase")) || "COMPLETE".equals(state.get("phase")))
                    || !Long.valueOf(version).equals(state.get("bindingVersion"))
                    || !defaultId.equals(state.get("defaultSourceId"))) throw unreadable();
            // A later purpose may advance capability generation; installation and original operation remain fixed.
            if (!scope.clusterId().equals(state.get("clusterId"))
                    || !scope.installationId().equals(state.get("installationId"))
                    || !scope.operationId().equals(state.get("operationId"))) throw unreadable();
            Document source = sources.find(session, new Document("_id", defaultId)).first();
            if (source == null || !DEFAULT_ROLE.equals(source.get("systemRole"))
                    || pipelines.find(session, new Document("_id", defaultId)).first() != null
                    || !stored.get("sourceHash").equals(CanonicalHash.of(codec.decode(source)))) throw unreadable();
            String cacheJson = ciphers.current().decrypt(bindingContext(clusterId), "cache-binding", stored.getString("cache"));
            StorageBinding.Connection cache = decodeConnection(cacheJson);
            String candidateHash = fingerprint(mode, cacheJson, stored.getString("sourceHash"));
            if (!candidateHash.equals(stored.get("candidateHash")) || !candidateHash.equals(state.get("candidateHash"))) throw unreadable();
            Document global = noteScopes.find(session, new Document("_id", new Document("bindingVersion", version)
                    .append("namespace", "pdk.global-state"))).first();
            if (global == null || !"GLOBAL".equals(global.get("ownerKind")) || !clusterId.equals(global.get("ownerId"))
                    || !(global.get("generation") instanceof Long generation) || generation < 1
                    || !(global.get("writeFence") instanceof Long writeFence) || writeFence < 0 || writeFence == Long.MAX_VALUE
                    || !Long.valueOf(0).equals(global.get("outgoingCarries"))
                    || !Long.valueOf(0).equals(global.get("incomingCarries"))) throw unreadable();
            return Optional.of(new Loaded(new StorageBinding(scope, mode, version, defaultId, cache), candidateHash));
        } catch (NullPointerException | IllegalArgumentException | ClassCastException invalid) {
            // Stored or decrypted values may contain credentials. Never attach parser input or causes.
            throw unreadable();
        }
    }

    private SourceConfigCipher cipherForWrite(ClientSession session) {
        SourceConfigCipher cipher = ciphers.currentForWrite();
        if (ciphers.requiresWriteFence() && !ciphers.fenceWrite(new SourceConfigWriteScope(session), cipher.activeKeyId())) {
            throw new TapstateException(StoreError.SOURCE_CONFIG_KEYRING_INVALID, Map.of(), null);
        }
        return cipher;
    }

    private <T> T transaction(Function<ClientSession, T> body) {
        try (ClientSession session = client.startSession()) {
            return session.withTransaction(() -> {
                requireActive.run();
                T result = body.apply(session);
                requireActive.run();
                return result;
            }, TRANSACTION);
        }
    }

    private static void requireSetup(Document state, StorageSetupScope scope, String phase) {
        if (state == null || !scope.clusterId().equals(state.get("clusterId"))
                || !scope.installationId().equals(state.get("installationId"))
                || !scope.operationId().equals(state.get("operationId"))
                || !Long.valueOf(scope.generation()).equals(state.get("generation"))
                || !phase.equals(state.get("phase"))) throw refused();
    }

    private static Document setupFilter(StorageSetupScope scope, String phase) {
        return new Document("_id", "cluster").append("clusterId", scope.clusterId())
                .append("installationId", scope.installationId()).append("operationId", scope.operationId())
                .append("generation", scope.generation()).append("phase", phase);
    }

    private static String bindingContext(String clusterId) { return JsonWriter.write(List.of(clusterId, 1L)); }
    private static String encodeConnection(StorageBinding.Connection cache) {
        return JsonWriter.write(new Document("kind", cache.kind().name()).append("uri", cache.uri())
                .append("database", cache.database()).append("caPem", cache.caPem()));
    }
    private static StorageBinding.Connection decodeConnection(String json) {
        Map<?, ?> tree = (Map<?, ?>) JsonReader.parse(json);
        return new StorageBinding.Connection(StorageBinding.Connection.Kind.valueOf((String) tree.get("kind")),
                (String) tree.get("uri"), (String) tree.get("database"), (String) tree.get("caPem"));
    }
    private static String fingerprint(StorageBinding.Mode mode, String cacheJson, String sourceHash) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(JsonWriter.write(List.of(mode.name(), cacheJson, sourceHash)).getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
    private static MongoCollection<Document> durable(MongoCollection<Document> collection) {
        return collection.withReadConcern(ReadConcern.MAJORITY).withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }
    private static TapstateException refused() {
        return new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
    }
    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "storage-binding", "field", "binding"), null);
    }
    private record Loaded(StorageBinding binding, String candidateHash) {}
}
