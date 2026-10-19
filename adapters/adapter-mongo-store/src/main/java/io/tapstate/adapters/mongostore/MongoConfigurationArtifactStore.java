package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.PipelineConnectorNodes;
import io.tapstate.spi.store.RequiredNoteOwner;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactBatchWrite;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ArtifactWrite;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StoredArtifactRecord;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Configuration resources in separate collections for each authoring kind. Every mutation writes the same
 * configuration revision inside its transaction, including mutations that only read another resource
 * as a precondition. This serializes cross-collection identity and dependency decisions without a
 * separate id directory or a cross-client transaction.
 *
 * <p>The configuration bootstrap must have installed the revision singleton before exposing this
 * store. A missing or malformed singleton is corruption, not permission to initialize an empty store.
 * All five authoring kinds share one identity and revision transaction domain.
 */
public final class MongoConfigurationArtifactStore implements ArtifactStore {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true))
            .build();

    private final MongoClient client;
    private static final CanonicalWriter WRITER = new CanonicalWriter();
    private final Map<String, MongoCollection<Document>> collections;
    private final MongoCollection<Document> versions;
    private final EncryptedArtifactCodec codec;
    private final MongoPipelineStateMappings stateMappings;
    private final MongoRequiredNoteStore notes;
    private final MongoCollection<Document> identity;
    private final MongoCollection<Document> mappings;

    public MongoConfigurationArtifactStore(MongoClient client, MongoDatabase database, SourceConfigCipher cipher) {
        this(client, database, SourceConfigCipherProvider.fixed(Objects.requireNonNull(cipher, "cipher")));
    }

    MongoConfigurationArtifactStore(MongoClient client, MongoDatabase database, SourceConfigCipherProvider ciphers) {
        this.client = Objects.requireNonNull(client, "client");
        Objects.requireNonNull(database, "database");
        Map<String, MongoCollection<Document>> tables = new LinkedHashMap<>();
        tables.put("source", durable(SystemCollections.SOURCES.on(database)));
        tables.put("pipeline", durable(SystemCollections.PIPELINES.on(database)));
        tables.put("transform", durable(SystemCollections.TRANSFORM_DEFINITIONS.on(database)));
        tables.put("view", durable(SystemCollections.VIEW_DEFINITIONS.on(database)));
        tables.put("serve", durable(SystemCollections.SERVE_DEFINITIONS.on(database)));
        this.collections = Collections.unmodifiableMap(tables);
        this.versions = durable(SystemCollections.CONFIGURATION_VERSION.on(database));
        this.codec = new EncryptedArtifactCodec(Objects.requireNonNull(ciphers, "ciphers"));
        this.stateMappings = new MongoPipelineStateMappings(client, database);
        this.notes = new MongoRequiredNoteStore(client, database, ciphers);
        this.identity = durable(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database));
        this.mappings = durable(SystemCollections.OPERATOR_GENERATIONS.on(database));
    }

    private static MongoCollection<Document> durable(MongoCollection<Document> collection) {
        return collection.withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }

    @Override
    public ArtifactMutation create(Resource artifact) {
        ArtifactBatchWrite outcome = writeAll(List.of(ArtifactWrite.createOnly(artifact)));
        return outcome.appliedSuccessfully() ? ArtifactMutation.CREATED : outcome.refusal();
    }

    @Override
    public ArtifactMutation replace(String id, String expectedContentHash, Resource replacement) {
        Objects.requireNonNull(id, "id");
        if (!id.equals(replacement.id())) throw new IllegalArgumentException("replacement id must equal artifact id");
        ArtifactBatchWrite outcome = writeAll(List.of(ArtifactWrite.replaceOnly(replacement, expectedContentHash)));
        return outcome.appliedSuccessfully() ? ArtifactMutation.REPLACED : outcome.refusal();
    }

    @Override
    public ArtifactMutation delete(String id, String expectedContentHash) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(expectedContentHash, "expectedContentHash");
        return transaction(session -> {
            Document stored = find(session, id);
            if (stored == null) throw new Refused(id, ArtifactMutation.NOT_FOUND);
            if (!expectedContentHash.equals(stored.getString("contentHash"))) {
                throw new Refused(id, ArtifactMutation.VERSION_CONFLICT);
            }
            requireUnmanaged(stored);
            if ("pipeline".equals(stored.getString("kind"))) stateMappings.refuseRemoval(session, id);
            else stateMappings.requireNoRetainedIdentity(session, id);
            collection(stored.getString("kind")).deleteOne(session, new Document("_id", id));
            return ArtifactMutation.DELETED;
        }, refused -> refused.outcome);
    }

    @Override
    public ArtifactBatchWrite writeAll(List<ArtifactWrite> writes) {
        List<ArtifactWrite> requested = List.copyOf(writes);
        if (requested.isEmpty()) return ArtifactBatchWrite.applied();
        requested.forEach(write -> collection(write.resource().kind()));
        return transaction(session -> {
            writeAllInTransaction(session, requested);
            return ArtifactBatchWrite.applied();
        }, refused -> ArtifactBatchWrite.refused(refused.id, refused.outcome));
    }

    /** The caller owns the configuration revision and transaction; a refusal must abort the entire callback. */
    void writeAllInTransaction(ClientSession session, List<ArtifactWrite> requested) {
        requested.forEach(write -> collection(write.resource().kind()));
        // Evaluate all dependencies against one snapshot before applying any sibling write.
        for (ArtifactWrite write : requested) {
            for (Map.Entry<String, String> expected : write.readPreconditions().entrySet()) {
                Document observed = find(session, expected.getKey());
                if (observed == null || !expected.getValue().equals(observed.getString("contentHash"))) {
                    throw new Refused(expected.getKey(), ArtifactMutation.VERSION_CONFLICT);
                }
            }
        }
        Map<String, Set<PipelineConnectorNodes.Identity>> before = privateNodes(session);
        for (ArtifactWrite write : requested) write(session, write);
        reservePrivateScopes(session, before, requested);
    }

    boolean belongsTo(MongoClient expected,String database) {
        return client == expected && versions.getNamespace().getDatabaseName().equals(database);
    }

    Optional<Resource> getInTransaction(ClientSession session, String id) {
        return Optional.ofNullable(find(session, id)).map(this::decode);
    }

    private Map<String, Set<PipelineConnectorNodes.Identity>> privateNodes(ClientSession session) {
        Map<String, Set<PipelineConnectorNodes.Identity>> result = new LinkedHashMap<>();
        for (Document row : read(session, collection("pipeline"), "pipeline")) {
            PipelineResource pipeline = (PipelineResource) decode(row);
            result.put(pipeline.id(), PipelineConnectorNodes.of(pipeline, id -> {
                Document definition = find(session, id);
                if (definition == null || !"serve".equals(definition.getString("kind"))) {
                    throw unreadable(id, "serveDefinition");
                }
                return decode(definition);
            }));
        }
        return result;
    }

    private void reservePrivateScopes(ClientSession session,
            Map<String, Set<PipelineConnectorNodes.Identity>> before, List<ArtifactWrite> requested) {
        Map<String, Set<PipelineConnectorNodes.Identity>> after = privateNodes(session);
        Set<String> writtenPipelines = new HashSet<>();
        requested.stream().map(ArtifactWrite::resource).filter(PipelineResource.class::isInstance)
                .forEach(resource -> writtenPipelines.add(resource.id()));
        for (var entry : after.entrySet()) {
            Set<PipelineConnectorNodes.Identity> prior = before.getOrDefault(entry.getKey(), Set.of());
            Set<PipelineConnectorNodes.Identity> current = entry.getValue();
            if (prior.equals(current) && !writtenPipelines.contains(entry.getKey())) continue;
            if (prior.isEmpty() && current.isEmpty()) continue;
            Document cluster = identity.find(session, new Document("_id", "cluster")).first();
            if (cluster == null || !MongoConfigurationDomain.LAYOUT.equals(cluster.get("layoutId"))
                    || !(cluster.get("bindingVersion") instanceof Long binding) || binding < 1) {
                throw new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
            }
            if (!(cluster.get("clusterId") instanceof String clusterId) || clusterId.isBlank()) {
                throw new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
            }
            notes.open(session, new RequiredNoteOwner(binding, "pdk.global-state", RequiredNoteOwner.Kind.GLOBAL,
                    clusterId, 0));
            stateMappings.requireActive(session, entry.getKey());
            var mapping = MongoPipelineStateMappings.decode(entry.getKey(),
                    mappings.find(session, new Document("_id", entry.getKey())).first());
            for (var node : prior) {
                notes.open(session, RequiredNoteOwner.forNode(binding, node.node(), node.purpose(), mapping.stateGeneration()));
            }
            for (var node : current) {
                if (prior.contains(node)) continue;
                notes.initialize(session, RequiredNoteOwner.forNode(binding, node.node(), node.purpose(), mapping.stateGeneration()));
            }
        }
    }

    private void write(ClientSession session, ArtifactWrite write) {
        Resource resource = write.resource();
        Document observed = find(session, resource.id());
        if (write.intent() == ArtifactWrite.Intent.CREATE_ONLY && observed != null) {
            throw new Refused(resource.id(), ArtifactMutation.ALREADY_EXISTS);
        }
        if (write.intent() == ArtifactWrite.Intent.REPLACE_ONLY) {
            if (observed == null) throw new Refused(resource.id(), ArtifactMutation.NOT_FOUND);
            if (!write.expectedContentHash().equals(observed.getString("contentHash"))) {
                throw new Refused(resource.id(), ArtifactMutation.VERSION_CONFLICT);
            }
        }
        if (!"pipeline".equals(resource.kind())) stateMappings.requireNoRetainedIdentity(session, resource.id());
        if (observed != null) {
            requireUnmanaged(observed);
            if ("pipeline".equals(observed.getString("kind"))) {
                stateMappings.requireActive(session, resource.id());
                if (!"pipeline".equals(resource.kind())) stateMappings.refuseRemoval(session, resource.id());
            }
        }
        if ("pipeline".equals(resource.kind()) && (observed == null || !"pipeline".equals(observed.getString("kind")))) {
            stateMappings.create(session, resource.id());
        }
        // Unconditional apply may replace a kind, but must never leave the same id in both tables.
        if (observed != null && !resource.kind().equals(observed.getString("kind"))) {
            collection(observed.getString("kind")).deleteOne(session, new Document("_id", resource.id()));
        }
        collection(resource.kind()).replaceOne(session, new Document("_id", resource.id()),
                codec.encode(resource, session), new ReplaceOptions().upsert(true));
    }

    private <T> T transaction(java.util.function.Function<ClientSession, T> body,
            java.util.function.Function<Refused, T> refusal) {
        return StoreIo.call(() -> {
            try (ClientSession session = client.startSession()) {
                try {
                    return session.withTransaction(() -> {
                        ConfigurationVersion.advance(session, versions);
                        return body.apply(session);
                    }, TRANSACTION);
                } catch (Refused refused) {
                    // The driver aborts the transaction before returning a conditional refusal.
                    return refusal.apply(refused);
                }
            }
        });
    }

    @Override
    public void saveAll(List<Resource> artifacts) {
        saveAll(artifacts, Map.of());
    }

    @Override
    public Optional<String> saveAll(List<Resource> artifacts, Map<String, String> expectedContentHashes) {
        Objects.requireNonNull(expectedContentHashes, "expectedContentHashes");
        if (artifacts.isEmpty()) return Optional.empty();
        List<ArtifactWrite> writes = new ArrayList<>(artifacts.stream().map(ArtifactWrite::upsert).toList());
        writes.set(0, writes.getFirst().guardedBy(expectedContentHashes));
        ArtifactBatchWrite outcome = writeAll(writes);
        return outcome.appliedSuccessfully() ? Optional.empty() : Optional.of(outcome.refusedId());
    }

    @Override
    public Optional<Resource> get(String id) {
        Objects.requireNonNull(id, "id");
        return readSnapshot(session -> Optional.ofNullable(find(session, id)).map(this::decode));
    }

    @Override
    public List<Resource> list() {
        return readSnapshot(session -> documents(session).stream().map(this::decode).toList());
    }

    @Override
    public List<StoredArtifactRecord> listStored() {
        return readSnapshot(session -> documents(session).stream().map(this::browse).toList());
    }

    @Override
    public List<StoredArtifactRecord> listStored(String kind) {
        Objects.requireNonNull(kind, "kind");
        if (!collections.containsKey(kind)) return List.of();
        return readSnapshot(session -> {
            List<Document> selected = read(session, collection(kind), kind);
            if (!selected.isEmpty()) {
                List<String> ids = selected.stream().map(row -> row.getString("_id")).toList();
                for (var entry : collections.entrySet()) {
                    if (entry.getKey().equals(kind)) continue;
                    Document collision = entry.getValue().find(session,
                            new Document("_id", new Document("$in", ids))).projection(new Document("_id", 1)).first();
                    if (collision != null) throw unreadable(String.valueOf(collision.get("_id")), "identity");
                }
            }
            return selected.stream().map(this::browse).toList();
        });
    }

    /** Verifies encrypted Source bodies at open without retaining the entire inventory in memory. */
    void verifySourceEnvelopes() {
        readSnapshot(session -> {
            try (MongoCursor<Document> cursor = collection("source").find(session).iterator()) {
                while (cursor.hasNext()) {
                    Document source = cursor.next();
                    requireKind(source, "source");
                    codec.decode(source);
                }
            }
            return null;
        });
    }

    private <T> T readSnapshot(java.util.function.Function<ClientSession, T> body) {
        return StoreIo.call(() -> {
            try (ClientSession session = client.startSession()) {
                return session.withTransaction(() -> body.apply(session), TRANSACTION);
            }
        });
    }

    private Document find(ClientSession session, String id) {
        Document found = null;
        for (var entry : collections.entrySet()) {
            Document candidate = entry.getValue().find(session, new Document("_id", id)).first();
            if (candidate == null) continue;
            requireKind(candidate, entry.getKey());
            if (found != null) throw unreadable(id, "identity");
            found = candidate;
        }
        return found;
    }

    private List<Document> documents(ClientSession session) {
        List<Document> documents = new ArrayList<>();
        for (var entry : collections.entrySet()) documents.addAll(read(session, entry.getValue(), entry.getKey()));
        Set<String> ids = new HashSet<>();
        for (Document document : documents) {
            if (!ids.add(document.getString("_id"))) throw unreadable(document.getString("_id"), "identity");
        }
        return documents;
    }

    private List<Document> read(ClientSession session, MongoCollection<Document> collection, String kind) {
        List<Document> documents = new ArrayList<>();
        try (MongoCursor<Document> cursor = collection.find(session).iterator()) {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                requireKind(document, kind);
                documents.add(document);
            }
        }
        return documents;
    }

    private static void requireUnmanaged(Document document) {
        if (document.containsKey("systemRole")) {
            throw new TapstateException(StoreError.DEFAULT_SOURCE_PROTECTED, Map.of(), null);
        }
    }

    private static void requireKind(Document document, String kind) {
        if (!(document.get("_id") instanceof String id) || id.isBlank() || !kind.equals(document.get("kind"))) {
            throw unreadable(String.valueOf(document.get("_id")), "kind");
        }
    }

    private MongoCollection<Document> collection(String kind) {
        MongoCollection<Document> table = collections.get(kind);
        if (table == null) throw new IllegalArgumentException("unsupported configuration resource kind");
        return table;
    }

    private Resource decode(Document document) {
        Resource resource = codec.decode(document);
        String id = document.getString("_id");
        if (!id.equals(resource.id()) || !resource.kind().equals(document.get("kind"))
                || !CanonicalHash.of(resource).equals(document.get("contentHash"))) throw unreadable(id, "body");
        return resource;
    }

    private StoredArtifactRecord browse(Document document) {
        String id = document.getString("_id");
        String kind = document.getString("kind");
        String hash = document.get("contentHash") instanceof String value ? value : null;
        try {
            return new StoredArtifactRecord(id, kind, WRITER.write(decode(document)), hash, true);
        } catch (TapstateException invalid) {
            if (invalid.code() != IoError.DOCUMENT_UNREADABLE) throw invalid;
            return new StoredArtifactRecord(id, kind, null, hash, false);
        }
    }

    private static TapstateException unreadable(String id, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id, "field", field), null);
    }

    /** Internal transaction control: a refused condition must abort sibling writes and the revision. */
    static final class Refused extends RuntimeException {
        private final String id;
        private final ArtifactMutation outcome;

        private Refused(String id, ArtifactMutation outcome) {
            super(null, null, false, false);
            this.id = id;
            this.outcome = outcome;
        }
    }
}
