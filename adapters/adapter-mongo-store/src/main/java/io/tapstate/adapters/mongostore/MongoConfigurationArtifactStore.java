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
import io.tapstate.spi.store.ArtifactBatchWrite;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ArtifactWrite;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StoredArtifactRecord;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Configuration resources in separate Source and Pipeline collections. Every mutation writes the same
 * configuration revision inside its transaction, including mutations that only read another resource
 * as a precondition. This serializes cross-collection identity and dependency decisions without a
 * separate id directory or a cross-client transaction.
 *
 * <p>The configuration bootstrap must have installed the revision singleton before exposing this
 * store. A missing or malformed singleton is corruption, not permission to initialize an empty store.
 * Only Source and Pipeline resources reach this adapter; authoring translation rejects other kinds.
 */
public final class MongoConfigurationArtifactStore implements ArtifactStore {
    static final String REVISION_ID = "configuration";
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true))
            .build();

    private final MongoClient client;
    private final MongoCollection<Document> sources;
    private final MongoCollection<Document> pipelines;
    private final MongoCollection<Document> versions;
    private final EncryptedArtifactCodec codec;

    public MongoConfigurationArtifactStore(MongoClient client, MongoDatabase database, SourceConfigCipher cipher) {
        this(client, database, SourceConfigCipherProvider.fixed(Objects.requireNonNull(cipher, "cipher")));
    }

    MongoConfigurationArtifactStore(MongoClient client, MongoDatabase database, SourceConfigCipherProvider ciphers) {
        this.client = Objects.requireNonNull(client, "client");
        Objects.requireNonNull(database, "database");
        this.sources = durable(SystemCollections.SOURCES.on(database));
        this.pipelines = durable(SystemCollections.PIPELINES.on(database));
        this.versions = durable(SystemCollections.CONFIGURATION_VERSION.on(database));
        this.codec = new EncryptedArtifactCodec(Objects.requireNonNull(ciphers, "ciphers"));
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
            // Evaluate all dependencies against one snapshot before applying any sibling write.
            for (ArtifactWrite write : requested) {
                for (Map.Entry<String, String> expected : write.readPreconditions().entrySet()) {
                    Document observed = find(session, expected.getKey());
                    if (observed == null || !expected.getValue().equals(observed.getString("contentHash"))) {
                        throw new Refused(expected.getKey(), ArtifactMutation.VERSION_CONFLICT);
                    }
                }
            }
            for (ArtifactWrite write : requested) write(session, write);
            return ArtifactBatchWrite.applied();
        }, refused -> ArtifactBatchWrite.refused(refused.id, refused.outcome));
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
                        contendOnRevision(session);
                        return body.apply(session);
                    }, TRANSACTION);
                } catch (Refused refused) {
                    // The driver aborts the transaction before returning a conditional refusal.
                    return refusal.apply(refused);
                }
            }
        });
    }

    private void contendOnRevision(ClientSession session) {
        Document current = versions.find(session, new Document("_id", REVISION_ID)).first();
        if (current == null || !(current.get("revision") instanceof Long revision)
                || revision < 0 || revision == Long.MAX_VALUE) {
            throw unreadable(REVISION_ID, "revision");
        }
        long matched = versions.updateOne(session,
                new Document("_id", REVISION_ID).append("revision", revision),
                new Document("$inc", new Document("revision", 1L))).getMatchedCount();
        if (matched != 1) throw new IllegalStateException("revision changed within one transaction snapshot");
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
        return readSnapshot(session -> Optional.ofNullable(find(session, id)).map(codec::decode));
    }

    @Override
    public List<Resource> list() {
        return readSnapshot(session -> documents(session).stream().map(codec::decode).toList());
    }

    @Override
    public List<StoredArtifactRecord> listStored() {
        return readSnapshot(session -> documents(session).stream().map(codec::browse).toList());
    }

    @Override
    public List<StoredArtifactRecord> listStored(String kind) {
        Objects.requireNonNull(kind, "kind");
        if (!kind.equals("source") && !kind.equals("pipeline")) return List.of();
        return readSnapshot(session -> read(session, collection(kind), kind).stream().map(codec::browse).toList());
    }

    /** Verifies encrypted Source bodies at open without retaining the entire inventory in memory. */
    void verifySourceEnvelopes() {
        readSnapshot(session -> {
            try (MongoCursor<Document> cursor = sources.find(session).iterator()) {
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
        Document source = sources.find(session, new Document("_id", id)).first();
        Document pipeline = pipelines.find(session, new Document("_id", id)).first();
        if (source != null) requireKind(source, "source");
        if (pipeline != null) requireKind(pipeline, "pipeline");
        if (source != null && pipeline != null) throw unreadable(id, "identity");
        return source == null ? pipeline : source;
    }

    private List<Document> documents(ClientSession session) {
        List<Document> documents = new ArrayList<>(read(session, sources, "source"));
        documents.addAll(read(session, pipelines, "pipeline"));
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

    private static void requireKind(Document document, String kind) {
        if (!kind.equals(document.get("kind"))) throw unreadable(String.valueOf(document.get("_id")), "kind");
    }

    private MongoCollection<Document> collection(String kind) {
        return switch (kind) {
            case "source" -> sources;
            case "pipeline" -> pipelines;
            default -> throw new IllegalArgumentException("configuration artifact must be a Source or Pipeline");
        };
    }

    private static TapstateException unreadable(String id, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id, "field", field), null);
    }

    /** Internal transaction control: a refused condition must abort sibling writes and the revision. */
    private static final class Refused extends RuntimeException {
        private final String id;
        private final ArtifactMutation outcome;

        private Refused(String id, ArtifactMutation outcome) {
            super(null, null, false, false);
            this.id = id;
            this.outcome = outcome;
        }
    }
}
