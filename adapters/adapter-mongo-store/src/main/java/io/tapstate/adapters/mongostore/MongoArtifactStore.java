package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactIdentity;
import io.tapstate.spi.store.ArtifactBatchWrite;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ArtifactWrite;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StoredArtifactRecord;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * The MongoDB artifact truth layer: stores each applied resource as one document keyed by the
 * resource's top-level id, holding the resource's canonical structure and its logical content hash.
 * A Source is the physical exception inside that structure: its whole config is one authenticated
 * ciphertext envelope, decrypted only while reconstructing the in-memory model. Reading binds the
 * resulting structure straight back to the model and parses no canonical text on the way, so the
 * canonical form is something the store renders on request rather than something it keeps.
 *
 * <p>The document carries the id (as {@code _id}), the kind, the structure (as {@code body}), and the
 * content hash. Kind is kept beside the body rather than only inside it because it is what a read by
 * kind filters on, and an index cannot be asked to reach into a body a query never looks at. A batch
 * is written in one multi-document transaction, so a mid-batch write failure aborts the whole
 * transaction and leaves no partial batch behind; the transaction is why the store binds a
 * replica-set.
 *
 * <p>Driver IO failures are translated into coded io diagnostics so no driver type escapes the module
 * (rule R3), and a stored body this build cannot bind is surfaced the same way, naming the field it
 * failed at. A failure that is not about the document — a bug in the read mapping — is left to crash
 * bare: coding it would file a defect of ours under storage corruption, where nobody would look for it.
 */
public final class MongoArtifactStore implements ArtifactStore {

    private static final CanonicalWriter WRITER = new CanonicalWriter();
    private static final DslParser PARSER = new DslParser();
    private static final TransactionOptions DURABLE_WRITE = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();

    private final MongoClient client;
    private final MongoCollection<Document> collection;
    private final EncryptedArtifactCodec codec;
    private final SourceConfigCipherProvider ciphers;

    public MongoArtifactStore(
            MongoClient client, MongoCollection<Document> collection, SourceConfigCipher cipher) {
        this(client, collection, SourceConfigCipherProvider.fixed(Objects.requireNonNull(cipher, "cipher")));
    }

    MongoArtifactStore(
            MongoClient client, MongoCollection<Document> collection, SourceConfigCipherProvider ciphers) {
        this.client = Objects.requireNonNull(client, "client");
        this.collection = Objects.requireNonNull(collection, "collection")
                .withReadPreference(ReadPreference.primary()).withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
        this.ciphers = Objects.requireNonNull(ciphers, "ciphers");
        this.codec = new EncryptedArtifactCodec(ciphers);
    }

    @Override
    public ArtifactMutation create(Resource artifact) {
        Objects.requireNonNull(artifact, "artifact");
        if (needsWriteFence(artifact)) {
            ArtifactBatchWrite result = writeAll(List.of(ArtifactWrite.createOnly(artifact)));
            return result.appliedSuccessfully() ? ArtifactMutation.CREATED : result.refusal();
        }
        return StoreIo.call(() -> {
            try {
                collection.insertOne(createdDocument(artifact, null));
                return ArtifactMutation.CREATED;
            } catch (MongoException e) {
                if (ErrorCategory.fromErrorCode(e.getCode()) == ErrorCategory.DUPLICATE_KEY
                        && idExistsAfterDuplicate(artifact.id())) {
                    return ArtifactMutation.ALREADY_EXISTS;
                }
                throw e;
            }
        });
    }

    @Override
    public ArtifactMutation replace(String id, String expectedContentHash, Resource replacement) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(expectedContentHash, "expectedContentHash");
        Objects.requireNonNull(replacement, "replacement");
        if (!id.equals(replacement.id())) {
            throw new IllegalArgumentException("replacement id must equal the artifact id");
        }
        if (needsWriteFence(replacement)) {
            ArtifactBatchWrite result = writeAll(List.of(ArtifactWrite.replaceOnly(replacement, expectedContentHash)));
            return result.appliedSuccessfully() ? ArtifactMutation.REPLACED : result.refusal();
        }
        return StoreIo.call(() -> {
            Document filter = new Document("_id", id).append("contentHash", expectedContentHash);
            Document stored = collection.find(new Document("_id", id)).first();
            if (stored != null) { filter.append("incarnation", identityOf(stored).incarnation()); }
            if (collection.updateOne(filter, replacementPipeline(codec.encode(replacement))).getMatchedCount() == 1) {
                return ArtifactMutation.REPLACED;
            }
            return collection.find(new Document("_id", id)).first() == null
                    ? ArtifactMutation.NOT_FOUND
                    : ArtifactMutation.VERSION_CONFLICT;
        });
    }

    @Override
    public ArtifactMutation delete(String id, String expectedContentHash) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(expectedContentHash, "expectedContentHash");
        return StoreIo.call(() -> {
            Document filter = new Document("_id", id).append("contentHash", expectedContentHash);
            if (collection.findOneAndDelete(filter) != null) {
                return ArtifactMutation.DELETED;
            }
            return collection.find(new Document("_id", id)).first() == null
                    ? ArtifactMutation.NOT_FOUND
                    : ArtifactMutation.VERSION_CONFLICT;
        });
    }

    @Override
    public ArtifactBatchWrite writeAll(List<ArtifactWrite> writes) {
        Objects.requireNonNull(writes, "writes");
        if (writes.isEmpty()) {
            return ArtifactBatchWrite.applied();
        }
        if (writes.size() == 1 && writes.getFirst().readPreconditions().isEmpty()
                && !needsWriteFence(writes.getFirst().resource())) {
            return singleWrite(writes.getFirst());
        }
        return StoreIo.call(() -> writeTransactionally(writes));
    }

    private ArtifactBatchWrite singleWrite(ArtifactWrite write) {
        ArtifactMutation outcome = switch (write.intent()) {
            case CREATE_ONLY -> create(write.resource());
            case REPLACE_ONLY -> replace(write.resource().id(), write.expectedContentHash(), write.resource());
            case UPSERT -> {
                save(write.resource());
                yield ArtifactMutation.REPLACED;
            }
        };
        return switch (outcome) {
            case CREATED, REPLACED -> ArtifactBatchWrite.applied();
            case NOT_FOUND, ALREADY_EXISTS, VERSION_CONFLICT ->
                    ArtifactBatchWrite.refused(write.resource().id(), outcome);
            case DELETED -> throw new IllegalStateException("artifact write cannot report deletion");
        };
    }

    private ArtifactBatchWrite writeTransactionally(List<ArtifactWrite> writes) {
        boolean fenced = writes.stream().anyMatch(write -> needsWriteFence(write.resource()));
        Map<String, String> observedReplacementIncarnations = new HashMap<>();
        return retryTransaction(fenced, () -> writeTransactionOnce(writes, observedReplacementIncarnations));
    }

    private static <T> T retryTransaction(boolean fenced, Supplier<T> transaction) {
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                return transaction.get();
            } catch (RuntimeException error) {
                if (!fenced || attempt == 2 || !retryableKeyringWrite(error)) throw error;
            }
        }
        throw new IllegalStateException("transaction attempts must return or throw");
    }

    /** Rewrites only the observed Source envelope, preserving all unrelated raw document fields. */
    ArtifactMutation reencryptSource(Document observed) {
        Resource resource = codec.decode(observed);
        if (!(resource instanceof SourceResource)) throw new IllegalStateException("re-encryption requires a Source");
        Document filter = new Document("_id", observed.get("_id"))
                .append("contentHash", observed.get("contentHash"))
                .append("body.config", observed.get("body", Document.class).get("config"));
        if (observed.containsKey("incarnation")) { filter.append("incarnation", identityOf(observed).incarnation()); }
        return StoreIo.call(() -> retryTransaction(needsWriteFence(resource), () -> {
            try (ClientSession session = client.startSession()) {
                session.startTransaction(DURABLE_WRITE);
                try {
                    String replacement = codec.encode(resource, session).get("body", Document.class).getString("config");
                    if (collection.updateOne(session, filter,
                            new Document("$set", new Document("body.config", replacement))).getMatchedCount() == 0) {
                        boolean absent = collection.find(session, new Document("_id", observed.get("_id"))).first() == null;
                        session.abortTransaction();
                        return absent ? ArtifactMutation.NOT_FOUND : ArtifactMutation.VERSION_CONFLICT;
                    }
                } catch (RuntimeException error) {
                    try {
                        session.abortTransaction();
                    } catch (RuntimeException abortFailure) {
                        error.addSuppressed(abortFailure);
                    }
                    throw error;
                }
                session.commitTransaction();
                return ArtifactMutation.REPLACED;
            }
        }));
    }

    private boolean needsWriteFence(Resource resource) {
        return resource instanceof SourceResource && ciphers.requiresWriteFence();
    }

    private static boolean retryableKeyringWrite(RuntimeException error) {
        if (error instanceof TapstateException coded) {
            return coded.code() == StoreError.SOURCE_CONFIG_KEYRING_NOT_READY;
        }
        return error instanceof MongoException driver && driver.hasErrorLabel("TransientTransactionError")
                && !driver.hasErrorLabel("UnknownTransactionCommitResult");
    }

    private ArtifactBatchWrite writeTransactionOnce(
            List<ArtifactWrite> writes, Map<String, String> observedReplacementIncarnations) {
        Set<String> insertedThisAttempt = new HashSet<>();
        try (ClientSession session = client.startSession()) {
            session.startTransaction(DURABLE_WRITE);
            try {
                for (ArtifactWrite write : writes) {
                    String stale = firstStalePrecondition(session, write.readPreconditions());
                    if (stale != null) {
                        session.abortTransaction();
                        return ArtifactBatchWrite.refused(stale, ArtifactMutation.VERSION_CONFLICT);
                    }
                }
                for (ArtifactWrite write : writes) {
                    ArtifactBatchWrite refusal = writeOne(session, write, observedReplacementIncarnations, insertedThisAttempt);
                    if (!refusal.appliedSuccessfully()) {
                        if (session.hasActiveTransaction()) session.abortTransaction();
                        return refusal;
                    }
                }
            } catch (RuntimeException error) {
                try {
                    if (session.hasActiveTransaction()) session.abortTransaction();
                } catch (RuntimeException abortFailure) {
                    error.addSuppressed(abortFailure);
                }
                throw error;
            }
            // A commit result may be ambiguous; do not turn it into an unconditional write retry.
            session.commitTransaction();
            return ArtifactBatchWrite.applied();
        }
    }

    private ArtifactBatchWrite writeOne(ClientSession session, ArtifactWrite write,
            Map<String, String> observedReplacementIncarnations, Set<String> insertedThisAttempt) {
        return switch (write.intent()) {
            case CREATE_ONLY -> insertOnly(session, write, insertedThisAttempt);
            case REPLACE_ONLY -> replaceOnly(session, write, observedReplacementIncarnations, insertedThisAttempt);
            case UPSERT -> {
                upsert(session, write.resource(), insertedThisAttempt);
                yield ArtifactBatchWrite.applied();
            }
        };
    }

    private ArtifactBatchWrite insertOnly(ClientSession session, ArtifactWrite write, Set<String> insertedThisAttempt) {
        try {
            collection.insertOne(session, createdDocument(write.resource(), session));
            insertedThisAttempt.add(write.resource().id());
            return ArtifactBatchWrite.applied();
        } catch (MongoException error) {
            if (ErrorCategory.fromErrorCode(error.getCode()) == ErrorCategory.DUPLICATE_KEY) {
                // The failed transaction must end before observing committed ids: its own earlier
                // batch writes must not supply the evidence for a create-only condition refusal.
                try {
                    if (session.hasActiveTransaction()) session.abortTransaction();
                } catch (MongoException abortFailure) {
                    error.addSuppressed(abortFailure);
                    throw error;
                }
                if (idExistsAfterDuplicate(write.resource().id())) {
                    return ArtifactBatchWrite.refused(write.resource().id(), ArtifactMutation.ALREADY_EXISTS);
                }
            }
            throw error;
        }
    }

    private boolean idExistsAfterDuplicate(String id) {
        // The driver retains errInfo but can discard the server's duplicate keyPattern. A 11000
        // alone does not establish an id collision. Observe only the committed id, without decoding
        // an existing Source. This judges the current create-only condition, not the discarded index
        // metadata; an absent id or failed observation preserves the original store fault.
        try {
            return collection.find(new Document("_id", id)).projection(new Document("_id", 1)).first() != null;
        } catch (MongoException observationFailure) {
            return false;
        }
    }

    private ArtifactBatchWrite replaceOnly(ClientSession session, ArtifactWrite write,
            Map<String, String> observedReplacementIncarnations, Set<String> insertedThisAttempt) {
        Document filter = new Document("_id", write.resource().id())
                .append("contentHash", write.expectedContentHash());
        Document current = collection.find(session, new Document("_id", write.resource().id())).first();
        if (current == null) {
            return ArtifactBatchWrite.refused(write.resource().id(), ArtifactMutation.NOT_FOUND);
        }
        String currentIncarnation = identityOf(current).incarnation();
        String observedIncarnation = observedReplacementIncarnations.get(write.resource().id());
        if (observedIncarnation != null && !observedIncarnation.equals(currentIncarnation)) {
            return ArtifactBatchWrite.refused(write.resource().id(), ArtifactMutation.VERSION_CONFLICT);
        }
        // A retry may refresh the keyring, but cannot adopt a different durable artifact lifetime.
        // A resource inserted earlier in this attempt has no committed incarnation to freeze yet.
        if (!insertedThisAttempt.contains(write.resource().id())) {
            observedReplacementIncarnations.putIfAbsent(write.resource().id(), currentIncarnation);
        }
        filter.append("incarnation", currentIncarnation);
        if (collection.updateOne(session, filter, replacementPipeline(codec.encode(write.resource(), session))).getMatchedCount() == 1) {
            return ArtifactBatchWrite.applied();
        }
        return collection.find(session, new Document("_id", write.resource().id())).first() == null
                ? ArtifactBatchWrite.refused(write.resource().id(), ArtifactMutation.NOT_FOUND)
                : ArtifactBatchWrite.refused(write.resource().id(), ArtifactMutation.VERSION_CONFLICT);
    }

    @Override
    public void saveAll(List<Resource> artifacts) {
        saveAll(artifacts, Map.of());
    }

    @Override
    public Optional<String> saveAll(List<Resource> artifacts, Map<String, String> expectedContentHashes) {
        Objects.requireNonNull(artifacts, "artifacts");
        Objects.requireNonNull(expectedContentHashes, "expectedContentHashes");
        if (artifacts.isEmpty()) {
            // An empty batch writes nothing, and opens no transaction. A precondition declared against a
            // batch that writes nothing has nothing to guard, so it is not read either.
            return Optional.empty();
        }
        // The batch is one atomic unit: every upsert runs inside a single multi-document transaction, so
        // a failure on any one write aborts the whole transaction and no partial batch is stored. Each
        // upsert is by the top-level id (the document _id) — a full replacement that overwrites in place
        // rather than accumulating documents.
        //
        // The declared versions are compared inside that same transaction, ahead of the writes. Reading
        // them here rather than before it is the whole point: a comparison outside the transaction is a
        // check-then-act, and the write that follows would happily overwrite a version that landed in
        // between. Inside it, the documents compared are the documents written, so a concurrent writer
        // either loses the write conflict or is seen by the comparison.
        List<ArtifactWrite> writes = new ArrayList<>(artifacts.stream().map(ArtifactWrite::upsert).toList());
        writes.set(0, writes.getFirst().guardedBy(expectedContentHashes));
        ArtifactBatchWrite result = StoreIo.call(() -> writeTransactionally(writes));
        if (result.appliedSuccessfully()) return Optional.empty();
        if (result.refusal() != ArtifactMutation.VERSION_CONFLICT) {
            throw new IllegalStateException("unconditional upserts can only be refused by their read preconditions");
        }
        return Optional.of(result.refusedId());
    }

    /**
     * The first id whose stored content hash is not the one declared against it, or null when every
     * declared version still holds. An id with no stored document is stale too: it has no version at
     * all, so it cannot be the one the caller read.
     *
     * <p>Only the hash field is read — the canonical body is never reconstructed here, so a sibling the
     * running version cannot parse does not veto a batch that merely declares a version of it.
     */
    private String firstStalePrecondition(ClientSession session, Map<String, String> expectedContentHashes) {
        for (Map.Entry<String, String> expected : expectedContentHashes.entrySet()) {
            Document stored = collection
                    .find(session, new Document("_id", expected.getKey()))
                    .projection(new Document("contentHash", 1))
                    .first();
            if (stored == null || !expected.getValue().equals(stored.getString("contentHash"))) {
                return expected.getKey();
            }
        }
        return null;
    }

    @Override
    public Optional<Resource> get(String id) {
        Objects.requireNonNull(id, "id");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", id)).first());
        return document == null ? Optional.empty() : Optional.of(codec.decode(document));
    }

    @Override
    public Optional<ArtifactIdentity> identity(String id) {
        Objects.requireNonNull(id, "id");
        Document stored = StoreIo.call(() -> collection.find(new Document("_id", id))
                .projection(new Document("incarnation", 1).append("contentHash", 1)).first());
        return Optional.ofNullable(stored).map(MongoArtifactStore::identityOf);
    }

    static ArtifactIdentity identityOf(Document stored) {
        String id = String.valueOf(stored.get("_id"));
        String incarnation = text(stored, "incarnation");
        String hash = text(stored, "contentHash");
        if (incarnation == null || incarnation.isBlank() || hash == null || hash.isBlank()) {
            throw unreadable(id, incarnation == null || incarnation.isBlank() ? "incarnation" : "contentHash", null);
        }
        return new ArtifactIdentity(id, incarnation, hash);
    }

    private Document createdDocument(Resource artifact, ClientSession session) {
        return codec.encode(artifact, session).append("incarnation", UUID.randomUUID().toString());
    }

    /** Legacy structural mapping for codec tests; physical writes pass an already encrypted document. */
    static List<Document> replacementPipeline(Resource artifact) {
        return replacementPipeline(toDocument(artifact));
    }

    /** Preserves the durable incarnation and unrelated metadata while replacing the encoded resource. */
    private static List<Document> replacementPipeline(Document encoded) {
        Document next = new Document(encoded);
        next.remove("_id");
        Document fields = new Document();
        next.forEach((key, value) -> fields.put(key, new Document("$literal", value)));
        return List.of(new Document("$set", fields));
    }

    private void upsert(ClientSession session, Resource artifact, Set<String> insertedThisAttempt) {
        Document existing = collection.find(session, new Document("_id", artifact.id())).first();
        if (existing == null) {
            collection.insertOne(session, createdDocument(artifact, session));
            insertedThisAttempt.add(artifact.id());
        } else {
            Document exact = new Document("_id", artifact.id()).append("incarnation", identityOf(existing).incarnation());
            if (collection.updateOne(session, exact, replacementPipeline(codec.encode(artifact, session))).getMatchedCount() != 1) {
                throw new IllegalStateException("an artifact changed inside its write transaction");
            }
        }
    }

    @Override
    public List<Resource> list() {
        // A reconstruction failure (io.document-unreadable) passes through the driver-failure
        // translation untouched, and the explicitly-closed cursor is released even on that path — a
        // for-each over the iterable would not close it on the exception path.
        return StoreIo.call(() -> {
            List<Resource> resources = new ArrayList<>();
            try (MongoCursor<Document> cursor = collection.find().iterator()) {
                while (cursor.hasNext()) {
                    resources.add(codec.decode(cursor.next()));
                }
            }
            return resources;
        });
    }

    @Override
    public List<StoredArtifactRecord> listStored(String kind) {
        Objects.requireNonNull(kind, "kind");
        // Pushed to the store rather than filtered here: kind is an indexed field, and reading every
        // document to discard most of them is the cost this layout exists to remove.
        return browse(collection.find(new Document("kind", kind)));
    }

    @Override
    public List<StoredArtifactRecord> listStored() {
        return browse(collection.find());
    }

    /**
     * The browse projection over a query: document metadata read and the body tested one row at a time.
     * A body this build cannot bind is retained as an unreadable row instead of aborting the whole
     * inventory; get() and the strict resource list still surface the coded IO error.
     */
    private List<StoredArtifactRecord> browse(FindIterable<Document> found) {
        return StoreIo.call(() -> {
            List<StoredArtifactRecord> rows = new ArrayList<>();
            try (MongoCursor<Document> cursor = found.iterator()) {
                while (cursor.hasNext()) {
                    rows.add(codec.browse(cursor.next()));
                }
            }
            return rows;
        });
    }

    /** Legacy plaintext structural mapping retained for migrations and codec-focused tests. */
    static Document toDocument(Resource artifact) {
        return new Document("_id", artifact.id())
                .append("kind", artifact.kind())
                .append("body", new Document(WRITER.tree(artifact)))
                .append("contentHash", CanonicalHash.of(artifact));
    }

    /** Legacy plaintext structural reader retained for migrations and codec-focused tests. */
    static Resource toResource(Document document) {
        String id = String.valueOf(document.get("_id"));
        if (!(document.get("body") instanceof Document body)) {
            throw unreadable(id, "body", null);
        }
        try {
            return PARSER.fromTree(body);
        } catch (DslException e) {
            // A stored body this build cannot bind — corruption, or a shape written by a newer grammar
            // — is a storage-layer failure, surfaced as an io diagnostic naming the field it failed at
            // rather than as an authoring code for a document the user never wrote. Only this type is
            // caught: everything else is a fault in the mapping rather than in the document.
            throw unreadable(id, e.path().isEmpty() ? "body" : e.path(), e);
        }
    }

    /**
     * Maps one stored document to the tolerant browse projection. A body this build cannot bind keeps
     * its row, with no canonical form to show: the form is rendered from the model, so a body that does
     * not become a model has none. The document itself is still there to be inspected in the store.
     */
    static StoredArtifactRecord toStoredArtifactRecord(Document document) {
        String id = String.valueOf(document.get("_id"));
        String kind = text(document, "kind");
        if (kind == null) {
            kind = "unknown";
        }
        String contentHash = text(document, "contentHash");
        if (document.get("body") instanceof Document body) {
            try {
                return new StoredArtifactRecord(id, kind, WRITER.write(PARSER.fromTree(body)), contentHash, true);
            } catch (DslException retained) {
                // Falls through to the unreadable row: one damaged document must not make the whole
                // inventory unavailable. get() and the strict list still surface the coded IO error.
            }
        }
        return new StoredArtifactRecord(id, kind, null, contentHash, false);
    }

    private static TapstateException unreadable(String id, String field, Throwable cause) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id, "field", field), cause);
    }

    private static String text(Document document, String field) {
        Object value = document.get(field);
        return value instanceof String s ? s : null;
    }
}
