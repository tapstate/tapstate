package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactBatchWrite;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ArtifactWrite;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The MongoDB artifact truth layer: stores each applied resource as one document keyed by the
 * resource's top-level id, holding the resource in its canonical form and its canonical content
 * hash. Reading reconstructs the resource from that canonical form through the canonical parser —
 * the inverse of the canonical writer — so the store keeps a single serialization contract and a
 * written artifact reads back to the same canonical form.
 *
 * <p>The document carries the id (as {@code _id}), kind, canonical text, and canonical content hash.
 * A batch is written in one multi-document transaction, so a mid-batch write failure aborts the whole
 * transaction and leaves no partial batch behind; the transaction is why the store binds a
 * replica-set. Driver IO failures during mutations, save, get, or list are translated into coded io
 * diagnostics, and a stored body that no longer reconstructs is surfaced as an io diagnostic rather
 * than a leaked authoring code, so no driver type escapes the module (rule R3).
 */
public final class MongoArtifactStore implements ArtifactStore {

    private static final CanonicalWriter WRITER = new CanonicalWriter();
    private static final DslParser PARSER = new DslParser();

    private final MongoClient client;
    private final MongoCollection<Document> collection;

    public MongoArtifactStore(MongoClient client, MongoCollection<Document> collection) {
        this.client = Objects.requireNonNull(client, "client");
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public ArtifactMutation create(Resource artifact) {
        Objects.requireNonNull(artifact, "artifact");
        return StoreIo.call(() -> {
            try {
                collection.insertOne(toDocument(artifact));
                return ArtifactMutation.CREATED;
            } catch (MongoException e) {
                if (ErrorCategory.fromErrorCode(e.getCode()) == ErrorCategory.DUPLICATE_KEY) {
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
        return StoreIo.call(() -> {
            Document filter = new Document("_id", id).append("contentHash", expectedContentHash);
            if (collection.replaceOne(filter, toDocument(replacement)).getMatchedCount() == 1) {
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
        if (writes.size() == 1) {
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
        try (ClientSession session = client.startSession()) {
            session.startTransaction();
            try {
                for (ArtifactWrite write : writes) {
                    ArtifactBatchWrite refusal = writeOne(session, write);
                    if (!refusal.appliedSuccessfully()) {
                        session.abortTransaction();
                        return refusal;
                    }
                }
                session.commitTransaction();
                return ArtifactBatchWrite.applied();
            } catch (RuntimeException error) {
                try {
                    session.abortTransaction();
                } catch (RuntimeException abortFailure) {
                    error.addSuppressed(abortFailure);
                }
                throw error;
            }
        }
    }

    private ArtifactBatchWrite writeOne(ClientSession session, ArtifactWrite write) {
        return switch (write.intent()) {
            case CREATE_ONLY -> insertOnly(session, write);
            case REPLACE_ONLY -> replaceOnly(session, write);
            case UPSERT -> {
                collection.replaceOne(session, new Document("_id", write.resource().id()), toDocument(write.resource()),
                        new ReplaceOptions().upsert(true));
                yield ArtifactBatchWrite.applied();
            }
        };
    }

    private ArtifactBatchWrite insertOnly(ClientSession session, ArtifactWrite write) {
        try {
            collection.insertOne(session, toDocument(write.resource()));
            return ArtifactBatchWrite.applied();
        } catch (MongoException error) {
            if (ErrorCategory.fromErrorCode(error.getCode()) == ErrorCategory.DUPLICATE_KEY) {
                return ArtifactBatchWrite.refused(write.resource().id(), ArtifactMutation.ALREADY_EXISTS);
            }
            throw error;
        }
    }

    private ArtifactBatchWrite replaceOnly(ClientSession session, ArtifactWrite write) {
        Document filter = new Document("_id", write.resource().id())
                .append("contentHash", write.expectedContentHash());
        if (collection.replaceOne(session, filter, toDocument(write.resource())).getMatchedCount() == 1) {
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
        List<String> conflicted = new ArrayList<>(1);
        StoreIo.run(() -> {
            try (ClientSession session = client.startSession()) {
                session.startTransaction();
                try {
                    String stale = firstStalePrecondition(session, expectedContentHashes);
                    if (stale != null) {
                        conflicted.add(stale);
                        session.abortTransaction();
                        return;
                    }
                    for (Resource artifact : artifacts) {
                        collection.replaceOne(session, new Document("_id", artifact.id()), toDocument(artifact),
                                new ReplaceOptions().upsert(true));
                    }
                } catch (RuntimeException e) {
                    // A write failed before commit: roll the whole batch back and surface the write failure
                    // (StoreIo codes it). If the abort itself fails, keep the original failure as the
                    // surfaced error rather than letting the abort mask it.
                    try {
                        session.abortTransaction();
                    } catch (RuntimeException abortFailure) {
                        e.addSuppressed(abortFailure);
                    }
                    throw e;
                }
                // Commit stands outside the abort guard: once the writes have all succeeded, a commit-time
                // driver failure must propagate to StoreIo to be coded — aborting after commit would throw
                // and mask it. A dangling transaction on any exit path is closed with the session.
                session.commitTransaction();
            }
        });
        return conflicted.isEmpty() ? Optional.empty() : Optional.of(conflicted.get(0));
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
        return document == null ? Optional.empty() : Optional.of(toResource(document));
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
                    resources.add(toResource(cursor.next()));
                }
            }
            return resources;
        });
    }

    /** Maps a resource to its stored id, kind, canonical text, and canonical-content hash. */
    static Document toDocument(Resource artifact) {
        String canonical = WRITER.write(artifact);
        return new Document("_id", artifact.id())
                .append("kind", artifact.kind())
                .append("canonical", canonical)
                .append("contentHash", CanonicalHash.of(canonical));
    }

    /** Reconstructs a resource from its stored document by parsing the canonical body. */
    static Resource toResource(Document document) {
        String id = document.getString("_id");
        String canonical = document.getString("canonical");
        if (canonical == null) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", String.valueOf(id)), null);
        }
        try {
            return PARSER.parse(canonical);
        } catch (RuntimeException e) {
            // A stored body that no longer reconstructs — corruption, or a newer grammar whose kind or
            // shape this version cannot build — is a storage-layer failure, surfaced as an io diagnostic
            // (with the original failure kept as the cause) rather than a leaked authoring code for a
            // document the user never authored. The catch is deliberately broad: a body from a newer
            // grammar can fail as more than a coded parse error (an unsupported kind, say), and all such
            // failures are the same storage-integrity signal.
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", String.valueOf(id)), e);
        }
    }
}
