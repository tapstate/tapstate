package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StoredArtifactRecord;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The MongoDB artifact truth layer: stores each applied resource as one document keyed by the
 * resource's top-level id, holding the resource's canonical structure and its content hash. Reading
 * binds that structure straight back to the model and parses no text on the way, so the canonical
 * form is something the store renders on request rather than something it keeps.
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
                    rows.add(toStoredArtifactRecord(cursor.next()));
                }
            }
            return rows;
        });
    }

    /** Maps a resource to its stored id, kind, canonical structure, and content hash. */
    static Document toDocument(Resource artifact) {
        return new Document("_id", artifact.id())
                .append("kind", artifact.kind())
                .append("body", new Document(WRITER.tree(artifact)))
                .append("contentHash", CanonicalHash.of(artifact));
    }

    /** Binds a resource back out of its stored document's structure, parsing no text. */
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
