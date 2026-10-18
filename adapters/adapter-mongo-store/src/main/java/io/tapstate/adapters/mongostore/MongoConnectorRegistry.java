package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import org.bson.BsonObjectId;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * The MongoDB connector distribution registry: stores each registered connector artifact in a GridFS
 * bucket keyed by the content hash of its bytes (the GridFS filename), carrying its identity — connector
 * id, declared PDK API version, and registration source — in the file's metadata.
 *
 * <p>Registration is content-hash idempotent: the hash is computed from the bytes, and a re-register of
 * bytes whose hash is already stored is found here and returns a no-op outcome, so a startup seed sweep
 * and an explicit runtime register share one path without ever storing a second copy. Keying identity
 * and bytes in one GridFS file means a registration and its bytes are never half-present. Driver IO
 * failures are translated into coded io diagnostics and a file whose metadata cannot be reconstructed is
 * surfaced as {@code io.document-unreadable}, so no driver type escapes the module (rule R3).
 */
public final class MongoConnectorRegistry implements ConnectorRegistry {

    private final GridFSBucket artifacts;

    private final MongoCollection<Document> chunks;
    private final String filesNamespace;
    private final String contentHashIndex;

    /** Binds the immutable bucket and verifies its content-hash constraint before any registration. */
    public MongoConnectorRegistry(MongoDatabase database) {
        Objects.requireNonNull(database, "database");
        SystemCollections collection = SystemCollections.CONNECTOR_ARTIFACTS;
        this.artifacts = collection.bucketOn(database);
        this.chunks = collection.chunksOn(database);
        MongoCollection<Document> files = collection.indexTargetOn(database);
        this.filesNamespace = files.getNamespace().getFullName();
        SystemCollections.IndexSpec index = collection.indexes().stream()
                .filter(spec -> spec.keys().equals(List.of("filename")) && spec.unique())
                .findFirst().orElseThrow(() -> new IllegalStateException("content-hash constraint is undeclared"));
        this.contentHashIndex = index.indexName();
        StoreIo.run(() -> {
            Document existing = IndexEnsure.existing(files, index);
            if (existing != null && (!existing.getBoolean("unique", false)
                    || !new Document("filename", 1).equals(existing.get("key"))
                    || existing.getBoolean("sparse", false)
                    || existing.containsKey("partialFilterExpression"))) {
                throw unreadable(filesNamespace + "." + contentHashIndex, "index");
            }
            if (existing == null) {
                Document duplicate = files.aggregate(List.of(
                        Aggregates.group("$filename", Accumulators.sum("count", 1)),
                        Aggregates.match(Filters.gt("count", 1)), Aggregates.limit(1))).first();
                if (duplicate != null) {
                    // Refuse existing corruption without choosing a winner or deleting any bytes.
                    throw unreadable(String.valueOf(duplicate.get("_id")), "filename");
                }
            }
            IndexEnsure.ensure(database, files, index);
        });
    }

    @Override
    public RegistrationOutcome register(
            String connectorId, String pdkApiVersion, RegistrationSource source, byte[] artifact) {
        Objects.requireNonNull(connectorId, "connectorId");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(artifact, "artifact");
        String contentHash = sha256Hex(artifact);
        return StoreIo.call(() -> {
            // register-if-absent: the bytes are stored under their content hash (the filename), so an
            // already-registered artifact is found here and the call is a no-op returning what is stored.
            GridFSFile existing = uniqueArtifact(contentHash);
            if (existing != null) {
                return new RegistrationOutcome(toRegistration(contentHash, existing.getMetadata()), false);
            }
            GridFSUploadOptions options = new GridFSUploadOptions().metadata(metadata(connectorId, pdkApiVersion, source));
            // Each contender owns a different upload id. A shared deterministic id would let the
            // driver's failed-upload cleanup remove another contender's chunks.
            BsonObjectId attempt = new BsonObjectId(new ObjectId());
            GridFSUploadStream upload = artifacts.openUploadStream(attempt, contentHash, options);
            try {
                upload.write(artifact, 0, artifact.length);
            } catch (MongoException failure) {
                // No file insert has been attempted while writing. This upload owns every such chunk.
                upload.abort();
                throw failure;
            }
            try {
                upload.close();
            } catch (MongoWriteException e) {
                // A definitive write rejection did not publish this file, regardless of its index.
                // Ambiguous close failures are deliberately not caught here: their file insert may
                // have committed, so deleting chunks after a missing-file read could corrupt it.
                chunks.deleteMany(Filters.eq("files_id", attempt));
                if (!isContentHashDuplicate(e, contentHash)) {
                    throw e;
                }
                GridFSFile winner = uniqueArtifact(contentHash);
                if (winner == null || winner.getId().equals(attempt)) {
                    throw e;
                }
                // Cleanup above must succeed before this race can be reported as idempotent.
                return new RegistrationOutcome(toRegistration(contentHash, winner.getMetadata()), false);
            }
            return new RegistrationOutcome(
                    new ConnectorRegistration(connectorId, contentHash, pdkApiVersion, source), true);
        });
    }

    @Override
    public List<ConnectorRegistration> list() {
        return StoreIo.call(() -> {
            List<ConnectorRegistration> all = new ArrayList<>();
            try (MongoCursor<GridFSFile> cursor = artifacts.find().iterator()) {
                while (cursor.hasNext()) {
                    GridFSFile file = cursor.next();
                    all.add(toRegistration(file.getFilename(), file.getMetadata()));
                }
            }
            return all;
        });
    }

    @Override
    public List<ConnectorRegistration> findAll(String connectorId) {
        Objects.requireNonNull(connectorId, "connectorId");
        return StoreIo.call(() -> {
            // Queried on the identity carried in the file's metadata, so the answer costs one lookup and
            // depends on no other stored artifact: a registration that cannot be reconstructed fails the
            // question about that connector alone, never every connector at once.
            //
            // Ordered by the content hash, which is the filename, so repeated calls agree on the order
            // they report an id's artifacts in. Normally there is one; where there are two, a caller told
            // them in storage order could be told something different on the next call.
            List<ConnectorRegistration> found = new ArrayList<>();
            try (MongoCursor<GridFSFile> cursor = artifacts.find(Filters.eq("metadata.connectorId", connectorId))
                    .sort(new Document("filename", 1))
                    .iterator()) {
                while (cursor.hasNext()) {
                    GridFSFile file = cursor.next();
                    found.add(toRegistration(file.getFilename(), file.getMetadata()));
                }
            }
            return found;
        });
    }

    @Override
    public Optional<byte[]> artifact(String contentHash) {
        Objects.requireNonNull(contentHash, "contentHash");
        return StoreIo.call(() -> {
            GridFSFile file = uniqueArtifact(contentHash);
            if (file == null) {
                return Optional.empty();
            }
            ExactArtifactBytes bytes = new ExactArtifactBytes(contentHash, file.getLength());
            try {
                artifacts.downloadToStream(file.getObjectId(), bytes);
            } catch (MongoException failure) {
                // The driver wraps stream failures; restore only this sink's exact own rejection.
                if (bytes.writeFailure != null && failure.getCause() == bytes.writeFailure) throw bytes.writeFailure;
                throw failure;
            }
            return Optional.of(bytes.complete());
        });
    }

    /** One exact allocation avoids both geometric expansion and a second full-size return copy. */
    private static final class ExactArtifactBytes extends OutputStream {
        private final String contentHash;
        private final byte[] bytes;
        private int position;
        private RuntimeException writeFailure;

        private ExactArtifactBytes(String contentHash, long length) {
            // Stay within the conservative Java array bound, including the VM header reserve.
            if (length < 0 || length > Integer.MAX_VALUE - 8L) throw unreadable(contentHash, "artifact");
            this.contentHash = contentHash;
            try {
                this.bytes = new byte[(int) length];
            } catch (OutOfMemoryError unavailable) {
                // The stored length is untrusted. Translate only failure of this exact allocation,
                // not unrelated VM errors from reading, hashing, or other registry operations.
                throw unreadable(contentHash, "artifact");
            }
        }

        @Override
        public void write(int value) {
            if (position == bytes.length) throw reject(unreadable(contentHash, "artifact"));
            bytes[position++] = (byte) value;
        }

        @Override
        public void write(byte[] source, int offset, int length) {
            try {
                Objects.checkFromIndexSize(offset, length, source.length);
            } catch (RuntimeException failure) {
                throw reject(failure);
            }
            if (length > bytes.length - position) throw reject(unreadable(contentHash, "artifact"));
            System.arraycopy(source, offset, bytes, position, length);
            position += length;
        }

        private RuntimeException reject(RuntimeException failure) {
            writeFailure = failure;
            return failure;
        }

        private byte[] complete() {
            if (position != bytes.length) throw unreadable(contentHash, "artifact");
            return bytes;
        }
    }

    /** The identity carried in a stored artifact's GridFS metadata (the content hash is the filename). */
    static Document metadata(String connectorId, String pdkApiVersion, RegistrationSource source) {
        return new Document("connectorId", connectorId)
                .append("pdkApiVersion", pdkApiVersion)
                .append("source", source.name());
    }

    /** Reconstructs a registration from a content hash and the stored metadata, or fails coded if unreadable. */
    static ConnectorRegistration toRegistration(String contentHash, Document metadata) {
        if (metadata == null) {
            throw unreadable(contentHash);
        }
        String connectorId = metadata.getString("connectorId");
        String sourceName = metadata.getString("source");
        if (connectorId == null || sourceName == null) {
            // A stored artifact missing its identity is registry corruption, surfaced as a coded io
            // diagnostic rather than a bare null-argument crash while reconstructing.
            throw unreadable(contentHash);
        }
        RegistrationSource source;
        try {
            source = RegistrationSource.valueOf(sourceName);
        } catch (IllegalArgumentException e) {
            // A stored source that is not a known enum constant is corruption, not silently coerced.
            throw unreadable(contentHash);
        }
        return new ConnectorRegistration(connectorId, contentHash, metadata.getString("pdkApiVersion"), source);
    }

    /** Lower-hex SHA-256 of the artifact bytes: the content-addressed registration key. */
    static String sha256Hex(byte[] bytes) {
        return ContentHash.of(bytes);
    }

    private static TapstateException unreadable(String contentHash) {
        return unreadable(contentHash, "artifact");
    }

    private static TapstateException unreadable(String id, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", String.valueOf(id), "field", field), null);
    }

    @Override
    public boolean hasArtifact(String contentHash) {
        Objects.requireNonNull(contentHash, "contentHash");
        // The same GridFS lookup artifact() does, stopping before the download.
        return StoreIo.call(() -> uniqueArtifact(contentHash)) != null;
    }

    private GridFSFile uniqueArtifact(String contentHash) {
        List<GridFSFile> matches = artifacts.find(Filters.eq("filename", contentHash))
                .limit(2).into(new ArrayList<>());
        if (matches.size() > 1) {
            throw unreadable(contentHash, "filename");
        }
        return matches.isEmpty() ? null : matches.getFirst();
    }

    /** Only the exact declared filename constraint's duplicate is an idempotent registration race. */
    private boolean isContentHashDuplicate(MongoWriteException failure, String contentHash) {
        if (failure.getError().getCategory() != ErrorCategory.DUPLICATE_KEY) return false;
        BsonDocument details = failure.getError().getDetails();
        if (details.containsKey("keyPattern") || details.containsKey("keyValue")) {
            // Structured fields are authoritative, including evidence contradicting message text.
            return BsonDocument.parse("{filename: 1}").equals(details.get("keyPattern"))
                    && new BsonDocument("filename", new BsonString(contentHash)).equals(details.get("keyValue"));
        }
        // Older endpoints omit structured fields and vary the dup-key value's prose. Match the
        // bounded namespace/index tokens, never the entire message or an index-name substring.
        String message = failure.getError().getMessage();
        return Pattern.compile("(?:^|\\s)collection:\\s+" + Pattern.quote(filesNamespace) + "(?=\\s|$)")
                .matcher(message).find()
                && Pattern.compile("(?:^|\\s)index:\\s+" + Pattern.quote(contentHashIndex) + "(?=\\s|$)")
                .matcher(message).find();
    }
}
