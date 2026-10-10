package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.gridfs.GridFSBucket;
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
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
            try {
                artifacts.uploadFromStream(attempt, contentHash, new ByteArrayInputStream(artifact), options);
            } catch (MongoWriteException e) {
                if (!isContentHashDuplicate(e, contentHash)) {
                    throw e;
                }
                GridFSFile winner = uniqueArtifact(contentHash);
                if (winner == null || winner.getId().equals(attempt)) {
                    throw e;
                }
                // The losing file was never published; remove only chunks written by this attempt.
                // A cleanup failure remains an IO failure rather than claiming an idempotent success.
                chunks.deleteMany(Filters.eq("files_id", attempt));
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
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            artifacts.downloadToStream(file.getObjectId(), bytes);
            return Optional.of(bytes.toByteArray());
        });
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
        // The sync driver's WriteError details can be empty. Accept only the endpoint's exact
        // namespace, constraint name and SHA value; an unfamiliar response remains an IO failure.
        String expected = "E11000 duplicate key error collection: " + filesNamespace
                + " index: " + contentHashIndex + " dup key: { filename: \"" + contentHash + "\" }";
        return failure.getError().getCategory() == ErrorCategory.DUPLICATE_KEY
                && expected.equals(failure.getError().getMessage());
    }
}
