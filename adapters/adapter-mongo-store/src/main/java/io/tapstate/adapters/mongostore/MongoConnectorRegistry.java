package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.Filters;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import org.bson.Document;

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

    public MongoConnectorRegistry(GridFSBucket artifacts) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
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
            GridFSFile existing = artifacts.find(Filters.eq("filename", contentHash)).first();
            if (existing != null) {
                return new RegistrationOutcome(toRegistration(contentHash, existing.getMetadata()), false);
            }
            GridFSUploadOptions options = new GridFSUploadOptions().metadata(metadata(connectorId, pdkApiVersion, source));
            artifacts.uploadFromStream(contentHash, new ByteArrayInputStream(artifact), options);
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
            GridFSFile file = artifacts.find(Filters.eq("filename", contentHash)).first();
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
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", String.valueOf(contentHash), "field", "artifact"), null);
    }

    @Override
    public boolean hasArtifact(String contentHash) {
        Objects.requireNonNull(contentHash, "contentHash");
        // The same GridFS lookup artifact() does, stopping before the download.
        return StoreIo.call(() -> artifacts.find(Filters.eq("filename", contentHash)).first()) != null;
    }
}
