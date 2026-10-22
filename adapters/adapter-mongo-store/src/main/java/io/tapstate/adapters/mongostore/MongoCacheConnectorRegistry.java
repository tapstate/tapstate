package io.tapstate.adapters.mongostore;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoGridFSException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ArtifactStreams;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.BsonString;
import org.bson.Document;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** Large bytes remain quarantined until a small metadata transaction publishes their original scope. */
final class MongoCacheConnectorRegistry implements ConnectorRegistry {
    private static final String STAGED = "STAGED";
    private static final String PUBLISHED = "PUBLISHED";
    private static final int CHUNK_SIZE = 255 * 1024;
    private static final Set<String> FILE_FIELDS = Set.of("_id", "length", "chunkSize", "uploadDate", "filename", "metadata");
    private static final Set<String> PUBLISHED_METADATA = Set.of("cacheScope", "publication", "contentHash", "connectorId", "pdkApiVersion", "source");
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final CacheBusinessScope scope;
    private final Runnable ownerOpen;
    private final Runnable requireActive;
    private final MongoCacheMetadataAccess access;
    private final GridFSBucket bucket;
    private final MongoCollection<Document> chunks;

    MongoCacheConnectorRegistry(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable ownerOpen, Runnable requireActive) {
        this.client = Objects.requireNonNull(client, "client"); this.scope = Objects.requireNonNull(scope, "scope");
        this.ownerOpen = Objects.requireNonNull(ownerOpen, "ownerOpen"); this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        this.access = new MongoCacheMetadataAccess(client, database, scope, SystemCollections.CACHE_CONNECTOR_ARTIFACTS, this::active);
        this.bucket = SystemCollections.CACHE_CONNECTOR_ARTIFACTS.bucketOn(database).withReadConcern(ReadConcern.MAJORITY)
                .withReadPreference(ReadPreference.primary()).withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
        this.chunks = SystemCollections.CACHE_CONNECTOR_ARTIFACTS.chunksOn(database);
    }
    void verify() { access.verify(); }

    @Override public RegistrationOutcome register(String connectorId, String pdkApiVersion, RegistrationSource source, byte[] artifact) {
        Objects.requireNonNull(connectorId, "connectorId"); Objects.requireNonNull(source, "source");
        byte[] captured = Objects.requireNonNull(artifact, "artifact").clone();
        try {
            return register(connectorId, pdkApiVersion, source, ContentHash.of(captured), captured.length,
                    new ByteArrayInputStream(captured));
        } catch (IOException impossible) { throw new IllegalStateException("In-memory artifact could not be read", impossible); }
    }

    @Override public RegistrationOutcome register(String connectorId, String pdkApiVersion, RegistrationSource source,
                                                  String hash, long length, InputStream artifact) throws IOException {
        Objects.requireNonNull(connectorId, "connectorId"); Objects.requireNonNull(source, "source");
        Objects.requireNonNull(artifact, "artifact");
        if (hash == null || !hash.matches("[0-9a-f]{64}") || length < 0) throw unreadable("connector-artifact");
        var registration = new ConnectorRegistration(connectorId, hash, pdkApiVersion, source);
        Stored existing = readHash(hash);
        if (existing != null) {
            ArtifactStreams.copyVerified(artifact, guarded(OutputStream.nullOutputStream()), hash, length);
            Stored current = readHash(hash);
            if (current == null || !existing.id().equals(current.id())) throw unreadable(hash);
            active(); return new RegistrationOutcome(existing.registration(), false);
        }
        String upload = UUID.randomUUID().toString();
        BsonDocument id = uploadId(upload);
        String filename = "staging-" + upload;
        Document stage = access.stamp(new Document("publication", STAGED).append("contentHash", hash)
                .append("registration", MongoConnectorRegistry.metadata(connectorId, pdkApiVersion, source)));
        GridFSUploadStream stream = null;
        boolean complete = false;
        boolean ioAttempted = false;
        boolean publicationAttempted = false;
        try {
            active();
            stream = bucket.openUploadStream(id, filename, new GridFSUploadOptions().chunkSizeBytes(CHUNK_SIZE).metadata(stage));
            ioAttempted = true;
            ArtifactStreams.copyVerified(artifact, guarded(stream), hash, length);
            active(); ioAttempted = true; stream.close(); complete = true; active();
            publicationAttempted = true;
            var outcome = access.write(session -> {
                Stored winner = findHash(session, hash);
                if (winner != null) return new RegistrationOutcome(winner.registration(), id.equals(winner.id()));
                Document row = access.collection.find(session, new Document("_id", id)).first();
                if (row == null || !row.keySet().equals(FILE_FIELDS) || !filename.equals(row.get("filename"))
                        || !Long.valueOf(length).equals(row.get("length")) || !Integer.valueOf(CHUNK_SIZE).equals(row.get("chunkSize"))
                        || !(row.get("uploadDate") instanceof Date) || !stage.equals(row.get("metadata"))) throw unreadable(hash);
                Document published = access.stamp(MongoConnectorRegistry.metadata(connectorId, pdkApiVersion, source)
                        .append("publication", PUBLISHED).append("contentHash", hash));
                if (access.collection.updateOne(session, new Document("_id", id).append("filename", filename).append("metadata", stage),
                        new Document("$set", new Document("filename", hash).append("metadata", published))).getMatchedCount() != 1) throw unreadable(hash);
                return new RegistrationOutcome(registration, true);
            });
            if (!outcome.newlyRegistered()) cleanup(id, hash, upload, publicationAttempted);
            return outcome;
        } catch (IOException | RuntimeException original) {
            Throwable failure = original instanceof RuntimeException runtime ? safeFailure(runtime, hash) : original;
            boolean cleanupConfirmed = true;
            if (stream != null && !complete) {
                try { stream.abort(); }
                catch (RuntimeException ignored) { cleanupConfirmed = false; }
            }
            if (ioAttempted) {
                try { cleanup(id, hash, upload, publicationAttempted); }
                catch (RuntimeException ignored) { cleanupConfirmed = false; }
                // An upload command with an unknown network result may still leave a quarantined row.
                if (!complete && original instanceof MongoException) cleanupConfirmed = false;
            }
            if (!cleanupConfirmed) failure.addSuppressed(cleanupUnconfirmed(hash, upload));
            if (failure instanceof IOException io) throw io;
            throw (RuntimeException) failure;
        }
    }

    private OutputStream guarded(OutputStream target) {
        return new OutputStream() {
            @Override public void write(int value) throws IOException { active(); target.write(value); active(); }
            @Override public void write(byte[] bytes, int offset, int length) throws IOException {
                active(); target.write(bytes, offset, length); active();
            }
        };
    }

    @Override public List<ConnectorRegistration> list() { return registrations(null); }
    @Override public List<ConnectorRegistration> findAll(String connectorId) { return registrations(Objects.requireNonNull(connectorId, "connectorId")); }
    private List<ConnectorRegistration> registrations(String connectorId) {
        return access.read(session -> {
            Document filter = publishedFilter();
            if (connectorId != null) filter.append("metadata.connectorId", connectorId);
            var registrations = new ArrayList<ConnectorRegistration>(); var hashes = new HashSet<String>();
            try (var cursor = access.collection.find(session, filter).sort(new Document("filename", 1)).iterator()) {
                while (cursor.hasNext()) {
                    Stored row = decode(cursor.next());
                    if (!hashes.add(row.registration().contentHash())) throw unreadable(row.registration().contentHash());
                    registrations.add(row.registration());
                }
            }
            return List.copyOf(registrations);
        });
    }
    @Override public boolean hasArtifact(String contentHash) { return readHash(Objects.requireNonNull(contentHash, "contentHash")) != null; }
    @Override public Optional<byte[]> artifact(String contentHash) {
        Objects.requireNonNull(contentHash, "contentHash");
        Stored original = readHash(contentHash);
        if (original == null) return Optional.empty();
        var bytes = new ByteArrayOutputStream();
        try { copyArtifact(original, contentHash, bytes); }
        catch (IOException impossible) { throw new IllegalStateException("In-memory artifact could not be written", impossible); }
        return Optional.of(bytes.toByteArray());
    }

    @Override public boolean copyArtifactTo(String contentHash, OutputStream target) throws IOException {
        Objects.requireNonNull(contentHash, "contentHash"); Objects.requireNonNull(target, "target");
        Stored original = readHash(contentHash);
        if (original == null) return false;
        copyArtifact(original, contentHash, target);
        return true;
    }

    private void copyArtifact(Stored original, String contentHash, OutputStream target) throws IOException {
        try {
            active();
            try (var stream = bucket.openDownloadStream(original.id())) {
                ArtifactStreams.copyVerified(stream, guarded(target), contentHash, original.length());
            }
            active();
            // The download is outside the metadata transaction; query the same original result again.
            Stored current = readHash(contentHash);
            if (current == null || !original.id().equals(current.id())) throw unreadable(contentHash);
            active();
        } catch (MongoGridFSException invalid) { active(); throw unreadable(contentHash); }
        catch (RuntimeException failure) { active(); throw safeFailure(failure, contentHash); }
    }

    private Stored readHash(String hash) { return access.read(session -> findHash(session, hash)); }
    private Stored findHash(com.mongodb.client.ClientSession session, String hash) {
        try (var cursor = access.collection.find(session, publishedFilter().append("filename", hash)).limit(2).iterator()) {
            if (!cursor.hasNext()) return null;
            Stored first = decode(cursor.next());
            if (cursor.hasNext()) throw unreadable(hash);
            return first;
        }
    }
    private Document publishedFilter() {
        return new Document("metadata.publication", PUBLISHED).append("metadata.cacheScope.clusterId", scope.clusterId())
                .append("metadata.cacheScope.bindingVersion", scope.bindingVersion()).append("metadata.cacheScope.cacheIncarnation", scope.cacheIncarnation());
    }
    private Stored decode(Document row) {
        String hash = row.get("filename") instanceof String value && value.matches("[0-9a-f]{64}") ? value : "connector-artifact";
        try {
            if (!hash.matches("[0-9a-f]{64}") || !row.keySet().equals(FILE_FIELDS) || !(row.get("_id") instanceof Document id)
                    || !id.keySet().equals(Set.of("clusterId", "bindingVersion", "cacheIncarnation", "uploadId"))
                    || !(id.get("bindingVersion") instanceof Long binding) || binding != scope.bindingVersion()
                    || !scope.clusterId().equals(id.getString("clusterId")) || !scope.cacheIncarnation().equals(id.getString("cacheIncarnation"))
                    || !UUID.fromString(id.getString("uploadId")).toString().equals(id.getString("uploadId"))
                    || !(row.get("length") instanceof Long length) || length < 0 || !(row.get("chunkSize") instanceof Integer size) || size < 1
                    || !(row.get("uploadDate") instanceof Date) || !(row.get("metadata") instanceof Document metadata)
                    || !metadata.keySet().equals(PUBLISHED_METADATA) || !access.inScope(metadata, hash)
                    || !PUBLISHED.equals(metadata.get("publication")) || !hash.equals(metadata.get("contentHash"))) throw unreadable(hash);
            Document registration = new Document(metadata); registration.remove("cacheScope"); registration.remove("publication"); registration.remove("contentHash");
            ConnectorRegistration value = MongoConnectorRegistry.toRegistration(hash, registration);
            if (!registration.equals(MongoConnectorRegistry.metadata(value.connectorId(), value.pdkApiVersion(), value.source()))) throw unreadable(hash);
            return new Stored(id.toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry()), value, length);
        } catch (TapstateException | NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadable(hash); }
    }
    private BsonDocument uploadId(String upload) {
        return new BsonDocument("clusterId", new BsonString(scope.clusterId())).append("bindingVersion", new BsonInt64(scope.bindingVersion()))
                .append("cacheIncarnation", new BsonString(scope.cacheIncarnation())).append("uploadId", new BsonString(upload));
    }

    /** Exact invocation-owned, unpublished resources may be removed after the caller's business guard is lost. */
    private void cleanup(BsonDocument id, String hash, String upload, boolean publicationAttempted) {
        try {
            ownerOpen.run();
            try (var session = client.startSession()) {
                session.withTransaction(() -> {
                    ownerOpen.run();
                    Document row = access.collection.find(session, new Document("_id", id)).first();
                    if (row == null && publicationAttempted) throw cleanupUnconfirmed(hash, upload);
                    if (row != null) {
                        if (!(row.get("metadata") instanceof Document metadata) || !access.inScope(metadata, hash)) throw cleanupUnconfirmed(hash, upload);
                        if (PUBLISHED.equals(metadata.get("publication"))) { decode(row); return null; }
                        if (!STAGED.equals(metadata.get("publication"))) throw cleanupUnconfirmed(hash, upload);
                        if (access.collection.deleteOne(session, new Document("_id", id).append("metadata.publication", STAGED)
                                .append("metadata.cacheScope.clusterId", scope.clusterId()).append("metadata.cacheScope.bindingVersion", scope.bindingVersion())
                                .append("metadata.cacheScope.cacheIncarnation", scope.cacheIncarnation())).getDeletedCount() != 1) throw cleanupUnconfirmed(hash, upload);
                    }
                    chunks.deleteMany(session, new Document("files_id", id));
                    ownerOpen.run(); return null;
                }, TRANSACTION);
            }
            ownerOpen.run();
        } catch (RuntimeException failed) { throw cleanupUnconfirmed(hash, upload); }
    }
    private void active() { ownerOpen.run(); requireActive.run(); }
    private static TapstateException unreadable(String hash) { return MongoCacheMetadataAccess.unreadable(hash, "artifact"); }
    private static TapstateException cleanupUnconfirmed(String hash, String upload) { return new TapstateException(IoError.CONNECTOR_ARTIFACT_CLEANUP_UNCONFIRMED,
            Map.of("hash", hash, "upload", upload), null); }
    private static RuntimeException safeFailure(RuntimeException failure, String hash) {
        if (failure instanceof MongoException mongo) return StoreIo.coded(mongo);
        if (failure instanceof BsonMaximumSizeExceededException) return new TapstateException(IoError.DOCUMENT_TOO_LARGE, Map.of("id", hash), null);
        return failure;
    }
    private record Stored(BsonDocument id, ConnectorRegistration registration, long length) {}
}
