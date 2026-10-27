package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SourceBindingIdentity;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.bson.Document;

/** One current lifetime and two independent revisions, never derived from a cipher IV or logical hash. */
final class MongoSourceIdentity {
    static final int VERSION = 1;
    static final int MAX_HEADER_BYTES = 192;
    private static final Set<String> FIELDS = Set.of("formatVersion", "incarnation", "bindingRevision", "writeFence");
    record Header(SourceBindingIdentity binding, long writeFence) { }
    private MongoSourceIdentity() { }

    static Header create(String id) {
        return new Header(new SourceBindingIdentity(id, UUID.randomUUID().toString(), 1), 0);
    }
    static Document encode(Header header) {
        if (header.writeFence() < 0) { throw new IllegalArgumentException("Source write fence must not be negative"); }
        return new Document("formatVersion", VERSION).append("incarnation", header.binding().incarnation())
                .append("bindingRevision", header.binding().bindingRevision()).append("writeFence", header.writeFence());
    }
    static Header read(Document source) {
        try {
            if (!(source.get("_id") instanceof String id) || !"source".equals(source.get("kind"))
                    || !(source.get(MongoSourceIdentitySchema.FIELD) instanceof Document row)
                    || !row.keySet().equals(FIELDS) || !Integer.valueOf(VERSION).equals(row.get("formatVersion"))
                    || !(row.get("incarnation") instanceof String incarnation)
                    || !(row.get("bindingRevision") instanceof Long revision)
                    || !(row.get("writeFence") instanceof Long fence) || fence < 0) { throw unreadable(); }
            return new Header(new SourceBindingIdentity(id, incarnation, revision), fence);
        } catch (IllegalArgumentException | NullPointerException invalid) { throw unreadable(); }
    }

    static void requireWritable(MongoSourceIdentitySchema.State state, Document existing, String id) {
        if (state != null && state.phase() != MongoSourceIdentitySchema.Phase.READY) {
            throw MongoSourceIdentitySchema.unavailable(id);
        }
        if (existing != null && "source".equals(existing.get("kind"))) {
            if (state == null) {
                if (existing.containsKey(MongoSourceIdentitySchema.FIELD)) { throw unreadable(); }
            } else { read(existing); }
        }
    }

    static Document decorate(MongoSourceIdentitySchema.State state, Document existing, SourceResource desired,
            Document encoded, SourceConfigCipher cipher) {
        requireWritable(state, existing, desired.id());
        if (state == null) { return encoded; }
        Header header;
        if (existing == null || !"source".equals(existing.get("kind"))) { header = create(desired.id()); }
        else {
            header = read(existing);
            // The already captured original key keeps this comparison pure inside its CONFIG transaction.
            SourceResource old = (SourceResource) new EncryptedArtifactCodec(SourceConfigCipherProvider.fixed(cipher)).decode(existing);
            if (!bindingHash(old).equals(bindingHash(desired))) {
                if (header.binding().bindingRevision() == Long.MAX_VALUE) { throw exhausted(desired.id()); }
                header = new Header(new SourceBindingIdentity(desired.id(), header.binding().incarnation(),
                        header.binding().bindingRevision() + 1), header.writeFence());
            }
        }
        return encoded.append(MongoSourceIdentitySchema.FIELD, encode(header));
    }

    static boolean fence(ClientSession session, MongoDatabase database, SourceBindingIdentity expected,
            String originalKeyId) {
        MongoAlertStateStore.requireTransaction(session);
        var state = MongoSourceIdentitySchema.read(session, database, originalKeyId);
        if (state == null || state.phase() != MongoSourceIdentitySchema.Phase.READY) {
            throw MongoSourceIdentitySchema.unavailable(expected.sourceId());
        }
        var sources = SystemCollections.SOURCES.on(database);
        Document row = sources.find(session, new Document("_id", expected.sourceId()))
                .projection(new Document("kind", 1).append(MongoSourceIdentitySchema.FIELD, 1)).first();
        if (row == null) { return false; }
        Header actual = read(row);
        if (!actual.binding().equals(expected)) { return false; }
        if (actual.writeFence() == Long.MAX_VALUE) { throw exhausted(expected.sourceId()); }
        fenceOriginalKey(session, database, state);
        return sources.updateOne(session, new Document("_id", expected.sourceId())
                        .append("sourceIdentity.incarnation", expected.incarnation())
                        .append("sourceIdentity.bindingRevision", expected.bindingRevision())
                        .append("sourceIdentity.writeFence", actual.writeFence()),
                new Document("$inc", new Document("sourceIdentity.writeFence", 1L))).getMatchedCount() == 1;
    }

    private static void fenceOriginalKey(ClientSession session, MongoDatabase database, MongoSourceIdentitySchema.State state) {
        var keys = SystemCollections.CLUSTER_KEYS.on(database);
        Document key = keys.find(session, new Document("_id", "cluster")).first();
        if (key == null || !state.clusterId().equals(key.get("clusterId")) || !Integer.valueOf(1).equals(key.get("formatVersion"))
                || !state.keyId().equals(key.get("keyId")) || !(key.get("material") instanceof String material)) {
            throw MongoSourceIdentitySchema.unreadable("original-key");
        }
        byte[] bytes;
        try { bytes = java.util.Base64.getUrlDecoder().decode(material); }
        catch (IllegalArgumentException invalid) { throw MongoSourceIdentitySchema.unreadable("original-key"); }
        try {
            if (bytes.length != 32 || !state.keyId().equals(SourceConfigCipher.fingerprint(bytes))) {
                throw MongoSourceIdentitySchema.unreadable("original-key");
            }
        } finally { java.util.Arrays.fill(bytes, (byte) 0); }
        if (keys.updateOne(session, new Document("_id", "cluster").append("keyId", state.keyId()),
                new Document("$inc", new Document("sourceWriteFence", 1L))).getModifiedCount() != 1) {
            throw MongoSourceIdentitySchema.unreadable("original-key");
        }
    }

    private static String bindingHash(SourceResource source) {
        return CanonicalHash.of(new SourceResource(source.id(), null, source.connector(), source.config(),
                source.mode(), source.tables(), source.srs(), source.execution(), source.experimental()));
    }
    private static TapstateException exhausted(String id) {
        return new TapstateException(IoError.SOURCE_IDENTITY_EXHAUSTED, Map.of("id", id), null);
    }
    private static TapstateException unreadable() { return MongoSourceIdentitySchema.unreadable("header"); }
}
