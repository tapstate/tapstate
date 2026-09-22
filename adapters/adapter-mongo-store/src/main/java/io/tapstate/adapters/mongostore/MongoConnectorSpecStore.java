package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.bson.types.Binary;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The MongoDB connector spec store: one document per distinct spec source, keyed by the content hash
 * of its bytes. The bytes are stored opaquely as binary rather than as a parsed document — the point
 * of keeping the source is that it survives byte-exact, which a bson round-trip through a structured
 * document would not guarantee (key order, number representation and any shape bson cannot express
 * would all be at risk).
 *
 * <p>Specs are small documents extracted from an artifact, so they live in a plain collection rather
 * than GridFS, which is where whole artifacts go. No driver type escapes this module (rule R3): the
 * binary wrapper is unwrapped to {@code byte[]} on the way out, and a stored document the unwrapping
 * cannot read is surfaced as a coded {@code io.document-unreadable} diagnostic rather than a bare crash.
 */
public final class MongoConnectorSpecStore implements ConnectorSpecStore {

    private static final String SPEC = "spec";

    private final MongoCollection<Document> collection;

    public MongoConnectorSpecStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public void put(String contentHash, byte[] spec) {
        Objects.requireNonNull(contentHash, "contentHash");
        Objects.requireNonNull(spec, "spec");
        // Upsert by the content hash (the document _id). Content-addressed, so a second write under a
        // hash carries the same bytes: replacing in place is the idempotent outcome, not an overwrite
        // of something different.
        try {
            collection.replaceOne(
                    new Document("_id", contentHash),
                    toDocument(contentHash, spec),
                    new ReplaceOptions().upsert(true));
        } catch (MongoWriteException e) {
            if (e.getError().getCategory() != ErrorCategory.DUPLICATE_KEY) {
                throw StoreIo.coded(e);
            }
            // Two writers both found nothing under this hash and both inserted; one of them lost the race
            // on the key. Because the key IS the content, the winner wrote the same bytes this call
            // carries - the store now holds exactly what was asked for, so reporting a failure would fail
            // a write that succeeded.
        } catch (MongoException e) {
            throw StoreIo.coded(e);
        }
    }

    /** Maps spec source to its stored document: the content hash as {@code _id}, the bytes as binary. */
    static Document toDocument(String contentHash, byte[] spec) {
        return new Document("_id", contentHash).append(SPEC, new Binary(spec));
    }

    @Override
    public Optional<byte[]> get(String contentHash) {
        Objects.requireNonNull(contentHash, "contentHash");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", contentHash)).first());
        if (document == null) {
            return Optional.empty();
        }
        return Optional.of(toSpec(document, contentHash));
    }

    @Override
    public boolean has(String contentHash) {
        Objects.requireNonNull(contentHash, "contentHash");
        // Projected down to the key: the question is whether a source is filed here, and a spec source is
        // a whole connector form, so answering it must not carry the form back.
        return StoreIo.call(() -> collection.find(new Document("_id", contentHash))
                .projection(new Document("_id", 1))
                .first()) != null;
    }

    /** Reads the stored bytes out of a spec document, or fails coded when the document cannot be read. */
    static byte[] toSpec(Document document, String contentHash) {
        // A document filed under the hash but carrying no readable binary under its field is store
        // corruption — an out-of-band write, an interrupted migration. Dereferencing it blind would throw
        // a bare null or cast failure out of a module whose contract is that no unreadable document
        // escapes uncoded, and the read face above would answer a bodyless 500 instead of stating that
        // the source is not available.
        if (document.get(SPEC) instanceof Binary binary && binary.getData() != null) {
            return binary.getData();
        }
        throw new TapstateException(
                IoError.DOCUMENT_UNREADABLE,
                Map.of("id", String.valueOf(contentHash), "field", "spec"), null);
    }
}
