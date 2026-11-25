package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import java.util.*;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

/** SRS alone accepts the bounded full variant; ordinary and DIRECT request encoding stays unchanged. */
final class MongoSrsSourceRequests {
    private MongoSrsSourceRequests() {}
    record Decoded(SourceResumeOverride request, SrsFreshFull full) {}

    static Document stored(SourceResumeOverride request, SrsFreshFull full) {
        if (full == null) return MongoResumeOverrides.stored(request);
        Objects.requireNonNull(request, "request");
        Document row = new Document("token", request.token()).append("fromEpoch", request.fromEpoch())
                .append("editedAt", Date.from(request.editedAt())).append("pendingFreshFull", full(full));
        row.append("contentHash", CanonicalHash.ofText(row.toJson()));
        requireSize(row, full.models().values().iterator().next().miningChainId());
        return row;
    }

    static Decoded read(Object value) {
        if (!(value instanceof Document row) || !row.containsKey("pendingFreshFull"))
            return new Decoded(MongoResumeOverrides.read(value), null);
        try {
            if (!row.keySet().equals(Set.of("token", "fromEpoch", "editedAt", "pendingFreshFull", "contentHash"))
                    || !(row.get("token") instanceof String token) || token.isBlank()
                    || !(row.get("fromEpoch") instanceof Long epoch) || !(row.get("editedAt") instanceof Date at)) throw unreadable();
            var request = new SourceResumeOverride(token, epoch, at.toInstant());
            var pending = full(row.get("pendingFreshFull", Document.class));
            if (!stored(request, pending).equals(row)) throw unreadable();
            return new Decoded(request, pending);
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }

    private static Document full(SrsFreshFull value) {
        var origin = value.origin();
        var identity = new Document("operationId", origin.operationId())
                .append("targetScope", MongoPipelinePreparationFacts.scope(origin.targetScope()))
                .append("preparationId", origin.preparationId()).append("confirmationHash", origin.confirmationHash())
                .append("sourceId", origin.sourceId());
        var models = new ArrayList<Document>();
        for (String table : new TreeSet<>(value.models().keySet()))
            models.add(new Document("table", table).append("model", MongoSrsModelStore.key(value.models().get(table))));
        return new Document("formatVersion", 1).append("origin", identity)
                .append("snapshotEpoch", value.snapshotEpoch()).append("models", models);
    }

    private static SrsFreshFull full(Document row) {
        if (!row.keySet().equals(Set.of("formatVersion", "origin", "snapshotEpoch", "models"))
                || !Integer.valueOf(1).equals(row.get("formatVersion")) || !(row.get("snapshotEpoch") instanceof Long epoch)
                || !(row.get("origin") instanceof Document origin) || !(row.get("models") instanceof List<?> entries)
                || !origin.keySet().equals(Set.of("operationId", "targetScope", "preparationId", "confirmationHash", "sourceId"))) throw unreadable();
        var identity = new SrsFreshFullOrigin(origin.getString("operationId"),
                MongoPipelinePreparationFacts.scope(origin.get("targetScope", Document.class)), origin.getString("preparationId"),
                origin.getString("confirmationHash"), origin.getString("sourceId"));
        var references = new TreeMap<String, SrsModelKey>();
        for (Object entry : entries) {
            if (!(entry instanceof Document item) || !item.keySet().equals(Set.of("table", "model"))
                    || !(item.get("table") instanceof String table)
                    || references.put(table, MongoSrsModelStore.readKey(item.get("model", Document.class))) != null) throw unreadable();
        }
        var result = new SrsFreshFull(identity, epoch, references);
        if (!full(result).equals(row)) throw unreadable();
        return result;
    }

    private static void requireSize(Document row, String chain) {
        try (var output = new BasicOutputBuffer(); var writer = new BsonBinaryWriter(output)) {
            new DocumentCodec().encode(writer, row, EncoderContext.builder().build());
            if (output.getSize() > SrsFreshFull.MAX_ENCODED_REQUEST_BYTES)
                throw new TapstateException(IoError.SRS_FULL_PREPARATION_CAPACITY,
                        Map.of("chain", chain, "bytes", SrsFreshFull.MAX_ENCODED_REQUEST_BYTES), null);
        }
    }
    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "source-position", "field", "pendingFreshFull"), null);
    }
}
