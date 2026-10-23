package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.NestDeadLetterRecord;
import io.tapstate.spi.store.RequiredDeadLetterFact;
import org.bson.BSONException;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecConfigurationException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Captures portable row values once without interpreting user field names as extended JSON. */
final class RequiredDeadLetterCodec {
    private static final DocumentCodec BSON = new DocumentCodec();
    private RequiredDeadLetterCodec() {}

    static Document capture(RequiredDeadLetterFact fact) {
        try {
            Document captured = new RawBsonDocument(body(fact), BSON).decode(BSON);
            Document reproduced = new RawBsonDocument(body(decode(captured)), BSON).decode(BSON);
            if (!captured.equals(reproduced)) throw invalid();
            return captured;
        } catch (TapstateException invalid) {
            throw invalid();
        } catch (BSONException | CodecConfigurationException | IllegalArgumentException | ClassCastException | NullPointerException invalid) {
            throw invalid();
        }
    }
    private static Document body(RequiredDeadLetterFact fact) {
        var record = fact.record(); var positions = new ArrayList<Document>();
        fact.positions().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            var position = entry.getValue(); var order = position.order();
            positions.add(new Document("chain", entry.getKey()).append("order", order == null ? null
                    : new Document("epoch", order.epoch()).append("seq", order.seq())).append("token", position.token()));
        });
        Document body = new Document("cacheIncarnation", fact.cacheIncarnation()).append("namespace", record.namespace())
                .append("element", record.element()).append("chain", record.chain()).append("order", record.order()).append("positions", positions)
                .append("heldForMillis", record.heldForMillis()).append("discardedAt", record.discardedAt());
        if (!record.deletion()) body.append("row", RequiredDeadLetterRows.encode(record.row()));
        return body;
    }
    static RequiredDeadLetterFact decode(Document body) {
        try {
            var fields = new HashSet<>(Set.of("cacheIncarnation", "namespace", "element", "chain", "order", "positions", "heldForMillis", "discardedAt"));
            if (body.containsKey("row")) fields.add("row");
            if (!body.keySet().equals(fields) || !(body.get("heldForMillis") instanceof Long held)
                    || !(body.get("discardedAt") instanceof Long at) || !(body.get("positions") instanceof List<?> entries)) throw unreadable();
            var positions = new LinkedHashMap<String, ChainPosition>();
            for (Object value : entries) {
                if (!(value instanceof Document entry) || !entry.keySet().equals(Set.of("chain", "order", "token"))
                        || !(entry.get("chain") instanceof String chain) || chain.isBlank()
                        || (entry.get("token") != null && !(entry.get("token") instanceof String))) throw unreadable();
                SourceOrder order = null;
                if (entry.get("order") != null) {
                    if (!(entry.get("order") instanceof Document ordinal) || !ordinal.keySet().equals(Set.of("epoch", "seq"))
                            || !(ordinal.get("epoch") instanceof Long epoch) || !(ordinal.get("seq") instanceof Long sequence)) throw unreadable();
                    order = new SourceOrder(epoch, sequence);
                }
                if (positions.put(chain, new ChainPosition(order, entry.getString("token"))) != null) throw unreadable();
            }
            if (body.containsKey("row") && !(body.get("row") instanceof Document)) throw unreadable();
            var record = new NestDeadLetterRecord(body.getString("namespace"), body.getString("element"), body.getString("chain"), body.getString("order"),
                    held, at, body.containsKey("row") ? RequiredDeadLetterRows.decode(body.get("row", Document.class)) : null);
            return new RequiredDeadLetterFact(body.getString("cacheIncarnation"), positions, record);
        } catch (BSONException | CodecConfigurationException | IllegalArgumentException | ClassCastException | NullPointerException invalid) {
            throw unreadable();
        }
    }
    static Document content(Document body) {
        Document content = new Document(body); content.remove("heldForMillis"); content.remove("discardedAt"); return content;
    }
    static Document eventIdentity(Document body) {
        Document identity = content(body); identity.remove("row");
        var positions = new ArrayList<Document>();
        for (Document position : body.getList("positions", Document.class)) {
            Document copy = new Document(position); copy.remove("token"); positions.add(copy);
        }
        identity.put("positions", positions); return identity;
    }
    private static TapstateException invalid() { return diagnostic(IoError.DOCUMENT_INVALID); }
    static TapstateException unreadable() { return diagnostic(IoError.DOCUMENT_UNREADABLE); }
    private static TapstateException diagnostic(IoError code) {
        return new TapstateException(code, Map.of("id", "required-dead-letter", "field", "fact"), null);
    }
}
