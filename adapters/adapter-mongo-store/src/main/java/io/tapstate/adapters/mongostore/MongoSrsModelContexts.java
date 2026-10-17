package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SrsModel;
import io.tapstate.spi.store.SrsModelKey;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Small mutable ordering facts survive loss of immutable model payloads. All writes use the caller's transaction. */
final class MongoSrsModelContexts {
    private final MongoDatabase database;
    private final CacheBusinessScope scope;

    MongoSrsModelContexts(MongoDatabase database, CacheBusinessScope scope) {
        this.database = database; this.scope = scope;
    }

    record Context(SrsModelKey key, String ring, WorkloadClaimFence fence, long firstSequence,
                   long publishedVersion, long publishedSequence, String publishedHash,
                   Document nextId, Long nextSequence, Document row) {
        Document id() { return contextId(key); }
    }

    static Document contextId(SrsModelKey key) {
        return new Document("chain", key.miningChainId()).append("table", key.table())
                .append("captureContext", new Document("cacheIncarnation", key.cacheIncarnation()).append("epoch", key.captureEpoch()));
    }

    Context find(ClientSession session, SrsModelKey key) { return find(session, contextId(key)); }
    Context find(ClientSession session, Document id) {
        Document row = SystemCollections.SRS_MODEL_CONTEXTS.on(database).find(session, new Document("_id", id)).first();
        return row == null ? null : decode(row);
    }
    Context require(ClientSession session, Document id) {
        Context context = find(session, id);
        if (context == null) throw unavailable(identity(id));
        return context;
    }

    Context firstOnRing(ClientSession session, String ring) {
        Document row = SystemCollections.SRS_MODEL_CONTEXTS.on(database).find(session,
                new Document("cacheIncarnation", scope.cacheIncarnation()).append("ringName", ring)).first();
        return row == null ? null : decode(row);
    }

    Document head(Document chain, String table) {
        for (Document head : heads(chain)) if (table.equals(head.getString("table"))) return head;
        return null;
    }

    void requireAnchor(ClientSession session, Document chain, Context context) {
        Document head = head(chain, context.key().table());
        if (head == null || !context.ring().equals(head.get("ringName"))) throw unavailable(context.key());
        Context latest = require(session, head.get("contextId", Document.class));
        if (!latest.ring().equals(context.ring()) || latest.nextId() != null
                || latest.key().captureEpoch() < context.key().captureEpoch()
                || !(chain.get("epoch") instanceof Long current) || latest.key().captureEpoch() > current) throw unreadable();
        if (!latest.id().equals(context.id()) && context.nextId() == null) throw unavailable(context.key());
        if (context.nextId() != null && identity(context.nextId()).captureEpoch() > latest.key().captureEpoch()) throw unreadable();
    }

    void insert(ClientSession session, Context context) { SystemCollections.SRS_MODEL_CONTEXTS.on(database).insertOne(session, context.row()); }
    void replace(ClientSession session, Context expected, Context replacement) {
        if (SystemCollections.SRS_MODEL_CONTEXTS.on(database).replaceOne(session,
                new Document("_id", expected.id()).append("contentHash", expected.row().get("contentHash")), replacement.row()).getMatchedCount() != 1) {
            throw unavailable(expected.key());
        }
    }
    void setHead(ClientSession session, Document chain, Context context) {
        List<Document> before = heads(chain); var after = new ArrayList<Document>();
        for (Document head : before) if (!context.key().table().equals(head.getString("table"))) after.add(head);
        after.add(new Document("table", context.key().table()).append("ringName", context.ring()).append("contextId", context.id()));
        if (SystemCollections.SRS_CHAINS.on(database).updateOne(session,
                new Document("_id", chain.get("_id")).append("epoch", chain.get("epoch")).append("modelContextHeads", before),
                new Document("$set", new Document("modelContextHeads", after))).getMatchedCount() != 1) throw unavailable(context.key());
    }

    Context initial(SrsModel model, WorkloadClaimFence fence, String hash) {
        return create(model.key(), model.ringName(), fence, model.ddlSequence(), 0, model.ddlSequence(), hash, null, null);
    }
    Context advance(Context current, SrsModel model, String hash) {
        return create(current.key(), current.ring(), current.fence(), current.firstSequence(), model.key().modelVersion(),
                model.ddlSequence(), hash, current.nextId(), current.nextSequence());
    }
    Context close(Context current, Context next) {
        if (current.nextId() != null || next.key().captureEpoch() <= current.key().captureEpoch()
                || next.firstSequence() < current.publishedSequence()) throw unavailable(current.key());
        return create(current.key(), current.ring(), current.fence(), current.firstSequence(), current.publishedVersion(),
                current.publishedSequence(), current.publishedHash(), next.id(), next.firstSequence());
    }

    private Context create(SrsModelKey key, String ring, WorkloadClaimFence fence, long first,
                           long version, long sequence, String hash, Document next, Long nextSequence) {
        Document row = new Document("_id", contextId(key)).append("formatVersion", 1).append("cacheIncarnation", scope.cacheIncarnation())
                .append("ringName", ring).append("captureFence", WorkloadClaimDocuments.stored(fence)).append("firstSequence", first)
                .append("publishedVersion", version).append("publishedSequence", sequence).append("publishedHash", hash)
                .append("nextContext", next).append("nextSequence", nextSequence);
        row.append("contentHash", CanonicalHash.ofText(row.toJson()));
        return decode(row);
    }

    private List<Document> heads(Document chain) {
        Object value = chain.get("modelContextHeads");
        if (!(value instanceof List<?> entries)) throw unreadable();
        var result = new ArrayList<Document>(); var tables = new HashSet<String>(); var rings = new HashSet<String>();
        for (Object entry : entries) {
            if (!(entry instanceof Document head) || !head.keySet().equals(Set.of("table", "ringName", "contextId"))
                    || !(head.get("table") instanceof String table) || table.isBlank()
                    || !(head.get("ringName") instanceof String ring) || ring.isBlank()) throw unreadable();
            if (!(head.get("contextId") instanceof Document contextId)) throw unreadable();
            SrsModelKey key = identity(contextId);
            if (!key.miningChainId().equals(chain.get("_id")) || !table.equals(key.table()) || !tables.add(table) || !rings.add(ring)) throw unreadable();
            result.add(head);
        }
        return result;
    }

    private Context decode(Document row) {
        try {
            if (!row.keySet().equals(Set.of("_id", "formatVersion", "cacheIncarnation", "ringName", "captureFence", "firstSequence",
                    "publishedVersion", "publishedSequence", "publishedHash", "nextContext", "nextSequence", "contentHash"))
                    || !Integer.valueOf(1).equals(row.get("formatVersion")) || !scope.cacheIncarnation().equals(row.get("cacheIncarnation"))
                    || !(row.get("ringName") instanceof String ring) || ring.isBlank()
                    || !(row.get("firstSequence") instanceof Long first) || first < 0
                    || !(row.get("publishedVersion") instanceof Long version) || version < 0 || version == Long.MAX_VALUE
                    || !(row.get("publishedSequence") instanceof Long sequence) || sequence < first
                    || (version == 0 && sequence != first) || (version > 0 && sequence <= first)
                    || !(row.get("publishedHash") instanceof String hash) || hash.isBlank()) throw unreadable();
            SrsModelKey key = identity(row.get("_id", Document.class));
            Document storedFence = row.get("captureFence", Document.class);
            if (!(storedFence.get("claimGeneration") instanceof Long claim) || !(storedFence.get("executionGeneration") instanceof Long execution)
                    || !(storedFence.get("topologyRevision") instanceof Long topology)) throw unreadable();
            var fence = new WorkloadClaimFence(new WorkloadClaimKey(storedFence.getString("clusterId"), WorkloadClaimType.CAPTURE, storedFence.getString("resourceId")),
                    new WorkloadOwner(storedFence.getString("ownerNodeId"), storedFence.getString("ownerBootId")), claim, execution, topology);
            if (!scope.clusterId().equals(fence.key().clusterId()) || !storedFence.equals(WorkloadClaimDocuments.stored(fence))) throw unreadable();
            Document next = row.get("nextContext", Document.class); Object nextValue = row.get("nextSequence");
            Long nextSequence = null;
            if (next != null) {
                var nextKey = identity(next);
                if (!(nextValue instanceof Long end) || end < sequence || nextKey.captureEpoch() <= key.captureEpoch()
                        || !nextKey.miningChainId().equals(key.miningChainId()) || !nextKey.table().equals(key.table())) throw unreadable();
                nextSequence = end;
            } else if (nextValue != null) throw unreadable();
            Document payload = new Document(row); payload.remove("contentHash");
            if (!CanonicalHash.ofText(payload.toJson()).equals(row.get("contentHash"))) throw unreadable();
            return new Context(key, ring, fence, first, version, sequence, hash, next, nextSequence, row);
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }
    private SrsModelKey identity(Document id) {
        try {
            if (!id.keySet().equals(Set.of("chain", "table", "captureContext"))) throw unreadable();
            Document context = id.get("captureContext", Document.class);
            if (!context.keySet().equals(Set.of("cacheIncarnation", "epoch")) || !(context.get("epoch") instanceof Long epoch)
                    || !scope.cacheIncarnation().equals(context.get("cacheIncarnation"))) throw unreadable();
            return new SrsModelKey(id.getString("chain"), id.getString("table"), scope.cacheIncarnation(), epoch, 0);
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }
    private static TapstateException unavailable(SrsModelKey key) {
        return new TapstateException(IoError.SRS_MODEL_UNAVAILABLE, Map.of("chain", key.miningChainId(), "table", key.table(), "version", "0"), null);
    }
    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "srs-model-context", "field", "ordering"), null);
    }
}
