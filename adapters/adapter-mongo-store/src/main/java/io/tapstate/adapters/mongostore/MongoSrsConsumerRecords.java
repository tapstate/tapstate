package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SrsConsumerContext;
import io.tapstate.spi.store.SrsConsumerKey;
import io.tapstate.spi.store.SrsConsumerMembership;
import io.tapstate.spi.store.SrsConsumerProgress;
import io.tapstate.spi.store.SrsSnapshotPin;
import io.tapstate.spi.store.SrsModelKey;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Strict current membership and cursor formats; driver values never cross either resource's port. */
final class MongoSrsConsumerRecords {
    private final CacheBusinessScope scope;
    MongoSrsConsumerRecords(CacheBusinessScope scope) { this.scope = scope; }

    Document id(SrsConsumerKey key) {
        return new Document("cacheIncarnation", scope.cacheIncarnation()).append("chain", key.miningChainId()).append("consumer", key.consumerId());
    }
    Document chain(String chain) {
        return new Document("_id.cacheIncarnation", scope.cacheIncarnation()).append("_id.chain", chain);
    }
    Document membership(SrsConsumerMembership value) {
        var context = value.context(); var key = context.key();
        return hash(new Document("_id", id(key)).append("formatVersion", 1).append("pipelineId", key.pipelineId()).append("sourceId", key.sourceId())
                .append("stateGeneration", context.stateGeneration()).append("revision", value.revision()).append("detached", value.detached())
                .append("tables", sorted(value.tables())).append("commandFence", WorkloadClaimDocuments.stored(value.commandFence())));
    }
    SrsConsumerMembership membership(Document row) {
        try {
            if (!row.keySet().equals(Set.of("_id", "formatVersion", "pipelineId", "sourceId", "stateGeneration", "revision", "detached", "tables", "commandFence", "contentHash"))
                    || !Integer.valueOf(1).equals(row.get("formatVersion")) || !(row.get("stateGeneration") instanceof Long generation)
                    || !(row.get("revision") instanceof Long revision) || !(row.get("detached") instanceof Boolean detached)) throw unreadable();
            Document id = row.get("_id", Document.class);
            var key = new SrsConsumerKey(id.getString("chain"), row.getString("pipelineId"), row.getString("sourceId"));
            if (!id(key).equals(id)) throw unreadable();
            var fence = WorkloadClaimDocuments.readPipeline(row.get("commandFence", Document.class));
            if (!scope.clusterId().equals(fence.key().clusterId())) throw unreadable();
            var result = new SrsConsumerMembership(new SrsConsumerContext(key, generation), revision, detached, stringSet(row.get("tables")), fence);
            if (!membership(result).equals(row)) throw unreadable();
            return result;
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }

    record Cursor(ConsumerOffset value, long revision, SrsSnapshotPin snapshot) {}

    Document cursor(SrsConsumerMembership membership, ConsumerOffset value, long revision) { return cursor(membership, value, revision, null); }
    Document cursor(SrsConsumerMembership membership, ConsumerOffset value, long revision, SrsSnapshotPin snapshot) {
        new SrsConsumerProgress(membership.context(), membership.revision(), revision, value, snapshot);
        if (revision < 0 || revision == Long.MAX_VALUE || membership.detached()
                || !value.pipelineId().equals(membership.context().key().consumerId()) || value.progressKind() != ConsumerProgressKind.SRS) throw unreadable();
        var read = new ArrayList<Document>();
        for (String table : sorted(value.perTableSeq().keySet())) read.add(new Document("table", table).append("sequence", value.perTableSeq().get(table)));
        var acked = new ArrayList<Document>();
        for (String table : sorted(value.sinkAckedByTable().keySet())) acked.add(new Document("table", table).append("position", position(value.sinkAckedByTable().get(table))));
        Document progress = new Document("perTableSeq", read).append("sinkAcked", position(value.sinkAcked()))
                .append("snapshotCompletedTables", sorted(Set.copyOf(value.snapshotCompletedTables()))).append("cdcStartPosition", value.cdcStartPosition())
                .append("snapshotEpoch", value.snapshotEpoch()).append("sinkAckedByTable", acked).append("progressKind", "SRS");
        return hash(new Document("_id", id(membership.context().key())).append("formatVersion", 1)
                .append("stateGeneration", membership.context().stateGeneration()).append("membershipRevision", membership.revision())
                .append("progressRevision", revision).append("progress", progress).append("snapshot", snapshot(snapshot)));
    }
    Cursor cursor(SrsConsumerMembership membership, Document row) {
        try {
            if (row == null || !row.keySet().equals(Set.of("_id", "formatVersion", "stateGeneration", "membershipRevision", "progressRevision", "progress", "snapshot", "contentHash"))
                    || !Integer.valueOf(1).equals(row.get("formatVersion")) || !(row.get("progressRevision") instanceof Long revision)
                    || !Long.valueOf(membership.context().stateGeneration()).equals(row.get("stateGeneration"))
                    || !Long.valueOf(membership.revision()).equals(row.get("membershipRevision")) || !id(membership.context().key()).equals(row.get("_id"))) throw unreadable();
            Document progress = row.get("progress", Document.class);
            if (!progress.keySet().equals(Set.of("perTableSeq", "sinkAcked", "snapshotCompletedTables", "cdcStartPosition", "snapshotEpoch", "sinkAckedByTable", "progressKind"))
                    || !"SRS".equals(progress.get("progressKind")) || !(progress.get("snapshotEpoch") instanceof Long epoch)
                    || (progress.get("cdcStartPosition") != null && !(progress.get("cdcStartPosition") instanceof String))) throw unreadable();
            var read = new HashMap<String, Long>();
            for (Document entry : documents(progress.get("perTableSeq"))) {
                if (!entry.keySet().equals(Set.of("table", "sequence")) || !(entry.get("table") instanceof String table) || table.isBlank()
                        || !(entry.get("sequence") instanceof Long sequence) || sequence < -1 || sequence == Long.MAX_VALUE
                        || read.put(table, sequence) != null) throw unreadable();
            }
            var acked = new HashMap<String, ChainPosition>();
            for (Document entry : documents(progress.get("sinkAckedByTable"))) {
                if (!entry.keySet().equals(Set.of("table", "position")) || !(entry.get("table") instanceof String table) || table.isBlank()
                        || acked.put(table, position(entry.get("position", Document.class))) != null || acked.get(table) == null) throw unreadable();
            }
            var value = new ConsumerOffset(membership.context().key().consumerId(), read, position(progress.get("sinkAcked", Document.class)),
                    sorted(stringSet(progress.get("snapshotCompletedTables"))), progress.getString("cdcStartPosition"), epoch, acked, ConsumerProgressKind.SRS);
            SrsSnapshotPin snapshot = snapshot(row.get("snapshot", Document.class));
            if (!read.keySet().containsAll(membership.tables()) || !cursor(membership, value, revision, snapshot).equals(row)) throw unreadable();
            return new Cursor(value, revision, snapshot);
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }
    private static Document snapshot(SrsSnapshotPin pin) {
        if (pin == null) return null;
        var models = new ArrayList<Document>();
        for (String table : sorted(pin.models().keySet())) models.add(new Document("table", table).append("model", MongoSrsModelStore.key(pin.models().get(table))));
        return new Document("snapshotEpoch", pin.snapshotEpoch()).append("sourcePosition", pin.sourcePosition()).append("models", models);
    }
    private static SrsSnapshotPin snapshot(Document row) {
        if (row == null) return null;
        if (!row.keySet().equals(Set.of("snapshotEpoch", "sourcePosition", "models")) || !(row.get("snapshotEpoch") instanceof Long epoch)
                || !(row.get("sourcePosition") instanceof String source)) throw unreadable();
        var models = new HashMap<String, SrsModelKey>();
        for (Document entry : documents(row.get("models"))) {
            if (!entry.keySet().equals(Set.of("table", "model")) || !(entry.get("table") instanceof String table)
                    || models.put(table, MongoSrsModelStore.readKey(entry.get("model", Document.class))) != null) throw unreadable();
        }
        var result = new SrsSnapshotPin(epoch, source, models);
        if (!snapshot(result).equals(row)) throw unreadable();
        return result;
    }
    ConsumerOffset initial(SrsConsumerKey key, Set<String> tables) {
        var positions = new HashMap<String, Long>(); tables.forEach(table -> positions.put(table, -1L));
        return new ConsumerOffset(key.consumerId(), positions, null, List.of(), null, 0L, Map.of(), ConsumerProgressKind.SRS);
    }
    ConsumerOffset include(ConsumerOffset before, Set<String> tables) {
        var positions = new HashMap<>(before.perTableSeq()); tables.forEach(table -> positions.putIfAbsent(table, -1L));
        return new ConsumerOffset(before.pipelineId(), positions, before.sinkAcked(), before.snapshotCompletedTables(), before.cdcStartPosition(),
                before.snapshotEpoch(), before.sinkAckedByTable(), before.progressKind());
    }
    private static Document position(ChainPosition value) {
        if (value == null) return null;
        SourceOrder order = value.order();
        if (order == null || order.epoch() < 1 || order.epoch() == Long.MAX_VALUE || order.seq() == Long.MAX_VALUE
                || (order.seq() < 0 && order.seq() != SourceOrder.SNAPSHOT_SEQ)) throw unreadable();
        return new Document("epoch", order.epoch()).append("sequence", order.seq()).append("token", value.token());
    }
    private static ChainPosition position(Document row) {
        if (row == null) return null;
        if (!row.keySet().equals(Set.of("epoch", "sequence", "token")) || !(row.get("epoch") instanceof Long epoch)
                || !(row.get("sequence") instanceof Long sequence) || (row.get("token") != null && !(row.get("token") instanceof String))) throw unreadable();
        var result = new ChainPosition(new SourceOrder(epoch, sequence), row.getString("token"));
        if (!position(result).equals(row)) throw unreadable();
        return result;
    }
    private static List<Document> documents(Object value) {
        if (!(value instanceof List<?> entries)) throw unreadable();
        var result = new ArrayList<Document>();
        for (Object entry : entries) { if (!(entry instanceof Document row)) throw unreadable(); result.add(row); }
        return result;
    }
    private static Set<String> stringSet(Object value) {
        if (!(value instanceof List<?> entries)) throw unreadable();
        var result = new TreeSet<String>();
        for (Object entry : entries) if (!(entry instanceof String text) || text.isBlank() || !result.add(text)) throw unreadable();
        if (!new ArrayList<>(result).equals(entries)) throw unreadable();
        return Set.copyOf(result);
    }
    private static List<String> sorted(Set<String> values) { return new ArrayList<>(new TreeSet<>(values)); }
    private static Document hash(Document row) { return row.append("contentHash", CanonicalHash.ofText(row.toJson())); }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "srs-consumer", "field", "membership-or-progress"), null); }
}
