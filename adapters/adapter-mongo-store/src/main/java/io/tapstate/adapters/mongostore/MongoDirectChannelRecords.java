package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import org.bson.Document;
import java.util.*;

/** Strict private-channel records; the pending ledger is independent from its small high-water receipt. */
final class MongoDirectChannelRecords {
    static Document id(DirectChannelContext context) {
        return new Document("scope",MongoPipelinePreparationFacts.scope(context.scope())).append("channelId",context.channelId())
                .append("sourceId",context.sourceId()).append("captureId",context.captureId());
    }
    static DirectChannelContext context(Document id) {
        if (id == null || !id.keySet().equals(Set.of("scope","channelId","sourceId","captureId"))) throw unreadable();
        var value = new DirectChannelContext(MongoPipelinePreparationFacts.scope(id.get("scope",Document.class)),
                id.getString("channelId"),id.getString("sourceId"),id.getString("captureId"));
        if (!id(value).equals(id)) throw unreadable(); return value;
    }
    static Document batchId(DirectChannelContext context,long epoch,long ordinal) {
        return new Document("channel",id(context)).append("epoch",epoch).append("ordinal",ordinal);
    }
    static Document batchFilter(DirectChannelContext context,long epoch) {
        return new Document("_id.channel",id(context)).append("_id.epoch",epoch);
    }
    static Document batch(DirectBatch value) {
        var targets = new ArrayList<Document>();
        for (String table : sorted(value.targets().keySet())) targets.add(new Document("table",table).append("sequence",value.targets().get(table)));
        return hash(new Document("_id",batchId(value.context(),value.epoch(),value.ordinal())).append("formatVersion",1)
                .append("boundary",position(value.boundary())).append("targets",targets));
    }
    static DirectBatch batch(Document row) {
        try {
            if (row == null || !row.keySet().equals(Set.of("_id","formatVersion","boundary","targets","contentHash"))
                    || !Integer.valueOf(1).equals(row.get("formatVersion"))) throw unreadable();
            var id = row.get("_id",Document.class);
            if (!id.keySet().equals(Set.of("channel","epoch","ordinal"))) throw unreadable();
            var result = new DirectBatch(context(id.get("channel",Document.class)),number(id,"epoch"),number(id,"ordinal"),
                    position(row.get("boundary",Document.class)),sequences(row.get("targets")));
            if (!batch(result).equals(row)) throw unreadable(); return result;
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }
    static String batchHash(DirectBatch value) { return batch(value).getString("contentHash"); }
    static Document progress(DirectChannelProgress value) {
        if (value.current() != null && value.current().pendingCount() > 0 || value.retained() != null)
            throw new IllegalArgumentException("a pending queue requires its complete original records");
        return progress(value,List.of(),List.of());
    }
    static Document progress(DirectChannelProgress value,List<DirectBatch> current,List<DirectBatch> retained) {
        if (current.size() != (value.current() == null ? 0 : value.current().pendingCount())
                || retained.size() != (value.retained() == null ? 0 : value.retained().pendingCount()))
            throw new IllegalArgumentException("pending queue does not close its original progress");
        return progress(value,pendingHash(current),pendingHash(retained));
    }
    private static Document progress(DirectChannelProgress value,String currentHash,String retainedHash) {
        if (currentHash == null || retainedHash == null || !currentHash.matches("[0-9a-f]{64}") || !retainedHash.matches("[0-9a-f]{64}")) throw unreadable();
        return hash(new Document("_id",id(value.context())).append("formatVersion",1).append("tables",sorted(value.tables())).append("revision",value.revision())
                .append("current",window(value.current())).append("retained",window(value.retained())).append("cursor",cursor(value.cursor()))
                .append("writerRun",writers(value.writerRun())).append("currentPendingHash",currentHash).append("retainedPendingHash",retainedHash));
    }
    static DirectChannelProgress progress(Document row) {
        try {
            if (row == null || !row.keySet().equals(Set.of("_id","formatVersion","tables","revision","current","retained","cursor","writerRun","currentPendingHash","retainedPendingHash","contentHash"))
                    || !Integer.valueOf(1).equals(row.get("formatVersion"))) throw unreadable();
            var context = context(row.get("_id",Document.class));
            var result = new DirectChannelProgress(context,strings(row.get("tables")),number(row,"revision"),window(row.get("current",Document.class)),
                    window(row.get("retained",Document.class)),cursor(context,row.get("cursor",Document.class)),writers(row.get("writerRun",Document.class)));
            if (!progress(result,row.getString("currentPendingHash"),row.getString("retainedPendingHash")).equals(row)) throw unreadable(); return result;
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }
    /** A fixed-size original receipt covers every pending descriptor, including its targets and order. */
    static String pendingHash(List<DirectBatch> batches) {
        return CanonicalHash.ofText(new Document("batches",batches.stream().map(MongoDirectChannelRecords::batchHash).toList()).toJson());
    }
    static DirectChannelProgress initial(DirectChannelContext context,Set<String> tables) {
        return new DirectChannelProgress(context,tables,0,null,null,new ConsumerOffset(context.consumerId(),Map.of(),null,List.of(),null,0,Map.of(),ConsumerProgressKind.DIRECT_SOURCE),null);
    }
    private static Document window(DirectCaptureWindow value) {
        return value == null ? null : new Document("epoch",value.epoch()).append("captureFence",WorkloadClaimDocuments.stored(value.captureFence()))
                .append("pipelineFence",WorkloadClaimDocuments.stored(value.pipelineFence())).append("anchor",value.anchor())
                .append("recordedThrough",value.recordedThrough()).append("settledThrough",value.settledThrough())
                .append("lastBoundary",position(value.lastBoundary())).append("lastBatchHash",value.lastBatchHash());
    }
    private static DirectCaptureWindow window(Document row) {
        if (row == null) return null;
        if (!row.keySet().equals(Set.of("epoch","captureFence","pipelineFence","anchor","recordedThrough","settledThrough","lastBoundary","lastBatchHash"))) throw unreadable();
        return new DirectCaptureWindow(number(row,"epoch"),WorkloadClaimDocuments.readCapture(row.get("captureFence",Document.class)),
                WorkloadClaimDocuments.readPipeline(row.get("pipelineFence",Document.class)),text(row,"anchor"),number(row,"recordedThrough"),
                number(row,"settledThrough"),position(row.get("lastBoundary",Document.class)),text(row,"lastBatchHash"));
    }
    private static Document cursor(ConsumerOffset value) {
        var acked = new ArrayList<Document>();
        for (String table : sorted(value.sinkAckedByTable().keySet())) acked.add(new Document("table",table).append("position",position(value.sinkAckedByTable().get(table))));
        return new Document("sinkAcked",position(value.sinkAcked())).append("snapshotCompletedTables",sorted(Set.copyOf(value.snapshotCompletedTables())))
                .append("cdcStartPosition",value.cdcStartPosition()).append("snapshotEpoch",value.snapshotEpoch()).append("sinkAckedByTable",acked);
    }
    private static ConsumerOffset cursor(DirectChannelContext context,Document row) {
        if (!row.keySet().equals(Set.of("sinkAcked","snapshotCompletedTables","cdcStartPosition","snapshotEpoch","sinkAckedByTable"))) throw unreadable();
        var acked = new HashMap<String,ChainPosition>();
        for (var entry : documents(row.get("sinkAckedByTable"))) {
            if (!entry.keySet().equals(Set.of("table","position")) || acked.put(entry.getString("table"),Objects.requireNonNull(position(entry.get("position",Document.class)))) != null) throw unreadable();
        }
        return new ConsumerOffset(context.consumerId(),Map.of(),position(row.get("sinkAcked",Document.class)),sorted(strings(row.get("snapshotCompletedTables"))),
                text(row,"cdcStartPosition"),number(row,"snapshotEpoch"),acked,ConsumerProgressKind.DIRECT_SOURCE);
    }
    private static Document writers(DirectWriterRun value) {
        if (value == null) return null;
        var expected = new ArrayList<Document>(); var progress = new ArrayList<Document>(); var run = value.accounting();
        for (String table : sorted(run.expected().keySet())) expected.add(new Document("table",table).append("writers",run.expectedFor(table)));
        for (String table : sorted(run.progress().keySet())) {
            var writers = new ArrayList<Document>();
            for (String writer : sorted(run.progressFor(table).keySet())) {
                var point = run.progressFor(table).get(writer);
                writers.add(new Document("writer",writer).append("durableThrough",order(point.durableThrough())).append("lastTokened",position(point.lastTokened())));
            }
            progress.add(new Document("table",table).append("writers",writers));
        }
        return new Document("epoch",value.epoch()).append("commandFence",WorkloadClaimDocuments.stored(value.commandFence()))
                .append("runId",run.runId()).append("expected",expected).append("progress",progress).append("snapshotEpoch",run.snapshotEpoch());
    }
    private static DirectWriterRun writers(Document row) {
        if (row == null) return null;
        if (!row.keySet().equals(Set.of("epoch","commandFence","runId","expected","progress","snapshotEpoch"))) throw unreadable();
        var expected = new HashMap<String,List<String>>(); var progress = new HashMap<String,Map<String,WriterProgress>>();
        for (var entry : documents(row.get("expected"))) {
            if (!entry.keySet().equals(Set.of("table","writers")) || !(entry.get("writers") instanceof List<?> names)
                    || names.stream().anyMatch(name -> !(name instanceof String)) || expected.put(entry.getString("table"),names.stream().map(String.class::cast).toList()) != null) throw unreadable();
        }
        for (var entry : documents(row.get("progress"))) {
            if (!entry.keySet().equals(Set.of("table","writers"))) throw unreadable();
            var writers = new HashMap<String,WriterProgress>();
            for (var point : documents(entry.get("writers"))) {
                if (!point.keySet().equals(Set.of("writer","durableThrough","lastTokened")) || writers.put(point.getString("writer"),new WriterProgress(
                        order(point.get("durableThrough",Document.class)),position(point.get("lastTokened",Document.class)))) != null) throw unreadable();
            }
            if (writers.isEmpty() || progress.put(entry.getString("table"),writers) != null) throw unreadable();
        }
        if (row.get("snapshotEpoch") != null && !(row.get("snapshotEpoch") instanceof Long)) throw unreadable();
        return new DirectWriterRun(number(row,"epoch"),WorkloadClaimDocuments.readPipeline(row.get("commandFence",Document.class)),
                new WriterRun(row.getString("runId"),expected,progress,(Long)row.get("snapshotEpoch")));
    }
    private static Document order(SourceOrder value) { return new Document("epoch",value.epoch()).append("sequence",value.seq()); }
    private static SourceOrder order(Document row) {
        if (row == null || !row.keySet().equals(Set.of("epoch","sequence"))) throw unreadable();
        return new SourceOrder(number(row,"epoch"),number(row,"sequence"));
    }
    private static Document position(ChainPosition value) { return value == null ? null : order(value.order()).append("token",value.token()); }
    private static ChainPosition position(Document row) {
        if (row == null) return null;
        if (!row.keySet().equals(Set.of("epoch","sequence","token"))) throw unreadable();
        return new ChainPosition(new SourceOrder(number(row,"epoch"),number(row,"sequence")),text(row,"token"));
    }
    private static Map<String,Long> sequences(Object values) {
        var result = new HashMap<String,Long>();
        for (var entry : documents(values)) if (!entry.keySet().equals(Set.of("table","sequence")) || result.put(entry.getString("table"),number(entry,"sequence")) != null) throw unreadable();
        return result;
    }
    private static List<Document> documents(Object values) {
        if (!(values instanceof List<?> rows)) throw unreadable(); var result = new ArrayList<Document>();
        for (var row : rows) { if (!(row instanceof Document document)) throw unreadable(); result.add(document); } return result;
    }
    private static Set<String> strings(Object values) {
        if (!(values instanceof List<?> names)) throw unreadable(); var result = new TreeSet<String>();
        for (var name : names) if (!(name instanceof String text) || text.isBlank() || !result.add(text)) throw unreadable();
        if (!sorted(result).equals(names)) throw unreadable(); return result;
    }
    private static long number(Document row,String field) { if (!(row.get(field) instanceof Long value)) throw unreadable(); return value; }
    private static String text(Document row,String field) { Object value = row.get(field); if (value != null && !(value instanceof String)) throw unreadable(); return (String)value; }
    private static List<String> sorted(Set<String> values) { return new ArrayList<>(new TreeSet<>(values)); }
    private static Document hash(Document row) { return row.append("contentHash",CanonicalHash.ofText(row.toJson())); }
    static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","direct-channel","field","progress-or-batch"),null); }
}
