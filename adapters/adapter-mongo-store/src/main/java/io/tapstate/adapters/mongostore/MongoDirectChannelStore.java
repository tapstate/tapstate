package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.*;
import org.bson.Document;
import java.time.Duration;
import java.util.*;
import java.util.function.BiFunction;

/** Private source recovery: the checkpoint, writer progress and ordered settlement compete in one CACHE transaction. */
public final class MongoDirectChannelStore implements DirectChannelStore {
    private final MongoClient client;
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final Runnable requireActive;
    private final Duration retention;

    MongoDirectChannelStore(MongoClient client,MongoDatabase database,CacheBusinessScope scope,Runnable requireActive,Duration retention) {
        this.client = Objects.requireNonNull(client,"client"); this.database = Objects.requireNonNull(database,"database");
        this.scope = Objects.requireNonNull(scope,"scope"); this.requireActive = Objects.requireNonNull(requireActive,"requireActive");
        this.retention = MongoCacheMetadataStructure.requireRetention(retention);
    }
    @Override public DirectChannelStore guardedBy(Runnable additional) {
        Objects.requireNonNull(additional,"requireActive");
        return new MongoDirectChannelStore(client,database,scope,() -> { requireActive.run(); additional.run(); },retention);
    }
    @Override public DirectChannelProgress load(DirectChannelContext context) { return transaction(context,null,(session,held) -> held.value()); }
    @Override public List<DirectBatch> pending(DirectChannelContext context,long epoch) {
        return transaction(context,null,(session,held) -> {
            if (held.value().current() != null && held.value().current().epoch() == epoch) return held.current();
            if (held.value().retained() != null && held.value().retained().epoch() == epoch) return held.retained();
            throw unavailable(context);
        });
    }
    @Override public OptionalLong beginCapture(DirectChannelContext context,long expectedRevision,long expectedEpoch,WorkloadClaimFence capture,WorkloadClaimFence pipeline) {
        requireCounter(expectedRevision); requireCounter(expectedEpoch); requireCapture(context,capture);
        return transaction(context,pipeline,(session,held) -> {
            proveCapture(session,capture); var old = held.value(); var before = old.current();
            if (before != null && before.captureFence().equals(capture) && before.pipelineFence().equals(pipeline)) {
                return before.epoch() == expectedEpoch + 1 ? OptionalLong.of(before.epoch()) : OptionalLong.empty();
            }
            if (old.revision() != expectedRevision || old.epoch() != expectedEpoch) return OptionalLong.empty();
            if (old.epoch() >= Long.MAX_VALUE - 1 || before != null && old.cursor().sinkAcked() == null) throw unavailable(context);
            var retained = before != null && before.pendingCount() > 0 ? before : old.retained();
            var current = new DirectCaptureWindow(old.epoch()+1,capture,pipeline,null,0,0,null,null);
            replace(session,old,next(old,current,retained,old.cursor(),null),List.of(),before != null && before.pendingCount() > 0 ? held.current() : held.retained());
            return OptionalLong.of(current.epoch());
        });
    }
    @Override public boolean confirmAnchor(DirectChannelContext context,long epoch,String sourcePosition,WorkloadClaimFence capture,WorkloadClaimFence pipeline) {
        requireToken(sourcePosition); requireCapture(context,capture);
        return transaction(context,pipeline,(session,held) -> {
            proveCapture(session,capture); var old = held.value(); var current = original(old,epoch,capture,pipeline);
            if (current == null) return false;
            if (current.anchor() != null) return current.anchor().equals(sourcePosition);
            boolean overriding = old.resumeOverride() != null && epoch > old.resumeOverride().fromEpoch();
            if (overriding ? !old.resumeOverride().token().equals(sourcePosition)
                    : old.cursor().sinkAcked() != null && !old.cursor().sinkAcked().token().equals(sourcePosition)) throw unavailable(context);
            if (old.retained() != null) remove(session,held.retained());
            var cursor = cursor(old.cursor(),new ChainPosition(SourceOrder.snapshotRow(epoch),sourcePosition));
            var anchored = new DirectCaptureWindow(epoch,capture,pipeline,sourcePosition,0,0,null,null);
            if (old.revision() >= Long.MAX_VALUE - 1) throw unavailable(context);
            var next = new DirectChannelProgress(old.context(),old.tables(),old.revision()+1,anchored,null,cursor,old.writerRun(),overriding?null:old.resumeOverride());
            replace(session,old,next,List.of(),List.of()); return true;
        });
    }
    @Override public boolean pinSnapshot(DirectChannelContext context,long expectedRevision,long snapshotEpoch,String sourcePosition,WorkloadClaimFence pipeline) {
        requireCounter(expectedRevision); requireToken(sourcePosition);
        if (snapshotEpoch < 1 || snapshotEpoch == Long.MAX_VALUE) throw new IllegalArgumentException("invalid original direct snapshot epoch");
        return transaction(context,pipeline,(session,held) -> {
            var old = held.value(); var cursor = old.cursor();
            if (old.current() == null || !old.current().pipelineFence().equals(pipeline)) throw fenced();
            if (cursor.snapshotEpoch() > 0) return cursor.snapshotEpoch() == snapshotEpoch && sourcePosition.equals(cursor.cdcStartPosition());
            if (old.revision() != expectedRevision) return false;
            if (snapshotEpoch != old.epoch() || old.writerRun() != null || !cursor.snapshotCompletedTables().isEmpty()) throw unavailable(context);
            var pinned = new ConsumerOffset(cursor.pipelineId(),Map.of(),cursor.sinkAcked(),List.of(),sourcePosition,snapshotEpoch,cursor.sinkAckedByTable(),ConsumerProgressKind.DIRECT_SOURCE);
            replace(session,old,next(old,old.current(),old.retained(),pinned,null),held.current(),held.retained()); return true;
        });
    }
    @Override public boolean beginWriterRun(DirectChannelContext context,long epoch,String runId,Map<String,List<String>> expected,WorkloadClaimFence pipeline) {
        var request = new DirectWriterRun(epoch,pipeline,new WriterRun(runId,expected,Map.of(),null));
        return transaction(context,pipeline,(session,held) -> {
            var old = held.value(); var current = old.current();
            if (current == null || current.epoch() != epoch) return false;
            if (!current.pipelineFence().equals(pipeline)) throw fenced();
            if (!old.tables().equals(request.accounting().expected().keySet())) throw unavailable(context);
            var before = old.writerRun();
            if (before != null) return before.commandFence().equals(pipeline) && before.accounting().runId().equals(runId)
                    && before.accounting().expected().equals(request.accounting().expected());
            var accounting = request.accounting(); var run = new DirectWriterRun(epoch,pipeline,new WriterRun(runId,accounting.expected(),Map.of(),old.cursor().snapshotEpoch() == 0 ? null : old.cursor().snapshotEpoch()));
            replace(session,old,next(old,current,old.retained(),old.cursor(),run),held.current(),held.retained()); return true;
        });
    }
    @Override public boolean recordBatch(DirectBatch batch,long expectedRecordedThrough,WorkloadClaimFence capture,WorkloadClaimFence pipeline) {
        requireCounter(expectedRecordedThrough); var context = batch.context(); requireCapture(context,capture);
        return transaction(context,pipeline,(session,held) -> {
            proveCapture(session,capture); var old = held.value(); var current = original(old,batch.epoch(),capture,pipeline);
            if (current == null) return false;
            if (current.anchor() == null || !old.tables().containsAll(batch.targets().keySet())) throw unavailable(context);
            if (batch.ordinal() <= current.recordedThrough()) {
                if (batch.ordinal() == current.recordedThrough()) return batch.boundary().equals(current.lastBoundary()) && MongoDirectChannelRecords.batchHash(batch).equals(current.lastBatchHash());
                return held.current().stream().anyMatch(batch::equals);
            }
            if (current.recordedThrough() != expectedRecordedThrough || batch.ordinal() != expectedRecordedThrough+1) return false;
            if (current.lastBoundary() != null && batch.boundary().order().compareTo(current.lastBoundary().order()) <= 0) throw unavailable(context);
            SystemCollections.DIRECT_BATCHES.on(database).insertOne(session,MongoDirectChannelRecords.batch(batch));
            var added = new DirectCaptureWindow(current.epoch(),capture,pipeline,current.anchor(),batch.ordinal(),current.settledThrough(),batch.boundary(),MongoDirectChannelRecords.batchHash(batch));
            var pending = new ArrayList<>(held.current()); pending.add(batch);
            settleAndReplace(session,old,added,old.cursor(),old.writerRun(),pending); return true;
        });
    }
    @Override public boolean advanceWriter(DirectChannelContext context,long epoch,String runId,String table,String writer,WriterProgress report,WorkloadClaimFence pipeline) {
        Objects.requireNonNull(report,"report");
        return transaction(context,pipeline,(session,held) -> {
            var old = held.value(); var current = old.current(); var run = old.writerRun();
            if (current == null || current.epoch() != epoch || run == null || !run.accounting().runId().equals(runId)) return false;
            if (!current.pipelineFence().equals(pipeline) || !run.commandFence().equals(pipeline)) throw fenced();
            if (!run.accounting().expectedFor(table).contains(writer)) throw unavailable(context);
            if (current.anchor() == null && !snapshotBeforeRequestedAnchor(old, report)) throw unavailable(context);
            var next = DirectChannelAccounting.report(old,table,writer,report);
            if (!next.cursor().equals(old.cursor()) || !next.writers().equals(run)) {
                if (current.anchor() == null) replace(session,old,next(old,current,old.retained(),next.cursor(),next.writers()),held.current(),held.retained());
                else settleAndReplace(session,old,current,next.cursor(),next.writers(),held.current());
            }
            return true;
        });
    }
    private static boolean snapshotBeforeRequestedAnchor(DirectChannelProgress old, WriterProgress report) {
        var pending = old.resumeOverride(); var cursor = old.cursor();
        if (pending == null || old.epoch() <= pending.fromEpoch() || cursor.snapshotEpoch() < 1) return false;
        if (report.durableThrough().epoch() == 0) return report.lastTokened() == null;
        var row = SourceOrder.snapshotRow(cursor.snapshotEpoch());
        if (!report.durableThrough().equals(row) && !report.durableThrough().equals(new SourceOrder(cursor.snapshotEpoch(), Long.MAX_VALUE))) return false;
        return report.lastTokened() == null || row.equals(report.lastTokened().order()) && cursor.cdcStartPosition().equals(report.lastTokened().token());
    }
    void editPosition(ClientSession session, DirectChannelProgress expected, String token, java.time.Instant at,
                      WorkloadClaimFence capture, WorkloadClaimFence pipeline) {
        var context = expected.context();
        requireCapture(context, capture); proveCapture(session, capture);
        var held = held(session, context); var old = held.value();
        if (!old.equals(expected) || old.cursor().sinkAcked() == null || token.isBlank()
                || old.current() == null || !MongoPositionFences.retained(old.current().pipelineFence(), pipeline)) throw unavailable(context);
        var before = old.cursor();
        var cursor = new ConsumerOffset(before.pipelineId(), Map.of(), before.sinkAcked(), before.snapshotCompletedTables(),
                before.cdcStartPosition(), before.snapshotEpoch(), Map.of(), ConsumerProgressKind.DIRECT_SOURCE);
        if (old.revision() >= Long.MAX_VALUE - 1) throw unavailable(context);
        var next = new DirectChannelProgress(context, old.tables(), old.revision()+1, old.current(), old.retained(), cursor, null,
                new SourceResumeOverride(token, old.epoch(), at));
        replace(session, old, next, held.current(), held.retained());
    }
    private void settleAndReplace(ClientSession session,DirectChannelProgress before,DirectCaptureWindow window,ConsumerOffset cursor,DirectWriterRun run,List<DirectBatch> pending) {
        var prospective = new DirectChannelProgress(before.context(),before.tables(),before.revision(),window,null,cursor,run,before.resumeOverride());
        var settled = DirectChannelAccounting.settle(prospective,pending);
        remove(session,pending.stream().filter(batch -> batch.ordinal() <= settled.through()).toList());
        var nextWindow = new DirectCaptureWindow(window.epoch(),window.captureFence(),window.pipelineFence(),window.anchor(),window.recordedThrough(),settled.through(),window.lastBoundary(),window.lastBatchHash());
        replace(session,before,next(before,nextWindow,null,settled.cursor(),run),pending.stream().filter(batch -> batch.ordinal() > settled.through()).toList(),List.of());
    }
    private void remove(ClientSession session,List<DirectBatch> batches) {
        for (var batch : batches) if (SystemCollections.DIRECT_BATCHES.on(database).deleteOne(session,
                new Document("_id",MongoDirectChannelRecords.batchId(batch.context(),batch.epoch(),batch.ordinal())).append("contentHash",MongoDirectChannelRecords.batchHash(batch))).getDeletedCount() != 1)
            throw unavailable(batch.context());
    }
    private void replace(ClientSession session,DirectChannelProgress before,DirectChannelProgress after,List<DirectBatch> current,List<DirectBatch> retained) {
        if (SystemCollections.DIRECT_CHANNEL_OFFSETS.on(database).replaceOne(session,new Document("_id",MongoDirectChannelRecords.id(before.context()))
                .append("revision",before.revision()),MongoDirectChannelRecords.progress(after,current,retained)).getMatchedCount() != 1) throw unavailable(before.context());
    }
    private static DirectChannelProgress next(DirectChannelProgress old,DirectCaptureWindow current,DirectCaptureWindow retained,ConsumerOffset cursor,DirectWriterRun run) {
        if (old.revision() >= Long.MAX_VALUE - 1) throw unavailable(old.context());
        return new DirectChannelProgress(old.context(),old.tables(),old.revision()+1,current,retained,cursor,run,old.resumeOverride());
    }
    private record Held(DirectChannelProgress value,List<DirectBatch> current,List<DirectBatch> retained) {}
    void requireChannel(ClientSession session,DirectChannelContext context) { held(session,context); }
    private Held held(ClientSession session,DirectChannelContext context) {
        var row = SystemCollections.DIRECT_CHANNEL_OFFSETS.on(database).find(session,new Document("_id",MongoDirectChannelRecords.id(context))).first();
        if (row == null) throw unavailable(context);
        var progress = MongoDirectChannelRecords.progress(row);
        MongoPipelineSourcePreparations.requireDirect(session,database,context,progress.tables());
        var byEpoch = new HashMap<Long,List<DirectBatch>>();
        for (var batchRow : SystemCollections.DIRECT_BATCHES.on(database).find(session,new Document("_id.channel",MongoDirectChannelRecords.id(context)))) {
            var batch = MongoDirectChannelRecords.batch(batchRow); if (!batch.context().equals(context)) throw unavailable(context);
            byEpoch.computeIfAbsent(batch.epoch(),ignored -> new ArrayList<>()).add(batch);
        }
        var current = closure(context,progress.tables(),progress.current(),byEpoch); var retained = closure(context,progress.tables(),progress.retained(),byEpoch);
        if (progress.current() != null && progress.current().anchor() != null) {
            try { DirectChannelAccounting.settle(progress,current); }
            catch (IllegalArgumentException inconsistent) { throw unavailable(context); }
        }
        if (!byEpoch.isEmpty() || !MongoDirectChannelRecords.pendingHash(current).equals(row.get("currentPendingHash"))
                || !MongoDirectChannelRecords.pendingHash(retained).equals(row.get("retainedPendingHash"))) throw unavailable(context);
        return new Held(progress,current,retained);
    }
    private static List<DirectBatch> closure(DirectChannelContext context,Set<String> tables,DirectCaptureWindow window,Map<Long,List<DirectBatch>> byEpoch) {
        if (window == null) return List.of();
        var rows = byEpoch.remove(window.epoch()); if (rows == null) rows = new ArrayList<>(); rows.sort(Comparator.comparingLong(DirectBatch::ordinal));
        if (rows.size() != window.pendingCount()) throw unavailable(context);
        long ordinal = window.settledThrough(); SourceOrder previous = null;
        for (var batch : rows) {
            if (!tables.containsAll(batch.targets().keySet()) || batch.ordinal() != ++ordinal || previous != null && batch.boundary().order().compareTo(previous) <= 0) throw unavailable(context);
            previous = batch.boundary().order();
        }
        if (!rows.isEmpty() && (!rows.getLast().boundary().equals(window.lastBoundary()) || !MongoDirectChannelRecords.batchHash(rows.getLast()).equals(window.lastBatchHash()))) throw unavailable(context);
        return List.copyOf(rows);
    }
    private <T> T transaction(DirectChannelContext context,WorkloadClaimFence pipeline,BiFunction<ClientSession,Held,T> action) {
        Objects.requireNonNull(context,"context");
        if (!scope.equals(new CacheBusinessScope(context.scope().clusterId(),context.scope().bindingVersion(),context.scope().cacheIncarnation()))) throw fenced();
        var access = new MongoCachePipelineAccess(client,database,context.scope(),pipeline,requireActive,retention);
        return access.metadataTransaction(SystemCollections.DIRECT_CHANNEL_OFFSETS,pipeline != null,session -> {
            new MongoCacheMetadataStructure(database,retention).verifyDirect();
            return action.apply(session,held(session,context));
        });
    }
    private static DirectCaptureWindow original(DirectChannelProgress progress,long epoch,WorkloadClaimFence capture,WorkloadClaimFence pipeline) {
        var current = progress.current(); if (current == null || current.epoch() != epoch) return null;
        if (!current.captureFence().equals(capture) || !current.pipelineFence().equals(pipeline)) throw fenced(); return current;
    }
    private void proveCapture(ClientSession session,WorkloadClaimFence capture) {
        var table = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
        var row = table.find(session,new Document("_id",MongoWorkloadClaimStore.id(capture.key()))).first();
        if (row == null) throw fenced();
        var original = new MongoWorkloadClaimStore(table).inStrictSession(session).decodeStored(row);
        if (!capture.equals(WorkloadClaimFence.from(original))) throw fenced();
        if (SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(session,WorkloadClaimDocuments.live(capture),
                new Document("$inc",new Document("fencedDirectWrites",1L))).getMatchedCount() != 1) throw fenced();
    }
    private static void requireCapture(DirectChannelContext context,WorkloadClaimFence capture) {
        if (capture == null || capture.key().type() != WorkloadClaimType.CAPTURE || !capture.key().clusterId().equals(context.scope().clusterId())
                || !capture.key().resourceId().equals(context.captureId())) throw fenced();
    }
    private static ConsumerOffset cursor(ConsumerOffset old,ChainPosition confirmed) {
        return new ConsumerOffset(old.pipelineId(),Map.of(),confirmed,old.snapshotCompletedTables(),old.cdcStartPosition(),old.snapshotEpoch(),old.sinkAckedByTable(),ConsumerProgressKind.DIRECT_SOURCE);
    }
    private static void requireCounter(long value) { if (value < 0 || value == Long.MAX_VALUE) throw new IllegalArgumentException("invalid original direct revision"); }
    private static void requireToken(String token) { if (token == null || token.isBlank()) throw new IllegalArgumentException("an actual direct source token is required"); }
    private static TapstateException fenced() { return MongoCachePipelineAccess.claimFenced(); }
    private static TapstateException unavailable(DirectChannelContext context) { return new TapstateException(IoError.DIRECT_CHANNEL_UNAVAILABLE,Map.of("channel",context.channelId()),null); }
}
