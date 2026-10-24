package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.*;

/** Immutable original capture results consumed by one deferred Pipeline topology. */
record PipelineCaptureBindings(ExecutionFence original,Map<String,LoggedSource> logged,Map<String,DirectSourceRun> direct,
        Map<String,SnapshotOnlySource> snapshots,boolean freshFullLoad) {
    record SnapshotOnlySource(String chain,Set<String> tables,long epoch) {
        SnapshotOnlySource {
            Objects.requireNonNull(chain,"chain"); tables=Set.copyOf(tables);
            if (chain.isBlank() || tables.isEmpty() || tables.stream().anyMatch(String::isBlank) || epoch<1 || epoch==Long.MAX_VALUE)
                throw new IllegalArgumentException("a bounded source requires its original selection and snapshot order");
        }
    }
    PipelineCaptureBindings(ExecutionFence original,Map<String,LoggedSource> logged,Map<String,DirectSourceRun> direct,boolean freshFullLoad) {
        this(original,logged,direct,Map.of(),freshFullLoad);
    }
    PipelineCaptureBindings(ExecutionFence original,Map<String,LoggedSource> logged,boolean freshFullLoad) {
        this(original,logged,Map.of(),Map.of(),freshFullLoad);
    }
    record LoggedSource(SrsConsumerRun run,long snapshotEpoch,Map<String,Long> recoveryAfter) {
        LoggedSource {
            Objects.requireNonNull(run,"run"); recoveryAfter = Map.copyOf(recoveryAfter);
            if (snapshotEpoch < 1 || snapshotEpoch == Long.MAX_VALUE || recoveryAfter.isEmpty()
                    || recoveryAfter.entrySet().stream().anyMatch(e -> e.getKey().isBlank() || e.getValue() < -1 || e.getValue() == Long.MAX_VALUE))
                throw new IllegalArgumentException("source binding requires original table recovery floors and snapshot order");
        }
    }
    PipelineCaptureBindings {
        Objects.requireNonNull(original,"original"); logged = Map.copyOf(logged); direct = Map.copyOf(direct); snapshots=Map.copyOf(snapshots);
        if (original.scope() == null) throw new IllegalArgumentException("capture bindings require a physical Pipeline scope");
        if (!Collections.disjoint(logged.keySet(),direct.keySet()) || !snapshots.isEmpty() && (!logged.isEmpty() || !direct.isEmpty())
                || snapshots.values().stream().map(SnapshotOnlySource::epoch).distinct().count()>1) throw refused();
        for (var entry : direct.entrySet()) if (!entry.getKey().equals(entry.getValue().context().sourceId())
                || !original.equals(entry.getValue().execution())) throw refused();
        for (var entry : logged.entrySet()) {
            var run = entry.getValue().run();
            if (!entry.getKey().equals(run.sourceId()) || !original.equals(new ExecutionFence(run.scope(),run.originalClaim()))) throw refused();
        }
    }
    LoggedSource require(String sourceId,String chain,String table) {
        var source = logged.get(sourceId);
        if (source == null || !source.run().miningChainId().equals(chain) || !source.recoveryAfter().containsKey(table)) throw refused();
        return source;
    }
    DirectSourceRun requireDirect(String sourceId,String channel,String table) {
        var source = direct.get(sourceId);
        if (source == null || !source.context().channelId().equals(channel) || !source.tables().contains(table)) throw refused();
        return source;
    }
    SnapshotOnlySource requireSnapshot(String sourceId,String chain,String table) {
        var source=snapshots.get(sourceId);
        if (source==null || !source.chain().equals(chain) || !source.tables().contains(table)) throw refused(); return source;
    }
    String bufferKey(String sourceId) {
        if (!logged.containsKey(sourceId) && !direct.containsKey(sourceId) && !snapshots.containsKey(sourceId)) throw refused();
        return bufferKey(original,sourceId);
    }
    static String bufferKey(ExecutionFence original,String sourceId) {
        return "capture-"+original.operatorExecutionKey()+"-"+io.tapstate.core.model.canonical.CanonicalHash.ofText(sourceId);
    }
    private static TapstateException refused() { return new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null); }
}
