package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.*;

/** Immutable original capture results consumed by one deferred Pipeline topology. */
record PipelineCaptureBindings(ExecutionFence original,Map<String,LoggedSource> logged,Map<String,DirectSourceRun> direct,boolean freshFullLoad) {
    PipelineCaptureBindings(ExecutionFence original,Map<String,LoggedSource> logged,boolean freshFullLoad) {
        this(original,logged,Map.of(),freshFullLoad);
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
        Objects.requireNonNull(original,"original"); logged = Map.copyOf(logged); direct = Map.copyOf(direct);
        if (original.scope() == null) throw new IllegalArgumentException("capture bindings require a physical Pipeline scope");
        if (!Collections.disjoint(logged.keySet(),direct.keySet())) throw refused();
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
    String bufferKey() { return "capture-"+original.operatorExecutionKey(); }
    private static TapstateException refused() { return new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null); }
}
