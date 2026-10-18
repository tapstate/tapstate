package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.LoadLandings;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.spi.store.*;
import java.io.Serializable;
import java.util.*;

/** Writer callbacks retain one prepared source membership and execution; only coordinates cross the wire. */
final class ScopedSrsSinkAckFactory implements SinkAckFactory {
    private static final long serialVersionUID = 1L;
    record Source(SrsConsumerRun run,String table) implements Serializable {
        Source { Objects.requireNonNull(run,"run"); if (table == null || table.isBlank()) throw new IllegalArgumentException("blank consumer table"); }
    }
    private final Map<String,Source> sources;
    private final String runId;
    ScopedSrsSinkAckFactory(Map<String,Source> sources,String runId) {
        this.sources = Map.copyOf(sources); this.runId = Objects.requireNonNull(runId,"runId");
        if (sources.isEmpty() || runId.isBlank()) throw new IllegalArgumentException("writer accounting requires original source bindings and a run id");
        var original = this.sources.values().iterator().next().run();
        if (this.sources.values().stream().anyMatch(source -> !source.run().scope().equals(original.scope())
                || !source.run().originalClaim().equals(original.originalClaim()))) throw new IllegalArgumentException("source bindings must share one original Pipeline execution");
    }
    @Override public void beginRun(HazelcastInstance member,Map<String,List<String>> writers) { beginRun(member,writers,null); }
    @Override public void beginRun(HazelcastInstance member,Map<String,List<String>> writers,WorkloadClaimFence claimed) {
        if (!writers.keySet().equals(sources.keySet())) throw new IllegalStateException("the writer roster must name every compiled source stream");
        var grouped = new LinkedHashMap<SrsConsumerRun,Map<String,Set<String>>>();
        writers.forEach((stream,names) -> {
            var source = source(stream); grouped.computeIfAbsent(source.run(),ignored -> new TreeMap<>())
                    .computeIfAbsent(source.table(),ignored -> new TreeSet<>()).addAll(names);
        });
        grouped.forEach((run,tables) -> {
            var expected = new TreeMap<String,List<String>>(); tables.forEach((table,names) -> expected.put(table,List.copyOf(names)));
            var bound = bind(member,run); bound.requireClaim(claimed);
            if (!bound.store().beginWriterRun(run.context(),run.membershipRevision(),runId,expected,run.originalClaim())) throw refused(run);
        });
    }
    @Override public SinkAck resolve(HazelcastInstance member) {
        var bound = new HashMap<SrsConsumerRun,Bound>(); sources.values().forEach(source -> bound.computeIfAbsent(source.run(),run -> bind(member,run)));
        return new Writer(bound,null);
    }
    @Override public LoadLandings loadLandings(HazelcastInstance member) {
        var bound = new HashMap<SrsConsumerRun,Bound>(); sources.values().forEach(source -> bound.computeIfAbsent(source.run(),run -> bind(member,run)));
        return awaited -> {
            var held = new HashMap<SrsConsumerRun,SrsConsumerProgress>();
            return awaited.stream().filter(load -> {
                var source = source(load.table()); var run = source.run();
                var progress = held.computeIfAbsent(run,ignored -> bound.get(run).progress());
                if (progress.cursor().snapshotCompletedTables().contains(source.table())) return false;
                var writers = progress.writerRun();
                if (writers == null || !writers.runId().equals(runId) || writers.membershipRevision() != run.membershipRevision()
                        || !writers.commandFence().equals(run.originalClaim()) || progress.snapshot() == null) return true;
                return WriterFrontier.landed(writers.withSnapshotEpoch(progress.snapshot().snapshotEpoch()),source.table(),load.writers())
                        .filter(landed -> WriterFrontier.passedLoad(landed,progress.snapshot().snapshotEpoch())).isEmpty();
            }).toList();
        };
    }
    private Source source(String stream) {
        var source = sources.get(stream); if (source == null) throw new IllegalStateException("writer reported an uncompiled source stream: " + stream); return source;
    }
    private static Bound bind(HazelcastInstance member,SrsConsumerRun run) {
        var authorization = ExecutionAuthorization.of(member); var fence = new ExecutionFence(run.scope(),run.originalClaim());
        Runnable requireActive = () -> { if (!run.originalClaim().equals(authorization.require(fence))) throw refused(run); };
        requireActive.run();
        if (!(member.getUserContext().get(SrsStoreViews.USER_CONTEXT_KEY) instanceof SrsStoreViews views)) throw refused(run);
        return new Bound(run,views.srs(run.cacheScope(),requireActive).consumers().guardedBy(requireActive),requireActive);
    }
    private record Bound(SrsConsumerRun run,SrsConsumerStore store,Runnable requireActive) {
        void requireClaim(WorkloadClaimFence claimed) { requireActive.run(); if (claimed != null && !run.originalClaim().equals(claimed)) throw refused(run); }
        SrsConsumerProgress progress() {
            requireActive.run(); var held = store.progress(run.context()).orElseThrow(() -> refused(run)); requireActive.run();
            if (held.membershipRevision() != run.membershipRevision()) throw refused(run); return held;
        }
    }
    private final class Writer implements SinkAck {
        private static final long serialVersionUID = 1L;
        private final transient Map<SrsConsumerRun,Bound> bound;
        private final String writer;
        Writer(Map<SrsConsumerRun,Bound> bound,String writer) { this.bound = bound; this.writer = writer; }
        @Override public SinkAck forWriter(String writer) {
            if (writer == null || writer.isBlank()) throw new IllegalArgumentException("blank original writer"); return new Writer(bound,writer);
        }
        @Override public void advance(String stream,ChainPosition position) { advance(stream,position,null); }
        @Override public void advance(String stream,ChainPosition position,WorkloadClaimFence claimed) {
            Objects.requireNonNull(position,"position");
            report(stream,new WriterProgress(position.order(),position.token() == null ? null : position),claimed);
        }
        @Override public void bounded(String stream,SourceOrder through) { bounded(stream,through,null); }
        @Override public void bounded(String stream,SourceOrder through,WorkloadClaimFence claimed) { report(stream,new WriterProgress(through,null),claimed); }
        private void report(String stream,WriterProgress progress,WorkloadClaimFence claimed) {
            if (writer == null) throw new IllegalStateException("an acknowledgement must name its original writer");
            var source = source(stream); var run = source.run(); var handle = bound.get(run); handle.requireClaim(claimed);
            if (!handle.store().advanceWriter(run.context(),run.membershipRevision(),runId,source.table(),writer,progress,run.originalClaim())) throw refused(run);
        }
    }
    private static TapstateException refused(SrsConsumerRun run) {
        return new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,Map.of("pipeline",run.scope().pipelineId()),null);
    }
}
