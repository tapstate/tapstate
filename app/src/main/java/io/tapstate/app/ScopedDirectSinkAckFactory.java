package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.LoadLandings;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.spi.store.*;
import java.io.Serializable;
import java.util.*;

/** Native private-channel ACKs settle through the original CACHE transaction and writer roster. */
final class ScopedDirectSinkAckFactory implements SinkAckFactory {
    private static final long serialVersionUID = 1L;
    record Source(DirectSourceRun run,String table) implements Serializable {
        Source {
            Objects.requireNonNull(run,"run");
            if (table == null || !run.tables().contains(table)) throw new IllegalArgumentException("table is not in the original private source");
        }
    }
    private final Map<String,Source> sources;
    private final String runId;
    ScopedDirectSinkAckFactory(Map<String,Source> sources,String runId) {
        this.sources = Map.copyOf(sources); this.runId = Objects.requireNonNull(runId,"runId");
        if (sources.isEmpty() || runId.isBlank()) throw new IllegalArgumentException("writer accounting requires original sources");
        var original = sources.values().iterator().next().run().execution();
        if (sources.values().stream().anyMatch(source -> !original.equals(source.run().execution())))
            throw new IllegalArgumentException("private sources must belong to the same original execution");
    }
    @Override public void beginRun(HazelcastInstance member,Map<String,List<String>> writers) { beginRun(member,writers,null); }
    @Override public void beginRun(HazelcastInstance member,Map<String,List<String>> writers,WorkloadClaimFence claim) {
        if (!writers.keySet().equals(sources.keySet())) throw new IllegalStateException("the writer roster must name every compiled source stream");
        var grouped = new LinkedHashMap<DirectSourceRun,Map<String,Set<String>>>();
        writers.forEach((stream,names) -> {
            var source = source(stream); grouped.computeIfAbsent(source.run(),ignored -> new TreeMap<>())
                    .computeIfAbsent(source.table(),ignored -> new TreeSet<>()).addAll(names);
        });
        grouped.forEach((run,tables) -> {
            var expected = new TreeMap<String,List<String>>(); tables.forEach((table,names) -> expected.put(table,List.copyOf(names)));
            var bound = bind(member,run); bound.requireClaim(claim); bound.progress();
            if (!bound.store().beginWriterRun(run.context(),run.epoch(),runId,expected,run.pipelineClaim())) throw run.refused();
        });
    }
    @Override public SinkAck resolve(HazelcastInstance member) {
        var bound = bindings(member); return new Writer(bound,null);
    }
    @Override public LoadLandings loadLandings(HazelcastInstance member) {
        var bound = bindings(member);
        return awaited -> {
            var read = new HashMap<DirectSourceRun,DirectChannelProgress>();
            return awaited.stream().filter(load -> {
                var source = source(load.table()); var run = source.run();
                var progress = read.computeIfAbsent(run,ignored -> bound.get(run).progress());
                if (progress.cursor().snapshotCompletedTables().contains(source.table())) return false;
                var writers = progress.writerRun(); long snapshot = progress.cursor().snapshotEpoch();
                if (snapshot == 0 || writers == null || !writers.accounting().runId().equals(runId)) return true;
                return WriterFrontier.landed(writers.accounting(),source.table(),load.writers())
                        .filter(landed -> WriterFrontier.passedLoad(landed,snapshot)).isEmpty();
            }).toList();
        };
    }
    private Map<DirectSourceRun,Bound> bindings(HazelcastInstance member) {
        var bound = new HashMap<DirectSourceRun,Bound>();
        sources.values().forEach(source -> bound.computeIfAbsent(source.run(),run -> bind(member,run))); return bound;
    }
    private Source source(String stream) {
        var source = sources.get(stream); if (source == null) throw new IllegalStateException("writer reported an uncompiled private stream: "+stream); return source;
    }
    static Bound bind(HazelcastInstance member,DirectSourceRun run) {
        var authorization = ExecutionAuthorization.of(member);
        Runnable active = () -> { if (!run.pipelineClaim().equals(authorization.require(run.execution()))) throw run.refused(); };
        active.run();
        if (!(member.getUserContext().get(SrsStoreViews.USER_CONTEXT_KEY) instanceof SrsStoreViews views)) throw run.refused();
        return new Bound(run,views.directChannels(run.cacheScope(),active),active);
    }
    record Bound(DirectSourceRun run,DirectChannelStore store,Runnable active) {
        void requireClaim(WorkloadClaimFence claim) { active.run(); if (claim != null && !run.pipelineClaim().equals(claim)) throw run.refused(); }
        DirectChannelProgress progress() { active.run(); var current = store.load(run.context()); active.run(); run.requireOriginal(current); return current; }
    }
    private final class Writer implements SinkAck {
        private static final long serialVersionUID = 1L;
        private final transient Map<DirectSourceRun,Bound> bound;
        private final String writer;
        Writer(Map<DirectSourceRun,Bound> bound,String writer) { this.bound = bound; this.writer = writer; }
        @Override public SinkAck forWriter(String writer) {
            if (writer == null || writer.isBlank()) throw new IllegalArgumentException("blank private writer"); return new Writer(bound,writer);
        }
        @Override public void advance(String stream,ChainPosition point) { advance(stream,point,null); }
        @Override public void advance(String stream,ChainPosition point,WorkloadClaimFence claim) {
            Objects.requireNonNull(point,"position"); report(stream,new WriterProgress(point.order(),point.token()==null?null:point),claim);
        }
        @Override public void bounded(String stream,SourceOrder through) { bounded(stream,through,null); }
        @Override public void bounded(String stream,SourceOrder through,WorkloadClaimFence claim) { report(stream,new WriterProgress(through,null),claim); }
        private void report(String stream,WriterProgress progress,WorkloadClaimFence claim) {
            if (writer == null) throw new IllegalStateException("a private acknowledgement must name its original writer");
            var source = source(stream); var run = source.run(); var handle = bound.get(run); handle.requireClaim(claim);
            if (!handle.store().advanceWriter(run.context(),run.epoch(),runId,source.table(),writer,progress,run.pipelineClaim())) throw run.refused();
        }
    }
}
