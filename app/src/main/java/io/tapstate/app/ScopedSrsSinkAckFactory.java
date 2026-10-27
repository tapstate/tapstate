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
import io.tapstate.app.ScopedSrsConsumerAccess.Bound;
import static io.tapstate.app.ScopedSrsConsumerAccess.bind;
import static io.tapstate.app.ScopedSrsConsumerAccess.refused;

/** Writer callbacks retain one prepared source membership and execution; only coordinates cross the wire. */
final class ScopedSrsSinkAckFactory implements SinkAckFactory {
    private static final long serialVersionUID = 1L;
    record Source(SrsConsumerRun run,String table) implements Serializable {
        Source { Objects.requireNonNull(run,"run"); if (table == null || table.isBlank()) throw new IllegalArgumentException("blank consumer table"); }
    }
    private final Map<String,Source> sources;
    private final String runId;
    private final Map<String,ScopedDirectSinkAckFactory.Source> directSources;
    private final ScopedDirectSinkAckFactory directFactory;
    ScopedSrsSinkAckFactory(Map<String,Source> sources,String runId) { this(sources,Map.of(),runId); }
    ScopedSrsSinkAckFactory(Map<String,Source> sources,Map<String,ScopedDirectSinkAckFactory.Source> directSources,String runId) {
        this.sources = Map.copyOf(sources); this.runId = Objects.requireNonNull(runId,"runId");
        this.directSources = Map.copyOf(directSources);
        this.directFactory = directSources.isEmpty()?null:new ScopedDirectSinkAckFactory(directSources,runId);
        if (sources.isEmpty() && directSources.isEmpty() || runId.isBlank() || !Collections.disjoint(sources.keySet(),directSources.keySet()))
            throw new IllegalArgumentException("writer accounting requires disjoint original source bindings and a run id");
        ExecutionFence original = sources.isEmpty()?directSources.values().iterator().next().run().execution():
                new ExecutionFence(sources.values().iterator().next().run().scope(),sources.values().iterator().next().run().originalClaim());
        if (sources.values().stream().anyMatch(source -> !original.equals(new ExecutionFence(source.run().scope(),source.run().originalClaim())))
                || directSources.values().stream().anyMatch(source -> !original.equals(source.run().execution())))
            throw new IllegalArgumentException("source bindings must share one original Pipeline execution");
    }
    @Override public void beginRun(HazelcastInstance member,Map<String,List<String>> writers) { beginRun(member,writers,null); }
    @Override public void beginRun(HazelcastInstance member,Map<String,List<String>> writers,WorkloadClaimFence claimed) {
        var all = new HashSet<>(sources.keySet()); all.addAll(directSources.keySet());
        if (!writers.keySet().equals(all)) throw new IllegalStateException("the writer roster must name every compiled source stream");
        if (directFactory != null) {
            var selected = new LinkedHashMap<String,List<String>>();
            writers.forEach((stream,names) -> { if (directSources.containsKey(stream)) selected.put(stream,names); });
            directFactory.beginRun(member,selected,claimed);
        }
        var grouped = new LinkedHashMap<SrsConsumerRun,Map<String,Set<String>>>();
        writers.forEach((stream,names) -> {
            if (directSources.containsKey(stream)) return;
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
        return new Writer(bound,null,directFactory==null?null:directFactory.resolve(member));
    }
    @Override public LoadLandings loadLandings(HazelcastInstance member) {
        var bound = new HashMap<SrsConsumerRun,Bound>(); sources.values().forEach(source -> bound.computeIfAbsent(source.run(),run -> bind(member,run)));
        var directLandings = directFactory==null?null:directFactory.loadLandings(member);
        return awaited -> {
            var pendingDirect = directLandings==null?List.<io.tapstate.core.lifecycle.AwaitedLoad>of():
                    directLandings.stillLanding(awaited.stream().filter(load -> directSources.containsKey(load.table())).toList());
            var held = new HashMap<SrsConsumerRun,SrsConsumerProgress>();
            return awaited.stream().filter(load -> {
                if (directSources.containsKey(load.table())) return pendingDirect.contains(load);
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
    private final class Writer implements SinkAck {
        private static final long serialVersionUID = 1L;
        private final transient Map<SrsConsumerRun,Bound> bound;
        private final String writer;
        private final SinkAck direct;
        Writer(Map<SrsConsumerRun,Bound> bound,String writer,SinkAck direct) { this.bound = bound; this.writer = writer; this.direct = direct; }
        @Override public SinkAck forWriter(String writer) {
            if (writer == null || writer.isBlank()) throw new IllegalArgumentException("blank original writer"); return new Writer(bound,writer,direct==null?null:direct.forWriter(writer));
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
            if (directSources.containsKey(stream)) {
                if (progress.lastTokened() == null) direct.bounded(stream,progress.durableThrough(),claimed);
                else direct.advance(stream,progress.lastTokened(),claimed);
                return;
            }
            var source = source(stream); var run = source.run(); var handle = bound.get(run); handle.requireClaim(claimed);
            if (!handle.store().advanceWriter(run.context(),run.membershipRevision(),runId,source.table(),writer,progress,run.originalClaim())) throw refused(run);
        }
    }
}
