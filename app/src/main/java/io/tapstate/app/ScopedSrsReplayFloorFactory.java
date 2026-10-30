package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.runtime.engine.ReplayFloor;
import io.tapstate.runtime.engine.ReplayFloorFactory;
import java.util.*;

/** Operator reclamation uses the same original consumer and table as its sink acknowledgement. */
final class ScopedSrsReplayFloorFactory implements ReplayFloorFactory {
    private static final long serialVersionUID = 1L;
    private final Map<String,ScopedSrsSinkAckFactory.Source> sources;
    private final Map<String,ScopedDirectSinkAckFactory.Source> direct;
    ScopedSrsReplayFloorFactory(Map<String,ScopedSrsSinkAckFactory.Source> sources) { this(sources,Map.of()); }
    ScopedSrsReplayFloorFactory(Map<String,ScopedSrsSinkAckFactory.Source> sources,Map<String,ScopedDirectSinkAckFactory.Source> direct) {
        this.sources = Map.copyOf(sources); this.direct = Map.copyOf(direct);
        if (!Collections.disjoint(sources.keySet(),direct.keySet())) throw new IllegalArgumentException("replay source kinds overlap");
    }
    @Override public ReplayFloor resolve(HazelcastInstance member) {
        var bound = new HashMap<io.tapstate.spi.store.SrsConsumerRun,ScopedSrsConsumerAccess.Bound>();
        sources.values().forEach(source -> bound.computeIfAbsent(source.run(),run -> ScopedSrsConsumerAccess.bind(member,run)));
        var privateBound = new HashMap<DirectSourceRun,ScopedDirectSinkAckFactory.Bound>();
        direct.values().forEach(source -> privateBound.computeIfAbsent(source.run(),run -> ScopedDirectSinkAckFactory.bind(member,run)));
        return stream -> {
            var privateSource = direct.get(stream);
            if (privateSource != null) {
                var cursor=privateBound.get(privateSource.run()).progress().cursor();
                var settled=cursor.sinkAcked();var delivered=cursor.sinkAckedByTable().get(privateSource.table());
                if(settled==null || delivered==null)return Optional.empty();
                // A renamed baseline or empty source batch proves no new delivery on this table.
                return Optional.of(delivered.order().compareTo(settled.order())<0?delivered.order():settled.order());
            }
            var source = sources.get(stream); if (source == null) return Optional.empty();
            var progress = bound.get(source.run()).progress();
            return Optional.ofNullable(progress.cursor().sinkAckedByTable().get(source.table())).map(io.tapstate.core.event.ChainPosition::order);
        };
    }
}
