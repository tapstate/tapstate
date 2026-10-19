package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.runtime.engine.ReplayFloor;
import io.tapstate.runtime.engine.ReplayFloorFactory;
import java.util.*;

/** Operator reclamation uses the same original consumer and table as its sink acknowledgement. */
final class ScopedSrsReplayFloorFactory implements ReplayFloorFactory {
    private static final long serialVersionUID = 1L;
    private final Map<String,ScopedSrsSinkAckFactory.Source> sources;
    ScopedSrsReplayFloorFactory(Map<String,ScopedSrsSinkAckFactory.Source> sources) { this.sources = Map.copyOf(sources); }
    @Override public ReplayFloor resolve(HazelcastInstance member) {
        var bound = new HashMap<io.tapstate.spi.store.SrsConsumerRun,ScopedSrsConsumerAccess.Bound>();
        sources.values().forEach(source -> bound.computeIfAbsent(source.run(),run -> ScopedSrsConsumerAccess.bind(member,run)));
        return stream -> {
            var source = sources.get(stream); if (source == null) return Optional.empty();
            var progress = bound.get(source.run()).progress();
            return Optional.ofNullable(progress.cursor().sinkAckedByTable().get(source.table())).map(io.tapstate.core.event.ChainPosition::order);
        };
    }
}
