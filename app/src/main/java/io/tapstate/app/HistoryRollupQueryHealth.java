package io.tapstate.app;

import io.tapstate.control.core.PipelineHistoryQueryService.RollupFallback;
import io.tapstate.spi.store.HistoryRollupStore.Resolution;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/** Process-local counters for completed history queries that descended from rollup to raw. */
final class HistoryRollupQueryHealth implements Consumer<RollupFallback> {

    record Level(long queries, long buckets, long fullRawQueries) {
    }

    private static final class Counters {
        private final LongAdder queries = new LongAdder();
        private final LongAdder buckets = new LongAdder();
        private final LongAdder fullRawQueries = new LongAdder();
    }

    private final Counters[] levels = new Counters[Resolution.values().length];

    HistoryRollupQueryHealth() {
        for (int index = 0; index < levels.length; index++) {
            levels[index] = new Counters();
        }
    }

    @Override
    public void accept(RollupFallback fallback) {
        Counters level = levels[fallback.resolution().ordinal()];
        if (fallback.fullRaw()) {
            level.fullRawQueries.increment();
        } else {
            level.buckets.add(fallback.downDrilledBuckets());
        }
        level.queries.increment();
    }

    Map<Resolution, Level> snapshot() {
        Map<Resolution, Level> snapshot = new EnumMap<>(Resolution.class);
        for (Resolution resolution : Resolution.values()) {
            Counters counters = levels[resolution.ordinal()];
            long queries = counters.queries.sum();
            if (queries > 0) {
                snapshot.put(resolution, new Level(queries,
                        counters.buckets.sum(), counters.fullRawQueries.sum()));
            }
        }
        return Map.copyOf(snapshot);
    }
}
