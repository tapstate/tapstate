package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.metrics.Metric;
import com.hazelcast.jet.core.metrics.Metrics;
import java.util.HashMap;
import java.util.Map;

/**
 * Publishes a namespace's readings as run statistics of the job holding it. The engine already collects a
 * job's statistics on a cadence of its own and hands them out with the job, so a reading left here is
 * readable from outside the run without a second channel to keep alive.
 *
 * <p>Four numbers rather than the one ratio they imply. A ratio is an average over however long the run
 * has been going, and a state layer that fell off a cliff a minute into an hour-long run reads as healthy
 * in it; two scrapes of the counts give whatever window the reader wants, and the ratio is one division
 * away. The name carries the namespace for the reason the frontier readings carry the chain: one number
 * covering every namespace would average a resolver that is thrashing with an assembler that is not, and
 * read as neither.
 *
 * <p>Handles are kept once obtained - they belong to the store rather than to whichever thread ran it - so
 * a reading costs one lookup and one write per number.
 */
final class JetNestStateGauge implements NestStateGauge {

    private final Map<String, Metric> byName = new HashMap<>();

    @Override
    public void reading(String namespace, long entries, long accesses, long backfills, long backfillMillis,
            long pendingHighWater) {
        metric(NestStateMetricNames.ENTRIES, namespace).set(entries);
        metric(NestStateMetricNames.ACCESSES, namespace).set(accesses);
        metric(NestStateMetricNames.BACKFILLS, namespace).set(backfills);
        metric(NestStateMetricNames.BACKFILL_MILLIS, namespace).set(backfillMillis);
        metric(NestStateMetricNames.PENDING_HIGH_WATER, namespace).set(pendingHighWater);
    }

    @Override
    public void rootsReading(String namespace, long rootsRendered, long countingSinceMillis) {
        metric(NestStateMetricNames.ROOTS_RENDERED, namespace).set(rootsRendered);
        metric(NestStateMetricNames.ROOTS_RENDERED_SINCE, namespace).set(countingSinceMillis);
    }

    @Override
    public void commitReading(String namespace, NestStateStats.Commits commits, long evictions) {
        metric(NestStateMetricNames.COMMIT_GROUPS, namespace).set(commits.groups());
        metric(NestStateMetricNames.COMMIT_KEYS_SUM, namespace).set(commits.keySum());
        metric(NestStateMetricNames.COMMIT_ROUND_TRIPS_SUM, namespace).set(commits.roundTripSum());
        metric(NestStateMetricNames.COMMIT_NANOS_SUM, namespace).set(commits.durationNanos());
        metric(NestStateMetricNames.COMMIT_SINCE, namespace).set(commits.countingSinceMillis());
        metric(NestStateMetricNames.EVICTIONS, namespace).set(evictions);
        buckets(NestStateMetricNames.COMMIT_KEYS_BUCKET, namespace, commits.keyBuckets());
        buckets(NestStateMetricNames.COMMIT_ROUND_TRIPS_BUCKET, namespace, commits.roundTripBuckets());
        buckets(NestStateMetricNames.COMMIT_NANOS_BUCKET, namespace, commits.durationBuckets());
    }

    private void buckets(String kind, String namespace, long[] counts) {
        for (int i = 0; i < counts.length; i++) {
            metric(kind + i, namespace).set(counts[i]);
        }
    }

    private Metric metric(String kind, String namespace) {
        return byName.computeIfAbsent(NestStateMetricNames.nameOf(kind, namespace), Metrics::metric);
    }

    @Override public void keyAccess(String namespace, io.tapstate.core.lifecycle.NestKeyAccess reading) {
        keyPublisher(namespace).accept(reading);
    }

    @Override public java.util.function.Consumer<io.tapstate.core.lifecycle.NestKeyAccess> keyPublisher(String namespace) {
        Metric reads = Metrics.threadSafeMetric(NestStateMetricNames.nameOf(NestStateMetricNames.READ_KEYS, namespace));
        Metric writes = Metrics.threadSafeMetric(NestStateMetricNames.nameOf(NestStateMetricNames.WRITE_KEYS, namespace));
        Metric cold = Metrics.threadSafeMetric(NestStateMetricNames.nameOf(NestStateMetricNames.COLD_KEYS, namespace));
        return reading -> { reads.set(reading.reads()); writes.set(reading.writes()); cold.set(reading.coldLoads()); };
    }
}
