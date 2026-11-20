package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricTags;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.NestKeyAccess;
import io.tapstate.core.lifecycle.StateCommitReading;
import io.tapstate.core.lifecycle.StateWeightReading;
import java.time.Instant;

import java.util.*;

/** Keeps key-count numerators and denominators from the same processor scrape together. */
public final class NestStateSamples {
    private NestStateSamples() { }

    public static Map<String, NestKeyAccess> keyAccess(JobMetrics metrics) {
        return keyAccess(metrics, Set.of());
    }

    public static Map<String, NestKeyAccess> keyAccess(JobMetrics metrics, Set<String> dataMembers) {
        Map<String, NestKeyAccess> result = new HashMap<>();
        samples(metrics).forEach((namespace, sources) -> result.put(namespace, aggregate(sources, dataMembers)));
        return result;
    }

    private static Map<String, Map<Source, Map<String, Long>>> samples(JobMetrics metrics) {
        Map<String, Map<Source, Map<String, Long>>> samples = new HashMap<>();
        for (String metric : metrics.metrics()) {
            var name = NestStateMetricNames.readingOf(metric);
            if (name == null) continue;
            for (Measurement point : metrics.get(metric)) {
                Source source = new Source(point.tag(MetricTags.MEMBER), point.tag(MetricTags.EXECUTION),
                        point.tag(MetricTags.VERTEX), point.tag(MetricTags.PROCESSOR), point.timestamp());
                samples.computeIfAbsent(name.namespace(), ignored -> new HashMap<>())
                        .computeIfAbsent(source, ignored -> new HashMap<>()).put(name.kind(), point.value());
            }
        }
        return samples;
    }

    /** Selects a complete scrape per member, so sibling processors and individual buckets are never summed twice. */
    public static Map<String, Diagnostics> diagnostics(JobMetrics metrics) {
        Map<String, Diagnostics> result = new HashMap<>();
        samples(metrics).forEach((namespace, sources) -> {
            Map<String, Source> commitSources = new HashMap<>();
            Map<String, Source> rootSources = new HashMap<>();
            Map<String, StateCommitReading> commits = new HashMap<>();
            Map<String, Source> weightSources = new HashMap<>();
            Map<String, StateWeightReading> weights = new HashMap<>();
            Map<String, Long> roots = new HashMap<>();
            sources.forEach((source, values) -> {
                if (source.member() == null) return;
                var commit = NestStateMetricNames.commitsFrom(values);
                if (commit != null && newer(source, commitSources.get(source.member()), commit.groups(),
                        commits.containsKey(source.member()) ? commits.get(source.member()).groups() : 0L)) {
                    commitSources.put(source.member(), source);
                    commits.put(source.member(), commit);
                }
                var weight = NestStateMetricNames.weightFrom(values);
                if (weight != null && newer(source, weightSources.get(source.member()), weight.commitBytes().count(),
                        weights.containsKey(source.member()) ? weights.get(source.member()).commitBytes().count() : 0L)) {
                    weightSources.put(source.member(), source);
                    weights.put(source.member(), weight);
                }
                Long rendered = values.get(NestStateMetricNames.ROOTS_RENDERED);
                if (rendered != null && values.containsKey(NestStateMetricNames.ROOTS_RENDERED_SINCE)
                        && newer(source, rootSources.get(source.member()), rendered, roots.getOrDefault(source.member(), 0L))) {
                    rootSources.put(source.member(), source);
                    roots.put(source.member(), rendered);
                }
            });
            Instant since = rootSources.values().stream().map(sources::get)
                    .map(values -> Instant.ofEpochMilli(values.get(NestStateMetricNames.ROOTS_RENDERED_SINCE)))
                    .max(Instant::compareTo).orElse(null);
            var commit = commits.values().stream().reduce(NestStateSamples::merge).orElse(null);
            var weight = weights.values().stream().reduce(NestStateSamples::merge).orElse(null);
            if (since != null || commit != null || weight != null) {
                result.put(namespace, new Diagnostics(roots.values().stream().mapToLong(Long::longValue).sum(), since,
                        commit, weight));
            }
        });
        return result;
    }

    private static boolean newer(Source source, Source previous, long count, long previousCount) {
        return previous == null || source.timestamp() > previous.timestamp()
                || source.timestamp() == previous.timestamp() && count > previousCount;
    }

    private static StateCommitReading merge(StateCommitReading a, StateCommitReading b) {
        return new StateCommitReading(a.groups() + b.groups(), merge(a.keys(), b.keys()),
                merge(a.roundTrips(), b.roundTrips()), merge(a.duration(), b.duration()),
                a.evictions() + b.evictions(), a.countingSince().isAfter(b.countingSince()) ? a.countingSince() : b.countingSince());
    }

    private static HistogramValue merge(HistogramValue a, HistogramValue b) {
        List<Long> buckets = new ArrayList<>(a.bucketCounts().size());
        for (int i = 0; i < a.bucketCounts().size(); i++) {
            buckets.add(a.bucketCounts().get(i) + b.bucketCounts().get(i));
        }
        return new HistogramValue(a.count() + b.count(), a.sum() + b.sum(), a.bounds(), buckets);
    }

    public record Diagnostics(long rootsRendered, Instant rootsSince, StateCommitReading commits,
            StateWeightReading weight) { }

    /** Two members' weights as one: distributions add, the largest is the larger, folds where either folded. */
    private static StateWeightReading merge(StateWeightReading a, StateWeightReading b) {
        HistogramValue folds = a.foldDuration() == null ? b.foldDuration()
                : b.foldDuration() == null ? a.foldDuration() : merge(a.foldDuration(), b.foldDuration());
        return new StateWeightReading(merge(a.documentBytes(), b.documentBytes()),
                Math.max(a.largestDocumentBytes(), b.largestDocumentBytes()), merge(a.commitBytes(), b.commitBytes()),
                folds, a.countingSince().isAfter(b.countingSince()) ? a.countingSince() : b.countingSince());
    }

    private static NestKeyAccess aggregate(Map<Source, Map<String, Long>> samples, Set<String> dataMembers) {
        Map<String, List<Source>> members = new TreeMap<>();
        for (Source source : samples.keySet()) {
            if (source.member() == null || source.execution() == null) return NestKeyAccess.unavailable();
            members.computeIfAbsent(source.member(), ignored -> new ArrayList<>()).add(source);
        }
        if (!members.keySet().containsAll(dataMembers)) return NestKeyAccess.unavailable();
        long reads = 0, writes = 0, cold = 0;
        List<String> generations = new ArrayList<>();
        for (var member : members.entrySet()) {
            long latest = member.getValue().stream().mapToLong(Source::timestamp).max().orElseThrow();
            Source selected = null;
            NestKeyAccess counted = null;
            for (Source source : member.getValue()) {
                if (source.timestamp() != latest) continue;
                NestKeyAccess candidate = counts(samples.get(source));
                if (!candidate.available()) continue;
                if (counted == null || candidate.attempts() > counted.attempts()
                        || (candidate.attempts() == counted.attempts() && candidate.coldLoads() > counted.coldLoads())) {
                    selected = source; counted = candidate;
                }
            }
            if (counted == null) return NestKeyAccess.unavailable();
            try {
                reads = Math.addExact(reads, counted.reads());
                writes = Math.addExact(writes, counted.writes());
                cold = Math.addExact(cold, counted.coldLoads());
            } catch (ArithmeticException overflow) { return NestKeyAccess.unavailable(); }
            generations.add(member.getKey() + ":" + selected.execution());
        }
        // A member restart or execution change establishes a new differencing baseline.
        return new NestKeyAccess(reads, writes, cold, generations.toString());
    }

    private static NestKeyAccess counts(Map<String, Long> values) {
        return new NestKeyAccess(values.getOrDefault(NestStateMetricNames.READ_KEYS, -1L),
                values.getOrDefault(NestStateMetricNames.WRITE_KEYS, -1L),
                values.getOrDefault(NestStateMetricNames.COLD_KEYS, -1L), "sample");
    }

    private record Source(String member, String execution, String vertex, String processor, long timestamp) { }
}
