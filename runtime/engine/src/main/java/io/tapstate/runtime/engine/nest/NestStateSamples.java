package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricTags;
import io.tapstate.core.lifecycle.NestKeyAccess;

import java.util.*;

/** Keeps key-count numerators and denominators from the same processor scrape together. */
public final class NestStateSamples {
    private NestStateSamples() { }

    public static Map<String, NestKeyAccess> keyAccess(JobMetrics metrics) {
        return keyAccess(metrics, Set.of());
    }

    public static Map<String, NestKeyAccess> keyAccess(JobMetrics metrics, Set<String> dataMembers) {
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
        Map<String, NestKeyAccess> result = new HashMap<>();
        samples.forEach((namespace, sources) -> result.put(namespace, aggregate(sources, dataMembers)));
        return result;
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
