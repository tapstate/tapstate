package io.tapstate.control.core;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.RateHistoryStore;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Shared deterministic stores and clocks for the discriminating history-query cases. */
final class HistoryTestSupport {

    static final Instant NOW = Instant.parse("2026-09-21T12:00:00Z");
    static final Instant COUNTING_SINCE = Instant.parse("2026-09-21T00:00:00Z");
    static final byte[] SECRET = "history-discriminating-test-secret".getBytes(StandardCharsets.UTF_8);

    private HistoryTestSupport() {
    }

    static PipelineHistoryQueryService service(MutableHistory history, int batch, int budget) {
        return service(history, batch, budget, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    static PipelineHistoryQueryService service(
            MutableHistory history, int batch, int budget, Clock clock) {
        return new PipelineHistoryQueryService(artifactsWith("orders"), history, Duration.ofMinutes(1),
                clock, new HistoryCursorCodec(SECRET, clock), batch, budget);
    }

    static PipelineHistoryQuery query(String from, String to, HistoryResolution resolution,
            int limit, List<String> tables, String cursor) {
        return new PipelineHistoryQuery("orders", Instant.parse(from), Instant.parse(to),
                resolution, limit, tables, cursor);
    }

    static RateSample sample(String at, long records, long bytes, long inbound,
            Map<String, Long> lag, Instant countingSince) {
        return new RateSample("orders", Instant.parse(at),
                Map.of("records.out", records, "bytes.out", bytes, "records.in", inbound),
                lag, countingSince);
    }

    static List<PipelineMetricsHistory.Point> points(PipelineMetricsHistory history) {
        return history.segments().stream().flatMap(segment -> segment.points().stream()).toList();
    }

    private static ArtifactQueryService artifactsWith(String... ids) {
        Map<String, Resource> resources = new LinkedHashMap<>();
        for (String id : ids) {
            resources.put(id, new DslParser().parse("""
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: source
                    serve:
                      from: /.*/
                      sync:
                        - id: sink
                          source: target
                          write_mode: upsert
                          ddl: apply
                    """.formatted(id)));
        }
        return new ArtifactQueryService(new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
                artifacts.forEach(resource -> resources.put(resource.id(), resource));
            }

            @Override
            public Optional<Resource> get(String id) {
                return Optional.ofNullable(resources.get(id));
            }

            @Override
            public List<Resource> list() {
                return List.copyOf(resources.values());
            }
        });
    }

    static final class MutableHistory implements RateHistoryStore {
        private static final Comparator<Entry> ORDER = Comparator
                .comparing((Entry entry) -> entry.key().observedAt())
                .thenComparing(entry -> entry.key().internalKey());

        private final List<Entry> entries = new ArrayList<>();
        private final List<Integer> requestedLimits = new ArrayList<>();
        private long nextKey;

        Entry add(RateSample sample) {
            Entry entry = new Entry(new Key(sample.observedAt(), "%020d".formatted(nextKey++)), sample);
            entries.add(entry);
            entries.sort(ORDER);
            return entry;
        }

        void removeAt(Instant observedAt) {
            entries.removeIf(entry -> entry.sample().observedAt().equals(observedAt));
        }

        List<Integer> requestedLimits() {
            return List.copyOf(requestedLimits);
        }

        @Override
        public void append(RateSample sample) {
            add(sample);
        }

        @Override
        public Page readPage(String pipelineId, Instant from, Instant to, Key after, int limit) {
            requestedLimits.add(limit);
            List<Entry> found = entries.stream()
                    .filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                    .filter(entry -> !entry.key().observedAt().isBefore(from)
                            && entry.key().observedAt().isBefore(to))
                    .filter(entry -> after == null || compare(entry.key(), after) > 0)
                    .limit((long) limit + 1)
                    .toList();
            boolean hasMore = found.size() > limit;
            return new Page(hasMore ? found.subList(0, limit) : found, hasMore);
        }

        @Override
        public Optional<Entry> read(String pipelineId, Key key) {
            return entries.stream()
                    .filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                    .filter(entry -> entry.key().equals(key))
                    .findFirst();
        }

        @Override
        public Optional<Entry> predecessor(String pipelineId, Instant at) {
            return entries.stream()
                    .filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                    .filter(entry -> entry.key().observedAt().isBefore(at))
                    .max(ORDER);
        }

        @Override
        public Optional<Entry> successor(String pipelineId, Instant at) {
            return entries.stream()
                    .filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                    .filter(entry -> !entry.key().observedAt().isBefore(at))
                    .min(ORDER);
        }

        @Override
        public void deleteAll(String pipelineId) {
            entries.removeIf(entry -> entry.sample().pipelineId().equals(pipelineId));
        }

        @Override
        public Duration retention() {
            return Duration.ofDays(15);
        }

        private static int compare(Key left, Key right) {
            int time = left.observedAt().compareTo(right.observedAt());
            return time != 0 ? time : left.internalKey().compareTo(right.internalKey());
        }
    }

    static final class MutableClock extends Clock {
        private Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return Clock.fixed(now, zone);
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
