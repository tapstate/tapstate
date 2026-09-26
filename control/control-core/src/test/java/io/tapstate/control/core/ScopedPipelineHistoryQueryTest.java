package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.RateHistoryStore;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/** A query must never bridge two resources merely because they reused the same public id. */
class ScopedPipelineHistoryQueryTest {

    private static final Instant NOW = Instant.parse("2026-09-26T10:10:00Z");
    private static final Instant START = NOW.minusSeconds(600);

    @Test
    void newResourceReadsOnlyItsOwnHistoryAndRejectsTheOldResourcesCursor() {
        Artifacts artifacts = new Artifacts();
        History history = new History();
        history.add(START, 1, "inc-old");
        history.add(START.plusSeconds(60), 2, "inc-old");
        history.add(START.plusSeconds(120), 3, "inc-new");
        PipelineHistoryQueryService service = service(artifacts, history);
        PipelineHistoryQuery firstPage = query(1, null);

        PipelineMetricsHistory old = service.query(firstPage);
        assertThat(old.nextCursor()).isNotNull();
        artifacts.incarnation = "inc-new";

        PipelineMetricsHistory current = service.query(query(10, null));
        assertThat(current.segments()).flatExtracting(PipelineMetricsHistory.Segment::points)
                .extracting(PipelineMetricsHistory.Point::intervalEnd)
                .containsExactly(START.plusSeconds(120));
        TapstateException stale = catchThrowableOfType(() -> service.query(query(1, old.nextCursor())),
                TapstateException.class);
        assertThat(stale.code()).isEqualTo(MonitorError.INVALID_CURSOR);
        assertThat(stale.args()).containsEntry("reason", "QUERY_MISMATCH");
    }

    private static PipelineHistoryQueryService service(Artifacts artifacts, History history) {
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        return new PipelineHistoryQueryService(new ArtifactQueryService(artifacts), history,
                Duration.ofMinutes(1), clock,
                new HistoryCursorCodec("scoped-history-secret".getBytes(StandardCharsets.UTF_8), clock));
    }

    private static PipelineHistoryQuery query(int limit, String cursor) {
        return new PipelineHistoryQuery("orders", START, NOW,
                HistoryResolution.RAW, limit, List.of(), cursor);
    }

    private static final class Artifacts implements ArtifactStore {
        private final Resource pipeline = new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: orders
                source: source
                """);
        private String incarnation = "inc-old";

        @Override public void saveAll(List<Resource> resources) { throw new UnsupportedOperationException(); }
        @Override public Optional<Resource> get(String id) {
            return "orders".equals(id) ? Optional.of(pipeline) : Optional.empty();
        }
        @Override public List<Resource> list() { return List.of(pipeline); }
        @Override public Optional<String> pipelineIncarnationId(String id) {
            return Optional.of(incarnation);
        }
        @Override public Optional<HistoryOwner> pipelineHistoryOwner(String id) {
            return Optional.of(new HistoryOwner(new RateHistoryStore.Visibility(incarnation, false)));
        }
    }

    private static final class History implements RateHistoryStore {
        private record Scoped(Entry entry, String incarnation) { }
        private static final Comparator<Key> KEY_ORDER = Comparator.comparing(Key::observedAt)
                .thenComparing(Key::internalKey);
        private static final Comparator<Entry> ORDER = Comparator.comparing(Entry::key, KEY_ORDER);
        private final List<Scoped> samples = new ArrayList<>();

        void add(Instant at, long value, String incarnation) {
            RateSample sample = new RateSample("orders", at, Map.of("records.out", value),
                    Map.of(), START);
            samples.add(new Scoped(new Entry(new Key(at, "key-" + value), sample), incarnation));
        }

        @Override public void append(RateSample sample) { throw new UnsupportedOperationException(); }
        @Override public Page readPage(String id, Instant from, Instant to, Key after, int limit) {
            return page(samples, from, to, after, limit);
        }
        @Override public Page readPageVisible(String id, Visibility visibility,
                Instant from, Instant to, Key after, int limit) {
            return page(visible(visibility), from, to, after, limit);
        }
        @Override public Optional<Entry> read(String id, Key key) {
            return samples.stream().map(Scoped::entry).filter(entry -> entry.key().equals(key)).findFirst();
        }
        @Override public Optional<Entry> readVisible(String id, Visibility visibility, Key key) {
            return visible(visibility).stream().map(Scoped::entry)
                    .filter(entry -> entry.key().equals(key)).findFirst();
        }
        @Override public Optional<Entry> predecessor(String id, Instant at) {
            return boundary(samples, at, true);
        }
        @Override public Optional<Entry> predecessorVisible(String id, Visibility visibility, Instant at) {
            return boundary(visible(visibility), at, true);
        }
        @Override public Optional<Entry> successor(String id, Instant at) {
            return boundary(samples, at, false);
        }
        @Override public Optional<Entry> successorVisible(String id, Visibility visibility, Instant at) {
            return boundary(visible(visibility), at, false);
        }
        @Override public void deleteAll(String id) { throw new UnsupportedOperationException(); }
        @Override public Duration retention() { return Duration.ofDays(15); }

        private List<Scoped> visible(Visibility visibility) {
            return samples.stream().filter(sample -> sample.incarnation().equals(visibility.incarnationId()))
                    .toList();
        }

        private static Page page(List<Scoped> scoped, Instant from, Instant to, Key after, int limit) {
            List<Entry> found = scoped.stream().map(Scoped::entry)
                    .filter(entry -> !entry.key().observedAt().isBefore(from)
                            && entry.key().observedAt().isBefore(to))
                    .filter(entry -> after == null || KEY_ORDER.compare(entry.key(), after) > 0)
                    .sorted(ORDER).limit(limit + 1L).toList();
            return new Page(found.size() > limit ? found.subList(0, limit) : found, found.size() > limit);
        }

        private static Optional<Entry> boundary(List<Scoped> scoped, Instant at, boolean before) {
            return scoped.stream().map(Scoped::entry)
                    .filter(entry -> before ? entry.key().observedAt().isBefore(at)
                            : !entry.key().observedAt().isBefore(at))
                    .min(before ? ORDER.reversed() : ORDER);
        }
    }
}
