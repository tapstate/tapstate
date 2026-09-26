package io.tapstate.app;

import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.spi.store.ObservationStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/** An append-only list of samples; nothing expires, since no test here waits fifteen days. */
final class InMemoryRateHistoryStore implements RateHistoryStore {

    private record Stored(Entry entry, ObservationStore.Scope scope) { }

    private static final Comparator<Entry> ORDER = Comparator
            .comparing((Entry entry) -> entry.key().observedAt())
            .thenComparing(entry -> entry.key().internalKey());

    private final List<Stored> samples = new ArrayList<>();
    private long nextKey;

    @Override
    public void append(RateSample sample) {
        appendWithScope(sample, null);
    }

    @Override
    public void appendScoped(RateSample sample, ObservationStore.Scope scope) {
        appendWithScope(sample, scope);
    }

    private void appendWithScope(RateSample sample, ObservationStore.Scope scope) {
        samples.add(new Stored(new Entry(new Key(sample.observedAt(), "%020d".formatted(nextKey++)), sample,
                Optional.ofNullable(scope)), scope));
    }

    @Override
    public Page readPage(String pipelineId, Instant from, Instant to, Key after, int limit) {
        return readPageFiltered(pipelineId, null, from, to, after, limit);
    }

    @Override
    public Page readPageVisible(String pipelineId, Visibility visibility,
            Instant from, Instant to, Key after, int limit) {
        return readPageFiltered(pipelineId, visibility, from, to, after, limit);
    }

    private Page readPageFiltered(String pipelineId, Visibility visibility,
            Instant from, Instant to, Key after, int limit) {
        List<Entry> matching = samples.stream().filter(stored -> visible(stored, visibility))
                .map(Stored::entry)
                .filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                .filter(entry -> !entry.key().observedAt().isBefore(from) && entry.key().observedAt().isBefore(to))
                .filter(entry -> after == null || compare(entry.key(), after) > 0)
                .sorted(ORDER)
                .limit((long) limit + 1)
                .toList();
        boolean hasMore = matching.size() > limit;
        return new Page(hasMore ? matching.subList(0, limit) : matching, hasMore);
    }

    @Override
    public Optional<Entry> predecessor(String pipelineId, Instant at) {
        return predecessorFiltered(pipelineId, null, at);
    }

    @Override
    public Optional<Entry> predecessorVisible(String pipelineId, Visibility visibility, Instant at) {
        return predecessorFiltered(pipelineId, visibility, at);
    }

    private Optional<Entry> predecessorFiltered(String pipelineId, Visibility visibility, Instant at) {
        return samples.stream().filter(stored -> visible(stored, visibility)).map(Stored::entry)
                .filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                .filter(entry -> entry.key().observedAt().isBefore(at))
                .max(ORDER);
    }

    @Override
    public Optional<Entry> read(String pipelineId, Key key) {
        return readFiltered(pipelineId, null, key);
    }

    @Override
    public Optional<Entry> readVisible(String pipelineId, Visibility visibility, Key key) {
        return readFiltered(pipelineId, visibility, key);
    }

    private Optional<Entry> readFiltered(String pipelineId, Visibility visibility, Key key) {
        return samples.stream().filter(stored -> visible(stored, visibility)).map(Stored::entry)
                .filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                .filter(entry -> entry.key().equals(key))
                .findFirst();
    }

    @Override
    public Optional<Entry> successor(String pipelineId, Instant at) {
        return successorFiltered(pipelineId, null, at);
    }

    @Override
    public Optional<Entry> successorVisible(String pipelineId, Visibility visibility, Instant at) {
        return successorFiltered(pipelineId, visibility, at);
    }

    private Optional<Entry> successorFiltered(String pipelineId, Visibility visibility, Instant at) {
        return samples.stream().filter(stored -> visible(stored, visibility)).map(Stored::entry)
                .filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                .filter(entry -> !entry.key().observedAt().isBefore(at))
                .min(ORDER);
    }

    @Override
    public void deleteAll(String pipelineId) {
        samples.removeIf(stored -> stored.entry().sample().pipelineId().equals(pipelineId));
    }

    @Override
    public void deleteIncarnation(String pipelineId, String incarnationId) {
        samples.removeIf(stored -> stored.entry().sample().pipelineId().equals(pipelineId)
                && stored.scope() != null
                && stored.scope().pipelineIncarnationId().equals(incarnationId));
    }

    @Override
    public void deleteLegacy(String pipelineId) {
        samples.removeIf(stored -> stored.entry().sample().pipelineId().equals(pipelineId)
                && stored.scope() == null);
    }

    @Override
    public Duration retention() {
        return Duration.ofDays(15);
    }

    List<RateSample> all() {
        return samples.stream().map(stored -> stored.entry().sample()).toList();
    }

    private static boolean visible(Stored stored, Visibility visibility) {
        if (visibility == null) {
            return true;
        }
        if (stored.scope() == null) {
            return visibility.includeLegacy();
        }
        return stored.scope().pipelineIncarnationId().equals(visibility.incarnationId());
    }

    private static int compare(Key left, Key right) {
        int byTime = left.observedAt().compareTo(right.observedAt());
        return byTime != 0 ? byTime : left.internalKey().compareTo(right.internalKey());
    }
}
