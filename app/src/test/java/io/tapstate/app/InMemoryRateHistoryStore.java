package io.tapstate.app;

import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.RateHistoryStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/** An append-only list of samples; nothing expires, since no test here waits fifteen days. */
final class InMemoryRateHistoryStore implements RateHistoryStore {

    private static final Comparator<Entry> ORDER = Comparator
            .comparing((Entry entry) -> entry.key().observedAt())
            .thenComparing(entry -> entry.key().internalKey());

    private final List<Entry> samples = new ArrayList<>();
    private long nextKey;

    @Override
    public void append(RateSample sample) {
        samples.add(new Entry(new Key(sample.observedAt(), "%020d".formatted(nextKey++)), sample));
    }

    @Override
    public Page readPage(String pipelineId, Instant from, Instant to, Key after, int limit) {
        List<Entry> matching = samples.stream()
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
        return samples.stream()
                .filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                .filter(entry -> entry.key().observedAt().isBefore(at))
                .max(ORDER);
    }

    @Override
    public Optional<Entry> read(String pipelineId, Key key) {
        return samples.stream()
                .filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                .filter(entry -> entry.key().equals(key))
                .findFirst();
    }

    @Override
    public Optional<Entry> successor(String pipelineId, Instant at) {
        return samples.stream()
                .filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                .filter(entry -> !entry.key().observedAt().isBefore(at))
                .min(ORDER);
    }

    @Override
    public void deleteAll(String pipelineId) {
        samples.removeIf(entry -> entry.sample().pipelineId().equals(pipelineId));
    }

    @Override
    public Duration retention() {
        return Duration.ofDays(15);
    }

    List<RateSample> all() {
        return samples.stream().map(Entry::sample).toList();
    }

    private static int compare(Key left, Key right) {
        int byTime = left.observedAt().compareTo(right.observedAt());
        return byTime != 0 ? byTime : left.internalKey().compareTo(right.internalKey());
    }
}
