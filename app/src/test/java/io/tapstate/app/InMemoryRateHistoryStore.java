package io.tapstate.app;

import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.RateHistoryStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** An append-only list of samples; nothing expires, since no test here waits fifteen days. */
final class InMemoryRateHistoryStore implements RateHistoryStore {

    private final List<RateSample> samples = new ArrayList<>();

    @Override
    public void append(RateSample sample) {
        samples.add(sample);
    }

    @Override
    public List<RateSample> readBetween(String pipelineId, Instant from, Instant to) {
        return samples.stream()
                .filter(sample -> sample.pipelineId().equals(pipelineId))
                .filter(sample -> !sample.observedAt().isBefore(from) && !sample.observedAt().isAfter(to))
                .sorted(Comparator.comparing(RateSample::observedAt))
                .toList();
    }

    @Override
    public void deleteAll(String pipelineId) {
        samples.removeIf(sample -> sample.pipelineId().equals(pipelineId));
    }

    @Override
    public Duration retention() {
        return Duration.ofDays(15);
    }

    List<RateSample> all() {
        return List.copyOf(samples);
    }
}
