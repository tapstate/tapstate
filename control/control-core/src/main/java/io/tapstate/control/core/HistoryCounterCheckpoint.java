package io.tapstate.control.core;

import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.spi.store.RateHistoryStore.Entry;
import io.tapstate.spi.store.RateHistoryStore.Key;
import io.tapstate.spi.store.RateHistoryStore.Page;
import io.tapstate.spi.store.RateHistoryStore.Visibility;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Consumer;

/** Fixed scalar reset evidence for one visibility-qualified pipeline walk, never a rate baseline. */
public final class HistoryCounterCheckpoint {
    private final String pipelineId;
    private Instant knownStart;
    private Long records;
    private Long bytes;
    private Instant lastObservedAt;
    private String lastIncarnation;

    public HistoryCounterCheckpoint(String pipelineId) {
        this.pipelineId = Objects.requireNonNull(pipelineId, "pipelineId");
    }

    /** Absence preserves real evidence; the values are never used to calculate a rate. */
    public boolean observe(RateSample sample) {
        Objects.requireNonNull(sample, "sample");
        if (!pipelineId.equals(sample.pipelineId())) {
            throw new IllegalStateException("a counter checkpoint cannot cross pipelines");
        }
        Instant start = sample.countingSince();
        Long nextRecords = sample.counters().get(HistoryAggregator.RECORDS_OUT);
        Long nextBytes = sample.counters().get(HistoryAggregator.BYTES_OUT);
        boolean reset = knownStart != null && start != null && !knownStart.equals(start)
                || decreased(records, nextRecords) || decreased(bytes, nextBytes);
        if (reset) { clearCounters(); }
        if (start != null) { knownStart = start; }
        if (nextRecords != null) { records = nextRecords; }
        if (nextBytes != null) { bytes = nextBytes; }
        return reset;
    }

    /** Gaps and unrelated incarnations end the qualified evidence, while execution changes retain it. */
    public boolean observe(Entry entry, Duration sampleInterval) {
        Instant at = entry.sample().observedAt();
        String incarnation = entry.scope().map(scope -> scope.pipelineIncarnationId()).orElse(null);
        if (entry.gapFrom() != null
                || lastObservedAt != null && Duration.between(lastObservedAt, at)
                        .compareTo(sampleInterval.multipliedBy(2)) >= 0
                || lastIncarnation != null && incarnation != null && !lastIncarnation.equals(incarnation)) {
            clearCounters();
        }
        boolean reset = observe(entry.sample());
        lastObservedAt = at;
        lastIncarnation = incarnation;
        return reset;
    }

    /** Clears all evidence without synthesizing an initial value or accumulation start. */
    public void clear() {
        clearCounters(); lastObservedAt = null; lastIncarnation = null;
    }

    private void clearCounters() {
        knownStart = null; records = null; bytes = null;
    }

    /**
     * Reconstructs an incomplete anchor from retained stable-key pages. The caller charges every
     * returned page and lookahead against its existing budget before this method consumes that page.
     */
    public static HistoryCounterCheckpoint replay(RateHistoryStore store, Visibility visibility,
            Instant cutoff, Entry anchor, Duration sampleInterval, int batchSize, Consumer<Page> onPage) {
        HistoryCounterCheckpoint checkpoint = new HistoryCounterCheckpoint(anchor.sample().pipelineId());
        if (anchor.gapFrom() != null || anchor.sample().countingSince() != null
                && anchor.sample().counters().containsKey(HistoryAggregator.RECORDS_OUT)
                && anchor.sample().counters().containsKey(HistoryAggregator.BYTES_OUT)) {
            checkpoint.observe(anchor, sampleInterval);
            return checkpoint;
        }
        Key after = null;
        Instant end = anchor.key().observedAt().plusMillis(1);
        while (cutoff.isBefore(end)) {
            Page page = store.readPageVisible(anchor.sample().pipelineId(), visibility,
                    cutoff, end, after, batchSize);
            onPage.accept(page);
            for (Entry entry : page.entries()) {
                int order = compare(entry.key(), anchor.key());
                if (order > 0) {
                    checkpoint.observe(anchor, sampleInterval);
                    return checkpoint;
                }
                checkpoint.observe(entry, sampleInterval);
                if (order == 0) { return checkpoint; }
            }
            if (!page.hasMore()) { break; }
            after = page.lastKey().orElseThrow();
        }
        // The exact anchor was actually read before replay, even if TTL removed it in the meantime.
        checkpoint.observe(anchor, sampleInterval);
        return checkpoint;
    }

    private static int compare(Key left, Key right) {
        int time = left.observedAt().compareTo(right.observedAt());
        return time != 0 ? time : left.internalKey().compareTo(right.internalKey());
    }

    private static boolean decreased(Long priorValue, Long nextValue) {
        return priorValue != null && nextValue != null && nextValue < priorValue;
    }
}
