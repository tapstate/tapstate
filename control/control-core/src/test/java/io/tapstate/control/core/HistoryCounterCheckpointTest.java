package io.tapstate.control.core;

import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.RateHistoryStore.Entry;
import io.tapstate.spi.store.RateHistoryStore.Key;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HistoryCounterCheckpointTest {
    private static final Instant AT = Instant.parse("2026-10-04T00:00:00Z");
    private static final Instant START = AT.minusSeconds(60);

    @Test
    void twoKnownEpochsAcrossUnknownProveReset() {
        var checkpoint = new HistoryCounterCheckpoint("orders");
        assertThat(checkpoint.observe(sample(Map.of("records.out", 3L), START))).isFalse();
        assertThat(checkpoint.observe(sample(Map.of(), null))).isFalse();
        assertThat(checkpoint.observe(sample(Map.of("records.out", 1L), START.plusSeconds(1)))).isTrue();
    }

    @Test
    void firstKnownOrContinuedStartNeverInventsReset() {
        var checkpoint = new HistoryCounterCheckpoint("orders");
        assertThat(checkpoint.observe(sample(Map.of(), null))).isFalse();
        assertThat(checkpoint.observe(sample(Map.of("records.out", 3L), START))).isFalse();
        assertThat(checkpoint.observe(sample(Map.of(), null))).isFalse();
        assertThat(checkpoint.observe(sample(Map.of("records.out", 5L), START))).isFalse();
    }

    @Test
    void missingOneCounterDoesNotEraseTheOtherKnownWitness() {
        var checkpoint = new HistoryCounterCheckpoint("orders");
        assertThat(checkpoint.observe(sample(Map.of("records.out", 3L), START))).isFalse();
        assertThat(checkpoint.observe(sample(Map.of("bytes.out", 30L), START))).isFalse();
        assertThat(checkpoint.observe(sample(Map.of("records.out", 1L), START))).isTrue();
        assertThat(checkpoint.observe(sample(Map.of("bytes.out", 1L), START))).isFalse();
    }

    @Test
    void twoExplicitKnownStartsStillProveResetWhenTheFlatCounterIsAbsent() {
        var checkpoint = new HistoryCounterCheckpoint("orders");
        assertThat(checkpoint.observe(sample(Map.of(), START))).isFalse();
        assertThat(checkpoint.observe(sample(Map.of(), null))).isFalse();
        assertThat(checkpoint.observe(sample(Map.of(), START.plusSeconds(1)))).isTrue();
    }

    @Test
    void clearingAtAQualifiedBoundaryDropsPriorResetEvidence() {
        var checkpoint = new HistoryCounterCheckpoint("orders");
        checkpoint.observe(sample(Map.of("records.out", 3L), START));
        checkpoint.clear();
        assertThat(checkpoint.observe(sample(Map.of("records.out", 1L), START.plusSeconds(1)))).isFalse();
    }

    @Test
    void aCheckpointRejectsAnUnrelatedPipeline() {
        var checkpoint = new HistoryCounterCheckpoint("orders");
        assertThatThrownBy(() -> checkpoint.observe(new RateSample("other", AT, Map.of(), Map.of(), null)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void realCounterDecreaseStillProvesResetWhenTheStartIsAbsent() {
        var checkpoint = new HistoryCounterCheckpoint("orders");
        assertThat(checkpoint.observe(sample(Map.of("records.out", 3L), null))).isFalse();
        assertThat(checkpoint.observe(sample(Map.of(), null))).isFalse();
        assertThat(checkpoint.observe(sample(Map.of("records.out", 1L), null))).isTrue();
    }

    @Test
    void aRealSamplingGapDiscardsOlderKnownEvidence() {
        var checkpoint = new HistoryCounterCheckpoint("orders");
        checkpoint.observe(new Entry(new Key(AT, "001"), sample(Map.of("records.out", 3L), START)),
                Duration.ofSeconds(30));
        Instant gap = AT.plusSeconds(60);
        assertThat(checkpoint.observe(new Entry(new Key(gap, "002"),
                new RateSample("orders", gap, Map.of(), Map.of(), null)), Duration.ofSeconds(30))).isFalse();
        Instant resumed = gap.plusSeconds(30);
        assertThat(checkpoint.observe(new Entry(new Key(resumed, "003"),
                new RateSample("orders", resumed, Map.of("records.out", 1L), Map.of(), START.plusSeconds(1))),
                Duration.ofSeconds(30))).isFalse();
    }

    private static RateSample sample(Map<String, Long> counters, Instant since) {
        return new RateSample("orders", AT, counters, Map.of(), since);
    }
}
