package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnFailureRetentionTest {
    @Test
    void aLaterClockRefusalRetainsCompletedRawCaptureSourceAndResourcesWithoutChangingTheFailure() {
        var before = new BenchmarkTargetClock.Reading("primary", "process", 1_791_651_769_430L,
                811_052_440_615_541L, 811_052_441_947_250L, 1_791_651_769_431L, 1_791_651_769_432L);
        var after = new BenchmarkTargetClock.Reading("primary", "process", 1_791_651_790_527L,
                811_073_543_091_708L, 811_073_549_747_666L, 1_791_651_790_533L, 1_791_651_790_540L);
        var capture = Map.<String, Object>of("completed", true, "epoch", 1L,
                "retainedPagesBase64", List.of("AA=="), "terminalSummary", Map.of("reportedRecords", 96_000L));
        var source = Map.<String, Object>of("completeSourceRoster", true, "batches", List.of(0, 1));
        var resources = Map.<String, Object>of("sampleCount", 87L, "cpuNanos", 17_877_132_000L);
        var evidence = Map.<String, Object>of("state", "UNKNOWN", "capture", capture, "sourceIssue", source,
                "resources", resources, "performanceAcceptanceEligible", false, "samplingCostQualified", false);
        var retained = new AtomicReference<Map<String, Object>>();
        assertThatThrownBy(() -> BenchmarkReturnFailureRetention.run(() -> BenchmarkTargetClock.validate(before, after),
                () -> evidence, retained::set)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("target server clock stepped outside");
        assertThat(retained.get()).containsAllEntriesOf(evidence);
        assertThat(retained.get().get("capture")).isSameAs(capture);
        assertThat(retained.get().get("sourceIssue")).isSameAs(source);
        assertThat(retained.get().get("resources")).isSameAs(resources);
    }

    @Test
    void aSuccessfulValidationDoesNotReadOrPublishFailureEvidence() {
        assertThat(BenchmarkReturnFailureRetention.run(() -> "accepted", () -> {
            throw new AssertionError("successful validation requested failure evidence");
        }, ignored -> { throw new AssertionError("successful validation emitted a refusal"); })).isEqualTo("accepted");
    }

    @Test
    void anEvidenceRecorderFailureCannotReplaceTheOriginalClockRefusal() {
        var primary = new AssertionError("original clock refusal");
        var recording = new IllegalStateException("controlled recorder failure");
        assertThatThrownBy(() -> BenchmarkReturnFailureRetention.run(() -> { throw primary; },
                () -> Map.of("capture", "already retained"), ignored -> { throw recording; })).isSameAs(primary);
        assertThat(primary.getSuppressed()).containsExactly(recording);
    }

    @Test
    void anInactiveReturnCaptureDoesNotPublishInventedEvidence() {
        var primary = new AssertionError("legacy clock refusal");
        assertThatThrownBy(() -> BenchmarkReturnFailureRetention.run(() -> { throw primary; }, Map::of,
                ignored -> { throw new AssertionError("inactive capture emitted evidence"); })).isSameAs(primary);
    }
}
