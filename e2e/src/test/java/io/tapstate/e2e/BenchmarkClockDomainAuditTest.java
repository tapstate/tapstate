package io.tapstate.e2e;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkClockDomainAuditTest {
    @Test void serial_actual_points_allow_two_reader_threads_without_claiming_clock_accuracy() {
        var rows = List.of(exchange(0, 0, 0), exchange(1, 0, 0), exchange(2, 0, 0));
        var evidence = BenchmarkClockDomainAudit.evidence(rows);
        assertThat(evidence).containsEntry("guestRefusals", 0L).containsEntry("hostRefusals", 0L)
                .containsEntry("purpose", "CLOCK_READ_FACTS_ONLY").containsEntry("clockCause", "UNKNOWN")
                .containsEntry("performanceAcceptanceEligible", false).containsEntry("formalPerformance", false)
                .containsEntry("samplingCostQualified", false).containsEntry("utcAccuracyQualified", false)
                .containsEntry("continuousClockErrorBoundEstablished", false);
        assertThat(evidence.get("readings")).isEqualTo(BenchmarkClockDomainAudit.raw(rows));
        assertThat(evidence).doesNotContainKeys("accuracyNanos", "utcErrorBoundMillis", "clockCauseIdentified");
    }

    @Test void host_only_and_guest_only_disagreement_remain_distinguishable_observations() {
        var host = BenchmarkClockDomainAudit.evidence(List.of(exchange(0, 0, 0), exchange(1, 10, 0)));
        var guest = BenchmarkClockDomainAudit.evidence(List.of(exchange(0, 0, 0), exchange(1, 0, 10)));
        assertThat(host.get("guestRefusals")).isEqualTo(0L);
        assertThat((long) host.get("hostRefusals")).isPositive();
        assertThat(guest.get("hostRefusals")).isEqualTo(0L);
        assertThat((long) guest.get("guestRefusals")).isPositive();
        assertThat(host.get("clockCause")).isEqualTo("UNKNOWN"); assertThat(guest.get("clockCause")).isEqualTo("UNKNOWN");
    }

    @Test void first_to_each_refusal_is_retained_when_each_adjacent_guest_pair_is_within_tolerance() {
        var rows = List.of(exchange(0, 0, 0), exchange(1, 0, 2), exchange(2, 0, 4));
        var evidence = BenchmarkClockDomainAudit.evidence(rows);
        var checks = (List<?>) evidence.get("guestChecks");
        assertThat(checks).hasSize(4);
        assertThat(checks.stream().map(check -> (Map<?, ?>) check)
                .filter(check -> "ADJACENT".equals(check.get("pairKind"))))
                .allSatisfy(check -> assertThat(check.get("state")).isEqualTo("WITHIN_RECORDED_PAIR_TOLERANCE"));
        assertThat(evidence.get("guestRefusals")).isEqualTo(1L);
        var refused = checks.stream().map(check -> (Map<?, ?>) check)
                .filter(check -> "REFUSED".equals(check.get("state"))).findFirst().orElseThrow();
        assertThat(refused.get("reason").toString()).contains("serverElapsedMillis=404");
        assertThat(evidence.get("readings")).isEqualTo(BenchmarkClockDomainAudit.raw(rows));
    }

    @Test void foreign_identity_or_reordered_brackets_are_refused_while_raw_facts_remain_available() {
        var first = exchange(0, 0, 0); var second = exchange(1, 0, 0);
        var foreign = new BenchmarkClockDomainAudit.Exchange(1, new BenchmarkClockDomainAudit.Owner(18, 1000),
                second.threadId(), second.hostBefore(), second.guest(), second.hostAfter());
        assertThatThrownBy(() -> BenchmarkClockDomainAudit.evidence(List.of(first, foreign))).isInstanceOf(AssertionError.class);
        assertThat(BenchmarkClockDomainAudit.raw(List.of(first, foreign)).getLast().get("rootPid")).isEqualTo(18L);
        assertThatThrownBy(() -> BenchmarkClockDomainAudit.evidence(List.of(second, first))).isInstanceOf(AssertionError.class);
        var overlap = new BenchmarkClockDomainAudit.Exchange(1, second.owner(), second.threadId(), first.hostBefore(), first.guest(), first.hostAfter());
        assertThatThrownBy(() -> BenchmarkClockDomainAudit.evidence(List.of(first, overlap)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("not serial");
        var changed = new BenchmarkClockDomainAudit.Exchange(1, second.owner(), second.threadId(), second.hostBefore(),
                new BenchmarkTargetClock.Reading("owned:27017", "foreign", 1202, 201_000_000, 202_000_000, 1200, 1203), second.hostAfter());
        assertThatThrownBy(() -> BenchmarkClockDomainAudit.evidence(List.of(first, changed))).isInstanceOf(AssertionError.class);
    }

    @Test void an_unavailable_hello_retains_both_actual_host_points_without_substituting_a_guest_stamp() {
        var point = exchange(0, 0, 0);
        var partial = new BenchmarkClockDomainAudit.Exchange(0, point.owner(), point.threadId(), point.hostBefore(), null, point.hostAfter());
        assertThatThrownBy(() -> BenchmarkClockDomainAudit.evidence(List.of(partial))).isInstanceOf(AssertionError.class);
        var raw = BenchmarkClockDomainAudit.raw(List.of(partial)).getFirst();
        assertThat(raw).containsEntry("helloReadingAvailable", false).containsEntry("hello", Map.of())
                .containsEntry("hostBefore", point.hostBefore().evidence()).containsEntry("hostAfter", point.hostAfter().evidence());
    }

    private static BenchmarkClockDomainAudit.Exchange exchange(int index, long hostShift, long guestShift) {
        long nanos = index * 200_000_000L, millis = 1000 + index * 200L;
        return new BenchmarkClockDomainAudit.Exchange(index, new BenchmarkClockDomainAudit.Owner(17, 1000), index == 0 ? 4 : 5,
                new BenchmarkClockDomainAudit.WallPoint(millis + hostShift, nanos, nanos + 100),
                new BenchmarkTargetClock.Reading("owned:27017", "one", millis + 2 + guestShift,
                        nanos + 1_000_000, nanos + 2_000_000, millis + hostShift, millis + 3 + hostShift),
                new BenchmarkClockDomainAudit.WallPoint(millis + 3 + hostShift, nanos + 3_000_000, nanos + 3_000_100));
    }
}
