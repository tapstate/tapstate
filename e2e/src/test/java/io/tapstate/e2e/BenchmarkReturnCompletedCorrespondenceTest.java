package io.tapstate.e2e;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnCompletedCorrespondenceTest {
    @Test void retained_complete_raw_material_reconstructs_every_call_sample_and_summary() throws Exception {
        var fixture = BenchmarkWriteReturnPhaseEvidenceTest.nativeFixture(BenchmarkWorkloadDefinitions.steadyPilot("stateless"));
        var result = fixture.result(); var owner = result.samples().getFirst().identity();
        var retained = capture(result);
        var reconstructed = BenchmarkReturnCompletedMethod.reconstructCapture(retained, owner, "stateless/cdc-update");
        assertThat(reconstructed.calls()).isEqualTo(result.calls());
        assertThat(reconstructed.samples()).isEqualTo(result.samples());
        assertThat(reconstructed.summary()).isEqualTo(result.summary());
        assertThat(reconstructed.pagesBase64()).isEqualTo(result.pagesBase64());
        assertThatThrownBy(() -> BenchmarkReturnCompletedMethod.reconstructCapture(retained,
                new BenchmarkCausalClock.Identity(owner.pid() + 1, owner.jvmStartTimeMillis()), "stateless/cdc-update"))
                .isInstanceOf(AssertionError.class);
        retained.put("pagesBase64", result.pagesBase64().subList(0, result.pagesBase64().size() - 1));
        assertThatThrownBy(() -> BenchmarkReturnCompletedMethod.reconstructCapture(retained, owner, "stateless/cdc-update"))
                .isInstanceOf(AssertionError.class);
    }
    @Test void individually_plausible_but_inconsistent_report_count_duration_rate_and_p99_are_refused() {
        var metric = metric(); var correct = projection(metric);
        BenchmarkReturnCompletedMethod.requireMatchingInterval(correct, metric);
        for (String field : List.of("completedRows", "durationNanos", "p99Nanos", "throughput")) {
            var changed = new LinkedHashMap<String, Object>(correct);
            @SuppressWarnings("unchecked") var values = new LinkedHashMap<String, Object>((Map<String, Object>) correct.get(field));
            Object lower = values.get("lower");
            values.put("lower", lower instanceof String ? new BigDecimal((String) lower).add(BigDecimal.ONE).toString()
                    : ((Number) lower).longValue() + 1);
            changed.put(field, values);
            assertThatThrownBy(() -> BenchmarkReturnCompletedMethod.requireMatchingInterval(changed, metric))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("retained pooled interval differs");
        }
    }
    @Test void negative_full_cohort_p99_and_all_phase_population_are_preserved_in_correspondence() {
        var metric = metric(); var correct = projection(metric);
        assertThat(metric.p99().lowerNanos()).isEqualTo(-5);
        BenchmarkReturnCompletedMethod.requireMatchingInterval(correct, metric);
        var changed = new LinkedHashMap<String, Object>(correct);
        changed.put("p99Nanos", Map.of("lower", 0L, "upper", 10L));
        var clamped = changed;
        assertThatThrownBy(() -> BenchmarkReturnCompletedMethod.requireMatchingInterval(clamped, metric)).isInstanceOf(AssertionError.class);
        changed = new LinkedHashMap<>(correct); changed.put("fullCohortRows", 10000);
        var trimmed = changed;
        assertThatThrownBy(() -> BenchmarkReturnCompletedMethod.requireMatchingInterval(trimmed, metric)).isInstanceOf(AssertionError.class);
    }
    private static BenchmarkReturnIntervalMetrics.Fork metric() {
        var latencies = java.util.Collections.nCopies(10000, new BenchmarkCausalClock.Interval(-5, 10));
        return BenchmarkReturnIntervalMetrics.aggregate(List.of(
                new BenchmarkReturnIntervalMetrics.Phase("cold-read", 10000, 10000, 1000000000L, 1000000000L, latencies),
                new BenchmarkReturnIntervalMetrics.Phase("cdc-update", 10000, 10000, 1000000000L, 1000000000L, latencies)));
    }
    private static Map<String, Object> projection(BenchmarkReturnIntervalMetrics.Fork metric) {
        return Map.of("completedRows", Map.of("lower", metric.completedLower(), "upper", metric.completedUpper()),
                "durationNanos", Map.of("lower", metric.durationLower(), "upper", metric.durationUpper()),
                "p99Nanos", Map.of("lower", metric.p99().lowerNanos(), "upper", metric.p99().upperNanos()),
                "throughput", Map.of("lower", decimal(metric.throughput().lowerRecordsPerSecond(), false),
                        "upper", decimal(metric.throughput().upperRecordsPerSecond(), true)),
                "fullCohortRows", metric.fullCohortRows(), "phases", metric.phases());
    }
    private static String decimal(double value, boolean upper) {
        return new BigDecimal(value).setScale(48, upper ? RoundingMode.CEILING : RoundingMode.FLOOR).stripTrailingZeros().toPlainString();
    }
    private static Map<String, Object> capture(BenchmarkWriteReturnCapture.Result capture) {
        var summary = capture.summary(); var owner = capture.samples().getFirst().identity();
        var out = new LinkedHashMap<String, Object>();
        out.put("clockSamplingMode", "PERIODIC"); out.put("returnTimeCoordinate", "UNROUNDED_NOMINAL_COUNTER_ENCLOSURES");
        out.put("summary", Map.of("window", summary.window(), "state", summary.state(), "completedCalls", summary.completedCalls(),
                "failedCalls", summary.failedCalls(), "reportedRecords", summary.reportedRecords(), "openCalls", summary.openCalls(),
                "retainedBytes", summary.retainedBytes()));
        out.put("pagesBase64", capture.pagesBase64());
        out.put("ownedRuntime", Map.of("pid", owner.pid(), "jvmStartTimeMillis", owner.jvmStartTimeMillis()));
        out.put("clockSamples", capture.samples().stream().map(sample -> Map.of("sequence", sample.sequence(), "pid", sample.identity().pid(),
                "jvmStartTimeMillis", sample.identity().jvmStartTimeMillis(), "driverBeforeNanos", sample.driverBeforeNanos(),
                "driverAfterNanos", sample.driverAfterNanos(), "ownedNanos", sample.ownedNanos())).toList());
        return out;
    }
}
