package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnCpuPerRowTest {
    private static final long MS = 1_000_000L;

    @Test void pooled_phases_divide_both_cpu_ends_by_all_cohort_rows_with_outward_rounding() {
        var bounds = BenchmarkReturnCpuPerRow.perRow(List.of(
                new BenchmarkReturnCpuPerRow.Phase(1_000, 1_100, 3), new BenchmarkReturnCpuPerRow.Phase(2_000, 2_100, 4)));
        assertThat(bounds.lower().multiply(BigDecimal.valueOf(7))).isLessThanOrEqualTo(BigDecimal.valueOf(3_000));
        assertThat(bounds.upper().multiply(BigDecimal.valueOf(7))).isGreaterThanOrEqualTo(BigDecimal.valueOf(3_200));
        assertThat(bounds.lower()).isGreaterThan(new BigDecimal("428.5714285"));
        assertThat(bounds.upper()).isLessThan(new BigDecimal("457.1428572"));
        assertThatThrownBy(() -> BenchmarkReturnCpuPerRow.perRow(List.of(new BenchmarkReturnCpuPerRow.Phase(0, 0, 1))))
                .hasMessageContaining("no APP CPU");
    }

    @Test void the_retained_raw_trace_rebuilds_the_sampler_summary_exactly() {
        var summary = trace();
        var written = JsonReader.parse(JsonWriter.write(PipelineBenchmarkLiveRunIT.resourceEvidence(summary, null)));
        assertThat(BenchmarkReturnCpuPerRow.summary(object(written))).isEqualTo(summary);
    }

    @Test void the_window_enclosure_is_recomputed_and_must_match_what_the_fork_retained() {
        var summary = trace(); var cohort = cohort();
        var resources = object(JsonReader.parse(JsonWriter.write(PipelineBenchmarkLiveRunIT.resourceEvidence(summary, null))));
        var retained = object(JsonReader.parse(JsonWriter.write(BenchmarkReturnResourceBounds.evidence(summary, cohort))));
        var phase = BenchmarkReturnCpuPerRow.phase(resources, cohort, retained);
        // Reads 4..7 lie wholly inside the return window [1.0s, 2.0s]; the outer reads are 2 and 9.
        assertThat(phase).isEqualTo(new BenchmarkReturnCpuPerRow.Phase(3 * 40 * MS, 7 * 40 * MS, cohort.size()));

        var tampered = new LinkedHashMap<>(retained); tampered.put("cpuNanosUpper", 7 * 40 * MS - 1);
        assertThatThrownBy(() -> BenchmarkReturnCpuPerRow.phase(resources, cohort, tampered))
                .hasMessageContaining("differs from the raw resource trace");
    }

    @Test void a_trace_without_outer_coverage_is_not_a_cpu_measurement() {
        var summary = trace(); var late = new ArrayList<BenchmarkReturnTimeBounds.Delivery>();
        for (var row : cohort()) late.add(delivery(row.key(), 2_400 * MS));
        var resources = object(JsonReader.parse(JsonWriter.write(PipelineBenchmarkLiveRunIT.resourceEvidence(summary, null))));
        var retained = object(JsonReader.parse(JsonWriter.write(BenchmarkReturnResourceBounds.evidence(summary, late))));
        assertThatThrownBy(() -> BenchmarkReturnCpuPerRow.phase(resources, late, retained))
                .hasMessageContaining("OUTER_RESOURCE_READ_COVERAGE_MISSING");
    }

    /** Ten serial 20ms reads every 200ms starting at 0.6s; CPU advances 40ms per read. */
    private static BenchmarkResourceSampler.Summary trace() {
        var attempts = new ArrayList<BenchmarkResourceSampler.Attempt>();
        for (int index = 1; index <= 10; index++) {
            long started = 400 * MS + index * 200 * MS;
            attempts.add(new BenchmarkResourceSampler.Attempt(index, started, started + 20 * MS, BenchmarkResourceSampler.Outcome.SUCCESS,
                    null, new BenchmarkProcessProbe.Snapshot(OptionalLong.of(10_000 * MS + index * 40 * MS),
                            OptionalLong.of(100_000_000L + index), OptionalLong.of(200_000_000L + index), OptionalLong.of(index))));
        }
        var diagnostics = new BenchmarkResourceSampler.SamplingDiagnostics("COMPLETE", 10, 0, 10 * 20 * MS, 20 * MS, 0, attempts, 1504);
        return new BenchmarkResourceSampler.Summary(9 * 40 * MS, 9, 100_000_010L, 200_000_010L, 10, Optional.of(diagnostics));
    }
    private static List<BenchmarkReturnTimeBounds.Delivery> cohort() {
        var rows = new ArrayList<BenchmarkReturnTimeBounds.Delivery>();
        for (int index = 0; index <= 10; index++) rows.add(delivery("k" + index, 1_000 * MS + index * 100 * MS));
        return rows;
    }
    private static BenchmarkReturnTimeBounds.Delivery delivery(String key, long returned) {
        var point = new BenchmarkCausalClock.Interval(returned, returned + 1);
        return new BenchmarkReturnTimeBounds.Delivery("bench_copy/bench_copy_orders", key, 1, point, point,
                new BenchmarkCausalClock.Interval(1, 2));
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> object(Object value) { return (Map<String, Object>) value; }
}
