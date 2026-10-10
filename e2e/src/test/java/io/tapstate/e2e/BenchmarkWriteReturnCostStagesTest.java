package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkWriteReturnCostStagesTest {
    private static final BenchmarkCausalClock.Identity OWNER = new BenchmarkCausalClock.Identity(17, 1_000);

    @Test void complete_elapsed_stages_remain_immutable_observations_without_acceptance() {
        String raw = raw(); var result = parse(raw);
        assertThat(result).containsEntry("state", "RECORDED").containsEntry("completeTimedCalls", 2L)
                .containsEntry("raw", raw).containsEntry("rawRetained", true)
                .containsEntry("performanceAcceptanceEligible", false).containsEntry("samplingCostQualified", false)
                .containsEntry("costAcceptanceEligible", false).containsEntry("formalPerformance", false)
                .containsEntry("causalOverheadQualified", false);
        assertThat((Map<?, ?>) result.get("stages")).hasSize(6);
        assertThatThrownBy(() -> result.put("state", "changed")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> ((Map<?, ?>) result.get("stages")).clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void unknown_retains_exact_finite_incomplete_counts_and_its_original_reason() {
        var root = summary(); root.put("state", "UNKNOWN"); root.put("reason", "CLOCK_UNAVAILABLE"); root.put("completeTimedCalls", 0L);
        var stages = new LinkedHashMap<String, Object>();
        BenchmarkWriteReturnCostStages.STAGES.forEach(name -> stages.put(name, Map.of("count", 0L, "sumNanos", 0L, "maxNanos", 0L)));
        root.put("stages", stages); String raw = JsonWriter.write(root);
        assertThat(parse(raw)).containsEntry("state", "UNKNOWN").containsEntry("completeTimedCalls", 0L)
                .containsEntry("reason", "CLOCK_UNAVAILABLE").containsEntry("raw", raw);
    }

    @Test void malformed_duplicate_and_oversized_transport_is_refused_without_losing_bounded_raw() {
        for (String raw : List.of("{}", raw() + "x", "{\"pid\":17," + raw().substring(1),
                raw().replace("\"count\":2", "\"count\":2,\"count\":2"), " " + raw(), "x".repeat(8193))) {
            assertThatThrownBy(() -> parse(raw)).isInstanceOf(AssertionError.class);
        }
        assertThat(BenchmarkWriteReturnCostStages.rawEvidence("malformed"))
                .containsEntry("raw", "malformed").containsEntry("rawRetained", true);
        assertThat(BenchmarkWriteReturnCostStages.rawEvidence("x".repeat(8193)))
                .containsEntry("rawAvailable", true).containsEntry("rawRetained", false).containsEntry("rawLengthChars", 8193)
                .doesNotContainKey("raw");
    }

    @Test void foreign_runtime_epoch_window_or_full_call_count_cannot_supply_cost_facts() {
        for (String field : List.of("pid", "jvmStartTimeMillis", "epoch", "fullCallCount")) {
            var root = summary(); root.put(field, ((Long) root.get(field)) + 1);
            assertThatThrownBy(() -> parse(JsonWriter.write(root))).isInstanceOf(AssertionError.class);
        }
        var foreign = summary(); foreign.put("windowBase64", encode("other"));
        assertThatThrownBy(() -> parse(JsonWriter.write(foreign))).isInstanceOf(AssertionError.class).hasMessageContaining("window");
    }

    @Test void window_base64_and_utf8_are_canonical_and_bounded() {
        for (String encoded : List.of(encode("measured").replace("=", ""), "!", "wA==", encode("x".repeat(513)))) {
            var root = summary(); root.put("windowBase64", encoded);
            assertThatThrownBy(() -> parse(JsonWriter.write(root))).isInstanceOf(AssertionError.class);
        }
        var unicode = summary(); unicode.put("windowBase64", encode("caf\u00e9"));
        assertThat(BenchmarkWriteReturnCostStages.parse(JsonWriter.write(unicode), OWNER, 4, "caf\u00e9", 2))
                .containsEntry("state", "RECORDED");
    }

    @Test void schema_shape_state_scope_and_every_qualification_flag_are_exact() {
        for (String flag : List.of("performanceAcceptanceEligible", "samplingCostQualified", "costAcceptanceEligible", "formalPerformance", "causalOverheadQualified")) {
            var root = summary(); root.put(flag, true);
            assertThatThrownBy(() -> parse(JsonWriter.write(root))).isInstanceOf(AssertionError.class);
            root.remove(flag);
            assertThatThrownBy(() -> parse(JsonWriter.write(root))).isInstanceOf(AssertionError.class);
        }
        for (String field : List.of("schemaVersion", "enabled", "state", "reason", "timeUnit", "timeScope")) {
            var root = summary(); root.put(field, "unsupported");
            assertThatThrownBy(() -> parse(JsonWriter.write(root))).isInstanceOf(AssertionError.class);
        }
        var extra = summary(); extra.put("unregistered", 1L);
        assertThatThrownBy(() -> parse(JsonWriter.write(extra))).isInstanceOf(AssertionError.class);
        var unknown = summary(); unknown.put("state", "UNKNOWN");
        assertThatThrownBy(() -> parse(JsonWriter.write(unknown))).isInstanceOf(AssertionError.class);
    }

    @Test void numbers_never_coerce_floats_exponents_leading_zeroes_or_overflow_to_longs() {
        for (String value : List.of("2.0", "2e0", "02", "-0", "true", "9223372036854775808")) {
            String bad = raw().replace("\"fullCallCount\":2", "\"fullCallCount\":" + value);
            assertThatThrownBy(() -> parse(bad)).isInstanceOf(AssertionError.class);
        }
        var root = summary(); root.put("stages", stages(2, Long.MAX_VALUE, Long.MAX_VALUE));
        assertThat(parse(JsonWriter.write(root))).containsEntry("state", "RECORDED");
    }

    @Test void stage_roster_and_count_sum_maximum_relationships_are_complete() {
        for (Map<String, Object> bad : List.of(stages(1, 30, 20), stages(2, -1, 0), stages(2, 10, 20), stages(2, 41, 20), stages(0, 1, 0))) {
            var root = summary(); root.put("stages", bad);
            assertThatThrownBy(() -> parse(JsonWriter.write(root))).isInstanceOf(AssertionError.class);
        }
        var missing = stages(2, 30, 20); missing.remove("SCOPE_AFTER"); var root = summary(); root.put("stages", missing);
        assertThatThrownBy(() -> parse(JsonWriter.write(root))).isInstanceOf(AssertionError.class);
        var extra = stages(2, 30, 20); extra.put("UNREGISTERED_STAGE", Map.of("count", 2L, "sumNanos", 30L, "maxNanos", 20L));
        root.put("stages", extra);
        assertThatThrownBy(() -> parse(JsonWriter.write(root))).isInstanceOf(AssertionError.class);
    }

    static String raw() { return JsonWriter.write(summary()); }
    private static Map<String, Object> parse(String raw) { return BenchmarkWriteReturnCostStages.parse(raw, OWNER, 4, "measured", 2); }
    private static String encode(String value) { return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8)); }
    private static Map<String, Object> stages(long count, long sum, long maximum) {
        var stages = new LinkedHashMap<String, Object>();
        BenchmarkWriteReturnCostStages.STAGES.forEach(name -> stages.put(name, Map.of("count", count, "sumNanos", sum, "maxNanos", maximum)));
        return stages;
    }
    private static Map<String, Object> summary() {
        var root = new LinkedHashMap<String, Object>();
        root.put("schemaVersion", 1L); root.put("enabled", true); root.put("pid", 17L); root.put("jvmStartTimeMillis", 1_000L);
        root.put("epoch", 4L); root.put("windowBase64", encode("measured")); root.put("fullCallCount", 2L); root.put("completeTimedCalls", 2L);
        root.put("state", "RECORDED"); root.put("reason", "NONE"); root.put("timeUnit", "ns"); root.put("timeScope", "ELAPSED_NOT_CPU");
        root.put("performanceAcceptanceEligible", false); root.put("samplingCostQualified", false); root.put("costAcceptanceEligible", false);
        root.put("formalPerformance", false); root.put("causalOverheadQualified", false); root.put("stages", stages(2, 30, 20));
        return root;
    }
}
