package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineBenchmarkReturnLiveRunTest {
    @Test void original_three_workloads_have_five_alternating_forks_per_arm() {
        var config = PipelineBenchmarkReturnLiveRun.config(plan());
        assertThat(config.slots()).hasSize(30);
        for (String workload : List.of("copy", "stateless", "stateful")) {
            assertThat(config.slots().stream().filter(slot -> slot.workload().equals(workload)).map(PipelineBenchmarkReturnLiveRun.Slot::arm).toList())
                    .isEqualTo(PipelineBenchmarkComparison.schedule());
        }
    }
    @Test void fewer_or_reordered_forks_cannot_be_dispatched() {
        var plan = plan(); var slots = slots(plan); slots.removeLast();
        assertThatThrownBy(() -> PipelineBenchmarkReturnLiveRun.config(plan)).isInstanceOf(AssertionError.class);
        var reordered = plan(); java.util.Collections.swap(slots(reordered), 0, 1);
        assertThatThrownBy(() -> PipelineBenchmarkReturnLiveRun.config(reordered)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("original workload/arm schedule");
    }
    @Test void a_reused_ended_slot_path_cannot_be_refilled() {
        var plan = plan(); var slots = slots(plan);
        @SuppressWarnings("unchecked") var first = (Map<String, Object>) slots.getFirst();
        @SuppressWarnings("unchecked") var second = (Map<String, Object>) slots.get(1);
        second.put("reportPath", first.get("reportPath"));
        assertThatThrownBy(() -> PipelineBenchmarkReturnLiveRun.config(plan)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("slot path is repeated");
    }
    @Test void old_date_or_business_gate_does_not_select_the_return_controller() {
        var plan = plan(); plan.put("method", BenchmarkTargetClock.OPERATION_DATE_MEASUREMENT_METHOD);
        assertThatThrownBy(() -> PipelineBenchmarkReturnLiveRun.config(plan)).isInstanceOf(AssertionError.class);
        plan.put("method", BenchmarkReturnMethodAdmission.METHOD); plan.put("gate", "BUSINESS_OPTIMIZATION");
        assertThatThrownBy(() -> PipelineBenchmarkReturnLiveRun.config(plan)).isInstanceOf(AssertionError.class);
    }
    private static Map<String, Object> plan() {
        var slots = new ArrayList<Map<String, Object>>(); int at = 0;
        for (String workload : List.of("copy", "stateless", "stateful")) {
            int a = 0, b = 0;
            for (var arm : PipelineBenchmarkComparison.schedule()) {
                int ordinal = arm == PipelineBenchmarkComparison.Arm.A ? ++a : ++b; String prefix = "/controlled/slot-" + ++at;
                var row = new LinkedHashMap<String, Object>(); row.put("workload", workload); row.put("arm", arm.name()); row.put("ordinal", ordinal);
                row.put("registrationPath", prefix + "/input.json"); row.put("registrationSha256", "0".repeat(64));
                row.put("reportPath", prefix + "/report.json"); row.put("outcomeXmlPath", prefix + "/outcome.xml"); slots.add(row);
            }
        }
        var plan = new LinkedHashMap<String, Object>(); plan.put("schemaVersion", 1); plan.put("method", BenchmarkReturnMethodAdmission.METHOD);
        plan.put("gate", "OBSERVABILITY_COST"); plan.put("slots", slots); plan.put("repository", "/controlled/repo");
        plan.put("maven", "/controlled/maven/bin/mvn"); plan.put("javaHome", "/controlled/java");
        plan.put("baselineJar", "/controlled/a.jar"); plan.put("candidateJar", "/controlled/b.jar");
        plan.put("baselineJarBytes", Map.of("sha256", "a".repeat(64), "bytes", 10));
        plan.put("candidateJarBytes", Map.of("sha256", "b".repeat(64), "bytes", 11)); return plan;
    }
    @SuppressWarnings("unchecked") private static List<Object> slots(Map<String, Object> plan) { return (List<Object>) plan.get("slots"); }
}
