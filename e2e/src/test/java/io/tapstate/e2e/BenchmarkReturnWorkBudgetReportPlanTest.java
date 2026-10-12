package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnWorkBudgetReportPlanTest {
    @Test
    void complete_plan_covers_full_delivery_arrays_coverage_and_source_batch_material() {
        var prepared = BenchmarkReturnWorkBudgetReportPlan.prepare(original());
        List<Long> full = java.util.Collections.nCopies(96000, 1L), cohort = java.util.Collections.nCopies(48000, 2L);
        var coverage = new LinkedHashMap<String, Object>();
        for (int row = 0; row < 96000; row++) coverage.put("cdc:" + row + ":0", 1L);
        Map<String, Object> fork = Map.of("deliveryNanos", cohort, "declaredSourceCoverage", coverage,
                "observedTargetCoverage", coverage, "logicalCoverage", coverage,
                "deliveryTimeline", Map.of("fullObservedAtNanos", full, "cohortObservedAtNanos", cohort,
                        "cohortServerOperationWallMillis", cohort, "cohortOperationStreams", List.of(cohort)));
        var checked = BenchmarkReturnWorkBudgetDefinitions.requireOutputTree(fork, prepared.plan());
        assertThat(checked.nodes()).isGreaterThan(3L * 96000 + 48000);
        assertThat(prepared.components()).extracting(BenchmarkReturnWorkBudgetReportPlan.Component::id)
                .contains("ALL_COVERAGE_MAPS", "FULL_AND_COHORT_TIMELINES", "SOURCE_BATCH_DOUBLE_REPRESENTATION");
        assertThat(prepared.metadata()).containsEntry("actualSizeUsedAsPriorBound", false)
                .containsEntry("wholeMethodCostQualified", false).containsEntry("performanceAcceptanceEligible", false);
    }

    @Test
    void complete_plan_includes_return_resource_runtime_budget_and_final_seal_sources() {
        var prepared = BenchmarkReturnWorkBudgetReportPlan.prepare(original());
        Map<String, BenchmarkReturnWorkBudgetReportPlan.Component> parts = index(prepared);
        assertThat(parts.get("RETURN_ROWS_IF_MATERIALIZED").rows()).isEqualTo(262144);
        assertThat(parts.get("RESOURCE_TRACES_AND_REFUSALS").rows()).isEqualTo(3L * 4096);
        assertThat(parts.get("RETURN_CLOCK_ROSTERS").rows()).isEqualTo(2L * BenchmarkCausalClock.MAX_SAMPLES);
        assertThat(parts.get("NATIVE_BASELINE_CHECKPOINTS").rows()).isEqualTo(8);
        assertThat(parts.get("LEGACY_TARGET_HELLO_TIMELINE").rows()).isEqualTo(6002);
        assertThat(parts).containsKeys("OWNED_RUNTIME_AND_FAILURE_RECEIPT", "PRODUCER_WORK_BODY_AND_EVALUATION",
                "ROOT_WORK_SELF_AND_CLOSED_SEAL", "CLOCK_REQUEST_COST_AND_FAILURE_PACKETS");
        assertThat(prepared.plan().depthUpper()).isEqualTo(28);
    }

    @Test
    void bounded_raw_base64_padding_is_derived_before_any_output_is_observed() {
        var prepared = BenchmarkReturnWorkBudgetReportPlan.prepare(original());
        var raw = index(prepared).get("RAW_RETURN_BASE64");
        long independent = 2L * 4 * ((8L * 1024 * 1024 + 2L * 512) / 3);
        assertThat(raw.strings()).isEqualTo(independent);
        assertThat(raw.nodes()).isEqualTo(2L * (512 + 1));
        assertThat(raw.leafUtf8Upper()).isEqualTo(4 * ((65536 + 2) / 3));
        String page = "x".repeat(raw.leafUtf8Upper());
        assertThat(BenchmarkReturnWorkBudgetDefinitions.requireOutputTree(Map.of("pagesBase64", List.of(page)), prepared.plan()).stringUtf8Bytes())
                .isGreaterThanOrEqualTo(page.length());
    }

    @Test
    void report_plan_preserves_typed_refusal_and_pending_resource_shapes() {
        var prepared = BenchmarkReturnWorkBudgetReportPlan.prepare(original());
        List<Map<String, Object>> attempts = new ArrayList<>();
        for (int at = 1; at <= 4096; at++) attempts.add(Map.of("index", at, "startedAtNanos", 1L,
                "completedAtNanos", 2L, "durationNanos", 1L, "outcome", "SUCCESS", "failureType", "NONE",
                "reading", Map.of("state", "COMPLETE", "cpuNanos", 3L, "heapUsedBytes", 4L,
                        "rssBytes", 5L, "gcCollectionMillis", 6L)));
        var refusal = Map.of("state", "UNKNOWN", "completedSampling", Map.of("attempts", attempts),
                "actualPendingRead", Map.of("state", "PENDING", "index", 4097, "startedAtNanos", 3L,
                        "finalRead", true, "checkpointRead", false));
        assertThat(BenchmarkReturnWorkBudgetDefinitions.requireOutputTree(refusal, prepared.plan()).nodes()).isPositive();
        assertThat(prepared.metadata().get("scope")).isEqualTo("ONE_COMPLETE_FORK_OR_ONE_CONTROLLED_STDOUT_FAILURE_TREE");
    }

    @Test
    void source_components_combine_with_checked_sums_without_replacing_caps_with_actual_size() {
        var prepared = BenchmarkReturnWorkBudgetReportPlan.prepare(original());
        long nodes = 1, strings = 0;
        for (var component : prepared.components()) {
            nodes = Math.addExact(nodes, Math.multiplyExact(component.rows(), component.nodesPerRow()));
            strings = Math.addExact(strings, Math.multiplyExact(component.rows(), component.stringsPerRow()));
        }
        assertThat(prepared.plan().nodeUpper()).isEqualTo(nodes);
        assertThat(prepared.plan().stringUtf8Upper()).isEqualTo(strings);
        assertThat(prepared.plan().encodedUpperBytes()).isEqualTo(Math.addExact(Math.multiplyExact(nodes, 72), Math.multiplyExact(strings, 6)));
        assertThatThrownBy(() -> new BenchmarkReturnWorkBudgetReportPlan.Component("overflow", "fixed", Long.MAX_VALUE,
                2, 0, 1, 1, "registered")).isInstanceOf(ArithmeticException.class);
        assertThat(prepared.plan().numberTextUpper()).isEqualTo(64);
    }

    @Test
    void original_seed_rows_topology_and_pacing_cannot_be_relaxed_by_a_report_cap() {
        var original = original();
        var wrongSeed = new BenchmarkReturnWorkBudgetProfile.WorkloadInput(original.id(), original.rows(), 7,
                original.database(), original.setupStatements(), original.pipelines(), original.sourceChains(), original.phases());
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetReportPlan.completePilot(wrongSeed)).isInstanceOf(AssertionError.class);
        var paced = BenchmarkReturnWorkBudgetProfile.WorkloadInput.from(BenchmarkWorkloadDefinitions.pacedCalibration("stateless"));
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetReportPlan.completePilot(paced)).isInstanceOf(AssertionError.class);
        assertThat(BenchmarkReturnWorkBudgetReportPlan.completePilot(
                BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("copy")).nodeUpper()).isPositive();
    }

    @Test void original_all_three_geometries_scale_every_measured_report_family_and_keep_per_fork_receipts_once() {
        for (String id : List.of("copy", "stateless", "stateful")) {
            var input = BenchmarkReturnWorkBudgetProfile.WorkloadInput.original(id);
            var shape = BenchmarkReturnWorkBudgetSourceTails.geometry(input);
            var prepared = BenchmarkReturnWorkBudgetReportPlan.prepareWithReturnComponents(input, false);
            var parts = index(prepared); long phases = id.equals("stateful") ? 2 : 1;
            assertThat(prepared.metadata()).containsEntry("measuredPhases", phases)
                    .containsEntry("fullMeasuredOutputs", id.equals("stateful") ? 288000L : 96000L)
                    .containsEntry("pooledCohortRows", id.equals("stateful") ? 144000L : 48000L);
            assertThat(parts.get("SOURCE_BATCH_DOUBLE_REPRESENTATION").rows()).isEqualTo(960 * phases);
            assertThat(parts.get("RAW_RETURN_BASE64").rows()).isEqualTo(phases);
            assertThat(parts.get("RESOURCE_TRACES_AND_REFUSALS").rows()).isEqualTo(3 * phases * 4096);
            assertThat(parts.get("RETURN_CLOCK_ROSTERS").rows()).isEqualTo(2L * phases * BenchmarkCausalClock.MAX_SAMPLES);
            assertThat(parts.get("RETURN_ROWS_IF_MATERIALIZED").rows()).isEqualTo(phases * 262144);
            assertThat(parts.get("NATIVE_METADATA_AND_REGISTRATIONS").rows()).isEqualTo(8 * phases);
            assertThat(parts.get("PRODUCER_COLD_ORIGIN_PACKETS").rows()).isEqualTo(6 * phases);
            assertThat(parts.get("NATIVE_BASELINE_CHECKPOINTS").rows()).isEqualTo(8 * phases);
            assertThat(parts.get("LEGACY_TARGET_HELLO_TIMELINE").rows()).isEqualTo(6002 * phases);
            assertThat(parts.get("ROOT_CPU_ENVELOPE").rows()).isEqualTo(phases);
            assertThat(parts.get("METHOD_SOURCE_BATCH_PROJECTION").rows()).isEqualTo(shape.measuredBatches() + phases);
            assertThat(parts.get("METHOD_RAW_RETURN_PAGES").rows()).isEqualTo(phases);
            assertThat(parts.get("METHOD_ADMISSION_HEADERS").rows()).isEqualTo(phases + 1);
            assertThat(parts.get("METHOD_COMMON_WINDOW_CERTIFICATES").rows()).isEqualTo(2 * phases);
            assertThat(parts.get("METHOD_SERIAL_HELLO_READINGS").rows()).isEqualTo(6000 * phases);
            assertThat(parts.get("OWNED_RUNTIME_AND_FAILURE_RECEIPT").rows()).isEqualTo(2);
            assertThat(parts.get("ROOT_CLASS_ORIGIN_PACKETS").rows()).isEqualTo(2);
            assertThat(parts.get("EMITTED_COMPILER_ROSTER").rows()).isEqualTo(1);
            assertThat(parts.get("PRODUCER_WORK_BODY_AND_EVALUATION").rows()).isEqualTo(2);
            assertThat(parts.get("NATIVE_CLASS_PHASE_AND_CUT_ROSTER").rows()).isEqualTo(3);
            assertThat(prepared.components()).hasSizeLessThanOrEqualTo(64);
        }
    }

    @Test void stateful_complete_measured_timeline_and_declared_cold_roster_header_fit_before_serialization() {
        var prepared = BenchmarkReturnWorkBudgetReportPlan.prepareWithReturnComponents(
                BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateful"), false);
        var full = java.util.Collections.nCopies(288000, 1L);
        var cohort = java.util.Collections.nCopies(144000, 2L);
        var packet = Map.of("deliveryNanos", cohort, "fullObservedAtNanos", full,
                "cohortObservedAtNanos", cohort, "cohortServerOperationWallMillis", cohort,
                "cohortOperationStreams", List.of(cohort), "producerClassMetadataExpectedPhaseKeys",
                List.of("stateful/cold-read", "stateful/cdc-update"), "producerClassMetadataExpectedPackets", 4);
        var checked = BenchmarkReturnWorkBudgetDefinitions.requireOutputTree(packet, prepared.plan());
        assertThat(checked.nodes()).isGreaterThan(288000 + 4L * 144000);
        assertThat(prepared.metadata()).containsEntry("wholeMethodCostQualified", false)
                .containsEntry("performanceAcceptanceEligible", false);
    }

    @Test
    void source_catalog_and_guard_dependencies_are_immutable_and_do_not_claim_method_qualification() {
        var prepared = BenchmarkReturnWorkBudgetReportPlan.prepare(original());
        assertThat(prepared.components()).allSatisfy(component -> {
            assertThat(component.sourceBranch()).isNotBlank(); assertThat(component.boundBasis()).isNotBlank();
        });
        assertThatThrownBy(() -> prepared.components().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> prepared.metadata().put("wholeMethodCostQualified", true)).isInstanceOf(UnsupportedOperationException.class);
        assertThat(prepared.metadata().get("encodingInvocationBasis"))
                .isEqualTo("COMPLETE_ACTUAL_REPORT_SERIALIZE_CALLS_NO_FUTURE_STDOUT_COUNT_BOUND");
    }

    private static BenchmarkReturnWorkBudgetProfile.WorkloadInput original() {
        return BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateless");
    }
    private static Map<String, BenchmarkReturnWorkBudgetReportPlan.Component> index(BenchmarkReturnWorkBudgetReportPlan.Prepared prepared) {
        var values = new LinkedHashMap<String, BenchmarkReturnWorkBudgetReportPlan.Component>();
        for (var value : prepared.components()) values.put(value.id(), value);
        return values;
    }
}
