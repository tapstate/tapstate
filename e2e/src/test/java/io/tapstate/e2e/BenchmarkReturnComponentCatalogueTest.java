package io.tapstate.e2e;

import java.util.AbstractList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnComponentCatalogueTest {
    @Test void old_catalogue_has_no_component_payload_new_catalogue_keeps_each_duplicate_representation() {
        var workload = BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateless");
        var old = BenchmarkReturnWorkBudgetReportPlan.prepare(workload);
        var added = BenchmarkReturnWorkBudgetReportPlan.prepareWithReturnComponents(workload, false);
        assertThat(old.metadata()).doesNotContainKey("returnComponentsIncluded");
        var components = index(added);
        assertThat(components).containsKeys("METHOD_SOURCE_BATCH_PROJECTION", "METHOD_CLOCK_SAMPLE_PROJECTION",
                "METHOD_RAW_RETURN_PAGES", "METHOD_NOMINAL_MAPPING", "METHOD_ADMISSION_HEADERS",
                "METHOD_NATIVE_SOURCE_AUTHORITY", "METHOD_COMMON_WINDOW_CERTIFICATES", "METHOD_SERIAL_HELLO_READINGS");
        assertThat(components.get("METHOD_SOURCE_BATCH_PROJECTION").rows()).isEqualTo(961);
        assertThat(components.get("METHOD_CLOCK_SAMPLE_PROJECTION").rows()).isEqualTo(513);
        assertThat(components.get("METHOD_COMMON_WINDOW_CERTIFICATES").rows()).isEqualTo(2);
        assertThat(components.get("METHOD_SERIAL_HELLO_READINGS").rows()).isEqualTo(6000);
        assertThat(components.get("METHOD_RAW_RETURN_PAGES").strings()).isEqualTo(4L * ((2097152L + 2L * 512) / 3));
        assertThat(added.plan().nodeUpper()).isGreaterThan(old.plan().nodeUpper());
        assertThat(added.plan().stringUtf8Upper()).isGreaterThan(old.plan().stringUtf8Upper());
        assertThat(added.metadata()).containsEntry("wholeMethodCostQualified", false)
                .containsEntry("performanceAcceptanceEligible", false);
    }

    @Test void new_source_catalogue_accepts_real_owned_authority_and_boundary_hello_scalar_without_encoding_a_payload() {
        var source = BenchmarkNativeSourceAuthority.sourceRecipeEvidence();
        var reading = new BenchmarkTargetClock.Reading("x".repeat(65536), "0".repeat(24), 1, 2, 3, 4, 5);
        var packet = Map.of("authority", source, "helloSerialValidation", Map.of("readings", List.of(reading.evidence())));
        var prepared = BenchmarkReturnWorkBudgetReportPlan.prepareWithReturnComponents(
                BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateless"), false);
        var frozen = BenchmarkReturnWorkBudgetDefinitions.freezeOutputTree(packet, prepared.plan());
        assertThat(frozen.work().stringUtf8Bytes()).isGreaterThanOrEqualTo(65536);
        for (String flag : BenchmarkReturnProducerOrigins.FLAGS) assertThat(source.get(flag)).isEqualTo(false);
        @SuppressWarnings("unchecked") var copy = (Map<String, Object>) frozen.value();
        assertThatThrownBy(() -> copy.put("authority", Map.of())).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void actual_table_entry_is_counted_once_without_charging_its_upper_as_observed_material() {
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "component-table")) {
            scope.bindRoot(17, 23);
            var prepared = BenchmarkReturnComponentSourceWork.prepare(limits());
            assertThat(prepared.additions()).containsKeys(BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS,
                    BenchmarkReturnWorkBudget.Activity.ASSOCIATION, BenchmarkReturnWorkBudget.Activity.COPY,
                    BenchmarkReturnWorkBudget.Activity.COMMON_WINDOW, BenchmarkReturnWorkBudget.Activity.FAILURE_RETENTION);
            var row = activity(scope.snapshot(), "INSTALL_BINDINGS");
            assertThat(row).containsEntry("attempts", 2L).containsEntry("completedAttempts", 2L)
                    .containsEntry("issuedRequests", 0L).containsEntry("failedAttempts", 0L);
            @SuppressWarnings("unchecked") var observed = (Map<String, Object>) row.get("actual");
            assertThat(observed).containsEntry("records", 0L).containsEntry("bytes", 0L);
            assertThat(scope.snapshot()).containsEntry("state", "OPEN").containsEntry("pendingOperations", 0L);
            assertThatThrownBy(() -> prepared.sourceEvidence().put("state", "CLOSED"))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test void exact_role_count_guard_precedes_table_construction() {
        int roles = BenchmarkReturnWorkBudgetBindings.SourceRole.values().length;
        assertThatThrownBy(() -> new BenchmarkReturnComponentSourceWork.Limits(roles - 1, plan()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("source role roster differs");
        assertThatThrownBy(() -> new BenchmarkReturnComponentSourceWork.Limits(roles + 1, plan()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("source role roster differs");
    }

    @Test void actual_phase_constructor_refuses_unbounded_obtained_hello_before_copy_or_projection() {
        var primary = new BenchmarkTargetClock.Reading("x".repeat(65537), "0".repeat(24), 1, 2, 3, 4, 5);
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "hello-material")) {
            assertThatThrownBy(() -> new BenchmarkReturnMethodAdmission.PhaseFacts(null, null, List.of(), null,
                    null, null, Map.of(), Map.of(), List.of(primary)))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("component scalar exceeds prior material cap");
            assertThat(scope.primaryCause()).isInstanceOf(AssertionError.class);
            assertThat(activity(scope.snapshot(), "ASSOCIATION")).containsEntry("failedAttempts", 1L);
            assertThat(scope.snapshot()).containsEntry("pendingOperations", 0L).containsEntry("state", "UNKNOWN");
        }
        // The ordinary constructor keeps the previous first refusal when no work scope is active.
        assertThatThrownBy(() -> new BenchmarkReturnMethodAdmission.PhaseFacts(null, null, List.of(), null,
                null, null, Map.of(), Map.of(), List.of(primary))).hasMessageContaining("CAPTURE_CALL_ROSTER_EMPTY");
    }

    @Test void oversized_hello_roster_is_rejected_without_get_or_iteration() {
        var gets = new AtomicInteger();
        List<BenchmarkTargetClock.Reading> hostile = new AbstractList<>() {
            @Override public int size() { return 6001; }
            @Override public BenchmarkTargetClock.Reading get(int index) {
                gets.incrementAndGet(); throw new AssertionError("the refused roster was traversed");
            }
        };
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "hello-roster")) {
            assertThatThrownBy(() -> new BenchmarkReturnMethodAdmission.PhaseFacts(null, null, List.of(), null,
                    null, null, Map.of(), Map.of(), hostile)).hasMessageContaining("phase source or hello roster");
            assertThat(gets).hasValue(0);
            assertThat(scope.snapshot()).containsEntry("pendingOperations", 0L);
        }
    }

    @Test void profile_uses_source_parent_maxima_and_expanded_registered_tree_without_completed_method_credit() {
        var source = BenchmarkReturnWorkBudgetProfile.inspect(inputs());
        var expected = BenchmarkReturnComponentSourceWork.additions(new BenchmarkReturnComponentSourceWork.Limits(
                BenchmarkReturnWorkBudgetBindings.SourceRole.values().length, source.fieldsRegisteredOutputPlan()));
        expected.forEach((actor, upper) -> assertThat(upper.within(source.perOperationUpper().get(actor))).isTrue());
        assertThat(source.gaps()).isEmpty();
        assertThat(source.inputRegistrationInfo()).containsEntry("structuralWorkBudgetQualified", false)
                .containsEntry("wholeMethodCostQualified", false).containsKey("returnComponentLocalSourceUpper");
        @SuppressWarnings("unchecked") var report = (Map<String, Object>) source.inputRegistrationInfo().get("outputPlanSource");
        assertThat(report).containsEntry("returnComponentsIncluded", true);
        assertThat(source.requireDefinition().sourceFormulaSha256()).hasSize(64);
    }

    @Test void helper_and_known_authority_are_append_only_root_sources_with_no_new_producer_binding() {
        var helper = BenchmarkReturnWorkBudgetBindings.SourceRole.RETURN_COMPONENT_SOURCE_WORK;
        var authority = BenchmarkReturnWorkBudgetBindings.SourceRole.NATIVE_SOURCE_AUTHORITY;
        assertThat(helper.ordinal()).isEqualTo(BenchmarkReturnWorkBudgetBindings.SourceRole.RETURN_METHOD_ADMISSION.ordinal() + 1);
        assertThat(authority.ordinal()).isEqualTo(helper.ordinal() + 1);
        assertThat(helper.owners()).isEqualTo(Set.of(BenchmarkReturnWorkBudgetBindings.Owner.ROOT));
        assertThat(authority.owners()).isEqualTo(Set.of(BenchmarkReturnWorkBudgetBindings.Owner.ROOT));
        assertThat(helper.sourcePath()).endsWith("io/tapstate/e2e/BenchmarkReturnComponentSourceWork.java");
        assertThat(authority.binaryName()).isEqualTo("io.tapstate.e2e.BenchmarkNativeSourceAuthority");
        assertThat(BenchmarkReturnWorkBudgetBindings.SourceRole.PDK_WRITER.owners())
                .containsExactlyInAnyOrder(BenchmarkReturnWorkBudgetBindings.Owner.ROOT, BenchmarkReturnWorkBudgetBindings.Owner.PRODUCER);
    }

    @Test void fixed_factories_keep_eight_checked_units_and_overflow_refusal() {
        var one = BenchmarkReturnComponentSourceWork.factoryUpper(1);
        var two = BenchmarkReturnComponentSourceWork.factoryUpper(2);
        assertThat(two).isEqualTo(one.times(2));
        assertThat(one.requests()).isGreaterThan(one.records());
        assertThat(one.frames()).isZero();
        assertThatThrownBy(() -> BenchmarkReturnComponentSourceWork.factoryUpper(Long.MAX_VALUE))
                .isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetReportPlan.returnCatalogueUpper(Long.MAX_VALUE))
                .isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> BenchmarkReturnComponentSourceWork.factoryUpper(-1)).isInstanceOf(AssertionError.class);
        assertThat(BenchmarkReturnComponentSourceWork.trustedCacheUpper().requests()).isPositive();
        assertThat(BenchmarkReturnComponentSourceWork.authorityInitializationUpper().requests()).isPositive();
    }

    @Test void actual_144000_pool_uses_existing_sort_and_explicit_copy_entries_with_the_full_material_basis() {
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "pooled-material-source")) {
            var one = new BenchmarkReturnIntervalMetrics.Phase("cold", 48000, 48000, 1000000000L, 1000000000L,
                    java.util.Collections.nCopies(48000, new BenchmarkCausalClock.Interval(1, 2)));
            var two = new BenchmarkReturnIntervalMetrics.Phase("cdc", 96000, 96000, 1000000000L, 1000000000L,
                    java.util.Collections.nCopies(96000, new BenchmarkCausalClock.Interval(1, 2)));
            var pooled = BenchmarkReturnIntervalMetrics.aggregate(List.of(one, two));
            assertThat(pooled.fullCohortRows()).isEqualTo(144000);
            assertThat(pooled.p99()).isEqualTo(new BenchmarkCausalClock.Interval(1, 2));
            assertThat(activity(scope.snapshot(), "SORT")).containsEntry("issuedRequests", 2L).containsEntry("completedRequests", 2L);
            assertThat(activity(scope.snapshot(), "COPY")).containsEntry("issuedRequests", 3L).containsEntry("completedRequests", 3L);
            assertThat(scope.snapshot()).containsEntry("state", "OPEN").containsEntry("pendingOperations", 0L);
            assertThat(BenchmarkReturnComponentSourceWork.pooledSortUpper().requests()).isEqualTo(32L + 8L * 262144);
        }
    }

    @Test void actual_native_lexical_guard_refuses_deep_material_before_owned_decode() {
        String nested = "[".repeat(17) + "0" + "]".repeat(17);
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "native-prior-shape")) {
            assertThatThrownBy(() -> BenchmarkReturnComponentSourceWork.entered(
                    BenchmarkReturnComponentSourceWork.Kind.RUN_ROOT_COMPARE,
                    () -> { BenchmarkReturnComponentSourceWork.guardNativeJson(nested); return null; }))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("prior shape bound");
            assertThat(activity(scope.snapshot(), "INSTALL_BINDINGS")).containsEntry("failedAttempts", 1L);
            assertThat(scope.snapshot()).containsEntry("pendingOperations", 0L).containsEntry("state", "UNKNOWN");
        }
    }

    @Test void absent_trusted_run_keeps_wrapper_identity_and_no_component_entry() {
        assertThat(BenchmarkReturnWorkBudgetRun.current()).isNull();
        var original = Map.<String, Object>of("state", "controlled");
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "absent-trusted-run")) {
            assertThat(BenchmarkReturnWorkBudgetRun.rootClockMetadataBeforeEvidence(original, "phase")).isSameAs(original);
            assertThat(BenchmarkReturnWorkBudgetRun.rootClockMetadataBeforeBasis("phase"))
                    .isEqualTo("ORIGINAL_PHASE_ROOT_METADATA_GETTER");
            BenchmarkReturnWorkBudgetRun.requireEarlyRootMetadataMatchesAfter(null);
            assertThat(activity(scope.snapshot(), "INSTALL_BINDINGS")).containsEntry("attempts", 0L);
        }
    }

    private static BenchmarkReturnWorkBudgetProfile.Inputs inputs() {
        var files = java.util.Arrays.stream(BenchmarkReturnWorkBudgetProfile.FileRole.values())
                .filter(role -> role != BenchmarkReturnWorkBudgetProfile.FileRole.ROOT_CPU_PROVIDER_LIBRARY)
                .map(role -> new BenchmarkReturnWorkBudgetProfile.FileInput(role, "f".repeat(64), 1)).toList();
        return new BenchmarkReturnWorkBudgetProfile.Inputs("component-source-profile", "e".repeat(64),
                BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateless"), files,
                new BenchmarkReturnWorkBudgetProfile.Flags("B", "PLAIN", "PERIODIC", "ON", true, true, true, true, false, Set.of()));
    }
    private static BenchmarkReturnComponentSourceWork.Limits limits() {
        return new BenchmarkReturnComponentSourceWork.Limits(BenchmarkReturnWorkBudgetBindings.SourceRole.values().length, plan());
    }
    private static BenchmarkReturnWorkBudgetDefinitions.OutputPlan plan() {
        return new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(32768, 16, 8192, 65536, 512, 64);
    }
    private static BenchmarkReturnWorkBudget.Definition definition() {
        var upper = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        upper.putAll(BenchmarkReturnComponentSourceWork.additions(limits()));
        var excluded = EnumSet.allOf(BenchmarkReturnWorkBudget.Activity.class); excluded.removeAll(upper.keySet());
        return new BenchmarkReturnWorkBudget.Definition("component-catalogue-controls", "a".repeat(64), Map.of(), upper, excluded);
    }
    private static Map<String, BenchmarkReturnWorkBudgetReportPlan.Component> index(BenchmarkReturnWorkBudgetReportPlan.Prepared prepared) {
        var result = new LinkedHashMap<String, BenchmarkReturnWorkBudgetReportPlan.Component>();
        for (var item : prepared.components()) result.put(item.id(), item);
        return result;
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> activity(Map<String, Object> snapshot, String key) {
        return (Map<String, Object>) ((Map<String, Object>) snapshot.get("activities")).get(key);
    }
}
