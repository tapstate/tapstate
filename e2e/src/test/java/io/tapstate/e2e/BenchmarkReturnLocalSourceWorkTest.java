package io.tapstate.e2e;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnLocalSourceWorkTest {
    private static final BenchmarkReturnLocalSourceWork.Capacity ORIGINAL =
            new BenchmarkReturnLocalSourceWork.Capacity(96_000, 48_000, 960);

    @Test void exact_local_roster_has_positive_request_allowances_and_retains_the_original_caps() {
        var bounds = BenchmarkReturnLocalSourceWork.bounds(ORIGINAL);
        assertThat(bounds.keySet()).containsExactlyInAnyOrderElementsOf(Set.of(
                BenchmarkReturnWorkBudget.Activity.DECODE, BenchmarkReturnWorkBudget.Activity.FRAME_VISIT,
                BenchmarkReturnWorkBudget.Activity.ASSOCIATION, BenchmarkReturnWorkBudget.Activity.RETURN_MAP,
                BenchmarkReturnWorkBudget.Activity.SOURCE_MAP, BenchmarkReturnWorkBudget.Activity.CALL_POINT_CHECK,
                BenchmarkReturnWorkBudget.Activity.SORT, BenchmarkReturnWorkBudget.Activity.SPAN,
                BenchmarkReturnWorkBudget.Activity.RESOURCE_BOUNDS));
        assertThat(bounds.values()).allSatisfy(work -> assertThat(work.requests()).isPositive());
        assertThat(BenchmarkReturnLocalSourceWork.maximumFrameRows()).isEqualTo(512L * 512);
        assertThat(BenchmarkReturnLocalSourceWork.PAGE_BYTES).isEqualTo(65_536);
        assertThat(bounds.get(BenchmarkReturnWorkBudget.Activity.FRAME_VISIT).records()).isEqualTo(1024);
        assertThatThrownBy(bounds::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void a_real_valid_v3_decode_has_a_local_source_upper_without_a_transport_request() throws Exception {
        var bounds = BenchmarkReturnLocalSourceWork.bounds(ORIGINAL);
        try (var scope = scope(bounds)) {
            var page = BenchmarkReturnWorkBudget.operation(scope, BenchmarkReturnWorkBudget.Activity.DECODE,
                    () -> BenchmarkWriteReturnLedger.decode(idlePage()));
            assertThat(page.epoch()).isZero(); assertThat(page.state()).isEqualTo("IDLE"); assertThat(page.calls()).isEmpty();
            var row = activity(scope.snapshot(), "DECODE");
            assertThat(row).containsEntry("attempts", 1L).containsEntry("completedAttempts", 1L).containsEntry("issuedRequests", 0L);
            assertThat(bounds.get(BenchmarkReturnWorkBudget.Activity.DECODE).requests()).isGreaterThan(9);
        }
    }

    @Test void two_sort_and_four_span_pipelines_fit_inclusive_parent_bounds_with_owned_projection_visits() {
        var bounds = BenchmarkReturnLocalSourceWork.bounds(ORIGINAL);
        var sort = bounds.get(BenchmarkReturnWorkBudget.Activity.SORT);
        var span = bounds.get(BenchmarkReturnWorkBudget.Activity.SPAN);
        assertThat(sort.requests()).isGreaterThanOrEqualTo(2 * 6);
        assertThat(sort.passes()).isEqualTo(2); assertThat(sort.elements()).isEqualTo(2L * 48_000);
        assertThat(span.requests()).isGreaterThanOrEqualTo(4 * 4 + 3);
        assertThat(span.passes()).isEqualTo(4); assertThat(span.elements()).isEqualTo(4L * 48_000);
        assertThat(sort.calls()).isGreaterThanOrEqualTo(2L * 48_000);
        assertThat(span.calls()).isGreaterThanOrEqualTo(4L * 48_000);
        assertThat(bounds.get(BenchmarkReturnWorkBudget.Activity.ASSOCIATION).records()).isEqualTo(262_144);
        assertThat(bounds.get(BenchmarkReturnWorkBudget.Activity.ASSOCIATION).elements()).isGreaterThan(2L * 262_144);
    }

    @Test void nominal_point_and_actual_binding_preserve_false_qualifications_without_transport() throws Exception {
        var fixture = BenchmarkWriteReturnPhaseEvidenceTest.nativeFixture(BenchmarkWorkloadDefinitions.byId("copy"));
        var result = fixture.result(); var mapper = BenchmarkWriteReturnPhaseEvidenceTest.nominalClock(result);
        var point = mapper.sourcePoint(100);
        assertThat(point).isEqualTo(new BenchmarkCausalClock.Interval(100, 101));
        BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> assertThat(mapper.evidence()).containsEntry(flag, false));
        var owner = result.samples().getFirst().identity(); long recorded = result.calls().getFirst().observedNanos();
        assertThat(mapper.map(owner, recorded)).isEqualTo(new BenchmarkCausalClock.Interval(recorded, recorded + 1));
        assertThatThrownBy(() -> mapper.map(new BenchmarkCausalClock.Identity(owner.pid() + 1, owner.jvmStartTimeMillis()), recorded))
                .isInstanceOf(AssertionError.class);
        assertThat(BenchmarkReturnLocalSourceWork.bounds(ORIGINAL).get(BenchmarkReturnWorkBudget.Activity.SOURCE_MAP).requests()).isEqualTo(2);
    }

    @Test void nominal_overflow_and_a_real_decoder_refusal_preserve_the_primary_instead_of_fabricating_success() {
        assertThatThrownBy(() -> BenchmarkNominalCounterEnclosures.point(Long.MAX_VALUE))
                .isInstanceOf(AssertionError.class).hasCauseInstanceOf(ArithmeticException.class);
        var bounds = BenchmarkReturnLocalSourceWork.bounds(ORIGINAL);
        try (var scope = scope(bounds)) {
            var original = new AssertionError[1];
            assertThatThrownBy(() -> BenchmarkReturnWorkBudget.operation(scope, BenchmarkReturnWorkBudget.Activity.DECODE, () -> {
                try { return BenchmarkWriteReturnLedger.decode(new byte[32]); }
                catch (AssertionError primary) { original[0] = primary; throw primary; }
            })).isSameAs(original[0]);
            assertThat(scope.primaryCause()).isSameAs(original[0]);
            assertThat(activity(scope.snapshot(), "DECODE")).containsEntry("failedAttempts", 1L).containsEntry("issuedRequests", 0L);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN");
        }
    }

    @Test void unsupported_or_missing_recipe_and_changed_half_cohort_refuse_source_credit() {
        assertThatThrownBy(() -> BenchmarkReturnLocalSourceWork.bounds(ORIGINAL, null)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("SOURCE_GAP_ORIGINAL_EXPECTED_CHANGE_AND_KEY_EXTRACTOR_RECIPE");
        assertThatThrownBy(() -> new BenchmarkReturnLocalSourceWork.Capacity(96_000, 24_000, 960)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> new BenchmarkReturnLocalSourceWork.Capacity(262_145, 131_072, 960)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> new BenchmarkReturnLocalSourceWork.Capacity(96_000, Long.MAX_VALUE, 960)).isInstanceOf(ArithmeticException.class);
    }

    @Test void prior_batch_quotient_refuses_a_bounded_text_roster_before_parser_or_layout() {
        var workload = BenchmarkWorkloadDefinitions.byId("copy"); var original = workload.phase("cdc-update");
        var altered = new BenchmarkWorkloadDefinitions.Phase(original.id(), original.stage(), true,
                original.expectedLogicalOutputChanges(), java.util.Collections.nCopies(961, "SELECT 1"), 1,
                original.batchInterval(), original.expectedLogicalCoverage(), original.targets());
        try (var ignored = scope(BenchmarkReturnLocalSourceWork.bounds(ORIGINAL))) {
            assertThatThrownBy(() -> BenchmarkReturnLocalSourceWork.guardMeasuredAssociationSql(altered))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("prior shape");
        }
        assertThat(BenchmarkReturnLocalSourceWork.COLD_TUPLES_PER_STATEMENT)
                .isEqualTo(BenchmarkReturnLocalSourceWork.MEASURED_SQL_CHARS + 1);
    }

    private static BenchmarkReturnWorkBudget.Scope scope(Map<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> bounds) {
        var excluded = EnumSet.allOf(BenchmarkReturnWorkBudget.Activity.class); excluded.removeAll(bounds.keySet());
        return BenchmarkReturnWorkBudget.activate(new BenchmarkReturnWorkBudget.Definition("local-control", "a".repeat(64),
                Map.of(), bounds, excluded), "local-control");
    }
    private static byte[] idlePage() {
        try {
            var bytes = new ByteArrayOutputStream(); var output = new DataOutputStream(bytes);
            output.writeInt(0x57525031); output.writeInt(3); output.writeLong(0);
            output.writeInt(0); output.writeInt(0); output.writeInt(0);
            output.writeShort(0); byte[] state = "IDLE".getBytes(StandardCharsets.UTF_8);
            output.writeShort(state.length); output.write(state); output.flush(); return bytes.toByteArray();
        } catch (java.io.IOException impossible) { throw new AssertionError(impossible); }
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> activity(Map<String, Object> receipt, String name) {
        return (Map<String, Object>) ((Map<?, ?>) receipt.get("activities")).get(name);
    }
}
