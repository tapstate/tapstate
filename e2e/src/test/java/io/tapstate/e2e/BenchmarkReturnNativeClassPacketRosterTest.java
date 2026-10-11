package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import static io.tapstate.e2e.BenchmarkReturnWorkBudgetRun.NativeMetadataCut.AFTER;
import static io.tapstate.e2e.BenchmarkReturnWorkBudgetRun.NativeMetadataCut.BEFORE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pure original-roster controls; synthetic owners are not original benchmark runtime receipts. */
class BenchmarkReturnNativeClassPacketRosterTest {
    private static final BenchmarkCausalClock.Identity OWNER = new BenchmarkCausalClock.Identity(17, 1000);
    private static Map<String, BenchmarkWorkloadDefinitions.Workload> originals = Map.of();

    @BeforeAll static void acquire_original_component_fixtures_once_outside_a_work_scope() {
        assertThat(BenchmarkReturnWorkBudget.current()).isNull();
        originals = Map.of("copy", BenchmarkWorkloadDefinitions.steadyPilot("copy"),
                "stateless", BenchmarkWorkloadDefinitions.steadyPilot("stateless"),
                "stateful", BenchmarkWorkloadDefinitions.steadyPilot("stateful"));
    }
    @AfterAll static void release_original_component_fixture_references() { originals = Map.of(); }

    @Test void all_three_original_workloads_derive_exact_measured_pairs_and_ignore_nonmeasured_phases() {
        for (String id : List.of("copy", "stateless", "stateful")) {
            var roster = roster(id);
            var expected = id.equals("stateful") ? List.of("stateful/cold-read", "stateful/cdc-update")
                    : List.of(id + "/cdc-update");
            assertThat(roster.phaseKeys()).containsExactlyElementsOf(expected);
            assertThat(roster.expectedPackets()).isEqualTo(id.equals("stateful") ? 4 : 2);
            for (String key : expected) { roster.record(key, BEFORE, OWNER); roster.record(key, AFTER, OWNER); }
            roster.requireComplete();
            assertThat(roster.recordedPackets()).isEqualTo(roster.expectedPackets());
            assertThatThrownBy(() -> roster.phaseKeys().clear()).isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test void one_complete_stateful_pair_cannot_stand_in_for_the_missing_second_phase() {
        var roster = roster("stateful");
        roster.record("stateful/cold-read", BEFORE, OWNER);
        roster.record("stateful/cold-read", AFTER, OWNER);
        assertThatThrownBy(roster::requireComplete).isInstanceOf(AssertionError.class).hasMessageContaining("incomplete");
        assertThat(roster.recordedPackets()).isEqualTo(2);
    }

    @Test void duplicate_before_does_not_advance_or_replace_the_required_after() {
        var roster = roster("copy"); roster.record("copy/cdc-update", BEFORE, OWNER);
        assertThatThrownBy(() -> roster.record("copy/cdc-update", BEFORE, OWNER))
                .isInstanceOf(AssertionError.class).hasMessageContaining("phase or cut differs");
        assertThat(roster.recordedPackets()).isEqualTo(1);
        roster.record("copy/cdc-update", AFTER, OWNER); roster.requireComplete();
    }

    @Test void repeating_the_first_phase_pair_cannot_supply_four_stateful_receipts() {
        var roster = roster("stateful"); roster.record("stateful/cold-read", BEFORE, OWNER);
        roster.record("stateful/cold-read", AFTER, OWNER);
        assertThatThrownBy(() -> roster.record("stateful/cold-read", BEFORE, OWNER))
                .isInstanceOf(AssertionError.class).hasMessageContaining("original measured roster");
        assertThatThrownBy(() -> roster.record("stateful/cold-read", AFTER, OWNER))
                .isInstanceOf(AssertionError.class).hasMessageContaining("original measured roster");
        assertThat(roster.recordedPackets()).isEqualTo(2);
        roster.record("stateful/cdc-update", BEFORE, OWNER);
        assertThatThrownBy(roster::requireComplete).isInstanceOf(AssertionError.class).hasMessageContaining("incomplete");
        roster.record("stateful/cdc-update", AFTER, OWNER); roster.requireComplete();
    }

    @Test void pid_and_start_changes_refuse_without_poisoning_the_previous_bound_owner() {
        var roster = roster("stateful"); roster.record("stateful/cold-read", BEFORE, OWNER);
        for (var changed : List.of(new BenchmarkCausalClock.Identity(18, 1000), new BenchmarkCausalClock.Identity(17, 1001))) {
            assertThatThrownBy(() -> roster.record("stateful/cold-read", AFTER, changed))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("owner changed");
        }
        assertThat(roster.recordedPackets()).isEqualTo(1);
        roster.record("stateful/cold-read", AFTER, OWNER);
    }

    @Test void after_without_before_wrong_phase_and_missing_material_refuse_before_commit() {
        var roster = roster("stateful");
        assertThatThrownBy(() -> roster.record("stateful/cold-read", AFTER, OWNER)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> roster.record("stateful/cdc-update", BEFORE, OWNER)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> roster.record("stateful/cold-read", null, OWNER)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> roster.record("stateful/cold-read", BEFORE, null)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> roster.record("x".repeat(129), BEFORE, OWNER)).isInstanceOf(AssertionError.class);
        assertThat(roster.recordedPackets()).isZero();
        roster.record("stateful/cold-read", BEFORE, OWNER);
    }

    @Test void extra_packets_refuse_after_exact_completion() {
        var roster = roster("stateless"); roster.record("stateless/cdc-update", BEFORE, OWNER);
        roster.record("stateless/cdc-update", AFTER, OWNER); roster.requireComplete();
        assertThatThrownBy(() -> roster.record("stateless/cdc-update", AFTER, OWNER))
                .isInstanceOf(AssertionError.class).hasMessageContaining("before/after bound");
        assertThat(roster.recordedPackets()).isEqualTo(2);
    }

    @Test void requireNext_is_noncommitting_and_record_is_the_only_slot_commit() {
        var roster = roster("stateful");
        roster.requireNext("stateful/cold-read", BEFORE, OWNER);
        assertThat(roster.recordedPackets()).isZero();
        assertThatThrownBy(roster::requireComplete).isInstanceOf(AssertionError.class);
        roster.requireNext("stateful/cold-read", BEFORE, OWNER);
        roster.record("stateful/cold-read", BEFORE, OWNER);
        assertThat(roster.recordedPackets()).isEqualTo(1);
    }

    @Test void constructor_roster_has_prior_bounds_and_never_silently_deduplicates() {
        var original = originals.get("copy");
        var measured = original.phases().stream().filter(BenchmarkWorkloadDefinitions.Phase::measured).findFirst().orElseThrow();
        var duplicate = new ArrayList<>(original.phases()); duplicate.add(measured);
        assertThatThrownBy(() -> new BenchmarkReturnWorkBudgetRun.NativeClassPacketRoster(withPhases(original, duplicate)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("duplicated");
        assertThatThrownBy(() -> new BenchmarkReturnWorkBudgetRun.NativeClassPacketRoster(withPhases(original, Collections.nCopies(7, measured))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("prior cap");
        assertThatThrownBy(() -> new BenchmarkReturnWorkBudgetRun.NativeClassPacketRoster(withPhases(original, List.of())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("missing");
    }

    @Test void twelve_prior_slots_cover_the_material_roster_and_all_source_arithmetic_is_checked() {
        var original = originals.get("copy");
        var sample = original.phases().stream().filter(BenchmarkWorkloadDefinitions.Phase::measured).findFirst().orElseThrow();
        var phases = new ArrayList<BenchmarkWorkloadDefinitions.Phase>();
        for (int i = 0; i < 6; i++) phases.add(new BenchmarkWorkloadDefinitions.Phase("controlled-" + i, sample.stage(), true,
                sample.expectedLogicalOutputChanges(), sample.sql(), sample.statementsPerBatch(), sample.batchInterval(),
                sample.expectedLogicalCoverage(), sample.targets()));
        var roster = new BenchmarkReturnWorkBudgetRun.NativeClassPacketRoster(withPhases(original, phases));
        for (String key : roster.phaseKeys()) { roster.record(key, BEFORE, OWNER); roster.record(key, AFTER, OWNER); }
        roster.requireComplete(); assertThat(roster.recordedPackets()).isEqualTo(12);
        var perPacket = BenchmarkReturnWorkBudgetRun.nativeClassPacketBindingUpper();
        assertThat(perPacket.requests()).isGreaterThan(12L * 6);
        assertThat(BenchmarkReturnWorkBudgetRun.nativeClassPacketRosterFactoryUpper().elements()).isGreaterThan(12);
        assertThatThrownBy(() -> perPacket.times(Long.MAX_VALUE)).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetRun.nativeClassPacketAfterExitUpper().times(Long.MAX_VALUE))
                .isInstanceOf(ArithmeticException.class);
    }

    private static BenchmarkReturnWorkBudgetRun.NativeClassPacketRoster roster(String id) {
        return new BenchmarkReturnWorkBudgetRun.NativeClassPacketRoster(originals.get(id));
    }
    private static BenchmarkWorkloadDefinitions.Workload withPhases(BenchmarkWorkloadDefinitions.Workload original,
                                                                 List<BenchmarkWorkloadDefinitions.Phase> phases) {
        return new BenchmarkWorkloadDefinitions.Workload(original.id(), original.seed(), original.rows(), original.database(),
                original.pipelineIds(), original.sourceChains(), original.setupSql(), phases);
    }
}
