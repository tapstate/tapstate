package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServerConnection;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkWriteReturnReaderTest {
    private static final BenchmarkCausalClock.Identity OWNER = new BenchmarkCausalClock.Identity(17, 1_000);
    private static final String RETURN_FLAG = "-Dtapstate.benchmark.write-return=";
    private static final String COST_FLAG = "-Dtapstate.benchmark.write-return-cost-stages=";
    private static final String NATIVE_FLAG = "-Dtapstate.benchmark.native-clock-library=";

    @Test void native_metadata_uses_one_cold_getter_and_actual_root_and_owned_runtime_identities() {
        var fixture = nativeFixture();
        var evidence = fixture.reader().nativeClockEvidence(BenchmarkNativeClockEvidenceTest.LIBRARY, rootMetadata());
        assertThat(evidence).containsEntry("state", "NATIVE_RECORDED").containsEntry("raw", fixture.clockResponse)
                .containsEntry("matchedRuntimeFlag", RETURN_FLAG + "true")
                .containsEntry("matchedNativeClockFlag", NATIVE_FLAG + BenchmarkNativeClockEvidenceTest.LIBRARY);
        BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> assertThat(evidence.get(flag)).isEqualTo(false));
        assertThat(evidence.get("clockMetadataRead")).isEqualTo(Map.of("startedAtNanos", 103L, "completedAtNanos", 104L));
        assertThat(fixture.operations).containsExactly("runtime", "clockMetadata", "runtime");
        assertThat(io.tapstate.core.common.JsonWriter.write(evidence)).doesNotContain("controlled_runtime_secret", "InputArguments");
    }

    @Test void native_metadata_refusals_retain_bounded_child_and_root_facts_without_repeated_getters() {
        var unknown = BenchmarkNativeClockEvidenceTest.metadataMap(OWNER);
        unknown.put("state", "UNKNOWN"); unknown.put("reason", "NATIVE_READ_UNAVAILABLE");
        unknown.put("failureType", "java.lang.IllegalStateException"); unknown.put("activeProvider", "SYSTEM_NANO_TIME");
        unknown.put("mixedDomainPossible", true);
        for (String response : List.of("{}", "x".repeat(8193),
                BenchmarkNativeClockEvidenceTest.metadata(new BenchmarkCausalClock.Identity(18, 1_000)),
                io.tapstate.core.common.JsonWriter.write(unknown))) {
            var fixture = nativeFixture(); fixture.clockResponse = response;
            var retained = nativeRefusal(fixture, rootMetadata()).retainedEvidence();
            assertThat(retained).containsEntry("state", "UNKNOWN").containsKey("rootMetadata");
            BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> assertThat(retained.get(flag)).isEqualTo(false));
            if (response.length() <= 8192) { assertThat(retained).containsEntry("raw", response); }
            else { assertThat(retained).doesNotContainKey("raw").containsEntry("rawRetained", false); }
            assertThat(fixture.operations).containsExactly("runtime", "clockMetadata", "runtime");
        }
        for (String root : List.of(BenchmarkNativeClockEvidenceTest.metadata(OWNER),
                rootMetadata().replace("\"timebaseOffsetUnsigned\":\"17\"", "\"timebaseOffsetUnsigned\":\"18\""))) {
            var fixture = nativeFixture();
            assertThat(nativeRefusal(fixture, root).retainedEvidence()).containsEntry("raw", fixture.clockResponse);
            assertThat(fixture.operations).containsExactly("runtime", "clockMetadata", "runtime");
        }
    }

    @Test void native_metadata_requires_unique_exact_flags_and_stable_live_runtime_arguments() {
        String library = NATIVE_FLAG + BenchmarkNativeClockEvidenceTest.LIBRARY;
        for (String[] args : List.of(new String[]{RETURN_FLAG + "true"}, new String[]{RETURN_FLAG + "false", library},
                new String[]{RETURN_FLAG + "true", library, library}, new String[]{RETURN_FLAG + "true", NATIVE_FLAG + "/other/library"})) {
            var fixture = nativeFixture(); fixture.before = runtimeAttributes(17L, 1_000L, args);
            nativeRefusal(fixture, rootMetadata()); assertThat(fixture.operations).containsExactly("runtime");
        }
        var changed = nativeFixture(); changed.after = runtimeAttributes(17L, 1_000L, new String[]{RETURN_FLAG + "true", library});
        assertThat(nativeRefusal(changed, rootMetadata()).retainedEvidence()).containsEntry("raw", changed.clockResponse);
        var reused = nativeFixture(); reused.after = runtimeAttributes(17L, 1_001L, nativeArguments());
        assertThat(nativeRefusal(reused, rootMetadata()).retainedEvidence()).containsEntry("raw", reused.clockResponse);
        var dead = nativeFixture(); dead.alive = () -> false;
        nativeRefusal(dead, rootMetadata()); assertThat(dead.operations).isEmpty();
    }

    @Test void native_metadata_io_never_becomes_fallback_absence_or_a_second_read() {
        for (String operation : List.of("runtime", "clockMetadata")) {
            var fixture = nativeFixture(); fixture.failingOperation = operation;
            assertThat(nativeRefusal(fixture, rootMetadata())).hasCause(fixture.readFailure);
            assertThat(fixture.operations.stream().filter("clockMetadata"::equals).count()).isLessThanOrEqualTo(1);
        }
    }

    @Test void cost_stages_use_one_optional_getter_between_owned_runtime_reads_without_probe_clock_or_controls() {
        var fixture = costFixture();
        var evidence = fixture.reader().costStages(4, "measured", 2);
        assertThat(evidence).containsEntry("state", "RECORDED").containsEntry("raw", fixture.costResponse)
                .containsEntry("matchedRuntimeFlag", RETURN_FLAG + "true").containsEntry("matchedCostStagesFlag", COST_FLAG + "true")
                .containsEntry("performanceAcceptanceEligible", false).containsEntry("samplingCostQualified", false)
                .containsEntry("costAcceptanceEligible", false).containsEntry("formalPerformance", false)
                .containsEntry("causalOverheadQualified", false);
        assertThat(evidence.get("costStagesRead")).isEqualTo(Map.of("startedAtNanos", 103L, "completedAtNanos", 104L));
        assertThat(fixture.operations).containsExactly("runtime", "cost", "runtime");
        assertThat(io.tapstate.core.common.JsonWriter.write(evidence)).doesNotContain("controlled_runtime_secret", "InputArguments");
        assertThatThrownBy(() -> evidence.put("state", "changed")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void malformed_oversized_and_foreign_cost_reads_carry_bounded_refusal_facts_without_retry() {
        for (Object response : List.of("{}", "x".repeat(8193), 42L,
                BenchmarkWriteReturnCostStagesTest.raw().replace("\"pid\":17", "\"pid\":18"))) {
            var fixture = costFixture(); fixture.costResponse = response;
            var failure = costRefusal(fixture);
            var retained = failure.retainedEvidence();
            assertThat(retained).containsEntry("state", "UNKNOWN").containsEntry("performanceAcceptanceEligible", false)
                    .containsEntry("samplingCostQualified", false).containsEntry("causalOverheadQualified", false);
            assertThat(retained).containsKeys("beforeRuntimeRead", "costStagesRead", "afterRuntimeRead");
            if (response instanceof String text && text.length() <= 8192) { assertThat(retained).containsEntry("raw", text); }
            else { assertThat(retained).doesNotContainKey("raw"); }
            assertThat(fixture.operations).containsExactly("runtime", "cost", "runtime");
            assertThatThrownBy(() -> retained.clear()).isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test void cost_runtime_io_exit_reuse_or_argument_changes_preserve_obtained_raw_and_identity_facts() {
        var foreign = costFixture();
        foreign.after = runtimeAttributes(18L, 1_000L, costArguments());
        var refused = costRefusal(foreign).retainedEvidence();
        assertThat(refused).containsEntry("raw", foreign.costResponse);
        assertThat(((Map<?, ?>) refused.get("afterRuntimeRead")).get("actualPid")).isEqualTo(18L);
        var changed = costFixture(); changed.after = runtimeAttributes(17L, 1_000L,
                new String[]{RETURN_FLAG + "true", COST_FLAG + "true", "-Xmx64m"});
        assertThat(costRefusal(changed).retainedEvidence()).containsEntry("raw", changed.costResponse);
        for (String operation : List.of("runtime", "cost")) {
            var fixture = costFixture(); fixture.failingOperation = operation;
            assertThat(costRefusal(fixture)).hasCause(fixture.readFailure);
            assertThat(fixture.operations.stream().filter("cost"::equals).count()).isLessThanOrEqualTo(1);
        }
        var dead = costFixture(); dead.alive = () -> false;
        assertThat(costRefusal(dead).retainedEvidence()).containsEntry("rawAvailable", false);
        assertThat(dead.operations).isEmpty();
        var exits = costFixture(); var checks = new AtomicLong(); exits.alive = () -> checks.incrementAndGet() <= 3;
        assertThat(costRefusal(exits).retainedEvidence()).containsEntry("raw", exits.costResponse);
    }

    @Test void cost_getter_requires_unique_explicit_true_flags_for_both_independent_properties() {
        for (String[] arguments : List.of(new String[]{RETURN_FLAG + "true"},
                new String[]{RETURN_FLAG + "false", COST_FLAG + "true"},
                new String[]{RETURN_FLAG + "true", COST_FLAG + "false"},
                new String[]{RETURN_FLAG + "true", COST_FLAG + "true", COST_FLAG + "true"},
                new String[]{RETURN_FLAG + "true", RETURN_FLAG + "true", COST_FLAG + "true"})) {
            var fixture = costFixture(); fixture.before = runtimeAttributes(17L, 1_000L, arguments);
            assertThat(costRefusal(fixture).retainedEvidence()).containsEntry("rawAvailable", false);
            assertThat(fixture.operations).containsExactly("runtime");
        }
    }

    @Test void unknown_cost_stage_state_is_retained_without_transport_turning_it_into_recorded() {
        var fixture = costFixture(); fixture.costResponse = BenchmarkWriteReturnCostStagesTest.raw()
                .replace("\"state\":\"RECORDED\"", "\"state\":\"UNKNOWN\"")
                .replace("\"reason\":\"NONE\"", "\"reason\":\"CLOCK_UNAVAILABLE\"")
                .replace("\"completeTimedCalls\":2", "\"completeTimedCalls\":0")
                .replace("\"count\":2", "\"count\":0").replace("\"sumNanos\":30", "\"sumNanos\":0")
                .replace("\"maxNanos\":20", "\"maxNanos\":0");
        assertThat(fixture.reader().costStages(4, "measured", 2)).containsEntry("state", "UNKNOWN")
                .containsEntry("reason", "CLOCK_UNAVAILABLE").containsEntry("completeTimedCalls", 0L)
                .containsEntry("raw", fixture.costResponse).containsEntry("costAcceptanceEligible", false);
    }

    @Test void registration_proofs_use_only_owned_runtime_attributes_and_actual_bean_presence() {
        for (boolean enabled : new boolean[]{true, false}) {
            var fixture = new RegistrationFixture(enabled);
            var evidence = fixture.reader().registrationEvidence(enabled);
            assertThat(evidence).containsEntry("state", enabled ? "RECORDED_ENABLED" : "RECORDED_DISABLED")
                    .containsEntry("pid", 17L).containsEntry("jvmStartTimeMillis", 1_000L)
                    .containsEntry("expectedEnabled", enabled).containsEntry("actualRegistered", enabled)
                    .containsEntry("matchedRuntimeFlag", RETURN_FLAG + enabled).containsEntry("argumentCount", 2)
                    .containsEntry("performanceAcceptanceEligible", false).containsEntry("formalPerformance", false)
                    .containsEntry("costAcceptanceEligible", false).containsEntry("samplingCostQualified", false)
                    .containsEntry("returnCaptureDelayQualified", false).containsEntry("phaseAttributionQualified", false);
            assertThat(evidence.get("beforeRuntimeRead")).isEqualTo(Map.of("startedAtNanos", 101L, "completedAtNanos", 102L));
            assertThat(evidence.get("registrationRead")).isEqualTo(Map.of("startedAtNanos", 102L, "completedAtNanos", 103L));
            assertThat(evidence.get("afterRuntimeRead")).isEqualTo(Map.of("startedAtNanos", 103L, "completedAtNanos", 104L));
            assertThat(fixture.operations).containsExactly("runtime", "registered", "runtime");
            assertThat(io.tapstate.core.common.JsonWriter.write(evidence)).doesNotContain("controlled_runtime_secret", "InputArguments");
            assertThatThrownBy(() -> evidence.put("state", "changed")).isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test void registration_checks_refuse_pid_reuse_missing_attributes_and_changed_argument_rosters() {
        for (var attributes : List.of(runtimeAttributes(18L, 1_000L, new String[]{RETURN_FLAG + "true"}),
                runtimeAttributes(17L, 1_001L, new String[]{RETURN_FLAG + "true"}), new AttributeList())) {
            var fixture = new RegistrationFixture(true); fixture.after = attributes;
            assertThatThrownBy(() -> fixture.reader().registrationEvidence(true)).isInstanceOf(AssertionError.class);
        }
        var duplicate = runtimeAttributes(17L, 1_000L, new String[]{RETURN_FLAG + "true"});
        duplicate.add(new Attribute("Pid", 17L));
        var malformed = new RegistrationFixture(true); malformed.before = duplicate;
        assertThatThrownBy(() -> malformed.reader().registrationEvidence(true))
                .isInstanceOf(AssertionError.class).hasMessageContaining("duplicate");
        var changed = new RegistrationFixture(true);
        changed.after = runtimeAttributes(17L, 1_000L, new String[]{RETURN_FLAG + "true", "-Xmx64m"});
        assertThatThrownBy(() -> changed.reader().registrationEvidence(true))
                .isInstanceOf(AssertionError.class).hasMessageContaining("runtime arguments changed");
    }

    @Test void missing_duplicate_conflicting_or_implicit_runtime_flags_cannot_prove_registration() {
        for (boolean enabled : new boolean[]{true, false}) {
            String expected = RETURN_FLAG + enabled;
            for (String[] arguments : List.of(new String[0], new String[]{expected, expected},
                    new String[]{expected, RETURN_FLAG + !enabled}, new String[]{RETURN_FLAG + !enabled},
                    new String[]{"-Dtapstate.benchmark.write-return"}, new String[]{RETURN_FLAG + "TRUE"})) {
                var fixture = new RegistrationFixture(enabled);
                fixture.before = runtimeAttributes(17L, 1_000L, arguments);
                assertThatThrownBy(() -> fixture.reader().registrationEvidence(enabled))
                        .isInstanceOf(AssertionError.class).hasMessageContaining("runtime flag");
                assertThat(fixture.operations).containsExactly("runtime");
            }
        }
    }

    @Test void runtime_argument_shape_and_cold_profile_bounds_are_enforced_before_registration_reads() {
        String[] tooMany = new String[129]; java.util.Arrays.fill(tooMany, "-Xmx64m"); tooMany[0] = RETURN_FLAG + "true";
        String[] tooLarge = new String[33]; java.util.Arrays.fill(tooLarge, "x".repeat(16_384)); tooLarge[0] = RETURN_FLAG + "true";
        for (Object arguments : List.of(tooMany, tooLarge, new String[]{RETURN_FLAG + "true", "x".repeat(16_385)},
                new String[]{RETURN_FLAG + "true", null}, List.of(RETURN_FLAG + "true"))) {
            var fixture = new RegistrationFixture(true); fixture.before = runtimeAttributes(17L, 1_000L, arguments);
            assertThatThrownBy(() -> fixture.reader().registrationEvidence(true))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("argument");
            assertThat(fixture.operations).containsExactly("runtime");
        }
    }

    @Test void wrong_actual_bean_presence_is_refused_for_both_explicit_modes() {
        for (boolean enabled : new boolean[]{true, false}) {
            var fixture = new RegistrationFixture(enabled); fixture.registered = !enabled;
            assertThatThrownBy(() -> fixture.reader().registrationEvidence(enabled))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("registration does not match");
            assertThat(fixture.operations).containsExactly("runtime", "registered", "runtime");
        }
    }

    @Test void unavailable_runtime_or_registration_reads_never_become_disabled_evidence() {
        for (String operation : List.of("runtime", "registered")) {
            var fixture = new RegistrationFixture(false); fixture.failingOperation = operation;
            assertThatThrownBy(() -> fixture.reader().registrationEvidence(false))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("registration is unavailable")
                    .hasCause(fixture.readFailure);
        }
        var dead = new RegistrationFixture(false); dead.alive = () -> false;
        assertThatThrownBy(() -> dead.reader().registrationEvidence(false))
                .isInstanceOf(AssertionError.class).hasMessageContaining("exited");
        assertThat(dead.operations).isEmpty();
        for (long lastAliveCheck : List.of(3L, 5L)) {
            var exits = new RegistrationFixture(false); var checks = new AtomicLong();
            exits.alive = () -> checks.incrementAndGet() <= lastAliveCheck;
            assertThatThrownBy(() -> exits.reader().registrationEvidence(false))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("exited");
        }
    }

    @Test void the_getter_point_is_read_during_its_actual_root_request() {
        AtomicLong root = new AtomicLong(100);
        var reader = reader(attributes(17L, 1_000L, -50L), true, () -> true, root);
        var point = reader.clockSample(9);
        assertThat(point).isEqualTo(new BenchmarkCausalClock.Sample(9, OWNER, 101, 102, -50));
    }

    @Test void missing_duplicate_and_non_long_attributes_do_not_create_clock_proof() {
        var duplicate = attributes(17L, 1_000L, -50L);
        duplicate.add(new Attribute("Pid", 17L));
        var duplicateNull = new AttributeList();
        duplicateNull.add(new Attribute("Pid", null)); duplicateNull.addAll(attributes(17L, 1_000L, -50L));
        for (var values : List.of(new AttributeList(), duplicate, duplicateNull,
                attributes(17, 1_000L, -50L))) {
            var reader = reader(values, true, () -> true, new AtomicLong());
            assertThatThrownBy(() -> reader.clockSample(0)).isInstanceOf(AssertionError.class);
        }
    }

    @Test void pid_reuse_and_foreign_processes_are_rejected() {
        for (var values : List.of(attributes(18L, 1_000L, -50L), attributes(17L, 1_001L, -50L))) {
            assertThatThrownBy(() -> reader(values, true, () -> true, new AtomicLong()).clockSample(0))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("owned runtime identity");
        }
    }

    @Test void an_exit_before_or_during_the_read_is_not_a_clock_point() {
        var dead = reader(attributes(17L, 1_000L, -50L), true, () -> false, new AtomicLong());
        assertThatThrownBy(() -> dead.clockSample(0)).isInstanceOf(AssertionError.class).hasMessageContaining("exited");
        AtomicLong checks = new AtomicLong();
        var exits = reader(attributes(17L, 1_000L, -50L), true,
                () -> checks.incrementAndGet() == 1, new AtomicLong());
        assertThatThrownBy(() -> exits.clockSample(0)).isInstanceOf(AssertionError.class).hasMessageContaining("exited");
    }

    @Test void controls_preserve_a_typed_refusal_and_require_typed_results() {
        var refusal = reader(attributes(17L, 1_000L, -50L), false, () -> true, new AtomicLong());
        assertThat(refusal.start("measured")).isFalse(); assertThat(refusal.stop()).isFalse();
        assertThatThrownBy(() -> reader(attributes(17L, 1_000L, -50L), "true", () -> true, new AtomicLong()).stop())
                .isInstanceOf(AssertionError.class).hasMessageContaining("typed result");
    }

    @Test void receipt_pages_must_be_binary_and_within_the_frozen_frame_bound() {
        var accepted = reader(attributes(17L, 1_000L, -50L), new byte[65_536], () -> true, new AtomicLong());
        assertThat(accepted.page(0)).hasSize(65_536);
        for (Object bad : List.of("bytes", new byte[0], new byte[65_537])) {
            assertThatThrownBy(() -> reader(attributes(17L, 1_000L, -50L), bad, () -> true, new AtomicLong()).page(0))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("page");
        }
    }

    @Test void terminal_totals_are_actual_typed_reads_with_the_same_owned_identity() {
        var values = summary(3L, 0L, 6L, 0L, 826L);
        var result = summaryReader(values).summary();
        assertThat(result).isEqualTo(new BenchmarkWriteReturnReader.Summary("measured",
                "RECORDED_SCOPE_UNQUALIFIED", 3, 0, 6, 0, 826));
    }

    @Test void terminal_summary_cannot_hide_missing_counters_foreign_owner_or_overflow() {
        var missing = summary(3L, 0L, 6L, 0L, 826L); missing.remove(0);
        var foreign = summary(3L, 0L, 6L, 0L, 826L); foreign.set(0, new Attribute("Pid", 18L));
        var duplicate = summary(3L, 0L, 6L, 0L, 826L); duplicate.add(new Attribute("State", "IDLE"));
        for (var values : List.of(missing, foreign, duplicate, summary(3L, 4L, 6L, 0L, 826L),
                summary(3L, 0L, -1L, 0L, 826L), summary(3L, 0L, 6L, 0L, 2_097_153L),
                summary(3, 0L, 6L, 0L, 826L))) {
            assertThatThrownBy(() -> summaryReader(values).summary()).isInstanceOf(AssertionError.class);
        }
    }

    private static AttributeList summary(Object calls, Object failed, Object records, Object open, Object bytes) {
        var values = new AttributeList();
        values.add(new Attribute("Pid", 17L)); values.add(new Attribute("JvmStartTimeMillis", 1_000L));
        values.add(new Attribute("Window", "measured")); values.add(new Attribute("State", "RECORDED_SCOPE_UNQUALIFIED"));
        values.add(new Attribute("CompletedCalls", calls)); values.add(new Attribute("FailedCalls", failed));
        values.add(new Attribute("ReportedRecords", records)); values.add(new Attribute("OpenCalls", open));
        values.add(new Attribute("RetainedBytes", bytes)); return values;
    }
    private static BenchmarkWriteReturnReader summaryReader(AttributeList values) {
        var connection = (MBeanServerConnection) Proxy.newProxyInstance(
                BenchmarkWriteReturnReaderTest.class.getClassLoader(), new Class<?>[]{MBeanServerConnection.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("getAttributes")) {
                        return ((String[]) arguments[1]).length == 3 ? attributes(17L, 1_000L, -50L) : values;
                    }
                    throw new AssertionError("unexpected management operation: " + method.getName());
                });
        return new BenchmarkWriteReturnReader(OWNER, connection, () -> true, new AtomicLong()::incrementAndGet);
    }

    private static AttributeList attributes(Object pid, Object start, Object point) {
        var attributes = new AttributeList();
        attributes.add(new Attribute("Pid", pid)); attributes.add(new Attribute("JvmStartTimeMillis", start));
        attributes.add(new Attribute("NanoTime", point)); return attributes;
    }

    private static AttributeList runtimeAttributes(Object pid, Object start, Object arguments) {
        var attributes = new AttributeList();
        attributes.add(new Attribute("Pid", pid)); attributes.add(new Attribute("StartTime", start));
        attributes.add(new Attribute("InputArguments", arguments)); return attributes;
    }

    private static String[] costArguments() {
        return new String[]{RETURN_FLAG + "true", COST_FLAG + "true", "-Dpassword=controlled_runtime_secret"};
    }
    private static String[] nativeArguments() {
        return new String[]{RETURN_FLAG + "true", NATIVE_FLAG + BenchmarkNativeClockEvidenceTest.LIBRARY, "-Dpassword=controlled_runtime_secret"};
    }
    private static RegistrationFixture nativeFixture() {
        var fixture = new RegistrationFixture(true); fixture.before = runtimeAttributes(17L, 1_000L, nativeArguments()); fixture.after = fixture.before;
        fixture.clockResponse = BenchmarkNativeClockEvidenceTest.metadata(OWNER); return fixture;
    }
    private static String rootMetadata() { return BenchmarkNativeClockEvidenceTest.metadata(BenchmarkNativeClockEvidenceTest.rootOwner()); }
    private static BenchmarkWriteReturnReader.NativeClockRefusal nativeRefusal(RegistrationFixture fixture, String root) {
        try { fixture.reader().nativeClockEvidence(BenchmarkNativeClockEvidenceTest.LIBRARY, root); }
        catch (BenchmarkWriteReturnReader.NativeClockRefusal refused) { return refused; }
        throw new AssertionError("controlled native metadata unexpectedly succeeded");
    }
    private static RegistrationFixture costFixture() {
        var fixture = new RegistrationFixture(true);
        fixture.before = runtimeAttributes(17L, 1_000L, costArguments()); fixture.after = fixture.before;
        fixture.costResponse = BenchmarkWriteReturnCostStagesTest.raw(); return fixture;
    }
    private static BenchmarkWriteReturnReader.CostStagesRefusal costRefusal(RegistrationFixture fixture) {
        try { fixture.reader().costStages(4, "measured", 2); }
        catch (BenchmarkWriteReturnReader.CostStagesRefusal refusal) { return refusal; }
        throw new AssertionError("controlled cost read unexpectedly succeeded");
    }

    private static final class RegistrationFixture {
        AttributeList before, after;
        boolean registered;
        BooleanSupplier alive = () -> true;
        final List<String> operations = new ArrayList<>();
        final java.io.IOException readFailure = new java.io.IOException("controlled registration read failure");
        String failingOperation;
        Object costResponse;
        Object clockResponse;
        RegistrationFixture(boolean enabled) {
            before = runtimeAttributes(17L, 1_000L, new String[]{RETURN_FLAG + enabled, "-Dpassword=controlled_runtime_secret"});
            after = before; registered = enabled;
        }
        BenchmarkWriteReturnReader reader() {
            var reads = new AtomicLong();
            var connection = (MBeanServerConnection) Proxy.newProxyInstance(
                    BenchmarkWriteReturnReaderTest.class.getClassLoader(), new Class<?>[]{MBeanServerConnection.class},
                    (proxy, method, arguments) -> {
                        if (method.getName().equals("getAttributes")) {
                            assertThat(arguments[0].toString()).isEqualTo("java.lang:type=Runtime");
                            assertThat((String[]) arguments[1]).containsExactly("Pid", "StartTime", "InputArguments");
                            operations.add("runtime");
                            if ("runtime".equals(failingOperation)) { throw readFailure; }
                            long ordinal = reads.incrementAndGet();
                            assertThat(ordinal).isBetween(1L, 2L);
                            return ordinal == 1 ? before : after;
                        }
                        if (method.getName().equals("isRegistered")) {
                            assertThat(arguments[0].toString()).isEqualTo("io.tapstate.benchmark:type=WriteReturn");
                            operations.add("registered");
                            if ("registered".equals(failingOperation)) { throw readFailure; }
                            return registered;
                        }
                        if (method.getName().equals("getAttribute")) {
                            assertThat(arguments[0].toString()).isEqualTo("io.tapstate.benchmark:type=WriteReturn");
                            if (arguments[1].equals("ClockMetadata")) {
                                operations.add("clockMetadata");
                                if ("clockMetadata".equals(failingOperation)) { throw readFailure; }
                                return clockResponse;
                            }
                            assertThat(arguments[1]).isEqualTo("CostStages");
                            operations.add("cost");
                            if ("cost".equals(failingOperation)) { throw readFailure; }
                            return costResponse;
                        }
                        throw new AssertionError("registration proof called a probe getter or control");
                    });
            return new BenchmarkWriteReturnReader(OWNER, connection, alive, new AtomicLong(100)::incrementAndGet);
        }
    }
    private static BenchmarkWriteReturnReader reader(AttributeList attributes, Object result,
                                                     BooleanSupplier alive, AtomicLong root) {
        var connection = (MBeanServerConnection) Proxy.newProxyInstance(
                BenchmarkWriteReturnReaderTest.class.getClassLoader(), new Class<?>[]{MBeanServerConnection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getAttributes" -> attributes;
                    case "invoke" -> result;
                    default -> throw new AssertionError("unexpected management operation: " + method.getName());
                });
        return new BenchmarkWriteReturnReader(OWNER, connection, alive, root::incrementAndGet);
    }
}
