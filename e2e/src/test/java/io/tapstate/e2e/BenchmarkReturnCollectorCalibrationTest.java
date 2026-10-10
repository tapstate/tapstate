package io.tapstate.e2e;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServerConnection;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnCollectorCalibrationTest {
    private static final String LIBRARY = "/private/tmp/controlled-collector-clock.dylib";
    private static final String PROPERTY = RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY;

    @Test void default_entry_and_child_arguments_do_not_enable_a_calibration_or_native_control() {
        assertThat(RealBenchmarkForkDriver.collectorCalibration(null, "copy", PipelineBenchmarkComparison.Arm.A,
                false, false, BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL, null, false, false, true)).isNull();
        assertThat(RealBenchmarkForkDriverIT.returnArguments(false, false, null))
                .containsExactly("-Dtapstate.benchmark.write-return=true");
        assertThatThrownBy(() -> RealBenchmarkForkDriverIT.returnArguments(true, false, LIBRARY))
                .isInstanceOf(AssertionError.class);
        for (var mode : RealBenchmarkForkDriver.CollectorCalibration.values()) {
            assertThat(RealBenchmarkForkDriverIT.collectorCalibrationArguments(mode, LIBRARY)).containsExactly(
                    "-Dtapstate.benchmark.write-return=" + (mode == RealBenchmarkForkDriver.CollectorCalibration.ON),
                    "-Dtapstate.benchmark.native-clock-library=" + LIBRARY);
        }
    }

    @Test void actual_entry_refuses_foreign_scope_and_other_controls_before_an_artifact_is_used() {
        try (var properties = new Properties()) {
            properties.valid();
            assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(BenchmarkWorkloadDefinitions.steadyPilot("stateless"),
                    PipelineBenchmarkComparison.Arm.A, 1, null)).isInstanceOf(AssertionError.class).hasMessageContaining("collector calibration");
            assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(BenchmarkWorkloadDefinitions.steadyPilot("copy"),
                    PipelineBenchmarkComparison.Arm.B, 1, null)).isInstanceOf(AssertionError.class).hasMessageContaining("collector calibration");
            for (String control : List.of(RealBenchmarkForkDriver.WRITE_RETURN_METHOD_CONTROL_PROPERTY,
                    RealBenchmarkForkDriver.WRITE_RETURN_COST_STAGES_PROPERTY, RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY,
                    "tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics", RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY)) {
                properties.valid(); System.setProperty(control, "true");
                assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(BenchmarkWorkloadDefinitions.steadyPilot("stateless"),
                        PipelineBenchmarkComparison.Arm.B, 1, null)).isInstanceOf(AssertionError.class).hasMessageContaining("collector calibration");
            }
            properties.valid(); System.clearProperty("tapstate.e2e.benchmark-smoke.fork-output");
            assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(BenchmarkWorkloadDefinitions.steadyPilot("stateless"),
                    PipelineBenchmarkComparison.Arm.B, 1, null)).isInstanceOf(AssertionError.class).hasMessageContaining("collector calibration");
            for (String mode : List.of("ON", "OFF")) {
                properties.valid(); System.setProperty(PROPERTY, mode);
                assertThatThrownBy(() -> new RealBenchmarkForkDriver().run(BenchmarkWorkloadDefinitions.steadyPilot("stateless"),
                        PipelineBenchmarkComparison.Arm.B, 1, null)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("application JAR");
            }
        }
    }

    @Test void off_registration_and_root_only_policy_never_invoke_an_owned_probe_getter() {
        var operations = new ArrayList<String>(); var ticks = new AtomicLong(100);
        var connection = (MBeanServerConnection) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{MBeanServerConnection.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("getAttributes")) {
                        operations.add("runtime");
                        return new AttributeList(List.of(new Attribute("Pid", 17L), new Attribute("StartTime", 1000L),
                                new Attribute("InputArguments", new String[]{"-Dtapstate.benchmark.write-return=false",
                                        "-Dtapstate.benchmark.native-clock-library=" + LIBRARY})));
                    }
                    if (method.getName().equals("isRegistered")) { operations.add("registration"); return false; }
                    throw new AssertionError("disabled collector attempted an owned probe getter or control: " + method.getName());
                });
        var reader = new BenchmarkWriteReturnReader(new BenchmarkCausalClock.Identity(17, 1000),
                connection, () -> true, ticks::incrementAndGet);
        assertThat(reader.registrationEvidence(false)).containsEntry("actualRegistered", false)
                .containsEntry("matchedRuntimeFlag", "-Dtapstate.benchmark.write-return=false");
        var receipt = RealBenchmarkForkDriver.collectorOwnedClockEvidence(RealBenchmarkForkDriver.CollectorCalibration.OFF,
                reader, LIBRARY, "unused root metadata");
        assertThat(receipt).containsEntry("state", "UNAVAILABLE");
        BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> assertThat(receipt.get(flag)).isEqualTo(false));
        assertThat(operations).containsExactly("runtime", "registration", "runtime");
        assertThat(RealBenchmarkForkDriver.calibrationReadsOwnedClock(RealBenchmarkForkDriver.CollectorCalibration.ON)).isTrue();
    }

    @Test void actual_root_identity_and_serial_common_cutoff_cannot_be_replaced_by_owned_or_tail_facts() {
        assertThatThrownBy(() -> RealBenchmarkForkDriver.supportedRootClockEvidence(
                BenchmarkNativeClockEvidenceTest.metadata(new BenchmarkCausalClock.Identity(17, 1000)), LIBRARY))
                .isInstanceOf(AssertionError.class).hasMessageContaining("runtime identity");
        var cutoff = RealBenchmarkForkDriver.collectorCutoffOrder(100, 101, 110, 111, 120, 121, 130);
        assertThat(cutoff).containsEntry("fourWriterTableAckAtNanos", 100L);
        BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> assertThat(cutoff.get(flag)).isEqualTo(false));
        for (long[] invalid : List.of(new long[]{100, 99, 110, 111, 120, 121, 130},
                new long[]{100, 101, 110, 109, 120, 121, 130}, new long[]{100, 101, 110, 111, 120, 119, 130})) {
            assertThatThrownBy(() -> RealBenchmarkForkDriver.collectorCutoffOrder(invalid[0], invalid[1], invalid[2],
                    invalid[3], invalid[4], invalid[5], invalid[6])).isInstanceOf(AssertionError.class).hasMessageContaining("precede capture drain");
        }
    }

    @Test void every_explicit_calibration_is_refused_by_formal_entry_before_configuration() {
        try (var properties = new Properties()) {
            for (String mode : List.of("ON", "OFF", "false", "invalid")) {
                System.setProperty(PROPERTY, mode);
                assertThatThrownBy(() -> new PipelineBenchmarkLiveRunIT().interleavedRealForksWriteEvidenceAndEnforceTheSelectedGate())
                        .isInstanceOf(AssertionError.class).hasMessageContaining("collector calibration cannot establish");
            }
        }
    }

    @Test void a_command_tail_failure_keeps_already_acquired_full_resources_and_the_same_primary() {
        var resources = BenchmarkResourceSampler.summarize(List.of(reading(100), reading(177)));
        var anchor = new BenchmarkForkEnvironment.ClockAnchor(java.time.Instant.EPOCH, 100, 101);
        var retained = new LinkedHashMap<String, Object>();
        var recorded = new AtomicReference<Map<String, Object>>();
        var primary = new AssertionError("controlled command-tail refusal");
        var caught = org.assertj.core.api.Assertions.catchThrowable(() -> BenchmarkReturnFailureRetention.run(
                () -> RealBenchmarkForkDriver.finishCollectorCommands(resources, anchor, retained, () -> {
                    assertThat(retained).containsKey("fullPhaseResources");
                    throw primary;
                }), () -> retained, recorded::set));
        assertThat(caught).isSameAs(primary);
        var full = (Map<?, ?>) recorded.get().get("fullPhaseResources");
        assertThat(full.get("cpuNanos")).isEqualTo(77L); assertThat(full.get("sampleCount")).isEqualTo(2);
        assertThat(recorded.get()).doesNotContainKeys("fullPhaseCommands", "commandCheckpoint");
        assertThat(recorded.get().get("failureType")).isEqualTo(AssertionError.class.getName());
    }

    @Test void an_unresolved_checkpoint_retains_only_completed_attempts_and_its_actual_pending_start() {
        var log = new BenchmarkResourceSampler.AttemptLog(16);
        log.append(101, 111, BenchmarkResourceSampler.Outcome.SUCCESS, null, reading(100));
        var primary = new java.util.concurrent.TimeoutException("controlled unresolved checkpoint");
        var failure = new BenchmarkResourceSampler.SamplingFailure(null,
                BenchmarkResourceSampler.FailureStage.CHECKPOINT, BenchmarkResourceSampler.FailureReason.CHECKPOINT_TIMEOUT,
                log.snapshot(), Optional.of(new BenchmarkResourceSampler.PendingRead(2, 112, false, true)), primary);
        var retained = RealBenchmarkForkDriver.collectorSamplingRefusal(failure, null);
        assertThat(failure.getCause()).isSameAs(primary); assertThat(failure.inPhase("cdc-update").getCause()).isSameAs(failure);
        var completed = (Map<?, ?>) retained.get("completedSampling");
        assertThat(completed.get("attemptCount")).isEqualTo(1L);
        var attempts = (List<?>) completed.get("attempts"); assertThat(attempts).hasSize(1);
        var actual = (Map<?, ?>) attempts.getFirst();
        assertThat(actual.get("completedAtNanos")).isEqualTo(111L);
        assertThat(actual.keySet().stream().map(Object::toString).toList()).doesNotContain("startedAtUtcEarliest", "completedAtUtcLatest");
        var pending = (Map<?, ?>) retained.get("actualPendingRead");
        assertThat(pending.get("index")).isEqualTo(2L); assertThat(pending.get("startedAtNanos")).isEqualTo(112L);
        assertThat(pending.get("checkpointRead")).isEqualTo(true); assertThat(pending.get("finalRead")).isEqualTo(false);
        assertThat(pending.keySet().stream().map(Object::toString).toList()).doesNotContain("completedAtNanos", "durationNanos", "reading");
        assertThat(retained).doesNotContainKeys("fullPhaseResources", "commonResources");
        BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> assertThat(retained.get(flag)).isEqualTo(false));
        assertThatThrownBy(() -> completed.clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> actual.clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> pending.clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void a_completed_checkpoint_refusal_keeps_the_failed_attempt_without_inventing_pending_work() {
        var log = new BenchmarkResourceSampler.AttemptLog(16);
        log.append(101, 111, BenchmarkResourceSampler.Outcome.SUCCESS, null, reading(100));
        var primary = new IllegalStateException("controlled checkpoint read refusal");
        log.append(112, 120, BenchmarkResourceSampler.Outcome.ERROR, primary.getClass().getName(), null);
        var failure = new BenchmarkResourceSampler.SamplingFailure(null,
                BenchmarkResourceSampler.FailureStage.CHECKPOINT, BenchmarkResourceSampler.FailureReason.READ_ERROR,
                log.snapshot(), Optional.empty(), primary);
        var retained = RealBenchmarkForkDriver.collectorSamplingRefusal(failure, null);
        var completed = (Map<?, ?>) retained.get("completedSampling");
        assertThat(completed.get("failureCount")).isEqualTo(1L);
        var attempts = (List<?>) completed.get("attempts"); assertThat(attempts).hasSize(2);
        var failed = (Map<?, ?>) attempts.getLast();
        assertThat(failed.get("outcome")).isEqualTo("ERROR"); assertThat(failed.get("completedAtNanos")).isEqualTo(120L);
        assertThat(failed.get("reading")).isEqualTo(Map.of("state", "UNAVAILABLE"));
        assertThat(retained.get("actualPendingRead")).isEqualTo(Map.of("state", "NOT_PRESENT"));
        assertThat(failure.getCause()).isSameAs(primary);
    }

    @Test void a_typed_command_checkpoint_refusal_is_retained_without_replacing_earlier_facts_or_its_cause() {
        var reads = new java.util.concurrent.atomic.AtomicInteger();
        var primary = new IllegalStateException("controlled command checkpoint refusal");
        try (var sampler = BenchmarkMongoCommandSampler.from(() -> {
            if (reads.incrementAndGet() == 2) { throw primary; }
            return new BenchmarkMongoCommandSampler.Snapshot("owned-fixture", 17, 1, Map.of("insert", 1L));
        }, new AtomicLong()::incrementAndGet)) {
            sampler.start();
            var failure = org.assertj.core.api.Assertions.catchThrowableOfType(sampler::checkpoint,
                    BenchmarkMongoCommandSampler.CheckpointFailure.class);
            assertThat(failure.getCause()).isSameAs(primary);
            var retained = new LinkedHashMap<String, Object>();
            retained.put("sourceIssue", Map.of("completeSourceRoster", true));
            retained.put("commonResources", Map.of("sampleCount", 2));
            var recorded = new AtomicReference<Map<String, Object>>();
            var caught = org.assertj.core.api.Assertions.catchThrowable(() -> BenchmarkReturnFailureRetention.run(() -> {
                RealBenchmarkForkDriver.retainCollectorCommandRefusal(retained, failure);
                throw failure;
            }, () -> retained, recorded::set));
            assertThat(caught).isSameAs(failure);
            assertThat(recorded.get()).containsEntry("sourceIssue", Map.of("completeSourceRoster", true))
                    .containsEntry("commonResources", Map.of("sampleCount", 2))
                    .containsEntry("commandCheckpointRefusal", failure.retainedEvidence());
            BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> assertThat(failure.retainedEvidence().get(flag)).isEqualTo(false));
            assertThat(recorded.get()).doesNotContainKeys("commonCommands", "fullPhaseCommands");
            assertThat(reads).hasValue(2);
        }
    }

    private static BenchmarkProcessProbe.Snapshot reading(long cpu) {
        return new BenchmarkProcessProbe.Snapshot(OptionalLong.of(cpu), OptionalLong.of(1000),
                OptionalLong.of(2000), OptionalLong.of(20));
    }

    private static final class Properties implements AutoCloseable {
        private final Map<String, String> previous = new LinkedHashMap<>();
        Properties() {
            for (String key : List.of(PROPERTY, RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY,
                    RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, RealBenchmarkForkDriver.WRITE_RETURN_METHOD_CONTROL_PROPERTY,
                    RealBenchmarkForkDriver.WRITE_RETURN_COST_STAGES_PROPERTY, RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY,
                    RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY, "tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics",
                    "tapstate.e2e.benchmark-smoke.fork-output", "tapstate.e2e.benchmark-smoke.capture-mode")) {
                previous.put(key, System.getProperty(key)); System.clearProperty(key);
            }
        }
        void valid() {
            previous.keySet().forEach(System::clearProperty);
            System.setProperty(PROPERTY, "ON");
            System.setProperty(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY, LIBRARY);
            System.setProperty(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, "true");
            System.setProperty("tapstate.e2e.benchmark-smoke.fork-output", "/private/tmp/collector-calibration-fork.json");
            System.setProperty("tapstate.e2e.benchmark-smoke.capture-mode", "PLAIN");
        }
        public void close() {
            previous.forEach((key, value) -> { if (value == null) { System.clearProperty(key); } else { System.setProperty(key, value); } });
        }
    }
}
