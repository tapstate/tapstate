package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Formal clock qualification is independent of output steadiness and survives report serialization. */
class BenchmarkClockQualificationContractTest {
    @Test void aPilotWithoutMeasuredPhasesCannotVacuouslyPassTimingQualification() {
        var empty = new RealBenchmarkForkDriver.Evidence("clock-empty", BenchmarkWorkloadDefinitions.steadyPilot("copy"),
                PipelineBenchmarkComparison.Arm.B, Path.of("clock-empty.jar"), List.of(),
                new BenchmarkResourceSampler.Summary(70, 3, 1_500, 2_500, 2),
                new BenchmarkMongoCommandSampler.Summary(Map.of(), Map.of(), 1), Map.of(), Map.of(),
                "qualified", 0, Optional.empty());
        assertThatThrownBy(empty::requireSteadyStateWindow)
                .isInstanceOf(AssertionError.class).hasMessageContaining("no measured phases");
    }
    @Test void qualifiedHelloSamplesCannotSupplyAnOperationDateErrorBound() {
        assertThatThrownBy(() -> evidence(phase(proof(), false)).requireSteadyStateWindow())
                .isInstanceOf(AssertionError.class).hasMessageContaining("operation time lacks measured error bounds");
    }

    @Test void aCallerQualificationMarkerCannotReplaceAnActualOperationTimingMethod() {
        var claimed = new java.util.LinkedHashMap<String, Object>(proof());
        claimed.put("operationDateTimeEvidence", Map.of("state", "QUALIFIED", "maximumErrorNanos", 0));
        var input = phase(claimed, false);
        assertThatThrownBy(() -> evidence(input).requireSteadyStateWindow())
                .isInstanceOf(AssertionError.class).hasMessageContaining("operation time lacks measured error bounds");
        var output = PipelineBenchmarkLiveRunIT.phaseEvidence(input);
        assertThat(((Map<?, ?>) output.get("operationDateTimeEvidence")).get("state")).isEqualTo("UNQUALIFIED");
        assertThat(output.get("steadyStateEstablished")).isEqualTo(false);
    }

    @Test void anUnsupportedFormalTimingMethodStopsBeforeConfigurationOrFixtureAccess() {
        String property = "tapstate.e2e.benchmark.gate";
        String previous = System.getProperty(property);
        try {
            System.setProperty(property, "overhead");
            assertThatThrownBy(() -> new PipelineBenchmarkLiveRunIT()
                    .interleavedRealForksWriteEvidenceAndEnforceTheSelectedGate())
                    .isInstanceOf(AssertionError.class).hasMessageContaining("operation time lacks measured error bounds");
        } finally {
            if (previous == null) { System.clearProperty(property); }
            else { System.setProperty(property, previous); }
        }
    }

    @Test void anArithmeticDateMappingCannotBeReportedAsACalibratedResourceWindow() {
        var input = phase(proof(), false);
        var mapped = new RealBenchmarkForkDriver.MeasuredPhase(input.id(), input.acknowledgedOutputs(),
                input.firstIssuedAtNanos(), input.sourceCompletedAtNanos(), input.completedAckAtNanos(),
                input.expectedSourceChanges(), input.observedDeliveries(), input.reportedRecordsOut(),
                input.clockAnchor(), input.sourceBatches(), input.resources(), input.confirmationTiming(),
                input.deliveryTimeline(), input.steadyOutputProfile(),
                Optional.of(new BenchmarkTargetClock.LocalWindow(2, 4, 6, 8)), input.targetClockEvidence());
        var output = PipelineBenchmarkLiveRunIT.phaseEvidence(mapped);
        var window = (Map<?, ?>) output.get("operationResourceWindow");
        assertThat(window.get("state")).isEqualTo("UNQUALIFIED");
        assertThat(window.get("reason")).isEqualTo("OPERATION_TIME_ERROR_BOUND_NOT_ESTABLISHED");
        assertThat(window.get("earliestStartNanos")).isNull();
        assertThat(window.get("latestEndNanos")).isNull();
    }
    @Test void missingOuterOrInteriorClockEvidenceCannotEnterFormalSteadyQualification() {
        var complete = proof();
        for (var incomplete : List.of(Map.<String, Object>of(), Map.<String, Object>of("state", "QUALIFIED"),
                Map.<String, Object>of("sampledInterior", complete.get("sampledInterior")))) {
            assertThatThrownBy(() -> evidence(phase(incomplete, false)).requireSteadyStateWindow())
                    .isInstanceOf(AssertionError.class).hasMessageContaining("outer and sampled interior clocks");
        }
        assertThatThrownBy(() -> evidence(phase(complete, false)).requireSteadyStateWindow())
                .isInstanceOf(AssertionError.class).hasMessageContaining("operation time lacks measured error bounds");
    }

    @Test void qualifiedClockProofCannotMaskExcessiveOutputTrendAndItsFullReadingsRemainSerialized() {
        var complete = proof();
        assertThatThrownBy(() -> evidence(phase(complete, true)).requireSteadyStateWindow())
                .isInstanceOf(AssertionError.class).hasMessageContaining("trend");
        var parsed = (Map<?, ?>) JsonReader.parse(JsonWriter.write(PipelineBenchmarkLiveRunIT.phaseEvidence(phase(complete, false))));
        assertThat(parsed.get("targetClockQualification")).isEqualTo(JsonReader.parse(JsonWriter.write(complete)));
        assertThat(((Map<?, ?>) ((Map<?, ?>) parsed.get("targetClockQualification")).get("sampledInterior")).get("readings"))
                .isInstanceOf(List.class);
    }

    private static Map<String, Object> proof() {
        var readings = new ArrayList<BenchmarkTargetClock.Reading>();
        for (int i = 0; i <= 100; i++) {
            long millis = i * 200L, nanos = millis * 1_000_000L;
            readings.add(new BenchmarkTargetClock.Reading("owned:27017", "one", 1_000_000 + millis,
                    nanos, nanos + 2_000_000, 1_000_000 + millis, 1_000_000 + millis));
        }
        var result = new java.util.LinkedHashMap<String, Object>(BenchmarkTargetClock.validate(readings.getFirst(), readings.getLast()));
        result.put("sampledInterior", BenchmarkTargetClock.validateSeries(readings));
        return Map.copyOf(result);
    }

    private static RealBenchmarkForkDriver.MeasuredPhase phase(Map<String, Object> clock, boolean changing) {
        var observed = new ArrayList<Long>();
        var walls = new ArrayList<Long>();
        for (int i = 0; i < 12_000; i++) {
            long elapsed = changing && i >= 6_000 ? 6_000 + (i - 6_000) * 2L : i;
            observed.add((elapsed + 10) * 1_000_000L);
            walls.add(1_000_000 + elapsed);
        }
        var timeline = new RealBenchmarkForkDriver.DeliveryTimeline(observed.getFirst(), observed.getLast(),
                observed.getFirst(), observed.getLast(), 12_000, 12_000, observed, observed, walls, List.of(walls));
        var timing = new RealBenchmarkForkDriver.ConfirmationTiming(1, 2, observed.getLast() + 1,
                observed.getFirst(), observed.getLast());
        return new RealBenchmarkForkDriver.MeasuredPhase("cdc-update", 12_000, 0, 1, observed.getLast() + 1,
                12_000, 12_000, 12_000, new BenchmarkForkEnvironment.ClockAnchor(Instant.EPOCH, 0, 1), List.of(),
                new BenchmarkResourceSampler.Summary(70, 3, 1_500, 2_500, 2), Optional.of(timing), Optional.of(timeline),
                true, Optional.empty(), clock);
    }

    private static RealBenchmarkForkDriver.Evidence evidence(RealBenchmarkForkDriver.MeasuredPhase phase) {
        return new RealBenchmarkForkDriver.Evidence("clock-contract", BenchmarkWorkloadDefinitions.steadyPilot("copy"),
                PipelineBenchmarkComparison.Arm.B, Path.of("clock-contract.jar"), List.of(phase), phase.resources(),
                new BenchmarkMongoCommandSampler.Summary(Map.of(), Map.of(), 1), Map.of(), Map.of(), "qualified", 0, Optional.empty());
    }
}
