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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Formal clock qualification is independent of output steadiness and survives report serialization. */
class BenchmarkClockQualificationContractTest {
    @Test void missingOuterOrInteriorClockEvidenceCannotEnterFormalSteadyQualification() {
        var complete = proof();
        for (var incomplete : List.of(Map.<String, Object>of(), Map.<String, Object>of("state", "QUALIFIED"),
                Map.<String, Object>of("sampledInterior", complete.get("sampledInterior")))) {
            assertThatThrownBy(() -> evidence(phase(incomplete, false)).requireSteadyStateWindow())
                    .isInstanceOf(AssertionError.class).hasMessageContaining("outer and sampled interior clocks");
        }
        assertThatCode(() -> evidence(phase(complete, false)).requireSteadyStateWindow()).doesNotThrowAnyException();
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
