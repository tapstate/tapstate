package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkWriteReturnPhaseEvidenceTest {
    private static final String STATE = "RECORDED_SCOPE_UNQUALIFIED";
    private static final String SCOPE = "state=ORDINARY_ACKNOWLEDGED;reason=PINNED_RUNTIME_SCOPE;concern=w:1,j:DEFAULT,timeoutMs:DEFAULT";

    @Test
    void completePilotRetainsRawSourceSamplesAndBothIntervalExtremes() throws Exception {
        Fixture fixture = fixture(BenchmarkWorkloadDefinitions.steadyPilot("copy"), false);
        var evidence = record(fixture, fixture.result());
        assertThat(evidence.get("state")).isEqualTo("RECORDED_CAUSAL_RETURN_BOUNDS");
        assertThat(evidence.get("fullRows")).isEqualTo(96_000);
        assertThat(evidence.get("fixedCohortRows")).isEqualTo(48_000L);
        assertThat(evidence.get("pagesBase64")).isEqualTo(fixture.result().pagesBase64());
        assertThat((List<?>) evidence.get("sourceBatches")).hasSize(960);
        assertThat((List<?>) evidence.get("clockSamples")).hasSize(fixture.result().samples().size());
        assertThat(((Map<?, ?>) evidence.get("p99LatencyNanos")).keySet()
                .containsAll(List.of("lowerNanos", "upperNanos"))).isTrue();
        assertThat(((Map<?, ?>) evidence.get("throughput")).keySet()
                .containsAll(List.of("lowerRecordsPerSecond", "upperRecordsPerSecond"))).isTrue();
        assertThat(evidence.get("performanceAcceptanceEligible")).isEqualTo(false);
        assertThat(evidence.get("returnCaptureDelayQualified")).isEqualTo(false);
        assertThat(evidence.get("samplingCostQualified")).isEqualTo(false);
        assertThat(evidence.get("formalCommonActiveWindowQualified")).isEqualTo(false);
        assertThat(evidence.get("completionSpanScope")).isEqualTo("FULL_FIXED_COHORT_EARLIEST_TO_LATEST_RETURN");
        assertThat(evidence.get("physicalPerRowCommitTime")).isEqualTo(false);
        assertThatThrownBy(evidence::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(((List<?>) evidence.get("pagesBase64"))::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void broadCausalBoundsKeepNegativeLatencyAndRefuseAnAmbiguousRate() throws Exception {
        Fixture fixture = fixture(BenchmarkWorkloadDefinitions.byId("copy"), true);
        var evidence = record(fixture, fixture.result());
        assertThat(((Number) ((Map<?, ?>) evidence.get("p99LatencyNanos")).get("lowerNanos")).longValue()).isNegative();
        assertThat(((Number) ((Map<?, ?>) evidence.get("completionSpanNanos")).get("lowerNanos")).longValue()).isNegative();
        assertThat((Map<?, ?>) evidence.get("throughput")).isEqualTo(Map.of(
                "state", "UNAVAILABLE", "reason", "NONPOSITIVE_COMPLETION_SPAN_LOWER_BOUND"));
    }

    @Test
    void aSmallControlDoesNotManufactureP99Eligibility() throws Exception {
        var original = BenchmarkWorkloadDefinitions.byId("copy");
        var target = original.phase("cdc-update").targets().getFirst();
        var phase = new BenchmarkWorkloadDefinitions.Phase("cdc-update", BenchmarkWorkloadDefinitions.Stage.CDC_UPDATE,
                true, 4, List.of("UPDATE bench_copy_orders SET amount = amount + 1 WHERE id BETWEEN 1 AND 4"),
                1, Duration.ZERO, Map.of(), List.of(target));
        var workload = new BenchmarkWorkloadDefinitions.Workload("copy", original.seed(), 4, original.database(),
                original.pipelineIds(), original.sourceChains(), List.of(), List.of(phase));
        Fixture fixture = fixture(workload, true);
        var evidence = record(fixture, fixture.result());
        assertThat(evidence.get("fixedCohortRows")).isEqualTo(4L);
        assertThat((Map<?, ?>) evidence.get("p99LatencyNanos")).isEqualTo(Map.of(
                "state", "UNAVAILABLE", "reason", "INSUFFICIENT_COMPLETE_DELIVERIES_FOR_P99"));
        assertThat(evidence.get("performanceAcceptanceEligible")).isEqualTo(false);
    }

    @Test
    void firstFinalClockControlRetainsCompleteEvidenceButCannotProduceReturnPerformanceMetrics() throws Exception {
        Fixture fixture = fixture(BenchmarkWorkloadDefinitions.steadyPilot("copy"), false);
        var original = fixture.result();
        var last = original.samples().getLast();
        var samples = List.of(original.samples().getFirst(), new BenchmarkCausalClock.Sample(1, last.identity(),
                last.driverBeforeNanos(), last.driverAfterNanos(), last.ownedNanos()));
        var control = new BenchmarkWriteReturnCapture.Result(original.calls(), samples, original.summary(),
                original.pagesBase64(), BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL);
        var evidence = record(fixture, control);
        assertThat(evidence).containsEntry("state", "RECORDED_CLOCK_COST_CONTROL")
                .containsEntry("clockSamplingMode", "FIRST_FINAL_CONTROL")
                .containsEntry("fullRows", 96_000).containsEntry("fixedCohortRows", 48_000L)
                .containsEntry("performanceAcceptanceEligible", false).containsEntry("samplingCostQualified", false);
        assertThat(evidence.get("pagesBase64")).isEqualTo(original.pagesBase64());
        assertThat((List<?>) evidence.get("sourceBatches")).hasSize(960);
        assertThat((List<?>) evidence.get("clockSamples")).hasSize(2);
        for (String key : List.of("p99LatencyNanos", "throughput", "completionSpanNanos")) {
            assertThat((Map<?, ?>) evidence.get(key)).isEqualTo(Map.of(
                    "state", "UNAVAILABLE", "reason", "FIRST_FINAL_CLOCK_COST_CONTROL"));
        }
        assertThatThrownBy(() -> BenchmarkWriteReturnPhaseEvidence.record(fixture.workload(), fixture.phase(),
                fixture.batches().subList(1, fixture.batches().size()), control))
                .isInstanceOf(AssertionError.class).hasMessageContaining("source batch roster");
        reject(fixture, new BenchmarkWriteReturnCapture.Result(original.calls(), samples, original.summary(), List.of(),
                BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL), "raw page roster");
    }

    @Test
    void aClockControlCannotRelabelAPeriodicSampleRosterAsTwoBoundaryReads() throws Exception {
        Fixture fixture = fixture(BenchmarkWorkloadDefinitions.byId("copy"), false);
        var original = fixture.result();
        reject(fixture, new BenchmarkWriteReturnCapture.Result(original.calls(), original.samples(), original.summary(),
                original.pagesBase64(), BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL), "exactly two actual samples");
    }

    @Test
    void missingPagesForeignWindowAndContradictoryCountsAreRejected() throws Exception {
        Fixture fixture = fixture(BenchmarkWorkloadDefinitions.byId("copy"), false);
        var result = fixture.result();
        reject(fixture, new BenchmarkWriteReturnCapture.Result(result.calls(), result.samples(), result.summary(), List.of()), "raw page roster");
        var summary = result.summary();
        var foreign = new BenchmarkWriteReturnReader.Summary("foreign/cdc-update", STATE, summary.completedCalls(), 0,
                summary.reportedRecords(), 0, summary.retainedBytes());
        reject(fixture, new BenchmarkWriteReturnCapture.Result(result.calls(), result.samples(), foreign, result.pagesBase64()),
                "another workload or phase");
        var counts = new BenchmarkWriteReturnReader.Summary(summary.window(), STATE, summary.completedCalls(), 0,
                summary.reportedRecords() + 1, 0, summary.retainedBytes());
        reject(fixture, new BenchmarkWriteReturnCapture.Result(result.calls(), result.samples(), counts, result.pagesBase64()), "totals disagree");
    }

    @Test
    void rawPagesCannotBeReplacedByAnUnrelatedSuppliedCallList() throws Exception {
        Fixture fixture = fixture(BenchmarkWorkloadDefinitions.byId("copy"), false);
        var result = fixture.result();
        var calls = new ArrayList<>(result.calls());
        var first = calls.getFirst();
        calls.set(0, new BenchmarkWriteReturnAssembly.FullCall(first.sequence(), first.writer(), first.totalRows(),
                first.beganNanos() + 1, first.lastCallbackExitNanos(), first.observedNanos(), first.returnedNormally(), first.callbackCount(),
                first.inserted(), first.modified(), first.removed(), first.errors(), first.errorDetails(), first.failureType(),
                first.scope(), first.writerIdentity(), first.stream(), first.target(), first.keyFields(), first.rows()));
        reject(fixture, new BenchmarkWriteReturnCapture.Result(calls, result.samples(), result.summary(), result.pagesBase64()),
                "do not reproduce");
    }

    @Test
    void reusedOrNonenclosingOwnedClockEvidenceIsRejected() throws Exception {
        Fixture fixture = fixture(BenchmarkWorkloadDefinitions.byId("copy"), false);
        var result = fixture.result();
        var samples = new ArrayList<>(result.samples());
        var last = samples.getLast();
        samples.set(samples.size() - 1, new BenchmarkCausalClock.Sample(last.sequence(),
                new BenchmarkCausalClock.Identity(17, 1001), last.driverBeforeNanos(), last.driverAfterNanos(), last.ownedNanos()));
        reject(fixture, new BenchmarkWriteReturnCapture.Result(result.calls(), samples, result.summary(), result.pagesBase64()),
                "another or missing owned runtime");
        reject(fixture, new BenchmarkWriteReturnCapture.Result(result.calls(), result.samples().subList(0, 2),
                result.summary(), result.pagesBase64()), "strictly enclosing");
    }

    @Test
    void malformedRawPagesAndIncompleteSourceRostersAreRefused() throws Exception {
        Fixture fixture = fixture(BenchmarkWorkloadDefinitions.byId("copy"), false);
        var result = fixture.result();
        reject(fixture, new BenchmarkWriteReturnCapture.Result(result.calls(), result.samples(), result.summary(), List.of("!")), "not base64");
        assertThatThrownBy(() -> BenchmarkWriteReturnPhaseEvidence.record(fixture.workload(), fixture.phase(),
                fixture.batches().subList(1, fixture.batches().size()), result))
                .isInstanceOf(AssertionError.class).hasMessageContaining("source batch roster");
    }

    @Test
    void unknownOrStillOpenCaptureAndMissingClockSamplesCannotBecomeEvidence() throws Exception {
        Fixture fixture = fixture(BenchmarkWorkloadDefinitions.byId("copy"), false);
        var result = fixture.result(); var summary = result.summary();
        for (var invalid : List.of(
                new BenchmarkWriteReturnReader.Summary(summary.window(), "UNKNOWN:OPEN_CALLS_AT_STOP",
                        summary.completedCalls(), 0, summary.reportedRecords(), 0, summary.retainedBytes()),
                new BenchmarkWriteReturnReader.Summary(summary.window(), STATE, summary.completedCalls(), 0,
                        summary.reportedRecords(), 1, summary.retainedBytes()))) {
            reject(fixture, new BenchmarkWriteReturnCapture.Result(result.calls(), result.samples(), invalid, result.pagesBase64()),
                    "terminal counters are unqualified");
        }
        reject(fixture, new BenchmarkWriteReturnCapture.Result(result.calls(), List.of(), summary, result.pagesBase64()),
                "owned clock samples are missing");
    }

    private record Fixture(BenchmarkWorkloadDefinitions.Workload workload, BenchmarkWorkloadDefinitions.Phase phase,
            List<BenchmarkForkEnvironment.BatchResult> batches, BenchmarkWriteReturnCapture.Result result) { }

    private static Fixture fixture(BenchmarkWorkloadDefinitions.Workload workload, boolean broad) throws Exception {
        var phase = workload.phase("cdc-update");
        List<BenchmarkForkEnvironment.BatchResult> batches = new ArrayList<>();
        for (int index = 0; index < phase.batches().size(); index++) {
            long issued = broad ? 1500 + index * 20L : 50 + index * 20L;
            batches.add(new BenchmarkForkEnvironment.BatchResult(index, issued, issued + 10));
        }
        List<BenchmarkWriteReturnAssembly.FullCall> calls = new ArrayList<>();
        int writer = 0;
        for (var plan : BenchmarkExpectedChanges.forPhase(workload, phase)) {
            writer++;
            List<String> fields = switch (plan.target().projection()) {
                case COPY, NEST -> List.of("id");
                case JOIN -> List.of("order_id");
                case STATELESS -> List.of("item_index", "id");
            };
            List<BenchmarkWriteReturnLedger.Row> rows = new ArrayList<>();
            for (var batch : plan.batches()) {
                for (var change : batch) {
                    String[] tuple = change.key().split(":");
                    Map<String, Integer> values = Map.of("id", Integer.parseInt(tuple[0]), "order_id", Integer.parseInt(tuple[0]),
                            "item_index", tuple.length == 2 ? Integer.parseInt(tuple[1]) : 0);
                    rows.add(new BenchmarkWriteReturnLedger.Row(1, fields.stream().map(values::get).toList()));
                }
            }
            for (int offset = 0; offset < rows.size(); offset += 1024) {
                var slice = rows.subList(offset, Math.min(offset + 1024, rows.size()));
                long sequence = calls.size() + 1L;
                calls.add(new BenchmarkWriteReturnAssembly.FullCall(sequence, writer, slice.size(), sequence * 1000 + 100,
                        sequence * 1000 + 150, sequence * 1000 + 200, true, 1, slice.size(), 0, 0, 0, List.of(), "", SCOPE,
                        "pdk.state." + plan.target().pipelineId() + ".sink", "stream", plan.target().table(), fields, slice));
            }
        }
        var owner = new BenchmarkCausalClock.Identity(17, 1000);
        List<BenchmarkCausalClock.Sample> samples = new ArrayList<>();
        if (broad) {
            samples.add(new BenchmarkCausalClock.Sample(0, owner, 1000, 1010, 0));
            samples.add(new BenchmarkCausalClock.Sample(1, owner, 2_000_000, 2_000_010, 10_000_000));
        } else {
            for (int index = 0; index <= calls.size() + 1; index++) {
                samples.add(new BenchmarkCausalClock.Sample(index, owner, index * 1_000_000L,
                        index * 1_000_000L + 10, index * 1000L));
            }
        }
        String window = workload.id() + "/" + phase.id();
        List<byte[]> pieces = new ArrayList<>();
        for (var call : calls) for (int index = 0; index < (call.totalRows() + 511) / 512; index++) pieces.add(piece(call, index));
        List<String> pages = new ArrayList<>();
        int cursor = 0;
        while (cursor < pieces.size()) {
            int next = cursor, bytes = 32 + window.length() + STATE.length();
            while (next < pieces.size() && bytes + 4 + pieces.get(next).length <= 64 * 1024) bytes += 4 + pieces.get(next++).length;
            var buffer = new ByteArrayOutputStream(); var out = new DataOutputStream(buffer);
            out.writeInt(0x57525031); out.writeInt(3); out.writeLong(1); out.writeInt(cursor); out.writeInt(next); out.writeInt(pieces.size());
            text(out, window); text(out, STATE);
            for (int index = cursor; index < next; index++) { out.writeInt(pieces.get(index).length); out.write(pieces.get(index)); }
            out.flush(); pages.add(Base64.getEncoder().encodeToString(buffer.toByteArray())); cursor = next;
        }
        long retained = pieces.stream().mapToLong(piece -> piece.length + 4).sum();
        var summary = new BenchmarkWriteReturnReader.Summary(window, STATE, calls.size(), 0,
                phase.expectedLogicalOutputChanges(), 0, retained);
        return new Fixture(workload, phase, List.copyOf(batches), new BenchmarkWriteReturnCapture.Result(calls, samples, summary, pages));
    }

    private static byte[] piece(BenchmarkWriteReturnAssembly.FullCall call, int index) throws Exception {
        var buffer = new ByteArrayOutputStream(); var out = new DataOutputStream(buffer);
        out.writeLong(call.sequence()); out.writeInt(call.writer()); out.writeInt(call.totalRows());
        out.writeInt(index); out.writeInt((call.totalRows() + 511) / 512);
        out.writeLong(call.beganNanos()); out.writeLong(call.lastCallbackExitNanos()); out.writeLong(call.observedNanos()); out.writeBoolean(true); out.writeInt(1);
        out.writeLong(call.inserted()); out.writeLong(0); out.writeLong(0); out.writeInt(0); out.writeInt(0);
        text(out, ""); text(out, SCOPE); text(out, call.writerIdentity()); text(out, call.stream()); text(out, call.target());
        out.writeByte(call.keyFields().size()); for (String field : call.keyFields()) text(out, field);
        var rows = call.rows().subList(index * 512, Math.min((index + 1) * 512, call.totalRows()));
        out.writeInt(rows.size());
        for (var row : rows) { out.writeByte(row.kind()); for (int key : row.keys()) out.writeInt(key); }
        out.flush(); return buffer.toByteArray();
    }

    private static void text(DataOutputStream out, String value) throws Exception {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8); out.writeShort(bytes.length); out.write(bytes);
    }
    private static Map<String, Object> record(Fixture fixture, BenchmarkWriteReturnCapture.Result result) {
        return BenchmarkWriteReturnPhaseEvidence.record(fixture.workload(), fixture.phase(), fixture.batches(), result);
    }
    private static void reject(Fixture fixture, BenchmarkWriteReturnCapture.Result result, String reason) {
        assertThatThrownBy(() -> record(fixture, result)).isInstanceOf(AssertionError.class).hasMessageContaining(reason);
    }
}
