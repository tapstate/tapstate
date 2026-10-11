package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkWriteReturnCaptureTest {
    @Test void disabled_clock_request_retention_never_calls_the_diagnostic_access_helper() throws Exception {
        try (var property = new ClockRequestProperty(null)) {
            var access = new Fake(); access.refuseClockRequestRead = true;
            try (var capture = BenchmarkWriteReturnCapture.open(access, "measured",
                    BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL)) {
                capture.finish();
                assertThat(capture.retainedEvidence()).doesNotContainKey("clockRequests");
            }
            assertThat(access.clockRequestReads).hasValue(0);
            var refused = new Fake(); refused.acceptStart = false; refused.refuseClockRequestRead = true;
            var failure = org.assertj.core.api.Assertions.catchThrowable(() -> BenchmarkWriteReturnCapture.open(
                    refused, "measured", BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL));
            assertThat(failure).isInstanceOf(AssertionError.class).hasMessageContaining("start was refused");
            assertThat(failure.getSuppressed()).hasSize(1);
            assertThat(failure.getSuppressed()[0]).hasMessageContaining("ABORTED_BEFORE_SUCCESSFUL_STOP");
            assertThat(BenchmarkWriteReturnCapture.retainedClockRequests(failure)).isEmpty();
            assertThat(refused.clockRequestReads).hasValue(0);
        }
    }

    @Test void early_failure_freezes_nested_clock_request_facts_without_repeating_a_clock_or_page() throws Exception {
        try (var property = new ClockRequestProperty("true")) {
            var row = new java.util.LinkedHashMap<String, Object>(); row.put("requestCount", 1L);
            var rows = new java.util.ArrayList<Object>(); rows.add(row);
            var ledger = new java.util.LinkedHashMap<String, Object>();
            ledger.put("state", "UNKNOWN"); ledger.put("partialRequests", rows); ledger.put("wholeMethodCostQualified", false);
            var access = new Fake(); access.acceptStart = false; access.clockRequests = ledger;
            var failure = org.assertj.core.api.Assertions.catchThrowable(() -> BenchmarkWriteReturnCapture.open(
                    access, "measured", BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL));
            var retained = BenchmarkWriteReturnCapture.retainedClockRequests(failure);
            row.put("requestCount", 999L); rows.clear(); ledger.clear();
            var retainedRows = (java.util.List<?>) retained.get("partialRequests");
            var retainedRow = (java.util.Map<?, ?>) retainedRows.getFirst();
            assertThat(retainedRow.get("requestCount")).isEqualTo(1L);
            assertThat(retained.get("state")).isEqualTo("UNKNOWN");
            assertThatThrownBy(retained::clear).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(retainedRows::clear).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(retainedRow::clear).isInstanceOf(UnsupportedOperationException.class);
            assertThat(access.clockRequestReads).hasValue(1);
            assertThat(access.clockReads).hasValue(1); assertThat(access.pageReads).hasValue(0);
            assertThat(access.stops).hasValue(0);
        }
    }

    private static final class ClockRequestProperty implements AutoCloseable {
        private final String prior = System.getProperty(RealBenchmarkForkDriver.ROOT_CPU_DIAGNOSTICS_PROPERTY);
        ClockRequestProperty(String value) {
            if (value == null) { System.clearProperty(RealBenchmarkForkDriver.ROOT_CPU_DIAGNOSTICS_PROPERTY); }
            else { System.setProperty(RealBenchmarkForkDriver.ROOT_CPU_DIAGNOSTICS_PROPERTY, value); }
        }
        public void close() {
            if (prior == null) { System.clearProperty(RealBenchmarkForkDriver.ROOT_CPU_DIAGNOSTICS_PROPERTY); }
            else { System.setProperty(RealBenchmarkForkDriver.ROOT_CPU_DIAGNOSTICS_PROPERTY, prior); }
        }
    }

    @Test void first_and_last_actual_getters_enclose_the_complete_owned_capture() throws Exception {
        var access = new Fake();
        try (var capture = BenchmarkWriteReturnCapture.open(access, "measured")) {
            assertThat(capture.clockMode()).isEqualTo(BenchmarkReturnClockSampler.Mode.PERIODIC);
            var result = capture.finish();
            assertThat(result.clockMode()).isEqualTo(BenchmarkReturnClockSampler.Mode.PERIODIC);
            assertThat(result.calls()).hasSize(1);
            assertThat(result.calls().getFirst().lastCallbackExitNanos()).isEqualTo(1550);
            assertThat(result.calls().getFirst().rows().getFirst().keys()).containsExactly(1);
            assertThat(result.samples().size()).isBetween(2, 512);
            assertThat(result.pagesBase64()).containsExactly(java.util.Base64.getEncoder().encodeToString(access.page));
            assertThatThrownBy(() -> result.pagesBase64().clear()).isInstanceOf(UnsupportedOperationException.class);
            assertThat(access.starts).hasValue(1); assertThat(access.stops).hasValue(1);
            assertThat(access.firstBeforeStart).isTrue(); assertThat(access.finalAfterStop).isTrue();
            assertThat(access.costStageReads).hasValue(0);
            assertThat(result.costStages()).isEmpty();
            var compatible = new BenchmarkWriteReturnCapture.Result(result.calls(), result.samples(), result.summary(), result.pagesBase64());
            assertThat(compatible.clockMode()).isEqualTo(BenchmarkReturnClockSampler.Mode.PERIODIC);
            assertThat(compatible.calls()).isEqualTo(result.calls());
            assertThat(compatible.samples()).isEqualTo(result.samples());
        }
        assertThat(access.stops).hasValue(1);
    }

    @Test void the_first_final_control_preserves_complete_receipts_and_explicit_sampler_mode() throws Exception {
        var access = new Fake();
        try (var capture = BenchmarkWriteReturnCapture.open(access, "measured",
                BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL)) {
            assertThat(capture.clockMode()).isEqualTo(BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL);
            assertThat(access.clockReads).hasValue(1);
            var result = capture.finish();
            assertThat(result.clockMode()).isEqualTo(BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL);
            assertThat(result.samples()).extracting(BenchmarkCausalClock.Sample::sequence).containsExactly(0L, 1L);
            assertThat(result.calls()).hasSize(1);
            assertThat(result.calls().getFirst().lastCallbackExitNanos()).isEqualTo(1550);
            assertThat(result.calls().getFirst().rows().getFirst().keys()).containsExactly(1);
            assertThat(result.pagesBase64()).containsExactly(java.util.Base64.getEncoder().encodeToString(access.page));
            assertThat(access.clockReads).hasValue(2);
            assertThat(access.starts).hasValue(1); assertThat(access.stops).hasValue(1);
            assertThat(access.firstBeforeStart).isTrue(); assertThat(access.finalAfterStop).isTrue();
            var evidence = capture.retainedEvidence();
            assertThat(evidence).containsEntry("completed", true).containsEntry("performanceAcceptanceEligible", false);
            var sampling = (java.util.Map<?, ?>) evidence.get("sampler");
            assertThat(sampling.containsKey("fixedDelayNanos")).isFalse();
            assertThat(sampling.get("mode")).isEqualTo("FIRST_FINAL_CONTROL");
            assertThat(sampling.get("periodicPollingEnabled")).isEqualTo(false);
            assertThat(access.costStageReads).hasValue(0);
        }
        assertThat(access.clockReads).hasValue(2);
        assertThat(access.stops).hasValue(1);
    }

    @Test void a_refused_start_never_stops_someone_elses_capture() throws Exception {
        for (var mode : BenchmarkReturnClockSampler.Mode.values()) {
            var access = new Fake(); access.acceptStart = false;
            assertThatThrownBy(() -> BenchmarkWriteReturnCapture.open(access, "measured", mode))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("start was refused");
            assertThat(access.stops).hasValue(0);
            if (mode == BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL) { assertThat(access.clockReads).hasValue(1); }
        }
    }

    @Test void an_open_call_at_stop_never_obtains_a_final_coverage_result() throws Exception {
        for (var mode : BenchmarkReturnClockSampler.Mode.values()) {
            var access = new Fake(); access.acceptStop = false;
            var capture = BenchmarkWriteReturnCapture.open(access, "measured", mode);
            assertThatThrownBy(capture::finish).isInstanceOf(AssertionError.class).hasMessageContaining("incomplete call");
            assertThatThrownBy(capture::close).isInstanceOf(AssertionError.class);
            assertThat(access.stops).hasValue(1);
            if (mode == BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL) { assertThat(access.clockReads).hasValue(1); }
        }
    }

    @Test void a_lost_stop_reply_cannot_stop_a_later_window_during_cleanup() throws Exception {
        var access = new Fake(); access.throwStop = true;
        var capture = BenchmarkWriteReturnCapture.open(access, "measured");
        assertThatThrownBy(capture::finish).isInstanceOf(IllegalStateException.class).hasMessage("lost stop reply");
        assertThatThrownBy(capture::close).isInstanceOf(AssertionError.class);
        assertThat(access.stops).hasValue(1);
    }

    @Test void the_same_window_name_cannot_replace_the_started_capture_epoch() throws Exception {
        var access = new Fake();
        var capture = BenchmarkWriteReturnCapture.open(access, "measured");
        java.nio.ByteBuffer.wrap(access.page).putLong(8, 2);
        try {
            assertThatThrownBy(capture::finish).isInstanceOf(AssertionError.class).hasMessageContaining("epoch changed");
        } finally { capture.close(); }
    }

    @Test void terminal_totals_and_window_cannot_promote_a_missing_or_foreign_receipt() throws Exception {
        for (boolean foreign : new boolean[]{false, true}) {
            var access = new Fake(); access.foreignSummary = foreign; access.extraCount = !foreign;
            var capture = BenchmarkWriteReturnCapture.open(access, "measured");
            try {
                assertThatThrownBy(capture::finish).isInstanceOf(AssertionError.class);
            } finally { capture.close(); }
        }
    }

    @Test void normal_close_before_finish_is_an_abort_not_a_qualified_window() throws Exception {
        for (var mode : BenchmarkReturnClockSampler.Mode.values()) {
            var access = new Fake();
            var capture = BenchmarkWriteReturnCapture.open(access, "measured", mode);
            assertThatThrownBy(capture::close).isInstanceOf(AssertionError.class).hasMessageContaining("UNKNOWN");
            assertThat(access.stops).hasValue(1);
            assertThatThrownBy(capture::finish).isInstanceOf(AssertionError.class).hasMessageContaining("not active");
            if (mode == BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL) { assertThat(access.clockReads).hasValue(1); }
        }
    }

    @Test void final_control_identity_or_reader_failure_cannot_create_a_result_or_repeat_stop() throws Exception {
        for (boolean changedIdentity : new boolean[]{false, true}) {
            var access = new Fake();
            access.changeFinalIdentity = changedIdentity; access.throwFinalClock = !changedIdentity;
            var capture = BenchmarkWriteReturnCapture.open(access, "measured", BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL);
            try {
                assertThatThrownBy(capture::finish).isInstanceOf(AssertionError.class).hasMessageContaining("UNKNOWN");
                assertThat(capture.retainedEvidence()).containsEntry("completed", false);
                var evidence = (java.util.Map<?, ?>) capture.retainedEvidence().get("sampler");
                assertThat(evidence.get("state")).isEqualTo("UNKNOWN");
                assertThat(evidence.get("finalRecorded")).isEqualTo(false);
                assertThat(access.clockReads).hasValue(2);
            } finally { assertThatThrownBy(capture::close).isInstanceOf(AssertionError.class).hasMessageContaining("UNKNOWN"); }
            assertThat(access.clockReads).hasValue(2);
            assertThat(access.stops).hasValue(1);
        }
    }

    private static final class Fake implements BenchmarkWriteReturnCapture.Access {
        final AtomicInteger starts = new AtomicInteger(), stops = new AtomicInteger();
        final AtomicInteger clockReads = new AtomicInteger();
        final AtomicInteger pageReads = new AtomicInteger(), costStageReads = new AtomicInteger();
        final AtomicInteger clockRequestReads = new AtomicInteger();
        java.util.Map<String, Object> clockRequests = java.util.Map.of();
        boolean refuseClockRequestRead;
        java.util.Map<String, Object> costStages = java.util.Map.of("state", "RECORDED", "samplingCostQualified", false);
        BenchmarkWriteReturnReader.CostStagesRefusal costStageRefusal;
        boolean acceptStart = true, acceptStop = true, foreignSummary, extraCount, wrongBytes, throwStop, failedSummary;
        boolean changeFinalIdentity, throwFinalClock;
        volatile boolean firstBeforeStart, finalAfterStop;
        final byte[] page;
        Fake() throws Exception { page = BenchmarkWriteReturnCaptureTest.page(); }
        public BenchmarkCausalClock.Sample clock(long sequence) {
            clockReads.incrementAndGet();
            if (sequence == 0) { firstBeforeStart = starts.get() == 0; }
            if (stops.get() > 0) {
                finalAfterStop = true;
                if (throwFinalClock) { throw new IllegalStateException("controlled final getter failure"); }
            }
            return new BenchmarkCausalClock.Sample(sequence, new BenchmarkCausalClock.Identity(17,
                    changeFinalIdentity && stops.get() > 0 ? 1001 : 1000),
                    sequence * 10_000, sequence * 10_000 + 10, sequence == 0 ? 1000 : 1_000_000 + sequence);
        }
        public boolean start(String window) { starts.incrementAndGet(); return acceptStart; }
        public boolean stop() {
            stops.incrementAndGet();
            if (throwStop) { throw new IllegalStateException("lost stop reply"); }
            return acceptStop;
        }
        public BenchmarkWriteReturnReader.Summary summary() {
            return new BenchmarkWriteReturnReader.Summary(foreignSummary ? "foreign" : "measured",
                    failedSummary ? "UNKNOWN:PARTIAL_OR_FAILED_CALL" : "RECORDED_SCOPE_UNQUALIFIED",
                    extraCount ? 2 : 1, failedSummary ? 1 : 0, extraCount ? 2 : 1, 0,
                    wrongBytes ? 0 : page.length - 32 - "measured".length() - "RECORDED_SCOPE_UNQUALIFIED".length());
        }
        public byte[] page(long cursor) { pageReads.incrementAndGet(); return page; }
        public java.util.Map<String, Object> clockRequestsEvidence() {
            clockRequestReads.incrementAndGet();
            if (refuseClockRequestRead) { throw new AssertionError("disabled capture requested clock diagnostics"); }
            return clockRequests;
        }
        public java.util.Map<String, Object> costStages(long epoch, String window, long completedCalls) {
            costStageReads.incrementAndGet();
            if (stops.get() != 1 || pageReads.get() != 2 || epoch != 1 || !"measured".equals(window) || completedCalls != 1) {
                throw new AssertionError("cost-stage read must follow successful stop and complete receipt assembly");
            }
            if (costStageRefusal != null) { throw costStageRefusal; }
            return costStages;
        }
    }

    @Test void stage_summaries_are_read_once_after_complete_receipts_and_unknown_stays_explicit() throws Exception {
        for (String state : java.util.List.of("RECORDED", "UNKNOWN")) {
            var access = new Fake();
            access.costStages = java.util.Map.of("state", state, "samplingCostQualified", false);
            try (var capture = BenchmarkWriteReturnCapture.open(access, "measured",
                    BenchmarkReturnClockSampler.Mode.PERIODIC, true)) {
                var result = capture.finish();
                assertThat(result.costStages()).isEqualTo(access.costStages);
                assertThat(capture.retainedEvidence().get("costStages")).isEqualTo(access.costStages);
                assertThat(access.costStageReads).hasValue(1);
                assertThatThrownBy(() -> result.costStages().put("state", "changed"))
                        .isInstanceOf(UnsupportedOperationException.class);
            }
            assertThat(access.costStageReads).hasValue(1);
        }
    }

    @Test void refused_receipts_and_other_clock_controls_never_read_cost_stages() throws Exception {
        var access = new Fake(); access.wrongBytes = true;
        try (var capture = BenchmarkWriteReturnCapture.open(access, "measured",
                BenchmarkReturnClockSampler.Mode.PERIODIC, true)) {
            assertThatThrownBy(capture::finish).isInstanceOf(AssertionError.class).hasMessageContaining("retained-byte");
            assertThat(access.costStageReads).hasValue(0);
        }
        assertThatThrownBy(() -> BenchmarkWriteReturnCapture.open(access, "measured",
                BenchmarkReturnClockSampler.Mode.FIRST_FINAL_CONTROL, true))
                .isInstanceOf(AssertionError.class).hasMessageContaining("unchanged periodic");
        assertThat(access.costStageReads).hasValue(0);
    }

    @Test void a_cost_stage_read_refusal_retains_its_raw_facts_without_repeating_the_read_or_stop() throws Exception {
        var access = new Fake();
        var retained = java.util.Map.<String, Object>of("state", "UNKNOWN", "raw", "malformed",
                "samplingCostQualified", false);
        var refusal = new BenchmarkWriteReturnReader.CostStagesRefusal(new AssertionError("controlled shape refusal"), retained);
        access.costStageRefusal = refusal;
        try (var capture = BenchmarkWriteReturnCapture.open(access, "measured",
                BenchmarkReturnClockSampler.Mode.PERIODIC, true)) {
            assertThatThrownBy(capture::finish).isSameAs(refusal);
            assertThat(capture.retainedEvidence()).containsEntry("completed", false).containsEntry("costStages", retained);
            assertThat((java.util.List<?>) capture.retainedEvidence().get("retainedPagesBase64")).hasSize(1);
        }
        assertThat(access.costStageReads).hasValue(1); assertThat(access.stops).hasValue(1);
    }

    @Test void a_retained_byte_total_cannot_hide_missing_or_replaced_page_contents() throws Exception {
        var access = new Fake(); access.wrongBytes = true;
        var capture = BenchmarkWriteReturnCapture.open(access, "measured");
        try {
            assertThatThrownBy(capture::finish).isInstanceOf(AssertionError.class).hasMessageContaining("retained-byte");
            assertThat(capture.retainedEvidence()).containsEntry("completed", false)
                    .containsEntry("terminalSummaryAvailable", true);
            assertThat(capture.retainedEvidence().get("retainedPagesBase64"))
                    .isEqualTo(java.util.List.of(java.util.Base64.getEncoder().encodeToString(access.page)));
        } finally { capture.close(); }
    }

    @Test void an_unknown_failed_terminal_retains_its_original_pages_without_promoting_them() throws Exception {
        var access = new Fake(); access.failedSummary = true;
        var capture = BenchmarkWriteReturnCapture.open(access, "measured");
        try {
            assertThatThrownBy(capture::finish).isInstanceOf(AssertionError.class).hasMessageContaining("unqualified");
            assertThat(capture.retainedEvidence()).containsEntry("completed", false);
            assertThat(capture.retainedEvidence().get("retainedPagesBase64"))
                    .isEqualTo(java.util.List.of(java.util.Base64.getEncoder().encodeToString(access.page)));
        } finally { capture.close(); }
        assertThat(access.stops).hasValue(1);
    }

    private static byte[] page() throws Exception {
        var record = new ByteArrayOutputStream(); var call = new DataOutputStream(record);
        call.writeLong(1); call.writeInt(1); call.writeInt(1); call.writeInt(0); call.writeInt(1);
        call.writeLong(1500); call.writeLong(1550); call.writeLong(1600); call.writeBoolean(true); call.writeInt(1);
        call.writeLong(1); call.writeLong(0); call.writeLong(0); call.writeInt(0); call.writeInt(0);
        text(call, ""); text(call, "state=ORDINARY_ACKNOWLEDGED;reason=PINNED_RUNTIME_SCOPE;concern=w:1,j:DEFAULT,timeoutMs:DEFAULT");
        text(call, "pdk.state.pipeline.sink"); text(call, "orders"); text(call, "orders");
        call.writeByte(1); text(call, "id"); call.writeInt(1); call.writeByte(1); call.writeInt(1);
        call.flush(); var raw = record.toByteArray();
        var page = new ByteArrayOutputStream(); var out = new DataOutputStream(page);
        out.writeInt(0x57525031); out.writeInt(3); out.writeLong(1); out.writeInt(0); out.writeInt(1); out.writeInt(1);
        text(out, "measured"); text(out, "RECORDED_SCOPE_UNQUALIFIED"); out.writeInt(raw.length); out.write(raw);
        out.flush(); return page.toByteArray();
    }
    private static void text(DataOutputStream out, String value) throws Exception {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8); out.writeShort(bytes.length); out.write(bytes);
    }
}
