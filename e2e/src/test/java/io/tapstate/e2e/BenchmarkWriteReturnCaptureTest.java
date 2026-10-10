package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkWriteReturnCaptureTest {
    @Test void first_and_last_actual_getters_enclose_the_complete_owned_capture() throws Exception {
        var access = new Fake();
        try (var capture = BenchmarkWriteReturnCapture.open(access, "measured")) {
            var result = capture.finish();
            assertThat(result.calls()).hasSize(1);
            assertThat(result.calls().getFirst().rows().getFirst().keys()).containsExactly(1);
            assertThat(result.samples().size()).isBetween(2, 512);
            assertThat(access.starts).hasValue(1); assertThat(access.stops).hasValue(1);
            assertThat(access.firstBeforeStart).isTrue(); assertThat(access.finalAfterStop).isTrue();
        }
        assertThat(access.stops).hasValue(1);
    }

    @Test void a_refused_start_never_stops_someone_elses_capture() throws Exception {
        var access = new Fake(); access.acceptStart = false;
        assertThatThrownBy(() -> BenchmarkWriteReturnCapture.open(access, "measured"))
                .isInstanceOf(AssertionError.class).hasMessageContaining("start was refused");
        assertThat(access.stops).hasValue(0);
    }

    @Test void an_open_call_at_stop_never_obtains_a_final_coverage_result() throws Exception {
        var access = new Fake(); access.acceptStop = false;
        var capture = BenchmarkWriteReturnCapture.open(access, "measured");
        assertThatThrownBy(capture::finish).isInstanceOf(AssertionError.class).hasMessageContaining("incomplete call");
        assertThatThrownBy(capture::close).isInstanceOf(AssertionError.class);
        assertThat(access.stops).hasValue(1);
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
        var access = new Fake();
        var capture = BenchmarkWriteReturnCapture.open(access, "measured");
        assertThatThrownBy(capture::close).isInstanceOf(AssertionError.class).hasMessageContaining("UNKNOWN");
        assertThat(access.stops).hasValue(1);
        assertThatThrownBy(capture::finish).isInstanceOf(AssertionError.class).hasMessageContaining("not active");
    }

    private static final class Fake implements BenchmarkWriteReturnCapture.Access {
        final AtomicInteger starts = new AtomicInteger(), stops = new AtomicInteger();
        boolean acceptStart = true, acceptStop = true, foreignSummary, extraCount, wrongBytes, throwStop;
        volatile boolean firstBeforeStart, finalAfterStop;
        final byte[] page;
        Fake() throws Exception { page = BenchmarkWriteReturnCaptureTest.page(); }
        public BenchmarkCausalClock.Sample clock(long sequence) {
            if (sequence == 0) { firstBeforeStart = starts.get() == 0; }
            if (stops.get() > 0) { finalAfterStop = true; }
            return new BenchmarkCausalClock.Sample(sequence, new BenchmarkCausalClock.Identity(17, 1000),
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
                    "RECORDED_SCOPE_UNQUALIFIED", extraCount ? 2 : 1, 0, extraCount ? 2 : 1, 0,
                    wrongBytes ? 0 : page.length - 32 - "measured".length() - "RECORDED_SCOPE_UNQUALIFIED".length());
        }
        public byte[] page(long cursor) { return page; }
    }

    @Test void a_retained_byte_total_cannot_hide_missing_or_replaced_page_contents() throws Exception {
        var access = new Fake(); access.wrongBytes = true;
        var capture = BenchmarkWriteReturnCapture.open(access, "measured");
        try {
            assertThatThrownBy(capture::finish).isInstanceOf(AssertionError.class).hasMessageContaining("retained-byte");
        } finally { capture.close(); }
    }

    private static byte[] page() throws Exception {
        var record = new ByteArrayOutputStream(); var call = new DataOutputStream(record);
        call.writeLong(1); call.writeInt(1); call.writeInt(1); call.writeInt(0); call.writeInt(1);
        call.writeLong(1500); call.writeLong(1600); call.writeBoolean(true); call.writeInt(1);
        call.writeLong(1); call.writeLong(0); call.writeLong(0); call.writeInt(0); call.writeInt(0);
        text(call, ""); text(call, "state=ORDINARY_ACKNOWLEDGED;reason=PINNED_RUNTIME_SCOPE;concern=w:1,j:DEFAULT,timeoutMs:DEFAULT");
        text(call, "pdk.state.pipeline.sink"); text(call, "orders"); text(call, "orders");
        call.writeByte(1); text(call, "id"); call.writeInt(1); call.writeByte(1); call.writeInt(1);
        call.flush(); var raw = record.toByteArray();
        var page = new ByteArrayOutputStream(); var out = new DataOutputStream(page);
        out.writeInt(0x57525031); out.writeInt(2); out.writeLong(1); out.writeInt(0); out.writeInt(1); out.writeInt(1);
        text(out, "measured"); text(out, "RECORDED_SCOPE_UNQUALIFIED"); out.writeInt(raw.length); out.write(raw);
        out.flush(); return page.toByteArray();
    }
    private static void text(DataOutputStream out, String value) throws Exception {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8); out.writeShort(bytes.length); out.write(bytes);
    }
}
