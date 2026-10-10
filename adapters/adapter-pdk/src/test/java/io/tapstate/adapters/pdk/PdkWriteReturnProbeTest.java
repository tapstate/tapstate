package io.tapstate.adapters.pdk;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteMode;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkWriteReturnProbeTest {
    private static final String PROPERTY = "tapstate.benchmark.write-return";
    private static final String NAME = "io.tapstate.benchmark:type=WriteReturn";

    @Test
    void callbacks_do_not_complete_the_call_and_all_flush_counts_survive(@TempDir Path dir) throws Exception {
        withEnabledProbe(() -> {
            PdkConnector connector = connector(dir);
            try (SinkWriter writer = new PdkSinkWriter(connector, (context, rows, table, callback) -> {
                callback.accept(new WriteListResult<>(1L, 0L, 0L));
                assertThat(attribute("CompletedCalls")).isEqualTo(0L);
                callback.accept(new WriteListResult<>(0L, 1L, 0L));
            }, WriteMode.UPSERT, DdlPolicy.FAIL, Map.of())) {
                start();
                assertThat(writer.write(List.of(row(1), row(2))).toCompletableFuture()
                        .get(5, TimeUnit.SECONDS).written()).isEqualTo(2);
                assertThat(attribute("CompletedCalls")).isEqualTo(1L);
                assertThat(attribute("ReportedRecords")).isEqualTo(2L);
                assertThat(attribute("FailedCalls")).isEqualTo(0L);
            }
        });
    }

    @Test
    void a_throw_after_partial_success_keeps_the_original_failure_and_receipt(@TempDir Path dir) throws Exception {
        withEnabledProbe(() -> {
            PdkConnector connector = connector(dir);
            try (SinkWriter writer = new PdkSinkWriter(connector, (context, rows, table, callback) -> {
                callback.accept(new WriteListResult<>(1L, 0L, 0L));
                throw new IllegalStateException("synthetic partial write");
            }, WriteMode.UPSERT, DdlPolicy.FAIL, Map.of())) {
                start();
                assertThatThrownBy(() -> writer.write(List.of(row(1), row(2))).toCompletableFuture()
                        .get(5, TimeUnit.SECONDS)).hasRootCauseMessage("synthetic partial write");
                assertThat(attribute("CompletedCalls")).isEqualTo(1L);
                assertThat(attribute("ReportedRecords")).isEqualTo(1L);
                assertThat(attribute("FailedCalls")).isEqualTo(1L);
            }
        });
    }

    @Test
    void each_table_return_has_its_own_receipt(@TempDir Path dir) throws Exception {
        withEnabledProbe(() -> {
            PdkConnector connector = connector(dir);
            try (SinkWriter writer = new PdkSinkWriter(connector, (context, rows, table, callback) ->
                    callback.accept(new WriteListResult<>((long) rows.size(), 0L, 0L)),
                    WriteMode.UPSERT, DdlPolicy.FAIL, Map.of())) {
                start();
                writer.write(List.of(row(1), Envelope.insert(1, "other", Map.of("id", 2), null)))
                        .toCompletableFuture().get(5, TimeUnit.SECONDS);
                assertThat(attribute("CompletedCalls")).isEqualTo(2L);
                assertThat(attribute("ReportedRecords")).isEqualTo(2L);
            }
        });
    }

    @Test
    void the_full_key_roster_and_initial_return_observation_are_retained() throws Exception {
        AtomicLong clock = new AtomicLong(100);
        var probe = new PdkWriteReturnProbe(clock::get);
        var writer = probe.writer("pipeline/sink");
        assertThat(probe.start("measured")).isTrue();
        var call = writer.begin("source", table(), List.of(event(1), event(2)));
        clock.set(150);
        call.callback(new WriteListResult<>(1L, 0L, 0L));
        assertThat(probe.getCompletedCalls()).isZero();
        call.callback(new WriteListResult<>(1L, 0L, 0L));
        clock.set(300);
        call.returned(null);
        var frame = decode(probe.read(0)).frames().getFirst();
        assertThat(frame.began()).isEqualTo(100);
        assertThat(frame.observed()).isEqualTo(300);
        assertThat(frame.callbacks()).isEqualTo(2);
        assertThat(frame.keys()).containsExactly(1, 2);
        assertThat(frame.scope()).isEqualTo("UNKNOWN");
        assertThat(probe.getState()).isEqualTo("RECORDED_SCOPE_UNQUALIFIED");
        assertThat(probe.stop()).isTrue();
    }

    @Test
    void completion_cursor_preserves_reverse_return_order_across_writers() throws Exception {
        var probe = new PdkWriteReturnProbe(new AtomicLong(100)::incrementAndGet);
        var first = probe.writer("first"); var second = probe.writer("second");
        probe.start("measured");
        var one = first.begin("source", table(), List.of(event(1)));
        var two = second.begin("source", table(), List.of(event(2)));
        two.callback(new WriteListResult<>(1L, 0L, 0L)); two.returned(null);
        var page = decode(probe.read(0));
        assertThat(page.next()).isEqualTo(1);
        assertThat(page.frames().getFirst().sequence()).isEqualTo(2);
        one.callback(new WriteListResult<>(1L, 0L, 0L)); one.returned(null);
        var next = decode(probe.read(page.next()));
        assertThat(next.frames().getFirst().sequence()).isEqualTo(1);
        assertThat(next.next()).isEqualTo(2);
    }

    @Test
    void repeated_keys_keep_distinct_attempt_ids_and_unknown_partial_results() throws Exception {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
        var writer = probe.writer("writer"); probe.start("measured");
        var partial = writer.begin("source", table(), List.of(event(1), event(2)));
        partial.callback(new WriteListResult<>(1L, 0L, 0L)); partial.returned(null);
        var retry = writer.begin("source", table(), List.of(event(1), event(2)));
        retry.callback(new WriteListResult<>(2L, 0L, 0L)); retry.returned(null);
        var frames = decode(probe.read(0)).frames();
        assertThat(frames).extracting(Frame::sequence).containsExactly(1L, 2L);
        assertThat(frames).extracting(Frame::keys).containsExactly(List.of(1, 2), List.of(1, 2));
        assertThat(probe.getState()).isEqualTo("UNKNOWN:FAILED_PARTIAL_OR_MISSING_CALLBACK");
    }

    @Test
    void late_callbacks_cannot_backfill_a_completed_receipt() throws Exception {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
        var writer = probe.writer("writer"); probe.start("measured");
        var call = writer.begin("source", table(), List.of(event(1)));
        call.callback(new WriteListResult<>(1L, 0L, 0L)); call.returned(null);
        byte[] original = probe.read(0);
        call.callback(new WriteListResult<>(1L, 0L, 0L));
        assertThat(probe.getReportedRecords()).isEqualTo(1);
        assertThat(probe.getState()).isEqualTo("UNKNOWN:LATE_CALLBACK");
        assertThat(decode(probe.read(0)).frames()).isEqualTo(decode(original).frames());
    }

    @Test
    void callback_counts_cannot_cancel_a_negative_flush_into_success() {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
        var writer = probe.writer("writer"); probe.start("measured");
        var call = writer.begin("source", table(), List.of(event(1), event(2)));
        call.callback(new WriteListResult<>(3L, 0L, 0L));
        call.callback(new WriteListResult<>(-1L, 0L, 0L)); call.returned(null);
        assertThat(probe.getReportedRecords()).isEqualTo(2);
        assertThat(probe.getState()).isEqualTo("UNKNOWN:INVALID_CALLBACK_COUNT");
    }

    @Test
    void a_callback_during_the_return_clock_read_is_already_late() {
        var callRef = new java.util.concurrent.atomic.AtomicReference<PdkWriteReturnProbe.Ticket>();
        AtomicLong reads = new AtomicLong();
        var probe = new PdkWriteReturnProbe(() -> {
            long read = reads.incrementAndGet();
            if (read == 2) { callRef.get().callback(new WriteListResult<>(1L, 0L, 0L)); }
            return read;
        });
        var writer = probe.writer("writer"); probe.start("measured");
        var call = writer.begin("source", table(), List.of(event(1)));
        callRef.set(call); call.returned(null);
        assertThat(probe.getReportedRecords()).isZero();
        assertThat(probe.getState()).isEqualTo("UNKNOWN:LATE_CALLBACK");
    }

    @Test
    void missing_callbacks_and_thrown_partial_writes_are_unqualified() throws Exception {
        for (boolean thrown : List.of(false, true)) {
            var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
            var writer = probe.writer("writer"); probe.start("measured");
            var call = writer.begin("source", table(), List.of(event(1)));
            call.returned(thrown ? new IllegalStateException("private diagnostic must not leak") : null);
            assertThat(probe.getState()).isEqualTo("UNKNOWN:FAILED_PARTIAL_OR_MISSING_CALLBACK");
            assertThat(probe.getFailedCalls()).isEqualTo(thrown ? 1 : 0);
            assertThat(new String(probe.read(0), StandardCharsets.UTF_8)).doesNotContain("private diagnostic");
        }
    }

    @Test
    void stopped_open_calls_and_duplicate_completion_cannot_appear_complete() {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
        var writer = probe.writer("writer"); probe.start("measured");
        var call = writer.begin("source", table(), List.of(event(1)));
        assertThat(probe.stop()).isFalse();
        assertThat(probe.getState()).isEqualTo("UNKNOWN:OPEN_CALLS_AT_STOP");
        call.callback(new WriteListResult<>(1L, 0L, 0L)); call.returned(null); call.returned(null);
        assertThat(probe.getCompletedCalls()).isEqualTo(1);
        assertThat(probe.getOpenCalls()).isZero();
    }

    @Test
    void an_active_window_cannot_be_reset_and_an_inactive_window_reads_no_clock() {
        AtomicLong reads = new AtomicLong();
        var probe = new PdkWriteReturnProbe(reads::incrementAndGet);
        var writer = probe.writer("writer");
        assertThat(writer.begin("source", table(), List.of(event(1)))).isNull();
        assertThat(reads).hasValue(0);
        probe.start("measured");
        assertThat(probe.start("other")).isFalse();
        assertThat(probe.getWindow()).isEqualTo("measured");
    }

    @Test
    void unsupported_numeric_keys_never_get_rounded_into_the_cohort() {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
        var writer = probe.writer("writer"); probe.start("measured");
        var bad = TapInsertRecordEvent.create().table("target").after(Map.of("id", 1.5));
        var call = writer.begin("source", table(), List.of(bad));
        call.callback(new WriteListResult<>(1L, 0L, 0L)); call.returned(null);
        assertThat(probe.getState()).isEqualTo("UNKNOWN:UNSUPPORTED_NUMERIC_KEY");
        assertThat(probe.getRetainedBytes()).isZero();
        assertThat(probe.getReportedRecords()).isEqualTo(1);
    }

    @Test
    void frame_overflow_preserves_the_original_pages_and_marks_loss() throws Exception {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
        var writer = probe.writer("writer"); probe.start("measured");
        for (int i = 0; i <= PdkWriteReturnProbe.MAX_FRAMES; i++) {
            var call = writer.begin("source", table(), List.of(event(i)));
            call.callback(new WriteListResult<>(1L, 0L, 0L)); call.returned(null);
        }
        assertThat(probe.getCompletedCalls()).isEqualTo(PdkWriteReturnProbe.MAX_FRAMES + 1);
        assertThat(probe.getState()).isEqualTo("UNKNOWN:LEDGER_OVERFLOW");
        assertThat(probe.getRetainedBytes()).isLessThanOrEqualTo(PdkWriteReturnProbe.MAX_LEDGER_BYTES);
        int cursor = 0; List<Integer> keys = new ArrayList<>();
        do {
            byte[] bytes = probe.read(cursor);
            assertThat(bytes.length).isLessThanOrEqualTo(PdkWriteReturnProbe.MAX_FRAME_BYTES);
            var page = decode(bytes);
            page.frames().forEach(frame -> keys.addAll(frame.keys()));
            assertThat(page.next()).isGreaterThan(cursor);
            cursor = page.next();
        } while (cursor < PdkWriteReturnProbe.MAX_FRAMES);
        assertThat(keys).hasSize(PdkWriteReturnProbe.MAX_FRAMES).doesNotHaveDuplicates();
    }

    @Test
    void the_largest_admitted_error_receipt_still_advances_its_page() throws Exception {
        List<TapRecordEvent> rows = java.util.stream.IntStream.range(0, 512).mapToObj(PdkWriteReturnProbeTest::event).toList();
        Throwable type = new ErrorWithALongNameForTheBoundedReceiptBoundarySoErrorTypeMetadataStillFitsTheSameTransportFrameAsTheFullBatchKeyRoster();
        int detailBytes = 6 + type.getClass().getName().getBytes(StandardCharsets.UTF_8).length;
        var empty = errorReceipt("writer", rows, type, 0);
        int base = firstFrameLength(empty.read(0));
        int limit = PdkWriteReturnProbe.MAX_FRAME_BYTES - 2048 - Integer.BYTES;
        int errors = (limit - base) / detailBytes;
        int padding = limit - base - errors * detailBytes;
        var exact = errorReceipt("writer" + "x".repeat(padding), rows, type, errors);
        byte[] page = exact.read(0);
        assertThat(firstFrameLength(page)).isEqualTo(limit);
        assertThat(decode(page).next()).isEqualTo(1);
        assertThat(page.length).isLessThanOrEqualTo(PdkWriteReturnProbe.MAX_FRAME_BYTES);
        var tooLarge = errorReceipt("writer" + "x".repeat(padding + 1), rows, type, errors);
        assertThat(tooLarge.getRetainedBytes()).isZero();
        assertThat(tooLarge.getState()).isEqualTo("UNKNOWN:FRAME_OVERFLOW");
    }

    @Test
    void a_native_1024_row_call_keeps_every_key_in_bounded_parts() throws Exception {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
        var writer = probe.writer("writer"); probe.start("measured");
        var rows = java.util.stream.IntStream.range(0, 1024).mapToObj(PdkWriteReturnProbeTest::event).toList();
        var call = writer.begin("source", table(), rows);
        call.callback(new WriteListResult<>(1024L, 0L, 0L)); call.returned(null);
        assertThat(probe.getState()).isEqualTo("RECORDED_SCOPE_UNQUALIFIED");
        assertThat(probe.getCompletedCalls()).isEqualTo(1);
        assertThat(decode(probe.read(0)).frames().stream().flatMap(frame -> frame.keys().stream()).toList())
                .containsExactlyElementsOf(java.util.stream.IntStream.range(0, 1024).boxed().toList());
    }

    private static PdkWriteReturnProbe errorReceipt(String identity, List<TapRecordEvent> rows, Throwable type, int errors) {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
        var writer = probe.writer(identity); probe.start("measured");
        var call = writer.begin("source", table(), rows);
        var result = new WriteListResult<TapRecordEvent>(rows.size() - errors, 0L, 0L);
        for (int i = 0; i < errors; i++) { result.addError(rows.get(i), type); }
        call.callback(result); call.returned(null); return probe;
    }
    private static int firstFrameLength(byte[] page) throws Exception {
        var input = new DataInputStream(new ByteArrayInputStream(page));
        input.skipNBytes(28); text(input); text(input); return input.readInt();
    }
    private static final class ErrorWithALongNameForTheBoundedReceiptBoundarySoErrorTypeMetadataStillFitsTheSameTransportFrameAsTheFullBatchKeyRoster
            extends RuntimeException { }

    @Test
    void a_backwards_counter_is_not_clamped_into_a_successful_time() {
        AtomicLong clock = new AtomicLong(100);
        var probe = new PdkWriteReturnProbe(clock::get);
        var writer = probe.writer("writer"); probe.start("measured");
        var call = writer.begin("source", table(), List.of(event(1)));
        call.callback(new WriteListResult<>(1L, 0L, 0L)); clock.set(99); call.returned(null);
        assertThat(probe.getState()).isEqualTo("UNKNOWN:CALL_IDENTITY_OR_CLOCK_ORDER");
        assertThat(probe.getCompletedCalls()).isZero();
    }

    private static TapTable table() {
        return new TapTable("target").add(new TapField("id", "int").primaryKeyPos(1));
    }
    private static TapRecordEvent event(int key) {
        return TapInsertRecordEvent.create().table("target").after(Map.of("id", key));
    }
    private record Frame(long sequence, long began, long observed, int callbacks, String scope, List<Integer> keys) { }
    private record Page(int next, List<Frame> frames) { }
    private static Page decode(byte[] bytes) throws Exception {
        var in = new DataInputStream(new ByteArrayInputStream(bytes));
        assertThat(in.readInt()).isEqualTo(0x57525031); assertThat(in.readInt()).isEqualTo(2);
        in.readLong(); int first = in.readInt(); int next = in.readInt(); in.readInt(); text(in); text(in);
        List<Frame> frames = new ArrayList<>();
        for (int i = first; i < next; i++) {
            var frame = new DataInputStream(new ByteArrayInputStream(in.readNBytes(in.readInt())));
            long sequence = frame.readLong(); frame.readInt();
            frame.readInt(); frame.readInt(); frame.readInt();
            long began = frame.readLong(); long observed = frame.readLong(); frame.readBoolean();
            int callbacks = frame.readInt(); frame.readLong(); frame.readLong(); frame.readLong(); frame.readInt();
            int errors = frame.readInt();
            for (int error = 0; error < errors; error++) { frame.readInt(); text(frame); }
            text(frame); String scope = text(frame); text(frame); text(frame); text(frame);
            int keyCount = frame.readUnsignedByte();
            for (int key = 0; key < keyCount; key++) { text(frame); }
            int rows = frame.readInt(); List<Integer> keys = new ArrayList<>();
            for (int row = 0; row < rows; row++) {
                frame.readByte();
                for (int key = 0; key < keyCount; key++) { keys.add(frame.readInt()); }
            }
            assertThat(frame.available()).isZero();
            frames.add(new Frame(sequence, began, observed, callbacks, scope, List.copyOf(keys)));
        }
        assertThat(in.available()).isZero(); return new Page(next, List.copyOf(frames));
    }
    private static String text(DataInputStream in) throws Exception {
        return new String(in.readNBytes(in.readUnsignedShort()), StandardCharsets.UTF_8);
    }

    private static PdkConnector connector(Path dir) throws Exception {
        Path jar = Synthetic.countingSink(dir);
        ConnectorRef ref = new ConnectorRef(List.of(jar), "synthetic.CountingSink", "2.0.8", null);
        return PdkConnector.open("demo", ref, Map.of());
    }

    private static Envelope row(int id) {
        return Envelope.insert(1, "t1", Map.of("id", id), null);
    }

    private static ObjectName name() throws Exception { return new ObjectName(NAME); }
    private static Object attribute(String key) throws Exception {
        return ManagementFactory.getPlatformMBeanServer().getAttribute(name(), key);
    }
    private static void start() throws Exception {
        assertThat(ManagementFactory.getPlatformMBeanServer().isRegistered(name()))
                .as("enabled capture must install its owned local return probe").isTrue();
        assertThat(ManagementFactory.getPlatformMBeanServer().invoke(name(), "start", new Object[]{"control"},
                new String[]{String.class.getName()})).isEqualTo(true);
    }
    private static void withEnabledProbe(Checked action) throws Exception {
        String previous = System.getProperty(PROPERTY);
        System.setProperty(PROPERTY, "true");
        try { action.run(); }
        finally {
            if (ManagementFactory.getPlatformMBeanServer().isRegistered(name())) {
                ManagementFactory.getPlatformMBeanServer().invoke(name(), "stop", new Object[0], new String[0]);
            }
            if (previous == null) { System.clearProperty(PROPERTY); }
            else { System.setProperty(PROPERTY, previous); }
        }
    }
    @FunctionalInterface private interface Checked { void run() throws Exception; }
}
