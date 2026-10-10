package io.tapstate.adapters.pdk;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
    void synchronous_callback_exit_is_bounded_before_the_outer_return(@TempDir Path dir) throws Exception {
        withEnabledProbe(() -> {
            CountDownLatch callbackReturned = new CountDownLatch(1);
            CountDownLatch allowOuterReturn = new CountDownLatch(1);
            AtomicLong beforeCallback = new AtomicLong();
            AtomicLong afterCallback = new AtomicLong();
            AtomicLong beforeOuterReturn = new AtomicLong();
            PdkConnector connector = connector(dir);
            TargetTable target = new TargetTable("target", List.of(new TargetField("id", "int", true)));
            try (SinkWriter writer = new PdkSinkWriter(connector, (context, rows, table, callback) -> {
                assertThat(rows).hasSize(1);
                assertThat(table.primaryKeys()).containsExactly("id");
                beforeCallback.set(System.nanoTime());
                callback.accept(new WriteListResult<>(1L, 0L, 0L));
                afterCallback.set(System.nanoTime());
                callbackReturned.countDown();
                assertThat(allowOuterReturn.await(5, TimeUnit.SECONDS))
                        .as("the owner must release the connector's outer return").isTrue();
                beforeOuterReturn.set(System.nanoTime());
            }, WriteMode.UPSERT, DdlPolicy.FAIL, Map.of("t1", target))) {
                start();
                var completion = writer.write(List.of(row(1))).toCompletableFuture();
                try {
                    assertThat(callbackReturned.await(5, TimeUnit.SECONDS))
                            .as("the synchronous callback must return before the connector is released").isTrue();
                    assertThat(attribute("CompletedCalls")).isEqualTo(0L);
                    assertThat(completion.isDone()).isFalse();
                    allowOuterReturn.countDown();
                    assertThat(completion.get(5, TimeUnit.SECONDS).written()).isEqualTo(1);
                    long afterOuterReturn = System.nanoTime();
                    assertThat(attribute("CompletedCalls")).isEqualTo(1L);
                    assertThat(attribute("ReportedRecords")).isEqualTo(1L);
                    assertThat(attribute("FailedCalls")).isEqualTo(0L);
                    byte[] page = (byte[]) ManagementFactory.getPlatformMBeanServer().invoke(name(), "read",
                            new Object[]{0L}, new String[]{"long"});
                    var frame = decodeCallbackExit(page);
                    assertThat(frame.began()).isLessThanOrEqualTo(beforeCallback.get());
                    assertThat(frame.lastCallbackExit()).isBetween(beforeCallback.get(), afterCallback.get());
                    assertThat(frame.lastCallbackExit()).isLessThan(beforeOuterReturn.get());
                    assertThat(frame.observed()).isBetween(beforeOuterReturn.get(), afterOuterReturn);
                    assertThat(frame.lastCallbackExit()).isLessThan(frame.observed());
                } finally {
                    allowOuterReturn.countDown();
                    completion.handle((result, failure) -> null).get(5, TimeUnit.SECONDS);
                }
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
        call.callbackExited();
        assertThat(probe.getCompletedCalls()).isZero();
        call.callback(new WriteListResult<>(1L, 0L, 0L));
        clock.set(200);
        call.callbackExited();
        clock.set(300);
        call.returned(null);
        var frame = decode(probe.read(0)).frames().getFirst();
        assertThat(frame.began()).isEqualTo(100);
        assertThat(frame.lastCallbackExit()).isEqualTo(200);
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
        two.callback(new WriteListResult<>(1L, 0L, 0L)); two.callbackExited(); two.returned(null);
        var page = decode(probe.read(0));
        assertThat(page.next()).isEqualTo(1);
        assertThat(page.frames().getFirst().sequence()).isEqualTo(2);
        one.callback(new WriteListResult<>(1L, 0L, 0L)); one.callbackExited(); one.returned(null);
        var next = decode(probe.read(page.next()));
        assertThat(next.frames().getFirst().sequence()).isEqualTo(1);
        assertThat(next.next()).isEqualTo(2);
    }

    @Test
    void repeated_keys_keep_distinct_attempt_ids_and_unknown_partial_results() throws Exception {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
        var writer = probe.writer("writer"); probe.start("measured");
        var partial = writer.begin("source", table(), List.of(event(1), event(2)));
        partial.callback(new WriteListResult<>(1L, 0L, 0L)); partial.callbackExited(); partial.returned(null);
        var retry = writer.begin("source", table(), List.of(event(1), event(2)));
        retry.callback(new WriteListResult<>(2L, 0L, 0L)); retry.callbackExited(); retry.returned(null);
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
        call.callback(new WriteListResult<>(1L, 0L, 0L)); call.callbackExited(); call.returned(null);
        byte[] original = probe.read(0);
        call.callback(new WriteListResult<>(1L, 0L, 0L));
        assertThat(probe.getReportedRecords()).isEqualTo(1);
        assertThat(probe.getState()).isEqualTo("UNKNOWN:LATE_CALLBACK");
        assertThat(decode(probe.read(0)).frames()).isEqualTo(decode(original).frames());
    }

    @Test
    void foreign_callback_threads_do_not_turn_a_successful_write_into_a_data_error(@TempDir Path dir) throws Exception {
        withEnabledProbe(() -> {
            PdkConnector connector = connector(dir);
            TargetTable target = new TargetTable("target", List.of(new TargetField("id", "int", true)));
            try (SinkWriter writer = new PdkSinkWriter(connector, (context, rows, table, callback) ->
                    onOtherThread(() -> callback.accept(new WriteListResult<>(1L, 0L, 0L))),
                    WriteMode.UPSERT, DdlPolicy.FAIL, Map.of("t1", target))) {
                start();
                assertThat(writer.write(List.of(row(1))).toCompletableFuture()
                        .get(5, TimeUnit.SECONDS).written()).isEqualTo(1);
                assertThat(attribute("CompletedCalls")).isEqualTo(1L);
                assertThat(attribute("FailedCalls")).isEqualTo(0L);
                assertThat(attribute("ReportedRecords")).isEqualTo(0L);
                assertThat(attribute("State")).isEqualTo("UNKNOWN:CALLBACK_THREAD_MISMATCH");
                byte[] page = (byte[]) ManagementFactory.getPlatformMBeanServer().invoke(name(), "read",
                        new Object[]{0L}, new String[]{"long"});
                var frame = decode(page).frames().getFirst();
                assertThat(frame.callbacks()).isZero();
                assertThat(frame.keys()).containsExactly(1);
            }
        });
    }

    @Test
    void a_foreign_callback_exit_cannot_supply_an_owner_callback_bound() throws Exception {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
        var writer = probe.writer("writer"); probe.start("measured");
        var call = writer.begin("source", table(), List.of(event(1)));
        call.callback(new WriteListResult<>(1L, 0L, 0L));
        onOtherThread(call::callbackExited);
        call.returned(null);
        assertThat(probe.getState()).isEqualTo("UNKNOWN:CALLBACK_EXIT_THREAD_MISMATCH");
        assertThat(probe.getCompletedCalls()).isEqualTo(1);
        assertThat(probe.getReportedRecords()).isEqualTo(1);
        assertThat(decode(probe.read(0)).frames().getFirst().lastCallbackExit()).isZero();
    }

    @Test
    void a_missing_callback_exit_cannot_be_inferred_from_normal_counts() throws Exception {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
        var writer = probe.writer("writer"); probe.start("measured");
        var call = writer.begin("source", table(), List.of(event(1)));
        call.callback(new WriteListResult<>(1L, 0L, 0L)); call.returned(null);
        assertThat(probe.getState()).isEqualTo("UNKNOWN:CALLBACK_EXIT_MISSING_OR_UNPAIRED");
        assertThat(probe.getReportedRecords()).isEqualTo(1);
        assertThat(decode(probe.read(0)).frames().getFirst().lastCallbackExit()).isZero();
    }

    @Test
    void one_valid_last_exit_cannot_hide_a_missing_exit_from_an_earlier_flush() throws Exception {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
        var writer = probe.writer("writer"); probe.start("measured");
        var call = writer.begin("source", table(), List.of(event(1), event(2)));
        call.callback(new WriteListResult<>(1L, 0L, 0L));
        call.callback(new WriteListResult<>(1L, 0L, 0L)); call.callbackExited(); call.returned(null);
        assertThat(probe.getState()).isEqualTo("UNKNOWN:CALLBACK_EXIT_MISSING_OR_UNPAIRED");
        assertThat(probe.getReportedRecords()).isEqualTo(2);
        var frame = decode(probe.read(0)).frames().getFirst();
        assertThat(frame.callbacks()).isEqualTo(2);
        assertThat(frame.lastCallbackExit()).isEqualTo(2);
        assertThat(frame.observed()).isEqualTo(3);
    }

    @Test
    void observed_zero_and_negative_callback_exit_counters_remain_valid() throws Exception {
        for (long exit : List.of(0L, -5L)) {
            AtomicLong clock = new AtomicLong(exit - 1);
            var probe = new PdkWriteReturnProbe(clock::get);
            var writer = probe.writer("writer"); probe.start("measured");
            var call = writer.begin("source", table(), List.of(event(1)));
            call.callback(new WriteListResult<>(1L, 0L, 0L));
            clock.set(exit); call.callbackExited();
            clock.set(exit + 1); call.returned(null);
            assertThat(probe.getState()).isEqualTo("RECORDED_SCOPE_UNQUALIFIED");
            assertThat(decode(probe.read(0)).frames().getFirst().lastCallbackExit()).isEqualTo(exit);
        }
    }

    @Test
    void callback_exits_must_pair_once_with_recorded_entries() throws Exception {
        for (boolean beforeEntry : List.of(false, true)) {
            var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
            var writer = probe.writer("writer"); probe.start("measured");
            var call = writer.begin("source", table(), List.of(event(1)));
            if (beforeEntry) { call.callbackExited(); }
            call.callback(new WriteListResult<>(1L, 0L, 0L)); call.callbackExited();
            if (!beforeEntry) { call.callbackExited(); }
            call.returned(null);
            assertThat(probe.getState()).isEqualTo("UNKNOWN:CALLBACK_EXIT_COUNT_MISMATCH");
            assertThat(probe.getReportedRecords()).isEqualTo(1);
            var frame = decode(probe.read(0)).frames().getFirst();
            assertThat(frame.callbacks()).isEqualTo(1);
            assertThat(frame.lastCallbackExit()).isEqualTo(beforeEntry ? 3 : 2);
        }
    }

    @Test
    void a_backwards_callback_exit_does_not_replace_the_last_ordered_exit() throws Exception {
        AtomicLong clock = new AtomicLong(100);
        var probe = new PdkWriteReturnProbe(clock::get);
        var writer = probe.writer("writer"); probe.start("measured");
        var call = writer.begin("source", table(), List.of(event(1), event(2)));
        call.callback(new WriteListResult<>(1L, 0L, 0L)); clock.set(150); call.callbackExited();
        call.callback(new WriteListResult<>(1L, 0L, 0L)); clock.set(140); call.callbackExited();
        clock.set(200); call.returned(null);
        assertThat(probe.getState()).isEqualTo("UNKNOWN:CALLBACK_EXIT_CLOCK_ORDER");
        assertThat(probe.getReportedRecords()).isEqualTo(2);
        var frame = decode(probe.read(0)).frames().getFirst();
        assertThat(frame.callbacks()).isEqualTo(2);
        assertThat(frame.lastCallbackExit()).isEqualTo(150);
    }

    @Test
    void a_callback_exit_after_the_return_observation_is_unqualified() throws Exception {
        AtomicLong clock = new AtomicLong(100);
        var probe = new PdkWriteReturnProbe(clock::get);
        var writer = probe.writer("writer"); probe.start("measured");
        var call = writer.begin("source", table(), List.of(event(1)));
        call.callback(new WriteListResult<>(1L, 0L, 0L)); clock.set(300); call.callbackExited();
        clock.set(200); call.returned(null);
        assertThat(probe.getState()).isEqualTo("UNKNOWN:CALLBACK_EXIT_CLOCK_ORDER");
        var frame = decode(probe.read(0)).frames().getFirst();
        assertThat(frame.lastCallbackExit()).isEqualTo(300);
        assertThat(frame.observed()).isEqualTo(200);
    }

    @Test
    void late_callback_exits_cannot_backfill_completed_or_restarted_windows() throws Exception {
        for (boolean restart : List.of(false, true)) {
            var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
            var writer = probe.writer("writer"); probe.start("measured");
            var old = writer.begin("source", table(), List.of(event(1)));
            old.callback(new WriteListResult<>(1L, 0L, 0L)); old.callbackExited(); old.returned(null);
            if (restart) {
                assertThat(probe.stop()).isTrue(); assertThat(probe.start("next")).isTrue();
                var current = writer.begin("source", table(), List.of(event(2)));
                current.callback(new WriteListResult<>(1L, 0L, 0L)); current.callbackExited(); current.returned(null);
            }
            var original = decode(probe.read(0)).frames();
            old.callbackExited();
            assertThat(probe.getState()).isEqualTo("UNKNOWN:LATE_CALLBACK_EXIT");
            assertThat(probe.getCompletedCalls()).isEqualTo(1);
            assertThat(probe.getReportedRecords()).isEqualTo(1);
            assertThat(decode(probe.read(0)).frames()).isEqualTo(original);
        }
    }

    @Test
    void a_callback_exit_during_the_return_clock_read_is_already_late() throws Exception {
        var callRef = new AtomicReference<PdkWriteReturnProbe.Ticket>();
        AtomicLong reads = new AtomicLong();
        var probe = new PdkWriteReturnProbe(() -> {
            long read = reads.incrementAndGet();
            if (read == 2) { callRef.get().callbackExited(); }
            return read;
        });
        var writer = probe.writer("writer"); probe.start("measured");
        var call = writer.begin("source", table(), List.of(event(1)));
        callRef.set(call); call.callback(new WriteListResult<>(1L, 0L, 0L)); call.returned(null);
        assertThat(probe.getState()).isEqualTo("UNKNOWN:LATE_CALLBACK_EXIT");
        assertThat(probe.getReportedRecords()).isEqualTo(1);
        assertThat(decode(probe.read(0)).frames().getFirst().lastCallbackExit()).isZero();
    }

    @Test
    void a_foreign_return_observation_cannot_be_qualified_by_owner_completion() throws Exception {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
        var writer = probe.writer("writer"); probe.start("measured");
        var call = writer.begin("source", table(), List.of(event(1)));
        call.callback(new WriteListResult<>(1L, 0L, 0L)); call.callbackExited();
        onOtherThread(call::observeReturn); call.completed(null);
        assertThat(probe.getState()).isEqualTo("UNKNOWN:RETURN_THREAD_MISMATCH");
        assertThat(probe.getReportedRecords()).isEqualTo(1);
    }

    @Test
    void callback_counts_cannot_cancel_a_negative_flush_into_success() {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
        var writer = probe.writer("writer"); probe.start("measured");
        var call = writer.begin("source", table(), List.of(event(1), event(2)));
        call.callback(new WriteListResult<>(3L, 0L, 0L));
        call.callbackExited();
        call.callback(new WriteListResult<>(-1L, 0L, 0L)); call.callbackExited(); call.returned(null);
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
        call.callback(new WriteListResult<>(1L, 0L, 0L)); call.callbackExited(); call.returned(null); call.returned(null);
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
        call.callback(new WriteListResult<>(1L, 0L, 0L)); call.callbackExited(); call.returned(null);
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
            call.callback(new WriteListResult<>(1L, 0L, 0L)); call.callbackExited(); call.returned(null);
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
        assertThat(limit).isEqualTo(63_484);
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
        call.callback(new WriteListResult<>(1024L, 0L, 0L)); call.callbackExited(); call.returned(null);
        assertThat(probe.getState()).isEqualTo("RECORDED_SCOPE_UNQUALIFIED");
        assertThat(probe.getCompletedCalls()).isEqualTo(1);
        assertThat(decode(probe.read(0)).frames().stream().flatMap(frame -> frame.keys().stream()).toList())
                .containsExactlyElementsOf(java.util.stream.IntStream.range(0, 1024).boxed().toList());
        assertThat(decode(probe.read(0)).frames()).extracting(Frame::lastCallbackExit).containsExactly(2L, 2L);
    }

    @Test
    void stateful_row_volume_fits_without_confusing_rows_with_a_byte_limit() throws Exception {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
        var writer = probe.writer("writer"); probe.start("measured");
        for (int first = 0; first < 192_000; first += 1024) {
            var rows = java.util.stream.IntStream.range(first, Math.min(first + 1024, 192_000))
                    .mapToObj(PdkWriteReturnProbeTest::event).toList();
            var call = writer.begin("source", table(), rows);
            call.callback(new WriteListResult<>((long) rows.size(), 0L, 0L)); call.callbackExited(); call.returned(null);
        }
        assertThat(probe.getState()).isEqualTo("RECORDED_SCOPE_UNQUALIFIED");
        assertThat(probe.getReportedRecords()).isEqualTo(192_000);
        assertThat(probe.getRetainedBytes()).isLessThanOrEqualTo(2 * 1024 * 1024);
        int cursor = 0, records = 0;
        while (true) {
            var page = decode(probe.read(cursor));
            if (page.next() == cursor) { break; }
            records += page.frames().stream().mapToInt(frame -> frame.keys().size()).sum(); cursor = page.next();
        }
        assertThat(records).isEqualTo(192_000);
    }

    private static PdkWriteReturnProbe errorReceipt(String identity, List<TapRecordEvent> rows, Throwable type, int errors) {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet);
        var writer = probe.writer(identity); probe.start("measured");
        var call = writer.begin("source", table(), rows);
        var result = new WriteListResult<TapRecordEvent>(rows.size() - errors, 0L, 0L);
        for (int i = 0; i < errors; i++) { result.addError(rows.get(i), type); }
        call.callback(result); call.callbackExited(); call.returned(null); return probe;
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
        call.callback(new WriteListResult<>(1L, 0L, 0L)); call.callbackExited(); clock.set(99); call.returned(null);
        assertThat(probe.getState()).isEqualTo("UNKNOWN:CALL_IDENTITY_OR_CLOCK_ORDER");
        assertThat(probe.getCompletedCalls()).isZero();
    }

    @Test
    void default_cost_stages_add_no_clock_reads_and_keep_the_original_V3_bytes() throws Exception {
        AtomicLong one = new AtomicLong(), two = new AtomicLong();
        var original = new PdkWriteReturnProbe(one::incrementAndGet);
        var explicitOff = new PdkWriteReturnProbe(two::incrementAndGet, false);
        for (var probe : List.of(original, explicitOff)) {
            var writer = probe.writer("writer"); probe.start("measured");
            var call = writer.begin("source", table(), List.of(event(1)));
            call.callback(new WriteListResult<>(1L, 0L, 0L)); call.callbackExited(); call.returned(null);
            assertThat(cost(probe)).containsEntry("enabled", false).containsEntry("state", "DISABLED");
        }
        assertThat(one).hasValue(3); assertThat(two).hasValue(3);
        assertThat(original.read(0)).isEqualTo(explicitOff.read(0));
        String producer = System.getProperty(PROPERTY), stages = System.getProperty(PdkWriteReturnProbe.COST_STAGES_PROPERTY);
        try {
            System.clearProperty(PROPERTY); System.setProperty(PdkWriteReturnProbe.COST_STAGES_PROPERTY, "true");
            assertThat(PdkWriteReturnProbe.forWriter(null)).isNull();
        } finally {
            restoreProperty(PROPERTY, producer); restoreProperty(PdkWriteReturnProbe.COST_STAGES_PROPERTY, stages);
        }
    }

    @Test
    void cost_stages_count_a_native_multipart_call_once_and_keep_the_six_elapsed_scopes() throws Exception {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet, true);
        var writer = probe.writer("writer", PdkMongoWriteScope.disabled()); probe.start("measured");
        var rows = java.util.stream.IntStream.range(0, 1024).mapToObj(PdkWriteReturnProbeTest::event).toList();
        var call = writer.begin("source", table(), rows);
        call.callback(new WriteListResult<>(1024L, 0L, 0L)); call.callbackExited(); call.returned(null);
        assertThat(probe.stop()).isTrue();
        var summary = cost(probe);
        assertThat(summary).containsEntry("state", "RECORDED").containsEntry("timeScope", "ELAPSED_NOT_CPU")
                .containsEntry("performanceAcceptanceEligible", false).containsEntry("samplingCostQualified", false)
                .containsEntry("costAcceptanceEligible", false).containsEntry("causalOverheadQualified", false);
        assertThat(number(summary, "fullCallCount")).isEqualTo(1);
        assertThat(number(summary, "completeTimedCalls")).isEqualTo(1);
        assertThat(stageMap(summary)).containsOnlyKeys("BEGIN_LOCK_WAIT", "IDENTITY_ENCODING", "SCOPE_BEFORE",
                "SCOPE_AFTER", "COMPLETE_LOCK_WAIT", "RECEIPT_ENCODING_PUBLICATION");
        for (var value : stageMap(summary).values()) {
            var stage = (Map<?, ?>) value;
            assertThat(number(stage, "count")).isEqualTo(1);
            assertThat(number(stage, "sumNanos")).isEqualTo(1);
            assertThat(number(stage, "maxNanos")).isEqualTo(1);
        }
        assertThat(decode(probe.read(0)).frames()).hasSize(2);
        assertThat(probe.getCostStages().getBytes(StandardCharsets.US_ASCII).length).isLessThanOrEqualTo(8192);
    }

    @Test
    void both_producer_lock_waits_are_measured_before_monitor_acquisition_without_moving_return_stamps() throws Exception {
        for (boolean completion : List.of(false, true)) {
            AtomicLong clock = new AtomicLong(); AtomicLong reads = new AtomicLong();
            CountDownLatch outside = new CountDownLatch(1), ready = new CountDownLatch(1), permit = new CountDownLatch(1);
            var probe = new PdkWriteReturnProbe(() -> {
                long value = clock.get(); if (reads.incrementAndGet() == 1) { outside.countDown(); } return value;
            }, true);
            var writer = probe.writer("writer", PdkMongoWriteScope.disabled()); probe.start("measured");
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread worker = new Thread(() -> {
                try {
                    var call = writer.begin("source", table(), List.of(event(1)));
                    call.callback(new WriteListResult<>(1L, 0L, 0L)); call.callbackExited(); call.observeReturn();
                    if (completion) { ready.countDown(); assertThat(permit.await(2, TimeUnit.SECONDS)).isTrue(); }
                    call.completed(null);
                } catch (Throwable problem) { failure.set(problem); }
            }, "owned-cost-stage-control");
            worker.setDaemon(true);
            try {
                if (completion) {
                    worker.start(); assertThat(ready.await(2, TimeUnit.SECONDS)).isTrue();
                    synchronized (probe) { permit.countDown(); awaitBlocked(worker); clock.set(50); }
                } else {
                    synchronized (probe) {
                        worker.start(); assertThat(outside.await(2, TimeUnit.SECONDS)).isTrue(); awaitBlocked(worker); clock.set(50);
                    }
                }
                worker.join(3000); assertThat(worker.isAlive()).isFalse(); assertThat(failure.get()).isNull();
                var summary = cost(probe); assertThat(summary).containsEntry("state", "RECORDED");
                var selected = (Map<?, ?>) stageMap(summary).get(completion ? "COMPLETE_LOCK_WAIT" : "BEGIN_LOCK_WAIT");
                assertThat(number(selected, "sumNanos")).isEqualTo(50);
                var frame = decode(probe.read(0)).frames().getFirst();
                assertThat(frame.observed()).isEqualTo(completion ? 0 : 50);
            } finally { permit.countDown(); worker.join(3000); }
        }
    }

    @Test
    void failed_duplicate_and_stale_completions_do_not_recount_or_contaminate_the_next_cost_window() {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet, true);
        var writer = probe.writer("writer", PdkMongoWriteScope.disabled()); probe.start("old");
        var old = writer.begin("source", table(), List.of(event(1)));
        old.callback(new WriteListResult<>(1L, 0L, 0L)); old.callbackExited(); old.returned(new IllegalStateException("controlled failure"));
        assertThat(cost(probe)).containsEntry("state", "UNKNOWN").containsEntry("reason", "FAILED_CALL");
        String failed = probe.getCostStages(); old.completed(null); assertThat(probe.getCostStages()).isEqualTo(failed);
        assertThat(probe.getFailedCalls()).isEqualTo(1); assertThat(probe.stop()).isTrue(); assertThat(probe.start("next")).isTrue();
        var current = writer.begin("source", table(), List.of(event(2)));
        current.callback(new WriteListResult<>(1L, 0L, 0L)); current.callbackExited(); current.returned(null);
        String next = probe.getCostStages(); old.completed(null); assertThat(probe.getCostStages()).isEqualTo(next);
        var beforeDuplicate = cost(probe); assertThat(beforeDuplicate).containsEntry("state", "RECORDED");
        current.completed(null);
        var duplicate = cost(probe);
        assertThat(duplicate).containsEntry("state", "UNKNOWN").containsEntry("reason", "RETURN_RECEIPT_UNQUALIFIED");
        assertThat(number(duplicate, "fullCallCount")).isEqualTo(1);
        assertThat(number(duplicate, "completeTimedCalls")).isEqualTo(1);
        assertThat(stageMap(duplicate)).isEqualTo(stageMap(beforeDuplicate));
    }

    @Test
    void aggregate_overflow_refuses_the_entire_call_without_partially_updating_other_stages() throws Exception {
        var probe = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet, true);
        var writer = probe.writer("writer", PdkMongoWriteScope.disabled()); probe.start("measured");
        var first = writer.begin("source", table(), List.of(event(1)));
        first.callback(new WriteListResult<>(1L, 0L, 0L)); first.callbackExited(); first.returned(null);
        var field = PdkWriteReturnProbe.class.getDeclaredField("costStages"); field.setAccessible(true);
        var aggregate = field.get(probe);
        var sumsField = aggregate.getClass().getDeclaredField("sums"); sumsField.setAccessible(true);
        ((long[]) sumsField.get(aggregate))[5] = Long.MAX_VALUE;
        var before = cost(probe);
        var second = writer.begin("source", table(), List.of(event(2)));
        second.callback(new WriteListResult<>(1L, 0L, 0L)); second.callbackExited(); second.returned(null);
        var after = cost(probe);
        assertThat(after).containsEntry("state", "UNKNOWN").containsEntry("reason", "STAGE_SUM_OVERFLOW");
        assertThat(number(after, "fullCallCount")).isEqualTo(2);
        assertThat(number(after, "completeTimedCalls")).isEqualTo(1);
        assertThat(stageMap(after)).isEqualTo(stageMap(before));
        assertThat(probe.getCompletedCalls()).isEqualTo(2); assertThat(probe.getReportedRecords()).isEqualTo(2);
    }

    @Test
    void missing_negative_and_overflowing_cost_stages_stay_UNKNOWN_without_changing_receipts() {
        var missing = new PdkWriteReturnProbe(new AtomicLong()::incrementAndGet, true);
        var writer = missing.writer("writer"); missing.start("measured");
        var call = writer.begin("source", table(), List.of(event(1)));
        call.callback(new WriteListResult<>(1L, 0L, 0L)); call.callbackExited(); call.returned(null);
        assertThat(cost(missing)).containsEntry("state", "UNKNOWN").containsEntry("reason", "STAGES_MISSING");
        assertThat(missing.getReportedRecords()).isEqualTo(1);
        for (boolean overflow : List.of(false, true)) {
            AtomicLong reads = new AtomicLong();
            var probe = new PdkWriteReturnProbe(() -> reads.incrementAndGet() == 1
                    ? (overflow ? Long.MIN_VALUE : 100) : (overflow ? Long.MAX_VALUE : 99), true);
            var scoped = probe.writer("writer", PdkMongoWriteScope.disabled()); probe.start("measured");
            var recorded = scoped.begin("source", table(), List.of(event(1)));
            recorded.callback(new WriteListResult<>(1L, 0L, 0L)); recorded.callbackExited(); recorded.returned(null);
            assertThat(cost(probe)).containsEntry("state", "UNKNOWN").containsEntry("reason", "CLOCK_ORDER_OR_OVERFLOW");
            assertThat(probe.getCompletedCalls()).isEqualTo(1); assertThat(probe.getReportedRecords()).isEqualTo(1);
        }
    }

    private static void awaitBlocked(Thread worker) {
        long began = System.nanoTime();
        while (worker.isAlive() && worker.getState() != Thread.State.BLOCKED
                && System.nanoTime() - began < TimeUnit.SECONDS.toNanos(2)) { Thread.yield(); }
        assertThat(worker.getState()).isEqualTo(Thread.State.BLOCKED);
    }
    @SuppressWarnings("unchecked")
    private static Map<String, Object> cost(PdkWriteReturnProbe probe) {
        return (Map<String, Object>) io.tapstate.core.common.JsonReader.parse(probe.getCostStages());
    }
    @SuppressWarnings("unchecked")
    private static Map<String, Object> stageMap(Map<String, Object> summary) { return (Map<String, Object>) summary.get("stages"); }
    private static long number(Map<?, ?> values, String key) { return ((Number) values.get(key)).longValue(); }
    private static void restoreProperty(String name, String value) {
        if (value == null) { System.clearProperty(name); } else { System.setProperty(name, value); }
    }

    private static TapTable table() {
        return new TapTable("target").add(new TapField("id", "int").primaryKeyPos(1));
    }
    private static TapRecordEvent event(int key) {
        return TapInsertRecordEvent.create().table("target").after(Map.of("id", key));
    }
    private record Frame(long sequence, long began, long lastCallbackExit, long observed,
                         int callbacks, String scope, List<Integer> keys) { }
    private record Page(int next, List<Frame> frames) { }
    private record CallbackExitFrame(long began, long lastCallbackExit, long observed) { }
    private static CallbackExitFrame decodeCallbackExit(byte[] bytes) throws Exception {
        assertThat(bytes.length).isLessThanOrEqualTo(PdkWriteReturnProbe.MAX_FRAME_BYTES);
        var in = new DataInputStream(new ByteArrayInputStream(bytes));
        assertThat(in.readInt()).isEqualTo(0x57525031);
        assertThat(in.readInt()).as("a successful writer receipt must include the V3 callback-exit bound").isEqualTo(3);
        in.readLong();
        assertThat(in.readInt()).isZero();
        assertThat(in.readInt()).isEqualTo(1);
        assertThat(in.readInt()).isEqualTo(1);
        assertThat(text(in)).isEqualTo("control");
        text(in);
        int length = in.readInt();
        assertThat(length).isPositive().isLessThanOrEqualTo(PdkWriteReturnProbe.MAX_FRAME_BYTES);
        byte[] payload = in.readNBytes(length);
        assertThat(payload).hasSize(length);
        assertThat(in.available()).isZero();
        var frame = new DataInputStream(new ByteArrayInputStream(payload));
        assertThat(frame.readLong()).isEqualTo(1);
        assertThat(frame.readInt()).isPositive();
        assertThat(frame.readInt()).isEqualTo(1);
        assertThat(frame.readInt()).isZero();
        assertThat(frame.readInt()).isEqualTo(1);
        long began = frame.readLong();
        long lastCallbackExit = frame.readLong();
        long observed = frame.readLong();
        assertThat(frame.readBoolean()).isTrue();
        assertThat(frame.readInt()).isEqualTo(1);
        assertThat(frame.readLong()).isEqualTo(1);
        assertThat(frame.readLong()).isZero();
        assertThat(frame.readLong()).isZero();
        assertThat(frame.readInt()).isZero();
        assertThat(frame.readInt()).isZero();
        text(frame); text(frame); text(frame);
        assertThat(text(frame)).isEqualTo("t1");
        assertThat(text(frame)).isEqualTo("target");
        assertThat(frame.readUnsignedByte()).isEqualTo(1);
        assertThat(text(frame)).isEqualTo("id");
        assertThat(frame.readInt()).isEqualTo(1);
        assertThat(frame.readUnsignedByte()).isEqualTo(1);
        assertThat(frame.readInt()).isEqualTo(1);
        assertThat(frame.available()).isZero();
        return new CallbackExitFrame(began, lastCallbackExit, observed);
    }
    private static Page decode(byte[] bytes) throws Exception {
        var in = new DataInputStream(new ByteArrayInputStream(bytes));
        assertThat(in.readInt()).isEqualTo(0x57525031); assertThat(in.readInt()).isEqualTo(3);
        in.readLong(); int first = in.readInt(); int next = in.readInt(); in.readInt(); text(in); text(in);
        List<Frame> frames = new ArrayList<>();
        for (int i = first; i < next; i++) {
            var frame = new DataInputStream(new ByteArrayInputStream(in.readNBytes(in.readInt())));
            long sequence = frame.readLong(); frame.readInt();
            frame.readInt(); frame.readInt(); frame.readInt();
            long began = frame.readLong(); long lastCallbackExit = frame.readLong(); long observed = frame.readLong(); frame.readBoolean();
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
            frames.add(new Frame(sequence, began, lastCallbackExit, observed, callbacks, scope, List.copyOf(keys)));
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

    private static void onOtherThread(Runnable action) throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try { action.run(); } catch (Throwable thrown) { failure.set(thrown); }
        }, "callback-exit-control");
        worker.setDaemon(true); worker.start(); worker.join(TimeUnit.SECONDS.toMillis(5));
        if (worker.isAlive()) { worker.interrupt(); worker.join(TimeUnit.SECONDS.toMillis(5)); }
        assertThat(worker.isAlive()).as("the owned callback control must finish").isFalse();
        assertThat(failure.get()).isNull();
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
