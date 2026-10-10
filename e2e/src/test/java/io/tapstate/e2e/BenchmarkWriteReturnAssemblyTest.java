package io.tapstate.e2e;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkWriteReturnAssemblyTest {
    private static final String RECORDED = "RECORDED_SCOPE_UNQUALIFIED";
    private static final String ACK = "state=ORDINARY_ACKNOWLEDGED;reason=PINNED_RUNTIME_SCOPE;concern=w:1,j:DEFAULT,timeoutMs:DEFAULT";

    @Test void a_1024_row_call_split_across_pages_is_counted_once_and_keeps_every_key() throws Exception {
        var assembly = new BenchmarkWriteReturnAssembly("window");
        assembly.add(page(1, 0, 1, 2, "window", RECORDED, multipart(1024, 0)));
        assembly.add(page(1, 1, 2, 2, "window", RECORDED, multipart(1024, 1)));
        var calls = assembly.finish(1, 1024);
        assertThat(calls).hasSize(1);
        var call = calls.getFirst();
        assertThat(call.totalRows()).isEqualTo(1024); assertThat(call.inserted()).isEqualTo(1024);
        assertThat(call.callbackCount()).isEqualTo(1); assertThat(call.beganNanos()).isEqualTo(100);
        assertThat(call.observedNanos()).isEqualTo(200); assertThat(call.scope()).isEqualTo(ACK);
        assertThat(call.rows().stream().map(row -> row.keys().getFirst()).toList())
                .containsExactlyElementsOf(IntStream.range(0, 1024).boxed().toList());
        assertThatThrownBy(() -> calls.clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> call.rows().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> call.rows().getFirst().keys().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void reverse_completion_sequences_and_composite_keys_are_not_sorted_or_deduplicated() throws Exception {
        var second = new Fixture(); second.sequence = 2; second.writer = 2; second.writerIdentity = "writer-two";
        second.keyFields = List.of("id", "item_index"); second.rows = List.of(new Row(1, List.of(9, 3)));
        var first = new Fixture(); first.writerIdentity = "writer-one";
        first.keyFields = List.of("id", "item_index"); first.rows = List.of(new Row(2, List.of(9, 3)));
        var assembly = new BenchmarkWriteReturnAssembly("window");
        assembly.add(page(1, 0, 2, 2, "window", RECORDED, second, first));
        var calls = assembly.finish(2, 2);
        assertThat(calls).extracting(BenchmarkWriteReturnAssembly.FullCall::sequence).containsExactly(2L, 1L);
        assertThat(calls.getFirst().keyFields()).containsExactly("id", "item_index");
        assertThat(calls.stream().flatMap(call -> call.rows().stream()).toList()).containsExactly(
                new BenchmarkWriteReturnLedger.Row(1, List.of(9, 3)), new BenchmarkWriteReturnLedger.Row(2, List.of(9, 3)));
    }

    @Test void growing_frame_totals_are_supported_but_a_later_terminal_count_cannot_shrink() throws Exception {
        var first = new Fixture(); var second = new Fixture(); second.sequence = 2;
        var assembly = new BenchmarkWriteReturnAssembly("window");
        assembly.add(page(1, 0, 1, 1, "window", RECORDED, first));
        assembly.add(page(1, 1, 2, 2, "window", RECORDED, second));
        assertThat(assembly.finish(2, 2)).hasSize(2);
        var shrinking = new BenchmarkWriteReturnAssembly("window");
        shrinking.add(page(1, 0, 1, 3, "window", RECORDED, first));
        assertThatThrownBy(() -> shrinking.add(page(1, 1, 2, 2, "window", RECORDED, second)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("moved backward");
    }

    @Test void gaps_replays_and_duplicate_calls_cannot_be_repaired_by_changing_expected_totals() throws Exception {
        var first = new Fixture(); var second = new Fixture(); second.sequence = 2;
        var gap = new BenchmarkWriteReturnAssembly("window");
        gap.add(page(1, 0, 1, 3, "window", RECORDED, first));
        assertThatThrownBy(() -> gap.add(page(1, 2, 3, 3, "window", RECORDED, second)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("gap or replay");
        assertThatThrownBy(() -> gap.finish(1, 1)).isInstanceOf(AssertionError.class).hasMessageContaining("refused");
        var replay = new BenchmarkWriteReturnAssembly("window");
        byte[] original = page(1, 0, 1, 2, "window", RECORDED, first); replay.add(original);
        assertThatThrownBy(() -> replay.add(original)).isInstanceOf(AssertionError.class).hasMessageContaining("gap or replay");
        var duplicate = new BenchmarkWriteReturnAssembly("window");
        duplicate.add(original);
        assertThatThrownBy(() -> duplicate.add(page(1, 1, 2, 2, "window", RECORDED, first)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("piece is duplicate");
    }

    @Test void a_duplicate_part_at_the_next_cursor_still_cannot_replace_the_missing_slice() throws Exception {
        var assembly = new BenchmarkWriteReturnAssembly("window");
        assembly.add(page(1, 0, 1, 3, "window", RECORDED, multipart(1024, 0)));
        assertThatThrownBy(() -> assembly.add(page(1, 1, 2, 3, "window", RECORDED, multipart(1024, 0))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("piece is duplicate");
    }

    @Test void capture_epoch_window_and_writer_identity_cannot_change_across_pages() throws Exception {
        var first = new Fixture(); var second = new Fixture(); second.sequence = 2;
        for (boolean epochChange : List.of(false, true)) {
            var assembly = new BenchmarkWriteReturnAssembly("window");
            assembly.add(page(1, 0, 1, 2, "window", RECORDED, first));
            assertThatThrownBy(() -> assembly.add(page(epochChange ? 2 : 1, 1, 2, 2,
                    epochChange ? "window" : "foreign", RECORDED, second)))
                    .isInstanceOf(AssertionError.class).hasMessageContaining(epochChange ? "epoch changed" : "another capture window");
        }
        second.writerIdentity = "other-writer";
        var assembly = new BenchmarkWriteReturnAssembly("window");
        assembly.add(page(1, 0, 1, 2, "window", RECORDED, first));
        assertThatThrownBy(() -> assembly.add(page(1, 1, 2, 2, "window", RECORDED, second)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("writer identity changed");
    }

    @Test void full_call_metadata_must_match_even_when_its_pieces_live_on_different_pages() throws Exception {
        List<Consumer<Fixture>> changes = List.of(
                call -> call.writer = 2,
                call -> call.began = 101,
                call -> call.observed = 201,
                call -> call.callbacks = 2,
                call -> { call.inserted = 1023; call.modified = 1; },
                call -> call.scope = "state=ORDINARY_ACKNOWLEDGED;reason=PINNED_RUNTIME_SCOPE;concern=w:MAJORITY,j:DEFAULT,timeoutMs:DEFAULT",
                call -> call.target = "other-target",
                call -> call.stream = "other-source",
                call -> call.keyFields = List.of("other-key"));
        for (var change : changes) {
            var assembly = new BenchmarkWriteReturnAssembly("window");
            var last = multipart(1024, 1); change.accept(last);
            assembly.add(page(1, 0, 1, 2, "window", RECORDED, multipart(1024, 0)));
            assertThatThrownBy(() -> assembly.add(page(1, 1, 2, 2, "window", RECORDED, last)))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("metadata changed");
        }
    }

    @Test void missing_final_pages_and_terminal_counter_contradictions_do_not_produce_full_calls() throws Exception {
        var missing = new BenchmarkWriteReturnAssembly("window");
        missing.add(page(1, 0, 1, 2, "window", RECORDED, multipart(1024, 0)));
        assertThatThrownBy(() -> missing.finish(1, 1024)).isInstanceOf(AssertionError.class).hasMessageContaining("missing");
        for (boolean wrongCalls : List.of(false, true)) {
            var assembly = new BenchmarkWriteReturnAssembly("window");
            assembly.add(page(1, 0, 1, 1, "window", RECORDED, new Fixture()));
            assertThatThrownBy(() -> assembly.finish(wrongCalls ? 2 : 1, wrongCalls ? 1 : 2))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("totals disagree");
        }
        assertThatThrownBy(() -> new BenchmarkWriteReturnAssembly("window").finish(0, 0))
                .isInstanceOf(AssertionError.class).hasMessageContaining("no actual receipt page");
    }

    @Test void an_unknown_header_cannot_be_erased_by_a_later_clean_page_or_qualified_marker() throws Exception {
        var assembly = new BenchmarkWriteReturnAssembly("window");
        assembly.add(page(1, 0, 1, 2, "window", "UNKNOWN:LATE_CALLBACK", multipart(1024, 0)));
        assembly.add(page(1, 1, 2, 2, "window", RECORDED, multipart(1024, 1)));
        assertThatThrownBy(() -> assembly.finish(1, 1024)).isInstanceOf(AssertionError.class).hasMessageContaining("state is unknown");
        var marker = new BenchmarkWriteReturnAssembly("window");
        assertThatThrownBy(() -> marker.add(page(1, 0, 1, 1, "window", "QUALIFIED", new Fixture())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("state is unsupported");
    }

    @Test void failed_partial_and_error_receipts_remain_unusable_even_with_matching_reported_totals() throws Exception {
        var failed = new Fixture(); failed.normal = 0; failed.failureType = "java.lang.IllegalStateException";
        var partial = new Fixture(); partial.inserted = 0;
        var error = new Fixture(); error.errors = 1; error.errorDetails = List.of(new Error(0, "java.lang.IllegalStateException"));
        for (var fixture : List.of(failed, partial, error)) {
            var assembly = new BenchmarkWriteReturnAssembly("window");
            assembly.add(page(1, 0, 1, 1, "window", "UNKNOWN:FAILED_PARTIAL_OR_MISSING_CALLBACK", fixture));
            assertThatThrownBy(() -> assembly.finish(1, fixture.inserted))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("state is unknown");
        }
    }

    @Test void scope_markers_or_unacknowledged_concern_cannot_supply_an_ordinary_return_declaration() throws Exception {
        for (String scope : List.of("UNKNOWN", "state=QUALIFIED;reason=PINNED_RUNTIME_SCOPE;concern=w:1,j:DEFAULT,timeoutMs:DEFAULT",
                "state=ORDINARY_ACKNOWLEDGED;reason=CALLER_MARKER;concern=w:1,j:DEFAULT,timeoutMs:DEFAULT",
                "state=ORDINARY_ACKNOWLEDGED;reason=PINNED_RUNTIME_SCOPE;concern=w:0,j:DEFAULT,timeoutMs:DEFAULT",
                "state=ORDINARY_ACKNOWLEDGED;reason=PINNED_RUNTIME_SCOPE;concern=w:2147483648,j:DEFAULT,timeoutMs:DEFAULT")) {
            var fixture = new Fixture(); fixture.scope = scope;
            var assembly = new BenchmarkWriteReturnAssembly("window");
            assembly.add(page(1, 0, 1, 1, "window", RECORDED, fixture));
            assertThatThrownBy(() -> assembly.finish(1, 1)).isInstanceOf(AssertionError.class);
        }
        var tagged = new Fixture();
        tagged.scope = "state=ORDINARY_ACKNOWLEDGED;reason=PINNED_RUNTIME_SCOPE;concern=w:TAG_SHA256_" + "a".repeat(64)
                + ",j:true,timeoutMs:100";
        var assembly = new BenchmarkWriteReturnAssembly("window");
        assembly.add(page(1, 0, 1, 1, "window", RECORDED, tagged));
        assertThat(assembly.finish(1, 1).getFirst().scope()).isEqualTo(tagged.scope);
    }

    @Test void oversized_pages_frame_rosters_and_raw_receipt_growth_are_rejected_before_closure() throws Exception {
        assertThatThrownBy(() -> new BenchmarkWriteReturnAssembly("window").add(new byte[65_537]))
                .isInstanceOf(AssertionError.class).hasMessageContaining("page exceeds");
        assertThatThrownBy(() -> new BenchmarkWriteReturnAssembly("window").add(page(1, 0, 1, 513,
                "window", RECORDED, new Fixture()))).isInstanceOf(AssertionError.class);
        var assembly = new BenchmarkWriteReturnAssembly("window");
        boolean refused = false;
        for (int index = 0; index < 512; index++) {
            var fixture = new Fixture(); fixture.sequence = index + 1L;
            fixture.writerIdentity = "w".repeat(512); fixture.stream = "s".repeat(512); fixture.target = "t".repeat(512);
            fixture.keyFields = List.of("a".repeat(128), "b".repeat(128)); fixture.inserted = 256;
            fixture.rows = IntStream.range(0, 256).mapToObj(key -> new Row(1, List.of(key, key))).toList();
            try { assembly.add(page(1, index, index + 1, 512, "window", RECORDED, fixture)); }
            catch (AssertionError failure) {
                assertThat(failure).hasMessageContaining("raw ledger exceeds"); refused = true; break;
            }
        }
        assertThat(refused).as("bounded received pages include their actual headers").isTrue();
    }

    @Test void the_stateful_192000_row_aggregate_is_not_clipped_by_a_produce_byte_limit() throws Exception {
        var assembly = new BenchmarkWriteReturnAssembly("window");
        for (int call = 0; call < 187; call++) {
            var first = multipart(1024, 0); var second = multipart(1024, 1);
            first.sequence = second.sequence = call + 1L;
            int start = call * 1024;
            first.rows = IntStream.range(start, start + 512).mapToObj(key -> new Row(1, List.of(key))).toList();
            second.rows = IntStream.range(start + 512, start + 1024).mapToObj(key -> new Row(1, List.of(key))).toList();
            assembly.add(page(1, call * 2, call * 2 + 2, 375, "window", RECORDED, first, second));
        }
        var last = new Fixture(); last.sequence = 188; last.inserted = 512;
        last.rows = IntStream.range(191_488, 192_000).mapToObj(key -> new Row(1, List.of(key))).toList();
        assembly.add(page(1, 374, 375, 375, "window", RECORDED, last));
        var calls = assembly.finish(188, 192_000);
        assertThat(calls).hasSize(188);
        assertThat(calls.stream().mapToInt(call -> call.rows().size()).sum()).isEqualTo(192_000);
        assertThat(calls.getFirst().rows().getFirst().keys()).containsExactly(0);
        assertThat(calls.getLast().rows().getLast().keys()).containsExactly(191_999);
    }

    @Test void zero_capture_closure_still_needs_a_real_page_and_cannot_be_reopened() throws Exception {
        var assembly = new BenchmarkWriteReturnAssembly("window");
        assembly.add(page(1, 0, 0, 0, "window", RECORDED));
        assertThat(assembly.finish(0, 0)).isEmpty();
        assertThatThrownBy(() -> assembly.add(page(1, 0, 0, 0, "window", RECORDED)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("already closed");
        assertThatThrownBy(() -> assembly.finish(0, 0)).isInstanceOf(AssertionError.class).hasMessageContaining("already closed");
        assertThatThrownBy(() -> new BenchmarkWriteReturnAssembly(" ")).isInstanceOf(AssertionError.class);
    }

    private static Fixture multipart(int total, int part) {
        var fixture = new Fixture(); fixture.totalRows = total; fixture.partIndex = part; fixture.inserted = total;
        fixture.rows = IntStream.range(part * 512, Math.min(total, (part + 1) * 512))
                .mapToObj(key -> new Row(1, List.of(key))).toList();
        return fixture;
    }
    private static byte[] page(long epoch, int cursor, int next, int total, String window, String state,
                               Fixture... calls) throws Exception {
        var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes);
        out.writeInt(0x57525031); out.writeInt(2); out.writeLong(epoch);
        out.writeInt(cursor); out.writeInt(next); out.writeInt(total); text(out, window); text(out, state);
        for (Fixture call : calls) { byte[] payload = call.bytes(); out.writeInt(payload.length); out.write(payload); }
        return bytes.toByteArray();
    }
    private static void text(DataOutputStream out, String value) throws Exception {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8); out.writeShort(bytes.length); out.write(bytes);
    }
    private record Row(int kind, List<Integer> keys) { }
    private record Error(int ordinal, String type) { }
    private static final class Fixture {
        long sequence = 1, began = 100, observed = 200, inserted = 1, modified, removed;
        int writer = 1, totalRows = -1, partIndex, normal = 1, callbacks = 1, errors;
        String scope = ACK, failureType = "", writerIdentity = "writer", stream = "source", target = "target";
        List<String> keyFields = List.of("id");
        List<Row> rows = List.of(new Row(1, List.of(7)));
        List<Error> errorDetails = List.of();
        byte[] bytes() throws Exception {
            var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes);
            int total = totalRows == -1 ? rows.size() : totalRows;
            out.writeLong(sequence); out.writeInt(writer); out.writeInt(total); out.writeInt(partIndex); out.writeInt((total + 511) / 512);
            out.writeLong(began); out.writeLong(observed); out.writeByte(normal); out.writeInt(callbacks);
            out.writeLong(inserted); out.writeLong(modified); out.writeLong(removed);
            out.writeInt(errors); out.writeInt(errorDetails.size());
            for (Error error : errorDetails) { out.writeInt(error.ordinal()); text(out, error.type()); }
            text(out, failureType); text(out, scope); text(out, writerIdentity); text(out, stream); text(out, target);
            out.writeByte(keyFields.size()); for (String field : keyFields) { text(out, field); }
            out.writeInt(rows.size());
            for (Row row : rows) { out.writeByte(row.kind()); for (int key : row.keys()) { out.writeInt(key); } }
            return bytes.toByteArray();
        }
    }
}
