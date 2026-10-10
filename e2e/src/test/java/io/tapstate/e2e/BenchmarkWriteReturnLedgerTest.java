package io.tapstate.e2e;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkWriteReturnLedgerTest {
    private static final String RECORDED = "RECORDED_SCOPE_UNQUALIFIED";
    private static final String UNKNOWN = "UNKNOWN:FAILED_PARTIAL_OR_MISSING_CALLBACK";

    @Test void completion_order_and_composite_keys_survive_without_sorting_or_rounding() throws Exception {
        var first = new Fixture();
        first.sequence = 2; first.writer = 2; first.began = -10; first.observed = 10;
        first.callbacks = 2; first.inserted = 1; first.modified = 1;
        first.keyFields = List.of("account_id", "line_id");
        first.rows = List.of(new Row(1, List.of(5, 6)), new Row(2, List.of(-7, Integer.MAX_VALUE)));
        first.scope = "state=ORDINARY_ACKNOWLEDGED;reason=PINNED_RUNTIME_SCOPE;concern=w:MAJORITY,j:DEFAULT,timeoutMs:DEFAULT";
        var page = BenchmarkWriteReturnLedger.decode(page(4, 3, 5, 8, "window", RECORDED,
                List.of(first.bytes(), new Fixture().bytes())));
        assertThat(page.epoch()).isEqualTo(4);
        assertThat(page.cursor()).isEqualTo(3); assertThat(page.nextCursor()).isEqualTo(5);
        assertThat(page.totalFrames()).isEqualTo(8);
        assertThat(page.calls()).extracting(BenchmarkWriteReturnLedger.Call::sequence).containsExactly(2L, 1L);
        var call = page.calls().getFirst();
        assertThat(call.writer()).isEqualTo(2); assertThat(call.beganNanos()).isEqualTo(-10);
        assertThat(call.observedNanos()).isEqualTo(10); assertThat(call.callbackCount()).isEqualTo(2);
        assertThat(call.inserted()).isEqualTo(1); assertThat(call.modified()).isEqualTo(1);
        assertThat(call.removed()).isZero(); assertThat(call.errors()).isZero();
        assertThat(call.returnedNormally()).isTrue(); assertThat(call.failureType()).isEmpty();
        assertThat(call.scope()).isEqualTo(first.scope);
        assertThat(call.writerIdentity()).isEqualTo("pipeline/sink");
        assertThat(call.stream()).isEqualTo("source"); assertThat(call.target()).isEqualTo("target");
        assertThat(call.keyFields()).containsExactly("account_id", "line_id");
        assertThat(call.rows()).containsExactly(new BenchmarkWriteReturnLedger.Row(1, List.of(5, 6)),
                new BenchmarkWriteReturnLedger.Row(2, List.of(-7, Integer.MAX_VALUE)));
        assertThatThrownBy(() -> page.calls().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> call.rows().getFirst().keys().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void partial_and_failed_calls_retain_detailed_errors_as_diagnostics() throws Exception {
        var partial = new Fixture();
        partial.rows = List.of(new Row(1, List.of(7)), new Row(3, List.of(8)));
        partial.errors = 2;
        partial.errorDetails = List.of(new Error(1, "java.lang.IllegalStateException"), new Error(-1, "UNKNOWN"));
        var failed = new Fixture(); failed.sequence = 2; failed.normal = 0;
        failed.failureType = "java.lang.IllegalArgumentException";
        var page = BenchmarkWriteReturnLedger.decode(page(1, 0, 2, 2, "window", UNKNOWN,
                List.of(partial.bytes(), failed.bytes())));
        assertThat(page.state()).isEqualTo(UNKNOWN);
        assertThat(page.calls().getFirst().errorDetails()).containsExactly(
                new BenchmarkWriteReturnLedger.ErrorDetail(1, "java.lang.IllegalStateException"),
                new BenchmarkWriteReturnLedger.ErrorDetail(-1, "UNKNOWN"));
        assertThat(page.calls().getFirst().returnedNormally()).isTrue();
        assertThat(page.calls().getLast().returnedNormally()).isFalse();
        assertThat(page.calls().getLast().failureType()).isEqualTo(failed.failureType);
        var missing = new Fixture(); missing.callbacks = 0; missing.inserted = 0;
        assertThat(decode(missing, UNKNOWN).calls().getFirst().callbackCount()).isZero();
    }

    @Test void every_truncated_prefix_and_extra_page_or_call_bytes_are_rejected() throws Exception {
        byte[] valid = page(new Fixture().bytes());
        for (int length = 0; length < valid.length; length++) {
            byte[] truncated = Arrays.copyOf(valid, length);
            assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(truncated)).isInstanceOf(AssertionError.class);
        }
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(Arrays.copyOf(valid, valid.length + 1)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("page has extra bytes");
        byte[] call = new Fixture().bytes();
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(page(Arrays.copyOf(call, call.length + 1))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("call has extra bytes");
    }

    @Test void wire_magic_version_boolean_and_declared_lengths_must_be_exact() throws Exception {
        byte[] valid = page(new Fixture().bytes());
        byte[] magic = valid.clone(); magic[0] ^= 1;
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(magic))
                .isInstanceOf(AssertionError.class).hasMessageContaining("magic");
        byte[] version = valid.clone(); version[7] = 1;
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(version))
                .isInstanceOf(AssertionError.class).hasMessageContaining("version");
        var flag = new Fixture(); flag.normal = 2;
        assertThatThrownBy(() -> decode(flag, UNKNOWN)).isInstanceOf(AssertionError.class).hasMessageContaining("boolean");
        for (int length : List.of(-1, 0, Integer.MAX_VALUE)) {
            byte[] invalid = valid.clone();
            int offset = 28 + 2 + "window".length() + 2 + RECORDED.length();
            ByteBuffer.wrap(invalid).putInt(offset, length);
            assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(invalid))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("call length");
        }
    }

    @Test void completion_cursors_require_progress_and_never_allow_duplicate_attempts() throws Exception {
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(page(1, 0, 0, 1, "window", RECORDED, List.of())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("no completion cursor progress");
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(page(1, 2, 1, 2, "window", RECORDED, List.of())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("cursor");
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(page(1, 513, 513, 513, "window", RECORDED, List.of())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("cursor");
        byte[] call = new Fixture().bytes();
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(page(1, 0, 2, 2, "window", RECORDED, List.of(call, call))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("repeats a call part");
        assertThat(BenchmarkWriteReturnLedger.decode(page(1, 2, 2, 2, "window", RECORDED, List.of())).calls()).isEmpty();
    }

    @Test void UTF8_is_strict_and_text_bounds_count_encoded_bytes() throws Exception {
        var fixture = new Fixture(); fixture.target = "caf\u00e9";
        assertThat(decode(fixture, RECORDED).calls().getFirst().target()).isEqualTo("caf\u00e9");
        byte[] invalid = page(new Fixture().bytes()); invalid[30] = (byte) 0xc0;
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(invalid))
                .isInstanceOf(AssertionError.class).hasMessageContaining("not valid UTF-8");
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(page(1, 0, 1, 1,
                "\u00e9".repeat(257), RECORDED, List.of(new Fixture().bytes()))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("text length");
        assertThat(BenchmarkWriteReturnLedger.decode(page(1, 0, 1, 1, "w".repeat(512), RECORDED,
                List.of(new Fixture().bytes()))).window()).hasSize(512);
    }

    @Test void the_largest_admissible_payload_pages_but_one_more_byte_is_rejected() throws Exception {
        var large = new Fixture(); large.writerIdentity = "w".repeat(512); large.stream = "s".repeat(303);
        large.rows = IntStream.range(0, 512).mapToObj(key -> new Row(1, List.of(key))).toList();
        large.inserted = 12; large.errors = 500;
        large.errorDetails = IntStream.range(0, 500)
                .mapToObj(ordinal -> new Error(ordinal, "pkg." + "E".repeat(110))).toList();
        assertThat(large.bytes()).hasSize(63_484);
        assertThat(decode(large, UNKNOWN).calls().getFirst().rows()).hasSize(512);
        large.target = "targetx";
        assertThat(large.bytes()).hasSize(63_485);
        assertThatThrownBy(() -> decode(large, UNKNOWN))
                .isInstanceOf(AssertionError.class).hasMessageContaining("call length");
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(new byte[65_537]))
                .isInstanceOf(AssertionError.class).hasMessageContaining("byte bound");
    }

    @Test void malformed_identity_key_shapes_and_row_kinds_cannot_enter_the_ledger() throws Exception {
        var duplicateFields = new Fixture(); duplicateFields.keyFields = List.of("id", "id");
        duplicateFields.rows = List.of(new Row(1, List.of(7, 7)));
        assertThatThrownBy(() -> decode(duplicateFields, RECORDED)).isInstanceOf(AssertionError.class).hasMessageContaining("duplicate");
        var tooManyFields = new Fixture(); tooManyFields.keyFields = List.of("a", "b", "c");
        assertThatThrownBy(() -> decode(tooManyFields, RECORDED)).isInstanceOf(AssertionError.class).hasMessageContaining("key field count");
        var rows = new Fixture(); rows.rows = IntStream.range(0, 513).mapToObj(key -> new Row(1, List.of(key))).toList();
        assertThatThrownBy(() -> decode(rows, UNKNOWN)).isInstanceOf(AssertionError.class).hasMessageContaining("row roster");
        var kind = new Fixture(); kind.rows = List.of(new Row(4, List.of(7)));
        assertThatThrownBy(() -> decode(kind, RECORDED)).isInstanceOf(AssertionError.class).hasMessageContaining("row kind");
        var identity = new Fixture(); identity.writer = 129;
        assertThatThrownBy(() -> decode(identity, RECORDED)).isInstanceOf(AssertionError.class).hasMessageContaining("call identity");
        identity.writer = 1; identity.sequence = 0;
        assertThatThrownBy(() -> decode(identity, RECORDED)).isInstanceOf(AssertionError.class).hasMessageContaining("call identity");
    }

    @Test void repeated_row_keys_are_preserved_for_the_independent_oracle_to_decide() throws Exception {
        var fixture = new Fixture(); fixture.inserted = 2;
        fixture.rows = List.of(new Row(1, List.of(7)), new Row(1, List.of(7)));
        assertThat(decode(fixture, RECORDED).calls().getFirst().rows())
                .containsExactly(new BenchmarkWriteReturnLedger.Row(1, List.of(7)), new BenchmarkWriteReturnLedger.Row(1, List.of(7)));
    }

    @Test void counter_failure_and_unknown_state_consistency_are_enforced() throws Exception {
        var negative = new Fixture(); negative.inserted = -1;
        assertThatThrownBy(() -> decode(negative, UNKNOWN)).isInstanceOf(AssertionError.class).hasMessageContaining("negative counters");
        var badFailure = new Fixture(); badFailure.failureType = "java.lang.IllegalStateException";
        assertThatThrownBy(() -> decode(badFailure, UNKNOWN)).isInstanceOf(AssertionError.class).hasMessageContaining("failure type disagree");
        badFailure.normal = 0; badFailure.failureType = "";
        assertThatThrownBy(() -> decode(badFailure, UNKNOWN)).isInstanceOf(AssertionError.class).hasMessageContaining("failure type disagree");
        var partial = new Fixture(); partial.inserted = 0;
        assertThatThrownBy(() -> decode(partial, RECORDED)).isInstanceOf(AssertionError.class).hasMessageContaining("unknown page state");
        var absentCallback = new Fixture(); absentCallback.callbacks = 0;
        assertThatThrownBy(() -> decode(absentCallback, UNKNOWN)).isInstanceOf(AssertionError.class).hasMessageContaining("no callback");
        var overflow = new Fixture(); overflow.inserted = Long.MAX_VALUE; overflow.modified = 1;
        assertThatThrownBy(() -> decode(overflow, UNKNOWN)).isInstanceOf(AssertionError.class).hasMessageContaining("overflow");
        overflow.inserted = 1; overflow.modified = 0; overflow.began = Long.MIN_VALUE; overflow.observed = Long.MAX_VALUE;
        assertThatThrownBy(() -> decode(overflow, UNKNOWN)).isInstanceOf(AssertionError.class).hasMessageContaining("overflow");
    }

    @Test void detailed_errors_are_bounded_and_must_name_a_row_or_explicit_unknown_identity() throws Exception {
        var fixture = new Fixture(); fixture.errors = 1;
        fixture.errorDetails = List.of(new Error(1, "java.lang.IllegalStateException"));
        assertThatThrownBy(() -> decode(fixture, UNKNOWN)).isInstanceOf(AssertionError.class).hasMessageContaining("row ordinal");
        fixture.errorDetails = List.of(new Error(-2, "java.lang.IllegalStateException"));
        assertThatThrownBy(() -> decode(fixture, UNKNOWN)).isInstanceOf(AssertionError.class).hasMessageContaining("row ordinal");
        fixture.errors = 513;
        fixture.errorDetails = IntStream.range(0, 513).mapToObj(ordinal -> new Error(0, "UNKNOWN")).toList();
        assertThatThrownBy(() -> decode(fixture, UNKNOWN)).isInstanceOf(AssertionError.class).hasMessageContaining("detail roster");
    }

    @Test void scope_remains_bounded_raw_ASCII_evidence_and_initial_pages_remain_diagnostic() throws Exception {
        for (String malformed : List.of("state=X;state=Y;concern=UNKNOWN", "state=X;reason=R;concern=",
                "state=X;reason=R;concern=\u00e9", "unqualified")) {
            var fixture = new Fixture(); fixture.scope = malformed;
            assertThatThrownBy(() -> decode(fixture, RECORDED)).isInstanceOf(AssertionError.class).hasMessageContaining("scope");
        }
        assertThat(BenchmarkWriteReturnLedger.decode(page(0, 0, 0, 0, "", "IDLE", List.of())).calls()).isEmpty();
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(page(0, 0, 1, 1, "window", RECORDED,
                List.of(new Fixture().bytes())))).isInstanceOf(AssertionError.class).hasMessageContaining("initial page");
    }

    @Test void native_1024_and_1023_calls_keep_exact_ordered_slices_and_full_call_counts() throws Exception {
        for (int total : List.of(1024, 1023)) {
            var first = multipart(total, 0); var second = multipart(total, 1);
            var decoded = BenchmarkWriteReturnLedger.decode(page(1, 0, 2, 2, "window", RECORDED,
                    List.of(first.bytes(), second.bytes())));
            assertThat(decoded.calls()).extracting(BenchmarkWriteReturnLedger.Call::sequence).containsExactly(1L, 1L);
            assertThat(decoded.calls()).extracting(BenchmarkWriteReturnLedger.Call::partIndex).containsExactly(0, 1);
            assertThat(decoded.calls()).allSatisfy(call -> {
                assertThat(call.totalRows()).isEqualTo(total); assertThat(call.partCount()).isEqualTo(2);
                assertThat(call.inserted()).isEqualTo(total); assertThat(call.callbackCount()).isEqualTo(1);
            });
            assertThat(decoded.calls().getFirst().rows()).hasSize(512);
            assertThat(decoded.calls().getLast().rows()).hasSize(total - 512);
            assertThat(decoded.calls().stream().flatMap(call -> call.rows().stream())
                    .map(row -> row.keys().getFirst()).toList())
                    .containsExactlyElementsOf(IntStream.range(0, total).boxed().toList());
        }
    }

    @Test void a_real_page_boundary_may_split_a_call_but_missing_ledger_edges_cannot() throws Exception {
        var first = multipart(1024, 0); var second = multipart(1024, 1);
        assertThat(BenchmarkWriteReturnLedger.decode(page(1, 0, 1, 2, "window", RECORDED,
                List.of(first.bytes()))).calls().getFirst().partIndex()).isZero();
        assertThat(BenchmarkWriteReturnLedger.decode(page(1, 1, 2, 2, "window", RECORDED,
                List.of(second.bytes()))).calls().getFirst().partIndex()).isEqualTo(1);
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(page(1, 0, 1, 1, "window", RECORDED,
                List.of(first.bytes())))).isInstanceOf(AssertionError.class).hasMessageContaining("final call part is missing");
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(page(1, 0, 1, 1, "window", RECORDED,
                List.of(second.bytes())))).isInstanceOf(AssertionError.class).hasMessageContaining("first call part is missing");
        var other = new Fixture(); other.sequence = 2;
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(page(1, 0, 2, 2, "window", RECORDED,
                List.of(first.bytes(), other.bytes())))).isInstanceOf(AssertionError.class).hasMessageContaining("interior call part is missing");
    }

    @Test void duplicate_or_noncontiguous_pieces_cannot_replace_a_missing_part() throws Exception {
        var first = multipart(1024, 0); var second = multipart(1024, 1);
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(page(1, 0, 2, 2, "window", RECORDED,
                List.of(first.bytes(), first.bytes())))).isInstanceOf(AssertionError.class).hasMessageContaining("repeats a call part");
        var other = new Fixture(); other.sequence = 2;
        assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(page(1, 1, 4, 4, "window", RECORDED,
                List.of(second.bytes(), other.bytes(), first.bytes()))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("not contiguous");
    }

    @Test void repeated_metadata_must_match_instead_of_selecting_the_more_favorable_piece() throws Exception {
        List<Consumer<Fixture>> changes = List.of(
                call -> call.writer = 2,
                call -> call.began = 101,
                call -> call.observed = 201,
                call -> call.callbacks = 2,
                call -> { call.inserted = 1023; call.modified = 1; },
                call -> { call.normal = 0; call.failureType = "java.lang.IllegalStateException"; },
                call -> { call.errors = 1; call.errorDetails = List.of(new Error(1023, "UNKNOWN")); },
                call -> call.scope = "state=UNKNOWN;reason=SCOPE_READ_FAILED;concern=UNKNOWN",
                call -> call.writerIdentity = "other-writer",
                call -> call.stream = "other-source",
                call -> call.target = "other-target",
                call -> call.keyFields = List.of("other-key"),
                call -> { call.totalRows = 1023; call.inserted = 1023; call.rows = multipart(1023, 1).rows; });
        for (var change : changes) {
            var first = multipart(1024, 0); var second = multipart(1024, 1); change.accept(second);
            assertThatThrownBy(() -> BenchmarkWriteReturnLedger.decode(page(1, 0, 2, 2, "window", UNKNOWN,
                    List.of(first.bytes(), second.bytes()))))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("mismatched metadata");
        }
    }

    @Test void total_rows_part_roster_and_exact_slice_lengths_are_checked() throws Exception {
        for (int total : List.of(0, 1025)) {
            var invalid = new Fixture(); invalid.totalRows = total;
            assertThatThrownBy(() -> decode(invalid, UNKNOWN)).isInstanceOf(AssertionError.class).hasMessageContaining("part metadata");
        }
        var missing = multipart(1024, 0); missing.partCount = 1;
        assertThatThrownBy(() -> decode(missing, UNKNOWN)).isInstanceOf(AssertionError.class).hasMessageContaining("part metadata");
        var index = multipart(1024, 1); index.partIndex = 2;
        assertThatThrownBy(() -> decode(index, UNKNOWN)).isInstanceOf(AssertionError.class).hasMessageContaining("part metadata");
        var wrongSlice = multipart(1024, 1); wrongSlice.totalRows = 1023;
        assertThatThrownBy(() -> decode(wrongSlice, UNKNOWN)).isInstanceOf(AssertionError.class).hasMessageContaining("declared slice");
    }

    @Test void error_ordinals_and_repeated_counters_belong_to_the_whole_native_call() throws Exception {
        var first = multipart(1024, 0); var second = multipart(1024, 1);
        for (var fixture : List.of(first, second)) {
            fixture.inserted = 1023; fixture.errors = 1;
            fixture.errorDetails = List.of(new Error(1023, "java.lang.IllegalStateException"));
        }
        var decoded = BenchmarkWriteReturnLedger.decode(page(1, 0, 2, 2, "window", UNKNOWN,
                List.of(first.bytes(), second.bytes())));
        assertThat(decoded.calls()).allSatisfy(call -> assertThat(call.errorDetails()).containsExactly(
                new BenchmarkWriteReturnLedger.ErrorDetail(1023, "java.lang.IllegalStateException")));
        // A diagnostic counter can be invalid for delivery while remaining an exact retained value.
        // Repeating that one call's metadata must not overflow a fictitious twice-counted page total.
        first.inserted = Long.MAX_VALUE; second.inserted = Long.MAX_VALUE;
        assertThat(BenchmarkWriteReturnLedger.decode(page(1, 0, 2, 2, "window", UNKNOWN,
                List.of(first.bytes(), second.bytes()))).calls()).hasSize(2);
    }

    private static Fixture multipart(int totalRows, int part) {
        var fixture = new Fixture(); fixture.totalRows = totalRows; fixture.partIndex = part;
        fixture.inserted = totalRows;
        fixture.rows = IntStream.range(part * 512, Math.min(totalRows, (part + 1) * 512))
                .mapToObj(key -> new Row(1, List.of(key))).toList();
        return fixture;
    }

    private static BenchmarkWriteReturnLedger.Page decode(Fixture fixture, String state) throws Exception {
        return BenchmarkWriteReturnLedger.decode(page(1, 0, 1, 1, "window", state, List.of(fixture.bytes())));
    }
    private static byte[] page(byte[] call) throws Exception {
        return page(1, 0, 1, 1, "window", RECORDED, List.of(call));
    }
    private static byte[] page(long epoch, int cursor, int next, int total, String window, String state,
                               List<byte[]> calls) throws Exception {
        var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes);
        out.writeInt(0x57525031); out.writeInt(2); out.writeLong(epoch);
        out.writeInt(cursor); out.writeInt(next); out.writeInt(total); text(out, window); text(out, state);
        for (byte[] call : calls) { out.writeInt(call.length); out.write(call); }
        return bytes.toByteArray();
    }
    private static void text(DataOutputStream out, String text) throws Exception {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8); out.writeShort(bytes.length); out.write(bytes);
    }
    private record Row(int kind, List<Integer> keys) { }
    private record Error(int ordinal, String type) { }
    private static final class Fixture {
        long sequence = 1, began = 100, observed = 200, inserted = 1, modified, removed;
        int writer = 1, normal = 1, callbacks = 1, errors, totalRows = -1, partIndex, partCount = -1;
        String failureType = "", scope = "UNKNOWN", writerIdentity = "pipeline/sink", stream = "source", target = "target";
        List<String> keyFields = List.of("id");
        List<Row> rows = List.of(new Row(1, List.of(7)));
        List<Error> errorDetails = List.of();
        byte[] bytes() throws Exception {
            var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes);
            int total = totalRows == -1 ? rows.size() : totalRows;
            int parts = partCount == -1 ? (total + 511) / 512 : partCount;
            out.writeLong(sequence); out.writeInt(writer);
            out.writeInt(total); out.writeInt(partIndex); out.writeInt(parts);
            out.writeLong(began); out.writeLong(observed);
            out.writeByte(normal); out.writeInt(callbacks); out.writeLong(inserted); out.writeLong(modified); out.writeLong(removed);
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
