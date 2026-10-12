package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnCaptureMaterialBoundsTest {
    @Test void unscoped_dto_construction_keeps_the_original_broad_material_contract() {
        assertThat(BenchmarkReturnWorkBudget.current()).isNull();
        var row = new BenchmarkWriteReturnLedger.Row(9, List.of(1, 2, 3));
        var broad = call("x".repeat(257), List.of(), Collections.nCopies(1025, row));
        var result = result(List.of(broad));
        assertThat(result.calls().getFirst().rows()).hasSize(1025);
        assertThat(result.calls().getFirst().failureType()).hasSize(257);
        assertThat(result.calls().getFirst().rows().getFirst().keys()).hasSize(3);
    }

    @Test void scoped_over_capacity_rosters_refuse_before_any_element_or_caller_iterator() {
        var accesses = new AtomicInteger();
        try (var scope = scope()) {
            assertThatThrownBy(() -> result(unreadable(73969, accesses)))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("prior cap");
            assertThat(accesses).hasValue(0);
            assertThat(scope.primaryCause()).isInstanceOf(AssertionError.class);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN");
        }
        try (var ignored = scope()) {
            assertThatThrownBy(() -> new BenchmarkWriteReturnLedger.Row(1, unreadable(3, accesses)))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("prior cap");
            assertThat(accesses).hasValue(0);
        }
        try (var ignored = scope()) {
            assertThatThrownBy(() -> call("", List.of(), unreadable(1025, accesses)))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("prior cap");
            assertThat(accesses).hasValue(0);
        }
    }

    @Test void scoped_owned_copy_uses_fixed_indexed_reads_and_validates_the_acquired_snapshot() {
        var gets = new AtomicInteger();
        var mutable = new ArrayList<>(List.of("id", "item_index"));
        var supplied = new AbstractList<String>() {
            @Override public int size() { return 2; }
            @Override public String get(int index) {
                gets.incrementAndGet(); String value = mutable.get(index);
                if (index == 1) mutable.set(0, "x".repeat(129));
                return value;
            }
            @Override public Object[] toArray() { throw new AssertionError("caller array must not be used"); }
            @Override public Iterator<String> iterator() { throw new AssertionError("caller iterator must not be used"); }
        };
        try (var ignored = scope()) {
            var copied = BenchmarkReturnCaptureMaterialBounds.copy(supplied, 2);
            var original = call("", List.of(), List.of(new BenchmarkWriteReturnLedger.Row(1, List.of(1, 2))));
            var bounded = new BenchmarkWriteReturnAssembly.FullCall(original.sequence(), original.writer(), 1,
                    10, 20, 30, true, 1, 1, 0, 0, 0, List.of(), "", "UNKNOWN", "writer", "stream", "target",
                    copied, original.rows());
            assertThat(result(List.of(bounded)).calls().getFirst().keyFields()).containsExactly("id", "item_index");
            assertThat(gets).hasValue(2);
            assertThat(mutable.getFirst()).hasSize(129);
        }
    }

    @Test void negative_sizes_and_roster_changes_never_expand_the_fixed_copy_loop() {
        var gets = new AtomicInteger();
        try (var ignored = scope()) {
            assertThatThrownBy(() -> BenchmarkReturnCaptureMaterialBounds.copy(unreadable(-1, gets), 2))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("prior cap");
            assertThat(gets).hasValue(0);
        }
        var changing = new AbstractList<Integer>() {
            @Override public int size() { return gets.get() == 0 ? 1 : 2048; }
            @Override public Integer get(int index) { gets.incrementAndGet(); return 7; }
        };
        try (var ignored = scope()) {
            assertThatThrownBy(() -> BenchmarkReturnCaptureMaterialBounds.copy(changing, 2))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("changed during copy");
            assertThat(gets).hasValue(1);
        }
    }

    @Test void failed_and_partial_error_material_is_bounded_before_encoding_or_raw_comparison() {
        var row = new BenchmarkWriteReturnLedger.Row(1, List.of(1));
        var wide = call("x".repeat(257), List.of(), List.of(row));
        var multibyte = call("", List.of(new BenchmarkWriteReturnLedger.ErrorDetail(0, "\u00e9".repeat(129))), List.of(row));
        var surrogate = call("", List.of(new BenchmarkWriteReturnLedger.ErrorDetail(0, "\ud800")), List.of(row));
        for (var malformed : List.of(wide, multibyte, surrogate)) {
            try (var ignored = scope()) {
                assertThatThrownBy(() -> result(List.of(malformed))).isInstanceOf(AssertionError.class)
                        .hasMessageContaining("capture material");
            }
        }
    }

    @Test void repeated_bounded_errors_cannot_exceed_the_original_aggregate_byte_envelope() {
        var details = Collections.nCopies(512, new BenchmarkWriteReturnLedger.ErrorDetail(-1, "x".repeat(256)));
        var boundedCall = call("", details, List.of(new BenchmarkWriteReturnLedger.Row(1, List.of(1))));
        try (var ignored = scope()) {
            assertThatThrownBy(() -> result(Collections.nCopies(512, boundedCall)))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("original ledger byte envelope");
        }
    }

    @Test void stage_wrapper_is_shallow_and_keys_are_bounded_before_hashing() {
        Object opaque = new Object() { @Override public String toString() { throw new AssertionError("opaque stage value"); } };
        try (var ignored = scope()) {
            assertThat(BenchmarkReturnCaptureMaterialBounds.stages(Map.of("stage", opaque)).get("stage")).isSameAs(opaque);
            assertThatThrownBy(() -> BenchmarkReturnCaptureMaterialBounds.stages(Map.of("x".repeat(129), opaque)))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("character cap");
        }
    }

    @Test void actual_copy_failure_is_retained_as_the_primary_without_another_element_read() {
        var primary = new IllegalStateException("controlled acquisition failure"); var reads = new AtomicInteger();
        var supplied = new AbstractList<String>() {
            @Override public int size() { return 2; }
            @Override public String get(int index) { reads.incrementAndGet(); throw primary; }
        };
        try (var scope = scope()) {
            assertThatThrownBy(() -> BenchmarkReturnCaptureMaterialBounds.copy(supplied, 2)).isSameAs(primary);
            assertThat(scope.primaryCause()).isSameAs(primary); assertThat(reads).hasValue(1);
            assertThat(scope.snapshot()).containsEntry("pendingOperations", 0L);
        }
    }

    @Test void source_rows_cover_explicit_copy_requests_and_checked_multiplication_never_wraps() {
        var reads = new AtomicInteger(); var supplied = new AbstractList<Integer>() {
            @Override public int size() { return 1024; }
            @Override public Integer get(int index) { reads.incrementAndGet(); return index; }
        };
        var upper = BenchmarkReturnCaptureMaterialBounds.upper().get(BenchmarkReturnWorkBudget.Activity.COPY);
        try (var ignored = scope()) {
            assertThat(BenchmarkReturnCaptureMaterialBounds.copy(supplied, 1024)).hasSize(1024);
            assertThat(reads).hasValue(1024); assertThat(upper.requests()).isGreaterThan(reads.get());
        }
        assertThatThrownBy(() -> upper.times(Long.MAX_VALUE)).isInstanceOf(ArithmeticException.class);
    }

    @Test void a_bounded_copied_projection_still_needs_exact_raw_reproduction_and_keeps_all_flags_false() throws Exception {
        var fixture = BenchmarkWriteReturnPhaseEvidenceTest.nativeFixture(BenchmarkWorkloadDefinitions.byId("copy"));
        var original = fixture.result(); BenchmarkWriteReturnCapture.Result copied;
        try (var ignored = scope()) {
            copied = new BenchmarkWriteReturnCapture.Result(original.calls(), original.samples(), original.summary(), original.pagesBase64());
        }
        var clock = BenchmarkWriteReturnPhaseEvidenceTest.nominalClock(copied);
        var evidence = BenchmarkWriteReturnPhaseEvidence.recordNominal(fixture.workload(), fixture.phase(), fixture.batches(), copied, clock);
        for (String flag : BenchmarkNativeClockEvidence.FLAGS) assertThat(evidence.get(flag)).isEqualTo(false);
        var calls = new ArrayList<>(copied.calls()); var first = calls.getFirst();
        calls.set(0, new BenchmarkWriteReturnAssembly.FullCall(first.sequence(), first.writer(), first.totalRows(),
                first.beganNanos(), first.lastCallbackExitNanos(), first.observedNanos(), first.returnedNormally(), first.callbackCount(),
                first.inserted(), first.modified(), first.removed(), first.errors(), first.errorDetails(), first.failureType(),
                first.scope(), first.writerIdentity(), first.stream(), "other_target", first.keyFields(), first.rows()));
        BenchmarkWriteReturnCapture.Result forged;
        try (var ignored = scope()) {
            forged = new BenchmarkWriteReturnCapture.Result(calls, copied.samples(), copied.summary(), copied.pagesBase64());
        }
        assertThatThrownBy(() -> BenchmarkWriteReturnPhaseEvidence.recordNominal(fixture.workload(), fixture.phase(), fixture.batches(), forged, clock))
                .isInstanceOf(AssertionError.class).hasMessageContaining("raw pages do not reproduce");
    }

    private static <T> List<T> unreadable(int count, AtomicInteger accesses) {
        return new AbstractList<T>() {
            @Override public int size() { return count; }
            @Override public T get(int index) { accesses.incrementAndGet(); throw new AssertionError("unexpected element access"); }
            @Override public Object[] toArray() { accesses.incrementAndGet(); throw new AssertionError("unexpected array copy"); }
            @Override public Iterator<T> iterator() { accesses.incrementAndGet(); throw new AssertionError("unexpected iterator"); }
        };
    }
    private static BenchmarkWriteReturnCapture.Result result(List<BenchmarkWriteReturnAssembly.FullCall> calls) {
        return new BenchmarkWriteReturnCapture.Result(calls, List.of(),
                new BenchmarkWriteReturnReader.Summary("window", "RECORDED_SCOPE_UNQUALIFIED", calls.size(), 0, 1, 0, 0), List.of());
    }
    private static BenchmarkWriteReturnAssembly.FullCall call(String failure, List<BenchmarkWriteReturnLedger.ErrorDetail> details,
                                                            List<BenchmarkWriteReturnLedger.Row> rows) {
        return new BenchmarkWriteReturnAssembly.FullCall(1, 1, rows.size(), 10, 20, 30, true, 1, rows.size(), 0, 0,
                details.size(), details, failure, "UNKNOWN", "writer", "stream", "target", List.of("id"), rows);
    }
    private static BenchmarkReturnWorkBudget.Scope scope() {
        var rows = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        rows.putAll(BenchmarkReturnCaptureMaterialBounds.upper());
        var excluded = EnumSet.allOf(BenchmarkReturnWorkBudget.Activity.class); excluded.removeAll(rows.keySet());
        return BenchmarkReturnWorkBudget.activate(new BenchmarkReturnWorkBudget.Definition("capture-material-control", "a".repeat(64),
                Map.of(), rows, excluded), "capture-material-control");
    }
}
