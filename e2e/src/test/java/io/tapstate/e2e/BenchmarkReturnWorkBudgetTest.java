package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnWorkBudgetTest {
    private static final String HASH = "a".repeat(64);

    @Test void no_active_scope_keeps_original_operations_and_does_not_create_a_worker_or_provider() {
        assertThat(BenchmarkReturnWorkBudget.current()).isNull(); var calls = new AtomicInteger();
        assertThat(BenchmarkReturnWorkBudget.request(null, BenchmarkReturnWorkBudget.Activity.CLOCK_REQUEST,
                calls::incrementAndGet)).isEqualTo(1);
        assertThat(BenchmarkReturnWorkBudget.begin(null, BenchmarkReturnWorkBudget.Activity.COPY, BenchmarkReturnWorkBudget.Work.call())).isNull();
        assertThat(calls).hasValue(1); assertThat(BenchmarkReturnWorkBudget.current()).isNull();
    }

    @Test void missing_or_conflicting_source_bound_bases_refuse_before_scope_activation() {
        assertThatThrownBy(() -> new BenchmarkReturnWorkBudget.Definition("control", HASH, Map.of(), Map.of(), Set.of()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("exactly one");
        assertThat(BenchmarkReturnWorkBudget.current()).isNull();
    }

    @Test void complete_actual_counts_close_a_this_run_derived_upper_without_a_future_lifetime_cap() throws Exception {
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "control")) {
            scope.bindRoot(17, 1000);
            for (int index = 0; index < 3; index++) {
                var ticket = scope.begin(BenchmarkReturnWorkBudget.Activity.PAGE_RPC, BenchmarkReturnWorkBudget.Work.request());
                scope.complete(ticket, BenchmarkReturnWorkBudget.Work.bytes(8), null);
            }
            write(scope);
            var receipt = scope.finish(); var row = row(receipt, "PAGE_RPC");
            assertThat(row).containsEntry("basis", "STATIC_PER_OPERATION_BOUND_TIMES_COMPLETE_ACTUAL_COUNT")
                    .containsEntry("attempts", 3L).containsEntry("completedAttempts", 3L).containsEntry("actualCountComplete", true);
            assertThat(map(row, "actual").get("bytes")).isEqualTo(24L);
            assertThat(receipt).containsEntry("state", "CLOSED").containsEntry("structuralWorkBudgetQualified", false);
            assertThatThrownBy(receipt::clear).isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test void an_async_owner_keeps_the_original_scope_and_pending_work_cannot_be_finalized() throws Exception {
        CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1);
        var observed = new AtomicReference<Throwable>();
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "async")) {
            scope.bindRoot(17, 1000);
            Thread worker = Thread.ofPlatform().daemon(true).start(() -> {
                var ticket = BenchmarkReturnWorkBudget.begin(scope, BenchmarkReturnWorkBudget.Activity.STREAM_READ, BenchmarkReturnWorkBudget.Work.request());
                started.countDown();
                try { if (!release.await(2, TimeUnit.SECONDS)) { throw new AssertionError("control release did not arrive"); } BenchmarkReturnWorkBudget.done(ticket); }
                catch (Throwable failure) { observed.set(failure); BenchmarkReturnWorkBudget.failed(ticket, failure); }
            });
            try {
                assertThat(started.await(2, TimeUnit.SECONDS)).isTrue(); write(scope);
                var prefix = scope.finish(); assertThat(prefix).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 1L);
                release.countDown(); worker.join(2000); assertThat(worker.isAlive()).isFalse(); assertThat(observed.get()).isNull();
                assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
                assertThat(prefix.get("pendingOperations")).isEqualTo(1L);
            } finally { release.countDown(); worker.join(2000); }
        }
    }

    @Test void excess_unknown_and_overflow_quantities_never_become_zero_or_closed_credit() throws Exception {
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "overflow")) {
            scope.bindRoot(17, 1000);
            scope.add(BenchmarkReturnWorkBudget.Activity.COPY, BenchmarkReturnWorkBudget.Work.bytes(Long.MAX_VALUE));
            scope.add(BenchmarkReturnWorkBudget.Activity.COPY, BenchmarkReturnWorkBudget.Work.bytes(1));
            scope.gap(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, "UNKNOWN_MATERIAL"); write(scope);
            assertThat(scope.finish()).containsEntry("state", "UNKNOWN");
            assertThat(scope.primaryCause()).isInstanceOf(ArithmeticException.class);
            assertThat(map(row(scope.snapshot(), "COPY"), "actual").get("bytes")).isEqualTo(Long.MAX_VALUE);
        }
    }

    @Test void a_failed_real_write_preserves_its_exact_primary_and_output_stays_pending() throws Exception {
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "failed-write")) {
            scope.bindRoot(17, 1000); var prepared = prepare(scope); IOException primary = new IOException("controlled partial write");
            assertThatThrownBy(() -> scope.writeOutput(prepared, bytes -> { throw primary; })).isSameAs(primary);
            var receipt = scope.finish(); assertThat(receipt).containsEntry("state", "UNKNOWN");
            assertThat(map(receipt, "output")).containsEntry("state", "OUTPUT_PENDING");
            assertThat(scope.primaryCause()).isSameAs(primary);
        }
    }

    @Test void a_prior_tree_bound_refuses_before_serialization_and_encoded_size_is_not_its_own_cap() {
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "tree")) {
            scope.bindRoot(17, 1000);
            assertThatThrownBy(() -> scope.prepareOutput("report", new BenchmarkReturnWorkBudget.TreePlan(2, 3, 20, 2),
                    Map.of("large", "outside-prior"))).isInstanceOf(AssertionError.class);
            assertThat(row(scope.snapshot(), "REPORT_SERIALIZE")).containsEntry("attempts", 0L);
        }
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "legacy-serializer")) {
            scope.prepareOutput("report", 100, () -> "{}");
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN");
        }
    }

    @Test void a_late_operation_after_a_successful_seal_invalidates_the_current_outcome_without_mutating_the_prefix() throws Exception {
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "late")) {
            scope.bindRoot(17, 1000); write(scope); var prefix = scope.finish();
            assertThat(prefix).containsEntry("state", "CLOSED");
            scope.add(BenchmarkReturnWorkBudget.Activity.CLEANUP, BenchmarkReturnWorkBudget.Work.call());
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN"); assertThat(prefix).containsEntry("state", "CLOSED");
        }
    }

    private static BenchmarkReturnWorkBudget.Definition definition() {
        var perOperation = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        for (var activity : BenchmarkReturnWorkBudget.Activity.values()) {
            perOperation.put(activity, new BenchmarkReturnWorkBudget.Work(10, 10, 100, 100, 100_000, 100, 10_000, 10_000));
        }
        return new BenchmarkReturnWorkBudget.Definition("control", HASH, Map.of(), perOperation, Set.of());
    }
    private static BenchmarkReturnWorkBudget.PreparedOutput prepare(BenchmarkReturnWorkBudget.Scope scope) {
        return scope.prepareOutput("report", new BenchmarkReturnWorkBudget.TreePlan(1000, 10_000, 100_000, 8), Map.of("data", List.of(1, 2, 3)));
    }
    private static void write(BenchmarkReturnWorkBudget.Scope scope) throws Exception {
        scope.writeOutput(prepare(scope), bytes -> { assertThat(bytes.length).isPositive(); });
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> map(Map<String, Object> values, String key) { return (Map<String, Object>) values.get(key); }
    private static Map<String, Object> row(Map<String, Object> receipt, String activity) { return map(map(receipt, "activities"), activity); }
}
