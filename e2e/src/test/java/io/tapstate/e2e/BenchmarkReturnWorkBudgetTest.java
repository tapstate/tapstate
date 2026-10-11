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

    @Test void root_clock_retains_the_original_first_point_and_accounts_one_existing_read_afterwards() {
        var reads = new AtomicInteger();
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "root-clock")) {
            long value = BenchmarkReturnWorkBudget.rootNanoTime(scope, () -> {
                assertThat(row(scope.snapshot(), "CLOCK_REQUEST")).containsEntry("attempts", 0L);
                reads.incrementAndGet(); return 42L;
            });
            assertThat(value).isEqualTo(42); assertThat(reads).hasValue(1);
            assertThat(row(scope.snapshot(), "CLOCK_REQUEST")).containsEntry("attempts", 1L)
                    .containsEntry("issuedRequests", 1L).containsEntry("completedRequests", 1L);
        }
    }

    @Test void a_failed_original_root_read_is_counted_once_and_preserves_the_exact_primary_without_an_end_read() {
        var reads = new AtomicInteger(); var primary = new IllegalStateException("controlled root read");
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "root-clock-failure")) {
            assertThatThrownBy(() -> BenchmarkReturnWorkBudget.rootNanoTime(scope, () -> {
                reads.incrementAndGet(); throw primary;
            })).isSameAs(primary);
            assertThat(reads).hasValue(1); assertThat(scope.primaryCause()).isSameAs(primary);
            assertThat(row(scope.snapshot(), "CLOCK_REQUEST")).containsEntry("issuedRequests", 1L)
                    .containsEntry("failedRequests", 1L).containsEntry("failedAttempts", 1L);
        }
    }

    @Test void bounded_bson_resume_material_refuses_before_a_json_writer_can_expand_it() {
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "bson-prior")) {
            var bounded = new org.bson.BsonDocument("token", new org.bson.BsonString("controlled"));
            BenchmarkReturnWorkBudget.requireLocalMaterial(scope, BenchmarkReturnWorkBudget.Activity.REPORT_SERIALIZE, bounded);
            var oversized = new org.bson.BsonDocument("token", new org.bson.BsonBinary(new byte[Math.toIntExact(BenchmarkReturnWorkBudget.MAX_MATERIAL_BYTES + 1)]));
            assertThatThrownBy(() -> BenchmarkReturnWorkBudget.requireLocalMaterial(scope,
                    BenchmarkReturnWorkBudget.Activity.REPORT_SERIALIZE, oversized)).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("local consumption bound");
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN");
        }
    }

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

    @Test void an_actual_reader_liveness_refusal_records_an_attempt_without_issuing_a_getter_or_clock() {
        var getters = new AtomicInteger(); var clocks = new AtomicInteger();
        var connection = (javax.management.MBeanServerConnection) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{javax.management.MBeanServerConnection.class},
                (proxy, method, arguments) -> { getters.incrementAndGet(); throw new AssertionError("getter must remain unissued"); });
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "reader-guard")) {
            var reader = new BenchmarkWriteReturnReader(new BenchmarkCausalClock.Identity(17, 1000), connection,
                    () -> false, clocks::incrementAndGet);
            assertThatThrownBy(() -> reader.clockSample(0)).isInstanceOf(AssertionError.class).hasMessageContaining("has exited");
            var clock = row(scope.snapshot(), "CLOCK_REQUEST");
            assertThat(clock).containsEntry("attempts", 1L).containsEntry("failedAttempts", 1L)
                    .containsEntry("issuedRequests", 0L).containsEntry("completedRequests", 0L);
            assertThat(map(clock, "actual")).containsEntry("requests", 0L);
            assertThat(getters).hasValue(0); assertThat(clocks).hasValue(0);
        }
    }

    @Test void an_issued_checked_service_retains_pending_then_the_same_primary_failure() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        var thrown = new AtomicReference<Throwable>(); IOException original = new IOException("controlled service refusal");
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "checked-service")) {
            Thread worker = Thread.ofPlatform().daemon(true).start(() -> {
                try {
                    BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, () -> {
                        entered.countDown(); if (!release.await(2, TimeUnit.SECONDS)) { throw new AssertionError("release is missing"); }
                        throw original;
                    });
                } catch (Throwable failure) { thrown.set(failure); }
            });
            try {
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                var pending = scope.snapshot();
                assertThat(row(pending, "RUNTIME_READ")).containsEntry("issuedRequests", 1L).containsEntry("completedRequests", 0L);
                assertThat(pending).containsEntry("pendingOperations", 1L);
                release.countDown(); worker.join(2000); assertThat(worker.isAlive()).isFalse();
                assertThat(thrown.get()).isSameAs(original); assertThat(scope.primaryCause()).isSameAs(original);
                assertThat(row(scope.snapshot(), "RUNTIME_READ")).containsEntry("completedRequests", 1L).containsEntry("failedRequests", 1L);
                assertThat(pending).containsEntry("pendingOperations", 1L);
            } finally { release.countDown(); worker.join(2000); }
        }
    }

    @Test void a_failed_first_return_mapper_does_not_fabricate_the_second_map_or_source_map() {
        var owner = new BenchmarkCausalClock.Identity(17, 1000);
        var target = BenchmarkWorkloadDefinitions.byId("copy").phase("cdc-update").targets().getFirst();
        var row = new BenchmarkWriteReturnExpectations.Association(target, "1", 0, 150, 160,
                1, 1, "pdk.state.bench_copy.sink", "orders", 1500, 2200, 2500, 2, true);
        AssertionError primary = new AssertionError("controlled first point refusal"); var calls = new AtomicInteger();
        BenchmarkReturnPointClock clock = (identity, point) -> { calls.incrementAndGet(); throw primary; };
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "mapper-failure")) {
            assertThatThrownBy(() -> BenchmarkReturnTimeBounds.map(List.of(row), owner, clock)).isSameAs(primary);
            assertThat(calls).hasValue(1); assertThat(scope.primaryCause()).isSameAs(primary);
            assertThat(row(scope.snapshot(), "RETURN_MAP")).containsEntry("attempts", 1L).containsEntry("failedAttempts", 1L);
            assertThat(row(scope.snapshot(), "SOURCE_MAP")).containsEntry("attempts", 0L);
        }
    }

    @Test void p99_copy_failure_cannot_be_reported_as_two_completed_sorts() {
        var input = new java.util.ArrayList<BenchmarkCausalClock.Interval>();
        for (int index = 0; index < 10_000; index++) { input.add(new BenchmarkCausalClock.Interval(index, index + 1)); }
        input.set(77, null);
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "sort-copy-failure")) {
            assertThatThrownBy(() -> BenchmarkReturnTimeBounds.p99(input)).isInstanceOf(NullPointerException.class);
            assertThat(row(scope.snapshot(), "SORT")).containsEntry("failedAttempts", 1L).containsEntry("issuedRequests", 0L);
            assertThat(row(scope.snapshot(), "COPY")).containsEntry("failedRequests", 1L);
        }
    }

    @Test void local_material_is_rejected_before_consumer_copy_and_never_claims_transport_allocation() {
        var consumed = new AtomicInteger(); String oversized = "x".repeat(BenchmarkReturnWorkBudget.MAX_MATERIAL_LEAF_CHARACTERS + 1);
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "material-bound")) {
            assertThatThrownBy(() -> {
                BenchmarkReturnWorkBudget.requireLocalMaterial(scope, BenchmarkReturnWorkBudget.Activity.STREAM_ACCEPT, oversized);
                consumed.incrementAndGet();
            }).isInstanceOf(AssertionError.class);
            var receipt = scope.snapshot(); assertThat(receipt).containsEntry("state", "UNKNOWN");
            assertThat(row(receipt, "STREAM_ACCEPT")).containsEntry("gap", "LOCAL_CONSUMPTION_BOUND_UNAVAILABLE");
            assertThat(map(receipt, "localMaterialBounds")).containsEntry("scope", "LOCAL_OBSERVER_CONSUMPTION_EXCLUDES_OPAQUE_RPC_ALLOCATION");
            assertThat(consumed).hasValue(0);
        }
    }

    @Test void all_three_self_counter_overflows_are_sticky_and_preserve_existing_primary_and_output() throws Exception {
        for (String field : List.of("outputCloneBytes", "outputHashBytes", "outputTreeNodeVisits")) {
            try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "self-overflow")) {
                IOException primary = new IOException("existing primary"); scope.failed(primary);
                var counter = scope.getClass().getDeclaredField(field); counter.setAccessible(true); counter.setLong(scope, Long.MAX_VALUE);
                var prepared = prepare(scope); assertThat(prepared.bytes()).isNotEmpty();
                var receipt = scope.snapshot(); assertThat(receipt).containsEntry("state", "UNKNOWN").containsEntry("selfOverflow", true);
                assertThat(scope.primaryCause()).isSameAs(primary);
                assertThat(map(receipt, "output")).containsEntry("state", "OUTPUT_PENDING");
            }
        }
    }

    @Test void source_byte_limit_is_a_prior_definition_input_and_not_the_observed_payload_size() {
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "prior-bytes")) {
            assertThatThrownBy(() -> scope.requirePriorBytes(BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, 100_001))
                    .isInstanceOf(AssertionError.class);
            assertThat(row(scope.snapshot(), "INPUT_FINGERPRINT")).containsEntry("attempts", 0L)
                    .containsEntry("gap", "PRIOR_SOURCE_BYTE_LIMIT_MISSING_OR_EXCEEDED");
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
