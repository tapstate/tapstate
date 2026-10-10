package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static io.tapstate.e2e.BenchmarkRootCpuEnvelope.Cutoff.AFTER_ACK_COMMON_CHECKPOINTS;
import static io.tapstate.e2e.BenchmarkRootCpuEnvelope.Cutoff.AFTER_COMPLETE_COLLECTION_BEFORE_TERMINAL_SQL;
import static io.tapstate.e2e.BenchmarkRootCpuEnvelope.Cutoff.BEFORE_COLLECTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkRootCpuEnvelopeTest {
    private static final BenchmarkRootCpuEnvelope.ProviderIdentity PROVIDER =
            new BenchmarkRootCpuEnvelope.ProviderIdentity("test.RootCpuProvider", "getProcessCpuTime", "libmanagement_ext.dylib", "a".repeat(64));
    private static final BenchmarkRootCpuEnvelope.Identity ROOT = new BenchmarkRootCpuEnvelope.Identity(17, 23, true, PROVIDER);
    private static final BenchmarkRootCpuEnvelope.Waiter WAIT = (future, timeout, unit) -> future.get(timeout, unit);

    @Test void complete_raw_counter_facts_are_immutable_and_never_supply_an_accounting_allowance() {
        Fixture fixture = new Fixture(10, 25, 40); var envelope = fixture.open(Set.of(), Set.of());
        envelope.read(BEFORE_COLLECTION); Map<String, Object> prefix = envelope.evidence();
        envelope.read(AFTER_ACK_COMMON_CHECKPOINTS); envelope.read(AFTER_COMPLETE_COLLECTION_BEFORE_TERMINAL_SQL);
        Map<String, Object> evidence = envelope.evidence();
        assertThat(evidence.get("state")).isEqualTo("RECORDED_DIAGNOSTIC");
        assertThat(rows(prefix, "readings")).hasSize(1); assertThat(rows(evidence, "readings")).hasSize(3);
        assertThat(map(evidence, "recordedCounterDeltasNanos")).containsEntry(AFTER_ACK_COMMON_CHECKPOINTS.name(), 15L)
                .containsEntry(AFTER_COMPLETE_COLLECTION_BEFORE_TERMINAL_SQL.name(), 30L);
        assertThat(map(evidence, "accountingErrorAllowance")).containsEntry("state", "UNKNOWN");
        assertThat(map(evidence, "collectionCpuUpperBound")).containsEntry("state", "UNKNOWN");
        assertThat(fixture.cpuCalls).hasValue(3); assertThat(fixture.identityCalls).hasValue(6);
        assertThatThrownBy(() -> evidence.put("state", "QUALIFIED")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> rows(evidence, "readings").getFirst().put("processCpuTimeNanos", 999L))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> rows(evidence, "operations").add(Map.of())).isInstanceOf(UnsupportedOperationException.class);
        assertUnqualified(evidence);
    }

    @Test void sealed_common_and_tail_operations_enclose_the_three_actual_reads() {
        Fixture fixture = new Fixture(10, 30, 80); var envelope = fixture.open(Set.of("RESOURCE", "COMMAND"), Set.of("TAIL"));
        envelope.read(BEFORE_COLLECTION);
        var resource = envelope.begin("RESOURCE"); var command = envelope.begin("COMMAND");
        envelope.complete(resource); envelope.complete(command); envelope.read(AFTER_ACK_COMMON_CHECKPOINTS);
        var tail = envelope.begin("TAIL"); envelope.complete(tail); envelope.read(AFTER_COMPLETE_COLLECTION_BEFORE_TERMINAL_SQL);
        Map<String, Object> evidence = envelope.evidence();
        assertThat(rows(evidence, "operations")).allSatisfy(operation -> {
            assertThat(operation.get("state")).isEqualTo("COMPLETED");
            int cutoff = operation.get("name").equals("TAIL") ? 2 : 1;
            assertThat((long) operation.get("completedAtNanos")).isLessThanOrEqualTo((long) rows(evidence, "readings").get(cutoff).get("startedAtNanos"));
        });
        assertThat(map(evidence, "recordedCounterDeltasNanos")).containsEntry(AFTER_COMPLETE_COLLECTION_BEFORE_TERMINAL_SQL.name(), 70L);
    }

    @Test void operation_budget_and_scope_conflicts_refuse_before_any_provider_read() {
        Fixture fixture = new Fixture(10);
        assertThatThrownBy(() -> fixture.open(Set.of("A", "B", "C", "D", "E"), Set.of())).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> fixture.open(Set.of("A"), Set.of("A"))).isInstanceOf(AssertionError.class);
        assertThat(fixture.cpuCalls).hasValue(0); assertThat(fixture.identityCalls).hasValue(0);
    }

    @Test void missing_operations_and_repeated_cutoffs_never_invent_an_endpoint() {
        Fixture missing = new Fixture(10, 20); var envelope = missing.open(Set.of("COMMON"), Set.of());
        envelope.read(BEFORE_COLLECTION);
        var failure = refusal(() -> envelope.read(AFTER_ACK_COMMON_CHECKPOINTS));
        assertThat(rows(failure.retainedEvidence(), "readings")).hasSize(1);
        assertThat(map(failure.retainedEvidence(), "pendingRead")).doesNotContainKey("completedAtNanos");
        assertThat(missing.cpuCalls).hasValue(1);
        Fixture repeated = new Fixture(10, 20); var second = repeated.open(Set.of(), Set.of()); second.read(BEFORE_COLLECTION);
        refusal(() -> second.read(BEFORE_COLLECTION)); assertThat(repeated.cpuCalls).hasValue(1);
    }

    @Test void operation_tickets_cannot_be_foreign_repeated_or_started_before_their_scope() {
        Fixture fixture = new Fixture(10, 20); var envelope = fixture.open(Set.of("COMMON"), Set.of("TAIL"));
        refusal(() -> envelope.begin("COMMON")); assertThat(fixture.cpuCalls).hasValue(0);
        var second = new Fixture(10, 20).open(Set.of("COMMON"), Set.of("TAIL")); second.read(BEFORE_COLLECTION);
        var ticket = second.begin("COMMON"); second.complete(ticket);
        refusal(() -> second.complete(ticket));
        var third = new Fixture(10, 20).open(Set.of("COMMON"), Set.of()); third.read(BEFORE_COLLECTION);
        refusal(() -> third.complete(ticket));
        var fourth = new Fixture(10, 20).open(Set.of("COMMON"), Set.of("TAIL")); fourth.read(BEFORE_COLLECTION);
        refusal(() -> fourth.begin("TAIL"));
    }

    @Test void unsupported_and_regressed_cpu_values_remain_raw_unknown_facts() {
        Fixture unsupported = new Fixture(-1); var first = unsupported.open(Set.of(), Set.of());
        var absent = refusal(() -> first.read(BEFORE_COLLECTION));
        assertThat(rows(absent.retainedEvidence(), "readings").getFirst()).containsEntry("processCpuTimeNanos", -1L);
        assertThat(map(absent.retainedEvidence(), "recordedCounterDeltasNanos")).isEmpty(); assertUnqualified(absent.retainedEvidence());
        Fixture regressed = new Fixture(100, 90, 200); var second = regressed.open(Set.of(), Set.of()); second.read(BEFORE_COLLECTION);
        var decrease = refusal(() -> second.read(AFTER_ACK_COMMON_CHECKPOINTS));
        assertThat(rows(decrease.retainedEvidence(), "readings")).hasSize(2);
        assertThat(map(decrease.retainedEvidence(), "recordedCounterDeltasNanos")).containsEntry(AFTER_ACK_COMMON_CHECKPOINTS.name(), -10L);
        assertThat(refusal(() -> second.read(AFTER_COMPLETE_COLLECTION_BEFORE_TERMINAL_SQL)).getCause()).isSameAs(decrease.getCause());
        assertThat(regressed.cpuCalls).hasValue(2);
    }

    @Test void runtime_provider_reuse_and_exit_are_refused_with_the_actual_identity_retained() {
        var changedProvider = new BenchmarkRootCpuEnvelope.ProviderIdentity("test.OtherCpuProvider", "getProcessCpuTime", "libmanagement_ext.dylib", "b".repeat(64));
        for (var actual : List.of(new BenchmarkRootCpuEnvelope.Identity(18, 23, true, PROVIDER),
                new BenchmarkRootCpuEnvelope.Identity(17, 24, true, PROVIDER),
                new BenchmarkRootCpuEnvelope.Identity(17, 23, false, PROVIDER),
                new BenchmarkRootCpuEnvelope.Identity(17, 23, true, changedProvider))) {
            Fixture fixture = new Fixture(10); fixture.identity = () -> actual; var envelope = fixture.open(Set.of(), Set.of());
            var failed = refusal(() -> envelope.read(BEFORE_COLLECTION));
            assertThat(map(map(failed.retainedEvidence(), "pendingRead"), "identityBefore"))
                    .containsEntry("pid", actual.pid()).containsEntry("jvmStartTimeMillis", actual.jvmStartTimeMillis()).containsEntry("alive", actual.alive());
            assertThat(fixture.cpuCalls).hasValue(0); assertUnqualified(failed.retainedEvidence());
        }
    }

    @Test void a_runtime_change_after_the_cpu_getter_keeps_the_returned_counter_and_real_end() {
        Fixture fixture = new Fixture(55);
        fixture.identity = () -> fixture.identityCalls.get() == 2 ? new BenchmarkRootCpuEnvelope.Identity(17, 24, true, PROVIDER) : ROOT;
        var failed = refusal(() -> fixture.open(Set.of(), Set.of()).read(BEFORE_COLLECTION));
        var actual = rows(failed.retainedEvidence(), "readings").getFirst();
        assertThat(actual).containsEntry("processCpuTimeNanos", 55L).containsKey("completedAtNanos");
        assertThat(map(actual, "identityAfter")).containsEntry("jvmStartTimeMillis", 24L);
    }

    @Test void provider_failure_preserves_the_original_cause_and_never_publishes_a_fake_read_end() {
        RuntimeException primary = new IllegalStateException("private fixture detail"); Fixture fixture = new Fixture(10);
        fixture.cpu = () -> { throw primary; }; var envelope = fixture.open(Set.of(), Set.of());
        var failed = refusal(() -> envelope.read(BEFORE_COLLECTION));
        assertThat(failed.getCause()).isSameAs(primary);
        assertThat(map(failed.retainedEvidence(), "pendingRead")).containsKey("startedAtNanos").doesNotContainKey("completedAtNanos");
        assertThat(rows(failed.retainedEvidence(), "readings")).isEmpty();
        assertThat(JsonWriter.write(failed.retainedEvidence())).doesNotContain("private fixture detail");
    }

    @Test void completion_clock_failure_retains_returned_cpu_and_identity_without_an_end() {
        RuntimeException primary = new IllegalStateException("completion clock unavailable"); Fixture fixture = new Fixture(55);
        AtomicInteger reads = new AtomicInteger(); fixture.clock = () -> {
            int index = reads.incrementAndGet(); if (index == 2) { throw primary; } return index;
        };
        var failed = refusal(() -> fixture.open(Set.of(), Set.of()).read(BEFORE_COLLECTION));
        assertThat(failed.getCause()).isSameAs(primary);
        assertThat(map(failed.retainedEvidence(), "pendingRead")).containsEntry("processCpuTimeNanos", 55L)
                .containsKey("identityAfter").doesNotContainKey("completedAtNanos");
    }

    @Test void negative_and_zero_root_clock_values_are_valid_but_backward_or_overflow_order_is_not() {
        Fixture fixture = new Fixture(0, 0, 0); fixture.clock = new AtomicLong(-4)::getAndIncrement;
        var envelope = fixture.open(Set.of(), Set.of()); envelope.read(BEFORE_COLLECTION);
        envelope.read(AFTER_ACK_COMMON_CHECKPOINTS); envelope.read(AFTER_COMPLETE_COLLECTION_BEFORE_TERMINAL_SQL);
        assertThat(rows(envelope.evidence(), "readings").getFirst()).containsEntry("startedAtNanos", -4L);
        assertThat(rows(envelope.evidence(), "readings").getLast()).containsEntry("startedAtNanos", 0L);
        Fixture backward = new Fixture(10); AtomicInteger reads = new AtomicInteger(); backward.clock = () -> switch (reads.incrementAndGet()) {
            case 1 -> 10; default -> 5;
        };
        var failed = refusal(() -> backward.open(Set.of(), Set.of()).read(BEFORE_COLLECTION));
        assertThat(rows(failed.retainedEvidence(), "readings").getFirst()).containsEntry("completedAtNanos", 5L);
        Fixture overflow = new Fixture(10); AtomicInteger points = new AtomicInteger(); overflow.clock = () -> points.incrementAndGet() == 1 ? Long.MIN_VALUE : Long.MAX_VALUE;
        assertThat(refusal(() -> overflow.open(Set.of(), Set.of()).read(BEFORE_COLLECTION)).getCause()).isInstanceOf(ArithmeticException.class);
    }

    @Test void the_final_endpoint_waits_for_its_real_tail_completion_without_holding_the_lifecycle_monitor() throws Exception {
        Fixture fixture = new Fixture(10, 20, 30); CountDownLatch waiting = new CountDownLatch(1);
        AtomicBoolean watchingTail = new AtomicBoolean();
        fixture.waiter = (future, timeout, unit) -> {
            requireBudget(timeout, unit);
            if (watchingTail.get()) { waiting.countDown(); }
            future.get(timeout, unit);
        };
        var envelope = fixture.open(Set.of(), Set.of("TAIL")); envelope.read(BEFORE_COLLECTION); envelope.read(AFTER_ACK_COMMON_CHECKPOINTS);
        watchingTail.set(true);
        var tail = envelope.begin("TAIL"); var failed = new AtomicReference<Throwable>();
        AtomicBoolean tailCompleted = new AtomicBoolean();
        Thread reader = Thread.ofPlatform().daemon(true).start(() -> {
            try { envelope.read(AFTER_COMPLETE_COLLECTION_BEFORE_TERMINAL_SQL); } catch (Throwable failure) { failed.set(failure); }
        });
        try {
            await(waiting); assertThat(fixture.cpuCalls).hasValue(2);
            assertThat(map(envelope.evidence(), "pendingRead")).containsEntry("waitStage", "OPERATION_COMPLETION").doesNotContainKey("completedAtNanos");
            envelope.complete(tail); tailCompleted.set(true); reader.join(2000); assertThat(reader.isAlive()).isFalse(); assertThat(failed.get()).isNull();
            assertThat(envelope.evidence().get("state")).isEqualTo("RECORDED_DIAGNOSTIC"); assertThat(fixture.cpuCalls).hasValue(3);
        } finally { if (!tailCompleted.get()) { envelope.complete(tail); } reader.join(2000); }
    }

    @Test void an_operation_timeout_is_sticky_but_keeps_later_real_completion_without_retry() {
        Fixture fixture = new Fixture(10, 20, 30); TimeoutException primary = new TimeoutException("controlled operation wait timeout");
        AtomicBoolean watchingTail = new AtomicBoolean();
        fixture.waiter = (future, timeout, unit) -> {
            requireBudget(timeout, unit); if (watchingTail.get()) { throw primary; } future.get(timeout, unit);
        };
        var envelope = fixture.open(Set.of(), Set.of("TAIL")); envelope.read(BEFORE_COLLECTION); envelope.read(AFTER_ACK_COMMON_CHECKPOINTS);
        watchingTail.set(true);
        var tail = envelope.begin("TAIL"); var failed = refusal(() -> envelope.read(AFTER_COMPLETE_COLLECTION_BEFORE_TERMINAL_SQL));
        assertThat(failed.getCause()).isSameAs(primary);
        assertThat(rows(failed.retainedEvidence(), "operations").getFirst()).doesNotContainKey("completedAtNanos");
        envelope.complete(tail); assertThat(envelope.evidence().get("state")).isEqualTo("UNKNOWN");
        assertThat(rows(envelope.evidence(), "operations").getFirst()).containsEntry("state", "COMPLETED").containsKey("completedAtNanos");
        assertThat(rows(failed.retainedEvidence(), "operations").getFirst()).doesNotContainKey("completedAtNanos");
        refusal(() -> envelope.read(AFTER_COMPLETE_COLLECTION_BEFORE_TERMINAL_SQL)); assertThat(fixture.cpuCalls).hasValue(2);
    }

    @Test void a_stalled_begin_clock_leaves_boundary_admission_and_evidence_available() throws Exception {
        Fixture fixture = new Fixture(10, 20); AtomicInteger points = new AtomicInteger();
        AtomicBoolean clockReturned = new AtomicBoolean();
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), boundaryReturned = new CountDownLatch(1);
        fixture.clock = () -> {
            int point = points.incrementAndGet();
            if (point == 3) { entered.countDown(); try { await(release); } finally { clockReturned.set(true); } }
            return point;
        };
        var envelope = fixture.open(Set.of("COMMON"), Set.of()); envelope.read(BEFORE_COLLECTION);
        var begun = new AtomicReference<BenchmarkRootCpuEnvelope.Ticket>();
        var beginFailure = new AtomicReference<Throwable>(); var boundaryFailure = new AtomicReference<Throwable>();
        Thread beginner = Thread.ofPlatform().daemon(true).start(() -> {
            try { begun.set(envelope.begin("COMMON")); } catch (Throwable failure) { beginFailure.set(failure); }
        });
        Thread boundary = Thread.ofPlatform().daemon(true).unstarted(() -> {
            try { envelope.read(AFTER_ACK_COMMON_CHECKPOINTS); } catch (Throwable failure) { boundaryFailure.set(failure); }
            finally { boundaryReturned.countDown(); }
        });
        try {
            await(entered); assertThat(rows(envelope.evidence(), "operations").getFirst()).containsEntry("state", "NOT_STARTED");
            boundary.start(); await(boundaryReturned);
            assertThat(boundaryFailure.get()).isInstanceOf(BenchmarkRootCpuEnvelope.Refusal.class);
            assertThat(clockReturned.get()).isFalse(); assertThat(beginFailure.get()).isNull();
            assertThat(boundaryFailure.get().getCause()).hasMessageContaining("unstarted registered operation");
            assertThat(envelope.evidence().get("state")).isEqualTo("UNKNOWN"); assertThat(fixture.cpuCalls).hasValue(1);
            assertThat(begun.get()).isNull(); release.countDown(); beginner.join(2000);
            assertThat(beginner.isAlive()).isFalse(); assertThat(beginFailure.get()).isInstanceOf(BenchmarkRootCpuEnvelope.Refusal.class);
            assertThat(beginFailure.get().getCause()).isSameAs(boundaryFailure.get().getCause());
        } finally {
            release.countDown(); beginner.join(2000);
            if (begun.get() != null) { envelope.complete(begun.get()); }
            if (boundary.getState() != Thread.State.NEW) { boundary.join(2000); }
        }
    }

    @Test void a_stalled_provider_refuses_boundedly_and_retains_its_later_actual_return() throws Exception {
        Fixture fixture = new Fixture(55); CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), workerReturned = new CountDownLatch(1);
        TimeoutException primary = new TimeoutException("controlled provider wait timeout");
        fixture.points.set(0);
        fixture.cpu = () -> { entered.countDown(); await(release); return 55; };
        fixture.clock = () -> { long value = fixture.points.incrementAndGet(); if (fixture.points.get() == 2) { workerReturned.countDown(); } return value; };
        fixture.waiter = (future, timeout, unit) -> { requireBudget(timeout, unit); await(entered); throw primary; };
        var envelope = fixture.open(Set.of(), Set.of());
        try {
            var failed = refusal(() -> envelope.read(BEFORE_COLLECTION)); assertThat(failed.getCause()).isSameAs(primary);
            assertThat(map(failed.retainedEvidence(), "pendingRead")).containsEntry("waitStage", "CPU_READ")
                    .containsKey("startedAtNanos").doesNotContainKey("processCpuTimeNanos").doesNotContainKey("completedAtNanos");
            assertThat(rows(failed.retainedEvidence(), "readings")).isEmpty();
            release.countDown(); await(workerReturned);
            awaitReading(envelope);
            assertThat(rows(envelope.evidence(), "readings").getFirst()).containsEntry("processCpuTimeNanos", 55L).containsKey("completedAtNanos");
            assertThat(envelope.evidence().get("state")).isEqualTo("UNKNOWN");
            assertThat(map(failed.retainedEvidence(), "pendingRead")).doesNotContainKey("completedAtNanos");
            refusal(() -> envelope.read(BEFORE_COLLECTION)); assertThat(fixture.cpuCalls).hasValue(1);
        } finally { release.countDown(); await(workerReturned); }
    }

    @Test void the_maximum_sealed_roster_and_counter_values_fit_the_four_kibibyte_evidence_budget() {
        Fixture fixture = new Fixture(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
        var envelope = fixture.open(Set.of("A".repeat(32), "B".repeat(32)), Set.of("C".repeat(32), "D".repeat(32)));
        envelope.read(BEFORE_COLLECTION);
        envelope.complete(envelope.begin("A".repeat(32))); envelope.complete(envelope.begin("B".repeat(32)));
        envelope.read(AFTER_ACK_COMMON_CHECKPOINTS);
        envelope.complete(envelope.begin("C".repeat(32))); envelope.complete(envelope.begin("D".repeat(32)));
        envelope.read(AFTER_COMPLETE_COLLECTION_BEFORE_TERMINAL_SQL);
        assertThat(JsonWriter.write(envelope.evidence()).getBytes(StandardCharsets.UTF_8).length)
                .isLessThanOrEqualTo(BenchmarkRootCpuEnvelope.MAX_EVIDENCE_BYTES);
        assertUnqualified(envelope.evidence());
    }

    @Test void a_stalled_measurement_clock_cannot_block_the_owner_or_invent_a_read_start() throws Exception {
        Fixture fixture = new Fixture(55); CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), returned = new CountDownLatch(1);
        AtomicInteger points = new AtomicInteger(); TimeoutException primary = new TimeoutException("controlled clock wait timeout");
        fixture.clock = () -> {
            int point = points.incrementAndGet();
            if (point == 1) { entered.countDown(); await(release); }
            if (point == 2) { returned.countDown(); }
            return point;
        };
        fixture.waiter = (future, timeout, unit) -> { requireBudget(timeout, unit); await(entered); throw primary; };
        var envelope = fixture.open(Set.of(), Set.of());
        try {
            var failed = refusal(() -> envelope.read(BEFORE_COLLECTION)); assertThat(failed.getCause()).isSameAs(primary);
            assertThat(map(failed.retainedEvidence(), "pendingRead")).containsEntry("state", "CPU_WORKER_REQUESTED")
                    .doesNotContainKey("startedAtNanos").doesNotContainKey("completedAtNanos");
            assertThat(fixture.cpuCalls).hasValue(0);
            release.countDown(); await(returned); awaitReading(envelope);
            assertThat(rows(envelope.evidence(), "readings").getFirst()).containsEntry("startedAtNanos", 1L).containsEntry("completedAtNanos", 2L);
            assertThat(envelope.evidence().get("state")).isEqualTo("UNKNOWN"); assertThat(fixture.cpuCalls).hasValue(1);
            assertThat(map(failed.retainedEvidence(), "pendingRead")).doesNotContainKey("startedAtNanos");
        } finally { release.countDown(); await(returned); }
    }

    @Test void the_actual_root_provider_reads_are_only_diagnostic_even_when_complete() {
        var envelope = BenchmarkRootCpuEnvelope.open(Set.of(), Set.of()); envelope.read(BEFORE_COLLECTION);
        envelope.read(AFTER_ACK_COMMON_CHECKPOINTS); envelope.read(AFTER_COMPLETE_COLLECTION_BEFORE_TERMINAL_SQL);
        assertThat(map(envelope.evidence(), "expectedRoot")).containsEntry("pid", ProcessHandle.current().pid())
                .containsEntry("jvmStartTimeMillis", ManagementFactory.getRuntimeMXBean().getStartTime());
        assertThat(rows(envelope.evidence(), "providers").getFirst()).containsEntry("className", ManagementFactory.getOperatingSystemMXBean().getClass().getName())
                .containsEntry("methodName", "getProcessCpuTime");
        assertThat(rows(envelope.evidence(), "readings")).hasSize(3).allSatisfy(reading -> assertThat((long) reading.get("processCpuTimeNanos")).isNotNegative());
        assertUnqualified(envelope.evidence());
    }

    private static final class Fixture {
        final AtomicInteger cpuCalls = new AtomicInteger(), identityCalls = new AtomicInteger();
        final AtomicLong points = new AtomicLong(1000);
        Supplier<BenchmarkRootCpuEnvelope.Identity> identity = () -> ROOT;
        LongSupplier clock = points::incrementAndGet;
        LongSupplier cpu;
        BenchmarkRootCpuEnvelope.Waiter waiter = WAIT;
        Fixture(long... values) { cpu = () -> values[cpuCalls.get() - 1]; }
        BenchmarkRootCpuEnvelope open(Set<String> common, Set<String> tail) {
            return BenchmarkRootCpuEnvelope.from(ROOT, new BenchmarkRootCpuEnvelope.Provider() {
                @Override public BenchmarkRootCpuEnvelope.Identity identity() { identityCalls.incrementAndGet(); return identity.get(); }
                @Override public long processCpuTime() { cpuCalls.incrementAndGet(); return cpu.getAsLong(); }
            }, clock, common, tail, waiter);
        }
    }

    private static BenchmarkRootCpuEnvelope.Refusal refusal(Runnable operation) {
        try { operation.run(); } catch (BenchmarkRootCpuEnvelope.Refusal failure) {
            assertThat(failure.retainedEvidence().get("state")).isEqualTo("UNKNOWN"); return failure;
        }
        throw new AssertionError("expected root CPU refusal");
    }

    @SuppressWarnings("unchecked") private static Map<String, Object> map(Map<String, Object> values, String key) { return (Map<String, Object>) values.get(key); }
    @SuppressWarnings("unchecked") private static List<Map<String, Object>> rows(Map<String, Object> values, String key) { return (List<Map<String, Object>>) values.get(key); }
    private static void assertUnqualified(Map<String, Object> evidence) {
        for (String flag : List.of("accountingErrorBoundQualified", "wholeMethodCostQualified", "collectionCostUpperBoundQualified",
                "samplingCostQualified", "causalOverheadQualified", "costAcceptanceEligible", "performanceAcceptanceEligible", "formalPerformance")) {
            assertThat(evidence).containsEntry(flag, false);
        }
    }
    private static void requireBudget(long timeout, TimeUnit unit) {
        assertThat(unit).isEqualTo(TimeUnit.NANOSECONDS);
        assertThat(timeout).isPositive().isLessThanOrEqualTo(BenchmarkRootCpuEnvelope.WAIT_LIMIT.toNanos());
    }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(2, TimeUnit.SECONDS)) { throw new AssertionError("root CPU control barrier did not complete"); } }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError("root CPU control barrier interrupted", interrupted); }
    }
    private static void awaitReading(BenchmarkRootCpuEnvelope envelope) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (rows(envelope.evidence(), "readings").isEmpty()) {
            if (System.nanoTime() - deadline >= 0) { throw new AssertionError("late root CPU reading did not complete"); }
            Thread.onSpinWait();
        }
    }
}
