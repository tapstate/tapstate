package io.tapstate.e2e;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkTargetClockSamplerTest {
    @Test void unused_setup_ping_reply_does_not_add_a_material_traversal_or_refuse_complete_request_counts() throws Exception {
        var calls = new AtomicInteger(); var traversals = new AtomicInteger();
        var reply = new org.bson.Document() {
            @Override public int size() { traversals.incrementAndGet(); throw new AssertionError("unused reply was traversed"); }
        };
        var admin = (com.mongodb.client.MongoDatabase) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{com.mongodb.client.MongoDatabase.class}, (proxy, method, args) -> {
                    assertThat(method.getName()).isEqualTo("runCommand");
                    assertThat(args).containsExactly(new org.bson.Document("ping", 1));
                    calls.incrementAndGet(); return reply;
                });
        try (var scope = BenchmarkReturnWorkBudget.activate(pingDefinition(), "unused-ping-reply")) {
            scope.bindRoot(17, 23);
            BenchmarkTargetClockSampler.ping(admin);
            var prepared = scope.prepareOutput("control", new BenchmarkReturnWorkBudget.TreePlan(16, 128, 1024, 8), java.util.Map.of("ping", "complete"));
            scope.writeOutput(prepared, bytes -> assertThat(bytes.length).isPositive());
            var closed = scope.finish();
            assertThat(closed).containsEntry("state", "CLOSED");
            @SuppressWarnings("unchecked") var activities = (java.util.Map<String, Object>) closed.get("activities");
            @SuppressWarnings("unchecked") var ping = (java.util.Map<String, Object>) activities.get("PING_REQUEST");
            assertThat(ping).containsEntry("attempts", 1L).containsEntry("completedAttempts", 1L)
                    .containsEntry("issuedRequests", 1L).containsEntry("completedRequests", 1L).doesNotContainKey("gap");
        }
        assertThat(calls).hasValue(1); assertThat(traversals).hasValue(0);
    }

    @Test void setup_ping_failure_preserves_the_exact_original_and_counts_one_failed_request() {
        var calls = new AtomicInteger(); var primary = new IllegalStateException("original ping refusal");
        var admin = (com.mongodb.client.MongoDatabase) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{com.mongodb.client.MongoDatabase.class}, (proxy, method, args) -> {
                    calls.incrementAndGet(); throw primary;
                });
        try (var scope = BenchmarkReturnWorkBudget.activate(pingDefinition(), "failed-ping")) {
            assertThatThrownBy(() -> BenchmarkTargetClockSampler.ping(admin)).isSameAs(primary);
            assertThat(scope.primaryCause()).isSameAs(primary);
            @SuppressWarnings("unchecked") var activities = (java.util.Map<String, Object>) scope.snapshot().get("activities");
            @SuppressWarnings("unchecked") var ping = (java.util.Map<String, Object>) activities.get("PING_REQUEST");
            assertThat(ping).containsEntry("attempts", 1L).containsEntry("issuedRequests", 1L)
                    .containsEntry("failedRequests", 1L).containsEntry("failedAttempts", 1L);
        }
        assertThat(calls).hasValue(1);
    }

    private static BenchmarkReturnWorkBudget.Definition pingDefinition() {
        var upper = new java.util.EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        for (var activity : BenchmarkReturnWorkBudget.Activity.values()) {
            upper.put(activity, new BenchmarkReturnWorkBudget.Work(10, 10, 100, 100, 100_000, 100, 10_000, 10_000));
        }
        return new BenchmarkReturnWorkBudget.Definition("controlled-ping", "a".repeat(64), java.util.Map.of(), upper, java.util.Set.of());
    }
    @Test void ownedSamplingStopsWithoutErasingItsClockReadFailure() {
        var calls = new AtomicInteger();
        var closed = new AtomicInteger();
        var sampler = new BenchmarkTargetClockSampler(() -> {
            calls.incrementAndGet(); throw new IllegalStateException("owned clock unavailable");
        }, closed::incrementAndGet);
        sampler.close(); sampler.close();
        assertThat(calls).hasValue(1);
        assertThat(closed).hasValue(1);
        assertThatThrownBy(sampler::evidence).isInstanceOf(AssertionError.class).hasRootCauseMessage("owned clock unavailable");
        assertThatThrownBy(sampler::evidenceForAck).isInstanceOf(AssertionError.class).hasRootCauseMessage("owned clock unavailable");
    }

    @Test void ack_evidence_uses_the_same_closed_readings_and_keeps_legacy_wall_refusal() throws Exception {
        var calls = new AtomicInteger(); var closed = new AtomicInteger(); var third = new CountDownLatch(1);
        var sampler = new BenchmarkTargetClockSampler(() -> {
            int index = calls.getAndIncrement(); long start = index * 200_000_000L;
            long wall = 1_000 + index * 300L;
            if (index >= 2) third.countDown();
            return new BenchmarkTargetClock.Reading("owned:27017", "one", wall, start, start + 1, wall, wall);
        }, closed::incrementAndGet, true);
        try {
            assertThatThrownBy(sampler::evidenceForAck).isInstanceOf(IllegalStateException.class);
            assertThat(third.await(2, TimeUnit.SECONDS)).isTrue();
        } finally { sampler.close(); }
        int acquired = calls.get();
        var raw = sampler.readings(); var ack = sampler.evidenceForAck();
        assertThat(ack).containsEntry("helloCommands", acquired).containsEntry("setupPingCommands", 1)
                .containsEntry("scope", "ACK_TARGET_PRIMARY_HELLO_CONTEXT_WITH_WALL_DIAGNOSTICS");
        assertThat(raw).hasSize(acquired); assertThat(sampler.readings()).isEqualTo(raw);
        assertThatThrownBy(sampler::evidence).isInstanceOf(AssertionError.class).hasMessageContaining("clock stepped");
        assertThat(calls).hasValue(acquired); assertThat(closed).hasValue(1);
        for (String flag : BenchmarkReturnProducerOrigins.FLAGS) assertThat(ack).containsEntry(flag, false);
    }

    @Test void default_sampling_latches_a_reversed_utc_read_and_stops_further_provider_requests() throws Exception {
        var calls = new AtomicInteger(); var closed = new AtomicInteger();
        var sampler = new BenchmarkTargetClockSampler(() -> {
            calls.incrementAndGet();
            return new BenchmarkTargetClock.Reading("owned:27017", "one", 1000, 0, 1, 1000, 999);
        }, closed::incrementAndGet);
        try {
            assertThat(sampler.samplesRecorded()).isZero();
            TimeUnit.MILLISECONDS.sleep(450);
        } finally { sampler.close(); }
        assertThat(calls).hasValue(1); assertThat(closed).hasValue(1);
        assertThatThrownBy(sampler::evidence).isInstanceOf(AssertionError.class)
                .hasMessageContaining("target clock sampling failed").hasRootCauseMessage("target clock read has no ordered actual identity");
    }

    @Test void explicit_ack_sampling_retains_reversed_utc_reads_and_the_same_serial_roster() throws Exception {
        var calls = new AtomicInteger(); var third = new CountDownLatch(1);
        var sampler = new BenchmarkTargetClockSampler(() -> {
            int index = calls.getAndIncrement(); long start = index * 200_000_000L;
            long wall = 1000 + index * 200L;
            if (index >= 2) third.countDown();
            return new BenchmarkTargetClock.Reading("owned:27017", "one", wall, start, start + 1, wall, wall - 1);
        }, () -> { }, true);
        try { assertThat(third.await(2, TimeUnit.SECONDS)).isTrue(); }
        finally { sampler.close(); }
        var ack = sampler.evidenceForAck();
        assertThat(ack).containsEntry("helloCommands", calls.get()).containsEntry("samples", calls.get())
                .containsEntry("adjacentWallRefusals", (long) calls.get() - 1);
        assertThat(sampler.readings()).hasSize(calls.get());
        assertThatThrownBy(sampler::evidence).isInstanceOf(AssertionError.class)
                .hasMessageContaining("no ordered actual identity");
    }

    @Test void default_utc_refusal_is_the_original_failed_provider_request_before_publication() {
        var calls = new AtomicInteger();
        try (var scope = BenchmarkReturnWorkBudget.activate(pingDefinition(), "utc-read-request-refusal")) {
            var sampler = new BenchmarkTargetClockSampler(() -> {
                calls.incrementAndGet();
                return new BenchmarkTargetClock.Reading("owned:27017", "one", 1000, 0, 1, 1000, 999);
            }, () -> { });
            sampler.close();
            assertThat(calls).hasValue(1); assertThat(sampler.samplesRecorded()).isZero();
            @SuppressWarnings("unchecked") var activities = (java.util.Map<String, Object>) scope.snapshot().get("activities");
            @SuppressWarnings("unchecked") var read = (java.util.Map<String, Object>) activities.get("HELLO_READ");
            assertThat(read).containsEntry("attempts", 1L).containsEntry("issuedRequests", 1L)
                    .containsEntry("failedRequests", 1L).containsEntry("failedAttempts", 1L)
                    .containsEntry("completedRequests", 1L).containsEntry("completedAttempts", 1L);
            assertThat(scope.primaryCause()).isInstanceOf(AssertionError.class)
                    .hasMessage("target clock read has no ordered actual identity");
            assertThatThrownBy(sampler::evidence).isInstanceOf(AssertionError.class).hasCause(scope.primaryCause());
            assertThat(scope.snapshot()).containsEntry("pendingOperations", 0L);
        }
    }

    @Test void fixedCadencePreservesActualReadBracketsAndItsCommandCount() throws Exception {
        var calls = new AtomicInteger();
        var closed = new AtomicInteger();
        var third = new CountDownLatch(1);
        long anchor = System.nanoTime();
        var sampler = new BenchmarkTargetClockSampler(() -> {
            long began = System.nanoTime(), elapsed = (began - anchor) / 1_000_000L;
            if (calls.incrementAndGet() >= 3) { third.countDown(); }
            return new BenchmarkTargetClock.Reading("owned:27017", "one", 1_000 + elapsed,
                    began, System.nanoTime(), 1_000 + elapsed, 1_000 + elapsed);
        }, closed::incrementAndGet);
        try {
            assertThatThrownBy(sampler::evidence).isInstanceOf(IllegalStateException.class);
            assertThat(third.await(2, TimeUnit.SECONDS)).isTrue();
        } finally { sampler.close(); }
        assertThat(sampler.evidence()).containsEntry("helloCommands", calls.get()).containsEntry("setupPingCommands", 1);
        assertThat(closed).hasValue(1);
    }
}
