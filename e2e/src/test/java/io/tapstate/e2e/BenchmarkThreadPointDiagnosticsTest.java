package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class BenchmarkThreadPointDiagnosticsTest {
    @Test
    void factoryAndUnselectedOrdinalsPerformNoRemoteGetters() {
        FakeReader reader = new FakeReader();
        var diagnostics = BenchmarkThreadPointDiagnostics.prepare(reader,
                () -> { throw new AssertionError("no read clock before a selected ordinal"); });
        for (long ordinal : new long[] {0, 2, 27, 29, 32, 34, 44, 46, 49, 51}) { diagnostics.recordAttempt(ordinal); }
        assertThat(reader.calls).isEmpty();
        assertThat(diagnostics.evidence().get("roleCoverage")).isEqualTo("UNKNOWN");
        assertThat(diagnostics.evidence().get("pointGroups")).isEqualTo(List.of());
    }

    @Test
    void ordinalOnePreparesOnceAndOnlyTheFourFrozenPointsReadCpuAndStacks() {
        FakeReader reader = new FakeReader();
        var diagnostics = diagnostics(reader);
        for (long ordinal = 1; ordinal <= 60; ordinal++) {
            diagnostics.recordAttempt(ordinal); diagnostics.recordAttempt(ordinal);
        }
        assertThat(reader.idsCalls).isEqualTo(1);
        assertThat(reader.shallowCalls).isEqualTo(1);
        assertThat(reader.cpuCalls).isEqualTo(4);
        assertThat(reader.stackCalls).isEqualTo(4);
        assertThat(points(diagnostics)).hasSize(4);
        assertThat(points(diagnostics).stream().map(point -> point.get("resourceAttemptOrdinal")).toList())
                .containsExactly(28L, 33L, 45L, 50L);
        assertThat(diagnostics.evidence().get("attemptedResourceOrdinals")).isEqualTo(List.of(1L, 28L, 33L, 45L, 50L));
    }

    @Test
    void unsupportedOrDisabledCpuDoesNotEnableAnythingOrRetryLiveReads() {
        for (boolean supported : List.of(false, true)) {
            FakeReader reader = new FakeReader();
            reader.capability = new BenchmarkThreadPointDiagnostics.CpuCapability(supported, supported ? false : null);
            var diagnostics = diagnostics(reader);
            diagnostics.recordAttempt(1);
            int calls = reader.calls.size();
            diagnostics.recordAttempt(1);
            for (long ordinal : BenchmarkThreadPointDiagnostics.POINT_ORDINALS) { diagnostics.recordAttempt(ordinal); }
            assertThat(reader.calls).hasSize(calls);
            assertThat(reader.idsCalls).isZero(); assertThat(reader.cpuCalls).isZero();
            assertThat(preparation(diagnostics).get("unknownReason")).isEqualTo(supported ? "CPU_DISABLED" : "CPU_UNSUPPORTED");
            assertThat(points(diagnostics)).allSatisfy(point -> assertThat(point.get("state")).isEqualTo("UNKNOWN"));
        }
    }

    @Test
    void identityChangesAreUnknownWithoutComparingRemoteStartTimeToOsWallTime() {
        FakeReader reader = new FakeReader();
        var diagnostics = diagnostics(reader);
        diagnostics.recordAttempt(1);
        reader.identity = new BenchmarkThreadPointDiagnostics.Identity(71, 1001, true);
        diagnostics.recordAttempt(28);
        assertThat(points(diagnostics).getFirst().get("unknownReason")).isEqualTo("IDENTITY_CHANGED");
        assertThat(reader.cpuCalls).isZero();

        FakeReader wrongPid = new FakeReader();
        wrongPid.identity = new BenchmarkThreadPointDiagnostics.Identity(72, 1000, true);
        var mismatch = diagnostics(wrongPid); mismatch.recordAttempt(1);
        assertThat(preparation(mismatch).get("unknownReason")).isEqualTo("IDENTITY_MISMATCH");
    }

    @Test
    void realZeroAndIndependentRpcBracketsAreRetainedWithOnlyPositiveFrameRoles() {
        FakeReader reader = new FakeReader();
        var diagnostics = diagnostics(reader);
        diagnostics.recordAttempt(1); diagnostics.recordAttempt(28); diagnostics.recordAttempt(33);
        var point = points(diagnostics).getLast();
        assertThat(point.get("state")).isEqualTo("RECORDED");
        Map<?, ?> thread = thread(point);
        assertThat(thread.get("rawCpuNanos")).isEqualTo(0L);
        assertThat(thread.get("conditionalCpuDeltaNanos")).isEqualTo(0L);
        assertThat(thread.get("role")).isEqualTo("CAPTURE_ADMISSION");
        List<?> reads = (List<?>) point.get("reads");
        Map<?, ?> cpuRead = (Map<?, ?>) reads.get(2), stackRead = (Map<?, ?>) reads.get(3);
        assertThat(cpuRead.get("operation")).isEqualTo("BULK_CPU");
        assertThat(stackRead.get("operation")).isEqualTo("STACK_POINT");
        assertThat((Long) stackRead.get("startedAtNanos")).isGreaterThan((Long) cpuRead.get("completedAtNanos"));
        assertThat(cpuRead.get("durationNanos")).isEqualTo(10L);
    }

    @Test
    void missingThreadInfoAndNegativeCpuRemainUnknownRatherThanZero() {
        for (boolean missing : List.of(true, false)) {
            FakeReader reader = new FakeReader();
            var diagnostics = diagnostics(reader); diagnostics.recordAttempt(1);
            if (missing) { reader.stack = java.util.Collections.singletonList(null); }
            else { reader.cpu = new long[] {-1}; }
            diagnostics.recordAttempt(28);
            var point = points(diagnostics).getFirst();
            assertThat(point.get("state")).isEqualTo("UNKNOWN");
            assertThat(thread(point).containsKey("conditionalCpuDeltaNanos")).isFalse();
            assertThat(point.get("unknownReason")).isEqualTo(missing ? "THREAD_INFO_MISSING" : "CPU_UNAVAILABLE");
        }
    }

    @Test
    void cpuDecreaseAndThreadNameChangeCannotBecomeQualifiedDeltas() {
        for (boolean decrease : List.of(true, false)) {
            FakeReader reader = new FakeReader(); reader.cpu = new long[] {100};
            var diagnostics = diagnostics(reader); diagnostics.recordAttempt(1); diagnostics.recordAttempt(28);
            if (decrease) { reader.cpu = new long[] {99}; }
            else { reader.stack = List.of(row("tapstate-cdc-another", frames())); }
            diagnostics.recordAttempt(33);
            var point = points(diagnostics).getLast();
            assertThat(point.get("state")).isEqualTo("UNKNOWN");
            assertThat(point.get("unknownReason")).isEqualTo(decrease ? "CPU_DECREASED" : "THREAD_NAME_CHANGED");
            assertThat(thread(point).containsKey("conditionalCpuDeltaNanos")).isFalse();
        }
    }

    @Test
    void rosterAndCandidateLimitsRejectInsteadOfTruncatingOrSelectingBlcThreads() {
        for (int count : new int[] {9, 513}) {
            FakeReader reader = new FakeReader(); reader.ids = new long[count];
            var shallow = new ArrayList<BenchmarkThreadPointDiagnostics.ThreadRow>();
            for (int i = 0; i < count; i++) { reader.ids[i] = i + 1; shallow.add(new BenchmarkThreadPointDiagnostics.ThreadRow(i + 1,
                    "tapstate-cdc-mysql", "RUNNABLE", List.of())); }
            reader.shallow = shallow;
            var diagnostics = diagnostics(reader); diagnostics.recordAttempt(1);
            assertThat(preparation(diagnostics).get("unknownReason")).isEqualTo(count == 513 ? "THREAD_ROSTER_BOUND" : "CANDIDATE_BOUND");
        }
        FakeReader blc = new FakeReader(); blc.shallow = List.of(row("blc-localhost:12345", List.of()));
        var diagnostics = diagnostics(blc); diagnostics.recordAttempt(1);
        assertThat(preparation(diagnostics).get("unknownReason")).isEqualTo("NO_CANDIDATES");
    }

    @Test
    void frameDepthAndMalformedCpuAreBoundedUnknownFacts() {
        FakeReader reader = new FakeReader();
        var diagnostics = diagnostics(reader); diagnostics.recordAttempt(1);
        reader.stack = List.of(row("tapstate-cdc-mysql", java.util.Collections.nCopies(64, frames().getFirst())));
        diagnostics.recordAttempt(28);
        assertThat(thread(points(diagnostics).getFirst()).get("depthBoundReached")).isEqualTo(true);
        reader.stack = List.of(row("tapstate-cdc-mysql", java.util.Collections.nCopies(65, frames().getFirst())));
        diagnostics.recordAttempt(33);
        assertThat(points(diagnostics).getLast().get("unknownReason")).isEqualTo("FRAME_BOUND");
        reader.cpu = new long[0]; diagnostics.recordAttempt(45);
        assertThat(points(diagnostics).getLast().get("unknownReason")).isEqualTo("MALFORMED_CPU");
    }

    @Test
    void ordinaryRpcErrorsAreUnknownAndProgrammerErrorsEscapeAsTheOriginalObject() {
        FakeReader ordinary = new FakeReader() {
            @Override public long[] cpuNanos(long[] ids) { throw new IllegalStateException("failed external read"); }
        };
        var diagnostics = diagnostics(ordinary); diagnostics.recordAttempt(1); diagnostics.recordAttempt(28);
        assertThat(points(diagnostics).getFirst().get("unknownReason")).isEqualTo("RPC_ERROR");
        AssertionError invariant = new AssertionError("reader invariant");
        FakeReader fatal = new FakeReader() {
            @Override public BenchmarkThreadPointDiagnostics.Identity identity() { throw invariant; }
        };
        var failure = diagnostics(fatal);
        assertThatThrownBy(() -> failure.recordAttempt(1)).isSameAs(invariant);
        assertThat(preparation(failure).get("unknownReason")).isEqualTo("INVARIANT_ERROR");
        assertThat(failure.evidence().containsKey("pending")).isFalse();
    }

    @Test
    void preparationAndStackPendingSnapshotsAreReadableWithoutTheRpcLock() throws Exception {
        for (boolean preparing : List.of(true, false)) {
            CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
            FakeReader reader = new FakeReader() {
                private void hold() {
                    entered.countDown();
                    try {
                        if (!release.await(5, TimeUnit.SECONDS)) { throw new AssertionError("test RPC was not released"); }
                    } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                }
                @Override public BenchmarkThreadPointDiagnostics.Identity identity() {
                    if (preparing) { hold(); }
                    return super.identity();
                }
                @Override public List<BenchmarkThreadPointDiagnostics.ThreadRow> threadInfo(long[] ids, int depth) {
                    if (!preparing && depth > 0) { hold(); }
                    return super.threadInfo(ids, depth);
                }
            };
            var diagnostics = diagnostics(reader);
            if (!preparing) { diagnostics.recordAttempt(1); }
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread worker = Thread.ofPlatform().uncaughtExceptionHandler((thread, error) -> failure.set(error))
                    .start(() -> diagnostics.recordAttempt(preparing ? 1 : 28));
            try {
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                var snapshot = assertTimeoutPreemptively(Duration.ofSeconds(1), diagnostics::evidence);
                Map<?, ?> pending = (Map<?, ?>) snapshot.get("pending");
                assertThat(pending.get("operation")).isEqualTo(preparing ? "IDENTITY" : "STACK_POINT");
                assertThat(pending.containsKey("completedAtNanos")).isFalse();
                assertThat(pending.containsKey("durationNanos")).isFalse();
                List<?> completed = (List<?>) snapshot.get("openGroupCompletedReads");
                if (preparing) { assertThat(completed).isEmpty(); }
                else {
                    assertThat(completed).hasSize(3);
                    Map<?, ?> cpu = (Map<?, ?>) completed.getLast();
                    assertThat(cpu.get("operation")).isEqualTo("BULK_CPU");
                    assertThat(cpu.containsKey("completedAtNanos")).isTrue();
                    assertThat(cpu.containsKey("durationNanos")).isTrue();
                }
                assertThat(JsonWriter.write(assertTimeoutPreemptively(Duration.ofSeconds(1), diagnostics::wireEvidence)))
                        .contains("startedAtNanos");
            } finally { release.countDown(); worker.join(2_000); }
            assertThat(worker.isAlive()).isFalse(); assertThat(failure.get()).isNull();
        }
    }

    @Test
    void anOriginalErrorSurvivesBackwardOverflowOrUnavailableCompletionClock() {
        for (String mode : List.of("backward", "overflow", "unavailable")) {
            AssertionError sourceFailure = new AssertionError("original reader invariant");
            FakeReader reader = new FakeReader() {
                @Override public BenchmarkThreadPointDiagnostics.Identity identity() { throw sourceFailure; }
            };
            AtomicLong calls = new AtomicLong();
            var diagnostics = BenchmarkThreadPointDiagnostics.prepare(reader, () -> {
                if (calls.getAndIncrement() == 0) { return mode.equals("overflow") ? Long.MIN_VALUE : 10; }
                if (mode.equals("unavailable")) { throw new IllegalStateException("completion clock unavailable"); }
                return mode.equals("overflow") ? Long.MAX_VALUE : 9;
            });
            assertThatThrownBy(() -> diagnostics.recordAttempt(1)).isSameAs(sourceFailure);
            assertThat(preparation(diagnostics).containsKey("totalReadDurationNanos")).isFalse();
            assertThat(preparation(diagnostics).containsKey("maxReadDurationNanos")).isFalse();
            List<?> reads = (List<?>) preparation(diagnostics).get("reads");
            assertThat(reads).hasSize(1);
            Map<?, ?> actual = (Map<?, ?>) reads.getFirst();
            assertThat(actual.get("startedAtNanos")).isEqualTo(mode.equals("overflow") ? Long.MIN_VALUE : 10L);
            assertThat(actual.containsKey("durationNanos")).isFalse();
            if (mode.equals("unavailable")) {
                assertThat(actual.containsKey("completedAtNanos")).isFalse();
                assertThat(actual.get("clockFailureType")).isEqualTo(IllegalStateException.class.getName());
            } else {
                assertThat(actual.get("completedAtNanos")).isEqualTo(mode.equals("overflow") ? Long.MAX_VALUE : 9L);
                assertThat(actual.get("clockUnknownReason")).isEqualTo(mode.equals("overflow") ? "OVERFLOW" : "CLOCK_BACKWARD");
            }
        }
    }

    @Test
    void snapshotsAreImmutableJsonSafeAndClockOverflowDoesNotInventDuration() {
        FakeReader reader = new FakeReader(); var diagnostics = diagnostics(reader);
        diagnostics.recordAttempt(1); diagnostics.recordAttempt(28);
        var snapshot = diagnostics.evidence();
        assertThatThrownBy(snapshot::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> points(diagnostics).getFirst().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(JsonWriter.write(snapshot)).contains("CAPTURE_ADMISSION", "nativeMethod", "lineNumber")
                .doesNotContain("java.lang.management.ThreadInfo", "\"lockName\":", "\"lockOwnerId\":", "OptionalLong");
        AtomicLong call = new AtomicLong();
        var overflow = BenchmarkThreadPointDiagnostics.prepare(new FakeReader(),
                () -> call.getAndIncrement() == 0 ? Long.MIN_VALUE : Long.MAX_VALUE);
        overflow.recordAttempt(1);
        assertThat(preparation(overflow).get("unknownReason")).isEqualTo("OVERFLOW");
    }

    private static BenchmarkThreadPointDiagnostics diagnostics(FakeReader reader) {
        AtomicLong clock = new AtomicLong();
        return BenchmarkThreadPointDiagnostics.prepare(reader, () -> clock.getAndAdd(10));
    }
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> points(BenchmarkThreadPointDiagnostics diagnostics) {
        return (List<Map<String, Object>>) diagnostics.evidence().get("pointGroups");
    }
    private static Map<?, ?> preparation(BenchmarkThreadPointDiagnostics diagnostics) { return (Map<?, ?>) diagnostics.evidence().get("preparation"); }
    private static Map<?, ?> thread(Map<String, Object> point) { return (Map<?, ?>) ((List<?>) point.get("threads")).getFirst(); }
    private static List<BenchmarkThreadPointDiagnostics.Frame> frames() {
        return List.of(new BenchmarkThreadPointDiagnostics.Frame("io.tapstate.runtime.srs.CdcPhase", "admit", false, 352));
    }
    private static BenchmarkThreadPointDiagnostics.ThreadRow row(String name, List<BenchmarkThreadPointDiagnostics.Frame> frames) {
        return new BenchmarkThreadPointDiagnostics.ThreadRow(11, name, "WAITING", frames);
    }

    static class FakeReader implements BenchmarkThreadPointDiagnostics.Reader {
        BenchmarkThreadPointDiagnostics.Identity identity = new BenchmarkThreadPointDiagnostics.Identity(71, 1000, true);
        BenchmarkThreadPointDiagnostics.CpuCapability capability = new BenchmarkThreadPointDiagnostics.CpuCapability(true, true);
        long[] ids = {11}; long[] cpu = {0};
        List<BenchmarkThreadPointDiagnostics.ThreadRow> shallow = List.of(row("tapstate-cdc-mysql", List.of()));
        List<BenchmarkThreadPointDiagnostics.ThreadRow> stack = List.of(row("tapstate-cdc-mysql", frames()));
        final List<String> calls = new ArrayList<>();
        int idsCalls, shallowCalls, stackCalls, cpuCalls;
        @Override public long ownedPid() { return 71; }
        @Override public BenchmarkThreadPointDiagnostics.Identity identity() { calls.add("identity"); return identity; }
        @Override public BenchmarkThreadPointDiagnostics.CpuCapability cpuCapability() { calls.add("capability"); return capability; }
        @Override public long[] threadIds() { calls.add("ids"); idsCalls++; return ids.clone(); }
        @Override public long[] cpuNanos(long[] ids) { calls.add("cpu"); cpuCalls++; return cpu.clone(); }
        @Override public List<BenchmarkThreadPointDiagnostics.ThreadRow> threadInfo(long[] ids, int depth) {
            calls.add(depth == 0 ? "shallow" : "stack");
            if (depth == 0) { shallowCalls++; return shallow; }
            stackCalls++; return stack;
        }
    }
}
