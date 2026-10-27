package io.tapstate.control.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class StartCheckSchedulingTest {
    private final StartChecksBench bench = new StartChecksBench();

    @AfterEach
    void close() {
        bench.executor.close();
    }

    @Test
    void aBoundedCheckPoolDoesNotStarveItsNestedTargetProbe() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 4L, true));
        bench.budget = Duration.ofSeconds(5);
        var checks = Executors.newSingleThreadExecutor();
        try {
            StartEvaluation evaluated = bench.evaluator(checks, Clock.systemUTC()).evaluate("pl", bench.stored("pl"),
                    bench.hash("pl"), Optional.empty(), StartIntent.START);
            assertThat(evaluated.findings()).singleElement().satisfies(finding -> {
                assertThat(finding.behavior()).isEqualTo(StartFinding.Behavior.CONFIRM);
                assertThat(finding.code()).isEqualTo(StartCheckCode.TARGET_NOT_EMPTY);
            });
        } finally {
            checks.shutdownNow();
        }
    }

    @Test
    void aProbeReportsTheWaitActuallyPassedToItsFuture() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        var executor = new TimingOutExecutor();
        StartPlan plan = new StartPlan(io.tapstate.spi.store.StartLoad.FULL_LOAD,
                bench.writeTargets(bench.stored("pl")).stream().map(target ->
                        new StartPlan.Entry(target, io.tapstate.spi.store.StartLoad.FULL_LOAD)).toList());
        StartCheckContext context = new StartCheckContext("pl", StartIntent.START, bench.stored("pl"),
                bench.hash("pl"), plan, null, (connection, table) -> TargetProbe.TargetRows.EMPTY,
                executor, StartChecksBench.CLOCK, StartChecksBench.CLOCK.instant().plusMillis(250),
                Duration.ofMillis(1500), coded -> coded.code().code());

        StartCheckContext.Probed.Unknown unknown = (StartCheckContext.Probed.Unknown)
                context.probe(plan.entries()).get("warehouse/orders");

        assertThat(executor.waitMillis).isEqualTo(250);
        assertThat(unknown.reason()).contains("250 ms").doesNotContain("1500 ms");
    }

    @Test
    void aCheckReportsItsRemainingWaitInsteadOfTheWholeBudget() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        var advancing = new MutableClock();
        var executor = new TimingOutExecutor(() -> advancing.advance(Duration.ofMillis(1700)));

        StartEvaluation evaluated = bench.evaluator(executor, advancing).evaluate("pl", bench.stored("pl"),
                bench.hash("pl"), Optional.empty(), StartIntent.START);

        assertThat(executor.waitMillis).isEqualTo(100);
        assertThat(evaluated.findings()).singleElement().satisfies(finding ->
                assertThat((String) finding.params().get("reason")).contains("100 ms").doesNotContain("1800 ms"));
    }

    @Test
    void pipelinesKnownNotToUnwindNeverQueueAnIdentityCheck() {
        bench.checks.add(new UnwindTargetIdentityCheck(new PipelineTargetIdentities() {
            @Override public boolean relevant(io.tapstate.core.model.PipelineResource definition) {
                throw new AssertionError("irrelevant identity checks must not read metadata");
            }
            @Override public List<Assessment> assess(io.tapstate.core.model.PipelineResource definition, boolean fullLoad) {
                throw new AssertionError("irrelevant identity checks must not assess targets");
            }
        }));
        for (String transforms : List.of("", "transforms: []\n",
                "transforms: [{id: filtered, type: filter, from: orders, expr: \"op != 'd'\"}]\n")) {
            bench.artifacts.put(StartChecksBench.pipeline("clear").replace("serve:", transforms + "serve:"));
            var clock = new MutableClock();
            var executor = new TimingOutExecutor(() -> clock.advance(StartCheckEvaluator.BUDGET));

            StartEvaluation evaluated = bench.evaluator(executor, clock).evaluate("pl", bench.stored("pl"),
                    bench.hash("pl"), Optional.empty(), StartIntent.START);

            assertThat(executor.submissions).as("transforms: %s", transforms).isEqualTo(1);
            assertThat(evaluated.outcome(java.util.Set.of())).isEqualTo(StartCheckReport.Outcome.READY);
            assertThat(evaluated.findings()).singleElement().satisfies(finding -> {
                assertThat(finding.check()).isEqualTo(TargetNotEmptyCheck.ID);
                assertThat(finding.evaluation()).isEqualTo(StartFinding.Evaluation.UNAVAILABLE);
                assertThat(finding.params()).containsEntry("reason", "it did not answer during the remaining 0 ms wait");
            });
        }
    }

    @Test
    void unwindAndSharedTransformsStillBlockWhenTheirChecksCannotRun() {
        bench.checks.add(new UnwindTargetIdentityCheck((definition, fullLoad) -> {
            throw new AssertionError("the executor leaves checks queued");
        }));
        for (String transform : List.of(
                "{id: expanded, type: unwind, from: orders, path: items, include_array_index: position}",
                "{id: expanded, use: shared, from: orders}")) {
            bench.artifacts.put(StartChecksBench.pipeline("clear")
                    .replace("serve:", "transforms: [" + transform + "]\nserve:")
                    .replace("from: orders\n  sync:", "from: expanded\n  sync:"));
            var executor = new TimingOutExecutor();
            var service = new PipelineStartService(bench.lifecycle,
                    bench.evaluator(executor, StartChecksBench.CLOCK), bench.apply);

            StartRefusal refused = catchThrowableOfType(StartRefusal.class,
                    () -> service.start("alice", "pl", List.of(), null));

            assertThat(executor.submissions).isEqualTo(2);
            assertThat(executor.waitMillis).isEqualTo(StartCheckEvaluator.BUDGET.toMillis());
            assertThat(refused.code()).isEqualTo(io.tapstate.core.lifecycle.LifecycleError.START_BLOCKED);
            assertThat(refused.report().findings()).anySatisfy(finding -> {
                assertThat(finding.check()).isEqualTo(UnwindTargetIdentityCheck.CHECK_ID);
                assertThat(finding.behavior()).isEqualTo(StartFinding.Behavior.BLOCK);
                assertThat(finding.evaluation()).isEqualTo(StartFinding.Evaluation.UNAVAILABLE);
            });
            assertThat(bench.desiredById).isEmpty();
            assertThat(bench.audit).isEmpty();
        }
    }

    private static final class MutableClock extends Clock {
        private final AtomicReference<Instant> now = new AtomicReference<>(StartChecksBench.CLOCK.instant());
        void advance(Duration elapsed) { now.updateAndGet(instant -> instant.plus(elapsed)); }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now.get(); }
    }

    /** Exercises deadline reporting without scheduling work or sleeping. */
    private static final class TimingOutExecutor extends AbstractExecutorService {
        long waitMillis;
        int submissions;
        private final Runnable onSubmit;
        private boolean shutdown;
        TimingOutExecutor() { this(() -> { }); }
        TimingOutExecutor(Runnable onSubmit) { this.onSubmit = onSubmit; }
        @Override public void shutdown() { shutdown = true; }
        @Override public List<Runnable> shutdownNow() { shutdown = true; return List.of(); }
        @Override public boolean isShutdown() { return shutdown; }
        @Override public boolean isTerminated() { return shutdown; }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return shutdown; }
        @Override public void execute(Runnable task) { submissions++; onSubmit.run(); }
        @Override protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
            return new FutureTask<>(callable) {
                @Override public T get(long timeout, TimeUnit unit) throws TimeoutException {
                    waitMillis = unit.toMillis(timeout);
                    throw new TimeoutException("controlled deadline");
                }
            };
        }
    }
}
