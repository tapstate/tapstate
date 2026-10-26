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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

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
        var executor = new TimingOutExecutor();
        AtomicInteger reads = new AtomicInteger();
        Clock advancing = new Clock() {
            @Override public ZoneId getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(ZoneId zone) { return this; }
            @Override public Instant instant() {
                return StartChecksBench.CLOCK.instant().plusMillis(reads.getAndIncrement() < 2 ? 0 : 1700);
            }
        };

        StartEvaluation evaluated = bench.evaluator(executor, advancing).evaluate("pl", bench.stored("pl"),
                bench.hash("pl"), Optional.empty(), StartIntent.START);

        assertThat(executor.waitMillis).isEqualTo(100);
        assertThat(evaluated.findings()).singleElement().satisfies(finding ->
                assertThat((String) finding.params().get("reason")).contains("100 ms").doesNotContain("1800 ms"));
    }

    @Test
    void slowPlanningDoesNotExpireChecksBeforeTheyAreSubmitted() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        var now = new java.util.concurrent.atomic.AtomicReference<>(StartChecksBench.CLOCK.instant());
        Clock clock = new Clock() {
            @Override public ZoneId getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(ZoneId zone) { return this; }
            @Override public Instant instant() { return now.get(); }
        };
        var planner = new StartPlanner(pipelineId -> List.of(),
                (io.tapstate.spi.store.SrsMetaStore) java.lang.reflect.Proxy.newProxyInstance(
                        getClass().getClassLoader(), new Class<?>[] {io.tapstate.spi.store.SrsMetaStore.class},
                        (proxy, method, args) -> { throw new AssertionError("no chain is read"); }), definition -> {
                    now.updateAndGet(value -> value.plusSeconds(30));
                    return bench.writeTargets(definition);
                });
        var executor = new CompletingOnWaitExecutor();
        StartCheck check = new StartCheck() {
            @Override public String id() { return "quick-identity"; }
            @Override public StartFinding.Behavior whenUnavailable() { return StartFinding.Behavior.BLOCK; }
            @Override public List<StartFinding> evaluate(StartCheckContext context) { return List.of(); }
        };
        var evaluator = new StartCheckEvaluator(List.of(check), planner,
                (connection, table) -> TargetProbe.TargetRows.EMPTY, (code, params) -> code.code(),
                executor, clock);

        var evaluated = evaluator.evaluate("pl", bench.stored("pl"), bench.hash("pl"),
                Optional.empty(), StartIntent.START);

        assertThat(evaluated.findings()).isEmpty();
        assertThat(executor.waitMillis).isEqualTo(StartCheckEvaluator.BUDGET.toMillis());
        assertThat(evaluated.evaluatedAt()).isEqualTo(StartChecksBench.CLOCK.instant());
    }

    /** Starts a queued fast check only after its caller grants a positive wait budget. */
    private static final class CompletingOnWaitExecutor extends AbstractExecutorService {
        long waitMillis;
        @Override public void shutdown() { }
        @Override public List<Runnable> shutdownNow() { return List.of(); }
        @Override public boolean isShutdown() { return false; }
        @Override public boolean isTerminated() { return false; }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return false; }
        @Override public void execute(Runnable task) { }
        @Override protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
            return new FutureTask<>(callable) {
                @Override public T get(long timeout, TimeUnit unit)
                        throws InterruptedException, java.util.concurrent.ExecutionException, TimeoutException {
                    waitMillis = unit.toMillis(timeout);
                    if (waitMillis <= 0) throw new TimeoutException("check has not started");
                    run();
                    return super.get(timeout, unit);
                }
            };
        }
    }

    /** Exercises deadline reporting without scheduling work or sleeping. */
    private static final class TimingOutExecutor extends AbstractExecutorService {
        long waitMillis;
        private boolean shutdown;
        @Override public void shutdown() { shutdown = true; }
        @Override public List<Runnable> shutdownNow() { shutdown = true; return List.of(); }
        @Override public boolean isShutdown() { return shutdown; }
        @Override public boolean isTerminated() { return shutdown; }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return shutdown; }
        @Override public void execute(Runnable task) { }
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
