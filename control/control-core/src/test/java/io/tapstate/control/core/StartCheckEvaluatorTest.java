package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.LifecycleError;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * One start check's failure stays its own. A check that cannot tell -- a coded refusal, or no answer in
 * time -- becomes a finding that says so and never passes, while every other check's findings stand; a
 * check that throws anything else is a defect and fails the start with it.
 */
class StartCheckEvaluatorTest {

    private final StartChecksBench bench = new StartChecksBench();

    @AfterEach
    void close() {
        bench.executor.close();
    }

    @Test
    void aCheckThatCannotTellIsAFindingOfItsOwnAndTheOthersStillStand() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 4L, false));
        bench.checks.add(new Throwing("refused", new TapstateException(
                LifecycleError.UNKNOWN_PIPELINE, Map.of("pipeline", "elsewhere"), null)));
        bench.checks.add(new Slow("slow"));
        bench.budget = Duration.ofMillis(300);

        StartEvaluation evaluation = evaluate();

        assertThat(evaluation.findings()).extracting(StartFinding::key)
                .containsExactly("target-not-empty/warehouse/orders", "refused/pl", "slow/pl");
        StartFinding refused = evaluation.finding("refused/pl").orElseThrow();
        assertThat(refused.behavior()).isEqualTo(StartFinding.Behavior.WARN);
        assertThat(refused.evaluation()).isEqualTo(StartFinding.Evaluation.UNAVAILABLE);
        assertThat(refused.params()).containsEntry("check", "refused")
                .containsEntry("reasonCode", "lifecycle.unknown-pipeline");
        StartFinding slow = evaluation.finding("slow/pl").orElseThrow();
        assertThat(slow.behavior()).isEqualTo(StartFinding.Behavior.WARN);
        assertThat(slow.evaluation()).isEqualTo(StartFinding.Evaluation.UNAVAILABLE);
        assertThat((String) slow.params().get("reason")).contains("did not answer");
        assertThat(evaluation.finding("target-not-empty/warehouse/orders").orElseThrow().behavior())
                .isEqualTo(StartFinding.Behavior.CONFIRM);
        assertThat(evaluation.outcome(Set.of())).isEqualTo(StartCheckReport.Outcome.NEEDS_CONFIRMATION);
    }

    @Test
    void aCheckThatThrowsAnUncodedErrorFailsTheStartWithIt() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        IllegalStateException defect = new IllegalStateException("check defect");
        bench.checks.add(new Throwing("broken", defect));

        assertThatThrownBy(this::evaluate).isSameAs(defect);
    }

    @Test
    void aCheckCannotDeclareThatNotKnowingPasses() {
        bench.checks.add(new StartCheck() {
            @Override
            public String id() {
                return "optimist";
            }

            @Override
            public List<StartFinding> evaluate(StartCheckContext context) {
                return List.of();
            }

            @Override
            public StartFinding.Behavior whenUnavailable() {
                return StartFinding.Behavior.PASS;
            }
        });
        assertThatThrownBy(bench::evaluator).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("optimist");
    }

    @Test
    void aCheckIsNamedInLowerKebabOrRefused() {
        for (String id : List.of("target-not-empty", "a", "a1", "x-1-2")) {
            bench.checks.clear();
            bench.checks.add(named(id));
            assertThatCode(bench::evaluator).as("check id %s", id).doesNotThrowAnyException();
        }
        for (String id : List.of("Target", "target--x", "-target", "target-", "1target", "a_b", "")) {
            bench.checks.clear();
            bench.checks.add(named(id));
            assertThatThrownBy(bench::evaluator).as("check id '%s'", id)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("lower-kebab");
        }
    }

    /** Matching an id walks its segments without recursing once per segment, however many there are. */
    @Test
    void aLongIdIsCheckedWithoutRunningOutOfStack() {
        bench.checks.clear();
        bench.checks.add(named("a" + "-a".repeat(100_000)));

        assertThatCode(bench::evaluator).doesNotThrowAnyException();
    }

    private static StartCheck named(String id) {
        return new StartCheck() {
            @Override
            public String id() {
                return id;
            }

            @Override
            public List<StartFinding> evaluate(StartCheckContext context) {
                return List.of();
            }

            @Override
            public StartFinding.Behavior whenUnavailable() {
                return StartFinding.Behavior.WARN;
            }
        };
    }

    @Test
    void aStartWhosePlanCannotBeWorkedOutIsAFindingThatSaysSo() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        StartCheckEvaluator evaluator = new StartCheckEvaluator(bench.checks,
                new StartPlanner(pipelineId -> List.of(), noRecords(), definition -> {
                    throw new TapstateException(LifecycleError.PIPELINE_NOT_RUNNABLE, Map.of("pipeline", "pl"), null);
                }),
                (connection, table) -> TargetProbe.TargetRows.EMPTY, (code, params) -> code.code(),
                bench.executor, StartChecksBench.CLOCK);

        StartEvaluation evaluation = evaluator.evaluate("pl", bench.stored("pl"), bench.hash("pl"),
                Optional.empty(), StartIntent.START);

        assertThat(evaluation.plan()).isNull();
        assertThat(evaluation.findings()).singleElement().satisfies(finding -> {
            assertThat(finding.key()).isEqualTo("target-not-empty/pl");
            assertThat(finding.behavior()).isEqualTo(StartFinding.Behavior.WARN);
            assertThat(finding.evaluation()).isEqualTo(StartFinding.Evaluation.UNAVAILABLE);
            assertThat(finding.params()).containsEntry("reasonCode", "lifecycle.pipeline-not-runnable");
        });
        assertThat(evaluator.report(evaluation, Set.of()).plan()).isEmpty();
    }

    @Test
    void theReportRendersEveryTextAndKeepsTheParamsTyped() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 1465L, false));
        StartCheckEvaluator evaluator = bench.evaluator();

        StartCheckReport report = evaluator.report(evaluate(), Set.of());

        assertThat(report.pipelineId()).isEqualTo("pl");
        assertThat(report.contentHash()).isEqualTo(bench.hash("pl"));
        assertThat(report.outcome()).isEqualTo(StartCheckReport.Outcome.NEEDS_CONFIRMATION);
        assertThat(report.plan()).singleElement().satisfies(entry -> {
            assertThat(entry.element()).isEqualTo("out");
            assertThat(entry.target()).isEqualTo(new StartCheckReport.Target("warehouse", "orders"));
            assertThat(entry.load()).isEqualTo(io.tapstate.spi.store.StartLoad.FULL_LOAD);
            assertThat(entry.onFullLoad()).isEqualTo("append");
        });
        StartCheckReport.Finding finding = report.findings().getFirst();
        assertThat(finding.code()).isEqualTo("start-check.target-not-empty");
        assertThat(finding.params()).containsEntry("rows", 1465L);
        assertThat(finding.message()).startsWith("start-check.target-not-empty ");
        assertThat(finding.actions()).extracting(StartCheckReport.Action::message)
                .allSatisfy(message -> assertThat(message).startsWith("start-check."));
    }

    private StartEvaluation evaluate() {
        return bench.evaluator().evaluate("pl", bench.stored("pl"), bench.hash("pl"), Optional.empty(),
                StartIntent.START);
    }

    private static io.tapstate.spi.store.SrsMetaStore noRecords() {
        return (io.tapstate.spi.store.SrsMetaStore) java.lang.reflect.Proxy.newProxyInstance(
                StartCheckEvaluatorTest.class.getClassLoader(),
                new Class<?>[] {io.tapstate.spi.store.SrsMetaStore.class}, (proxy, method, args) -> {
                    throw new AssertionError("no chain is read here");
                });
    }

    /** A check that only exists here, and throws whatever it is given. */
    private record Throwing(String id, RuntimeException thrown) implements StartCheck {
        @Override
        public List<StartFinding> evaluate(StartCheckContext context) {
            throw thrown;
        }
    }

    /** A check that only exists here, and never answers in time. */
    private record Slow(String id) implements StartCheck {
        @Override
        public List<StartFinding> evaluate(StartCheckContext context) {
            try {
                Thread.sleep(Duration.ofSeconds(10));
            } catch (InterruptedException stopped) {
                Thread.currentThread().interrupt();
            }
            return List.of();
        }
    }
}
