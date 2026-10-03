package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.OnFullLoad;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.spi.store.AuditRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * A person's start, end to end on the control side: what stops it, what an answer does, and that
 * nothing is written by a start that does not go ahead -- including one that changed the definition in
 * memory and then found another question.
 */
class PipelineStartServiceTest {

    private static final String KEY = "target-not-empty/warehouse/orders";

    private final StartChecksBench bench = new StartChecksBench();

    @AfterEach
    void close() {
        bench.executor.close();
    }

    @Test
    void aStartWithNoAnswersStopsWithTheWholeReportAndWritesNothing() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 5L, false));
        String before = bench.hash("pl");

        StartRefusal refused = catchThrowableOfType(StartRefusal.class,
                () -> bench.service().start("alice", "pl", List.of(), null));

        assertThat(refused.code().code()).isEqualTo("lifecycle.start-needs-confirmation");
        assertThat(refused.args()).containsEntry("pipeline", "pl").containsEntry("count", 1)
                .containsEntry("check", "target-not-empty").containsEntry("subject", "warehouse/orders");
        assertThat(refused.report().outcome()).isEqualTo(StartCheckReport.Outcome.NEEDS_CONFIRMATION);
        assertThat(refused.report().findings()).extracting(StartCheckReport.Finding::key).containsExactly(KEY);
        assertThat(bench.desiredById).isEmpty();
        assertThat(bench.audit).isEmpty();
        assertThat(bench.hash("pl")).isEqualTo(before);
    }

    @Test
    void keepingTheRowsStartsAsConfiguredAndTheAuditRecordSaysSo() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 5L, false));
        String hash = bench.hash("pl");

        StartOutcome outcome = bench.service().start("alice", "pl", List.of(new StartDecision(KEY, "keep")), hash);

        assertThat(outcome.desired().targetState()).isEqualTo(PipelineState.RUNNING);
        assertThat(outcome.desired().revision()).isEqualTo(hash);
        assertThat(outcome.report().outcome()).isEqualTo(StartCheckReport.Outcome.READY);
        assertThat(outcome.decisionsApplied()).singleElement().satisfies(applied -> {
            assertThat(applied.finding()).isEqualTo(KEY);
            assertThat(applied.action()).isEqualTo("keep");
            assertThat(applied.contentHash()).isNull();
        });
        assertThat(bench.hash("pl")).isEqualTo(hash);
        AuditRecord start = only(bench.audit, "pipeline.start");
        assertThat(start.expectedContentHash()).isEqualTo(hash);
        assertThat(start.detail()).containsEntry("startChecks", "READY")
                .containsEntry("decisions", List.of(Map.of("finding", KEY, "action", "keep")));
    }

    @Test
    void clearingFirstRewritesTheDefinitionThenStartsTheRewrittenOne() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 5L, false));
        String before = bench.hash("pl");

        StartOutcome outcome = bench.service().start("alice", "pl", List.of(new StartDecision(KEY, "clear")), before);

        PipelineResource after = bench.stored("pl");
        assertThat(((ServeBlock.Inline) after.serve()).sync().getFirst().onFullLoad()).isEqualTo(OnFullLoad.CLEAR);
        String changed = bench.hash("pl");
        assertThat(changed).isNotEqualTo(before);
        assertThat(outcome.desired().revision()).isEqualTo(changed);
        assertThat(outcome.report().contentHash()).isEqualTo(changed);
        assertThat(outcome.report().findings()).singleElement().satisfies(finding -> {
            assertThat(finding.behavior()).isEqualTo(StartFinding.Behavior.PASS);
            assertThat(finding.code()).isEqualTo("start-check.target-set-to-clear");
        });
        assertThat(outcome.decisionsApplied()).singleElement().satisfies(applied -> {
            assertThat(applied.action()).isEqualTo("clear");
            assertThat(applied.changes()).containsExactly(new StartAction.Change("out", "on_full_load", "append", "clear"));
            assertThat(applied.contentHash()).isEqualTo(changed);
        });
        assertThat(bench.audit).extracting(AuditRecord::operationId).containsExactly("pipeline.update", "pipeline.start");
    }

    @Test
    void anAnswerWithoutTheDefinitionItWasGivenAgainstIsRefused() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        TapstateException refused = catchThrowableOfType(TapstateException.class,
                () -> bench.service().start("alice", "pl", List.of(new StartDecision(KEY, "keep")), null));
        assertThat(refused.code()).isEqualTo(PipelineError.PRECONDITION_REQUIRED);
        assertThat(bench.desiredById).isEmpty();
    }

    @Test
    void anAnswerGivenAgainstAnOlderDefinitionIsRefusedAndWritesNothing() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 5L, false));
        TapstateException refused = catchThrowableOfType(TapstateException.class, () -> bench.service()
                .start("alice", "pl", List.of(new StartDecision(KEY, "clear")), "0".repeat(64)));
        assertThat(refused.code()).isEqualTo(PipelineError.VERSION_CONFLICT);
        assertThat(bench.desiredById).isEmpty();
        assertThat(bench.audit).isEmpty();
    }

    @Test
    void aRefusalStandsWhateverIsAnswered() {
        bench.artifacts.put(StartChecksBench.pipeline("fail"));
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 5L, false));
        String hash = bench.hash("pl");

        StartRefusal refused = catchThrowableOfType(StartRefusal.class, () -> bench.service()
                .start("alice", "pl", List.of(new StartDecision(KEY, "keep")), hash));

        assertThat(refused.code().code()).isEqualTo("lifecycle.start-blocked");
        assertThat(refused.report().outcome()).isEqualTo(StartCheckReport.Outcome.BLOCKED);
        assertThat(bench.desiredById).isEmpty();
    }

    @Test
    void anActionTheQuestionDoesNotOfferIsRefused() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 5L, false));
        TapstateException refused = catchThrowableOfType(TapstateException.class, () -> bench.service()
                .start("alice", "pl", List.of(new StartDecision(KEY, "truncate")), bench.hash("pl")));
        assertThat(refused.code().code()).isEqualTo("lifecycle.invalid-start-decision");
        assertThat(refused.args()).containsEntry("finding", KEY).containsEntry("action", "truncate");
    }

    @Test
    void aQuestionAnsweredTwiceIsRefused() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 5L, false));
        TapstateException refused = catchThrowableOfType(TapstateException.class, () -> bench.service()
                .start("alice", "pl", List.of(new StartDecision(KEY, "keep"), new StartDecision(KEY, "clear")),
                        bench.hash("pl")));
        assertThat(refused.code().code()).isEqualTo("lifecycle.invalid-start-decision");
    }

    @Test
    void anAnswerToAQuestionNoLongerAskedIsReportedStaleAndIgnored() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        StartDecision stale = new StartDecision(KEY, "clear");

        StartOutcome outcome = bench.service().start("alice", "pl", List.of(stale), bench.hash("pl"));

        assertThat(outcome.staleDecisions()).containsExactly(stale);
        assertThat(outcome.decisionsApplied()).isEmpty();
        assertThat(((ServeBlock.Inline) bench.stored("pl").serve()).sync().getFirst().onFullLoad())
                .as("a stale answer changes nothing").isNull();
    }

    @Test
    void aChangeThatLeavesAQuestionOpenWritesNeitherTheDefinitionNorTheIntent() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 5L, false));
        bench.checks.add(new AsksOnceCleared());
        String before = bench.hash("pl");

        StartRefusal refused = catchThrowableOfType(StartRefusal.class, () -> bench.service()
                .start("alice", "pl", List.of(new StartDecision(KEY, "clear")), before));

        assertThat(refused.code().code()).isEqualTo("lifecycle.start-needs-confirmation");
        assertThat(refused.args()).containsEntry("check", "asks-once-cleared");
        assertThat(refused.report().contentHash()).isEqualTo(before);
        assertThat(bench.hash("pl")).as("the definition is not rewritten").isEqualTo(before);
        assertThat(bench.desiredById).isEmpty();
        assertThat(bench.audit).isEmpty();
    }

    @Test
    void aDefinitionReplacedWhileTheStartWasCheckedIsNotStarted() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        String evaluated = bench.hash("pl");
        bench.checks.add(new ReplacesTheDefinition(bench));

        TapstateException refused = catchThrowableOfType(TapstateException.class,
                () -> bench.service().start("alice", "pl", List.of(), null));

        assertThat(refused.code()).isEqualTo(PipelineError.VERSION_CONFLICT);
        assertThat(bench.hash("pl")).isNotEqualTo(evaluated);
        assertThat(bench.desiredById).as("the replaced definition was never checked, so it is not started").isEmpty();
    }

    @Test
    void aStartRefusedAfterTheDefinitionWasRewrittenSaysWhatItRewrote() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 5L, false));
        bench.audit.clear();
        StartChecksBench failingStarts = bench;
        PipelineLifecycleService lifecycle = new PipelineLifecycleService(new ArtifactQueryService(bench.artifacts),
                bench.desired, new AuditGate(record -> {
                    if (record.operationId().equals("pipeline.start")) {
                        throw new IllegalStateException("audit log is down");
                    }
                    failingStarts.audit.add(record);
                }, StartChecksBench.CLOCK), pipelineId -> java.util.Optional.empty());
        PipelineStartService service = new PipelineStartService(lifecycle, bench.evaluator(), new ApplyService(
                io.tapstate.core.catalog.TapstateCatalog::load, bench.artifacts,
                new AuditGate(bench.audit::add, StartChecksBench.CLOCK), new EmptySchemaStore(),
                PlanAdvisories.none(), SchemaDerivation.none()));

        StartRefusal refused = catchThrowableOfType(StartRefusal.class,
                () -> service.start("alice", "pl", List.of(new StartDecision(KEY, "clear")), bench.hash("pl")));

        assertThat(refused.code()).isEqualTo(ControlError.AUDIT_BLOCKED);
        assertThat(refused.report()).isNull();
        assertThat(refused.decisionsApplied()).singleElement()
                .satisfies(applied -> assertThat(applied.contentHash()).isEqualTo(bench.hash("pl")));
        assertThat(bench.desiredById).isEmpty();
    }

    @Test
    void theReadOnlyPreviewWritesNothing() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 5L, false));

        StartCheckReport report = bench.service().checks("pl", StartIntent.START);

        assertThat(report.outcome()).isEqualTo(StartCheckReport.Outcome.NEEDS_CONFIRMATION);
        assertThat(bench.desiredById).isEmpty();
        assertThat(bench.audit).isEmpty();
    }

    @Test
    void aPreviewOfARerunIsAnsweredWhileThePipelineStillRuns() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.desiredById.put("pl", new DesiredState("pl", PipelineState.RUNNING, bench.hash("pl"), false, null, false, null));
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 5L, false));

        assertThat(bench.service().checks("pl", StartIntent.RERUN).intent()).isEqualTo(StartIntent.RERUN);
        TapstateException plain = catchThrowableOfType(TapstateException.class,
                () -> bench.service().checks("pl", StartIntent.START));
        assertThat(plain.code().code()).isEqualTo("lifecycle.illegal-transition");
    }

    private static AuditRecord only(List<AuditRecord> records, String operation) {
        List<AuditRecord> matching = records.stream().filter(record -> record.operationId().equals(operation)).toList();
        assertThat(matching).hasSize(1);
        return matching.getFirst();
    }

    /** A check that only exists here: it asks a question of a definition set to clear, which nothing else does. */
    private static final class AsksOnceCleared implements StartCheck {
        @Override
        public String id() {
            return "asks-once-cleared";
        }

        @Override
        public List<StartFinding> evaluate(StartCheckContext context) {
            ServeBlock.Inline serve = (ServeBlock.Inline) context.definition().serve();
            if (serve.sync().getFirst().onFullLoad() != OnFullLoad.CLEAR) {
                return List.of();
            }
            return List.of(new StartFinding(id(),
                    new StartFinding.Subject(StartFinding.Subject.PIPELINE, "pl", null, "pipeline pl"),
                    StartFinding.Behavior.CONFIRM, StartFinding.Evaluation.COMPLETE, StartCheckCode.UNAVAILABLE,
                    Map.of("check", id(), "reason", "asked"),
                    List.of(StartAction.acknowledge("go", StartCheckCode.KEEP_EXISTING_ROWS, Map.of("target", "x")))));
        }
    }

    /** A check that only exists here: a person replaces the definition while the start is being checked. */
    private record ReplacesTheDefinition(StartChecksBench bench) implements StartCheck {
        @Override
        public String id() {
            return "replaces-the-definition";
        }

        @Override
        public List<StartFinding> evaluate(StartCheckContext context) {
            bench.artifacts.put(StartChecksBench.pipeline("clear"));
            return List.of();
        }
    }
}
