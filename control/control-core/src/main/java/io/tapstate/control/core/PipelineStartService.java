package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.model.PipelineResource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A person starting a pipeline: the start checks, the answers to them, and the start itself.
 *
 * <p>The order is the contract. The checks every verb makes come first -- the pipeline is applied, can run
 * and may start from where it is -- then every start check is evaluated against the stored definition, the
 * answers are matched to the findings by key, and a refusal or an open question stops the start with the
 * whole report before anything is written. A change a person chose is worked out in memory and the changed
 * definition evaluated again; only when that passes too is the definition written, through the same write
 * an edit goes through, and only then the intent, held to the definition that was checked.
 *
 * <p>Nothing about an earlier attempt is kept here. A second start evaluates everything again, and an
 * answer to a question this start no longer asks is reported as stale rather than applied to whatever
 * the question became.
 */
public final class PipelineStartService {

    private final PipelineLifecycleService lifecycle;
    private final StartCheckEvaluator evaluator;
    private final StartDefinitionWriter definitions;

    public PipelineStartService(
            PipelineLifecycleService lifecycle, StartCheckEvaluator evaluator, StartDefinitionWriter definitions) {
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
        this.evaluator = Objects.requireNonNull(evaluator, "evaluator");
        this.definitions = Objects.requireNonNull(definitions, "definitions");
    }

    /** A start whose changes to the definition are written the way an edit of the pipeline is, and only there. */
    public PipelineStartService(
            PipelineLifecycleService lifecycle, StartCheckEvaluator evaluator, ApplyService definitions) {
        this(lifecycle, evaluator, StartDefinitionWriter.through(definitions));
    }

    /** The start checks a start of {@code pipelineId} would be asked, evaluated now; writes nothing. */
    public StartCheckReport checks(String pipelineId, StartIntent intent) {
        Objects.requireNonNull(intent, "intent");
        PipelineLifecycleService.Startable startable = intent == StartIntent.RERUN
                ? lifecycle.rerunnable(pipelineId)
                : lifecycle.startable(pipelineId);
        return evaluator.report(evaluate(pipelineId, startable, startable.pipeline(), intent), Set.of());
    }

    /**
     * Starts {@code pipelineId} if its start checks let it, with {@code decisions} answering their
     * questions.
     *
     * @param expectedContentHash the definition the person's answers were given against; required with any
     *                            answer, and when given, the start is refused unless it is still current
     */
    public StartOutcome start(String principal, String pipelineId, List<StartDecision> decisions,
            String expectedContentHash) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(pipelineId, "pipelineId");
        List<StartDecision> answers = decisions == null ? List.of() : List.copyOf(decisions);
        PipelineLifecycleService.Startable startable = lifecycle.startable(pipelineId);
        String storedHash = startable.artifact().contentHash();
        if (!answers.isEmpty() && expectedContentHash == null) {
            throw new TapstateException(PipelineError.PRECONDITION_REQUIRED, Map.of("id", pipelineId), null);
        }
        if (expectedContentHash != null && !expectedContentHash.equals(storedHash)) {
            throw new TapstateException(PipelineError.VERSION_CONFLICT, Map.of("id", pipelineId), null);
        }

        StartEvaluation first = evaluate(pipelineId, startable, startable.pipeline(), StartIntent.START);
        Reconciled taken = reconcile(pipelineId, first, answers);
        refuseUnlessReady(pipelineId, first, taken.answered());

        PipelineResource definition = startable.pipeline();
        StartEvaluation started = first;
        List<StartOutcome.Applied> applied = new ArrayList<>();
        if (!taken.changes().isEmpty()) {
            for (Chosen chosen : taken.changes()) {
                definition = chosen.action().change().apply(definition);
            }
            started = evaluate(pipelineId, startable, definition, StartIntent.START);
            refuseUnlessReady(pipelineId, started, taken.answered());
            String changedHash = definitions.write(principal, definition, storedHash,
                    taken.changes().stream().flatMap(chosen -> chosen.action().changes().stream()).toList());
            for (Chosen chosen : taken.changes()) {
                applied.add(new StartOutcome.Applied(chosen.finding().key(), chosen.action().id(),
                        chosen.action().kind(), chosen.action().changes(), changedHash));
            }
            started = new StartEvaluation(started.pipelineId(), started.intent(), changedHash,
                    started.evaluatedAt(), started.plan(), started.findings());
        }
        for (Chosen chosen : taken.acknowledged()) {
            applied.add(new StartOutcome.Applied(chosen.finding().key(), chosen.action().id(),
                    chosen.action().kind(), List.of(), null));
        }

        StartCheckReport report = evaluator.report(started, taken.answered());
        DesiredState desired;
        try {
            desired = lifecycle.start(principal, pipelineId, started.contentHash(), auditDetail(report, applied));
        } catch (TapstateException refused) {
            if (applied.stream().anyMatch(each -> each.contentHash() != null)) {
                throw new StartRefusal(refused.code(), refused.args(), null, applied, refused);
            }
            throw refused;
        }
        return new StartOutcome(desired, report, applied, taken.stale());
    }

    private StartEvaluation evaluate(String pipelineId, PipelineLifecycleService.Startable startable,
            PipelineResource definition, StartIntent intent) {
        return evaluator.evaluate(pipelineId, definition, startable.artifact().contentHash(), startable.prior(), intent);
    }

    /**
     * Matches every answer to this start's findings. An answer to a question no longer asked -- the finding
     * is gone, or now passes -- is stale and ignored; one naming an action the question does not offer, or a
     * question answered twice, is refused. An answer to a refusal changes nothing: the refusal stands.
     */
    private Reconciled reconcile(String pipelineId, StartEvaluation evaluation, List<StartDecision> answers) {
        Map<String, String> seen = new HashMap<>();
        Set<String> answered = new LinkedHashSet<>();
        List<Chosen> changes = new ArrayList<>();
        List<Chosen> acknowledged = new ArrayList<>();
        List<StartDecision> stale = new ArrayList<>();
        for (StartDecision answer : answers) {
            if (answer == null || answer.finding() == null || answer.finding().isBlank()
                    || answer.action() == null || answer.action().isBlank()) {
                throw invalid(pipelineId, answer);
            }
            if (seen.putIfAbsent(answer.finding(), answer.action()) != null) {
                throw invalid(pipelineId, answer);
            }
            StartFinding finding = evaluation.finding(answer.finding()).orElse(null);
            if (finding == null || finding.behavior() == StartFinding.Behavior.PASS
                    || finding.behavior() == StartFinding.Behavior.WARN) {
                stale.add(answer);
                continue;
            }
            if (finding.behavior() == StartFinding.Behavior.BLOCK) {
                continue;
            }
            StartAction action = finding.action(answer.action()).orElseThrow(() -> invalid(pipelineId, answer));
            answered.add(finding.key());
            (action.change() == null ? acknowledged : changes).add(new Chosen(finding, action));
        }
        return new Reconciled(answered, changes, acknowledged, stale);
    }

    private void refuseUnlessReady(String pipelineId, StartEvaluation evaluation, Set<String> answered) {
        StartCheckReport.Outcome outcome = evaluation.outcome(answered);
        if (outcome == StartCheckReport.Outcome.READY) {
            return;
        }
        List<StartFinding> stopping = outcome == StartCheckReport.Outcome.BLOCKED
                ? evaluation.withBehavior(StartFinding.Behavior.BLOCK)
                : evaluation.withBehavior(StartFinding.Behavior.CONFIRM).stream()
                        .filter(finding -> !answered.contains(finding.key()))
                        .toList();
        StartFinding first = stopping.getFirst();
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("pipeline", pipelineId);
        args.put("count", stopping.size());
        args.put("check", first.check());
        args.put("subject", first.subject().id());
        throw new StartRefusal(
                outcome == StartCheckReport.Outcome.BLOCKED
                        ? LifecycleError.START_BLOCKED : LifecycleError.START_NEEDS_CONFIRMATION,
                args, evaluator.report(evaluation, answered), List.of(), null);
    }

    /** What the start's audit record says the checks found and what was answered. */
    private static Map<String, Object> auditDetail(StartCheckReport report, List<StartOutcome.Applied> applied) {
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("startChecks", report.outcome().name());
        detail.put("findings", report.findings().stream()
                .filter(finding -> finding.behavior() != StartFinding.Behavior.PASS)
                .map(finding -> Map.of("key", finding.key(), "behavior", finding.behavior().name()))
                .toList());
        detail.put("decisions", applied.stream()
                .map(each -> Map.of("finding", each.finding(), "action", each.action()))
                .toList());
        return detail;
    }

    private static TapstateException invalid(String pipelineId, StartDecision answer) {
        return new TapstateException(LifecycleError.INVALID_START_DECISION, Map.of(
                "pipeline", pipelineId,
                "finding", answer == null ? "" : String.valueOf(answer.finding()),
                "action", answer == null ? "" : String.valueOf(answer.action())), null);
    }

    private record Chosen(StartFinding finding, StartAction action) {
    }

    private record Reconciled(Set<String> answered, List<Chosen> changes, List<Chosen> acknowledged,
            List<StartDecision> stale) {
    }
}
