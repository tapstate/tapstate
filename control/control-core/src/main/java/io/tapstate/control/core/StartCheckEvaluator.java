package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.model.PipelineResource;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * Runs the registered start checks against one start, all at once and within one time budget, and keeps
 * each check's failure to itself.
 *
 * <p>A check that throws a coded error, or does not answer in time, becomes one finding of its own that
 * says so -- with the behavior the check declared for not knowing, which is never a pass -- and every
 * other check's findings still stand. Anything else a check throws is a defect, and fails the start with
 * it: laundering it into a warning would let a broken check pass for a target that is merely out of reach,
 * for ever.
 */
public final class StartCheckEvaluator {

    /**
     * Lower-kebab, written possessively: a repeated group that holds a repetition of its own is matched
     * recursively, one call per segment, and a long enough id runs the stack out; possessive repetition
     * matches the same ids in a loop.
     */
    private static final Pattern CHECK_ID = Pattern.compile("[a-z][a-z0-9]*+(?:-[a-z0-9]++)*+");

    /**
     * The longest all of one start's checks together may take. Clients released before start checks wait
     * three seconds for a lifecycle call, and the command line then sends the start a second time; the
     * checks have to fit well inside that, probes of every target included.
     */
    public static final Duration BUDGET = Duration.ofMillis(1800);

    /** The longest one target's probe may take, so one slow target cannot use up every other's time. */
    public static final Duration PER_TARGET = Duration.ofMillis(1500);

    private final List<StartCheck> checks;
    private final StartPlanner planner;
    private final TargetProbe probe;
    private final StartCheckMessages messages;
    private final ExecutorService executor;
    private final Clock clock;
    private final Duration budget;
    private final Duration perTarget;

    public StartCheckEvaluator(List<StartCheck> checks, StartPlanner planner, TargetProbe probe,
            StartCheckMessages messages, ExecutorService executor, Clock clock) {
        this(checks, planner, probe, messages, executor, clock, BUDGET, PER_TARGET);
    }

    public StartCheckEvaluator(List<StartCheck> checks, StartPlanner planner, TargetProbe probe,
            StartCheckMessages messages, ExecutorService executor, Clock clock, Duration budget,
            Duration perTarget) {
        this.checks = List.copyOf(checks);
        Set<String> ids = new HashSet<>();
        for (StartCheck check : this.checks) {
            if (!CHECK_ID.matcher(check.id()).matches() || !ids.add(check.id())) {
                throw new IllegalArgumentException("start check ids are unique lower-kebab: " + check.id());
            }
            if (check.whenUnavailable() == StartFinding.Behavior.PASS) {
                throw new IllegalArgumentException(
                        "start check " + check.id() + " would pass a start it could not evaluate");
            }
        }
        this.planner = Objects.requireNonNull(planner, "planner");
        this.probe = Objects.requireNonNull(probe, "probe");
        this.messages = Objects.requireNonNull(messages, "messages");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.budget = Objects.requireNonNull(budget, "budget");
        this.perTarget = Objects.requireNonNull(perTarget, "perTarget");
    }

    /**
     * Evaluates every check against starting {@code definition}.
     *
     * @param contentHash the stored definition's content hash, which the report carries as the precondition
     *                    an answer is sent back under -- also when {@code definition} is a changed copy
     */
    public StartEvaluation evaluate(String pipelineId, PipelineResource definition, String contentHash,
            Optional<DesiredState> prior, StartIntent intent) {
        Instant started = clock.instant();
        StartPlan plan = null;
        TapstateException planUnavailable = null;
        try {
            plan = planner.plan(definition, prior, intent);
        } catch (TapstateException coded) {
            planUnavailable = coded;
        }
        StartCheckContext context = new StartCheckContext(pipelineId, intent, definition, contentHash, plan,
                planUnavailable, probe, executor, clock, started.plus(budget), perTarget, this::describe);

        Map<StartCheck, Future<List<StartFinding>>> running = new LinkedHashMap<>();
        for (StartCheck check : checks) {
            running.put(check, executor.submit(() -> check.evaluate(context)));
        }
        List<StartFinding> findings = new ArrayList<>();
        Set<String> keys = new HashSet<>();
        for (Map.Entry<StartCheck, Future<List<StartFinding>>> each : running.entrySet()) {
            for (StartFinding finding : await(each.getKey(), each.getValue(), context)) {
                if (!finding.check().equals(each.getKey().id())) {
                    throw new IllegalStateException(
                            "start check " + each.getKey().id() + " made a finding for " + finding.check());
                }
                if (!keys.add(finding.key())) {
                    throw new IllegalStateException("two start check findings share the key " + finding.key());
                }
                findings.add(finding);
            }
        }
        return new StartEvaluation(pipelineId, intent, contentHash, started, plan, findings);
    }

    private List<StartFinding> await(StartCheck check, Future<List<StartFinding>> future, StartCheckContext context) {
        long wait = Math.max(0L, Duration.between(clock.instant(), context.deadline()).toMillis());
        try {
            return future.get(wait, TimeUnit.MILLISECONDS);
        } catch (TimeoutException late) {
            future.cancel(true);
            return List.of(unavailable(check, context,
                    "it did not answer within " + budget.toMillis() + " ms", null));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            return List.of(unavailable(check, context, "the start was interrupted while it was being checked", null));
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof TapstateException coded) {
                return List.of(unavailable(check, context, describe(coded), coded.code().code()));
            }
            if (failed.getCause() instanceof RuntimeException defect) {
                throw defect;
            }
            if (failed.getCause() instanceof Error fatal) {
                throw fatal;
            }
            throw new IllegalStateException(failed.getCause());
        }
    }

    private StartFinding unavailable(StartCheck check, StartCheckContext context, String reason, String reasonCode) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("check", check.id());
        params.put("reason", reason);
        if (reasonCode != null) {
            params.put("reasonCode", reasonCode);
        }
        return new StartFinding(check.id(),
                new StartFinding.Subject(StartFinding.Subject.PIPELINE, context.pipelineId(), null,
                        "pipeline " + context.pipelineId()),
                check.whenUnavailable(), StartFinding.Evaluation.UNAVAILABLE, StartCheckCode.UNAVAILABLE,
                params, List.of());
    }

    /** A coded failure as a person reads it, for a finding's {@code reason}. */
    String describe(TapstateException coded) {
        return messages.render(coded.code(), coded.args());
    }

    /**
     * The evaluation as a client receives it, its outcome worked out against the questions in
     * {@code answered} -- the keys of the confirmations a decision answers.
     */
    public StartCheckReport report(StartEvaluation evaluation, Set<String> answered) {
        List<StartCheckReport.PlanEntry> plan = evaluation.plan() == null ? List.of()
                : evaluation.plan().entries().stream()
                        .map(entry -> new StartCheckReport.PlanEntry(entry.target().element(),
                                new StartCheckReport.Target(entry.target().connection(), entry.target().table()),
                                entry.load(), entry.onFullLoad().yaml()))
                        .toList();
        List<StartCheckReport.Finding> findings = evaluation.findings().stream().map(this::rendered).toList();
        return new StartCheckReport(evaluation.pipelineId(), evaluation.intent(), evaluation.contentHash(),
                evaluation.evaluatedAt().toString(), evaluation.outcome(answered), plan, findings);
    }

    private StartCheckReport.Finding rendered(StartFinding finding) {
        StartFinding.Subject subject = finding.subject();
        return new StartCheckReport.Finding(finding.key(), finding.check(),
                new StartCheckReport.Subject(subject.kind(), subject.id(), subject.element(), subject.label()),
                finding.behavior(), finding.evaluation(), finding.code().code(), finding.params(),
                messages.render(finding.code(), finding.params()),
                finding.actions().stream()
                        .map(action -> new StartCheckReport.Action(action.id(), action.kind(), action.destructive(),
                                action.code().code(), messages.render(action.code(), action.params()),
                                action.changes()))
                        .toList());
    }
}
