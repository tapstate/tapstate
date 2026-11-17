package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import io.tapstate.control.core.AggregateStepChecks;
import io.tapstate.control.core.AuditContext;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.aggregate.AggregateError;
import io.tapstate.runtime.engine.aggregate.AggregateRequests;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Carries a check or a repair to the member that runs the aggregate step, and its answer back.
 *
 * <p>The request is put to every member. The one running the step hands it to the step and waits for
 * the step to answer between two of its own updates; every other member answers that the step is not
 * there. Nothing here reads or writes the step's state itself: only the running step, holding the
 * pipeline's claim, does that.
 */
final class ClusterAggregateStepChecks implements AggregateStepChecks {

    static final String EXECUTOR = "tapstate.aggregate-requests";
    /** Groups one request covers when it walks the step's groups. */
    static final int PAGE = 32;
    /** How long the running step is given to reach a request between its updates. */
    static final long ANSWER_SECONDS = 25;

    private final HazelcastInstance member;
    private final AuditGate auditGate;

    /** @param member this node's member of the cluster, or null where the node runs no data plane */
    ClusterAggregateStepChecks(HazelcastInstance member, AuditGate auditGate) {
        this.member = member;
        this.auditGate = Objects.requireNonNull(auditGate, "auditGate");
    }

    @Override
    public Report check(String pipelineId, String step, Request request) {
        return ask(pipelineId, step, request, AggregateRequests.Action.CHECK);
    }

    @Override
    public Report repair(String principal, String pipelineId, String step, Request request) {
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("step", step);
        if (request.group() != null) {
            detail.put("group", String.valueOf(request.group()));
        }
        return auditGate.dispatch(ControlOperations.PIPELINE_AGGREGATE_REPAIR,
                new AuditContext(principal, pipelineId, null, detail),
                () -> ask(pipelineId, step, request, AggregateRequests.Action.REPAIR));
    }

    private Report ask(String pipelineId, String step, Request request, AggregateRequests.Action action) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(step, "step");
        Objects.requireNonNull(request, "request");
        if (member == null) {
            throw notRunning(pipelineId, step);
        }
        AggregateRequests.Request asked = request.group() != null
                ? AggregateRequests.Request.one(action, request.group())
                : AggregateRequests.Request.page(action, cursor(step, request.after()), PAGE);
        Map<Member, Future<Answer>> answers = member.getExecutorService(EXECUTOR)
                .submitToAllMembers(new Ask(pipelineId, step, asked));
        boolean busy = false;
        AggregateRequests.Report report = null;
        for (Future<Answer> pending : answers.values()) {
            Answer answer;
            try {
                answer = pending.get(ANSWER_SECONDS + 5, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw busy(pipelineId, step);
            } catch (ExecutionException | TimeoutException | RuntimeException unanswered) {
                // A member that left or could not be reached does not run the step any more.
                continue;
            }
            if (answer.report() != null) {
                report = answer.report();
            }
            busy |= answer.busy();
        }
        if (report == null) {
            throw busy ? busy(pipelineId, step) : notRunning(pipelineId, step);
        }
        if (report.refusal() != null) {
            throw refused(report);
        }
        List<Finding> findings = new ArrayList<>();
        for (AggregateRequests.Finding finding : report.findings()) {
            findings.add(new Finding(finding.group(), finding.verdict(), finding.fields(), finding.failure(),
                    finding.failedField()));
        }
        return new Report(pipelineId, step, action == AggregateRequests.Action.REPAIR, report.groups(), findings,
                report.next() == null ? null : Base64.getUrlEncoder().withoutPadding().encodeToString(report.next()),
                report.conflicts(), report.withheld());
    }

    private static byte[] cursor(String step, String after) {
        if (after == null || after.isEmpty()) {
            return null;
        }
        try {
            return Base64.getUrlDecoder().decode(after);
        } catch (IllegalArgumentException notACursor) {
            throw new TapstateException(AggregateError.GROUP_INVALID, Map.of("step", step,
                    "reason", "`after` is not a place an earlier answer gave as `next`"), null);
        }
    }

    /** The step's own refusal, under the code it named by its text. */
    private static TapstateException refused(AggregateRequests.Report report) {
        for (AggregateError code : AggregateError.values()) {
            if (code.code().equals(report.refusal())) {
                return new TapstateException(code, new LinkedHashMap<>(report.refusalArgs()), null);
            }
        }
        throw new IllegalStateException("an aggregate step refused a request under a code this build does not know: "
                + report.refusal());
    }

    private static TapstateException notRunning(String pipelineId, String step) {
        return new TapstateException(AggregateError.STEP_NOT_RUNNING, Map.of("pipeline", pipelineId, "step", step), null);
    }

    private static TapstateException busy(String pipelineId, String step) {
        return new TapstateException(AggregateError.STEP_BUSY, Map.of("pipeline", pipelineId, "step", step), null);
    }

    /**
     * One member's answer.
     *
     * @param report what the step answered, or null when it does not run here or did not answer
     * @param busy   whether the step runs here and did not take or answer the request in time
     */
    record Answer(AggregateRequests.Report report, boolean busy) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    /** Runs on each member: hands the request to the step if it runs there, and waits for its answer. */
    static final class Ask implements Callable<Answer>, Serializable, HazelcastInstanceAware {
        private static final long serialVersionUID = 1L;
        private final String pipeline;
        private final String step;
        private final AggregateRequests.Request request;
        private transient HazelcastInstance local;

        Ask(String pipeline, String step, AggregateRequests.Request request) {
            this.pipeline = pipeline;
            this.step = step;
            this.request = request;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.local = instance;
        }

        @Override
        public Answer call() throws InterruptedException {
            if (!AggregateRequests.runsOn(local, pipeline, step)) {
                return new Answer(null, false);
            }
            var offered = AggregateRequests.offer(local, pipeline, step, request);
            if (offered.isEmpty()) {
                return new Answer(null, true);
            }
            try {
                return new Answer(offered.get().get(ANSWER_SECONDS, TimeUnit.SECONDS), false);
            } catch (TimeoutException late) {
                return new Answer(null, true);
            } catch (ExecutionException stopped) {
                // The step stopped, or failed while answering; either way it gave no answer.
                return new Answer(null, false);
            }
        }
    }
}
