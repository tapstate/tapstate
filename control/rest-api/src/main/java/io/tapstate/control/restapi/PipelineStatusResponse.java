package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.tapstate.control.core.PipelineStatus;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.messages.MessageCatalog;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The wire shape of a pipeline's status: its lifecycle state, plus why its run died when it did. A failed
 * state that cannot say what failed is only half an answer, and sending the reader to the logs for the rest
 * is what the coded reason exists to avoid.
 *
 * <p>The failure is omitted while the pipeline is healthy rather than serialized as null, so a client tells
 * "nothing wrong" from "something wrong" by presence alone.
 *
 * <p>{@code observedAt} says when the projection behind this answer was taken and {@code observedAgeMillis}
 * how long ago that was, measured here rather than by the caller: the caller's wall clock is its own, and a
 * client minutes out of step with the server would report a fresh pipeline as stale or the reverse. Both are
 * omitted together when the time is not known, so a reader tells "this is how old it is" from "nobody can
 * say how old this is" by presence alone — the absent case is an answer, not a gap to fill in locally.
 *
 * <p>The age is floored at zero, because the two clocks in it are not always the same one: a cluster
 * publishes an observation on whichever node converges and serves this read from whichever node was
 * dialled, so a node running milliseconds ahead of its peer yields a negative difference. Rendered, that
 * reaches a reader as an age before the present. Floored rather than dropped: the reading is still an age
 * and still says the observation is recent, and the direction is the safe one — a floor can only make a
 * reading look fresher, never stale, so nothing is ever reported as a stopped publisher by clock skew.
 *
 * <p>{@code plan} is the plan the pipeline's current run was submitted on, omitted when no run has one recorded.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
record PipelineStatusResponse(String pipelineId, PipelineState state, Failure failure, Instant observedAt,
        Long observedAgeMillis, Plan plan) {

    /**
     * A coded failure as a client reads it: the canonical code string (the stable identity — the enum never
     * leaves the process), its named arguments sorted for a stable machine contract, and the message
     * rendered from both through the shared catalog, so every face prints one wording.
     */
    record Failure(String code, Map<String, Object> params, String message) {
    }

    /**
     * The plan a run was submitted on, as a reader asking why each node runs as wide as it does reads it: which
     * run it is, the members the widths were worked out for, and every node in the order it was worked out. A
     * value the run does not have is omitted rather than sent as zero or null - a run on a single member is
     * fenced by nothing, so it names no generations.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record Plan(Long claimGeneration, Long executionGeneration, Long topologyRevision, List<String> members,
            List<Node> nodes, Instant plannedAt) {

        static Plan of(ExecutionPlan plan) {
            return plan == null ? null : new Plan(plan.claimGeneration(), plan.executionGeneration(),
                    plan.topologyRevision(), plan.members(), plan.nodes().stream().map(Node::of).toList(),
                    plan.plannedAt());
        }
    }

    /**
     * One node of a plan: the target it was given and where that came from, whether it runs as one processor for
     * the cluster or the same number on every member, the member count and per-member count it was worked out
     * for, the width that makes, why that is not the target where it is not, and the batch it takes its input
     * in. {@code computedLocal} is omitted for a node run as one processor for the cluster, which has none.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record Node(String node, int requested, String requestedOrigin, String scope, int memberCount,
            Integer computedLocal, int effective, List<String> reasons, Batch batch) {

        static Node of(ExecutionPlan.Node node) {
            return new Node(node.node(), node.requested(), node.origin(), node.scope(), node.memberCount(),
                    node.computedLocal(), node.effective(), node.reasons(),
                    new Batch(node.maxRecords(), node.maxWaitMillis()));
        }
    }

    /** The most records a node takes its input in at once, and the longest it waits for them. */
    record Batch(int maxRecords, long maxWaitMillis) {
    }

    static PipelineStatusResponse of(PipelineStatus status, MessageCatalog catalog) {
        return of(status, catalog, Clock.systemUTC());
    }

    /**
     * The same projection reading now from {@code clock}, so the age can be witnessed at a known instant
     * rather than by waiting for real time to pass.
     */
    static PipelineStatusResponse of(PipelineStatus status, MessageCatalog catalog, Clock clock) {
        Instant observedAt = status.observedAt();
        return new PipelineStatusResponse(status.pipelineId(), status.state(),
                failure(status.failure(), catalog), observedAt,
                observedAt == null ? null
                        : Math.max(0, Duration.between(observedAt, clock.instant()).toMillis()),
                Plan.of(status.plan()));
    }

    private static Failure failure(ObservationFailure failure, MessageCatalog catalog) {
        if (failure == null) {
            return null;
        }
        Map<String, Object> params = new TreeMap<>(failure.params());
        return new Failure(failure.code(), params, catalog.render(failure.code(), params).message());
    }
}
