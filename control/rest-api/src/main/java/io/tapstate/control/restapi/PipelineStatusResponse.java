package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.tapstate.control.core.PipelineStatus;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.messages.MessageCatalog;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
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
        Long observedAgeMillis, ExecutionPlanResponse plan) {

    /**
     * A coded failure as a client reads it: the canonical code string (the stable identity — the enum never
     * leaves the process), its named arguments sorted for a stable machine contract, and the message
     * rendered from both through the shared catalog, so every face prints one wording.
     */
    record Failure(String code, Map<String, Object> params, String message) {
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
                ExecutionPlanResponse.of(status.plan()));
    }

    private static Failure failure(ObservationFailure failure, MessageCatalog catalog) {
        if (failure == null) {
            return null;
        }
        Map<String, Object> params = new TreeMap<>(failure.params());
        return new Failure(failure.code(), params, catalog.render(failure.code(), params).message());
    }
}
