package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.tapstate.control.core.PipelineCatalogItem;
import io.tapstate.control.core.PipelineStatus;
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
 * The wire shape of a Pipeline's merged authoring, artifact, desired-state, and runtime projection, plus why
 * its run died when it did. A saved Pipeline has a status before its first runtime observation.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
record PipelineStatusResponse(
        String pipelineId,
        PipelineCatalogItem.DisplayState state,
        Failure failure,
        Instant observedAt,
        Long observedAgeMillis,
        PipelineState desiredState,
        PipelineState observedState,
        boolean hasArtifact,
        ExecutionPlanResponse plan,
        List<String> awaitingRebalance) {

    /**
     * A coded failure as a client reads it: the canonical code string, its named arguments sorted for a stable
     * machine contract, and the message rendered through the shared catalog.
     */
    record Failure(String code, Map<String, Object> params, String message) {
    }

    static PipelineStatusResponse of(PipelineStatus status, MessageCatalog catalog) {
        return of(status, catalog, Clock.systemUTC());
    }

    static PipelineStatusResponse of(PipelineStatus status, MessageCatalog catalog, Clock clock) {
        Instant observedAt = status.observedAt();
        return new PipelineStatusResponse(status.pipelineId(),
                PipelineCatalogItem.DisplayState.valueOf(status.state().name()),
                failure(status.failure(), catalog), observedAt,
                observedAt == null ? null
                        : Math.max(0, Duration.between(observedAt, clock.instant()).toMillis()),
                null, status.state(), true, ExecutionPlanResponse.of(status.plan()),
                status.awaitingRebalance().isEmpty() ? null : status.awaitingRebalance());
    }

    static PipelineStatusResponse of(PipelineCatalogItem item, MessageCatalog catalog) {
        return of(item, catalog, null);
    }

    static PipelineStatusResponse of(
            PipelineCatalogItem item, MessageCatalog catalog, PipelineStatus runtime) {
        PipelineCatalogItem.Status status = item.status();
        Instant observedAt = status.observedAt();
        return new PipelineStatusResponse(item.id(), status.state(), failure(status.failure(), catalog),
                observedAt, observedAt == null ? null
                        : Math.max(0, Duration.between(observedAt, Clock.systemUTC().instant()).toMillis()),
                status.desiredState(), status.observedState(), item.hasArtifact(),
                runtime == null ? null : ExecutionPlanResponse.of(runtime.plan()),
                runtime == null || runtime.awaitingRebalance().isEmpty() ? null : runtime.awaitingRebalance());
    }

    private static Failure failure(ObservationFailure failure, MessageCatalog catalog) {
        if (failure == null) {
            return null;
        }
        Map<String, Object> params = new TreeMap<>(failure.params());
        return new Failure(failure.code(), params, catalog.render(failure.code(), params).message());
    }
}
