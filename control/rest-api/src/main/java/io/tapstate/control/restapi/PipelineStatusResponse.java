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
 * The wire shape of the latest actual Pipeline observation. Optional UI metadata qualifies the observation
 * while state, failure, and observation time remain the runtime facts consumed by existing clients.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
record PipelineStatusResponse(
        String pipelineId,
        PipelineState state,
        PipelineCatalogItem.UiState displayState,
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
                status.state(), PipelineCatalogItem.UiState.valueOf(status.state().name()),
                failure(status.failure(), catalog), observedAt,
                observedAt == null ? null
                        : Math.max(0, Duration.between(observedAt, clock.instant()).toMillis()),
                null, status.state(), true, ExecutionPlanResponse.of(status.plan()),
                status.awaitingRebalance().isEmpty() ? null : status.awaitingRebalance());
    }

    static PipelineStatusResponse of(
            PipelineCatalogItem item, MessageCatalog catalog, PipelineStatus runtime) {
        PipelineCatalogItem.Status status = item.status();
        Instant observedAt = runtime.observedAt();
        return new PipelineStatusResponse(item.id(), runtime.state(), status.displayState(),
                failure(runtime.failure(), catalog),
                observedAt, observedAt == null ? null
                        : Math.max(0, Duration.between(observedAt, Clock.systemUTC().instant()).toMillis()),
                status.desiredState(), runtime.state(), item.hasArtifact(),
                ExecutionPlanResponse.of(runtime.plan()),
                runtime.awaitingRebalance().isEmpty() ? null : runtime.awaitingRebalance());
    }

    /** Builds a merged status for an authoring or applied Pipeline before its first runtime observation. */
    static PipelineStatusResponse of(PipelineCatalogItem item, MessageCatalog catalog) {
        PipelineCatalogItem.Status status = item.status();
        PipelineState state = PipelineState.NEW;
        if (status.desiredState() != null) {
            state = status.desiredState();
        }
        if (status.observedState() != null) {
            state = status.observedState();
        }
        return new PipelineStatusResponse(item.id(), state, status.displayState(),
                failure(status.failure(), catalog), status.observedAt(), null,
                status.desiredState(), status.observedState(), item.hasArtifact(), null, null);
    }

    private static Failure failure(ObservationFailure failure, MessageCatalog catalog) {
        if (failure == null) {
            return null;
        }
        Map<String, Object> params = new TreeMap<>(failure.params());
        return new Failure(failure.code(), params, catalog.render(failure.code(), params).message());
    }
}
