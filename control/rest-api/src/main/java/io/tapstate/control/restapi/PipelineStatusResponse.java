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
 * The wire shape of a Pipeline status read. {@code state}, failure, and observation time describe runtime
 * facts; before the first runtime observation, state is {@code NEW} and {@code observedState} is absent.
 * {@code desiredState} and {@code displayState} separately describe lifecycle intent and its UI projection.
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
        List<String> awaitingRebalance,
        Failure intentReadFailure) {

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
        return new PipelineStatusResponse(status.pipelineId(), status.state(), null,
                failure(status.failure(), catalog), observedAt,
                observedAt == null ? null
                        : Math.max(0, Duration.between(observedAt, clock.instant()).toMillis()),
                null, status.observedState(), false, ExecutionPlanResponse.of(status.plan()),
                status.awaitingRebalance().isEmpty() ? null : status.awaitingRebalance(), null);
    }

    static PipelineStatusResponse of(
            PipelineCatalogItem item, MessageCatalog catalog, PipelineStatus runtime) {
        PipelineCatalogItem.Status status = item.status();
        Instant observedAt = runtime.observedAt();
        return new PipelineStatusResponse(item.id(), runtime.state(), status.displayState(),
                failure(runtime.failure(), catalog),
                observedAt, observedAt == null ? null
                        : Math.max(0, Duration.between(observedAt, Clock.systemUTC().instant()).toMillis()),
                status.desiredState(), runtime.observedState(), item.hasArtifact(),
                ExecutionPlanResponse.of(runtime.plan()),
                runtime.awaitingRebalance().isEmpty() ? null : runtime.awaitingRebalance(),
                intentReadFailure(status.intentReadFailure(), catalog));
    }

    static PipelineStatusResponse of(PipelineCatalogItem item, MessageCatalog catalog) {
        return of(item, catalog, Clock.systemUTC());
    }

    /** Builds a catalog status without inventing a runtime observation when none has been published yet. */
    static PipelineStatusResponse of(PipelineCatalogItem item, MessageCatalog catalog, Clock clock) {
        PipelineCatalogItem.Status status = item.status();
        PipelineState state = status.observedState() == null ? PipelineState.NEW : status.observedState();
        Instant observedAt = status.observedAt();
        return new PipelineStatusResponse(item.id(), state, status.displayState(),
                failure(status.failure(), catalog), observedAt, observedAt == null ? null
                        : Math.max(0, Duration.between(observedAt, clock.instant()).toMillis()),
                status.desiredState(), status.observedState(), item.hasArtifact(), null, null,
                intentReadFailure(status.intentReadFailure(), catalog));
    }

    private static Failure intentReadFailure(
            PipelineCatalogItem.IntentReadFailure failure, MessageCatalog catalog) {
        if (failure == null) {
            return null;
        }
        Map<String, Object> params = new TreeMap<>(failure.params());
        return new Failure(failure.code(), params, catalog.render(failure.code(), params).message());
    }

    private static Failure failure(ObservationFailure failure, MessageCatalog catalog) {
        if (failure == null) {
            return null;
        }
        Map<String, Object> params = new TreeMap<>(failure.params());
        return new Failure(failure.code(), params, catalog.render(failure.code(), params).message());
    }
}
