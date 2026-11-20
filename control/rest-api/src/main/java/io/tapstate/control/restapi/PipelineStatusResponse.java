package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.tapstate.control.core.PipelineCatalogItem;
import io.tapstate.control.core.PipelineStatus;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.messages.MessageCatalog;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The wire shape of a Pipeline status read. {@code state}, failure, and observation time describe runtime
 * facts. A current, fenced engine job can supply state before monitoring publishes; observation fields
 * remain absent until an observation exists. An unproved transition does not invent a runtime state.
 * {@code desiredState} and {@code displayState} separately describe lifecycle intent and its UI projection.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
record PipelineStatusResponse(
        String pipelineId,
        PipelineState state,
        PipelineCatalogItem.UiState displayState,
        ApiError failure,
        Instant observedAt,
        Long observedAgeMillis,
        PipelineState desiredState,
        PipelineState observedState,
        boolean hasArtifact,
        ExecutionPlanResponse plan,
        List<String> awaitingRebalance,
        ApiError intentReadFailure) {

    static PipelineStatusResponse of(PipelineStatus status, MessageCatalog catalog) {
        return of(status, catalog, Clock.systemUTC(), Locale.ENGLISH);
    }

    static PipelineStatusResponse of(PipelineStatus status, MessageCatalog catalog, Clock clock) {
        return of(status, catalog, clock, Locale.ENGLISH);
    }

    static PipelineStatusResponse of(
            PipelineStatus status, MessageCatalog catalog, Clock clock, Locale locale) {
        Instant observedAt = status.observedAt();
        return new PipelineStatusResponse(status.pipelineId(), status.state(), null,
                ApiError.from(status.failure(), catalog, locale), observedAt,
                observedAt == null ? null
                        : Math.max(0, Duration.between(observedAt, clock.instant()).toMillis()),
                null, status.observedState(), false, ExecutionPlanResponse.of(status.plan()),
                status.awaitingRebalance().isEmpty() ? null : status.awaitingRebalance(), null);
    }

    static PipelineStatusResponse of(
            PipelineCatalogItem item, MessageCatalog catalog, PipelineStatus runtime) {
        return of(item, catalog, runtime, Locale.ENGLISH);
    }

    static PipelineStatusResponse of(
            PipelineCatalogItem item, MessageCatalog catalog, PipelineStatus runtime, Locale locale) {
        PipelineCatalogItem.Status status = item.status();
        Instant observedAt = runtime.observedAt();
        return new PipelineStatusResponse(item.id(), runtime.state(), status.displayState(),
                ApiError.from(runtime.failure(), catalog, locale),
                observedAt, observedAt == null ? null
                        : Math.max(0, Duration.between(observedAt, Clock.systemUTC().instant()).toMillis()),
                status.desiredState(), runtime.observedState(), item.hasArtifact(),
                ExecutionPlanResponse.of(runtime.plan()),
                runtime.awaitingRebalance().isEmpty() ? null : runtime.awaitingRebalance(),
                ApiError.from(status.intentReadFailure(), catalog, locale));
    }

    static PipelineStatusResponse of(PipelineCatalogItem item, MessageCatalog catalog) {
        return of(item, catalog, Clock.systemUTC(), Locale.ENGLISH);
    }

    /** Builds a catalog status without inventing a runtime observation when none has been published yet. */
    static PipelineStatusResponse of(PipelineCatalogItem item, MessageCatalog catalog, Clock clock) {
        return of(item, catalog, clock, Locale.ENGLISH);
    }

    static PipelineStatusResponse of(
            PipelineCatalogItem item, MessageCatalog catalog, Clock clock, Locale locale) {
        PipelineCatalogItem.Status status = item.status();
        PipelineState state = status.observedState();
        if (state == null && status.state() != null) {
            state = switch (status.state()) {
                case NEW, RUNNING, PAUSED, STOPPED, COMPLETED, FAILED -> PipelineState.valueOf(status.state().name());
                default -> null;
            };
        }
        if (state == null && item.hasArtifact() && !knownTransition(status.displayState())) {
            throw new io.tapstate.core.common.TapstateException(io.tapstate.spi.store.IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", item.id(), "field", "checkpoint"), null);
        }
        Instant observedAt = status.observedAt();
        return new PipelineStatusResponse(item.id(), state, status.displayState(),
                ApiError.from(status.failure(), catalog, locale), observedAt, observedAt == null ? null
                        : Math.max(0, Duration.between(observedAt, clock.instant()).toMillis()),
                status.desiredState(), status.observedState(), item.hasArtifact(), null, null,
                ApiError.from(status.intentReadFailure(), catalog, locale));
    }

    private static boolean knownTransition(PipelineCatalogItem.UiState state) {
        return state != null && switch (state) {
            case STARTING, STOPPING, PAUSING, RESUMING, RECOVERY_REQUIRED -> true;
            default -> false;
        };
    }
}
