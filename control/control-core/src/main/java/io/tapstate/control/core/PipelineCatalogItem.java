package io.tapstate.control.core;

import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** One list-facing Pipeline projection, joined from its saved authoring document and applied artifact. */
public record PipelineCatalogItem(
        String id,
        String name,
        String description,
        String mode,
        Long revision,
        Instant updatedAt,
        String contentHash,
        boolean hasArtifact,
        Status status) {

    public PipelineCatalogItem {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(status, "status");
    }

    /** Runtime truth and the current lifecycle target, exposed together so a list can show in-flight changes. */
    public record Status(
            DisplayState state,
            UiState displayState,
            PipelineState desiredState,
            PipelineState observedState,
            ObservationFailure failure,
            Instant observedAt,
            IntentReadFailure intentReadFailure) {

        public Status(DisplayState state, UiState displayState, PipelineState desiredState,
                PipelineState observedState, ObservationFailure failure, Instant observedAt) {
            this(state, displayState, desiredState, observedState, failure, observedAt, null);
        }

        public Status(DisplayState state, PipelineState desiredState, PipelineState observedState,
                ObservationFailure failure, Instant observedAt) {
            this(state, UiState.of(state, true), desiredState, observedState, failure, observedAt, null);
        }
    }

    /** A coded failure to read lifecycle intent, carried separately from runtime observation facts. */
    public record IntentReadFailure(String code, Map<String, Object> params) {
        public IntentReadFailure {
            Objects.requireNonNull(code, "code");
            params = params == null ? Map.of()
                    : Collections.unmodifiableMap(new LinkedHashMap<>(params));
        }
    }

    public enum DisplayState {
        NEW,
        STARTING,
        RUNNING,
        PAUSING,
        PAUSED,
        RESUMING,
        STOPPING,
        STOPPED,
        COMPLETED,
        FAILED,
        UNAVAILABLE
    }

    /** Authoring-aware UI projection carried separately from the existing catalog state vocabulary. */
    public enum UiState {
        DRAFT,
        NOT_STARTED,
        NEW,
        STARTING,
        RUNNING,
        PAUSING,
        PAUSED,
        RESUMING,
        STOPPING,
        STOPPED,
        COMPLETED,
        FAILED,
        UNAVAILABLE;

        /** Projects internal lifecycle state into the authoring-facing status vocabulary. */
        public static UiState of(DisplayState state, boolean hasArtifact) {
            if (!hasArtifact) {
                return DRAFT;
            }
            if (state == null || state == DisplayState.UNAVAILABLE) {
                return null;
            }
            if (state == DisplayState.NEW) {
                return NOT_STARTED;
            }
            return valueOf(state.name());
        }
    }
}
