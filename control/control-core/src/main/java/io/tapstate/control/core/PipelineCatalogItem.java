package io.tapstate.control.core;

import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;

import java.time.Instant;
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
            PipelineState desiredState,
            PipelineState observedState,
            ObservationFailure failure,
            Instant observedAt) {

        public Status {
            Objects.requireNonNull(state, "state");
        }
    }

    public enum DisplayState {
        DRAFT,
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
}
