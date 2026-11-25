package io.tapstate.spi.store;

/** Outcome of a conditional Pipeline draft mutation. */
public enum PipelineDraftMutation {
    CREATED,
    REPLACED,
    DELETED,
    PUBLISHED,
    NOT_FOUND,
    ALREADY_EXISTS,
    REVISION_CONFLICT,
    MODE_CONFLICT,
    ARTIFACT_CONFLICT
}
