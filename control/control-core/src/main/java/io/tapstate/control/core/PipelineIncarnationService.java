package io.tapstate.control.core;

import io.tapstate.spi.store.ArtifactStore;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Supplies the current system identity from the artifact truth document. */
public final class PipelineIncarnationService {

    private final ArtifactStore artifacts;

    public PipelineIncarnationService(ArtifactStore artifacts) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
    }

    /** Reads without initializing an upgrade-era pipeline that has no identity yet. */
    public Optional<String> current(String pipelineId) {
        return artifacts.pipelineIncarnationId(pipelineId);
    }

    /** Initializes an upgrade-era pipeline once, returning the concurrent winner on a lost race. */
    public Optional<String> ensureCurrent(String pipelineId) {
        return artifacts.ensurePipelineIncarnationId(pipelineId, newId());
    }

    static String newId() {
        return UUID.randomUUID().toString();
    }
}
