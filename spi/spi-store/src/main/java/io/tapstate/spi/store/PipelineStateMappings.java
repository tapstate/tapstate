package io.tapstate.spi.store;

import java.util.Optional;

/** Load-only access to retained Pipeline state mappings; reading cannot initialize or advance a generation. */
public interface PipelineStateMappings {
    Optional<PipelineStateMapping> read(String pipelineId);
}
