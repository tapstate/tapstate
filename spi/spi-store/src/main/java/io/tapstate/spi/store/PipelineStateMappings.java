package io.tapstate.spi.store;

import java.util.Optional;

/** Load-only access to retained Pipeline state mappings; reading cannot initialize or advance a generation. */
public interface PipelineStateMappings {
    Optional<PipelineStateMapping> read(String pipelineId);

    /** Inclusive physical generations that belong to the current Pipeline's lifetime. */
    record LogWindow(long firstGeneration, long lastGeneration) {
        public LogWindow {
            if (firstGeneration < 1 || lastGeneration < firstGeneration || lastGeneration == Long.MAX_VALUE) {
                throw new IllegalArgumentException("invalid Pipeline log window");
            }
        }
    }

    /** Empty when no live Pipeline log lifetime can be established. */
    default Optional<LogWindow> logWindow(String pipelineId) {
        return Optional.empty();
    }
}
