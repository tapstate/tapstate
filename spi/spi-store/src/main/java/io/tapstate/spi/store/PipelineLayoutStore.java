package io.tapstate.spi.store;

import java.util.Optional;

/** Durable, editor-only Pipeline layouts, isolated from canonical artifact mutations. */
public interface PipelineLayoutStore {

    Optional<PipelineLayout> get(String pipelineId);

    void save(PipelineLayout layout);

    void delete(String pipelineId);
}
