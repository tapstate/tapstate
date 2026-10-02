package io.tapstate.app;

import io.tapstate.control.core.PipelineWriteTargets;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.spi.store.StorePort;

import java.util.List;
import java.util.Objects;

/**
 * Names a pipeline's write targets through the topology build's own derivation, so a start check reads
 * the table a sink will write and not one worked out a second way.
 */
final class StoreBackedPipelineWriteTargets implements PipelineWriteTargets {

    private final StoreBackedDagSource source;

    StoreBackedPipelineWriteTargets(StorePort storePort) {
        this.source = new StoreBackedDagSource(Objects.requireNonNull(storePort, "storePort"));
    }

    @Override
    public List<WriteTarget> of(PipelineResource pipeline) {
        Objects.requireNonNull(pipeline, "pipeline");
        return source.writeTargets(pipeline);
    }
}
