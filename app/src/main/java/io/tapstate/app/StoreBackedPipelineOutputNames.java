package io.tapstate.app;

import io.tapstate.control.core.PipelineOutputNames;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.srs.SourcePlacement;
import io.tapstate.spi.store.StorePort;
import java.util.List;
import java.util.Map;
import io.tapstate.core.model.Resource;

/** Definition-backed output coordinates; querying them neither starts nor validates a write. */
final class StoreBackedPipelineOutputNames implements PipelineOutputNames {
    private final StoreBackedDagSource source;

    StoreBackedPipelineOutputNames(StorePort store, boolean requireMarkedStore) {
        source = new StoreBackedDagSource(store, NestSettings.defaults(), StoreReachability.assumingReachable(),
                SourcePlacement.anyMember(), requireMarkedStore);
    }

    @Override
    public List<Output> of(PipelineResource pipeline) {
        return source.outputNames(pipeline);
    }

    @Override
    public Result resolve(PipelineResource pipeline) {
        return source.outputNameResult(pipeline);
    }

    @Override
    public Description describe(PipelineResource pipeline, Map<String, Resource> resources) {
        return source.outputNameDetails(pipeline, resources);
    }
}
