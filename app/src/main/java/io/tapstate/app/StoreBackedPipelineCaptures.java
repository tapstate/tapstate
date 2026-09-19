package io.tapstate.app;

import io.tapstate.control.core.PipelineCaptures;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.spi.store.StorePort;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The capture identities a pipeline reads through, worked out from what is stored.
 *
 * <p>Derived rather than asked of whoever is running the pipeline, and that is the point of it. A
 * capture's identity is a function of the source contract -- connector settings, selected streams, the
 * read axis -- so every member computes the same ids from the same artifacts, and the read face answers
 * the same on a member that is running nothing. Asking the running member instead would give an answer
 * only that member could give, over a face whose whole promise is that any node answers.
 *
 * <p>It is derived through the same function the runtime derives it through, so the ids here are the ids
 * the claims are actually filed under. A second derivation written for reading would agree until one of
 * the two was changed.
 *
 * <p>A pipeline whose artifacts are not all in the store yields nothing. There is no contract to derive
 * an id from, and the shape already treats a capture with no claim as absent, so this adds no ambiguity
 * that was not there: what it must not do is invent an id, which would file a reader's attention under a
 * claim nobody holds.
 */
final class StoreBackedPipelineCaptures implements PipelineCaptures {

    private final StorePort storePort;

    StoreBackedPipelineCaptures(StorePort storePort) {
        this.storePort = Objects.requireNonNull(storePort, "storePort");
    }

    @Override
    public List<String> captureIds(String pipelineId) {
        Optional<Resource> stored = storePort.artifacts().get(pipelineId);
        if (stored.isEmpty() || !(stored.get() instanceof PipelineResource pipeline)) {
            return List.of();
        }
        List<String> ids = new ArrayList<>();
        for (SourceRef ref : pipeline.sources()) {
            if (!(ref instanceof SourceRef.Spec spec)) {
                // No srs switch recorded means this reference has never been through apply, so which
                // capture it would read through is not decided yet.
                continue;
            }
            Optional<Resource> source = storePort.artifacts().get(ref.id());
            if (source.isEmpty() || !(source.get() instanceof SourceResource resolved)) {
                continue;
            }
            CaptureRunSpec runSpec = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                    pipelineId,
                    pipeline.settings(),
                    resolved,
                    SourceCaptureResolution.of(resolved, SourceDiscovery.model(storePort, resolved)),
                    spec.srs());
            ids.add(CaptureId.of(runSpec).value());
        }
        return List.copyOf(ids);
    }
}
