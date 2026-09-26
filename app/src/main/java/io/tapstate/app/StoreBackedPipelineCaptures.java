package io.tapstate.app;

import io.tapstate.control.core.PipelineCaptures;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.StorePort;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The capture identities a pipeline reads through, worked out from what is stored.
 *
 * <p>Derived rather than asked of whoever is running the pipeline, and that is the point of it. A
 * capture's identity is a function of the source contract and read axis. A ring-backed tail is identified
 * by its physical mining chain, while direct and bounded reads also name their own node and streams. Every
 * member computes the same ids from the same artifacts, and the read face
 * answers the same on a member that is running nothing. Asking the running member instead would give an answer
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
        return captureIds(pipelineId, new Reads());
    }

    /**
     * Every pipeline's captures from one set of reads: a source several of them read, and its discovery,
     * is read once for all of them rather than once for each pipeline naming it. The read face asks this
     * for every pipeline in the cluster whenever anybody looks, and a handful of sources shared by many
     * pipelines is the ordinary shape of a cluster.
     */
    @Override
    public Map<String, List<String>> captureIdsByPipeline(Collection<String> pipelineIds) {
        Reads reads = new Reads();
        Map<String, List<String>> byPipeline = new LinkedHashMap<>();
        for (String pipelineId : pipelineIds) {
            byPipeline.put(pipelineId, captureIds(pipelineId, reads));
        }
        return byPipeline;
    }

    private List<String> captureIds(String pipelineId, Reads reads) {
        Optional<Resource> stored = reads.artifact(pipelineId);
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
            Optional<Resource> source = reads.artifact(ref.id());
            if (source.isEmpty() || !(source.get() instanceof SourceResource resolved)) {
                continue;
            }
            // Narrowed to the tables the pipeline addresses, as its start narrows them. Direct and bounded
            // capture identities include this selection; a ring-backed CDC identity shares the physical
            // chain while its attachment still keeps this pipeline's selected tables.
            Optional<SourceCaptureResolution> selected;
            try {
                selected = SourceCaptureResolution.forPipeline(pipeline, resolved, reads.discovery(resolved));
            } catch (TapstateException unresolvable) {
                // The start is refused with this same code, so no capture of it exists; and this face
                // answers for every pipeline at once, which one that cannot resolve must not take down.
                continue;
            }
            if (selected.isEmpty()) {
                // It reads none of this source's tables, and its start opens no capture on it.
                continue;
            }
            CaptureRunSpec runSpec = StoreBackedPipelineCaptureCoordinator.deriveSpec(
                    pipelineId, pipeline.settings(), resolved, selected.orElseThrow(), spec.srs());
            ids.add(CaptureId.of(runSpec).value());
        }
        return List.copyOf(ids);
    }

    /**
     * What one answer has read from the store, so that it reads each artifact and each discovery once
     * however many of the pipelines it answers for name them.
     *
     * <p>Held for one answer and dropped with it. Kept any longer it would be a copy of the store that
     * nothing tells when the store changes, and the face would go on naming captures from a source somebody
     * has since edited. A read that fails is not kept either: the next pipeline naming that source asks
     * again, so one failed read is not made every pipeline's answer.
     */
    private final class Reads {

        private final Map<String, Optional<Resource>> artifacts = new HashMap<>();
        private final Map<String, Optional<SourceModel>> discoveries = new HashMap<>();

        Optional<Resource> artifact(String id) {
            return artifacts.computeIfAbsent(id, storePort.artifacts()::get);
        }

        /**
         * The source's discovery as {@link SourceDiscovery#model} answers it, null when there is none of its
         * current connector. Kept by source id, which is safe because the source itself is read once per
         * answer: the connector its discovery is matched against is the same for every pipeline naming it.
         */
        SourceModel discovery(SourceResource source) {
            Optional<SourceModel> model = discoveries.computeIfAbsent(
                    source.id(), ignored -> Optional.ofNullable(SourceDiscovery.model(storePort, source)));
            return model.orElse(null);
        }
    }
}
