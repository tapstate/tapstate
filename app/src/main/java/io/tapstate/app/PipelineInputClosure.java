package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.ReferenceGraph;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelinePreparationInputs;
import java.util.*;

/** One dependency traversal over the exact authored snapshot used by both confirmation and startup. */
final class PipelineInputClosure {
    private PipelineInputClosure() {}

    static Map<String, String> dependencyHashes(ArtifactStore snapshot, PipelineResource authored, PipelinePreparationInputs inputs) {
        var dependencies = new HashMap<String, String>(); var queue = new ArrayDeque<Resource>(); var visited = new HashSet<String>();
        queue.add(authored);
        while (!queue.isEmpty()) {
            var resource = queue.remove(); if (!visited.add(resource.id())) continue;
            for (var edge : ReferenceGraph.declaredReferences(resource)) {
                var required = snapshot.get(edge.id()).orElseThrow(PipelineInputClosure::fenced);
                if (!edge.kind().equals(required.kind())) throw fenced(); queue.add(required);
                if (!inputs.sourceHashes().containsKey(required.id())) dependencies.put(required.id(), CanonicalHash.of(required));
            }
        }
        return Map.copyOf(dependencies);
    }

    private static TapstateException fenced() { return new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null); }
}
