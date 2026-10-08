package io.tapstate.control.core;

import io.tapstate.core.dsl.ReferenceGraph;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/** Deterministic revisions of a pipeline and the resolved resources its assembly references. */
public final class ObservationArtifactLineage {
    private ObservationArtifactLineage() { }

    public static Map<String, String> hashes(String pipelineId, Collection<Resource> snapshot) {
        Map<String, Resource> resources = new HashMap<>();
        snapshot.forEach(resource -> {
            if (resources.putIfAbsent(resource.id(), resource) != null) {
                throw new IllegalStateException("artifact lineage contains a duplicate resource id");
            }
        });
        ReferenceGraph graph = ReferenceGraph.of(snapshot);
        Map<String, String> hashes = new TreeMap<>();
        ArrayDeque<String> pending = new ArrayDeque<>();
        pending.add(pipelineId);
        while (!pending.isEmpty()) {
            String id = pending.removeFirst();
            Resource resource = resources.get(id);
            if (resource == null || hashes.containsKey(id)) { continue; }
            hashes.put(id, CanonicalHash.of(resource));
            graph.references(id).forEach(edge -> pending.addLast(edge.id()));
        }
        return Map.copyOf(hashes);
    }
}
