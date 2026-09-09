package io.tapstate.core.dsl;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.ViewBlock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The cross-resource reference index behind {@code desc}'s relationship view. It
 * records the coarse edges between top-level ids — which sources a pipeline reads, which reusable
 * definitions it {@code use:}s, which connection sources its serve sinks point at — and the
 * transpose, so {@code desc} can answer both "what does this reference" and "who references this".
 *
 * <p>Scope vs {@link ReferenceClosure}: the closure validates the fine-grained {@code from:}
 * addressing inside a pipeline (step ids, table names, regex, alias maps); the graph deliberately
 * ignores that intra-pipeline wiring and keeps only edges that cross top-level ids. An edge is
 * recorded only when its target resolves within the batch — a dangling name is a validate-layer
 * diagnostic ({@link ReferenceClosure}), not a phantom edge here. Edges are deduped (a source both
 * read and synced-to yields one edge) and sorted by id for a stable description.
 */
public final class ReferenceGraph {

    /** A directed edge to a resolved resource: its top-level id and its (parsed) kind. */
    public record Edge(String id, String kind) {
    }

    private static final Comparator<Edge> BY_ID = Comparator.comparing(Edge::id);

    /** Forward adjacency: resource id -> the resolved ids it references. */
    private final Map<String, List<Edge>> forward;
    /** Reverse adjacency: resource id -> the resolved ids that reference it. */
    private final Map<String, List<Edge>> reverse;

    private ReferenceGraph(Map<String, List<Edge>> forward, Map<String, List<Edge>> reverse) {
        this.forward = forward;
        this.reverse = reverse;
    }

    /** Builds the graph over a batch (a loaded workspace); references resolve within the batch. */
    public static ReferenceGraph of(Collection<Resource> resources) {
        Map<String, Resource> byId = new LinkedHashMap<>();
        for (Resource r : resources) {
            byId.putIfAbsent(r.id(), r);    // first wins; duplicate-id is a validate-layer concern
        }
        Map<String, List<Edge>> forward = new LinkedHashMap<>();
        Map<String, List<Edge>> reverse = new LinkedHashMap<>();
        for (Resource r : resources) {
            // on a duplicate id (a validate-layer error) only the first resource is canonical — skip the
            // shadows so the graph describes the same resource a consumer resolves first-wins by id
            if (byId.get(r.id()) != r) {
                continue;
            }
            Set<String> targets = referencedIds(r);
            List<Edge> edges = new ArrayList<>();
            for (String target : targets) {
                Resource resolved = byId.get(target);
                if (resolved != null) {
                    edges.add(new Edge(resolved.id(), resolved.kind()));
                }
            }
            edges.sort(BY_ID);
            forward.put(r.id(), edges);
            for (Edge e : edges) {
                reverse.computeIfAbsent(e.id(), k -> new ArrayList<>())
                        .add(new Edge(r.id(), r.kind()));
            }
        }
        reverse.values().forEach(list -> list.sort(BY_ID));
        return new ReferenceGraph(forward, reverse);
    }

    /** The resolved resources {@code id} references, sorted by id (empty for an unknown id). */
    public List<Edge> references(String id) {
        return forward.getOrDefault(id, List.of());
    }

    /** The resolved resources that reference {@code id}, sorted by id (empty for an unknown id). */
    public List<Edge> referencedBy(String id) {
        return reverse.getOrDefault(id, List.of());
    }

    /** The deduped set of top-level ids one resource names; intra-pipeline wiring is excluded. */
    private static Set<String> referencedIds(Resource r) {
        Set<String> ids = new LinkedHashSet<>();
        if (r instanceof PipelineResource p) {
            ids.addAll(p.sourceIds());
            if (p.transforms() != null) {
                for (Step step : p.transforms()) {
                    if (step instanceof Step.Use u) {
                        ids.add(u.use());
                    }
                }
            }
            if (p.view() instanceof ViewBlock.Use u) {
                ids.add(u.use());
            }
            collectServe(p.serve(), ids);
        } else if (r instanceof ServeResource serve) {
            collectSinkSources(serve.sync(), serve.push(), ids);
        }
        return ids;
    }

    /** A pipeline's serve block: a use-reference names its definition; an inline one names its sink sources. */
    private static void collectServe(ServeBlock serve, Set<String> ids) {
        switch (serve) {
            case null -> {
            }
            case ServeBlock.Use u -> ids.add(u.use());
            case ServeBlock.Inline inline -> collectSinkSources(inline.sync(), inline.push(), ids);
        }
    }

    /** The connection-source ids named by sync / push elements (X18). */
    private static void collectSinkSources(List<SyncElement> sync, List<PushElement> push, Set<String> ids) {
        if (sync != null) {
            for (SyncElement s : sync) {
                ids.add(s.source());
            }
        }
        if (push != null) {
            for (PushElement pe : push) {
                ids.add(pe.source());
            }
        }
    }
}
