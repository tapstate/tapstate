package io.tapstate.control.core;

import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Projects normalized DSL wiring to a graph without assigning editor coordinates. */
final class PipelineDagProjection {

    PipelineDag project(PipelineResource pipeline) {
        Objects.requireNonNull(pipeline, "pipeline");
        List<Step> transforms = pipeline.transforms() == null ? List.of() : pipeline.transforms();
        Set<String> transformIds = transforms.stream().map(Step::id).collect(Collectors.toSet());
        LinkedHashMap<String, PipelineDagNode> sourceNodes = new LinkedHashMap<>();
        List<PipelineDagEdge> edges = new ArrayList<>();
        Set<String> usedEdgeIds = new LinkedHashSet<>();
        Map<String, Integer> edgeCounts = new LinkedHashMap<>();

        for (Step transform : transforms) {
            addWiring(transform.from(), transformNodeId(transform.id()), transformIds, sourceNodes, edges, usedEdgeIds, edgeCounts);
        }
        if (pipeline.view() != null) {
            addWiring(viewFrom(pipeline.view()), viewNodeId(viewId(pipeline.view())), transformIds,
                    sourceNodes, edges, usedEdgeIds, edgeCounts);
        }
        if (pipeline.serve() != null) {
            String serveNodeId = serveNodeId(serveId(pipeline.serve(), pipeline.id()));
            FromRef serveFrom = serveFrom(pipeline.serve());
            if (pipeline.view() != null && isViewReference(serveFrom, pipeline.view())) {
                addKnownEdge(viewNodeId(viewId(pipeline.view())), serveNodeId, edges, usedEdgeIds);
            } else {
                addWiring(serveFrom, serveNodeId, transformIds, sourceNodes, edges, usedEdgeIds, edgeCounts);
            }
        }

        List<PipelineDagNode> nodes = new ArrayList<>(sourceNodes.values());
        for (Step transform : transforms) {
            nodes.add(new PipelineDagNode(
                    transformNodeId(transform.id()), "transform", transform.id(), transformType(transform)));
        }
        if (pipeline.view() != null) {
            String id = viewId(pipeline.view());
            nodes.add(new PipelineDagNode(viewNodeId(id), "view", id, null));
        }
        if (pipeline.serve() != null) {
            String id = serveId(pipeline.serve(), pipeline.id());
            nodes.add(new PipelineDagNode(serveNodeId(id), "serve", id, null));
            addTargets(pipeline.serve(), serveNodeId(id), nodes, edges, usedEdgeIds);
        }
        return new PipelineDag(nodes, edges);
    }

    private static void addWiring(
            FromClause from,
            String target,
            Set<String> transformIds,
            Map<String, PipelineDagNode> sourceNodes,
            List<PipelineDagEdge> edges,
            Set<String> usedEdgeIds,
            Map<String, Integer> edgeCounts) {
        if (from instanceof FromClause.Flow flow) {
            for (FromRef reference : flow.refs()) {
                addEdge(reference, null, target, transformIds, sourceNodes, edges, usedEdgeIds, edgeCounts);
            }
            return;
        }
        FromClause.Aliases aliases = (FromClause.Aliases) from;
        for (Map.Entry<String, FromRef> entry : aliases.aliases().entrySet()) {
            addEdge(entry.getValue(), entry.getKey(), target, transformIds, sourceNodes, edges, usedEdgeIds, edgeCounts);
        }
    }

    private static void addWiring(
            FromRef from,
            String target,
            Set<String> transformIds,
            Map<String, PipelineDagNode> sourceNodes,
            List<PipelineDagEdge> edges,
            Set<String> usedEdgeIds,
            Map<String, Integer> edgeCounts) {
        addEdge(from, null, target, transformIds, sourceNodes, edges, usedEdgeIds, edgeCounts);
    }

    private static void addEdge(
            FromRef reference,
            String alias,
            String target,
            Set<String> transformIds,
            Map<String, PipelineDagNode> sourceNodes,
            List<PipelineDagEdge> edges,
            Set<String> usedEdgeIds,
            Map<String, Integer> edgeCounts) {
        String source = nodeFor(reference, transformIds, sourceNodes);
        String base = source + "->" + target;
        int occurrence = edgeCounts.merge(base, 1, Integer::sum);
        String id = alias == null ? base : base + ":" + alias;
        while (!usedEdgeIds.add(id)) {
            id = base + ":" + occurrence++;
        }
        edges.add(new PipelineDagEdge(id, source, target, alias));
    }

    private static void addKnownEdge(
            String source, String target, List<PipelineDagEdge> edges, Set<String> usedEdgeIds) {
        String id = source + "->" + target;
        if (!usedEdgeIds.add(id)) {
            throw new IllegalStateException("Pipeline graph contains a duplicate edge: " + id);
        }
        edges.add(new PipelineDagEdge(id, source, target, null));
    }

    private static String nodeFor(
            FromRef reference, Set<String> transformIds, Map<String, PipelineDagNode> sourceNodes) {
        if (reference instanceof FromRef.Literal literal && transformIds.contains(literal.ref())) {
            return transformNodeId(literal.ref());
        }
        String label = referenceLabel(reference);
        String id = sourceNodeId(label);
        sourceNodes.putIfAbsent(id, new PipelineDagNode(id, "source", label, null));
        return id;
    }

    private static String transformType(Step step) {
        if (step instanceof Step.Inline inline) {
            TransformBody body = inline.body();
            return body.type();
        }
        return "use";
    }

    private static void addTargets(
            ServeBlock serve,
            String serveNodeId,
            List<PipelineDagNode> nodes,
            List<PipelineDagEdge> edges,
            Set<String> usedEdgeIds) {
        if (!(serve instanceof ServeBlock.Inline inline)) {
            return;
        }
        Map<String, List<String>> targetKinds = new LinkedHashMap<>();
        if (inline.sync() != null) {
            for (SyncElement sync : inline.sync()) {
                targetKinds.computeIfAbsent(sync.source(), ignored -> new ArrayList<>()).add("sync");
            }
        }
        if (inline.push() != null) {
            for (PushElement push : inline.push()) {
                targetKinds.computeIfAbsent(push.source(), ignored -> new ArrayList<>()).add("push");
            }
        }
        for (Map.Entry<String, List<String>> target : targetKinds.entrySet()) {
            String targetNodeId = "target:" + target.getKey();
            nodes.add(new PipelineDagNode(targetNodeId, "target", target.getKey(), String.join(" + ", target.getValue())));
            String edgeId = serveNodeId + "->" + targetNodeId;
            if (!usedEdgeIds.add(edgeId)) {
                throw new IllegalStateException("Pipeline graph contains a duplicate target edge: " + edgeId);
            }
            edges.add(new PipelineDagEdge(edgeId, serveNodeId, targetNodeId, null));
        }
    }

    private static String viewId(ViewBlock view) {
        return switch (view) {
            case ViewBlock.Inline inline -> inline.id();
            case ViewBlock.Use use -> use.id();
        };
    }

    private static FromRef viewFrom(ViewBlock view) {
        return switch (view) {
            case ViewBlock.Inline inline -> inline.from();
            case ViewBlock.Use use -> use.from();
        };
    }

    private static String serveId(ServeBlock serve, String pipelineId) {
        return switch (serve) {
            case ServeBlock.Inline inline -> inline.id() == null ? pipelineId + "_serve" : inline.id();
            case ServeBlock.Use use -> use.id();
        };
    }

    private static FromRef serveFrom(ServeBlock serve) {
        return switch (serve) {
            case ServeBlock.Inline inline -> inline.from();
            case ServeBlock.Use use -> use.from();
        };
    }

    private static boolean isViewReference(FromRef reference, ViewBlock view) {
        return reference instanceof FromRef.Literal literal && literal.ref().equals(viewId(view));
    }

    private static String referenceLabel(FromRef reference) {
        return switch (reference) {
            case FromRef.Literal literal -> literal.ref();
            case FromRef.Regex regex -> "/" + regex.pattern() + "/";
        };
    }

    private static String sourceNodeId(String reference) {
        return "source:" + reference;
    }

    private static String transformNodeId(String id) {
        return "transform:" + id;
    }

    private static String viewNodeId(String id) {
        return "view:" + id;
    }

    private static String serveNodeId(String id) {
        return "serve:" + id;
    }
}
