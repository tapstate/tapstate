package io.tapstate.control.core;

import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRename;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Projects normalized DSL wiring to a graph without assigning editor coordinates. */
final class PipelineDagProjection {

    PipelineDag project(PipelineResource pipeline, List<PipelineSourceSummary> sourceSummaries) {
        Objects.requireNonNull(pipeline, "pipeline");
        Objects.requireNonNull(sourceSummaries, "sourceSummaries");
        List<Step> transforms = pipeline.transforms() == null ? List.of() : pipeline.transforms();
        Map<String, String> upstreamNodes = new LinkedHashMap<>();
        for (Step transform : transforms) {
            upstreamNodes.put(transform.id(), transformNodeId(transform.id()));
        }
        if (pipeline.view() != null) {
            String viewId = viewId(pipeline.view());
            upstreamNodes.put(viewId, viewNodeId(viewId));
        }
        LinkedHashMap<String, PipelineDagNode> sourceNodes = new LinkedHashMap<>();
        LinkedHashMap<String, PipelineDagNode> targetNodes = new LinkedHashMap<>();
        List<PipelineDagEdge> edges = new ArrayList<>();
        Set<String> usedEdgeIds = new LinkedHashSet<>();
        Map<String, Integer> edgeCounts = new LinkedHashMap<>();

        for (Step transform : transforms) {
            addWiring(transform.from(), transformNodeId(transform.id()), upstreamNodes, sourceSummaries,
                    sourceNodes, edges, usedEdgeIds, edgeCounts);
        }
        if (pipeline.view() != null) {
            addWiring(viewFrom(pipeline.view()), viewNodeId(viewId(pipeline.view())), upstreamNodes, sourceSummaries,
                    sourceNodes, edges, usedEdgeIds, edgeCounts);
        }
        if (pipeline.serve() instanceof ServeBlock.Inline serve) {
            addServeTerminals(serve, upstreamNodes, sourceSummaries, sourceNodes, targetNodes,
                    edges, usedEdgeIds, edgeCounts);
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
        nodes.addAll(targetNodes.values());
        return new PipelineDag(nodes, edges);
    }

    private static void addWiring(
            FromClause from,
            String target,
            Map<String, String> upstreamNodes,
            List<PipelineSourceSummary> sourceSummaries,
            Map<String, PipelineDagNode> sourceNodes,
            List<PipelineDagEdge> edges,
            Set<String> usedEdgeIds,
            Map<String, Integer> edgeCounts) {
        if (from instanceof FromClause.Flow flow) {
            for (FromRef reference : flow.refs()) {
                addEdge(reference, null, target, upstreamNodes, sourceSummaries,
                        sourceNodes, edges, usedEdgeIds, edgeCounts);
            }
            return;
        }
        FromClause.Aliases aliases = (FromClause.Aliases) from;
        for (Map.Entry<String, FromRef> entry : aliases.aliases().entrySet()) {
            addEdge(entry.getValue(), entry.getKey(), target, upstreamNodes, sourceSummaries,
                    sourceNodes, edges, usedEdgeIds, edgeCounts);
        }
    }

    private static void addWiring(
            FromRef from,
            String target,
            Map<String, String> upstreamNodes,
            List<PipelineSourceSummary> sourceSummaries,
            Map<String, PipelineDagNode> sourceNodes,
            List<PipelineDagEdge> edges,
            Set<String> usedEdgeIds,
            Map<String, Integer> edgeCounts) {
        addEdge(from, null, target, upstreamNodes, sourceSummaries, sourceNodes, edges, usedEdgeIds, edgeCounts);
    }

    private static void addEdge(
            FromRef reference,
            String alias,
            String target,
            Map<String, String> upstreamNodes,
            List<PipelineSourceSummary> sourceSummaries,
            Map<String, PipelineDagNode> sourceNodes,
            List<PipelineDagEdge> edges,
            Set<String> usedEdgeIds,
            Map<String, Integer> edgeCounts) {
        String source = nodeFor(reference, upstreamNodes, sourceSummaries, sourceNodes);
        String base = source + "->" + target;
        int occurrence = edgeCounts.merge(base, 1, Integer::sum);
        String id = alias == null ? base : base + ":" + alias;
        while (!usedEdgeIds.add(id)) {
            id = base + ":" + occurrence++;
        }
        edges.add(new PipelineDagEdge(id, source, target, alias));
    }

    private static String nodeFor(
            FromRef reference,
            Map<String, String> upstreamNodes,
            List<PipelineSourceSummary> sourceSummaries,
            Map<String, PipelineDagNode> sourceNodes) {
        if (reference instanceof FromRef.Literal literal && upstreamNodes.containsKey(literal.ref())) {
            return upstreamNodes.get(literal.ref());
        }
        String label = referenceLabel(reference);
        SourceTable sourceTable = resolveSourceTable(reference, sourceSummaries);
        String id = sourceTable == null ? sourceNodeId(label) : sourceNodeId(sourceTable.sourceId(), sourceTable.table());
        sourceNodes.putIfAbsent(id, sourceTable == null
                ? new PipelineDagNode(id, "source", label, null)
                : new PipelineDagNode(id, "source", sourceTable.table(), sourceTable.sourceId()));
        return id;
    }

    private static String transformType(Step step) {
        if (step instanceof Step.Inline inline) {
            TransformBody body = inline.body();
            return body.type();
        }
        return "use";
    }

    private static void addServeTerminals(
            ServeBlock.Inline serve,
            Map<String, String> upstreamNodes,
            List<PipelineSourceSummary> sourceSummaries,
            Map<String, PipelineDagNode> sourceNodes,
            Map<String, PipelineDagNode> targetNodes,
            List<PipelineDagEdge> edges,
            Set<String> usedEdgeIds,
            Map<String, Integer> edgeCounts) {
        if (serve.sync() != null) {
            for (SyncElement sync : serve.sync()) {
                String sourceTable = tableForTerminal(serve.from());
                String targetTable = TableRename.apply(sourceTable, sync.rename());
                String targetNodeId = targetNodeId(sync.source(), targetTable);
                targetNodes.putIfAbsent(targetNodeId,
                        new PipelineDagNode(targetNodeId, "target", targetTable, sync.source()));
                addWiring(serve.from(), targetNodeId, upstreamNodes, sourceSummaries,
                        sourceNodes, edges, usedEdgeIds, edgeCounts, sync.id());
            }
        }
        if (serve.push() != null) {
            for (int index = 0; index < serve.push().size(); index++) {
                PushElement push = serve.push().get(index);
                String id = push.id() == null ? "push_" + (index + 1) : push.id();
                String targetNodeId = "target:" + push.source() + ":push:" + id;
                String label = push.topic() == null ? id : push.topic();
                targetNodes.putIfAbsent(targetNodeId,
                        new PipelineDagNode(targetNodeId, "target", label, push.source()));
                addWiring(serve.from(), targetNodeId, upstreamNodes, sourceSummaries,
                        sourceNodes, edges, usedEdgeIds, edgeCounts, push.id());
            }
        }
    }

    private static void addWiring(
            FromRef from,
            String target,
            Map<String, String> upstreamNodes,
            List<PipelineSourceSummary> sourceSummaries,
            Map<String, PipelineDagNode> sourceNodes,
            List<PipelineDagEdge> edges,
            Set<String> usedEdgeIds,
            Map<String, Integer> edgeCounts,
            String edgeLabel) {
        addEdge(from, edgeLabel, target, upstreamNodes, sourceSummaries,
                sourceNodes, edges, usedEdgeIds, edgeCounts);
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

    private static String referenceLabel(FromRef reference) {
        return switch (reference) {
            case FromRef.Literal literal -> literal.ref();
            case FromRef.Regex regex -> "/" + regex.pattern() + "/";
        };
    }

    private static String sourceNodeId(String reference) {
        return "source:" + reference;
    }

    private static String sourceNodeId(String sourceId, String table) {
        return "source:" + sourceId + ":" + table;
    }

    private static String targetNodeId(String sourceId, String table) {
        return "target:" + sourceId + ":" + table;
    }

    private static String transformNodeId(String id) {
        return "transform:" + id;
    }

    private static String viewNodeId(String id) {
        return "view:" + id;
    }

    private static String tableForTerminal(FromRef reference) {
        if (reference instanceof FromRef.Literal literal) {
            String value = literal.ref();
            int separator = value.indexOf('.');
            return separator < 0 ? value : value.substring(separator + 1);
        }
        return referenceLabel(reference);
    }

    private static SourceTable resolveSourceTable(FromRef reference, List<PipelineSourceSummary> sourceSummaries) {
        if (reference instanceof FromRef.Literal literal) {
            String value = literal.ref();
            int separator = value.indexOf('.');
            if (separator >= 0) {
                String sourceId = value.substring(0, separator);
                if (sourceSummaries.stream().anyMatch(source -> source.id().equals(sourceId))) {
                    return new SourceTable(sourceId, value.substring(separator + 1));
                }
                return null;
            }
            if (sourceSummaries.size() == 1) {
                return new SourceTable(sourceSummaries.getFirst().id(), value);
            }
            return null;
        }
        if (sourceSummaries.size() == 1) {
            return new SourceTable(sourceSummaries.getFirst().id(), referenceLabel(reference));
        }
        return null;
    }

    private record SourceTable(String sourceId, String table) {
    }
}
