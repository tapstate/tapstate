package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.RenameSpec;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.WriteMode;
import io.tapstate.spi.store.PipelineDraft;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Deterministic compiler from the server-owned Pipeline authoring model to a runnable Artifact. */
public final class PipelineDraftCompiler {

    /** Compiles one complete or incomplete draft without persisting or mutating it. */
    public PipelineResource compile(PipelineDraft draft) {
        Objects.requireNonNull(draft, "draft");
        return draft.mode() == PipelineDraft.Mode.WIZARD ? compileWizard(draft) : compileGraph(draft);
    }

    private PipelineResource compileWizard(PipelineDraft draft) {
        PipelineDraft.Wizard wizard = Objects.requireNonNull(draft.wizard(), "wizard payload");
        PipelineDraft.Root root = Objects.requireNonNull(wizard.root(), "wizard root");
        requiredText(root.sourceId(), "wizard root source");
        requiredText(root.table(), "wizard root table");
        validateWizardIds(root, wizard.related());

        LinkedHashSet<String> sourceIds = new LinkedHashSet<>();
        sourceIds.add(root.sourceId());
        wizard.related().forEach(related -> sourceIds.add(related.sourceId()));

        LinkedHashMap<String, FromRef> aliases = new LinkedHashMap<>();
        List<Step> steps = new ArrayList<>();
        Map<String, String> outputAliases = new LinkedHashMap<>();
        outputAliases.put(root.id(), compileBranch(root.id(), root.sourceId() + "." + root.table(),
                root.preTransforms(), aliases, steps));

        Map<String, List<PipelineDraft.Related>> children = new LinkedHashMap<>();
        for (PipelineDraft.Related related : wizard.related()) {
            requiredText(related.sourceId(), "related source");
            requiredText(related.table(), "related table");
            if (!related.parentId().equals(root.id()) && !containsRelated(wizard.related(), related.parentId())) {
                throw new IllegalArgumentException("related table parent does not exist: " + related.parentId());
            }
            children.computeIfAbsent(related.parentId(), ignored -> new ArrayList<>()).add(related);
            outputAliases.put(related.id(), compileBranch(related.id(),
                    related.sourceId() + "." + related.table(), related.preTransforms(), aliases, steps));
        }

        String previous;
        if (wizard.related().isEmpty()) {
            previous = outputAliases.get(root.id());
        } else {
            if (root.key().isEmpty()) {
                throw new IllegalArgumentException("wizard root requires a document key when relations exist");
            }
            List<Embed> embeds = embedsFor(root.id(), children);
            String nestId = draft.pipelineId() + "__nest";
            TransformBody.Nest nest = new TransformBody.Nest(null, null,
                    new NestRoot(root.id(), root.key(), null, null, embeds));
            steps.add(new Step.Inline(nestId, FromClause.aliases(aliases), nest, Map.of()));
            previous = nestId;
        }
        for (PipelineDraft.Transform transform : wizard.transforms()) {
            Step step = compileTransform(transform, previous);
            steps.add(step);
            previous = step.id();
        }
        ServeBlock serve = compileWizardOutput(wizard.output(), previous, root.table());
        return new PipelineResource(draft.pipelineId(), metadata(draft),
                sourceIds.stream().map(id -> (SourceRef) SourceRef.bare(id)).toList(), steps, null, serve, null, Map.of());
    }

    private static ServeBlock compileWizardOutput(PipelineDraft.Output output, String from, String inputTable) {
        if (output == null) {
            throw new IllegalArgumentException("wizard output is required for publication");
        }
        if (!"atlas".equals(output.kind()) && !"source".equals(output.kind())) {
            throw new IllegalArgumentException("unsupported wizard output: " + output.kind());
        }
        String destinationId = firstText(output.config(), "sourceId", "destinationId", "source");
        String destinationTable = firstText(output.config(), "table", "destinationTable");
        if (destinationId == null || destinationTable == null) {
            throw new IllegalArgumentException("wizard output requires sourceId and table");
        }
        String syncId = firstText(output.config(), "syncId");
        if (syncId == null) {
            syncId = destinationId + "_" + destinationTable;
        }
        RenameSpec rename = inputTable != null && !inputTable.equals(destinationTable)
                ? new RenameSpec(Map.of(inputTable, destinationTable), null, null, null) : null;
        String serveId = "atlas".equals(output.kind()) ? "atlas" : "target";
        return new ServeBlock.Inline(serveId, FromClause.list(FromRef.literal(from)),
                List.of(new SyncElement(syncId, destinationId,
                        outputWriteMode(output.config()), rename, null, null)), null, null);
    }

    private static String firstText(Map<String, Object> values, String... names) {
        for (String name : names) {
            Object value = values.get(name);
            if (value instanceof String text && !text.isBlank()) {
                return text;
            }
        }
        return null;
    }

    private static WriteMode outputWriteMode(Map<String, Object> config) {
        String value = firstText(config, "writeMode", "write_mode");
        if (value == null || "upsert".equals(value)) {
            return WriteMode.UPSERT;
        }
        if ("append".equals(value)) {
            return WriteMode.APPEND;
        }
        throw new IllegalArgumentException("unsupported wizard output write mode: " + value);
    }

    private static boolean containsRelated(List<PipelineDraft.Related> related, String id) {
        return related.stream().anyMatch(candidate -> candidate.id().equals(id));
    }

    private static void validateWizardIds(PipelineDraft.Root root, List<PipelineDraft.Related> related) {
        Set<String> ids = new HashSet<>();
        if (!ids.add(root.id())) {
            throw new IllegalArgumentException("duplicate wizard id: " + root.id());
        }
        for (PipelineDraft.Related child : related) {
            if (!ids.add(child.id())) {
                throw new IllegalArgumentException("duplicate wizard id: " + child.id());
            }
            if (child.id().equals(child.parentId())) {
                throw new IllegalArgumentException("wizard relation cannot point to itself: " + child.id());
            }
        }
        for (PipelineDraft.Related child : related) {
            String parent = child.parentId();
            Set<String> seen = new HashSet<>();
            while (!parent.equals(root.id())) {
                if (!seen.add(parent)) {
                    throw new IllegalArgumentException("wizard related table cycle at: " + child.id());
                }
                String parentId = parent;
                PipelineDraft.Related parentNode = related.stream()
                        .filter(candidate -> candidate.id().equals(parentId)).findFirst().orElse(null);
                if (parentNode == null) {
                    throw new IllegalArgumentException("related table parent does not exist: " + parent);
                }
                parent = parentNode.parentId();
            }
        }
    }

    private static String compileBranch(String owner, String source, List<PipelineDraft.Transform> transforms,
            Map<String, FromRef> aliases, List<Step> steps) {
        String previous = source;
        int index = 0;
        for (PipelineDraft.Transform transform : transforms) {
            String stepId = index++ == 0 ? owner + "__pre" : owner + "__pre__" + index + "__" + transform.id();
            Step step = compileTransform(new PipelineDraft.Transform(stepId, transform.type(), transform.fields()), previous);
            steps.add(step);
            previous = stepId;
        }
        aliases.put(owner, FromRef.literal(previous));
        return previous;
    }

    private static List<Embed> embedsFor(String parentId, Map<String, List<PipelineDraft.Related>> children) {
        List<Embed> result = new ArrayList<>();
        for (PipelineDraft.Related related : children.getOrDefault(parentId, List.of())) {
            PipelineDraft.Relation relation = related.relation();
            if (relation.shape() == PipelineDraft.Shape.ARRAY && relation.arrayKey().isEmpty()) {
                throw new IllegalArgumentException("array relation requires an array key");
            }
            if (relation.shape() == PipelineDraft.Shape.FLAT && relation.path() != null) {
                throw new IllegalArgumentException("flat relation must not have a target path");
            }
            if (relation.shape() != PipelineDraft.Shape.FLAT && (relation.path() == null || relation.path().isBlank())) {
                throw new IllegalArgumentException("object and array relations require a target path");
            }
            if (relation.on().isEmpty()) {
                throw new IllegalArgumentException("relation requires at least one association condition");
            }
            for (PipelineDraft.FieldPair pair : relation.on()) {
                requiredText(pair.childField(), "related child field");
                requiredText(pair.parentField(), "related parent field");
            }
            Map<String, String> on = new LinkedHashMap<>();
            relation.on().forEach(pair -> {
                if (on.putIfAbsent(pair.childField(), pair.parentField()) != null) {
                    throw new IllegalArgumentException(
                            "related association contains duplicate child field: " + pair.childField());
                }
            });
            List<String> key = relation.key().isEmpty() ? null : relation.key();
            List<String> arrayKey = relation.shape() == PipelineDraft.Shape.ARRAY ? relation.arrayKey() : null;
            EmbedAs shape = switch (relation.shape()) {
                case FLAT -> EmbedAs.FLAT;
                case OBJECT -> EmbedAs.OBJECT;
                case ARRAY -> EmbedAs.ARRAY;
            };
            result.add(new Embed(related.id(), on,
                    shape,
                    relation.path(), key, arrayKey, null, null,
                    embedsFor(related.id(), children)));
        }
        return result;
    }

    private static Step compileTransform(PipelineDraft.Transform transform, String previous) {
        FromClause from = FromClause.list(FromRef.literal(previous));
        TransformBody body = switch (transform.type()) {
            case "map" -> new TransformBody.MapProjection(mapFields(transform.fields()));
            case "filter" -> new TransformBody.Filter(requiredText(transform.fields(), "expr"));
            default -> throw new IllegalArgumentException("unsupported wizard transform: " + transform.type());
        };
        return Step.inline(transform.id(), from, body, Map.of());
    }

    private static Map<String, FieldRule> mapFields(Map<String, Object> fields) {
        Map<String, FieldRule> result = new LinkedHashMap<>();
        fields.forEach((name, value) -> {
            if (value instanceof String string && string.startsWith("$")) {
                result.put(name, FieldRule.rename(string.substring(1)));
            } else if (value instanceof String string && string.startsWith("=")) {
                String expression = string.substring(1);
                if (expression.isBlank()) {
                    throw new IllegalArgumentException("computed map expression must be non-blank: " + name);
                }
                result.put(name, FieldRule.computed(expression));
            } else if (Boolean.FALSE.equals(value)) {
                result.put(name, FieldRule.drop());
            } else {
                result.put(name, FieldRule.literal(value));
            }
        });
        return result;
    }

    private static String requiredText(Map<String, Object> fields, String name) {
        Object value = fields.get(name);
        if (!(value instanceof String string) || string.isBlank()) {
            throw new IllegalArgumentException("transform field must be non-blank: " + name);
        }
        return string;
    }

    private static String requiredText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("wizard field must be non-blank: " + name);
        }
        return value;
    }

    private PipelineResource compileGraph(PipelineDraft draft) {
        PipelineDraft.Graph graph = Objects.requireNonNull(draft.graph(), "graph payload");
        LinkedHashSet<String> sourceIds = new LinkedHashSet<>();
        Map<String, PipelineDraft.Node> nodes = new LinkedHashMap<>();
        for (PipelineDraft.Node node : graph.nodes()) {
            if (nodes.put(node.id(), node) != null) {
                throw new IllegalArgumentException("duplicate graph node: " + node.id());
            }
            if ("source".equals(node.type()) && node.sourceId() != null && !node.sourceId().isBlank()) {
                sourceIds.add(node.sourceId());
            }
        }
        Map<String, List<String>> inputs = new LinkedHashMap<>();
        for (PipelineDraft.Edge edge : graph.edges()) {
            if (!nodes.containsKey(edge.source()) || !nodes.containsKey(edge.target())) {
                throw new IllegalArgumentException("graph edge references an unknown node: " + edge.id());
            }
            inputs.computeIfAbsent(edge.target(), ignored -> new ArrayList<>()).add(edge.source());
        }
        List<Step> steps = new ArrayList<>();
        Map<String, List<String>> outputs = new LinkedHashMap<>();
        Set<String> visiting = new HashSet<>();
        ViewBlock view = null;
        ServeBlock serve = null;
        for (PipelineDraft.Node node : graph.nodes()) {
            if ("view".equals(node.type())) {
                if (view != null) {
                    throw new IllegalArgumentException("graph has more than one view node");
                }
                List<String> refs = graphOutputs(node.id(), nodes, inputs, outputs, visiting, steps);
                if (refs.size() != 1) {
                    throw new IllegalArgumentException("view node requires exactly one input: " + node.id());
                }
                String use = optionalText(node.config(), "use");
                String viewId = optionalText(node.metadata(), "viewId");
                if (viewId == null) {
                    viewId = node.id();
                }
                view = use == null
                        ? new ViewBlock.Inline(viewId, graphFromRef(refs.getFirst()),
                                optionalText(node.config(), "primaryKey", "primary_key"), null)
                        : new ViewBlock.Use(viewId, use, graphFromRef(refs.getFirst()));
            } else if ("target".equals(node.type())) {
                if (serve != null) {
                    throw new IllegalArgumentException("graph has more than one target node");
                }
                List<String> refs = graphOutputs(node.id(), nodes, inputs, outputs, visiting, steps);
                if (refs.isEmpty()) {
                    throw new IllegalArgumentException("target node requires an input: " + node.id());
                }
                String targetSource = requiredNodeText(node.sourceId(), "target source", node.id());
                RenameSpec rename = graphTargetRename(node, nodes, inputs);
                Set<String> occupiedIds = new HashSet<>(nodes.keySet());
                for (PipelineDraft.Node candidate : graph.nodes()) {
                    if ("view".equals(candidate.type())) {
                        String viewId = optionalText(candidate.metadata(), "viewId");
                        if (viewId != null) {
                            occupiedIds.add(viewId);
                        }
                    }
                }
                String serveId = node.id() + "__serve";
                while (occupiedIds.contains(serveId)) {
                    serveId += "_";
                }
                serve = new ServeBlock.Inline(serveId,
                        new FromClause.Flow(refs.stream().map(PipelineDraftCompiler::graphFromRef).toList()),
                        List.of(new SyncElement(node.id(), targetSource, writeMode(node.config(), node.id()), rename, null, null)), null, null);
            } else {
                graphOutputs(node.id(), nodes, inputs, outputs, visiting, steps);
            }
        }
        return new PipelineResource(draft.pipelineId(), metadata(draft),
                sourceIds.stream().map(id -> (SourceRef) SourceRef.bare(id)).toList(),
                steps, view, serve, null, Map.of());
    }

    private static RenameSpec graphTargetRename(PipelineDraft.Node target, Map<String, PipelineDraft.Node> nodes,
            Map<String, List<String>> inputs) {
        Map<String, String> tableMappings = graphTargetMappings(target);
        if (tableMappings != null) {
            return new RenameSpec(tableMappings, null, null, null);
        }
        if (target.table() == null || target.table().isBlank()) {
            return null;
        }
        Set<String> inputTables = new LinkedHashSet<>();
        for (String input : inputs.getOrDefault(target.id(), List.of())) {
            collectOutputTables(input, nodes, inputs, new HashSet<>(), inputTables);
        }
        if (inputTables.size() != 1) {
            throw new IllegalArgumentException("target table requires exactly one upstream source table: " + target.id());
        }
        String inputTable = inputTables.iterator().next();
        if (isRegexReference(inputTable)) {
            throw new IllegalArgumentException("target table cannot rename a regex-selected source table: " + target.id());
        }
        return inputTable.equals(target.table()) ? null
                : new RenameSpec(Map.of(inputTable, target.table()), null, null, null);
    }

    private static Map<String, String> graphTargetMappings(PipelineDraft.Node target) {
        Object configured = target.config().get("tableMappings");
        if (configured == null) {
            return null;
        }
        if (!(configured instanceof List<?> mappings)) {
            throw new IllegalArgumentException("target tableMappings must be a list: " + target.id());
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (int index = 0; index < mappings.size(); index++) {
            Object value = mappings.get(index);
            if (!(value instanceof Map<?, ?> mapping)) {
                throw new IllegalArgumentException("target tableMappings[" + index + "] must be an object: "
                        + target.id());
            }
            String sourceTable = mapText(mapping, "sourceTable");
            String targetTable = mapText(mapping, "targetTable");
            if (sourceTable == null || targetTable == null) {
                throw new IllegalArgumentException("target tableMappings[" + index
                        + "] requires sourceTable and targetTable: " + target.id());
            }
            if (isRegexReference(sourceTable)) {
                throw new IllegalArgumentException("target tableMappings cannot rename a regex selector: "
                        + sourceTable);
            }
            if (result.putIfAbsent(sourceTable, targetTable) != null) {
                throw new IllegalArgumentException("duplicate target table mapping: " + sourceTable);
            }
        }
        return result.isEmpty() ? null : result;
    }

    private static String mapText(Map<?, ?> values, String key) {
        Object value = values.get(key);
        return value instanceof String text && !text.isBlank() ? text : null;
    }

    private static void collectOutputTables(String nodeId, Map<String, PipelineDraft.Node> nodes,
            Map<String, List<String>> inputs, Set<String> visited, Set<String> tables) {
        if (!visited.add(nodeId)) {
            return;
        }
        PipelineDraft.Node node = nodes.get(nodeId);
        if ("source".equals(node.type())) {
            for (SourceTableSelection selection : sourceTableSelections(node)) {
                tables.add(selection.renameKey());
            }
            return;
        }
        if (isGraphTransform(node)) {
            tables.add(graphStepId(node));
            return;
        }
        for (String input : inputs.getOrDefault(nodeId, List.of())) {
            collectOutputTables(input, nodes, inputs, visited, tables);
        }
    }

    private static List<String> graphOutputs(String nodeId, Map<String, PipelineDraft.Node> nodes,
            Map<String, List<String>> inputs, Map<String, List<String>> outputs, Set<String> visiting, List<Step> steps) {
        List<String> existing = outputs.get(nodeId);
        if (existing != null) {
            return existing;
        }
        if (!visiting.add(nodeId)) {
            throw new IllegalArgumentException("graph contains a cycle at: " + nodeId);
        }
        PipelineDraft.Node node = nodes.get(nodeId);
        List<String> result;
        if ("source".equals(node.type())) {
            String sourceId = requiredNodeText(node.sourceId(), "source id", node.id());
            result = sourceTableSelections(node).stream()
                    .map(selection -> selection.reference(sourceId))
                    .toList();
        } else {
            List<String> refs = new ArrayList<>();
            for (String input : inputs.getOrDefault(nodeId, List.of())) {
                refs.addAll(graphOutputs(input, nodes, inputs, outputs, visiting, steps));
            }
            if ("view".equals(node.type()) || "target".equals(node.type())) {
                result = List.copyOf(refs);
            } else {
                if (!isGraphTransform(node)) {
                    throw new IllegalArgumentException("unsupported graph node: " + node.type());
                }
                FromClause from = graphTransformFrom(node, refs);
                TransformBody body = graphTransformBody(node);
                String stepId = graphStepId(node);
                steps.add(Step.inline(stepId, from, body, Map.of()));
                result = List.of(stepId);
            }
        }
        visiting.remove(nodeId);
        outputs.put(nodeId, result);
        return result;
    }

    private static boolean isGraphTransform(PipelineDraft.Node node) {
        return switch (node.type()) {
            case "map", "filter", "js", "unwind", "union", "join", "nest" -> true;
            default -> "transform".equals(node.metadata().get("editorKind"));
        };
    }

    private static String graphStepId(PipelineDraft.Node node) {
        return optionalText(node.metadata(), "stepId") == null
                ? node.id() : optionalText(node.metadata(), "stepId");
    }

    private static FromClause graphTransformFrom(PipelineDraft.Node node, List<String> graphRefs) {
        Object configured = node.config().get("from");
        boolean aliasMap = "join".equals(node.type()) || "nest".equals(node.type());
        if (configured != null) {
            if (aliasMap) {
                if (!(configured instanceof Map<?, ?> aliases)) {
                    throw new IllegalArgumentException("graph " + node.type()
                            + " transform requires an alias-map from: " + node.id());
                }
                Map<String, FromRef> refs = new LinkedHashMap<>();
                for (Map.Entry<?, ?> entry : aliases.entrySet()) {
                    if (!(entry.getKey() instanceof String alias) || alias.isBlank()
                            || !(entry.getValue() instanceof String ref) || ref.isBlank()) {
                        throw new IllegalArgumentException("graph transform from aliases must map names to refs: "
                                + node.id());
                    }
                    refs.put(alias, graphFromRef(ref));
                }
                return FromClause.aliases(refs);
            }
            if (configured instanceof String ref && !ref.isBlank()) {
                return FromClause.list(graphFromRef(ref));
            }
            if (configured instanceof List<?> values) {
                List<FromRef> refs = new ArrayList<>();
                for (Object value : values) {
                    if (!(value instanceof String ref) || ref.isBlank()) {
                        throw new IllegalArgumentException("graph transform from must contain non-blank refs: "
                                + node.id());
                    }
                    refs.add(graphFromRef(ref));
                }
                if (!refs.isEmpty()) {
                    return new FromClause.Flow(refs);
                }
                throw new IllegalArgumentException("graph transform from must not be empty: " + node.id());
            }
            throw new IllegalArgumentException("graph transform has an invalid from value: " + node.id());
        }
        if (aliasMap) {
            throw new IllegalArgumentException("graph " + node.type()
                    + " transform requires an explicit alias-map from: " + node.id());
        }
        if (graphRefs.isEmpty()) {
            throw new IllegalArgumentException("graph node requires an input: " + node.id());
        }
        return new FromClause.Flow(graphRefs.stream().map(PipelineDraftCompiler::graphFromRef).toList());
    }

    private static FromRef graphFromRef(String value) {
        if (isRegexReference(value)) {
            return FromRef.regex(value.substring(1, value.length() - 1));
        }
        return FromRef.literal(value);
    }

    private static TransformBody graphTransformBody(PipelineDraft.Node node) {
        Object options = node.config().get("options");
        if (options != null && (!(options instanceof Map<?, ?> map) || !map.isEmpty())) {
            throw new IllegalArgumentException("graph transform options are not supported: " + node.id());
        }
        Map<String, Object> payload = new LinkedHashMap<>(node.config());
        payload.remove("from");
        payload.remove("options");
        payload.remove("experimental");
        try {
            return PipelineRepresentation.body(node.type(), payload, "graph.nodes." + node.id());
        } catch (TapstateException error) {
            Object reason = error.args().get("reason");
            throw new IllegalArgumentException(reason instanceof String text
                    ? text : "invalid graph transform: " + node.id(), error);
        }
    }

    private static List<SourceTableSelection> sourceTableSelections(PipelineDraft.Node node) {
        Object configured = node.config().get("tables");
        List<SourceTableSelection> selections = new ArrayList<>();
        if (configured instanceof List<?> values) {
            for (Object value : values) {
                if (value instanceof String table && !table.isBlank()) {
                    selections.add(new SourceTableSelection(table, isRegexReference(table)));
                } else if (value instanceof Map<?, ?> tableValue) {
                    String table = mapText(tableValue, "table");
                    if (table == null) {
                        throw new IllegalArgumentException("source tables require a non-blank table: " + node.id());
                    }
                    selections.add(new SourceTableSelection(table,
                            "regex".equals(tableValue.get("tableKind")) || isRegexReference(table)));
                } else {
                    throw new IllegalArgumentException("source tables must contain table names or objects: "
                            + node.id());
                }
            }
        } else if (configured != null) {
            throw new IllegalArgumentException("source tables must be a list: " + node.id());
        }
        if (selections.isEmpty()) {
            String table = requiredNodeText(node.table(), "source table", node.id());
            selections.add(new SourceTableSelection(table,
                    "regex".equals(node.config().get("tableKind")) || isRegexReference(table)));
        }
        return List.copyOf(selections);
    }

    private static boolean isRegexReference(String value) {
        return value != null && value.length() > 1 && value.startsWith("/") && value.endsWith("/");
    }

    private record SourceTableSelection(String table, boolean regex) {
        private String reference(String sourceId) {
            if (!regex) {
                return sourceId + "." + table;
            }
            String pattern = isRegexReference(table) ? table.substring(1, table.length() - 1) : table;
            return "/" + Pattern.quote(sourceId) + "\\." + pattern + "/";
        }

        private String renameKey() {
            return regex && !isRegexReference(table) ? "/" + table + "/" : table;
        }
    }

    private static String requiredNodeText(String value, String field, String nodeId) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("graph " + field + " must be non-blank: " + nodeId);
        }
        return value;
    }

    private static String optionalText(Map<String, Object> values, String... names) {
        for (String name : names) {
            Object value = values.get(name);
            if (value instanceof String text && !text.isBlank()) {
                return text;
            }
        }
        return null;
    }

    private static WriteMode writeMode(Map<String, Object> config, String nodeId) {
        String value = optionalText(config, "writeMode", "write_mode");
        if (value == null || "upsert".equals(value)) {
            return WriteMode.UPSERT;
        }
        if ("append".equals(value)) {
            return WriteMode.APPEND;
        }
        throw new IllegalArgumentException("unsupported target write mode: " + nodeId);
    }

    private static Metadata metadata(PipelineDraft draft) {
        return draft.description() == null || draft.description().isBlank()
                ? new Metadata(Map.of(), draft.name()) : new Metadata(Map.of(), draft.description());
    }
}
