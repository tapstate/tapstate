package io.tapstate.control.core;

import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.NestRoot;
import io.tapstate.spi.store.PipelineDraft;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
        validateWizardIds(root, wizard.related());

        LinkedHashSet<String> sourceIds = new LinkedHashSet<>();
        sourceIds.add(root.sourceId());
        wizard.related().forEach(related -> sourceIds.add(related.sourceId()));

        LinkedHashMap<String, FromRef> aliases = new LinkedHashMap<>();
        List<Step> steps = new ArrayList<>();
        Map<String, String> outputAliases = new LinkedHashMap<>();
        aliases.put(root.id(), FromRef.literal(root.sourceId() + "." + root.table()));
        outputAliases.put(root.id(), compileBranch(root.id(), root.preTransforms(), aliases, steps));

        Map<String, List<PipelineDraft.Related>> children = new LinkedHashMap<>();
        for (PipelineDraft.Related related : wizard.related()) {
            if (!related.parentId().equals(root.id()) && !containsRelated(wizard.related(), related.parentId())) {
                throw new IllegalArgumentException("related table parent does not exist: " + related.parentId());
            }
            children.computeIfAbsent(related.parentId(), ignored -> new ArrayList<>()).add(related);
            aliases.put(related.id(), FromRef.literal(related.sourceId() + "." + related.table()));
            outputAliases.put(related.id(), compileBranch(related.id(), related.preTransforms(), aliases, steps));
        }

        List<Embed> embeds = embedsFor(root.id(), children, outputAliases);
        String nestId = draft.pipelineId() + "__nest";
        TransformBody.Nest nest = new TransformBody.Nest(null, null,
                new NestRoot(root.id(), root.key(), null, null, embeds));
        steps.add(new Step.Inline(nestId, FromClause.aliases(aliases), nest, Map.of(), Map.of()));

        String previous = nestId;
        for (PipelineDraft.Transform transform : wizard.transforms()) {
            Step step = compileTransform(transform, previous);
            steps.add(step);
            previous = step.id();
        }
        return new PipelineResource(draft.pipelineId(), metadata(draft),
                sourceIds.stream().map(id -> (SourceRef) SourceRef.bare(id)).toList(), steps, null, null, null, Map.of());
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

    private static String compileBranch(String owner, List<PipelineDraft.Transform> transforms,
            Map<String, FromRef> aliases, List<Step> steps) {
        String previous = owner;
        int index = 0;
        for (PipelineDraft.Transform transform : transforms) {
            String stepId = index++ == 0 ? owner + "__pre" : owner + "__pre__" + index + "__" + transform.id();
            Step step = compileTransform(new PipelineDraft.Transform(stepId, transform.type(), transform.fields()), previous);
            steps.add(step);
            aliases.put(stepId, FromRef.literal(stepId));
            previous = stepId;
        }
        return previous;
    }

    private static List<Embed> embedsFor(String parentId, Map<String, List<PipelineDraft.Related>> children,
            Map<String, String> outputAliases) {
        List<Embed> result = new ArrayList<>();
        for (PipelineDraft.Related related : children.getOrDefault(parentId, List.of())) {
            PipelineDraft.Relation relation = related.relation();
            if (relation.shape() == PipelineDraft.Shape.FLAT) {
                throw new IllegalArgumentException("flat wizard publication is not supported yet");
            }
            Map<String, String> on = new LinkedHashMap<>();
            relation.on().forEach(pair -> on.put(pair.childField(), pair.parentField()));
            List<String> arrayKey = relation.shape() == PipelineDraft.Shape.ARRAY ? relation.arrayKey() : null;
            result.add(new Embed(outputAliases.get(related.id()), on,
                    relation.shape() == PipelineDraft.Shape.ARRAY ? EmbedAs.ARRAY : EmbedAs.OBJECT,
                    relation.path(), arrayKey, null, null, embedsFor(related.id(), children, outputAliases)));
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
        return Step.inline(transform.id(), from, body, Map.of(), Map.of());
    }

    private static Map<String, FieldRule> mapFields(Map<String, Object> fields) {
        Map<String, FieldRule> result = new LinkedHashMap<>();
        fields.forEach((name, value) -> {
            if (value instanceof String string && string.startsWith("$")) {
                result.put(name, FieldRule.rename(string.substring(1)));
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

    private PipelineResource compileGraph(PipelineDraft draft) {
        PipelineDraft.Graph graph = Objects.requireNonNull(draft.graph(), "graph payload");
        LinkedHashSet<String> sourceIds = new LinkedHashSet<>();
        Map<String, PipelineDraft.Node> nodes = new LinkedHashMap<>();
        for (PipelineDraft.Node node : graph.nodes()) {
            if (nodes.put(node.id(), node) != null) {
                throw new IllegalArgumentException("duplicate graph node: " + node.id());
            }
            if (node.sourceId() != null && !node.sourceId().isBlank()) {
                sourceIds.add(node.sourceId());
            }
        }
        Map<String, List<String>> inputs = new HashMap<>();
        graph.edges().forEach(edge -> inputs.computeIfAbsent(edge.target(), ignored -> new ArrayList<>()).add(edge.source()));
        List<Step> steps = new ArrayList<>();
        for (PipelineDraft.Node node : graph.nodes()) {
            if ("source".equals(node.type())) {
                continue;
            }
            List<String> refs = inputs.getOrDefault(node.id(), List.of());
            if (refs.isEmpty() && node.table() != null) {
                refs = List.of(node.table());
            }
            if (refs.isEmpty()) {
                continue;
            }
            TransformBody body = switch (node.type()) {
                case "map" -> new TransformBody.MapProjection(mapFields(node.config()));
                case "filter" -> new TransformBody.Filter(requiredText(node.config(), "expr"));
                default -> throw new IllegalArgumentException("unsupported graph node: " + node.type());
            };
            steps.add(Step.inline(node.id(), FromClause.list(refs.stream().map(FromRef::literal).toArray(FromRef[]::new)),
                    body, Map.of(), Map.of()));
        }
        return new PipelineResource(draft.pipelineId(), metadata(draft),
                sourceIds.stream().map(id -> (SourceRef) SourceRef.bare(id)).toList(),
                steps, null, null, null, Map.of());
    }

    private static Metadata metadata(PipelineDraft draft) {
        return draft.description() == null || draft.description().isBlank()
                ? new Metadata(Map.of(), draft.name()) : new Metadata(Map.of(), draft.description());
    }
}
