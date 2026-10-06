package io.tapstate.control.core;

import io.tapstate.spi.store.PipelineDraft;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Carries a change made to a pipeline's definition into its draft, where the draft says the same thing.
 *
 * <p>A draft names its write elements the way publishing names them -- a graph's target node by its id, a
 * view node by the view id it carries or its own, a wizard's output by the sync id it names or its
 * destination and table -- so the element a change names is found by the same names the definition was
 * published under. A change the draft cannot say, because it names an element the draft does not have or a
 * field the draft does not carry, leaves the whole draft as it was: half a change carried over would be a
 * draft that publishes something nobody chose.
 */
final class PipelineDraftEdits {

    private PipelineDraftEdits() {
    }

    /**
     * {@code draft} with every change applied, its base moved from {@code fromHash} to {@code toHash} and its
     * revision advanced; {@code draft} itself when any change cannot be carried. A draft whose published
     * revision is the one changed here stays marked as published, now at the new definition.
     */
    static PipelineDraft carry(PipelineDraft draft, List<StartAction.Change> changes, String fromHash,
            String toHash, Instant at, String principal) {
        Objects.requireNonNull(draft, "draft");
        PipelineDraft.Graph graph = draft.graph();
        PipelineDraft.Wizard wizard = draft.wizard();
        for (StartAction.Change change : changes) {
            if (!"on_full_load".equals(change.field())) {
                return draft;
            }
            if (graph != null) {
                PipelineDraft.Graph changed = onFullLoad(graph, change.element(), change.to());
                if (changed == null) {
                    return draft;
                }
                graph = changed;
            } else {
                PipelineDraft.Wizard changed = onFullLoad(wizard, change.element(), change.to());
                if (changed == null) {
                    return draft;
                }
                wizard = changed;
            }
        }
        long revision = draft.revision() + 1;
        boolean publishedHere = Objects.equals(draft.publishedDraftRevision(), draft.revision())
                && Objects.equals(draft.publishedArtifactHash(), fromHash);
        return new PipelineDraft(draft.pipelineId(), draft.schemaVersion(), revision, draft.mode(), draft.name(),
                draft.description(), graph, wizard, toHash,
                publishedHere ? Long.valueOf(revision) : draft.publishedDraftRevision(),
                publishedHere ? toHash : draft.publishedArtifactHash(),
                draft.createdAt(), at, principal);
    }

    /** The graph with the policy set on the target or view node publishing {@code element}; null if none does. */
    private static PipelineDraft.Graph onFullLoad(PipelineDraft.Graph graph, String element, String policy) {
        List<PipelineDraft.Node> nodes = new ArrayList<>();
        boolean found = false;
        for (PipelineDraft.Node node : graph.nodes()) {
            boolean publishes = "target".equals(node.type()) && node.id().equals(element)
                    || "view".equals(node.type()) && PipelineDraftCompiler.graphViewId(node).equals(element);
            if (publishes) {
                found = true;
                nodes.add(new PipelineDraft.Node(node.id(), node.type(), node.sourceId(), node.table(),
                        with(node.config(), policy), node.metadata()));
            } else {
                nodes.add(node);
            }
        }
        return found ? new PipelineDraft.Graph(nodes, graph.edges(), graph.viewport()) : null;
    }

    /** The wizard with the policy set on its output when the output publishes {@code element}; null if not. */
    private static PipelineDraft.Wizard onFullLoad(PipelineDraft.Wizard wizard, String element, String policy) {
        if (wizard == null || wizard.output() == null
                || !element.equals(PipelineDraftCompiler.wizardSyncId(wizard.output().config()))) {
            return null;
        }
        return new PipelineDraft.Wizard(wizard.root(), wizard.related(), wizard.transforms(),
                new PipelineDraft.Output(wizard.output().kind(), with(wizard.output().config(), policy)));
    }

    private static Map<String, Object> with(Map<String, Object> config, String policy) {
        Map<String, Object> changed = new LinkedHashMap<>(config == null ? Map.of() : config);
        changed.remove("on_full_load");
        changed.put("onFullLoad", policy);
        return changed;
    }
}
