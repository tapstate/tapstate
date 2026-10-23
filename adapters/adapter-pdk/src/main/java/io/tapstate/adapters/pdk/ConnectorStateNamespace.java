package io.tapstate.adapters.pdk;

import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.store.RequiredNoteOwner;
import java.util.Objects;

/**
 * Where a connector's notes are filed: one namespace per pipeline node for what it keeps at that node,
 * and one namespace for the whole deployment for what the contract calls global.
 *
 * <p>Both sides of a pipeline name it here rather than each spelling it themselves. The read side and
 * the write side reach this from different call paths and a node's notes are found only under the name
 * they were written under, so two spellings of one node read as an empty notepad — indistinguishable
 * from a first run, which is the failure this exists to make impossible.
 *
 * <p>The physical address contains the pipeline id, fixed connector purpose and logical node id.
 * Pipeline ids have no dot and purpose is one fixed segment, so a dotted node id cannot erase either
 * boundary. Source, view and sync nodes may legally share a logical id and still name separate notes.
 */
public final class ConnectorStateNamespace {

    private static final String PREFIX = "pdk.state.";
    private static final String SHARED_PREFIX = "pdk.chain.";

    /**
     * The one namespace every connector in the deployment shares, whatever pipeline or node it was
     * opened for. What is written here is a fact about the installation rather than about a run, so it
     * belongs to no pipeline and no pipeline being taken down may take it along.
     *
     * <p><b>It is deliberately not under {@link #PREFIX}.</b> Nothing today drops a namespace by
     * prefix — the one bulk operation names a namespace whole — but a name that sat under the per-node
     * prefix would make this map's survival a property of how the cleanup happens to be written, and
     * the first prefix sweep anyone adds would silently empty it for every pipeline at once. Outside
     * the prefix, no such sweep can reach it however it is written.
     */
    static final String GLOBAL = "pdk.global-state";

    private ConnectorStateNamespace() {
    }

    /** The namespace {@code node}'s connector keeps its own notes in, or null for a drive naming no node. */
    public static String of(PipelineNode node) {
        return of(node, PipelineNode.Purpose.CAPTURE);
    }

    /** A fixed connector purpose distinguishes legal same-name source, view and sync nodes. */
    public static String of(PipelineNode node, PipelineNode.Purpose purpose) {
        return RequiredNoteOwner.namespace(node, purpose);
    }

    /** A physical capture's notes survive changes to the pipeline node that owns its subscription. */
    public static String ofShared(String chainId) {
        return SHARED_PREFIX + Objects.requireNonNull(chainId, "chainId");
    }

    /** Markers that prevent earlier private notes from repopulating a shared notes namespace. */
    public static String migrationOf(String namespace) {
        return "pdk.notes-migration." + Objects.requireNonNull(namespace, "namespace");
    }
}
