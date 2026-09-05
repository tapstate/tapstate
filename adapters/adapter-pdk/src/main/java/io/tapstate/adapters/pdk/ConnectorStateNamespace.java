package io.tapstate.adapters.pdk;

import io.tapstate.core.model.PipelineNode;

/**
 * Where a connector's notes are filed: one namespace per pipeline node for what it keeps at that node,
 * and one namespace for the whole deployment for what the contract calls global.
 *
 * <p>Both sides of a pipeline name it here rather than each spelling it themselves. The read side and
 * the write side reach this from different call paths and a node's notes are found only under the name
 * they were written under, so two spellings of one node read as an empty notepad — indistinguishable
 * from a first run, which is the failure this exists to make impossible.
 *
 * <p>The two ids are joined on a dot, the same way a table is addressed by the source that reads it.
 * No two nodes can name one namespace: a pipeline id carries no dot of its own, so the first dot after
 * the prefix is always the separator and everything after it is the node's id, whatever that contains.
 * The guarantee is the pipeline id's, not the node id's — a scheme relying on both would be resting on
 * something nothing states.
 */
final class ConnectorStateNamespace {

    private static final String PREFIX = "pdk.state.";

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
    static String of(PipelineNode node) {
        return node == null ? null : PREFIX + node.pipelineId() + "." + node.nodeId();
    }
}
