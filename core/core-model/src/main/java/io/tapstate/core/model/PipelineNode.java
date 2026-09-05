package io.tapstate.core.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Which node of which pipeline a connector was opened for: the identity that scopes whatever that
 * connector keeps for itself between one drive and the next.
 *
 * <p>A connector records things it must recognise again later - the replication slot it created, how
 * far it has checkpointed, an identity it minted on its first run. Those notes belong to the node that
 * opened the connector, not to the open: a pipeline that first loads a table in full and then follows
 * its changes opens the same source twice and both drives are the same node. Two pipelines reading the
 * same database through the same connection settings are two nodes, and what one holds is not the
 * other's - a physical resource such as a replication slot admits one live consumer, so handing both
 * pipelines one set of notes hands both the same slot.
 *
 * <p>Deliberately not the identity a shared change stream is keyed by. That one is the physical source
 * coordinate, and merging two pipelines onto one mined stream is exactly what it is for; this one
 * separates them. The two are derived apart and neither is computed from the other.
 *
 * <p>Absent - a null reference rather than a node with blank ids - is a real answer, and it is what the
 * read-only drives use: a schema discovery, a connection test and a data browse each live for one call,
 * so there is no second drive for anything they wrote to be read back by, and no node they could
 * honestly name. An immutable value; both ids are required and neither may be blank.
 *
 * <p>Serializable so the write side's identity travels with the sink factory the engine ships onto the
 * DAG — the sink opens its connector on whichever member runs the vertex, and a node that did not make
 * the trip would leave that connector filing its notes nowhere.
 */
public record PipelineNode(String pipelineId, String nodeId) implements Serializable {

    public PipelineNode {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(nodeId, "nodeId");
        if (pipelineId.isBlank()) {
            throw new IllegalArgumentException("pipelineId must be non-blank");
        }
        if (nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must be non-blank");
        }
    }
}
