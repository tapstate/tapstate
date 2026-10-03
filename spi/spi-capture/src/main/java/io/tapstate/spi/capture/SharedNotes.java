package io.tapstate.spi.capture;

import io.tapstate.core.model.PipelineNode;

import java.util.List;
import java.util.Objects;

/**
 * Notes a capture connector keeps for a change stream several pipelines share, rather than for one pipeline
 * node: filed under {@code sharedBy}, the physical chain the stream reads, so whichever pipeline happens to
 * open the stream finds what the last one left.
 *
 * <p>A connector records what it must recognise again later -- above all the replication slot it created on
 * the source. Filed under the pipeline that opened the stream, those notes are empty the next time a different
 * pipeline opens it: the connector reads that as a first run and creates a fresh slot beginning where the
 * source is now, every change in between is gone, and the old slot holds the source's log for good. Filed
 * under the chain, the stream is the same stream whoever opens it.
 *
 * <p>{@code carriedFrom} names the nodes that kept such notes before they were shared, in the order they are
 * asked. A note the shared notes do not hold yet is looked for there and carried over the first time it is
 * found, so a stream that used to be one pipeline's goes on with what that pipeline kept. Nothing is copied
 * wholesale: the notes cannot be listed, and a note nobody asks for is one nobody needs.
 */
public record SharedNotes(String sharedBy, List<PipelineNode> carriedFrom) {

    public SharedNotes {
        Objects.requireNonNull(sharedBy, "sharedBy");
        if (sharedBy.isBlank()) {
            throw new IllegalArgumentException("shared notes belong to a named chain");
        }
        carriedFrom = carriedFrom == null ? List.of() : List.copyOf(carriedFrom);
    }
}
