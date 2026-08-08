package io.tapstate.runtime.srs;

import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.SourceOrder;
import java.io.Serializable;

/**
 * What a source's read progress is stamped as, so that a source can announce how far the frontier may go
 * without knowing how a bound is encoded or which axis its stream travels on. A change ring knows a
 * generation and a sequence; which axis that stream was numbered onto, and how the pair packs into the one
 * long a bound travels as, are properties of the whole job and are settled by whoever wired it.
 *
 * <p>Supplied per source vertex, and only when a job propagates a frontier at all: a source built without
 * one announces nothing, which is a frontier that does not advance rather than one that advances too far.
 */
@FunctionalInterface
public interface SourceBoundStamp extends Serializable {

    /** The bound standing for {@code read}, on the axis this source's stream was numbered onto. */
    Watermark boundFor(SourceOrder read);
}
