package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;

import java.util.Objects;

/**
 * A chain's source read offset as it stands durably, and whether it is the checkpoint of a capture whose
 * rings write through to the recoverable change log.
 *
 * <p>{@code writtenThrough} is the record's {@code sourceReadDurable}. True: the offset was written once every
 * change before it was in the recoverable log, so a restart resumes the capture there and each consumer
 * replays the rest from the log. False: the offset is bounded by what the consumers have confirmed instead --
 * a direct channel, or a chain whose ring is not shared -- and a restart resumes from it all the same.
 */
public record DurableSourceRead(ChainPosition position, boolean writtenThrough) {

    public DurableSourceRead {
        Objects.requireNonNull(position, "position");
    }
}
