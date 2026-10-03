package io.tapstate.runtime.srs;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.LongConsumer;

/**
 * Resolves, on the member it runs on, the sink a ring reader reports its read cursor to. It exists because
 * the reader runs inside the {@link SrsRingSource self-built Jet source}, which is serialized on job submit,
 * yet the durable coordination store the cursor is published to is not serializable and lives on the member.
 * So this factory — holding only serializable coordinates — is carried onto the source and, once on the
 * member, resolves the actual store (from the member's user context) and binds the per-consumer,
 * per-table publish sink.
 *
 * <p>A member that has no store bound yet resolves to a no-op sink, so a source still runs before the
 * assembly layer populates the member's user context.
 */
@FunctionalInterface
public interface SrsReadCursorPublisherFactory extends Serializable {

    /** A factory that resolves to a sink reporting nothing — the default when no cursor wiring is bound. */
    SrsReadCursorPublisherFactory NONE = member -> lastReadSeq -> { };

    /**
     * Resolves the read-cursor sink on {@code member}: the {@link LongConsumer} the reader calls with the
     * last sequence it read, once per non-empty fill.
     */
    LongConsumer resolve(HazelcastInstance member);

    /**
     * This table's safely processed position, distinct from the read cursor published by {@link #resolve}.
     * A first-arrival ring marker is not a confirmation and is never returned here. Older factories that
     * only publish read progress need no confirmation lookup.
     */
    default Optional<ChainPosition> confirmedPosition(HazelcastInstance member) {
        return Optional.empty();
    }

    /** The table's confirmed cursor or explicit arrival marker, excluding mere read progress. */
    default java.util.OptionalLong recoverySequence(HazelcastInstance member) {
        return java.util.OptionalLong.empty();
    }
}
