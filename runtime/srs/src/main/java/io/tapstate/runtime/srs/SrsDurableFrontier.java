package io.tapstate.runtime.srs;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.spi.store.ConsumerOffset;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

/**
 * The durable-frontier bound on a source-read-offset advance. The in-memory change ring is volatile —
 * after a restart it is gone and the only durable landing for a change is the idempotent sink. So the
 * persisted source read offset must never pass the slowest consumer's sink-acked source position: every
 * change ahead of the offset must stay re-minable from the source, or a change that was only ever in the
 * ring and had not yet reached a sink would be lost.
 *
 * <p>This resolves the safe offset to persist — the reader's candidate clamped so it never passes the
 * minimum sink-acked position across all consumers. It is empty (advance nothing) when no consumer holds
 * the data durably yet: when there are no consumers, or when any consumer has acked nothing (its frontier
 * sits below the origin and pins the whole advance).
 *
 * <p>Positions are ranked by the order the engine assigned as it read them, never by the token. A token is
 * a connector value this never parses, with only equality defined on it, so ranking by it needed an order
 * no connector supplies. The order is that same sequence observed rather than computed: the ring assigns
 * it on append and the reader reads in it. What gets written down is still the token — the half a
 * connector understands and a read resumes from.
 */
public final class SrsDurableFrontier {

    private SrsDurableFrontier() {
    }

    /**
     * The source read offset that may be durably persisted: {@code candidate} clamped to not pass the
     * slowest consumer's sink-acked position, or empty when no advance is safe (no consumers, or a
     * consumer has acked nothing). {@code candidate} is the position the reader has read up to.
     *
     * <p>Empty also covers a winning position that carries no token of its own — a frontier that has
     * reached snapshot rows and no change yet. There is nothing to write down for it, and leaving the
     * offset where it was only ever means re-mining changes that were already read.
     */
    public static Optional<String> safeAdvance(
            ChainPosition candidate, Collection<ConsumerOffset> consumers) {
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(consumers, "consumers");
        if (consumers.isEmpty()) {
            return Optional.empty();
        }
        // The answer is the lowest of the reader's own position and every consumer's, so the reader's is
        // where the search starts rather than a separate clamp afterwards - one comparison, and no step
        // where the running lowest is still nothing.
        ChainPosition safe = candidate;
        for (ConsumerOffset consumer : consumers) {
            ChainPosition acked = consumer.sinkAcked();
            if (acked == null) {
                return Optional.empty();
            }
            if (acked.order().compareTo(safe.order()) < 0) {
                safe = acked;
            }
        }
        return Optional.ofNullable(safe.token());
    }
}
