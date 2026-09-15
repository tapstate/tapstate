package io.tapstate.runtime.engine.join;

import io.tapstate.core.event.Envelope;

/**
 * Where a join puts the changelog it produces.
 *
 * <p>It answers whether it took the change, and a caller that is refused must offer the same change
 * again before any later one. A recompute can be a million rows: emitting them all before returning
 * would hold a processor's thread for as long as the whole fan-out takes, and everything else on that
 * thread - the substrate's own bookkeeping included - waits behind it.
 */
@FunctionalInterface
public interface JoinSink {

    /** Takes {@code change}, or answers false when it could not and must be offered again. */
    boolean offer(Envelope change);
}
