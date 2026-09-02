package io.tapstate.runtime.engine.nest;

import io.tapstate.core.common.TapstateException;

import java.util.Map;

/**
 * The limits a nest refuses on rather than absorbs, kept together so that what an operator is told reads
 * the same wherever it is reached.
 *
 * <p>One place rather than one each because levels that refuse for the same reason would drift into saying
 * so differently. A resolver key holds the rows beneath a parent it cannot name yet; a document holds what
 * it absorbed under a root that has not come and what is parked for an ancestor. What an operator sees when
 * either fills up is the same news - this key is stuck and this much is stuck behind it - and it is worth
 * being one sentence rather than two that drifted apart.
 *
 * <p><b>The two here are not the same kind of limit, and keeping them side by side is what makes that
 * legible.</b> What one key holds is a quantity that has to fit somewhere; how many rows point at one row
 * fits perfectly well and is refused anyway, because what it measures is how much work one edit makes
 * rather than how much space anything takes.
 */
final class NestLimits {

    private NestLimits() {
    }

    /**
     * Fails the job when {@code pending} is past {@code limit}, naming where and how much. The limit is what
     * may be held, so reaching it exactly is allowed and the count that goes past it is refused.
     */
    static void refuse(NestVertex vertex, Object key, long pending, long limit) {
        if (pending <= limit) {
            return;
        }
        throw new TapstateException(NestError.PENDING_LIMIT_EXCEEDED,
                Map.of("namespace", vertex.mapName(), "key", String.valueOf(key),
                        "pending", pending, "limit", limit), null);
    }

    /**
     * Fails the job when more than {@code limit} rows point at the row {@code key} of {@code lookup},
     * naming which row and how many. Like the limit above, the number given is what is allowed, so a row
     * sitting exactly on it goes on running.
     *
     * <p>Weighed on an edit to the row being pointed at rather than as each row registers, which is where
     * the count is free: the caller has every bucket in hand there and is about to walk every identity in
     * them anyway. Per registration it would cost a read of every bucket on each arrival of the pointing
     * stream, to refuse the same thing a step earlier.
     */
    static void refuseFanout(NestLookup lookup, Object key, long referrers, long limit) {
        if (referrers <= limit) {
            return;
        }
        throw new TapstateException(NestError.REFERENCE_FANOUT_LIMIT_EXCEEDED,
                Map.of("refPath", NestTopology.render(lookup.pathId()), "identity", String.valueOf(key),
                        "referrers", referrers, "limit", limit), null);
    }
}
