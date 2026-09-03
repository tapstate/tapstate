package io.tapstate.runtime.engine.join;

/**
 * Where a join reports the one number that makes its worst shapes visible before they hurt.
 *
 * <p><b>The widest bucket is that number.</b> Everything a reverse index can go wrong at is invisible
 * in queue depth and throughput: a bucket approaching the size a stored entry may be, an initial load
 * whose cost is quietly quadratic in what one dimension key holds, a fan-out large enough that
 * recomputing it takes minutes. None of those shows up as a stalled queue or a slow rate until it is
 * already happening; the fan-out under one dimension key is what says it is coming.
 *
 * <p>Kept as the deepest ever reported rather than the last, because the last is whichever key happened
 * to be walked most recently and says nothing about the one that is large.
 *
 * <p>Reported by whoever already worked the number out - the walk that reads the bucket - so that a
 * second place cannot arrive at a different one. Counting it anywhere else would mean reading a bucket
 * to measure it, which is the cost this exists to warn about.
 */
@FunctionalInterface
public interface JoinGauge {

    /** One reported so nothing has to be configured before a join runs at all. */
    JoinGauge NONE = (source, dimensionKey, pages) -> {
    };

    /** Marks that one dimension key of {@code source} was found to hold {@code pages} pages of facts. */
    void bucketWalked(String source, String dimensionKey, int pages);
}
