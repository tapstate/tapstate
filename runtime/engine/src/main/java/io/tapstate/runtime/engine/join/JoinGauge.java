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

    /**
     * Marks how far the rebuild of one dimension key's fan-out has got: {@code rowsDone} rows sent, of
     * about {@code rowsExpected}.
     *
     * <p><b>This does not make a large recompute faster, and it is not meant to.</b> A dimension key
     * with a million fact rows under it takes as long as writing a million rows takes; what this changes
     * is that the wait stops being invisible. Throughout it the job is running, the error count is zero
     * and the target table holds half the old value and half the new one - a state that reads exactly
     * like a healthy steady one. An operator with this number can tell "wait a minute" from "something
     * is wrong", and decide whether to hold off a downstream read.
     *
     * <p>Reported as the rebuild goes rather than when it ends: a number that arrives afterwards
     * describes a situation that is over. {@code rowsExpected} is an estimate read off the index, not a
     * count - counting exactly would mean walking every page before walking them again to rebuild.
     *
     * <p>Called for every recompute, however small. <b>Deciding which are worth surfacing belongs to
     * whoever is reading</b>, not here: a threshold hard-coded at this end would be one every carrier
     * had to reproduce, and the reason for one - that a report on every dimension edit is noise, and
     * noise is how a number like this comes to be ignored - is a reporting concern rather than a
     * mechanical one. The default does nothing, so a gauge that only wants the bucket size is unchanged.
     */
    default void recomputing(String source, String dimensionKey, long rowsDone, long rowsExpected) {
    }
}
