package io.tapstate.spi.store;

/**
 * Durable bounds of one table's change log. The largest written sequence survives trimming, so a rebuilt
 * ring never reuses a sequence. Everything at or below {@code trimmedThrough} has been retired; an initial
 * log has both values at {@code -1}. These bounds do not prove that every retained sequence is present:
 * readers check the exact keys returned by the log before advancing over them.
 */
public record SrsLogBounds(long largestSequence, long trimmedThrough) {

    public SrsLogBounds {
        if (largestSequence < -1 || trimmedThrough < -1 || trimmedThrough > largestSequence) {
            throw new IllegalArgumentException("SRS log bounds must satisfy -1 <= trimmedThrough <= largestSequence");
        }
    }
}
