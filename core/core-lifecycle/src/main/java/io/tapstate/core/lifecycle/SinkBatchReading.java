package io.tapstate.core.lifecycle;

import java.time.Instant;

/**
 * Measured sink batch work from one live job. Issued counts describe handoff to a writer, not durable
 * delivery; pending counts batches whose writer confirmation has not finished processing at the sink.
 * The distinct confirmed record count remains the delivery reading.
 */
public record SinkBatchReading(long issuedBatches, long issuedRecords, long largestBatch,
        long pendingBatches, long inFlightLimit, Long backpressuredSinks,
        HistogramValue writeDuration, HistogramValue backpressureDuration, Instant countingSince) {

    public static final SinkBatchReading NONE =
            new SinkBatchReading(0, 0, 0, 0, 0, null, null, null, null);

    public SinkBatchReading {
        if (countingSince == null && (issuedBatches != 0 || issuedRecords != 0 || largestBatch != 0
                || pendingBatches != 0 || inFlightLimit != 0 || backpressuredSinks != null
                || writeDuration != null || backpressureDuration != null)) {
            throw new IllegalArgumentException("sink batch readings require their counting start");
        }
        if (countingSince != null && issuedBatches == 0) {
            throw new IllegalArgumentException("a counted sink batch reading requires an issued batch");
        }
        if (issuedBatches < 0 || issuedRecords < 0 || largestBatch < 0 || pendingBatches < 0
                || inFlightLimit < 0 || (backpressuredSinks != null && backpressuredSinks < 0)) {
            throw new IllegalArgumentException("sink batch readings cannot be negative");
        }
    }

    public boolean isEmpty() {
        return countingSince == null;
    }
}
