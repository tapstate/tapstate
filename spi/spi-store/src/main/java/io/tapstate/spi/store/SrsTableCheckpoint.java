package io.tapstate.spi.store;

import java.util.Objects;

/** One table's exact model and last persisted ring sequence, including a pre-first-event baseline. */
public record SrsTableCheckpoint(SrsModelKey model, long sequence) {
    public SrsTableCheckpoint {
        Objects.requireNonNull(model, "model");
        if (sequence < -1 || sequence == Long.MAX_VALUE) throw new IllegalArgumentException("invalid table checkpoint sequence");
    }
}
