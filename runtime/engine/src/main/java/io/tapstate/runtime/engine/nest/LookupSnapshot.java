package io.tapstate.runtime.engine.nest;

import java.io.Serializable;
import java.util.Map;

/** The input edge and source generations that must cross a lookup before absence is known. */
public record LookupSnapshot(int ordinal, Map<String, Long> epochs) implements Serializable {

    public LookupSnapshot {
        epochs = Map.copyOf(epochs);
        if (ordinal < 0 || epochs.isEmpty() || epochs.values().stream().anyMatch(epoch -> epoch <= 0)) {
            throw new IllegalArgumentException("a lookup snapshot needs an input edge and positive source generations");
        }
    }
}
