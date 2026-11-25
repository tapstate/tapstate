package io.tapstate.core.event;

import java.io.Serializable;

/** A source checks a candidate move before recording its recovery interlock or emitting the batch. */
public interface SourceKeyMovePreflight {
    void withKeyMovePreflight(Check check);

    @FunctionalInterface
    interface Check extends Serializable {
        void validate(Envelope row);
    }
}
