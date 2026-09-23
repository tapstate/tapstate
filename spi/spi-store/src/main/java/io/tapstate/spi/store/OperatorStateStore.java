package io.tapstate.spi.store;

import java.util.Objects;

/** The two durable stores one stateful operator routes as a unit. */
public record OperatorStateStore(KeyedStateStore state, NestDeadLetterStore deadLetters) {

    public OperatorStateStore {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(deadLetters, "deadLetters");
    }
}
