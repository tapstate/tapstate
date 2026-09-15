package io.tapstate.runtime.engine.join;

import io.tapstate.core.event.Envelope;

import java.io.Serializable;
import java.util.Objects;

/** Internal routing identity, kept outside the SQL projection and the emitted row's schema. */
public record JoinUpdate(String factKey, Envelope event) implements Serializable {
    public JoinUpdate {
        Objects.requireNonNull(factKey, "factKey");
        Objects.requireNonNull(event, "event");
    }
}
