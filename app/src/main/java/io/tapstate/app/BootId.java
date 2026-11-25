package io.tapstate.app;

import java.util.Objects;
import java.util.UUID;

/**
 * This start of the process: one id for the whole of its life, and a new one at every start. The member it runs
 * joins its cluster under it, and its store client is named for it, so that once the process has gone the members
 * left can tell which of what is still open at the store was this start's.
 */
record BootId(String value) {

    BootId {
        Objects.requireNonNull(value, "value");
    }

    /** An id no earlier start of any process had. */
    static BootId fresh() {
        return new BootId(UUID.randomUUID().toString());
    }
}
