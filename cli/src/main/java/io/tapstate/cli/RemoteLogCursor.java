package io.tapstate.cli;

import java.util.Objects;

/** Opaque server cursor for resuming a node-local pipeline log stream. */
record RemoteLogCursor(String generation, long sequence) {

    RemoteLogCursor {
        Objects.requireNonNull(generation, "generation");
    }

    String token() {
        return generation + ":" + sequence;
    }
}
