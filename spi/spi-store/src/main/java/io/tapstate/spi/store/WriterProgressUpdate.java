package io.tapstate.spi.store;

import java.util.Objects;

/** One writer's progress within the original run passed to the store. */
public record WriterProgressUpdate(String table, String writerId, WriterProgress progress) {
    public WriterProgressUpdate {
        Objects.requireNonNull(progress, "progress");
    }
}
