package io.tapstate.runtime.engine;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * One table's initial load, as a source holding its changes back waits for it: landed at every writer of one
 * sink.
 *
 * <p>{@code sink} is the sink vertex the writers belong to, {@code table} the table whose load has to have
 * landed, and {@code writers} every writer of that sink, named as {@link SinkProcessor#writerId} names them.
 * The writers are the sink's own and not every writer the table reaches: what a change could overtake is a
 * load row still being written by a writer of the sink the change is about to reach.
 */
public record AwaitedLoad(String sink, String table, List<String> writers) implements Serializable {

    public AwaitedLoad {
        Objects.requireNonNull(sink, "sink");
        Objects.requireNonNull(table, "table");
        writers = List.copyOf(Objects.requireNonNull(writers, "writers"));
        if (writers.isEmpty()) {
            throw new IllegalArgumentException("a load is awaited at the writers of a sink, and sink '" + sink
                    + "' was given none");
        }
    }
}
