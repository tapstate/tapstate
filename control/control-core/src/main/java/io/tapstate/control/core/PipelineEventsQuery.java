package io.tapstate.control.core;

import java.time.Instant;
import java.util.Objects;

/** One bounded event-history request; the query service validates its range and limit. */
public record PipelineEventsQuery(String pipelineId, Instant from, Instant to, int limit, String cursor) {

    public static final int DEFAULT_LIMIT = 100;
    public static final int MAX_LIMIT = 500;

    public PipelineEventsQuery {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
    }

    public PipelineEventsQuery(String pipelineId, Instant from, Instant to) {
        this(pipelineId, from, to, DEFAULT_LIMIT, null);
    }
}
