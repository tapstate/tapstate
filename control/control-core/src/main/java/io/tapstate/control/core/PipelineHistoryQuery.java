package io.tapstate.control.core;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** One request for a bounded pipeline history page. Validation belongs to the query service. */
public record PipelineHistoryQuery(String pipelineId, Instant from, Instant to,
        HistoryResolution resolution, int limit, List<String> tables, String cursor) {

    public static final int DEFAULT_LIMIT = 240;
    public static final int MAX_LIMIT = 1000;
    public static final int MAX_TABLES = 20;

    public PipelineHistoryQuery {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        resolution = resolution == null ? HistoryResolution.AUTO : resolution;
        tables = tables == null ? List.of() : List.copyOf(tables);
    }

    /** The ordinary first-page shape. */
    public PipelineHistoryQuery(String pipelineId, Instant from, Instant to) {
        this(pipelineId, from, to, HistoryResolution.AUTO, DEFAULT_LIMIT, List.of(), null);
    }
}
