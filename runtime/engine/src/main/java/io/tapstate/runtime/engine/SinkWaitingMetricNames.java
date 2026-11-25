package io.tapstate.runtime.engine;

/**
 * What a sink writer's readings of what is waiting in it are called among a run's statistics, and how to read one
 * back: the rows taken in and not yet handed to the writer, by the stream they came on, and the rows handed to it
 * whose writes have not settled, by the table they go to. Naming and parsing sit together because they are one
 * contract with two ends, and a reader parsing one spelling while the sink writes another reports every writer
 * as having nothing waiting.
 *
 * <p>The stream or table goes last in the name, because either may hold a dot and nothing can follow it without
 * the split becoming a guess.
 */
public final class SinkWaitingMetricNames {

    /** What a stream's count of rows waiting to be handed to the writer is named, before the stream. */
    public static final String QUEUED_PREFIX = "sinkQueued.";

    /** What a table's count of rows handed to the writer and not settled is named, before the table. */
    public static final String IN_FLIGHT_PREFIX = "sinkInFlight.";

    private SinkWaitingMetricNames() {
    }

    /** The name the count of rows of {@code stream} waiting to be handed to the writer is left under. */
    public static String queuedNameOf(String stream) {
        return QUEUED_PREFIX + stream;
    }

    /** The name the count of rows for {@code table} handed to the writer and not settled is left under. */
    public static String inFlightNameOf(String table) {
        return IN_FLIGHT_PREFIX + table;
    }

    /** The stream a waiting count named {@code metric} concerns, or {@code null} when it is not one. */
    public static String streamOfQueued(String metric) {
        return metric.startsWith(QUEUED_PREFIX) ? metric.substring(QUEUED_PREFIX.length()) : null;
    }

    /** The table an in-flight count named {@code metric} concerns, or {@code null} when it is not one. */
    public static String tableOfInFlight(String metric) {
        return metric.startsWith(IN_FLIGHT_PREFIX) ? metric.substring(IN_FLIGHT_PREFIX.length()) : null;
    }
}
