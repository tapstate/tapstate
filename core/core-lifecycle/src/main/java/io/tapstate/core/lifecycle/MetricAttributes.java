package io.tapstate.core.lifecycle;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The attribute keys a metric point may be broken down by, and the closed value sets three of them are
 * held to. One place spells them so that a producer, a projection and an export all mean the same key by
 * the same name; a key typed out at each site drifts the day one of them is retyped.
 *
 * <p>Three keys are closed sets, and a point carrying a value outside its set is refused where the fact
 * is built ({@link MetricFact}). An open set would be one the data could add a value to: a symbol nobody
 * recognised would become a series of its own, named by whatever produced it, and the number of series a
 * metric holds would be decided by the data rather than by anybody. The sets are small on purpose and grow
 * only by a decision somebody writes down.
 *
 * <ul>
 *   <li>{@link #DIRECTION} — which end of the pipeline the row crossed: {@code in} at the source
 *       adapter's handover, {@code out} at the target's confirmation.</li>
 *   <li>{@link #OP} — what the <em>source</em> did to the row, never what the target did with it. A
 *       snapshot read keeps its own name because at the source nothing was inserted; {@code other} is for a
 *       kind nothing recognises, and for nothing else.</li>
 *   <li>{@link #STAGE} — where in the graph a duration was spent: one value per family of processor the
 *       engine draws a vertex with, see {@link Stage}.</li>
 * </ul>
 *
 * <p>Three more keys name a thing the pipeline's own definition draws: the chain a frontier reading is
 * about, the namespace a nest keeps its state under, and the namespace a join rebuilds a dimension in.
 * Like the table, each is a name the definition gives rather than a stable identity the product mints —
 * rename the thing and its series ends where a new one begins — and, like the table, how many of them
 * there are is decided by the definition and not by the rows that flow through it. What a point may
 * never carry is a value read out of a row: the key a rebuild is about, say, would be one, and it stays
 * out of the attributes for that reason.
 *
 * <p>{@link #OVERFLOW} is not a dimension. It marks the one series per remaining combination that holds
 * what a cardinality budget would not name individually, under the key every OpenTelemetry consumer
 * already knows for exactly that series.
 */
public final class MetricAttributes {

    public static final String PIPELINE_ID = "tapstate.pipeline.id";
    public static final String TABLE_ID = "tapstate.table.id";
    /** The chain — one source's change log — a frontier reading is about. */
    public static final String CHAIN_ID = "tapstate.chain.id";
    /** The namespace a nest step keeps one level of its state under. */
    public static final String NEST_NAMESPACE = "tapstate.nest.namespace";
    /** The namespace a join step mirrors one dimension in. */
    public static final String JOIN_NAMESPACE = "tapstate.join.namespace";
    public static final String DIRECTION = "direction";
    public static final String OP = "op";
    public static final String CODE = "code";
    public static final String STAGE = "stage";
    /** The fixed telemetry sink whose local worker health a process instrument describes. */
    public static final String TELEMETRY_SINK = "sink";
    /** The marker OpenTelemetry puts on the series that absorbs what a cardinality limit turned away. */
    public static final String OVERFLOW = "otel.metric.overflow";

    public static final Set<String> DIRECTIONS = Set.of("in", "out");
    public static final Set<String> OPS = Set.of("insert", "update", "delete", "read", "ddl", "other");
    public static final Set<String> TELEMETRY_SINKS = Set.of("latest", "history", "export");

    private static final Map<String, Set<String>> CLOSED = Map.of(
            DIRECTION, DIRECTIONS,
            OP, OPS,
            STAGE, Stage.attributeValues(),
            TELEMETRY_SINK, TELEMETRY_SINKS);

    private MetricAttributes() {
    }

    /** The values {@code key} is held to, or empty for a key whose values the data may name freely. */
    public static Optional<Set<String>> closedDomain(String key) {
        return Optional.ofNullable(CLOSED.get(key));
    }
}
