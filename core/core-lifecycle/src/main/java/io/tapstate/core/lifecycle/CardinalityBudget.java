package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * How many series each instrument may hold, and what becomes of the ones past that number.
 *
 * <p>Every dimensioned instrument multiplies: rows are counted per pipeline, per table, per direction and
 * per operation, and the table factor is as wide as the pipeline's own definition makes it. A metric whose
 * series count is decided by the data is a metric whose memory, storage and export cost are decided by the
 * data, and the SDK that will eventually export these caps an instrument at two thousand series and folds
 * the rest into one — silently, with nothing here having chosen the number. So the number is chosen here,
 * per instrument, on the one dimension that grows with the data, and a fact whose name declares no budget
 * is refused where it is built ({@link MetricFact}) the moment it carries an attribute at all.
 *
 * <p><strong>Past the budget, the excess is folded, never dropped.</strong> A dropped series is work that
 * happened and is counted nowhere, so every total a consumer sums from the survivors comes up short by an
 * amount nobody can see. Folding keeps the total right: the series past the budget lose the one dimension
 * that grew and are added into a single series per remaining combination — a pipeline over twelve hundred
 * tables still reports the rows it took in and the rows it wrote out, per direction and per operation,
 * exactly; it is only the two hundred tables past the thousandth whose rows are reported under one name
 * rather than each under its own. That series carries {@link MetricAttributes#OVERFLOW}, the marker every
 * OpenTelemetry consumer already reads as "the rest".
 *
 * <p>"Added into a single series" is the counter's answer and most of the gauges', but not every gauge's:
 * some of them are readings of one subject each, and the age of the newest row in two tables is not an
 * age. Which of the two a gauge is cannot be read off its type, so every instrument declares it
 * ({@link Fold}) and the fold reads it from there.
 *
 * <p>The budget is per pipeline, not per process, so one wide pipeline cannot push another pipeline's
 * tables past the line. Which series are the named ones is decided by first sight, as the SDK decides it:
 * a table seen while there was room keeps its series for as long as the folder lives, and a table that
 * arrives once the budget is full is folded from its first observation. Deciding it per tick — by whatever
 * order the points happened to arrive in — would let a series alternate between having a name and not
 * having one, and a chart of it would show a line that breaks and mends with no change in the data.
 *
 * <p>{@link #EXPORT_SERIES_LIMIT} is the backstop behind this, for the dimension the fold leaves alone:
 * the pipeline id is bounded by the deployment rather than by any one pipeline's definition, and an export
 * across every pipeline of a process can only be bounded where every pipeline is visible. It is the number
 * an exporter configures its SDK's per-instrument limit to, and it is sized so that one pipeline at its
 * full budget fits under it whole.
 */
public enum CardinalityBudget {

    RECORDS("tapstate.pipeline.records", MetricAttributes.TABLE_ID, 1_000, Fold.ADDED),
    BYTES("tapstate.pipeline.bytes", MetricAttributes.TABLE_ID, 1_000, Fold.ADDED),
    /** The age of the newest row of a table: two tables' ages do not add, and the worst one is the reading. */
    LAG("tapstate.pipeline.lag", MetricAttributes.TABLE_ID, 1_000, Fold.HIGHEST),
    RECORD_DELIVERY_DURATION("tapstate.pipeline.record.delivery.duration", MetricAttributes.TABLE_ID, 1_000,
            Fold.ADDED),
    /** Broken down by stage only, and the stages are a closed set: nothing here can grow, so nothing folds. */
    PROCESS_DURATION("tapstate.pipeline.process.duration", null, Stage.values().length, Fold.ADDED),
    SNAPSHOT_ROWS("tapstate.pipeline.snapshot.rows", MetricAttributes.TABLE_ID, 1_000, Fold.ADDED),
    SNAPSHOT_ROWS_CURRENT_RUN("tapstate.pipeline.snapshot.rows.current_run",
            MetricAttributes.TABLE_ID, 1_000, Fold.ADDED),
    SNAPSHOT_ROWS_TOTAL("tapstate.pipeline.snapshot.rows.total", MetricAttributes.TABLE_ID, 1_000, Fold.ADDED),
    /** Codes come from a catalog and from connectors, not from rows, but a connector may contribute any number. */
    ERRORS("tapstate.pipeline.errors", MetricAttributes.CODE, 200, Fold.ADDED),
    /**
     * The families below carry a dimension the pipeline's definition draws — a chain, a nest level, a join
     * dimension — and are budgeted like the table for the same reason: a definition can name as many of them
     * as it names tables, and nothing that flows through the pipeline can add one.
     */
    FRONTIER_GAP("tapstate.pipeline.frontier.gap", MetricAttributes.CHAIN_ID, 1_000, Fold.HIGHEST),
    FRONTIER_STALL("tapstate.pipeline.frontier.stall", MetricAttributes.CHAIN_ID, 1_000, Fold.HIGHEST),
    NEST_ENTRIES("tapstate.pipeline.nest.entries", MetricAttributes.NEST_NAMESPACE, 1_000, Fold.ADDED),
    NEST_ACCESSES("tapstate.pipeline.nest.accesses", MetricAttributes.NEST_NAMESPACE, 1_000, Fold.ADDED),
    NEST_BACKFILLS("tapstate.pipeline.nest.backfills", MetricAttributes.NEST_NAMESPACE, 1_000, Fold.ADDED),
    NEST_BACKFILL_TIME("tapstate.pipeline.nest.backfill.time", MetricAttributes.NEST_NAMESPACE, 1_000,
            Fold.ADDED),
    /** A mark each namespace's own worst moment left: the peaks did not happen together, so they do not add. */
    NEST_PENDING_HIGH_WATER("tapstate.pipeline.nest.pending.high_water", MetricAttributes.NEST_NAMESPACE,
            1_000, Fold.HIGHEST),
    NEST_STORED("tapstate.pipeline.nest.stored", MetricAttributes.NEST_NAMESPACE, 1_000, Fold.ADDED),
    NEST_DEAD_LETTERED("tapstate.pipeline.nest.dead_lettered", MetricAttributes.NEST_NAMESPACE, 1_000,
            Fold.ADDED),
    JOIN_RECOMPUTE_ROWS("tapstate.pipeline.join.recompute.rows", MetricAttributes.JOIN_NAMESPACE, 1_000,
            Fold.ADDED),
    JOIN_RECOMPUTE_ROWS_TOTAL("tapstate.pipeline.join.recompute.rows.total", MetricAttributes.JOIN_NAMESPACE,
            1_000, Fold.ADDED),
    /** Two readings about the pipeline as a whole: the pipeline is their only attribute, so nothing grows and nothing folds. */
    RECORDS_DRIVEN("tapstate.pipeline.records.driven", null, 1, Fold.ADDED),
    RECONCILE_FAILURES_STREAK("tapstate.pipeline.reconcile.failures.streak", null, 1, Fold.HIGHEST);

    /**
     * What several series of one instrument make when they fold into one.
     *
     * <p>A counter's points always add: it measures work, and work done under two names is work done. A
     * gauge is a reading, and whether readings add is a property of the quantity and not of the metric
     * type — entries held in two namespaces are entries held, while the age of the newest row in two
     * tables is not an age at all. So every instrument says which of the two it is, once, here.
     *
     * <p>Getting it wrong in the adding direction is the dangerous one: a fold that keeps the highest of
     * quantities that add reports the largest holder in place of the total, which reads exactly like a
     * correct total and is short by everything else. A pipeline over two thousand namespaces each holding
     * a thousand entries would report a million entries as a thousand.
     */
    public enum Fold {

        /** The quantities add: the folded series carries their sum. */
        ADDED,

        /** Each is a reading of its own subject and they do not add: the folded series carries the largest. */
        HIGHEST
    }

    /**
     * The series one instrument may hold across every pipeline of a process, which is where an exporter
     * sets its SDK's own limit. One pipeline at its full budget takes a thousand tables times two
     * directions times five operations for the widest instrument, and that must fit under this whole.
     */
    public static final int EXPORT_SERIES_LIMIT = 10_000;

    private final String instrument;
    private final String openDimension;
    private final int distinctValues;
    private final Fold fold;

    CardinalityBudget(String instrument, String openDimension, int distinctValues, Fold fold) {
        this.instrument = instrument;
        this.openDimension = openDimension;
        this.distinctValues = distinctValues;
        this.fold = fold;
    }

    /** The budget declared for {@code instrument}, or empty for a name that declares none. */
    public static Optional<CardinalityBudget> forInstrument(String instrument) {
        return Arrays.stream(values()).filter(entry -> entry.instrument.equals(instrument)).findFirst();
    }

    /** The metric name this budget belongs to. */
    public String instrument() {
        return instrument;
    }

    /**
     * The attribute whose values grow with the data and are folded past the budget, or empty for an
     * instrument every one of whose dimensions is a closed set.
     */
    public Optional<String> openDimension() {
        return Optional.ofNullable(openDimension);
    }

    /** How many distinct values of the open dimension one pipeline may hold series for. */
    public int distinctValues() {
        return distinctValues;
    }

    /** What the series past the budget make when they fold into one. */
    public Fold fold() {
        return fold;
    }

    /** A folder that remembers, per instrument and pipeline, which values it has named. Not thread-safe. */
    public static Folder folder() {
        return new Folder();
    }

    /**
     * Applies the budgets to facts as they pass, remembering which values of each open dimension were
     * named while there was room. One folder lives as long as the consumer it folds for — an exporter,
     * typically — so that the named set is stable across ticks.
     */
    public static final class Folder {

        /** instrument -> pipeline id -> the values of the open dimension that hold a series of their own. */
        private final Map<String, Map<String, Set<String>>> named = new HashMap<>();

        private Folder() {
        }

        /**
         * Gives up what is remembered for every pipeline outside {@code pipelineIds}.
         *
         * <p>A folder lives as long as the consumer it folds for, and that outlives any one pipeline. The
         * values named for a pipeline that is gone are not merely idle: an id applied a second time over a
         * different set of tables finds its budget already spent on the tables of the pipeline that had
         * the id before, and every one of the new tables folds from its first observation.
         *
         * <p>A series carrying no pipeline at all is kept, the way the fold keeps it: it belongs to no
         * pipeline, so no pipeline's removal takes it away.
         */
        public void forgetPipelinesOutside(Collection<String> pipelineIds) {
            Set<String> kept = Set.copyOf(pipelineIds);
            named.values().forEach(byPipeline ->
                    byPipeline.keySet().removeIf(pipeline -> !pipeline.isEmpty() && !kept.contains(pipeline)));
        }

        /**
         * {@code fact} with every series past its instrument's budget folded into one series per remaining
         * attribute combination, or {@code fact} itself when nothing is past it. A fact that declares no
         * budget, or whose dimensions are all closed, is returned as it came.
         */
        public MetricFact fold(MetricFact fact) {
            CardinalityBudget budget = forInstrument(fact.name()).orElse(null);
            if (budget == null || budget.openDimension == null) {
                return fact;
            }
            List<MetricPoint> kept = new ArrayList<>();
            Map<Map<String, String>, List<MetricPoint>> folded = new LinkedHashMap<>();
            for (MetricPoint point : fact.points()) {
                String value = point.attributes().get(budget.openDimension);
                if (value == null) {
                    // Already without the growing dimension: a series folded upstream joins the fold here,
                    // anything else is a series about the whole pipeline and is named as it came.
                    if (point.attributes().containsKey(MetricAttributes.OVERFLOW)) {
                        folded.computeIfAbsent(point.attributes(), key -> new ArrayList<>()).add(point);
                    } else {
                        kept.add(point);
                    }
                    continue;
                }
                Set<String> namedHere = named
                        .computeIfAbsent(fact.name(), instrument -> new HashMap<>())
                        .computeIfAbsent(point.attributes().getOrDefault(MetricAttributes.PIPELINE_ID, ""),
                                pipeline -> new HashSet<>());
                if (namedHere.contains(value) || namedHere.size() < budget.distinctValues) {
                    namedHere.add(value);
                    kept.add(point);
                    continue;
                }
                Map<String, String> rest = new HashMap<>(point.attributes());
                rest.remove(budget.openDimension);
                rest.put(MetricAttributes.OVERFLOW, "true");
                folded.computeIfAbsent(Map.copyOf(rest), key -> new ArrayList<>()).add(point);
            }
            if (folded.isEmpty()) {
                return fact;
            }
            folded.forEach((attributes, points) -> kept.add(merge(fact.name(), fact.type(), attributes, points)));
            return new MetricFact(fact.name(), fact.type(), fact.unit(), kept);
        }
    }

    /**
     * One series holding what {@code points} held together, combined the way {@code instrument} allows: a
     * counter's points add up, a gauge's add or keep the highest as its {@link Fold} declares, and a
     * distribution's buckets are added bucket by bucket — every point of one histogram instrument carries
     * the same bounds, which is what makes that addition meaningful. An accumulation that combines several
     * begins when the earliest of them began.
     *
     * <p>Public because it is the one statement of how series combine: the fold above uses it for the
     * dimension a pipeline's data grows, and an exporter uses it again for the series beyond
     * {@link #EXPORT_SERIES_LIMIT}. Two copies of these rules would be two answers to one question.
     */
    public static MetricPoint merge(String instrument, MetricType type, Map<String, String> attributes,
            List<MetricPoint> points) {
        Instant start = null;
        Instant observed = null;
        for (MetricPoint point : points) {
            if (point.startTime() != null && (start == null || point.startTime().isBefore(start))) {
                start = point.startTime();
            }
            if (observed == null || point.observedAt().isAfter(observed)) {
                observed = point.observedAt();
            }
        }
        if (type == MetricType.HISTOGRAM) {
            HistogramValue first = points.get(0).histogram();
            long count = 0L;
            double sum = 0.0;
            long[] buckets = new long[first.bucketCounts().size()];
            for (MetricPoint point : points) {
                HistogramValue histogram = point.histogram();
                count += histogram.count();
                sum += histogram.sum();
                for (int bucket = 0; bucket < buckets.length; bucket++) {
                    buckets[bucket] += histogram.bucketCounts().get(bucket);
                }
            }
            List<Long> bucketCounts = new ArrayList<>(buckets.length);
            for (long bucket : buckets) {
                bucketCounts.add(bucket);
            }
            return MetricPoint.distribution(attributes, start, observed,
                    new HistogramValue(count, sum, first.bounds(), bucketCounts));
        }
        boolean added = type == MetricType.COUNTER || foldOf(instrument) == Fold.ADDED;
        long value = added ? 0L : Long.MIN_VALUE;
        for (MetricPoint point : points) {
            value = added ? value + point.value() : Math.max(value, point.value());
        }
        return type == MetricType.COUNTER
                ? MetricPoint.accumulated(attributes, start, observed, value)
                : MetricPoint.reading(attributes, observed, value);
    }

    /**
     * How several series of {@code instrument} combine. A name that declares no budget never folds here —
     * a fact declaring none is refused the moment it carries an attribute at all — and adding is the
     * answer for the one that somehow arrives, because a total is the reading a consumer sums.
     */
    static Fold foldOf(String instrument) {
        return forInstrument(instrument).map(CardinalityBudget::fold).orElse(Fold.ADDED);
    }
}
