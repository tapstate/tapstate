package io.tapstate.runtime.scheduler;

import io.tapstate.core.event.Op;
import io.tapstate.core.lifecycle.CaptureReading;
import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.DeliveryReading;
import io.tapstate.core.lifecycle.FlatMetricProjection;
import io.tapstate.core.lifecycle.FlatReduction;
import io.tapstate.core.lifecycle.FrontierStallPressure;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.NestColdLayerPressure;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.core.lifecycle.StageReading;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StateStore;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Publishes a pipeline's observation from its converged actual state: it reads the fenced checkpoint,
 * maps the wire-form state back to the lifecycle state, and writes the latest observation projection to
 * the observation store. This is the runtime side of the store-backed observation contract — the runtime
 * writes the observation, control reads it, and the two meet only at the store, never calling each other.
 * A pipeline with no checkpoint yet has nothing to observe and is left untouched (no empty doc is written).
 *
 * <p>The errorCount metric has two sources. On a converged pass it is derived from that same actual state —
 * 1 while the pipeline is FAILED, 0 otherwise — so a dead data-plane job is an observable statistic, not just
 * a log line. When a reconcile pass instead keeps throwing (its store is unreachable, so it never converges),
 * {@link #publishReconcileFailure} carries the consecutive-failure count as errorCount so the pipeline is
 * observable as broken rather than silently absent from the read face. Either way errorCount &gt; 0 means the
 * pipeline is unhealthy. recordCount, the per-table sink-acked positions and the per-table initial load all
 * come from injected sources: the record count rides the numeric metrics map when a live job reports one and
 * is absent otherwise, the positions ride their own String-valued map, and the snapshot dataset carries what
 * the capture side reports for each table it has loaded. A missing metric, position or table means its source
 * is not wired or has nothing to report, expressed by absence rather than a faked zero. Republishing
 * overwrites the latest projection in place — the observation is
 * current-state, not a time series, so the derived errorCount tracks the state and does not accumulate across
 * ticks (a recovered pipeline drops back to 0).
 */
public final class ObservationPublisher {

    /**
     * What a per-chain frontier reading is named in the metrics map, with the chain's own name appended.
     * The name is this layer's to choose: the metrics map is what a read face presents, and how a run
     * happens to publish its statistics internally is the engine's business behind its port.
     */
    private static final String FRONTIER_GAP_PREFIX = "frontierGap.";

    /**
     * What a per-chain pinned-for reading is named, with the chain's own name appended. Milliseconds are
     * in the name because the metrics map carries bare numbers: a duration whose unit is written down
     * anywhere but here is a duration somebody thresholds in seconds.
     *
     * <p>Published beside the distance rather than instead of it. The distance cannot say a frontier has
     * stopped — it reads zero for a chain held back by pending changes upstream and zero again for one
     * keeping up — and this cannot say which of the two stalls it is. What raises an alarm is this one:
     * a pinned durable position is racing the source's retention window, which is kept in time.
     */
    private static final String FRONTIER_STALLED_PREFIX = "frontierStalledMillis.";

    /**
     * What a per-namespace nest state reading is named, with the namespace appended. Four numbers rather
     * than the hit ratio they imply: a ratio published here would be an average over the whole run, which
     * is the one window in which a state layer that fell off its cliff a minute ago still reads as healthy.
     * Two scrapes of counts give any window a reader wants, and the ratio is one division away.
     */
    private static final String NEST_ENTRIES_PREFIX = "nestStateEntries.";
    private static final String NEST_ACCESSES_PREFIX = "nestStateAccesses.";
    private static final String NEST_BACKFILLS_PREFIX = "nestStateBackfills.";
    private static final String NEST_BACKFILL_MILLIS_PREFIX = "nestStateBackfillMillis.";

    /**
     * The deepest one key of the namespace has been seen holding for something that has not arrived, since
     * the run began. A mark rather than a count, and the only reading here that no other one can stand in
     * for: a queue waiting under a single key lives inside a single entry, so it moves none of the four
     * above however long it grows, and the run stops on the limit that bounds it with all of them flat.
     *
     * <p>Published and not judged. Whether a parent is still coming is not knowable from how much arrived
     * before it, so nothing here is an alarm - what a deep queue calls for is someone looking at the source
     * the rows point into.
     */
    private static final String NEST_PENDING_HIGH_WATER_PREFIX = "nestStatePendingHighWater.";

    /**
     * How much the namespace holds altogether, against the entries above which are only what is in memory.
     * Published where there is a layer behind the memory to ask and left out where there is not: a run
     * holding its state in memory alone has no second number, and one reported anyway would be the first
     * number wearing the name of the second.
     */
    private static final String NEST_STORED_PREFIX = "nestStateStored.";

    /**
     * How many changes the namespace could never place in a document. Published only for a namespace that
     * discarded any, so that nothing discarded is an absence rather than a zero - the one reading here whose
     * quiet state and whose unwired state would otherwise be spelled the same way.
     */
    private static final String NEST_DEAD_LETTERED_PREFIX = "nestDeadLettered.";

    /**
     * How far the large rebuilds in one dimension's namespace have got, and about how far they have to go,
     * with the namespace appended. Two numbers because the whole of what a rebuild says is the distance
     * between them; either alone is a number with nothing to be read against. The key a rebuild is about
     * is a value out of a row and travels in no reading here: rebuilds in one namespace are added together.
     *
     * <p>Published only while a rebuild large enough to be a wait is under way, so an absence here is the
     * quiet state rather than an unwired one. That is the opposite choice from the readings above, and it
     * is made for the same reason they made theirs: a rebuild reported on every dimension edit is a
     * constant stream, and a reader who has learned to scroll past it is a reader this cannot reach on the
     * one occasion it matters - an edit to a single row that owes a million rows of writing, throughout
     * which the pipeline runs, counts no error, and holds half the old value and half the new one.
     */
    private static final String JOIN_RECOMPUTE_DONE_PREFIX = "joinRecomputeRowsDone.";
    private static final String JOIN_RECOMPUTE_EXPECTED_PREFIX = "joinRecomputeRowsExpected.";

    /**
     * The canonical names of the per-chain and per-namespace readings above, under which they travel as
     * facts with the chain or the namespace as an attribute. The prefixes above are how the flat face
     * spells the same readings, one key per chain or namespace, and {@link #FLAT_REDUCTIONS} is where the
     * two are tied together: every reader of the flat face keeps its key, and a reader of the facts gets
     * the dimension as a dimension.
     */
    private static final String FRONTIER_GAP_METRIC = "tapstate.pipeline.frontier.gap";
    private static final String FRONTIER_STALL_METRIC = "tapstate.pipeline.frontier.stall";
    private static final String NEST_ENTRIES_METRIC = "tapstate.pipeline.nest.entries";
    private static final String NEST_ACCESSES_METRIC = "tapstate.pipeline.nest.accesses";
    private static final String NEST_BACKFILLS_METRIC = "tapstate.pipeline.nest.backfills";
    private static final String NEST_BACKFILL_TIME_METRIC = "tapstate.pipeline.nest.backfill.time";
    private static final String NEST_PENDING_HIGH_WATER_METRIC = "tapstate.pipeline.nest.pending.high_water";
    private static final String NEST_STORED_METRIC = "tapstate.pipeline.nest.stored";
    private static final String NEST_DEAD_LETTERED_METRIC = "tapstate.pipeline.nest.dead_lettered";
    private static final String JOIN_RECOMPUTE_ROWS_METRIC = "tapstate.pipeline.join.recompute.rows";
    private static final String JOIN_RECOMPUTE_ROWS_TOTAL_METRIC = "tapstate.pipeline.join.recompute.rows.total";

    /**
     * The three measurements of a pipeline's movement that carry their dimensions as attributes rather
     * than in their names. Their names are the canonical ones a monitoring backend sees; how the flat
     * face below spells them is that face's business and is decided in {@link #FLAT_REDUCTIONS}.
     */
    private static final String RECORDS_METRIC = "tapstate.pipeline.records";
    private static final String BYTES_METRIC = "tapstate.pipeline.bytes";
    private static final String LAG_METRIC = "tapstate.pipeline.lag";

    /**
     * How long settled rows took, per table, as a distribution over the registered bounds. It has no
     * single number to be on the flat face, which refuses it a rule rather than squeezing it, so it is
     * read on the facts alone -- which is where a percentile is a number somebody computes rather than one
     * that was averaged away before they saw it.
     */
    private static final String RECORD_DELIVERY_DURATION_METRIC = "tapstate.pipeline.record.delivery.duration";

    /**
     * Where in the graph time is spent: one distribution per stage of how long its units of work took. Read
     * on the facts alone, like the delivery time, and for the same reason. What a unit is belongs to the
     * stage: a row through a transform, a drain through a nest or a join, a batch issued by a sink, a read
     * of the ring by a source.
     */
    private static final String PROCESS_DURATION_METRIC = "tapstate.pipeline.process.duration";
    private static final String STAGE_ATTRIBUTE = MetricAttributes.STAGE;

    /**
     * The bounded load's two measurements, which carry their table as an attribute the way the pair above
     * do. They are a monitoring backend's view of the same load the observation's own snapshot dataset
     * describes, taken from the same reading rather than counted a second time -- one account, two
     * projections, so the two faces cannot come to disagree.
     *
     * <p>How far the load got accumulates and about how far it has to go does not, which is why they are a
     * counter and a gauge and not two of either. The total is what the last discovery of the source
     * counted: an estimate, never maintained since, and free to be revised downwards by the next
     * discovery. Declaring it a counter would promise a reader it only ever rises, and the first
     * re-discovery of a table that shrank would break that promise silently.
     */
    private static final String SNAPSHOT_ROWS_METRIC = "tapstate.pipeline.snapshot.rows";
    private static final String SNAPSHOT_ROWS_TOTAL_METRIC = "tapstate.pipeline.snapshot.rows.total";

    /**
     * How many failures this pipeline has had, by the code that names each one. A counter, and a real one:
     * what stood here before was the pipeline's own state written as a number - one while it was FAILED
     * and nought otherwise - which is a state wearing a count's name. It could not answer how many times
     * anything had happened, and it went back down when the pipeline recovered.
     *
     * <p>Counted once per failure the converge side witnesses, which is once per death: the pass that saw
     * it is the only one handed the cause, and every later pass while the state stays FAILED is handed
     * nothing and adds nothing.
     *
     * <p>The code is the attribute because it is the only part of a failure worth grouping by. "How many
     * errors" is a number nobody can act on; "which kind of failure is rising" is the question an operator
     * actually has, and it is the one the shape that stood here could not be asked at all.
     */
    private static final String ERRORS_METRIC = "tapstate.pipeline.errors";

    /**
     * How many convergence passes in a row have thrown for this pipeline, published only while that streak
     * is running. Kept apart from the counter above, and it is not a second spelling of it.
     *
     * <p><strong>It is a streak, not a total.</strong> One clean pass returns it to nothing, so it goes
     * down, and a counter that goes down is a counter every consumer reads as a restart. It also counts a
     * different thing: a pass that keeps throwing never reaches a publish at all, so there is no witnessed
     * failure to attribute a code to - what is observable is only that the attempt is not getting through.
     *
     * <p>It says streak in its name because the one reader it has needs it to be one: the command line
     * reports "N passes in a row have thrown", and a name that said total would have that sentence quietly
     * start lying the day somebody made it one.
     */
    private static final String RECONCILE_STREAK_METRIC = "tapstate.pipeline.reconcile.failures.streak";

    /** How the flat face has always spelled the streak, and still does. */
    private static final String RECONCILE_STREAK_FLAT = "reconcileFailuresInARow";

    /**
     * Records the live job has driven to its sinks, read off the job's own statistics: what reached the
     * sink, which is not what the target confirmed and is not the rows counted as delivered. It predates
     * the delivered count and its one reader on the command line still reads it; the canonical name says
     * what it is, and the flat face keeps spelling it the way that reader was built against.
     */
    private static final String RECORDS_DRIVEN_METRIC = "tapstate.pipeline.records.driven";
    private static final String RECORDS_DRIVEN_FLAT = "recordCount";

    private static final String CODE_ATTRIBUTE = MetricAttributes.CODE;

    private static final String PIPELINE_ID_ATTRIBUTE = MetricAttributes.PIPELINE_ID;
    private static final String TABLE_ID_ATTRIBUTE = MetricAttributes.TABLE_ID;
    private static final String CHAIN_ID_ATTRIBUTE = MetricAttributes.CHAIN_ID;
    private static final String NEST_NAMESPACE_ATTRIBUTE = MetricAttributes.NEST_NAMESPACE;
    private static final String JOIN_NAMESPACE_ATTRIBUTE = MetricAttributes.JOIN_NAMESPACE;
    private static final String DIRECTION_ATTRIBUTE = MetricAttributes.DIRECTION;
    private static final String OP_ATTRIBUTE = MetricAttributes.OP;
    private static final String INBOUND = "in";
    private static final String OUTBOUND = "out";

    /**
     * How the two dimensioned measurements are spelled on the flat face, which has one name per number and
     * so cannot hold them as they are. Rows collapse to one key per direction: the whole pipeline's
     * throughput rather than each table's and each operation's, because this face is read by a person at a
     * command line and a pipeline over twenty tables would otherwise bury them under two hundred keys
     * whose names are decided by the data. The distance a pipeline is behind keeps its table, the way the
     * per-chain readings above already do — there are as many of those as there are tables, and the table
     * that stopped moving is the one worth seeing on its own.
     *
     * <p>Collapsing is not the same as dropping and is recorded separately by the projection, so what a
     * reader of this face is actually looking at stays answerable.
     */
    static final Map<String, FlatReduction> FLAT_REDUCTIONS = Map.ofEntries(
            Map.entry(RECORDS_METRIC, keyed("records.", DIRECTION_ATTRIBUTE)),
            Map.entry(BYTES_METRIC, keyed("bytes.", DIRECTION_ATTRIBUTE)),
            Map.entry(LAG_METRIC, keyed("lag.", TABLE_ID_ATTRIBUTE)),
            // Reduced and not dropped, which is the opposite of what the load's two measurements get, and
            // for the reason that decides between them: a drop is only honest when another face carries
            // the metric, and the load has one - this observation's own snapshot dataset. Failures have
            // none. Dropped here they would be measured and readable nowhere at all until an exporter
            // exists, which is a gate away.
            Map.entry(ERRORS_METRIC, keyed("errors.", CODE_ATTRIBUTE)),
            // The per-chain and per-namespace families keep the flat keys they have always had, letter for
            // letter: one key per chain or namespace, the dimension appended to a fixed prefix. That is the
            // spelling every reader of this face was built against, and the facts beside it are where the
            // dimension became an attribute -- the change is in what is carried, not in what anybody reads.
            Map.entry(FRONTIER_GAP_METRIC, keyed(FRONTIER_GAP_PREFIX, CHAIN_ID_ATTRIBUTE)),
            Map.entry(FRONTIER_STALL_METRIC, keyed(FRONTIER_STALLED_PREFIX, CHAIN_ID_ATTRIBUTE)),
            Map.entry(NEST_ENTRIES_METRIC, keyed(NEST_ENTRIES_PREFIX, NEST_NAMESPACE_ATTRIBUTE)),
            Map.entry(NEST_ACCESSES_METRIC, keyed(NEST_ACCESSES_PREFIX, NEST_NAMESPACE_ATTRIBUTE)),
            Map.entry(NEST_BACKFILLS_METRIC, keyed(NEST_BACKFILLS_PREFIX, NEST_NAMESPACE_ATTRIBUTE)),
            Map.entry(NEST_BACKFILL_TIME_METRIC, keyed(NEST_BACKFILL_MILLIS_PREFIX, NEST_NAMESPACE_ATTRIBUTE)),
            Map.entry(NEST_PENDING_HIGH_WATER_METRIC,
                    keyed(NEST_PENDING_HIGH_WATER_PREFIX, NEST_NAMESPACE_ATTRIBUTE)),
            Map.entry(NEST_STORED_METRIC, keyed(NEST_STORED_PREFIX, NEST_NAMESPACE_ATTRIBUTE)),
            Map.entry(NEST_DEAD_LETTERED_METRIC, keyed(NEST_DEAD_LETTERED_PREFIX, NEST_NAMESPACE_ATTRIBUTE)),
            Map.entry(JOIN_RECOMPUTE_ROWS_METRIC, keyed(JOIN_RECOMPUTE_DONE_PREFIX, JOIN_NAMESPACE_ATTRIBUTE)),
            Map.entry(JOIN_RECOMPUTE_ROWS_TOTAL_METRIC,
                    keyed(JOIN_RECOMPUTE_EXPECTED_PREFIX, JOIN_NAMESPACE_ATTRIBUTE)),
            // The two per-pipeline readings carry only the pipeline as an attribute, the way every
            // canonical fact does, and the flat face keeps their bare keys.
            Map.entry(RECORDS_DRIVEN_METRIC, attributes -> RECORDS_DRIVEN_FLAT),
            Map.entry(RECONCILE_STREAK_METRIC, attributes -> RECONCILE_STREAK_FLAT));

    /**
     * A rule spelling a point's flat key as {@code prefix} and the value of {@code dimension}, and
     * answering nothing at all for a point that does not carry that dimension.
     *
     * <p>The second half is the whole reason this is a method. Written inline as {@code prefix +
     * attributes.get(dimension)}, an absent attribute does not produce no key — it produces the key
     * {@code prefix + "null"}, which looks like every other key on this face while the totals beside it
     * are short by exactly the points that landed there. The projection's refusal cannot see it, because
     * a key was answered. Asking for the attribute first is what turns that into the loud failure the
     * projection is there to make.
     */
    private static FlatReduction keyed(String prefix, String dimension) {
        return attributes -> {
            String value = attributes.get(dimension);
            if (value == null) {
                return "true".equals(attributes.get(MetricAttributes.OVERFLOW)) ? prefix + "~other" : null;
            }
            // A literal dimension beginning with '~' is escaped so the overflow key stays distinct.
            return prefix + (value.startsWith("~") ? "~" + value : value);
        };
    }

    /**
     * The name the metric contract gives each of this engine's change kinds.
     *
     * <p><strong>A snapshot read keeps its own name.</strong> This attribute carries the operation the
     * <em>source</em> performed, never what the target did with the row afterwards, and at the source
     * nothing was inserted: an existing row was read. Calling it an insert would describe the far end of
     * the pipeline under a field defined by the near end, and it would add a row that was already there to
     * the same total as a row that has just come into existence — two quantities an operator sizing a
     * source apart from its change rate needs kept apart. It is the name every change-data consumer
     * already expects for this, alongside the three row mutations.
     *
     * <p>It is emphatically not "other". That name is for a change kind nothing here recognises, and a
     * reader who meets it on a chart has been told only that somebody gave up — which they will interpret
     * anyway, and wrongly. A quantity a reader cannot interpret is worse than one they cannot see.
     *
     * <p>Which <em>phase</em> a row arrived in remains a separate question from what happened to it, and
     * an attribute of its own is where it belongs. That the two happen to coincide today — a source
     * produces reads only while it is loading — is a consequence rather than a definition, and it stops
     * being true of every operation but this one the moment the load ends.
     *
     * <p><strong>Derived from the engine's own change kinds, not copied from them.</strong> A list typed
     * out here would be right on the day it was written and would then go on publishing a kind added later
     * under the name kept for kinds nothing recognises — a value a reader cannot interpret, arriving
     * silently, with no gate anywhere that a second copy had stopped agreeing with the first.
     *
     * <p>The fallback covers a symbol nothing here recognises, which is the one case "other" is for: the
     * closed set of operation names exists so that nothing arriving from the data can add a value to it.
     * A symbol reaches that fallback only by arriving from outside the engine's own set. Two such symbols
     * therefore land on one name, which is what the summing below is for.
     */
    private static final Map<String, String> OP_NAMES = Arrays.stream(Op.values())
            .collect(Collectors.toUnmodifiableMap(Op::symbol, op -> op.name().toLowerCase(Locale.ROOT)));

    private final StateStore state;
    private final ObservationStore observations;
    private final Function<String, OptionalLong> recordCounts;
    private final Function<String, Map<String, String>> positions;
    private final Function<String, SnapshotReading> snapshots;
    private final Function<String, Map<String, Long>> frontierGaps;
    private final Function<String, Map<String, NestStateReading>> nestStateReadings;
    private final Function<String, Map<String, Long>> frontierStalls;
    private final Function<String, Map<String, Long>> nestDeadLetters;
    private final Function<String, Map<String, Long>> joinRecomputeDone;
    private final Function<String, Map<String, Long>> joinRecomputeExpected;
    private final Function<String, CaptureReading> captures;
    private final Function<String, DeliveryReading> deliveries;
    private final Function<String, StageReading> stages;
    private final FrontierStallWatch frontierStall;
    private final NestColdLayerWatch coldLayer;
    private final Clock clock;
    private final CardinalityBudget.Folder cardinality = CardinalityBudget.folder();
    private final Map<String, ObservationFailure> currentFailures = new ConcurrentHashMap<>();
    private final Map<String, ObservationStore.Scope> currentScopes = new ConcurrentHashMap<>();
    private final Map<String, Instant> failureCountingSinceByPipeline = new ConcurrentHashMap<>();

    /**
     * How many failures each pipeline has had, by code, since this publisher opened its account.
     *
     * <p>Held here because this is where the input already arrives: the converge side hands a coded cause
     * to {@link #publish(String, ObservationFailure)} on the one pass that witnesses a death, so counting
     * it needs no port of its own and cannot be fed twice for one failure.
     *
     * <p><strong>It does not survive a restart, and that is the answer rather than a shortfall.</strong>
     * A new process opens a new account and says so through a later start, which is what lets a consumer
     * tell a restart from a total that went backwards. Carrying totals across restarts and keeping
     * restarts visible cannot both be had, and every counter on this face has already chosen the second.
     */
    private final Map<String, Map<String, Long>> failuresByPipelineAndCode = new ConcurrentHashMap<>();

    /** When this publisher opened the failure account above; every failure point accumulates from it. */
    private final Instant countingFailuresSince;

    /**
     * A publisher with no metric, position or snapshot source: recordCount stays absent and positions and
     * snapshot stay empty, so it carries the state-derived errorCount alone. This is the shape callers used
     * before those sources were wired; the assembly point injects the real sources through the full
     * constructor.
     */
    public ObservationPublisher(StateStore state, ObservationStore observations) {
        this(state, observations, id -> OptionalLong.empty(), id -> Map.of(), id -> SnapshotReading.NONE,
                id -> Map.of());
    }

    /** A publisher wired to its metric and position sources but with no snapshot or frontier source. */
    public ObservationPublisher(StateStore state, ObservationStore observations,
            Function<String, OptionalLong> recordCounts, Function<String, Map<String, String>> positions) {
        this(state, observations, recordCounts, positions, id -> SnapshotReading.NONE, id -> Map.of());
    }

    /**
     * A publisher wired to its live run-statistic sources: {@code recordCounts} yields the records a
     * pipeline's live job has driven to its sinks (empty when it has no live job), {@code positions}
     * yields the durable per-table sink-acked source positions (empty when none), and {@code snapshots}
     * yields the per-table initial-load progress with the moment the load began (nothing when no load has
     * run). All three are
     * ports so the scheduler stays clear of the engine, the store and the capture side that back them.
     */
    public ObservationPublisher(StateStore state, ObservationStore observations,
            Function<String, OptionalLong> recordCounts, Function<String, Map<String, String>> positions,
            Function<String, SnapshotReading> snapshots) {
        this(state, observations, recordCounts, positions, snapshots, id -> Map.of(), id -> Map.of());
    }

    /**
     * A publisher also wired to the per-chain frontier readings: {@code frontierGaps} yields how far each
     * chain of a pipeline's live run trails the bound combined for it (empty when nothing reports one).
     * A fourth port for the same reason as the other three - the scheduler stays clear of what backs them.
     */
    public ObservationPublisher(StateStore state, ObservationStore observations,
            Function<String, OptionalLong> recordCounts, Function<String, Map<String, String>> positions,
            Function<String, SnapshotReading> snapshots,
            Function<String, Map<String, Long>> frontierGaps) {
        this(state, observations, recordCounts, positions, snapshots, frontierGaps, id -> Map.of());
    }

    /**
     * A publisher also wired to the per-namespace nest state readings: {@code nestStateReadings} yields how
     * full each namespace of a pipeline's live run is, how much of the reading it served from memory and
     * what the rest cost (empty when nothing reports any). A fifth port for the same reason as the others.
     */
    public ObservationPublisher(StateStore state, ObservationStore observations,
            Function<String, OptionalLong> recordCounts, Function<String, Map<String, String>> positions,
            Function<String, SnapshotReading> snapshots,
            Function<String, Map<String, Long>> frontierGaps,
            Function<String, Map<String, NestStateReading>> nestStateReadings) {
        this(state, observations, recordCounts, positions, snapshots, frontierGaps, nestStateReadings,
                new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, NestColdLayerAlert.NONE));
    }

    /**
     * A publisher that also hands each pass's nest state readings to {@code coldLayer}, which reports a
     * namespace that has stopped being served from memory. It is fed from here because this is the one
     * place the readings already exist: they are fetched once per pass, and asking for them a second time
     * would pay for counting the cold layer twice a tick.
     */
    public ObservationPublisher(StateStore state, ObservationStore observations,
            Function<String, OptionalLong> recordCounts, Function<String, Map<String, String>> positions,
            Function<String, SnapshotReading> snapshots,
            Function<String, Map<String, Long>> frontierGaps,
            Function<String, Map<String, NestStateReading>> nestStateReadings,
            NestColdLayerWatch coldLayer) {
        this(state, observations, recordCounts, positions, snapshots, frontierGaps, nestStateReadings,
                coldLayer, id -> Map.of());
    }

    /**
     * A publisher also wired to how long each chain has had its durable position pinned:
     * {@code frontierStalls} yields that, in milliseconds, for the chains of a pipeline's live run that
     * are pinned (empty when none is). A sixth port for the same reason as the others.
     *
     * <p>It is a separate port from {@code frontierGaps} despite carrying the same shape, because the two
     * describe different sets: a chain that caught up reports a distance and is not pinned, and a chain
     * that never advanced at all is pinned and has no distance to report. Folding them into one map would
     * need a sentinel for each absence, and both sentinels would be zero — the healthy end of both scales.
     */
    public ObservationPublisher(StateStore state, ObservationStore observations,
            Function<String, OptionalLong> recordCounts, Function<String, Map<String, String>> positions,
            Function<String, SnapshotReading> snapshots,
            Function<String, Map<String, Long>> frontierGaps,
            Function<String, Map<String, NestStateReading>> nestStateReadings,
            NestColdLayerWatch coldLayer,
            Function<String, Map<String, Long>> frontierStalls) {
        this(state, observations, recordCounts, positions, snapshots, frontierGaps, nestStateReadings,
                coldLayer, frontierStalls,
                new FrontierStallWatch(FrontierStallPressure.DEFAULT, FrontierStallAlert.NONE));
    }

    /**
     * A publisher that also hands each pass's frontier readings to {@code frontierStall}, which reports a
     * chain whose durable position has stopped moving for too long. Fed from here for the same reason the
     * cold-layer watch is: the readings already exist on this pass, and taking them again would both pay
     * for a second collection and let the alarm and the read face describe different moments.
     */
    public ObservationPublisher(StateStore state, ObservationStore observations,
            Function<String, OptionalLong> recordCounts, Function<String, Map<String, String>> positions,
            Function<String, SnapshotReading> snapshots,
            Function<String, Map<String, Long>> frontierGaps,
            Function<String, Map<String, NestStateReading>> nestStateReadings,
            NestColdLayerWatch coldLayer,
            Function<String, Map<String, Long>> frontierStalls,
            FrontierStallWatch frontierStall) {
        this(state, observations, recordCounts, positions, snapshots, frontierGaps, nestStateReadings,
                coldLayer, frontierStalls, frontierStall, id -> Map.of());
    }

    /**
     * A publisher also wired to how many changes each namespace could never place in a document:
     * {@code nestDeadLetters} yields that for the namespaces of a pipeline's live run that discarded any
     * (empty when none did). An eighth port for the same reason as the others.
     *
     * <p>Separate from the state readings despite the same shape, and for a sharper reason than they are
     * separate from each other: those describe how a namespace's memory is holding up and are published for
     * every namespace that runs, while this is published only where data was lost. Folded together, a
     * namespace would have to report a zero here on every pass to keep its readings complete - and a zero on
     * every pass is exactly what a discarding pipeline would look like if this ever stopped being wired.
     */
    public ObservationPublisher(StateStore state, ObservationStore observations,
            Function<String, OptionalLong> recordCounts, Function<String, Map<String, String>> positions,
            Function<String, SnapshotReading> snapshots,
            Function<String, Map<String, Long>> frontierGaps,
            Function<String, Map<String, NestStateReading>> nestStateReadings,
            NestColdLayerWatch coldLayer,
            Function<String, Map<String, Long>> frontierStalls,
            FrontierStallWatch frontierStall,
            Function<String, Map<String, Long>> nestDeadLetters) {
        this(state, observations, recordCounts, positions, snapshots, frontierGaps, nestStateReadings,
                coldLayer, frontierStalls, frontierStall, nestDeadLetters, id -> Map.of(), id -> Map.of());
    }

    /**
     * A publisher also wired to how far each large join rebuild has got: {@code joinRecomputeDone} yields
     * the rows already sent and {@code joinRecomputeExpected} about how many there are, both keyed by the
     * namespace and dimension key the rebuild is about (empty when none large enough to report is
     * running). The ninth and tenth ports, for the same reason as the others.
     *
     * <p>Two ports rather than one carrying both numbers, following the two frontier readings above: what
     * rides between here and the run is numbers by name, and a pair fetched as a pair would still have
     * arrived as two names. Nothing is lost by keeping them apart, because they are only ever read
     * together against each other, and a rebuild that reported one of them and not the other would be a
     * broken publish on either arrangement.
     */
    public ObservationPublisher(StateStore state, ObservationStore observations,
            Function<String, OptionalLong> recordCounts, Function<String, Map<String, String>> positions,
            Function<String, SnapshotReading> snapshots,
            Function<String, Map<String, Long>> frontierGaps,
            Function<String, Map<String, NestStateReading>> nestStateReadings,
            NestColdLayerWatch coldLayer,
            Function<String, Map<String, Long>> frontierStalls,
            FrontierStallWatch frontierStall,
            Function<String, Map<String, Long>> nestDeadLetters,
            Function<String, Map<String, Long>> joinRecomputeDone,
            Function<String, Map<String, Long>> joinRecomputeExpected) {
        this(state, observations, recordCounts, positions, snapshots, frontierGaps, nestStateReadings,
                coldLayer, frontierStalls, frontierStall, nestDeadLetters, joinRecomputeDone,
                joinRecomputeExpected, Clock.systemUTC());
    }

    /**
     * A publisher reading the observation time from {@code clock} rather than the system clock. Every
     * publish stamps when the projection was taken, so a read face can tell a run whose state has simply
     * not changed from one whose publisher stopped; a test drives that clock to witness the difference
     * without waiting for real time to pass.
     */
    public ObservationPublisher(StateStore state, ObservationStore observations,
            Function<String, OptionalLong> recordCounts, Function<String, Map<String, String>> positions,
            Function<String, SnapshotReading> snapshots,
            Function<String, Map<String, Long>> frontierGaps,
            Function<String, Map<String, NestStateReading>> nestStateReadings,
            NestColdLayerWatch coldLayer,
            Function<String, Map<String, Long>> frontierStalls,
            FrontierStallWatch frontierStall,
            Function<String, Map<String, Long>> nestDeadLetters,
            Function<String, Map<String, Long>> joinRecomputeDone,
            Function<String, Map<String, Long>> joinRecomputeExpected,
            Clock clock) {
        this(state, observations, recordCounts, positions, snapshots, frontierGaps, nestStateReadings,
                coldLayer, frontierStalls, frontierStall, nestDeadLetters, joinRecomputeDone,
                joinRecomputeExpected, id -> DeliveryReading.NONE, clock);
    }

    /**
     * A publisher also wired to what its sinks have delivered: {@code deliveries} yields the rows each
     * table has had confirmed by a target, broken out by source operation, the newest event time among
     * them, and the moment the counting began. The one port carries all three because none is readable
     * without the others.
     */
    public ObservationPublisher(StateStore state, ObservationStore observations,
            Function<String, OptionalLong> recordCounts, Function<String, Map<String, String>> positions,
            Function<String, SnapshotReading> snapshots,
            Function<String, Map<String, Long>> frontierGaps,
            Function<String, Map<String, NestStateReading>> nestStateReadings,
            NestColdLayerWatch coldLayer,
            Function<String, Map<String, Long>> frontierStalls,
            FrontierStallWatch frontierStall,
            Function<String, Map<String, Long>> nestDeadLetters,
            Function<String, Map<String, Long>> joinRecomputeDone,
            Function<String, Map<String, Long>> joinRecomputeExpected,
            Function<String, DeliveryReading> deliveries,
            Clock clock) {
        this(state, observations, recordCounts, positions, snapshots, frontierGaps, nestStateReadings,
                coldLayer, frontierStalls, frontierStall, nestDeadLetters, joinRecomputeDone,
                joinRecomputeExpected, id -> CaptureReading.NONE, deliveries, clock);
    }

    /**
     * A publisher wired to both ends of the crossing: {@code captures} yields what the pipeline's sources
     * have handed over, {@code deliveries} what its targets have confirmed.
     *
     * <p>Two ports and not one, because the two are read from different places and can be wired
     * independently — and because the difference between them is the reading that matters. Everything read
     * and not yet confirmed sits between the two totals, so a pipeline whose reading is healthy and whose
     * writing has stopped looks exactly like a healthy one on either number alone.
     */
    public ObservationPublisher(StateStore state, ObservationStore observations,
            Function<String, OptionalLong> recordCounts, Function<String, Map<String, String>> positions,
            Function<String, SnapshotReading> snapshots,
            Function<String, Map<String, Long>> frontierGaps,
            Function<String, Map<String, NestStateReading>> nestStateReadings,
            NestColdLayerWatch coldLayer,
            Function<String, Map<String, Long>> frontierStalls,
            FrontierStallWatch frontierStall,
            Function<String, Map<String, Long>> nestDeadLetters,
            Function<String, Map<String, Long>> joinRecomputeDone,
            Function<String, Map<String, Long>> joinRecomputeExpected,
            Function<String, CaptureReading> captures,
            Function<String, DeliveryReading> deliveries,
            Clock clock) {
        this(state, observations, recordCounts, positions, snapshots, frontierGaps, nestStateReadings,
                coldLayer, frontierStalls, frontierStall, nestDeadLetters, joinRecomputeDone,
                joinRecomputeExpected, captures, deliveries, id -> StageReading.NONE, clock);
    }

    /**
     * A publisher also wired to where the run spends its time: {@code stages} yields, per stage of the
     * graph, the distribution of how long its units of work took.
     */
    public ObservationPublisher(StateStore state, ObservationStore observations,
            Function<String, OptionalLong> recordCounts, Function<String, Map<String, String>> positions,
            Function<String, SnapshotReading> snapshots,
            Function<String, Map<String, Long>> frontierGaps,
            Function<String, Map<String, NestStateReading>> nestStateReadings,
            NestColdLayerWatch coldLayer,
            Function<String, Map<String, Long>> frontierStalls,
            FrontierStallWatch frontierStall,
            Function<String, Map<String, Long>> nestDeadLetters,
            Function<String, Map<String, Long>> joinRecomputeDone,
            Function<String, Map<String, Long>> joinRecomputeExpected,
            Function<String, CaptureReading> captures,
            Function<String, DeliveryReading> deliveries,
            Function<String, StageReading> stages,
            Clock clock) {
        this.captures = Objects.requireNonNull(captures, "captures");
        this.deliveries = Objects.requireNonNull(deliveries, "deliveries");
        this.stages = Objects.requireNonNull(stages, "stages");
        this.clock = Objects.requireNonNull(clock, "clock");
        // Read from the injected clock and not the system one, so a test that drives time can say what
        // the failure counter accumulates from instead of asserting against whenever it happened to run.
        this.countingFailuresSince = observedNow();
        this.joinRecomputeDone = Objects.requireNonNull(joinRecomputeDone, "joinRecomputeDone");
        this.joinRecomputeExpected =
                Objects.requireNonNull(joinRecomputeExpected, "joinRecomputeExpected");
        this.nestDeadLetters = Objects.requireNonNull(nestDeadLetters, "nestDeadLetters");
        this.state = Objects.requireNonNull(state, "state");
        this.observations = Objects.requireNonNull(observations, "observations");
        this.recordCounts = Objects.requireNonNull(recordCounts, "recordCounts");
        this.positions = Objects.requireNonNull(positions, "positions");
        this.snapshots = Objects.requireNonNull(snapshots, "snapshots");
        this.frontierGaps = Objects.requireNonNull(frontierGaps, "frontierGaps");
        this.nestStateReadings = Objects.requireNonNull(nestStateReadings, "nestStateReadings");
        this.coldLayer = Objects.requireNonNull(coldLayer, "coldLayer");
        this.frontierStalls = Objects.requireNonNull(frontierStalls, "frontierStalls");
        this.frontierStall = Objects.requireNonNull(frontierStall, "frontierStall");
    }

    /**
     * Publishes the pipeline's latest observation from its actual state; a no-op, answering empty, if it
     * has no checkpoint.
     */
    public Optional<Observation> publish(String pipelineId) {
        return publish(pipelineId, null);
    }

    /**
     * Publishes the pipeline's latest observation from its actual state, carrying {@code failure} as the
     * coded reason its run died ({@code null} while it is healthy); a no-op if it has no checkpoint. The
     * caller reports a failure only on the pass that witnesses the death; while the state stays FAILED,
     * later passes publish without one and the reason already stored is carried forward — the store is the
     * one carrier that survives a process restart, so a pipeline dead since before the restart keeps
     * saying why. Any other state publishes exactly what the caller passed, so a recovered pipeline drops
     * the reason its previous run died of the moment it leaves FAILED.
     */
    public Optional<Observation> publish(String pipelineId, ObservationFailure failure) {
        return prepare(pipelineId, failure).flatMap(prepared -> commit(prepared, null));
    }

    /** Publishes only under the execution identity acquired before this member started the run. */
    public Optional<Observation> publishScoped(String pipelineId, ObservationFailure failure,
            ObservationStore.Scope scope) {
        Objects.requireNonNull(scope, "scope");
        return prepareScoped(pipelineId, failure, scope).flatMap(prepared -> commit(prepared, scope));
    }

    /** Takes one frame and resets local run accounts when an execution identity changes. */
    public Optional<Prepared> prepareScoped(String pipelineId, ObservationFailure failure,
            ObservationStore.Scope scope) {
        Objects.requireNonNull(scope, "scope");
        ObservationStore.Scope previous = currentScopes.put(pipelineId, scope);
        if (!scope.equals(previous)) {
            currentFailures.remove(pipelineId);
            failuresByPipelineAndCode.remove(pipelineId);
            cardinality.forgetPipeline(pipelineId);
            failureCountingSinceByPipeline.put(pipelineId, observedNow());
        }
        return prepare(pipelineId, failure);
    }

    /** A single immutable measurement and its local alert inputs, before any telemetry store call. */
    public record Prepared(Observation observation, boolean inheritStoredFailure,
            Map<String, NestStateReading> nestReadings, Map<String, Long> pinned, Map<String, Long> gaps) {
        public Prepared {
            Objects.requireNonNull(observation, "observation");
            nestReadings = Map.copyOf(nestReadings);
            pinned = Map.copyOf(pinned);
            gaps = Map.copyOf(gaps);
        }
    }

    /** Takes the pipeline's current measurements without reading or writing any observation store. */
    public Optional<Prepared> prepare(String pipelineId, ObservationFailure failure) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        return state.read(pipelineId).map(checkpoint -> {
            PipelineState actual = StateJson.parse(checkpoint.stateJson());
            if (failure != null) {
                currentFailures.put(pipelineId, failure);
            } else if (actual != PipelineState.FAILED) {
                currentFailures.remove(pipelineId);
            }
            ObservationFailure carried = failure == null ? currentFailures.get(pipelineId) : failure;
            boolean inheritStoredFailure = carried == null && actual == PipelineState.FAILED;
            // Counted on the pass that witnesses it, which is the only pass handed a cause. A later pass
            // over a pipeline still FAILED is handed null and adds nothing, so one death is one count.
            if (failure != null) {
                failuresByPipelineAndCode
                        .computeIfAbsent(pipelineId, id -> new ConcurrentHashMap<>())
                        .merge(failure.code(), 1L, Long::sum);
            }
            Map<String, NestStateReading> readings = nestStateReadings.apply(pipelineId);
            // Both frontier readings are taken once and used twice - published as metrics and judged by
            // the watch. Asking each source again for the watch would pay for a second collection of the
            // run's statistics per pass, and would let the two answers disagree: what an operator reads
            // and what raised the alarm they are reading would then be different passes of the same run.
            Map<String, Long> gaps = frontierGaps.apply(pipelineId);
            Map<String, Long> pinned = frontierStalls.apply(pipelineId);
            // One instant for the whole pass, shared by the observation and by every point in it. Asking
            // the clock again per metric would stamp one pass with a spread of times, and a consumer
            // computing a rate across two passes would divide by a difference that is partly this
            // publisher's own loop.
            Instant at = observedNow();
            // The stored document carries these facts twice, from this one measurement: whole, as the
            // facts themselves, and as the flat numeric view of them. The flat view is a projection and it
            // drops what it cannot hold; what it drops is still on the document as a fact, so a reader who
            // finds a metric missing from the flat view finds it whole next to it rather than nowhere. What
            // the flat view drops today is pinned by this publisher's own test, so that the first metric it
            // cannot carry is a decision somebody makes rather than a metric that quietly fails to appear.
            // Taken once and used twice, like the frontier readings above: the load is published as two
            // metrics and as the observation's own snapshot dataset, and asking its source again for the
            // second use would let the two faces of one load describe different passes of it.
            SnapshotReading loaded = snapshots.apply(pipelineId);
            List<MetricFact> measured = facts(pipelineId, actual, at, readings, gaps, pinned,
                    nestDeadLetters.apply(pipelineId), joinRecomputeDone.apply(pipelineId),
                    joinRecomputeExpected.apply(pipelineId), loaded).stream().map(cardinality::fold).toList();
            Observation published = new Observation(pipelineId, actual,
                    FlatMetricProjection.of(measured, FLAT_REDUCTIONS).metrics(),
                    loaded.byTable(), positions.apply(pipelineId), carried, at, measured);
            return new Prepared(published, inheritStoredFailure, readings, pinned, gaps);
        });
    }

    /** Persists one previously measured frame; a stale scoped write produces no published frame. */
    public Optional<Observation> commit(Prepared prepared, ObservationStore.Scope scope) {
        Objects.requireNonNull(prepared, "prepared");
        Observation published = prepared.observation();
        if (prepared.inheritStoredFailure()) {
            ObservationFailure carried = previous(published.pipelineId(), scope)
                    .map(Observation::failure).orElse(null);
            if (carried != null) {
                published = new Observation(published.pipelineId(), published.state(), published.metrics(),
                        published.snapshot(), published.positions(), carried, published.observedAt(),
                        published.facts());
            }
        }
        if (!save(published, scope)) {
            return Optional.empty();
        }
            // Fed after the observation is written and never before. The observation is the contract and
            // the alert is a courtesy on top of it, so a fault in the alerting path must not be able to
            // cost a pipeline the read face that says it is alive at all.
            coldLayer.saw(published.pipelineId(), prepared.nestReadings());
            frontierStall.saw(published.pipelineId(), prepared.pinned(), prepared.gaps());
            // Handed back so that whoever runs the pass can take a sample off exactly what was published,
            // at the time it was published, rather than reading it back or measuring it again.
            return Optional.of(published);
    }

    private Optional<Observation> previous(String pipelineId, ObservationStore.Scope scope) {
        if (scope == null) {
            return observations.read(pipelineId);
        }
        return observations.readStored(pipelineId)
                .filter(stored -> stored.scope().filter(scope::equals).isPresent())
                .map(ObservationStore.Stored::observation);
    }

    private boolean save(Observation observation, ObservationStore.Scope scope) {
        if (scope != null) {
            return observations.saveScoped(observation, scope);
        }
        observations.save(observation);
        return true;
    }

    /**
     * Publishes a reconcile-failure observation for a pipeline whose converge pass keeps throwing and so
     * never reaches {@link #publish}. Without it the read face stays empty and "permanently broken" cannot be
     * told apart from "still converging". The consecutive-failure count rides out as errorCount; the last
     * observed lifecycle state, coded failure and source positions are all preserved as they last stood, since
     * a reconcile that could not run witnessed no transition in any of them — a pass that never ran did not
     * just recover either. The snapshot dataset is left unavailable: this path has no capture-side source to
     * read it from, unlike positions and failure which are simply carried forward from the last observation.
     */
    public void publishReconcileFailure(String pipelineId, long consecutiveFailures) {
        publishReconcileFailureInternal(pipelineId, consecutiveFailures, null);
    }

    /** Keeps an error projection from a failed pass within the same execution identity. */
    public void publishReconcileFailureScoped(String pipelineId, long consecutiveFailures,
            ObservationStore.Scope scope) {
        publishReconcileFailureInternal(pipelineId, consecutiveFailures, Objects.requireNonNull(scope, "scope"));
    }

    private void publishReconcileFailureInternal(String pipelineId, long consecutiveFailures,
            ObservationStore.Scope scope) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Observation previous = previous(pipelineId, scope).orElse(null);
        PipelineState lastState = previous != null ? previous.state() : PipelineState.NEW;
        Map<String, String> lastPositions = previous != null ? previous.positions() : Map.of();
        ObservationFailure lastFailure = previous != null ? previous.failure() : null;
        Instant at = observedNow();
        List<MetricFact> measured = List.of(
                readAt(pipelineId, RECONCILE_STREAK_METRIC, "{pass}", at, consecutiveFailures));
        save(new Observation(pipelineId, lastState,
                FlatMetricProjection.of(measured, FLAT_REDUCTIONS).metrics(),
                null, lastPositions, lastFailure, at, measured), scope);
    }

    /**
     * When this projection is being taken, truncated to milliseconds. The store keeps it as a BSON date,
     * which is millisecond precision, so truncating here rather than on the way out keeps the value a
     * caller holds identical to the one that comes back — nanoseconds that only exist until the first
     * round trip would make two equal observations compare unequal depending on where they were read.
     */
    private Instant observedNow() {
        return Instant.now(clock).truncatedTo(ChronoUnit.MILLIS);
    }

    /**
     * One metric read at {@code at} about the pipeline as a whole: one point, carrying the pipeline as its
     * only attribute, the way every canonical fact names the pipeline it is about.
     */
    private static MetricFact readAt(String pipelineId, String name, String unit, Instant at, long value) {
        return MetricFact.single(name, MetricType.GAUGE, unit,
                MetricPoint.reading(Map.of(PIPELINE_ID_ATTRIBUTE, pipelineId), at, value));
    }

    /**
     * One metric read at {@code at} with one point per value of {@code dimension}, each point carrying the
     * pipeline and that value as attributes; empty when nothing reported, so that a family with no reading
     * stays absent rather than present with no points. The dimension is an attribute here and a suffix on
     * the flat face, and {@link #FLAT_REDUCTIONS} is what keeps the two spellings of one reading in step.
     */
    private static Optional<MetricFact> readingsAt(String pipelineId, String name, String unit, Instant at,
            String dimension, Map<String, Long> byValue) {
        if (byValue.isEmpty()) {
            return Optional.empty();
        }
        List<MetricPoint> points = new ArrayList<>();
        byValue.forEach((value, reading) -> points.add(MetricPoint.reading(
                Map.of(PIPELINE_ID_ATTRIBUTE, pipelineId, dimension, value), at, reading)));
        return Optional.of(new MetricFact(name, MetricType.GAUGE, unit, points));
    }

    /**
     * The run statistics for the pipeline, as the internal facts every metric consumer projects from:
     * errorCount is derived from the actual state (a FAILED job is one observable error, else zero) and is
     * always present; recordCount is added from its source only when a live job reports one, so its
     * absence reads as "not wired" rather than a zero count. Naming each one's unit is the point of
     * assembling facts rather than bare numbers: a duration published as a number is a duration somebody
     * eventually thresholds in the wrong unit, and two of these are durations.
     *
     * <p><strong>Every fact here is a value read at this tick, and none of them accumulates.</strong>
     * Several of the underlying quantities do accumulate inside the engine - accesses, backfills, discards,
     * rows rebuilt - but nothing records what they accumulate from. Declaring them as accumulating without
     * that start would hand every consumer a stream in which a restart and a decrease are the same
     * observation; taking the start from this clock would invent one, and it would be wrong from the first
     * restart onward. So they are readings until the engine reports a start alongside them. That is a gap
     * in what is measured, and describing them as anything else here would only hide it.
     *
     * <p>The per-chain and per-namespace families carry their dimension as an attribute, one fact per family
     * with a point per chain or namespace; the flat face spells each point back out under the key it has
     * always used, so nothing that reads that face moved. The rebuild pair is keyed by the namespace its
     * dimension lives in and not by the row it is about: that key is a value out of a row, which no
     * attribute may carry, so the engine adds the rebuilds of one namespace together before they get here.
     */
    List<MetricFact> facts(String pipelineId, PipelineState actual, Instant at,
            Map<String, NestStateReading> nestReadings, Map<String, Long> gaps, Map<String, Long> pinned,
            Map<String, Long> discarded, Map<String, Long> rebuildDone, Map<String, Long> rebuildExpected,
            SnapshotReading loaded) {
        List<MetricFact> facts = new ArrayList<>();
        failures(pipelineId, at).ifPresent(facts::add);
        recordCounts.apply(pipelineId)
                .ifPresent(count -> facts.add(readAt(pipelineId, RECORDS_DRIVEN_METRIC, "{record}", at, count)));
        // One point per chain that reported a reading, so a chain keeping up and a chain that has stalled
        // stay distinguishable; a chain that reported none is absent rather than zero, which would read as
        // the healthy end of the same scale. The distance is dimensionless: what it counts is a position's
        // worth of ground, which is neither a row nor a second, and naming it either would be a guess a
        // reader would then threshold on.
        readingsAt(pipelineId, FRONTIER_GAP_METRIC, "1", at, CHAIN_ID_ATTRIBUTE, gaps).ifPresent(facts::add);
        // One point per chain that is pinned, and none for a chain that is not. The two readings do not
        // cover the same chains and neither is the other's default: a chain that caught up has a distance
        // and no pin, and a chain that never advanced has a pin and no distance.
        readingsAt(pipelineId, FRONTIER_STALL_METRIC, "ms", at, CHAIN_ID_ATTRIBUTE, pinned).ifPresent(facts::add);
        // One point per namespace that reported, so a resolver thrashing against its cold layer stays
        // distinguishable from an assembler that is not; a namespace reporting nothing is absent rather
        // than present at zero, which would read as a state layer that had emptied. Six quantities, six
        // facts: each is its own instrument, read against the others rather than added to them.
        Map<String, Long> entries = new LinkedHashMap<>();
        Map<String, Long> accesses = new LinkedHashMap<>();
        Map<String, Long> backfills = new LinkedHashMap<>();
        Map<String, Long> backfillMillis = new LinkedHashMap<>();
        Map<String, Long> pendingHighWater = new LinkedHashMap<>();
        Map<String, Long> stored = new LinkedHashMap<>();
        nestReadings.forEach((namespace, reading) -> {
            entries.put(namespace, reading.entries());
            accesses.put(namespace, reading.accesses());
            backfills.put(namespace, reading.backfills());
            backfillMillis.put(namespace, reading.backfillMillis());
            pendingHighWater.put(namespace, reading.pendingHighWater());
            reading.stored().ifPresent(whole -> stored.put(namespace, whole));
        });
        readingsAt(pipelineId, NEST_ENTRIES_METRIC, "{entry}", at, NEST_NAMESPACE_ATTRIBUTE, entries)
                .ifPresent(facts::add);
        readingsAt(pipelineId, NEST_ACCESSES_METRIC, "{access}", at, NEST_NAMESPACE_ATTRIBUTE, accesses)
                .ifPresent(facts::add);
        readingsAt(pipelineId, NEST_BACKFILLS_METRIC, "{backfill}", at, NEST_NAMESPACE_ATTRIBUTE, backfills)
                .ifPresent(facts::add);
        readingsAt(pipelineId, NEST_BACKFILL_TIME_METRIC, "ms", at, NEST_NAMESPACE_ATTRIBUTE, backfillMillis)
                .ifPresent(facts::add);
        readingsAt(pipelineId, NEST_PENDING_HIGH_WATER_METRIC, "{record}", at, NEST_NAMESPACE_ATTRIBUTE,
                pendingHighWater).ifPresent(facts::add);
        readingsAt(pipelineId, NEST_STORED_METRIC, "{entry}", at, NEST_NAMESPACE_ATTRIBUTE, stored)
                .ifPresent(facts::add);
        // One point per namespace that discarded something, and none for a namespace that discarded
        // nothing. The absence is load-bearing here rather than merely tidy: rows that never reach a
        // document leave no other trace, so a reader has only this to go on, and a zero published on every
        // pass is what an unwired count would look like too.
        readingsAt(pipelineId, NEST_DEAD_LETTERED_METRIC, "{change}", at, NEST_NAMESPACE_ATTRIBUTE, discarded)
                .ifPresent(facts::add);
        // One pair per namespace with a rebuild large enough to be worth telling anybody about, and nothing
        // at all for a pipeline where none is running. Both halves are published from their own map rather
        // than one being defaulted from the other: a rebuild whose size arrived without its progress would
        // read as one that has sent no rows, which is the shape of a rebuild that is stuck.
        readingsAt(pipelineId, JOIN_RECOMPUTE_ROWS_METRIC, "{row}", at, JOIN_NAMESPACE_ATTRIBUTE, rebuildDone)
                .ifPresent(facts::add);
        readingsAt(pipelineId, JOIN_RECOMPUTE_ROWS_TOTAL_METRIC, "{row}", at, JOIN_NAMESPACE_ATTRIBUTE,
                rebuildExpected).ifPresent(facts::add);
        movement(pipelineId, at, captures.apply(pipelineId), deliveries.apply(pipelineId))
                .forEach(facts::add);
        spent(pipelineId, at, stages.apply(pipelineId)).ifPresent(facts::add);
        load(pipelineId, at, loaded).forEach(facts::add);
        return facts;
    }

    /**
     * Where the run's time went, as one distribution per stage; empty for a run reporting none. The stage
     * is a closed set, so every point here is one of five and nothing arriving from the data can add one.
     */
    private static Optional<MetricFact> spent(String pipelineId, Instant at, StageReading spent) {
        if (spent == null || spent.isEmpty() || spent.start().isEmpty()) {
            return Optional.empty();
        }
        List<MetricPoint> points = new ArrayList<>();
        spent.durationByStage().forEach((stage, histogram) -> points.add(MetricPoint.distribution(
                Map.of(PIPELINE_ID_ATTRIBUTE, pipelineId, STAGE_ATTRIBUTE, stage), spent.start().get(), at,
                histogram)));
        return Optional.of(new MetricFact(PROCESS_DURATION_METRIC, MetricType.HISTOGRAM, HistogramBounds.UNIT, points));
    }

    /**
     * How many failures this pipeline has had, by code, or empty for one that has had none.
     *
     * <p>Absent rather than present at zero, like every other quantity here whose quiet state and whose
     * unwired state would otherwise be spelled the same way. What that costs is worth naming: a reader
     * cannot tell "nothing has failed" from "nobody is counting", and the flat face this reduces onto
     * gives them the same empty answer for both. The alternative costs more - a zero published for every
     * code nothing has produced is a row per code per pipeline, and the set of codes a connector can
     * contribute is open.
     */
    private Optional<MetricFact> failures(String pipelineId, Instant at) {
        Map<String, Long> byCode = failuresByPipelineAndCode.get(pipelineId);
        if (byCode == null || byCode.isEmpty()) {
            return Optional.empty();
        }
        List<MetricPoint> counted = new ArrayList<>();
        Instant since = failureCountingSinceByPipeline.getOrDefault(pipelineId, countingFailuresSince);
        byCode.forEach((code, count) -> counted.add(MetricPoint.accumulated(
                Map.of(PIPELINE_ID_ATTRIBUTE, pipelineId, CODE_ATTRIBUTE, code),
                since, at, count)));
        return Optional.of(new MetricFact(ERRORS_METRIC, MetricType.COUNTER, "{error}", counted));
    }

    /**
     * Drops the failure account of every pipeline outside {@code live}, which is the set that still has a
     * stored intent. Called once per convergence pass by the side that already has that set in hand.
     *
     * <p><strong>A pipeline leaves that set only when it is deleted.</strong> An intent is removed by the
     * reclaim of the pipeline itself and by nothing else, so this forgets a pipeline that no longer
     * exists and never one that is merely stopped - a stopped pipeline keeps its intent, and keeping its
     * count is the point: the gap either side of a stop is the stretch somebody most wants to compare.
     *
     * <p>It is swept from here rather than cleared by whoever deletes the pipeline because nobody on that
     * side may reach in: the control ring's synchronous surface into this one is a closed set, and a
     * seventh way in would have to widen it. The set that is already crossing between them once a tick
     * carries the same answer.
     */
    public void forgetPipelinesOutside(Collection<String> live) {
        Objects.requireNonNull(live, "live");
        failuresByPipelineAndCode.keySet().retainAll(Set.copyOf(live));
        currentFailures.keySet().retainAll(Set.copyOf(live));
        currentScopes.keySet().retainAll(Set.copyOf(live));
        failureCountingSinceByPipeline.keySet().retainAll(Set.copyOf(live));
        cardinality.forgetPipelinesOutside(live);
    }

    /**
     * The facts a pipeline's bounded load makes: how many rows of each table it read, and about how many
     * each of those tables was last counted to hold. Empty for a pipeline that ran no load, so one reading
     * a change stream alone is absent rather than present at zero rows.
     *
     * <p>Both come from the reading the observation's snapshot dataset is built from, on the same pass, so
     * a monitoring backend and the read face can never be describing different moments of the same load.
     *
     * <p>The total is left out per table rather than for the reading as a whole. A pipeline may read one
     * table off a connector that can count and another off one that cannot, and a total defaulted for the
     * second would size an unmeasured table at whatever the default was. Nothing here turns an absent
     * count into a zero or into the rows already done: both would read as a finished load.
     */
    private static List<MetricFact> load(String pipelineId, Instant at, SnapshotReading loaded) {
        if (loaded == null || loaded.start().isEmpty()) {
            return List.of();
        }
        Instant start = loaded.countingSince();
        List<MetricPoint> done = new ArrayList<>();
        List<MetricPoint> expected = new ArrayList<>();
        loaded.byTable().forEach((table, progress) -> {
            Map<String, String> attributes =
                    Map.of(PIPELINE_ID_ATTRIBUTE, pipelineId, TABLE_ID_ATTRIBUTE, table);
            done.add(MetricPoint.accumulated(attributes, start, at, progress.rowsDone()));
            if (progress.rowsTotal() != null) {
                expected.add(MetricPoint.reading(attributes, at, progress.rowsTotal()));
            }
        });
        List<MetricFact> facts = new ArrayList<>();
        if (!done.isEmpty()) {
            facts.add(new MetricFact(SNAPSHOT_ROWS_METRIC, MetricType.COUNTER, "{row}", done));
        }
        if (!expected.isEmpty()) {
            facts.add(new MetricFact(SNAPSHOT_ROWS_TOTAL_METRIC, MetricType.GAUGE, "{row}", expected));
        }
        return facts;
    }

    /**
     * The facts a pipeline's movement makes: how many rows of each table and operation crossed each end of
     * it, and how old the newest row of each table to reach a target is. Empty for a pipeline reporting
     * nothing on both ends, so a pipeline that is not running is absent rather than present at zero.
     *
     * <p>The two directions are one measurement with an attribute telling them apart, not two measurements.
     * What a reader wants from them is a comparison — everything read and not yet confirmed is the
     * difference — and two names could be defined, counted or published differently without anything
     * noticing.
     *
     * <p>Each direction carries its own start, because they are not counted from the same moment: the
     * capture opens its account when the run's sources are opened, and the targets theirs when the job that
     * writes to them starts, which is strictly later. One start over both would misdate whichever it was
     * not taken from.
     *
     * <p>Emptiness is decided per fact and per direction, never once up front. A run may have a start and
     * nothing counted, rows on one end and none on the other, or rows for one table and a recency reading
     * for another; each is left out on its own account, and a check covering several would be a second
     * place for the same decision to be made.
     */
    private List<MetricFact> movement(
            String pipelineId, Instant at, CaptureReading captured, DeliveryReading delivered) {
        List<MetricFact> facts = new ArrayList<>();
        List<MetricPoint> rows = new ArrayList<>();
        List<MetricPoint> payload = new ArrayList<>();
        if (captured != null) {
            captured.start().ifPresent(start -> {
                crossings(pipelineId, INBOUND, captured.rowsByTableAndOp(), start, at, rows);
                carried(pipelineId, INBOUND, captured.bytesByTable(), start, at, payload);
            });
        }
        if (delivered != null) {
            delivered.start().ifPresent(start -> {
                crossings(pipelineId, OUTBOUND, delivered.rowsByTableAndOp(), start, at, rows);
                carried(pipelineId, OUTBOUND, delivered.bytesByTable(), start, at, payload);
            });
        }
        if (!rows.isEmpty()) {
            facts.add(new MetricFact(RECORDS_METRIC, MetricType.COUNTER, "{record}", rows));
        }
        if (!payload.isEmpty()) {
            facts.add(new MetricFact(BYTES_METRIC, MetricType.COUNTER, "By", payload));
        }
        if (delivered == null) {
            return facts;
        }
        List<MetricPoint> ages = new ArrayList<>();
        delivered.newestEventTimeByTable().forEach((table, eventTime) -> ages.add(MetricPoint.reading(
                Map.of(PIPELINE_ID_ATTRIBUTE, pipelineId, TABLE_ID_ATTRIBUTE, table),
                at, ageInSeconds(at, eventTime))));
        if (!ages.isEmpty()) {
            facts.add(new MetricFact(LAG_METRIC, MetricType.GAUGE, "s", ages));
        }
        // The distribution accumulates from the same start as the counts: it is over the same rows.
        delivered.start().ifPresent(start -> {
            List<MetricPoint> took = new ArrayList<>();
            delivered.deliveryDurationByTable().forEach((table, histogram) -> took.add(MetricPoint.distribution(
                    Map.of(PIPELINE_ID_ATTRIBUTE, pipelineId, TABLE_ID_ATTRIBUTE, table), start, at, histogram)));
            if (!took.isEmpty()) {
                facts.add(new MetricFact(RECORD_DELIVERY_DURATION_METRIC, MetricType.HISTOGRAM,
                        HistogramBounds.UNIT, took));
            }
        });
        return facts;
    }

    /**
     * Adds one accumulating point per attribute set for what crossed one end of the pipeline.
     *
     * <p>Summed into one point per attribute set, not one per symbol the run reported. The name map is not
     * injective: every symbol it does not recognise lands on one name, so two unrecognised kinds reaching
     * one table produce two points carrying identical attributes — two values of one series, which a fact
     * refuses outright. Adding them is what the counter means: both are rows that crossed.
     */
    private static void crossings(String pipelineId, String direction,
            Map<String, Map<String, Long>> rowsByTableAndOp, Instant start, Instant at,
            List<MetricPoint> into) {
        Map<Map<String, String>, Long> byAttributes = new LinkedHashMap<>();
        rowsByTableAndOp.forEach((table, byOp) -> byOp.forEach((symbol, count) ->
                byAttributes.merge(Map.of(
                        PIPELINE_ID_ATTRIBUTE, pipelineId,
                        TABLE_ID_ATTRIBUTE, table,
                        DIRECTION_ATTRIBUTE, direction,
                        OP_ATTRIBUTE, OP_NAMES.getOrDefault(symbol, "other")),
                        count, Long::sum)));
        byAttributes.forEach(
                (attributes, count) -> into.add(MetricPoint.accumulated(attributes, start, at, count)));
    }

    /**
     * Adds one accumulating point per table for the payload that crossed one end of the pipeline.
     *
     * <p>No summing step, unlike the crossings above: what arrives here is already one figure per table,
     * because the operation a row came from is not a dimension of this measurement. Two points carrying
     * identical attributes cannot arise, so there is nothing for an attribute-keyed merge to do and a
     * merge written anyway would be a step no case could enter.
     *
     * <p>It shares the start with the counts taken at the same end, which is what makes the pair
     * divisible: a bytes-per-row worked out from two totals accumulated from different moments is not a
     * figure about any window at all.
     */
    private static void carried(String pipelineId, String direction,
            Map<String, Long> bytesByTable, Instant start, Instant at, List<MetricPoint> into) {
        bytesByTable.forEach((table, bytes) -> into.add(MetricPoint.accumulated(Map.of(
                PIPELINE_ID_ATTRIBUTE, pipelineId,
                TABLE_ID_ATTRIBUTE, table,
                DIRECTION_ATTRIBUTE, direction),
                start, at, bytes)));
    }

    /**
     * How old {@code eventTimeMillis} is at {@code at}, in whole seconds. Worked out here and not in the
     * run, because it goes on growing while nothing arrives: a distance recorded where the row settled
     * would stand still for exactly as long as a pipeline did, and read as healthy throughout.
     *
     * <p><strong>This is the age of the last row that landed, which is not how far the source is ahead.</strong>
     * A source with nothing new to send drives this up while the pipeline is perfectly caught up, and
     * telling the two apart needs something nobody here has: where the source's own log now ends.
     *
     * <p>A source clock running ahead of ours reads as nought rather than as a negative age. An age below
     * zero is not a state anything can be in, and publishing one would have every reader decide for
     * themselves what it meant.
     */
    private static long ageInSeconds(Instant at, long eventTimeMillis) {
        return Math.max(0L, (at.toEpochMilli() - eventTimeMillis) / 1000L);
    }
}
