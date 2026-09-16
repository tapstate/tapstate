package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.DeliveryReading;
import io.tapstate.core.lifecycle.FlatMetricProjection;
import io.tapstate.core.lifecycle.FlatReduction;
import io.tapstate.core.lifecycle.FrontierStallPressure;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.NestColdLayerPressure;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StateStore;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.Function;

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
     * How far a rebuild of one dimension key's fan-out has got, and about how far it has to go, with the
     * namespace and the key it is about appended. Two numbers because the whole of what a rebuild says is
     * the distance between them; either alone is a number with nothing to be read against.
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
     * The two measurements that carry their dimensions as attributes rather than in their names. Their
     * names are the canonical ones a monitoring backend sees; how the flat face below spells them is that
     * face's business and is decided in {@link #FLAT_REDUCTIONS}.
     */
    private static final String RECORDS_METRIC = "tapstate.pipeline.records";
    private static final String LAG_METRIC = "tapstate.pipeline.lag";

    private static final String PIPELINE_ID_ATTRIBUTE = "tapstate.pipeline.id";
    private static final String TABLE_ID_ATTRIBUTE = "tapstate.table.id";
    private static final String DIRECTION_ATTRIBUTE = "direction";
    private static final String OP_ATTRIBUTE = "op";
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
    private static final Map<String, FlatReduction> FLAT_REDUCTIONS = Map.of(
            RECORDS_METRIC, attributes -> "records." + attributes.get(DIRECTION_ATTRIBUTE),
            LAG_METRIC, attributes -> "lag." + attributes.get(TABLE_ID_ATTRIBUTE));

    /**
     * The name the metric contract gives each of this engine's change kinds. Its set is closed at five and
     * so is the engine's, but they are not the same five: a snapshot read is a source operation the
     * contract has no name of its own for, so it lands under the name the contract keeps for exactly that
     * — rather than being given one here, which would be this layer deciding a contract it implements.
     */
    private static final Map<String, String> OP_NAMES = Map.of(
            "i", "insert", "u", "update", "d", "delete", "ddl", "ddl", "r", "other");

    private final StateStore state;
    private final ObservationStore observations;
    private final Function<String, OptionalLong> recordCounts;
    private final Function<String, Map<String, String>> positions;
    private final Function<String, Map<String, TableSnapshot>> snapshots;
    private final Function<String, Map<String, Long>> frontierGaps;
    private final Function<String, Map<String, NestStateReading>> nestStateReadings;
    private final Function<String, Map<String, Long>> frontierStalls;
    private final Function<String, Map<String, Long>> nestDeadLetters;
    private final Function<String, Map<String, Long>> joinRecomputeDone;
    private final Function<String, Map<String, Long>> joinRecomputeExpected;
    private final Function<String, DeliveryReading> deliveries;
    private final FrontierStallWatch frontierStall;
    private final NestColdLayerWatch coldLayer;
    private final Clock clock;

    /**
     * A publisher with no metric, position or snapshot source: recordCount stays absent and positions and
     * snapshot stay empty, so it carries the state-derived errorCount alone. This is the shape callers used
     * before those sources were wired; the assembly point injects the real sources through the full
     * constructor.
     */
    public ObservationPublisher(StateStore state, ObservationStore observations) {
        this(state, observations, id -> OptionalLong.empty(), id -> Map.of(), id -> Map.of(), id -> Map.of());
    }

    /** A publisher wired to its metric and position sources but with no snapshot or frontier source. */
    public ObservationPublisher(StateStore state, ObservationStore observations,
            Function<String, OptionalLong> recordCounts, Function<String, Map<String, String>> positions) {
        this(state, observations, recordCounts, positions, id -> Map.of(), id -> Map.of());
    }

    /**
     * A publisher wired to its live run-statistic sources: {@code recordCounts} yields the records a
     * pipeline's live job has driven to its sinks (empty when it has no live job), {@code positions}
     * yields the durable per-table sink-acked source positions (empty when none), and {@code snapshots}
     * yields the per-table initial-load progress (empty when no table has been loaded). All three are
     * ports so the scheduler stays clear of the engine, the store and the capture side that back them.
     */
    public ObservationPublisher(StateStore state, ObservationStore observations,
            Function<String, OptionalLong> recordCounts, Function<String, Map<String, String>> positions,
            Function<String, Map<String, TableSnapshot>> snapshots) {
        this(state, observations, recordCounts, positions, snapshots, id -> Map.of(), id -> Map.of());
    }

    /**
     * A publisher also wired to the per-chain frontier readings: {@code frontierGaps} yields how far each
     * chain of a pipeline's live run trails the bound combined for it (empty when nothing reports one).
     * A fourth port for the same reason as the other three - the scheduler stays clear of what backs them.
     */
    public ObservationPublisher(StateStore state, ObservationStore observations,
            Function<String, OptionalLong> recordCounts, Function<String, Map<String, String>> positions,
            Function<String, Map<String, TableSnapshot>> snapshots,
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
            Function<String, Map<String, TableSnapshot>> snapshots,
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
            Function<String, Map<String, TableSnapshot>> snapshots,
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
            Function<String, Map<String, TableSnapshot>> snapshots,
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
            Function<String, Map<String, TableSnapshot>> snapshots,
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
            Function<String, Map<String, TableSnapshot>> snapshots,
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
            Function<String, Map<String, TableSnapshot>> snapshots,
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
            Function<String, Map<String, TableSnapshot>> snapshots,
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
            Function<String, Map<String, TableSnapshot>> snapshots,
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
        this.deliveries = Objects.requireNonNull(deliveries, "deliveries");
        this.clock = Objects.requireNonNull(clock, "clock");
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

    /** Publishes the pipeline's latest observation from its actual state; a no-op if it has no checkpoint. */
    public void publish(String pipelineId) {
        publish(pipelineId, null);
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
    public void publish(String pipelineId, ObservationFailure failure) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        state.read(pipelineId).ifPresent(checkpoint -> {
            PipelineState actual = StateJson.parse(checkpoint.stateJson());
            ObservationFailure carried = failure;
            if (carried == null && actual == PipelineState.FAILED) {
                carried = observations.read(pipelineId).map(Observation::failure).orElse(null);
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
            // The stored document carries the flat numeric view of these facts. It is a projection and it
            // drops what it cannot hold; nothing it drops today, which is what this publisher's own test
            // pins, so that the first metric with dimensions is a decision somebody makes rather than a
            // metric that quietly fails to appear on this face.
            List<MetricFact> measured = facts(pipelineId, actual, at, readings, gaps, pinned,
                    nestDeadLetters.apply(pipelineId), joinRecomputeDone.apply(pipelineId),
                    joinRecomputeExpected.apply(pipelineId));
            observations.save(new Observation(pipelineId, actual,
                    FlatMetricProjection.of(measured, FLAT_REDUCTIONS).metrics(),
                    snapshots.apply(pipelineId), positions.apply(pipelineId), carried, at));
            // Fed after the observation is written and never before. The observation is the contract and
            // the alert is a courtesy on top of it, so a fault in the alerting path must not be able to
            // cost a pipeline the read face that says it is alive at all.
            coldLayer.saw(pipelineId, readings);
            frontierStall.saw(pipelineId, pinned, gaps);
        });
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
        Objects.requireNonNull(pipelineId, "pipelineId");
        Observation previous = observations.read(pipelineId).orElse(null);
        PipelineState lastState = previous != null ? previous.state() : PipelineState.NEW;
        Map<String, String> lastPositions = previous != null ? previous.positions() : Map.of();
        ObservationFailure lastFailure = previous != null ? previous.failure() : null;
        Instant at = observedNow();
        observations.save(new Observation(pipelineId, lastState,
                FlatMetricProjection.of(List.of(readAt("errorCount", "{error}", at, consecutiveFailures)))
                        .metrics(),
                null, lastPositions, lastFailure, at));
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
     * One metric read at {@code at}, with no dimensions broken out of its name. Every statistic this
     * publisher produces is of that shape today, which is why the helper offers no other.
     */
    private static MetricFact readAt(String name, String unit, Instant at, long value) {
        return MetricFact.single(name, MetricType.GAUGE, unit, MetricPoint.reading(Map.of(), at, value));
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
     * <p>Each family still carries its one dimension inside its name, appended to a fixed prefix, which is
     * why every point below is attribute-free. That is what a flat numeric face can carry; the facts exist
     * first so that changing it has one place to happen instead of one per consumer.
     */
    List<MetricFact> facts(String pipelineId, PipelineState actual, Instant at,
            Map<String, NestStateReading> nestReadings, Map<String, Long> gaps, Map<String, Long> pinned,
            Map<String, Long> discarded, Map<String, Long> rebuildDone, Map<String, Long> rebuildExpected) {
        List<MetricFact> facts = new ArrayList<>();
        facts.add(readAt("errorCount", "{error}", at, actual == PipelineState.FAILED ? 1L : 0L));
        recordCounts.apply(pipelineId)
                .ifPresent(count -> facts.add(readAt("recordCount", "{record}", at, count)));
        // One fact per chain that reported a reading, so a chain keeping up and a chain that has stalled
        // stay distinguishable; a chain that reported none is absent rather than zero, which would read as
        // the healthy end of the same scale. The distance is dimensionless: what it counts is a position's
        // worth of ground, which is neither a row nor a second, and naming it either would be a guess a
        // reader would then threshold on.
        gaps.forEach((chain, gap) -> facts.add(readAt(FRONTIER_GAP_PREFIX + chain, "1", at, gap)));
        // One fact per chain that is pinned, and none for a chain that is not. The two readings do not
        // cover the same chains and neither is the other's default: a chain that caught up has a distance
        // and no pin, and a chain that never advanced has a pin and no distance.
        pinned.forEach(
                (chain, millis) -> facts.add(readAt(FRONTIER_STALLED_PREFIX + chain, "ms", at, millis)));
        // One set per namespace that reported, so a resolver thrashing against its cold layer stays
        // distinguishable from an assembler that is not; a namespace reporting nothing is absent rather
        // than present at zero, which would read as a state layer that had emptied.
        nestReadings.forEach((namespace, reading) -> {
            facts.add(readAt(NEST_ENTRIES_PREFIX + namespace, "{entry}", at, reading.entries()));
            facts.add(readAt(NEST_ACCESSES_PREFIX + namespace, "{access}", at, reading.accesses()));
            facts.add(readAt(NEST_BACKFILLS_PREFIX + namespace, "{backfill}", at, reading.backfills()));
            facts.add(readAt(NEST_BACKFILL_MILLIS_PREFIX + namespace, "ms", at, reading.backfillMillis()));
            facts.add(readAt(NEST_PENDING_HIGH_WATER_PREFIX + namespace, "{record}", at,
                    reading.pendingHighWater()));
            reading.stored().ifPresent(
                    stored -> facts.add(readAt(NEST_STORED_PREFIX + namespace, "{entry}", at, stored)));
        });
        // One fact per namespace that discarded something, and none for a namespace that discarded
        // nothing. The absence is load-bearing here rather than merely tidy: rows that never reach a
        // document leave no other trace, so a reader has only this to go on, and a zero published on every
        // pass is what an unwired count would look like too.
        discarded.forEach((namespace, count) -> facts.add(
                readAt(NEST_DEAD_LETTERED_PREFIX + namespace, "{change}", at, count)));
        // One pair per rebuild large enough to be worth telling anybody about, and nothing at all for a
        // pipeline where none is running. Both halves are published from their own map rather than one
        // being defaulted from the other: a rebuild whose size arrived without its progress would read as
        // one that has sent no rows, which is the shape of a rebuild that is stuck.
        rebuildDone.forEach((subject, rows) -> facts.add(
                readAt(JOIN_RECOMPUTE_DONE_PREFIX + subject, "{row}", at, rows)));
        rebuildExpected.forEach((subject, rows) -> facts.add(
                readAt(JOIN_RECOMPUTE_EXPECTED_PREFIX + subject, "{row}", at, rows)));
        delivered(pipelineId, at, deliveries.apply(pipelineId)).forEach(facts::add);
        return facts;
    }

    /**
     * The two facts a run's deliveries make: how many rows of each table and operation reached a target,
     * and how old the newest row of each table is. Empty for a pipeline whose run reports nothing, so a
     * pipeline that is not running is absent from both rather than present at zero.
     */
    private List<MetricFact> delivered(String pipelineId, Instant at, DeliveryReading reading) {
        if (reading == null || reading.isEmpty()) {
            return List.of();
        }
        List<MetricFact> facts = new ArrayList<>();
        // The one measurement here that accumulates, and the first that can: the run reports what its
        // totals count from, so the stream has the start identity every other family on this face still
        // lacks. Without it this would have to be a reading like the rest.
        reading.start().ifPresent(start -> {
            List<MetricPoint> rows = new ArrayList<>();
            reading.rowsByTableAndOp().forEach((table, byOp) -> byOp.forEach((symbol, count) ->
                    rows.add(MetricPoint.accumulated(Map.of(
                            PIPELINE_ID_ATTRIBUTE, pipelineId,
                            TABLE_ID_ATTRIBUTE, table,
                            DIRECTION_ATTRIBUTE, OUTBOUND,
                            OP_ATTRIBUTE, OP_NAMES.getOrDefault(symbol, "other")),
                            start, at, count))));
            if (!rows.isEmpty()) {
                facts.add(new MetricFact(RECORDS_METRIC, MetricType.COUNTER, "{record}", rows));
            }
        });
        List<MetricPoint> ages = new ArrayList<>();
        reading.newestEventTimeByTable().forEach((table, eventTime) -> ages.add(MetricPoint.reading(
                Map.of(PIPELINE_ID_ATTRIBUTE, pipelineId, TABLE_ID_ATTRIBUTE, table),
                at, ageInSeconds(at, eventTime))));
        if (!ages.isEmpty()) {
            facts.add(new MetricFact(LAG_METRIC, MetricType.GAUGE, "s", ages));
        }
        return facts;
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
