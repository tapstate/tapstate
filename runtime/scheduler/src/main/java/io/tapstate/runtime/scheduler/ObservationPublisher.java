package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StateStore;

import java.util.HashMap;
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
     * How much the namespace holds altogether, against the entries above which are only what is in memory.
     * Published where there is a layer behind the memory to ask and left out where there is not: a run
     * holding its state in memory alone has no second number, and one reported anyway would be the first
     * number wearing the name of the second.
     */
    private static final String NEST_STORED_PREFIX = "nestStateStored.";

    private final StateStore state;
    private final ObservationStore observations;
    private final Function<String, OptionalLong> recordCounts;
    private final Function<String, Map<String, String>> positions;
    private final Function<String, Map<String, TableSnapshot>> snapshots;
    private final Function<String, Map<String, Long>> frontierGaps;
    private final Function<String, Map<String, NestStateReading>> nestStateReadings;

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
        this.state = Objects.requireNonNull(state, "state");
        this.observations = Objects.requireNonNull(observations, "observations");
        this.recordCounts = Objects.requireNonNull(recordCounts, "recordCounts");
        this.positions = Objects.requireNonNull(positions, "positions");
        this.snapshots = Objects.requireNonNull(snapshots, "snapshots");
        this.frontierGaps = Objects.requireNonNull(frontierGaps, "frontierGaps");
        this.nestStateReadings = Objects.requireNonNull(nestStateReadings, "nestStateReadings");
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
            observations.save(new Observation(pipelineId, actual, metrics(pipelineId, actual),
                    snapshots.apply(pipelineId), positions.apply(pipelineId), carried));
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
        observations.save(new Observation(pipelineId, lastState, Map.of("errorCount", consecutiveFailures),
                null, lastPositions, lastFailure));
    }

    /**
     * The numeric run statistics for the pipeline: errorCount is derived from the actual state (a FAILED job
     * is one observable error, else zero) and is always present; recordCount is added from its source only
     * when a live job reports one, so its absence reads as "not wired" rather than a zero count.
     */
    private Map<String, Long> metrics(String pipelineId, PipelineState actual) {
        Map<String, Long> metrics = new HashMap<>();
        metrics.put("errorCount", actual == PipelineState.FAILED ? 1L : 0L);
        recordCounts.apply(pipelineId).ifPresent(count -> metrics.put("recordCount", count));
        // One entry per chain that reported a reading, so a chain keeping up and a chain that has stalled
        // stay distinguishable; a chain that reported none is absent rather than zero, which would read as
        // the healthy end of the same scale.
        frontierGaps.apply(pipelineId).forEach((chain, gap) -> metrics.put(FRONTIER_GAP_PREFIX + chain, gap));
        // One set per namespace that reported, so a resolver thrashing against its cold layer stays
        // distinguishable from an assembler that is not; a namespace reporting nothing is absent rather
        // than present at zero, which would read as a state layer that had emptied.
        nestStateReadings.apply(pipelineId).forEach((namespace, reading) -> {
            metrics.put(NEST_ENTRIES_PREFIX + namespace, reading.entries());
            metrics.put(NEST_ACCESSES_PREFIX + namespace, reading.accesses());
            metrics.put(NEST_BACKFILLS_PREFIX + namespace, reading.backfills());
            metrics.put(NEST_BACKFILL_MILLIS_PREFIX + namespace, reading.backfillMillis());
            reading.stored().ifPresent(stored -> metrics.put(NEST_STORED_PREFIX + namespace, stored));
        });
        return metrics;
    }
}
