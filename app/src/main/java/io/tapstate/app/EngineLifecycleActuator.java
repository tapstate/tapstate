package io.tapstate.app;

import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Binds the converge loop's lifecycle actuator seam to the Jet execution engine and the source-side capture
 * coordinator, composing the two so the data plane runs end to end. This is the assembly-layer wiring of the
 * data plane, which lets the runtime ring's converge loop stay engine- and framework-free.
 *
 * <p>Each verb composes the two sides in the order the data flow requires:
 * <ul>
 *   <li>{@code start} validates the pipeline before side effects, finishes any drop an earlier stop left
 *       outstanding, fills the change ring (capture), then submits the job that reads it (engine), so the
 *       topology never starts against an unmet prerequisite, an unfilled ring, nor state that was half let go
 *       of.</li>
 *   <li>{@code stop} cancels the job first (engine) then stops the capture behind it (coordinator), so the
 *       capture daemon is torn down only once nothing reads its ring; the operator state the run kept is
 *       let go of last, once the job it belonged to is actually over -- and only where the stop asked for
 *       it. A stop that was asked to keep the state does not write the drop down either, which is a
 *       stronger thing than not carrying it out: the note is what a later start finishes, so one written
 *       here would have the state dropped by the next start of a pipeline nobody asked to clear.</li>
 *   <li>{@code pause} / {@code resume} are engine-only once the initial load has reached the target: the
 *       capture keeps running while a pipeline is paused, held back by the ring's headroom backpressure,
 *       and a resume replays the buffered ring. A load still undelivered is the one case that cannot be
 *       resumed in place, and it rebuilds instead.</li>
 * </ul>
 */
final class EngineLifecycleActuator implements LifecycleActuator {

    private static final Logger LOG = LoggerFactory.getLogger(EngineLifecycleActuator.class);

    /**
     * How long a stop waits for the job to actually be over before it gives up on letting go of that job's
     * state in the same breath. Long enough that an ordinary cancel finishes inside it, short enough that
     * a job which will not die does not hold the converge loop: what is not dropped here is left noted and
     * dropped by the next start instead.
     */
    private static final Duration JOB_TEARDOWN_BUDGET = Duration.ofSeconds(30);

    private final Engine engine;
    private final DagSource dagSource;
    private final PipelineCaptureCoordinator captureCoordinator;
    private final NestStateTeardown stateTeardown;
    private final PipelineActuationOwnership actuation;

    EngineLifecycleActuator(Engine engine, DagSource dagSource, PipelineCaptureCoordinator captureCoordinator,
            NestStateTeardown stateTeardown) {
        this(engine, dagSource, captureCoordinator, stateTeardown, PipelineActuationOwnership.single());
    }

    EngineLifecycleActuator(Engine engine, DagSource dagSource, PipelineCaptureCoordinator captureCoordinator,
            NestStateTeardown stateTeardown, PipelineActuationOwnership actuation) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.dagSource = Objects.requireNonNull(dagSource, "dagSource");
        this.captureCoordinator = Objects.requireNonNull(captureCoordinator, "captureCoordinator");
        this.stateTeardown = Objects.requireNonNull(stateTeardown, "stateTeardown");
        this.actuation = Objects.requireNonNull(actuation, "actuation");
    }

    @Override
    public void start(String pipelineId) {
        // A refusal here is deliberately before teardown, capture, and submission: an unmet source-model
        // prerequisite must leave no data-plane component running and no start-side state mutation behind.
        DagSource.StartPreparation prepared = dagSource.prepareStart(
                pipelineId, stateTeardown.defaultDatabase());
        // The run's own generation, taken before the first side effect for the same reason: a run this
        // member cannot fence is one nothing could later stop from writing, so it must not be half built.
        // Nothing is recorded as failed here -- the pipeline is fine, this member is not its driver any
        // more (or cannot prove it is), and the member that is will put a run behind it.
        PipelineActuationOwnership.Execution execution = actuation.beginExecution(pipelineId);
        if (!execution.allowed()) {
            LOG.warn("Not starting pipeline {} on this member: its run could not be fenced to a new "
                    + "execution generation", pipelineId);
            return;
        }
        // Before anything reads it: a drop the last stop noted but did not finish is finished here, so this
        // run never starts onto a half-dropped state. A start with nothing noted drops nothing, which is
        // what leaves a run that died without a stop with its state - and so with a shape to be held to.
        stateTeardown.finishPending(pipelineId);
        DagSource.NestCapacity capacity = prepared.capacity();
        engine.configureNestState(capacity.mapDatabases(), capacity.settings());
        if (!capacity.mapDatabases().isEmpty()) {
            LOG.info("Nest state placement resolved before pipeline '{}' starts: {}",
                    pipelineId, capacity.mapDatabases());
        }
        // Where this run keeps state, said before anything can write any: the pipeline is only certainly
        // the one this run is built from now. The locations and DAG came from the same immutable artifact
        // snapshot, so an apply cannot move one without the others. Said after the drop above, which is the
        // one thing entitled to clear what earlier runs said.
        stateTeardown.willKeepStateAt(pipelineId, prepared.stateLocations());
        try {
            prepared.artifactSnapshot().ifPresentOrElse(
                    snapshot -> captureCoordinator.startCapture(pipelineId, snapshot),
                    () -> captureCoordinator.startCapture(pipelineId));
        } catch (RingNotOpenYet notYet) {
            // Nothing was opened, so nothing is submitted: the pipeline reads as started and carries no job,
            // which is exactly what the next pass starts again. Not recorded as failed -- a capture it reads is
            // being opened on another member, and how long that may take is bounded where it is decided.
            return;
        }
        // Capture opens the SRS generation that source vertices compile into the DAG. Build only now, but
        // from the same frozen artifacts used above; placement and teardown were already fixed, so any
        // shape record this writes remains named even if construction refuses the start.
        DagSource.StartPlan plan = prepared.build(execution.fence());
        // The capacity travels with the submission because the maps are made by the job: what a state map
        // holds is fixed as it is created, so a number applied after the job started would be accepted and
        // change nothing.
        engine.submit(pipelineId, plan.dag(), capacity.mapDatabases(), capacity.settings());
    }

    @Override
    public void pause(String pipelineId) {
        engine.suspend(pipelineId);
    }

    /**
     * Resumes the pipeline, rebuilding rather than carrying on when its initial load has not reached the
     * target yet.
     *
     * <p>The rows a load has read but not delivered live nowhere durable. They reach the source vertex
     * through a member-local hand-off that vertex consumes once, and resuming restarts the job under a
     * guarantee that keeps no execution state -- so the vertex that comes back finds an empty hand-off
     * over a capture that has moved on to tailing. It then reads nothing at all, indefinitely, with the
     * job running and nothing thrown.
     *
     * <p>Rebuilding is what a stop that keeps and a start already do correctly here: the tables the record
     * still owes are read again and nothing is cleared. Re-reading them is the right cost rather than a
     * regression -- every row of a snapshot carries one reserved position, so nothing anywhere represents
     * a table loaded part way, and avoiding the re-read would mean inventing a durable per-row notion of
     * load progress plus a new ordering question for every consumer of the chain.
     */
    @Override
    public void resume(String pipelineId) {
        if (!captureCoordinator.loadDelivered(pipelineId)) {
            stop(pipelineId, false);
            start(pipelineId);
            return;
        }
        engine.resume(pipelineId);
    }

    @Override
    public void stop(String pipelineId, boolean purgeState) {
        engine.cancel(pipelineId);
        if (purgeState) {
            // Noted before the job is even known to be over, and before the drop: a stop is driven once, on
            // the transition, so a process that dies anywhere after this point leaves a note the next start
            // finishes. What the runs said they keep is the half that survives an edit; what the pipeline
            // compiles to now is the half that covers state older than there being anywhere to say it. The
            // note takes both.
            stateTeardown.noteLocations(
                    pipelineId, dagSource.stateLocations(pipelineId, stateTeardown.defaultDatabase()));
        }
        boolean jobOver = engine.awaitTerminal(pipelineId, JOB_TEARDOWN_BUDGET);
        captureCoordinator.stopCapture(pipelineId, purgeState);
        if (purgeState && jobOver) {
            // Only once nothing is left to write into it. A processor still winding down writes state as it
            // closes, and a drop racing that leaves entries behind with the note already gone.
            stateTeardown.finishPending(pipelineId);
        }
    }

    @Override
    public Optional<Throwable> failure(String pipelineId) {
        // A pipeline fails two ways the converge loop must see as one: the Jet job itself dies (engine), or the
        // cdc capture feeding its ring dies while the job keeps running over a ring gone quiet (coordinator).
        // Either surfaces here so the converge side drives the pipeline into the observable FAILED state.
        return engine.failureOf(pipelineId).or(() -> captureCoordinator.captureFailure(pipelineId));
    }

    @Override
    public boolean isCarryingAJob(String pipelineId) {
        // The job side alone. A capture that died while the job runs is a failure, reported above; what
        // is asked here is whether anything is running this pipeline at all, and a process that has just
        // come up to a checkpoint an earlier one wrote answers no.
        return engine.hasLiveJob(pipelineId);
    }
}
