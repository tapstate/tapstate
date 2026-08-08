package io.tapstate.app;

import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.LifecycleActuator;

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
 *   <li>{@code start} finishes any drop an earlier stop left outstanding, fills the change ring (capture),
 *       then submits the job that reads it (engine), so the topology never starts against an unfilled ring
 *       nor onto state that was half let go of.</li>
 *   <li>{@code stop} cancels the job first (engine) then stops the capture behind it (coordinator), so the
 *       capture daemon is torn down only once nothing reads its ring; the operator state the run kept is
 *       let go of last, once the job it belonged to is actually over.</li>
 *   <li>{@code pause} / {@code resume} are engine-only: the capture keeps running while a pipeline is paused,
 *       held back by the ring's headroom backpressure, and a resume replays the buffered ring.</li>
 * </ul>
 */
final class EngineLifecycleActuator implements LifecycleActuator {

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

    EngineLifecycleActuator(Engine engine, DagSource dagSource, PipelineCaptureCoordinator captureCoordinator,
            NestStateTeardown stateTeardown) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.dagSource = Objects.requireNonNull(dagSource, "dagSource");
        this.captureCoordinator = Objects.requireNonNull(captureCoordinator, "captureCoordinator");
        this.stateTeardown = Objects.requireNonNull(stateTeardown, "stateTeardown");
    }

    @Override
    public void start(String pipelineId) {
        // Before anything reads it: a drop the last stop noted but did not finish is finished here, so this
        // run never starts onto a half-dropped state. A start with nothing noted drops nothing, which is
        // what leaves a run that died without a stop with its state - and so with a shape to be held to.
        stateTeardown.finishPending(pipelineId);
        // Where this run keeps state, said before anything can write any: the pipeline is only certainly
        // the one this run is built from now, and an apply may move it out from under the run at any point
        // after. Said after the drop above, which is the one thing entitled to clear what earlier runs said.
        stateTeardown.willKeepStateIn(pipelineId, dagSource.stateNamespacesOf(pipelineId));
        captureCoordinator.startCapture(pipelineId);
        // The capacity travels with the submission because the maps are made by the job: what a state map
        // holds is fixed as it is created, so a number applied after the job started would be accepted and
        // change nothing.
        DagSource.NestCapacity capacity = dagSource.capacityOf(pipelineId);
        engine.submit(pipelineId, dagSource.dagFor(pipelineId),
                capacity.mapNamespaces(), capacity.settings());
    }

    @Override
    public void pause(String pipelineId) {
        engine.suspend(pipelineId);
    }

    @Override
    public void resume(String pipelineId) {
        engine.resume(pipelineId);
    }

    @Override
    public void stop(String pipelineId) {
        engine.cancel(pipelineId);
        // Noted before the job is even known to be over, and before the drop: a stop is driven once, on the
        // transition, so a process that dies anywhere after this point leaves a note the next start finishes.
        // What the runs said they keep is the half that survives an edit; what the pipeline compiles to now
        // is the half that covers state older than there being anywhere to say it. The note takes both.
        stateTeardown.note(pipelineId, dagSource.stateNamespacesOf(pipelineId));
        boolean jobOver = engine.awaitTerminal(pipelineId, JOB_TEARDOWN_BUDGET);
        captureCoordinator.stopCapture(pipelineId);
        if (jobOver) {
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
}
