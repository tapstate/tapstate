package io.tapstate.app;

import io.tapstate.core.lifecycle.CaptureReading;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.spi.store.ArtifactStore;

import java.util.Optional;

/**
 * Runs a pipeline's source-side capture alongside its Jet job. When a pipeline starts, its cdc capture must
 * run so the per-table change rings its topology reads are actually filled; when it stops, that capture must
 * be torn down so no capture daemon leaks. The actuator drives both this and the engine, composing the two
 * so a start fills the ring before the job reads it and a stop stops the job before the capture behind it.
 */
interface PipelineCaptureCoordinator {

    /**
     * Starts the cdc capture for every source the pipeline reads, retaining the live handles for a later stop.
     *
     * <p>Returns once every source's load is open, before its rows are read. The rows are read while the job
     * that takes them runs, into a hand-off that holds a few thousand of them at a time, and the tail follows
     * each load once it is through; a failure of either is reported by {@link #captureFailure}.
     *
     * <p>May give the start back with {@link RingNotOpenYet} instead, having opened nothing: a capture it reads
     * is held by another member that has not opened its ring yet. The caller submits nothing for the
     * pipeline and starts it again on a later pass.
     */
    void startCapture(String pipelineId);

    /**
     * Starts from the same immutable artifact snapshot used to build the pipeline's DAG and state plan.
     * Lightweight coordinators that do not read artifacts keep their existing implementation.
     */
    default void startCapture(String pipelineId, ArtifactStore artifactSnapshot) {
        startCapture(pipelineId);
    }

    /**
     * Stops the cdc capture started for the pipeline, tearing down each source run and giving back its hold
     * on each chain it read. {@code purgeState} additionally lets go of what the pipeline left in the
     * source-side record -- the cursor it reads and acknowledges from.
     *
     * <p>That cursor is why the answer reaches this far down. It is not the stopped pipeline's own concern
     * alone: every consumer's cursor is folded into two minimums the whole chain is bounded by, so one that
     * will never advance again holds back every pipeline still on it. A stop asked to keep the pipeline's
     * position keeps that cost with it -- deliberately, because the position is the thing being kept.
     */
    void stopCapture(String pipelineId, boolean purgeState);

    /**
     * The failure a running pipeline's capture died with, or empty while it is healthy. The load and the cdc
     * stream both run on threads of their own feeding what the Jet job reads, so a read that dies leaves the
     * job running over a quiet hand-off or ring; this is how the actuator seam surfaces that death for the
     * converge loop to act on. A coordinator that runs no capture reports none.
     */
    default Optional<Throwable> captureFailure(String pipelineId) {
        return Optional.empty();
    }

    /**
     * Whether every table this pipeline reads has had its initial load confirmed at this pipeline's target.
     *
     * <p>Delivered, not read -- and the two come apart for the whole of the window this question exists for.
     * A table's read is over once its last row is in the hand-off the job takes it from, which is still ahead
     * of every step between there and the target; a load held part way through has tables read in full whose
     * rows have not all reached the target. A reading taken from the read side answers yes for those, which
     * is the same as not asking at all.
     *
     * <p>It has to be asked because the rows a read produced live nowhere durable until the target confirms
     * them: they reach the source vertex through a member-local hand-off that is consumed once. A job that
     * restarts comes back to an empty hand-off over a capture that has moved on to tailing, and then reads
     * nothing at all, indefinitely, while reporting healthy.
     *
     * <p>Reports true when there is nothing that could be unfinished: a coordinator running no capture, and
     * a pipeline whose read mode has no load. That default is what keeps every caller that never had this
     * question on the path it already takes.
     */
    default boolean loadDelivered(String pipelineId) {
        return true;
    }

    /**
     * How far each of the pipeline's tables got through its initial load, keyed by table, with the moment
     * that load began; nothing when no capture is running for it. A coordinator that runs no capture
     * reports nothing.
     *
     * <p>The start rides along because the rows are a total and a total without what it accumulates from
     * cannot be read: a pipeline restarted onto a fresh load and one whose count went backwards are the
     * same observation otherwise.
     *
     * <p>What this reports is each table's finished load, not a live position in one: a table's row count is
     * published once its read is through, and not while it runs. Until then the table is simply absent, which
     * the read face publishes as unavailable rather than as a table at zero rows.
     */
    default SnapshotReading snapshotProgress(String pipelineId) {
        return SnapshotReading.NONE;
    }

    /**
     * How many rows the pipeline's capture has taken from its sources, by table and source operation, with
     * the moment it began counting; nothing for a pipeline with no capture running. A coordinator that runs
     * no capture reports nothing.
     *
     * <p>This is the source side of the same question the target side answers, and the two are worth having
     * separately for the window where they disagree: everything read but not yet confirmed sits between
     * them, and that gap is the only thing distinguishing a pipeline that is keeping up from one whose
     * reading is fine and whose writing has stopped.
     *
     * <p>A load is counted as it is read, and what follows it as it arrives: the rows appear here while the
     * load is still running, which is the window {@link #snapshotProgress} leaves a table out of until its
     * read is through.
     */
    default CaptureReading capturedRows(String pipelineId) {
        return CaptureReading.NONE;
    }
}
