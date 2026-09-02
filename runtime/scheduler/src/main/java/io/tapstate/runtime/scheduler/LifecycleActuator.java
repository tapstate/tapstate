package io.tapstate.runtime.scheduler;

import java.util.Optional;

/**
 * Turns a converged lifecycle transition into the matching data-plane job operation. The converge
 * side owns the state decision and calls the verb here that the transition implies; the binding drives
 * the execution engine. Kept an interface so the converge loop stays free of the engine and its
 * framework — the data-plane binding is wired in at assembly and the loop is unit-tested against a
 * recording double.
 *
 * <p>Each verb is named by the pipeline id alone: the actuator owns the mapping from a pipeline to its
 * job and, for a start, the topology that job runs. Start begins a fresh run; resume continues a
 * paused one; pause holds a running one; stop ends it. Stop alone takes a second argument, because it
 * is the only verb that can also be asked to throw away what the pipeline has.
 */
public interface LifecycleActuator {

    /** Begins a fresh run of the pipeline: submits its topology as the pipeline's one job. */
    void start(String pipelineId);

    /** Holds the pipeline's running job so it can be resumed later. */
    void pause(String pipelineId);

    /** Continues the pipeline's paused job, re-reading its start position from the store. */
    void resume(String pipelineId);

    /**
     * Ends the pipeline's job, and clears what the pipeline has accumulated when {@code purgeState} says
     * to. Clearing is what makes the next run read its whole source again; keeping is what lets it carry
     * on, so the two are different outcomes rather than a tidiness setting.
     *
     * <p>{@code false} has to mean nothing is touched at all, not "cleared and then put back": the
     * clearing is written down before it is carried out so that it survives a process dying mid-way, and
     * a stop that wrote that down and then declined to act on it would have the next start finish the job
     * on a pipeline whose owner asked for it to be left alone.
     */
    void stop(String pipelineId, boolean purgeState);

    /**
     * The failure of the pipeline's job if it died on its own, or empty while it runs, has no job, or
     * was ended by a stop. This is how the converge loop observes a job that failed after it started:
     * a stop's own cancellation is not a failure and must not be reported as one.
     */
    Optional<Throwable> failure(String pipelineId);

    /**
     * Whether a job is running this pipeline right now, as this actuator sees it.
     *
     * <p>Asked because {@link #failure} cannot answer it. That query reports empty for a job that is
     * running, for a pipeline that has no job at all, and for one a stop ended - three states that a
     * pipeline believed to be RUNNING has to tell apart. The one they hide is a process that has come
     * up to a checkpoint an earlier one wrote: nothing failed, from this actuator's point of view
     * nothing has happened at all, and the pipeline's recorded state already matches its intent, so
     * without this query there is nothing left to notice that no job is carrying it.
     */
    boolean isCarryingAJob(String pipelineId);
}
