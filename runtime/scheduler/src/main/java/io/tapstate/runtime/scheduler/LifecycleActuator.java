package io.tapstate.runtime.scheduler;

import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.SuccessorAdmission;
import io.tapstate.spi.store.HandoffIdentity;
import io.tapstate.core.lifecycle.PipelineState;

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

    /** Holds admitted start work until the checkpoint transition has been recorded. */
    interface PreparedStart extends AutoCloseable {
        void submit();

        /** The actual scoped execution captured by a successful submission, when this binding supplies one. */
        default Optional<StopReservation.Source> submittedSource() {
            return Optional.empty();
        }

        @Override
        void close();
    }

    /** Atomically admits one successor after the actuator has secured its bounded capture capacity. */
    @FunctionalInterface
    interface ReplacementAdmission {
        Optional<SuccessorAdmission> admit(StopAuthority current, String incarnationId, String submissionBootId,
                java.util.Set<String> executionMembers);
    }

    /** Carries the exact durable admission through the start fence and the actual native submission. */
    interface PreparedReplacement extends PreparedStart {
        StopReservation admitted();

        Optional<StopReservation.JobIdentity> submittedJob();
    }

    /** Prepares a replacement while retaining the original durable instruction and counter policy. */
    default PreparedReplacement prepareReplacement(StopReservation reservation, ReplacementAdmission admission,
            Predicate<StopReservation> current) {
        throw new UnsupportedOperationException("durable replacement actuation is unavailable");
    }

    /** Factual lookup of the recorded slot; empty job means the exact admitted execution is absent. */
    record SuccessorInspection(Optional<StopReservation.JobIdentity> job, Optional<PipelineState> terminalState) {
        public SuccessorInspection {
            java.util.Objects.requireNonNull(job, "job"); java.util.Objects.requireNonNull(terminalState, "terminalState");
            if (terminalState.filter(state -> state != PipelineState.FAILED && state != PipelineState.COMPLETED).isPresent()
                    || terminalState.isPresent() && job.isEmpty()) {
                throw new IllegalArgumentException("a terminal inspection needs its actual job and completed or failed state");
            }
        }

        public SuccessorInspection(Optional<StopReservation.JobIdentity> job, boolean terminal) {
            this(job, Optional.empty());
            if (terminal) { throw new IllegalArgumentException("terminal inspection requires its factual state"); }
        }

        public boolean terminal() { return terminalState.isPresent(); }
    }

    default Optional<SuccessorInspection> inspectSuccessor(StopReservation reservation, BooleanSupplier current) {
        throw new UnsupportedOperationException("durable replacement inspection is unavailable");
    }

    /** Installs a verified actual target locally without submission or generation advancement. */
    default boolean adoptSuccessor(StopReservation reservation, BooleanSupplier current) {
        throw new UnsupportedOperationException("durable replacement adoption is unavailable");
    }

    /** Returns only a locally acknowledged durable carrier for this exact bound target. */
    default Optional<HandoffIdentity> continuationReady(StopReservation reservation, BooleanSupplier current) {
        return Optional.empty();
    }

    /** Qualifies a failure's already admitted metric owner even when native submission never happened. */
    default void observeReplacementFailure(StopReservation reservation, BooleanSupplier current) { }

    /**
     * Prepares a start before its RUNNING checkpoint is written. Actuators without bounded admission
     * retain their existing start behavior after the checkpoint transition.
     */
    default PreparedStart prepareStart(String pipelineId) {
        return new PreparedStart() {
            @Override
            public void submit() {
                start(pipelineId);
            }

            @Override
            public void close() {
            }
        };
    }

    /** Whether a held job has an undelivered load and must be replaced before it can resume. */
    default boolean needsRebuildOnResume(String pipelineId) {
        return false;
    }

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

    /** Stops a paused job that must be rebuilt while retaining its known cumulative observations. */
    default void stopForRebuildingResume(String pipelineId, boolean purgeState) {
        stop(pipelineId, purgeState);
    }

    /** A native binding supplies a factual absent job or the exact admitted old job before reservation. */
    default Optional<StopReservation.Subject> stopSubject(String pipelineId) {
        return Optional.empty();
    }

    /** Captures the immutable old execution facts before a phased reservation is accepted. */
    default Optional<StopReservation.Source> stopSource(String pipelineId) {
        return stopSubject(pipelineId).map(subject -> switch (subject) {
            case StopReservation.ExistingJob old -> new StopReservation.Source(old.oldJob().clusterId(),
                    new io.tapstate.spi.store.ObservationStore.Scope(
                            old.pipelineIncarnationId(), old.executionGeneration()), old.oldJob());
            case StopReservation.NoJob absent -> new StopReservation.Source(absent.clusterId(), null, null);
        });
    }

    /** Reads current authority without advancing the execution sequence. */
    default Optional<StopAuthority> stopAuthority(String pipelineId) {
        return Optional.empty();
    }

    /**
     * Finishes only the pinned old job and its capture. False is unfinished work; a retired intent cannot
     * authorize its old purge. Metrics are captured on first admission, independently of later retries.
     */
    default boolean finishStop(StopReservation reservation, boolean continuing, boolean firstAttempt,
            boolean retiring, BooleanSupplier current) {
        throw new UnsupportedOperationException("durable stop actuation is unavailable");
    }

    /**
     * The failure of the pipeline's job if it died on its own, or empty while it runs, has no job, or
     * was ended by a stop. This is how the converge loop observes a job that failed after it started:
     * a stop's own cancellation is not a failure and must not be reported as one.
     */
    Optional<Throwable> failure(String pipelineId);

    /**
     * Why nothing here can hold this pipeline's job any more, or empty while something can.
     *
     * <p>Asked of a paused pipeline, which {@link #failure} is not asked of: its job is held rather than run,
     * so it does not die on its own, and anything else that goes wrong behind it is found once it is meant to
     * run again. Losing the data plane that holds the job is different. The job goes with it, so there is
     * nothing left to resume.
     */
    Optional<Throwable> lost(String pipelineId);

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
