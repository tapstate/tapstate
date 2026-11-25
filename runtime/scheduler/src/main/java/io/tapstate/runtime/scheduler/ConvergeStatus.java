package io.tapstate.runtime.scheduler;

/** The outcome of a single convergence pass over one pipeline. */
public enum ConvergeStatus {

    /** The actual state is at the target — either this pass drove it there or it was already there. */
    CONVERGED,

    /** There was nothing to converge: no desired intent, or no checkpoint to complete. */
    NOTHING_TO_DO,

    /** An automatic replacement has yet to publish a live execution. */
    RECOVERING,

    /**
     * The pass was fenced on every attempt and gave up within its retry bound, having been superseded
     * by another writer. Not an error — the next pass re-reads and retries.
     */
    SUPERSEDED,

    /**
     * The pass found a pipeline it believed running had a dead job and drove it to the observable
     * FAILED state. The result carries the job's failure cause so the caller can surface it.
     */
    FAILED
}
