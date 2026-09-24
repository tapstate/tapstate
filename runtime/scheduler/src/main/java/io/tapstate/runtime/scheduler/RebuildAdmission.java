package io.tapstate.runtime.scheduler;

/**
 * Whether a run that has died may be rebuilt, asked once per pipeline per pass while its recorded state
 * is failed and its intent is still to run.
 *
 * <p>The converge loop cannot answer this itself, and deliberately does not try. What makes a death
 * worth rebuilding is that the cluster changed underneath the run -- a fact about members and claims,
 * which lives outside this ring. Nor can the engine answer it: a run ended by a member leaving and a run
 * ended by a connector giving up arrive at this loop as the same class with the same empty cause, so
 * branching on the failure itself would be branching on the wording of somebody else's message.
 *
 * <p>Everything else about a failed run is unchanged by this seam. The failure is recorded and published
 * first, on the pass that observed it, so nothing waits in silence while this is being decided; and a
 * refusal here leaves the pipeline exactly where the old behaviour left it, failed until a person says
 * otherwise. Bounding the attempts is the implementation's business, because an unbounded yes is how an
 * automatic recovery turns into a restart loop.
 */
@FunctionalInterface
public interface RebuildAdmission {

    /** Whether this pipeline's failed run may be replaced now. Called only while it is failed. */
    boolean admits(String pipelineId);

    /** The answer for a run nothing can rebuild: a single node, where no member left to change anything. */
    static RebuildAdmission never() {
        return pipelineId -> false;
    }
}
