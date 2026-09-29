package io.tapstate.app;

/**
 * What one committed cluster member is to one pipeline's current run.
 *
 * <p>A run is planned over the members that were committed when it was submitted, and nothing moves it
 * afterwards: a member that joins later is not given work by the engine, and this product does not hand
 * it any either. That is a deliberate choice rather than a gap -- redistributing a running pipeline is a
 * decision with a plan and an apply step behind it, not something that happens because a process started
 * somewhere. So the new member sits {@link #AWAITING_REBALANCE} until somebody asks for that, and the
 * point of naming the state is that "idle because nobody rebalanced" stops looking like "idle because
 * something is broken".
 */
enum MemberRunState {

    /** The run was planned over this member, so it is carrying whatever of the run landed there. */
    PARTICIPATING,

    /** Committed after the run was planned: the run does not use it, and no rebalance has been asked for. */
    AWAITING_REBALANCE
}
