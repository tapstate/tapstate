package io.tapstate.control.core;

import java.util.List;

/**
 * A read-only judgement made when a person starts a pipeline, before any desired state is written.
 *
 * <p>Adding one is a class, a line in the registry that lists every check in the order it is shown,
 * its codes with their catalog text, and its cases; no client changes, because a client renders a
 * finding from what the finding says rather than from which check made it.
 *
 * <p>A check reads and never writes: the context it is handed offers nothing to write through, and an
 * action that would change the definition is handed back as a function for the start to apply. A check
 * that cannot tell -- a target out of reach, a probe refused -- throws the coded error that says why, and
 * the start records that as a finding of its own that never passes. Anything else it throws is a defect
 * and fails the start with it.
 */
public interface StartCheck {

    /** Stable lower-kebab id; part of the external contract, never renamed. */
    String id();

    /**
     * The findings for the start the context describes, one per subject; empty when this check has
     * nothing to say about it. Honours {@link StartCheckContext#deadline()}.
     */
    List<StartFinding> evaluate(StartCheckContext context);

    /** The behavior a finding takes when this check cannot evaluate; never a pass. */
    default StartFinding.Behavior whenUnavailable() {
        return StartFinding.Behavior.WARN;
    }
}
