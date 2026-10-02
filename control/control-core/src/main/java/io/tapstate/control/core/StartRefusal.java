package io.tapstate.control.core;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;

import java.util.List;
import java.util.Map;

/**
 * A start that did not go ahead, with what the start checks said about it.
 *
 * <p>Two shapes. A start the checks stopped carries the report, so a client can show every question
 * and refusal at once rather than only the first. A start that changed the definition as a person chose
 * and then could not go ahead carries what it changed, because a definition that was rewritten under a
 * failed start is still rewritten, and saying nothing about it would leave the person believing it was not.
 */
public final class StartRefusal extends TapstateException {

    private final transient StartCheckReport report;
    private final transient List<StartOutcome.Applied> decisionsApplied;

    StartRefusal(TapstateErrorCode code, Map<String, Object> args, StartCheckReport report,
            List<StartOutcome.Applied> decisionsApplied, Throwable cause) {
        super(code, args, cause);
        this.report = report;
        this.decisionsApplied = List.copyOf(decisionsApplied);
    }

    /** What the start checks said, or null when the start was refused after they had let it through. */
    public StartCheckReport report() {
        return report;
    }

    /** The changes the start made to the definition before it was refused; empty when it made none. */
    public List<StartOutcome.Applied> decisionsApplied() {
        return decisionsApplied;
    }
}
