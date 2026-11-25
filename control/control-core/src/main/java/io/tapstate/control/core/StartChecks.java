package io.tapstate.control.core;

import java.util.List;

/**
 * Every start check a start is asked, in the order a report lists them: written out here rather than
 * discovered, so adding one is a line somebody writes and a reviewer reads.
 */
public final class StartChecks {

    private StartChecks() {
    }

    /** The registered checks with a target identity reader, in display order. */
    public static List<StartCheck> registered(PipelineTargetIdentities identities) {
        return List.of(new TargetNotEmptyCheck(), new UnwindTargetIdentityCheck(identities));
    }

    /** The basic checks for callers without target identity assessment. */
    public static List<StartCheck> registered() {
        return List.of(new TargetNotEmptyCheck());
    }
}
