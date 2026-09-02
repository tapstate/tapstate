package io.tapstate.e2e;

import org.junit.jupiter.api.Assumptions;

/**
 * Holds back the witnesses that need a build somebody already released.
 *
 * <p>Those witnesses are different in kind from the rest of this module. Everything else here is
 * built from the checkout under test, so a red run means the checkout is wrong. These reach for a
 * published release, and a red run can instead mean the release is stale, or missing, or that the
 * registry is having a bad morning. Blocking a pull request that touched none of it on that would be
 * a lie about what failed, which is the same reason the quickstart's live journey is not a per-commit
 * gate either.
 *
 * <p>So they run in a lane of their own, and the lane asks for them by name. Absence of the request
 * is the ordinary case -- a developer machine, and every pull request build -- and skips. There is no
 * third outcome to distinguish here, unlike the connector gate: this decides whether a witness was
 * asked for, not whether something it needs is broken. What it needs, it checks for itself.
 *
 * <p><b>The lane derives which classes to run from the calls to this gate</b> rather than naming
 * them, so a witness added later is picked up without anybody remembering to edit a workflow. That
 * matters more than it sounds: a hand-maintained list fails open, and a witness nobody runs looks
 * exactly like a lane with nothing to do.
 */
final class UpgradeLaneGate {

    /** What the lane sets to ask for these witnesses. Nothing else sets it. */
    private static final String LANE_REQUESTED = "tapstate.e2e.upgrade-lane";

    private UpgradeLaneGate() {
    }

    /** Returns when the lane asked for this witness; aborts the run when it did not. */
    static void require() {
        if (!Boolean.getBoolean(LANE_REQUESTED)) {
            Assumptions.abort("no -D" + LANE_REQUESTED
                    + ": skipping a witness that upgrades from a published release");
        }
    }
}
