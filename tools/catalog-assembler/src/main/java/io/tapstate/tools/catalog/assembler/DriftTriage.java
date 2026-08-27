package io.tapstate.tools.catalog.assembler;

import java.util.List;

import io.tapstate.core.catalog.OfficialConnectors;

/**
 * Decides whether an upstream drift opens a pull request now or waits for company.
 *
 * <p>Every drift is eventually carried into the catalog; the only question here is what gets its own
 * pull request. A scan that opened one per drift would interrupt roughly every four days, most times
 * over a connector this release does not support, and a review nobody has time for is a review that
 * stops happening. So a drift touching a supported connector opens immediately, and everything else
 * accumulates until the next pull request — whichever opens it — sweeps it up.
 *
 * <p>The wait has a ceiling, because the trigger can go quiet for a long time: which of the two
 * halves actually drives this depends on how many connectors the release supports, and that set
 * grows. While it is small, the ceiling is what opens nearly every pull request and the scan is in
 * effect a timer; once the set is large the trigger takes over and the ceiling is the safety net it
 * reads like. Both are the intended behaviour — a scan that has not opened anything in a while is not
 * evidence it is broken.
 *
 * <p>Holding stops once a pull request is open, and that is not a second policy but the same one read
 * correctly: holding means accumulating into whichever pull request opens next, and while one is open
 * that is the accumulator. Leaving it alone does not defer an interruption — the interruption already
 * happened — it lets an open pull request carry a catalog regenerated from a revision of this
 * repository that is falling further behind by the day. What it carries are generated artifacts, so
 * anything that regenerates them on the default branch conflicts with it outright and no reviewer can
 * resolve that by hand. Refreshing it is a force-push onto an unread pull request; the alternative is
 * one nobody can merge.
 */
final class DriftTriage {

    /** What the scan does with what it found. */
    enum Decision {
        /** Open a pull request carrying every drift seen so far. */
        OPEN,
        /** Carry on accumulating; some later run will take these along. */
        HOLD,
        /** Upstream matches the catalog — there is nothing to put in a pull request. */
        NOTHING
    }

    /**
     * How long held drift may wait. Deliberately a fixed number rather than "long enough": the point
     * is that an unsupported connector's specification cannot go stale indefinitely just because no
     * supported one moved.
     */
    static final int FALLBACK_DAYS = 7;

    private DriftTriage() {
    }

    static Decision decide(
            List<String> changedConnectorIds, int daysSinceLastPullRequest, boolean pullRequestAlreadyOpen) {
        if (changedConnectorIds.isEmpty()) {
            // Checked before both of the reasons to open: the ceiling exists to flush held drift and
            // the open pull request is where held drift goes, and with nothing held there is nothing
            // to flush - an empty pull request every seventh day would train its reviewers to close
            // this one unread. An open pull request with nothing left to carry is a different
            // question, and closing it is not this class's to answer.
            return Decision.NOTHING;
        }
        if (pullRequestAlreadyOpen) {
            return Decision.OPEN;
        }
        if (changedConnectorIds.stream().anyMatch(OfficialConnectors::isOfficial)) {
            return Decision.OPEN;
        }
        return daysSinceLastPullRequest >= FALLBACK_DAYS ? Decision.OPEN : Decision.HOLD;
    }
}
