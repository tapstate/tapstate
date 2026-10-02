package io.tapstate.app;

import io.tapstate.runtime.srs.CaptureHandoff;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;

/**
 * Starts a source attachment, optionally including the one shared tail for its capture identity. The run may
 * be handed back while its load is still being read. This subtype makes an attachment-capable coordinator
 * constructor win over the starter-only constructor for an overloaded run-unit method reference.
 */
@FunctionalInterface
interface CaptureAttacher extends CaptureStarter {

    CaptureRun start(CaptureRunSpec spec, CaptureHandoff handoff, boolean startTail);

    @Override
    default CaptureRun start(CaptureRunSpec spec, CaptureHandoff handoff) {
        return start(spec, handoff, true);
    }

    /** Replaces only the physical tail while retaining each attached pipeline reader. */
    default CaptureRun reopenPhysicalTail(CaptureRunSpec spec, CaptureRun previous) {
        throw new UnsupportedOperationException("physical capture expansion is unavailable");
    }
}
