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

    /**
     * Has the chain reader {@code run} carries take on every table pipelines have since asked it for;
     * answers whether its subscription changed. The default reads nothing, for an attacher whose runs have
     * no chain reader to widen.
     */
    default boolean widen(CaptureRun run) {
        return false;
    }

    @Override
    default CaptureRun start(CaptureRunSpec spec, CaptureHandoff handoff) {
        return start(spec, handoff, true);
    }
}
