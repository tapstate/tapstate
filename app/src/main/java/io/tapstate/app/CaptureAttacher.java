package io.tapstate.app;

import io.tapstate.runtime.srs.CaptureHandoff;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;

/**
 * Starts a source attachment, optionally including the one shared tail for its capture identity. The run may
 * be handed back while its load is still being read.
 */
@FunctionalInterface
interface CaptureAttacher {

    CaptureRun start(CaptureRunSpec spec, CaptureHandoff handoff, boolean startTail);
}
