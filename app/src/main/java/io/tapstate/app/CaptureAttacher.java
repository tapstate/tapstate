package io.tapstate.app;

import io.tapstate.core.event.Envelope;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;

import java.util.function.Consumer;

/** Starts a source attachment, optionally including the one shared tail for its capture identity. */
@FunctionalInterface
interface CaptureAttacher {

    CaptureRun start(CaptureRunSpec spec, Consumer<Envelope> passthrough, boolean startTail);
}
