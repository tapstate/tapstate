package io.tapstate.app;

import io.tapstate.runtime.srs.CaptureHandoff;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;

/**
 * The seam by which the capture coordinator starts one source run and gets back a live handle. Its production
 * binding is the capture run unit's {@code begin}; keeping it a seam lets the coordinator's handle-lifecycle
 * logic be driven without a running Jet member. The signature matches the run unit exactly, so the binding is
 * a plain method reference.
 */
@FunctionalInterface
interface CaptureStarter {

    /**
     * Starts the source run for {@code spec}, handing any snapshot rows to {@code handoff} and telling it as
     * each table's load is in. The run may be handed back while its load is still being read.
     */
    CaptureRun start(CaptureRunSpec spec, CaptureHandoff handoff);
}
