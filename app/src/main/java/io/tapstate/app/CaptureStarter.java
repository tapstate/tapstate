package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.srs.CaptureHandoff;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;

import java.util.Optional;

/**
 * The seam by which a test coordinator starts one source run and gets back a live handle. Production uses
 * {@link CaptureAttacher} so another pipeline can read its own load without opening a second change tail.
 */
@FunctionalInterface
interface CaptureStarter {

    /**
     * Starts the source run for {@code spec}, handing any snapshot rows to {@code handoff} and telling it as
     * each table's load is in. The run may be handed back while its load is still being read.
     */
    CaptureRun start(CaptureRunSpec spec, CaptureHandoff handoff);

    /**
     * Lets go of what {@code spec}'s source connector set up on the source to read changes, answering what the
     * source refused to let go of. The default sets nothing up, so it has nothing to let go of.
     */
    default Optional<TapstateException> release(CaptureRunSpec spec) {
        return Optional.empty();
    }
}
