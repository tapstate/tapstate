package io.tapstate.app;

import io.tapstate.runtime.srs.CaptureId;

/**
 * A start given back because a capture it reads is held by another member that has not opened its ring
 * yet: nothing was opened for it and no claim is left taken, so the next convergence pass simply starts it
 * again.
 *
 * <p>Not a failure, and deliberately not coded: there is nothing for anybody to act on, and the condition
 * clears on its own -- the holder opens its ring, or its lease runs out and this member takes the capture
 * over. What the start still owes is bounded where it is decided: once it has been given back for longer
 * than a lease, the start is refused with a code instead.
 */
final class RingNotOpenYet extends RuntimeException {

    RingNotOpenYet(CaptureId captureId) {
        super("capture " + captureId.value() + " is held by another member that has not opened its ring yet",
                null, false, false);
    }
}
