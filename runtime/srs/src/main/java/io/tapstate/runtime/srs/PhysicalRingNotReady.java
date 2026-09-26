package io.tapstate.runtime.srs;

/** A returning ring reader waits until its physical owner has published the generation start boundary. */
public final class PhysicalRingNotReady extends RuntimeException {

    public PhysicalRingNotReady(String chainId, String table) {
        super("physical ring start is not ready for " + chainId + "/" + table);
    }
}
