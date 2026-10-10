package io.tapstate.spi.capture;

/**
 * An optional bounded read with no change tail. Position discovery can allocate source resources that
 * only a tail consumes, so a port offering this capability does not sample an unused stream seam.
 */
@FunctionalInterface
public interface SnapshotOnlyCapture {

    /** Reads the configured streams once without sampling a change-stream position. */
    CaptureBatch snapshotOnly(CaptureConfig config);

    /** Uses the optional capability where offered; other ports keep their ordinary bounded read. */
    static CaptureBatch open(CapturePort port, CaptureConfig config) {
        return port instanceof SnapshotOnlyCapture capture ? capture.snapshotOnly(config) : port.snapshot(config);
    }
}
