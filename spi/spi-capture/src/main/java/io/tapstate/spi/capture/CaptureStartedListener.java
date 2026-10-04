package io.tapstate.spi.capture;

/**
 * An optional listener for the source's resolved starting position. An adapter reports this anchor
 * before delivering its first change, so recovery can retain unconfirmed work from that first batch.
 * Existing listeners keep their batched delivery and error contract.
 */
public interface CaptureStartedListener extends CaptureListener {

    /** The actual source recovery anchor; reporting it does not confirm any downstream effects. */
    void onStart(SourcePosition position);
}
