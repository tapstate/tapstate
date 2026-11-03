package io.tapstate.runtime.srs;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStartedListener;
import io.tapstate.spi.capture.SourcePosition;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** Records original physical input while retaining the normal listener's contract. */
final class RecordingCaptureObservation implements CaptureObservation.Factory {
    final List<CaptureObservation.Context> contexts = new ArrayList<>();
    final List<Envelope> events = new ArrayList<>();
    final List<Throwable> failures = new ArrayList<>();
    final AtomicInteger retirements = new AtomicInteger();

    @Override public CaptureObservation.Hooks open(CaptureObservation.Context context) {
        contexts.add(context);
        return new CaptureObservation.Hooks() {
            @Override public CaptureListener wrap(CaptureListener original) {
                return new CaptureStartedListener() {
                    @Override public void onStart(SourcePosition position) { ((CaptureStartedListener) original).onStart(position); }
                    @Override public void onBatch(List<Envelope> batch, Optional<SourcePosition> position) {
                        original.onBatch(batch, position); events.addAll(batch);
                    }
                    @Override public void onError(Throwable failure) { failures.add(failure); original.onError(failure); }
                };
            }
            @Override public void retire(Throwable failure) { retirements.incrementAndGet(); }
        };
    }
}
