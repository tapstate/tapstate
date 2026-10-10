package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.CaptureStartedListener;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.CaptureReadAttempt;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QualifiedCaptureReaderTest {
    @Test
    void onStartRecordsOnlyAnAnchorAndSuccessfulEmptyDeliveryRecordsAcceptanceAfterTheHandler() {
        List<String> observed = new ArrayList<>();
        SrsMetaStore meta = store(observed, true, false);
        CaptureListener callback = (rows, point) -> observed.add("delivered");
        var reader = reader(meta, callback);
        assertThat(observed).containsExactly("allocated");
        reader.onStart(new SourcePosition("original"));
        assertThat(observed).containsExactly("allocated", "anchor");
        reader.onBatch(List.of(), Optional.empty());
        assertThat(observed).containsExactly("allocated", "anchor", "delivered", "accepted");
    }

    @Test
    void failedHandlerNeverRecordsSuccessfulSourceDelivery() {
        List<String> observed = new ArrayList<>();
        var reader = reader(store(observed, true, false), (rows, point) -> { throw new IllegalStateException("sink handoff failed"); });
        reader.onStart(new SourcePosition("original"));
        assertThatThrownBy(() -> reader.onBatch(List.of(), Optional.empty())).isInstanceOf(IllegalStateException.class);
        assertThat(observed).doesNotContain("accepted");
    }

    @Test
    void refusedDurableMarkerDoesNotForgeAcceptance() {
        var reader = reader(store(new ArrayList<>(), false, false), (rows, point) -> { });
        reader.onStart(new SourcePosition("original"));
        assertThatThrownBy(() -> reader.onBatch(List.of(), Optional.empty())).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(CaptureError.CLAIM_LOST));
    }

    @Test
    void markerIoFailurePreservesTheOriginalSourceFailureAndItsAttemptToken() {
        List<String> observed = new ArrayList<>();
        RuntimeException actualSourceFailure = new RuntimeException("source refused original token");
        List<Throwable> delegated = new ArrayList<>();
        CaptureListener callback = new CaptureListener() {
            @Override public void onBatch(List<io.tapstate.core.event.Envelope> rows, Optional<SourcePosition> point) { }
            @Override public void onError(Throwable error) { delegated.add(error); }
        };
        reader(store(observed, true, true), callback).onError(actualSourceFailure);
        assertThat(delegated).containsExactly(actualSourceFailure);
        assertThat(actualSourceFailure.getSuppressed()).hasSize(1);
        assertThat(observed).containsExactly("allocated", "failed:original");
    }

    @Test
    void codedSourceFailureRetainsEveryNamedArgumentAndItsDisposition() {
        java.util.concurrent.atomic.AtomicReference<java.util.Map<String, Object>> saved =
                new java.util.concurrent.atomic.AtomicReference<>(java.util.Map.of());
        SrsMetaStore meta = (SrsMetaStore) Proxy.newProxyInstance(SrsMetaStore.class.getClassLoader(),
                new Class<?>[]{SrsMetaStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("beginCaptureReadAttempt")) {
                        return Optional.of(new CaptureReadAttempt("chain", 1, 2, fence(), List.of("orders"),
                                CaptureReadAttempt.Kind.RESUME, "original", null, Instant.EPOCH));
                    }
                    if (method.getName().equals("recordCaptureReadFailure")) {
                        if (args.length > 2) {
                            @SuppressWarnings("unchecked")
                            var params = (java.util.Map<String, Object>) args[2];
                            saved.set(params);
                            assertThat(args[3]).isEqualTo("stop-and-clear-source-state");
                        }
                        return true;
                    }
                    throw new AssertionError("unexpected source store call " + method.getName());
                });
        var actual = java.util.Map.<String, Object>of("requested", "old-instant", "earliest", "retained-head", "retention", "2h");
        reader(meta, (rows, point) -> { }).onError(new TapstateException(CaptureError.START_FROM_OUTSIDE_WINDOW, actual, null));
        assertThat(saved.get()).isEqualTo(actual);
    }

    private static CaptureStartedListener reader(SrsMetaStore store, CaptureListener callback) {
        return (CaptureStartedListener) CdcPhase.qualifiedReader(callback, store, "chain", 1, List.of("orders"),
                CaptureStart.resume(new SourcePosition("original")), fence(), null, null);
    }

    private static WorkloadClaimFence fence() {
        return new WorkloadClaimFence(new WorkloadClaimKey("east", WorkloadClaimType.CAPTURE, "capture"),
                new WorkloadOwner("node", "boot"), 1, 0, 1, 1);
    }

    private static SrsMetaStore store(List<String> events, boolean allowAcceptance, boolean failFailureMarker) {
        return (SrsMetaStore) Proxy.newProxyInstance(SrsMetaStore.class.getClassLoader(), new Class<?>[]{SrsMetaStore.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "beginCaptureReadAttempt":
                            events.add("allocated");
                            return Optional.of(new CaptureReadAttempt("chain", 1, 2, fence(), List.of("orders"),
                                    CaptureReadAttempt.Kind.RESUME, "original", null, Instant.EPOCH));
                        case "recordCaptureAnchor": events.add("anchor"); return true;
                        case "recordCaptureFirstDelivery": events.add("accepted"); return allowAcceptance;
                        case "recordCaptureReadFailure":
                            events.add("failed:" + ((CaptureReadAttempt) args[0]).requestedToken());
                            if (failFailureMarker) { throw new TapstateException(IoError.STORE_UNAVAILABLE, java.util.Map.of("detail", "durable marker unavailable"), null); }
                            return true;
                        default: throw new AssertionError("unexpected source store call " + method.getName());
                    }
                });
    }
}
