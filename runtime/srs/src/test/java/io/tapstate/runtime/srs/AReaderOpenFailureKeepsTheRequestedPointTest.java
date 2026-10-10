package io.tapstate.runtime.srs;

import com.hazelcast.ringbuffer.Ringbuffer;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.CaptureReadAttempt;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AReaderOpenFailureKeepsTheRequestedPointTest {
    @Test
    void aSynchronousCodedReaderRefusalIsRecordedBeforeCaptureCleanupCanRetireItsClaim() {
        var fence = new WorkloadClaimFence(new WorkloadClaimKey("east", WorkloadClaimType.CAPTURE, "capture"),
                new WorkloadOwner("node", "boot"), 1, 0, 1, 1);
        Map<String, Object> params = Map.of("requested", "original-point", "earliest", "retained-head", "retention", "2h");
        TapstateException actual = new TapstateException(CaptureError.START_FROM_OUTSIDE_WINDOW, params, null);
        List<String> order = new ArrayList<>();
        AtomicReference<Map<String, Object>> saved = new AtomicReference<>();
        SrsMetaStore meta = (SrsMetaStore) Proxy.newProxyInstance(SrsMetaStore.class.getClassLoader(),
                new Class<?>[]{SrsMetaStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("beginCaptureReadAttempt")) {
                        order.add("allocated");
                        return Optional.of(new CaptureReadAttempt("chain", 1, 2, fence, List.of("orders"),
                                CaptureReadAttempt.Kind.RESUME, "original-point", null, Instant.EPOCH));
                    }
                    if (method.getName().equals("recordCaptureReadFailure")) {
                        order.add("failure");
                        assertThat(args[1]).isEqualTo(actual.code().code());
                        @SuppressWarnings("unchecked")
                        Map<String, Object> recorded = (Map<String, Object>) args[2];
                        saved.set(recorded);
                        assertThat(args[3]).isEqualTo("stop-and-clear-source-state");
                        return true;
                    }
                    throw new AssertionError("unexpected source store method " + method.getName());
                });
        CapturePort port = (CapturePort) Proxy.newProxyInstance(CapturePort.class.getClassLoader(),
                new Class<?>[]{CapturePort.class}, (proxy, method, args) -> {
                    if (method.getName().equals("cdc")) { throw actual; }
                    throw new AssertionError("unexpected source method " + method.getName());
                });
        @SuppressWarnings("unchecked")
        Ringbuffer<SrsItem> ring = (Ringbuffer<SrsItem>) Proxy.newProxyInstance(Ringbuffer.class.getClassLoader(),
                new Class<?>[]{Ringbuffer.class}, (proxy, method, args) -> {
                    throw new AssertionError("a rejected reader must not write a ring");
                });
        CdcChain chain = new CdcChain(new SrsWriteGate(new SrsRingbuffer(ring)), meta, "chain", 1, 1, fence);
        CaptureHealth health = new CaptureHealth();
        assertThatThrownBy(() -> CdcPhase.runDurable(port, new CaptureConfig("mongo", Map.of(), List.of("orders")),
                CaptureStart.resume(new SourcePosition("original-point")),
                Map.of("orders", new CdcPhase.TableRoute(chain, List::of, sequence -> { })), health, new AtomicLong()))
                .isSameAs(actual);
        assertThat(order).containsExactly("allocated", "failure");
        assertThat(saved.get()).isEqualTo(params);
        assertThat(health.failure()).containsSame(actual);
        assertThat(health.readerAccepted()).isFalse();
    }
}
