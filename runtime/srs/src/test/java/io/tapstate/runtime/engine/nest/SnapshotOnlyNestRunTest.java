package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.ReadMode;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.runtime.srs.StartFrom;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.store.SrsMetaStore;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SnapshotOnlyNestRunTest {

    @Test
    void snapshotOnlyRowsReachANestWithAnOrder() {
        CaptureBatch snapshot = snapshot(List.of(
                Envelope.read(1L, "c1", Map.of("id", 1L), Map.of()),
                Envelope.read(2L, "c2", Map.of("id", 10L, "c1_id", 1L), Map.of())));
        CapturePort source = proxy(CapturePort.class, (ignored, method, arguments) -> {
            if (method.getName().equals("snapshot")) {
                return snapshot;
            }
            throw new AssertionError("unexpected source call: " + method.getName());
        });
        SrsMetaStore meta = unused(SrsMetaStore.class);
        CaptureRunUnit runUnit = new CaptureRunUnit(
                source, new SrsCoordinator(meta), meta, unused(HazelcastInstance.class));
        CaptureRunSpec spec = new CaptureRunSpec(
                new CaptureConfig("mongo", Map.of(), List.of("c1", "c2")),
                ReadMode.SNAPSHOT_ONLY, null, false, "source", "pipeline",
                StartFrom.earliest(), null, 0L);
        List<SourceOrder> accepted = new ArrayList<>();

        CaptureRun run = runUnit.start(spec, event -> accepted.add(NestKeys.orderOf(event)));

        assertThat(run.snapshotCount()).isEqualTo(2L);
        assertThat(accepted).hasSize(2).doesNotContainNull();
    }

    private static CaptureBatch snapshot(List<Envelope> rows) {
        return new CaptureBatch() {
            private final Iterator<Envelope> events = rows.iterator();

            @Override
            public boolean hasNext() {
                return events.hasNext();
            }

            @Override
            public Envelope next() {
                return events.next();
            }

            @Override
            public Optional<io.tapstate.spi.capture.SourcePosition> seam() {
                return Optional.empty();
            }

            @Override
            public void close() {
            }
        };
    }

    private static <T> T unused(Class<T> type) {
        return proxy(type, (ignored, method, arguments) -> {
            throw new AssertionError("unexpected " + type.getSimpleName() + " call: " + method.getName());
        });
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }
}
