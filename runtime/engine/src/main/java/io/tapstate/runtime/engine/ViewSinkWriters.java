package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Runtime guards applied to the generic writer behind a materialized view. */
public final class ViewSinkWriters {

    private ViewSinkWriters() {
    }

    /**
     * Requires every update and delete to carry the selected alternate key in its earlier row.
     *
     * <p>Discovery can prove that a current value is unique, but cannot prove what a connector sends
     * for an earlier image. A missing old value makes a delete or key-changing update impossible to
     * apply without leaving the old materialized row behind, so the batch is refused before the
     * delegate writes any of it.
     */
    public static SinkWriter requireAlternateKeyInBeforeImage(
            SinkWriter delegate, String view, String key) {
        return guarded(delegate, view, key);
    }

    /**
     * Every writer {@code writers} makes, held to {@link #requireAlternateKeyInBeforeImage}, with the tables prepared
     * as {@code writers} prepares them. Writers that open only onto tables prepared for them need that preparation
     * run as each execution starts, which the engine does for a sink whose writers say they prepare tables; wrapped
     * without saying so, the tables would never be prepared, and every writer would refuse to open.
     */
    public static SupplierEx<? extends SinkWriter> requiringAlternateKeyInBeforeImage(
            SupplierEx<? extends SinkWriter> writers, String view, String key) {
        Objects.requireNonNull(writers, "writers");
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(key, "key");
        if (writers instanceof PreparesTargets targets) {
            return new PreparingGuarded(writers, targets, view, key);
        }
        return () -> guarded(writers.getEx(), view, key);
    }

    /** Writers held to the alternate key, over tables prepared as the writers they wrap prepare them. */
    private record PreparingGuarded(SupplierEx<? extends SinkWriter> writers, PreparesTargets targets, String view,
            String key) implements SupplierEx<SinkWriter>, PreparesTargets {

        @Override
        public SinkWriter getEx() throws Exception {
            return guarded(writers.getEx(), view, key);
        }

        @Override
        public void prepareTargets(HazelcastInstance coordinator) {
            targets.prepareTargets(coordinator);
        }
    }

    private static SinkWriter guarded(SinkWriter delegate, String view, String key) {
        Objects.requireNonNull(delegate, "delegate");
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(key, "key");
        return new SinkWriter() {
            @Override
            public CompletionStage<WriteResult> write(List<Envelope> records) {
                for (Envelope record : records) {
                    if ((record.op() == Op.UPDATE || record.op() == Op.DELETE)
                            && (record.before() == null || !record.before().containsKey(key))) {
                        TapstateException failure = new TapstateException(
                                EngineError.VIEW_KEY_MISSING_FROM_BEFORE_IMAGE,
                                Map.of("view", view, "key", key,
                                        "operation", record.op().name().toLowerCase(Locale.ROOT)),
                                null);
                        return CompletableFuture.failedFuture(failure);
                    }
                }
                return delegate.write(records);
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
    }
}
