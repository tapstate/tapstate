package io.tapstate.runtime.engine;

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
