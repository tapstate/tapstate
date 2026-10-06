package io.tapstate.adapters.pdk;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.capture.CaptureConfig;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Serializes local PostgreSQL initial-position preparation for the same resolved connection settings.
 * That function may create a shared publication. The lock covers only that preparation, never seam
 * delivery, snapshot rows or CDC batches, and never retries or reclassifies a native failure.
 */
final class PostgresPublicationStartGate {

    static final PostgresPublicationStartGate LOCAL = new PostgresPublicationStartGate(Duration.ofSeconds(30));
    private final long waitNanos;
    private final ConcurrentHashMap<String, Slot> slots = new ConcurrentHashMap<>();

    PostgresPublicationStartGate(Duration wait) {
        if (wait.isZero() || wait.isNegative()) {
            throw new IllegalArgumentException("initial-position wait must be positive");
        }
        waitNanos = wait.toNanos();
    }

    <T> T sample(CaptureConfig config, PdkConnector.Action<T> prepare) throws Throwable {
        if (!"postgres".equals(config.connectorId())) {
            return prepare.run();
        }
        // The complete physical settings are normalized and hashed. No node, pipeline or execution
        // owner enters the key, and no plaintext connection setting is retained in a visible key.
        String key = CanonicalHash.ofText(JsonWriter.write(ordered(config.settings())));
        Slot slot = slots.compute(key, (ignored, current) -> {
            Slot registered = current == null ? new Slot() : current;
            registered.references++;
            return registered;
        });
        boolean acquired = false;
        try {
            try {
                acquired = slot.turn.tryLock(waitNanos, TimeUnit.NANOSECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new TapstateException(ConnectorError.CAPTURE_FAILED, Map.of(
                        "connector", config.connectorId(),
                        "detail", "initial source position preparation was interrupted while waiting"), interrupted);
            }
            if (!acquired) {
                throw new TapstateException(ConnectorError.CAPTURE_FAILED, Map.of(
                        "connector", config.connectorId(),
                        "detail", "initial source position preparation remained contended for "
                                + TimeUnit.NANOSECONDS.toMillis(waitNanos) + "ms"), null);
            }
            return prepare.run();
        } finally {
            if (acquired) { slot.turn.unlock(); }
            slots.computeIfPresent(key, (ignored, registered) ->
                    --registered.references == 0 ? null : registered);
        }
    }

    private static Object ordered(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new TreeMap<>();
            map.forEach((key, nested) -> result.put(String.valueOf(key), ordered(nested)));
            return result;
        }
        if (value instanceof List<?> list) {
            return list.stream().map(PostgresPublicationStartGate::ordered).toList();
        }
        return value;
    }

    private static final class Slot {
        private final ReentrantLock turn = new ReentrantLock(true);
        private int references;
    }
}
