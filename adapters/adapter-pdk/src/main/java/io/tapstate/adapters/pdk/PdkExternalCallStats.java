package io.tapstate.adapters.pdk;

import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.HistogramValue;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Member-local counts and elapsed time for completed calls into the PDK read and write functions.
 * One call includes the connector's callbacks. No connector id, pipeline id, row value, or exception
 * text is retained here, and a long-running change stream is not mistaken for a batch call.
 */
public final class PdkExternalCallStats {

    public enum Call {
        SNAPSHOT_READ("snapshot_read"), SINK_WRITE("sink_write");

        private final String label;

        Call(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    public enum Outcome {
        SUCCESS("success"), FAILURE("failure");

        private final String label;

        Outcome(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    public record Reading(long count, HistogramValue duration) {
    }

    private static final HistogramBounds BOUNDS = HistogramBounds.CONNECTOR_EXTERNAL_CALL_DURATION;
    private static final PdkExternalCallStats DISABLED = new PdkExternalCallStats(false);

    private final Map<Call, Map<Outcome, Cell>> cells = new EnumMap<>(Call.class);
    private final boolean enabled;
    private final LongSupplier nanoTime;

    public PdkExternalCallStats(boolean enabled) {
        this(enabled, System::nanoTime);
    }

    public static PdkExternalCallStats disabled() {
        return DISABLED;
    }

    PdkExternalCallStats(boolean enabled, LongSupplier nanoTime) {
        this.enabled = enabled;
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        for (Call call : Call.values()) {
            Map<Outcome, Cell> outcomes = new EnumMap<>(Outcome.class);
            for (Outcome outcome : Outcome.values()) {
                outcomes.put(outcome, new Cell());
            }
            cells.put(call, outcomes);
        }
    }

    /** Begins one call at a monotonic point; disabled collection avoids clock reads. */
    long begin() {
        return enabled ? nanoTime.getAsLong() : 0L;
    }

    /** Records only a returned or thrown PDK call, never a row or a long-running stream tick. */
    void completed(Call call, long began, boolean success) {
        if (!enabled) {
            return;
        }
        long elapsed = Math.max(0L, nanoTime.getAsLong() - began);
        cells.get(call).get(success ? Outcome.SUCCESS : Outcome.FAILURE).record(elapsed);
    }

    /** An immutable, fixed-shape snapshot for the process metric projection. */
    public Map<Call, Map<Outcome, Reading>> snapshot() {
        if (!enabled) {
            return Map.of();
        }
        Map<Call, Map<Outcome, Reading>> result = new EnumMap<>(Call.class);
        for (Call call : Call.values()) {
            Map<Outcome, Reading> outcomes = new EnumMap<>(Outcome.class);
            for (Outcome outcome : Outcome.values()) {
                outcomes.put(outcome, cells.get(call).get(outcome).snapshot());
            }
            result.put(call, Map.copyOf(outcomes));
        }
        return Map.copyOf(result);
    }

    private static final class Cell {
        private long count;
        private double sumSeconds;
        private final long[] buckets = new long[BOUNDS.buckets()];

        synchronized void record(long nanos) {
            double seconds = nanos / 1_000_000_000.0;
            count++;
            sumSeconds += seconds;
            int bucket = 0;
            while (bucket < BOUNDS.bounds().size() && seconds > BOUNDS.bounds().get(bucket)) {
                bucket++;
            }
            buckets[bucket]++;
        }

        synchronized Reading snapshot() {
            List<Long> counts = new ArrayList<>(buckets.length);
            for (long bucket : buckets) {
                counts.add(bucket);
            }
            return new Reading(count, BOUNDS.value(count, sumSeconds, counts));
        }
    }
}
