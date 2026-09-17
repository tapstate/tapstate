package io.tapstate.cli;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * What a pipeline had moved, and how far behind it stood, as the metrics face reported it at one
 * instant. Two of these a little apart are what a rate is made of; one alone is not, and says so.
 *
 * <p>The instant is the pipeline's own: when the observation the numbers came out of was taken, as the
 * server stamped it. A rate is a difference of counts over a difference of <em>these</em>, never over
 * this machine's clock. The two agree while the publisher keeps up and part exactly when it stops: two
 * readings a second apart by the wall that carry the same observation would, over the wall, be a
 * pipeline moving nothing, and over the observation's time are one reading — from which no rate can
 * honestly be given.
 *
 * @param observedAt          when the observation these numbers came out of was taken, or null when the
 *                            face did not say — which makes no rate, rather than a rate over a guess
 * @param recordsByDirection  rows the run has moved so far, in each direction it counts (a total over
 *                            every table)
 * @param lagSecondsByTable   how far behind each table stands, in seconds, as of the same instant
 */
record MovementReading(Instant observedAt, Map<String, Long> recordsByDirection, Map<String, Long> lagSecondsByTable) {

    MovementReading {
        recordsByDirection = Collections.unmodifiableMap(new TreeMap<>(
                recordsByDirection == null ? Map.of() : recordsByDirection));
        lagSecondsByTable = Collections.unmodifiableMap(new TreeMap<>(
                lagSecondsByTable == null ? Map.of() : lagSecondsByTable));
    }

    /** A rate, or the reason there is none. */
    sealed interface Movement {

        /** Rows per second in each direction, over the span of the pipeline's own time it was measured across. */
        record Rate(Duration over, Map<String, Double> rowsPerSecondByDirection) implements Movement {

            public Rate {
                rowsPerSecondByDirection = Collections.unmodifiableMap(new TreeMap<>(rowsPerSecondByDirection));
            }
        }

        /** No rate, and why not. The reason is a sentence for a reader, not a code. */
        record NotKnown(String reason) implements Movement {
        }
    }

    /**
     * The rate from {@code earlier} to this reading, over the pipeline's own time.
     *
     * <p>Not known when either reading carries no time, when the time has not moved on — the
     * publisher's reading is the same one, however long the wall says has passed — or when no
     * direction counted in this reading can be measured. A direction the earlier reading did not
     * count yet stood at nought then: the counter is cumulative and begins at nothing, so a run whose
     * first confirmed rows land between the two readings is rated from nought, not left out. A
     * counter that went backwards between the two is a run that restarted in between, and the
     * difference across a restart is not a rate.
     */
    Movement since(MovementReading earlier) {
        if (earlier == null) {
            return new Movement.NotKnown("one reading gives no rate");
        }
        if (observedAt == null || earlier.observedAt == null) {
            return new Movement.NotKnown("the reading carries no time, so no span to measure over");
        }
        Duration over = Duration.between(earlier.observedAt, observedAt);
        if (over.isZero() || over.isNegative()) {
            return new Movement.NotKnown("the reading has not advanced, so this is still one reading");
        }
        double seconds = over.toNanos() / 1_000_000_000.0;
        Map<String, Double> rates = new TreeMap<>();
        recordsByDirection.forEach((direction, now) -> {
            long before = earlier.recordsByDirection.getOrDefault(direction, 0L);
            if (now >= before) {
                rates.put(direction, (now - before) / seconds);
            }
        });
        if (rates.isEmpty()) {
            return new Movement.NotKnown(recordsByDirection.isEmpty()
                    ? "no records counter is published"
                    : "the counter went backwards between the readings, which is a run that restarted, not a rate");
        }
        return new Movement.Rate(over, rates);
    }

    /** The rate as one line: each direction, then the span it was measured over. */
    static String describe(Movement movement) {
        return switch (movement) {
            case Movement.NotKnown notKnown -> "not known -- " + notKnown.reason();
            case Movement.Rate rate -> rate.rowsPerSecondByDirection().entrySet().stream()
                    .map(entry -> entry.getKey() + " " + String.format(Locale.ROOT, "%.1f", entry.getValue()) + " rows/s")
                    .collect(Collectors.joining(" · "))
                    + " (over " + seconds(rate.over()) + " of the pipeline's own time)";
        };
    }

    /** How far behind each table stands, as one line, or that nothing says. */
    String describeLag() {
        if (lagSecondsByTable.isEmpty()) {
            return "not published";
        }
        return lagSecondsByTable.entrySet().stream()
                .map(entry -> entry.getKey() + " " + human(entry.getValue()))
                .collect(Collectors.joining(" · "));
    }

    /** A span a person reads at a glance, to a tenth of a second. */
    static String seconds(Duration span) {
        return String.format(Locale.ROOT, "%.1fs", span.toNanos() / 1_000_000_000.0);
    }

    /** Whole seconds a person reads at a glance: {@code 3s}, {@code 2m5s}, {@code 1h2m}. */
    static String human(long seconds) {
        if (seconds < 60) {
            return seconds + "s";
        }
        long minutes = seconds / 60;
        if (minutes < 60) {
            return minutes + "m" + (seconds % 60) + "s";
        }
        long hours = minutes / 60;
        return hours + "h" + (minutes % 60) + "m";
    }
}
