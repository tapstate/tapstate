package io.tapstate.spi.store;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A disposable, incarnation-scoped cache of closed history buckets. Raw samples remain authoritative;
 * callers must descend to a finer resolution or raw history when a bucket is missing, expired, marked
 * for fallback, or covers only part of the requested interval.
 */
public interface HistoryRollupStore {

    int MAX_PAGE_SIZE = 256;
    int MAX_FRAGMENTS = 64;
    int MAX_GAPS = 64;
    int MAX_LAGS_PER_FRAGMENT = 64;
    int MAX_TABLE_NAME_LENGTH = 256;
    Duration MAX_CACHE_AGE = Duration.ofMinutes(5);

    /** Only these fixed levels are persisted; minute-class raw samples are held separately. */
    enum Resolution {
        PT5M(Duration.ofMinutes(5)),
        PT30M(Duration.ofMinutes(30)),
        PT1H(Duration.ofHours(1)),
        PT3H(Duration.ofHours(3)),
        PT6H(Duration.ofHours(6));

        private final Duration duration;

        Resolution(Duration duration) {
            this.duration = duration;
        }

        public Duration duration() {
            return duration;
        }
    }

    /** Absence is the reserved upgrade-era legacy scope, never a newly assigned incarnation. */
    record Scope(Optional<String> incarnationId) {
        public Scope {
            Objects.requireNonNull(incarnationId, "incarnationId");
            incarnationId.ifPresent(value -> {
                if (value.isBlank()) {
                    throw new IllegalArgumentException("a rollup incarnation is not blank");
                }
            });
        }

        public static Scope legacy() {
            return new Scope(Optional.empty());
        }

        public static Scope incarnation(String id) {
            return new Scope(Optional.of(Objects.requireNonNull(id, "id")));
        }
    }

    /** The deterministic identity of one UTC-aligned bucket, independent of its derived contents. */
    record Key(String pipelineId, Scope scope, Resolution resolution, Instant bucketStart) {
        public Key {
            Objects.requireNonNull(pipelineId, "pipelineId");
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(resolution, "resolution");
            Objects.requireNonNull(bucketStart, "bucketStart");
            if (pipelineId.isBlank()) {
                throw new IllegalArgumentException("a rollup pipeline id is not blank");
            }
            long seconds = resolution.duration().toSeconds();
            if (bucketStart.getNano() != 0 || Math.floorMod(bucketStart.getEpochSecond(), seconds) != 0) {
                throw new IllegalArgumentException("a rollup bucket begins on its UTC resolution boundary");
            }
        }

        public Instant bucketEnd() {
            return bucketStart.plus(resolution.duration());
        }
    }

    /** Exact decimal values; serialization must not pass through a floating-point representation. */
    record Rate(BigDecimal delta, BigDecimal averageRate, BigDecimal maxRate) {
        public Rate {
            Objects.requireNonNull(delta, "delta");
            Objects.requireNonNull(averageRate, "averageRate");
            Objects.requireNonNull(maxRate, "maxRate");
            if (!boundedDecimal(delta) || !boundedDecimal(averageRate) || !boundedDecimal(maxRate)) {
                throw new IllegalArgumentException("rollup rates are non-negative bounded decimals");
            }
        }

        private static boolean boundedDecimal(BigDecimal value) {
            return value.signum() >= 0 && value.precision() <= 34
                    && value.scale() >= -30 && value.scale() <= 30;
        }
    }

    /** Additive counter values before the public nine-place rounding. Optional in older cache rows. */
    record CounterStats(BigDecimal delta, long coveredNanos, BigDecimal maxRate) {
        public CounterStats {
            Objects.requireNonNull(delta, "delta");
            Objects.requireNonNull(maxRate, "maxRate");
            if (delta.signum() < 0 || maxRate.signum() < 0 || coveredNanos <= 0
                    || coveredNanos > Duration.ofHours(6).toNanos()
                    || delta.precision() > 64 || maxRate.precision() > 64
                    || delta.scale() < -30 || delta.scale() > 30
                    || maxRate.scale() < -30 || maxRate.scale() > 30) {
                throw new IllegalArgumentException("rollup counter statistics are bounded");
            }
        }
    }

    record Lag(String table, Instant observedAt, long last, long max) {
        public Lag {
            Objects.requireNonNull(table, "table");
            Objects.requireNonNull(observedAt, "observedAt");
            if (table.isBlank() || table.length() > MAX_TABLE_NAME_LENGTH) {
                throw new IllegalArgumentException("a rollup lag table name is bounded and not blank");
            }
        }
    }

    enum StartReason { WINDOW_START, CONTINUATION, COUNTER_RESET, GAP }

    /** One reset-aware projection fragment within a bucket. Frequent boundaries use the fallback marker. */
    record Fragment(int segment, StartReason startReason, Instant intervalStart, Instant intervalEnd,
            Rate recordsOut, Rate bytesOut, List<Lag> lag,
            CounterStats recordsOutStats, CounterStats bytesOutStats,
            RateHistoryStore.Key resumeAfter, Instant resumeAt) {
        public Fragment {
            Objects.requireNonNull(startReason, "startReason");
            Objects.requireNonNull(intervalStart, "intervalStart");
            Objects.requireNonNull(intervalEnd, "intervalEnd");
            lag = List.copyOf(Objects.requireNonNull(lag, "lag"));
            if (segment < 0 || intervalStart.isAfter(intervalEnd) || lag.size() > MAX_LAGS_PER_FRAGMENT) {
                throw new IllegalArgumentException("a rollup fragment has bounded ordered content");
            }
            if ((recordsOutStats != null && recordsOut == null)
                    || (bytesOutStats != null && bytesOut == null)) {
                throw new IllegalArgumentException("rollup statistics require a projected rate");
            }
            if ((resumeAfter == null) != (resumeAt == null)
                    || (resumeAfter != null && resumeAfter.internalKey().length() > 256)) {
                throw new IllegalArgumentException("a rollup resume anchor is complete and bounded");
            }
        }

        public Fragment(int segment, StartReason startReason, Instant intervalStart, Instant intervalEnd,
                Rate recordsOut, Rate bytesOut, List<Lag> lag) {
            this(segment, startReason, intervalStart, intervalEnd, recordsOut, bytesOut, lag,
                    null, null, null, null);
        }
    }

    enum GapReason { SAMPLE_GAP }

    record Gap(int segment, Instant intervalStart, Instant intervalEnd, GapReason reason) {
        public Gap {
            Objects.requireNonNull(intervalStart, "intervalStart");
            Objects.requireNonNull(intervalEnd, "intervalEnd");
            Objects.requireNonNull(reason, "reason");
            if (segment < 0 || !intervalStart.isBefore(intervalEnd)) {
                throw new IllegalArgumentException("a rollup gap has a non-empty interval");
            }
        }
    }

    /**
     * A cache entry is fresh only before {@code validUntil}. Its deadline is no later than five minutes
     * after the earliest raw input read began; cascading uses the earliest input deadline, not a new one.
     * Computation may happen after that read, but only once the bucket is closed and before the deadline.
     * An empty clean bucket proves that its input had no projection at the read time.
     */
    record Bucket(Key key, Instant computedAt, Instant inputReadStartedAt, Instant validUntil,
            boolean requiresFinerResolution,
            List<Fragment> fragments, List<Gap> gaps, int inWindowSamples) {
        public Bucket {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(computedAt, "computedAt");
            Objects.requireNonNull(inputReadStartedAt, "inputReadStartedAt");
            Objects.requireNonNull(validUntil, "validUntil");
            computedAt = computedAt.truncatedTo(ChronoUnit.MILLIS);
            inputReadStartedAt = inputReadStartedAt.truncatedTo(ChronoUnit.MILLIS);
            validUntil = validUntil.truncatedTo(ChronoUnit.MILLIS);
            fragments = List.copyOf(Objects.requireNonNull(fragments, "fragments"));
            gaps = List.copyOf(Objects.requireNonNull(gaps, "gaps"));
            if (!validUntil.isAfter(inputReadStartedAt)
                    || validUntil.isAfter(inputReadStartedAt.plus(MAX_CACHE_AGE))) {
                throw new IllegalArgumentException("a rollup cache deadline is within its input freshness window");
            }
            if (computedAt.isBefore(key.bucketEnd()) || computedAt.isBefore(inputReadStartedAt)
                    || !computedAt.isBefore(validUntil)) {
                throw new IllegalArgumentException("a rollup is computed after bucket close and before expiry");
            }
            if (fragments.size() > MAX_FRAGMENTS || gaps.size() > MAX_GAPS
                    || inWindowSamples < -1
                    || (requiresFinerResolution && (!fragments.isEmpty() || !gaps.isEmpty()))) {
                throw new IllegalArgumentException("a rollup bucket has bounded fragments or a fallback marker");
            }
            for (Fragment fragment : fragments) {
                if (fragment.intervalStart().isBefore(key.bucketStart())
                        || fragment.intervalEnd().isAfter(key.bucketEnd())) {
                    throw new IllegalArgumentException("a rollup fragment is within its bucket");
                }
                for (Lag lag : fragment.lag()) {
                    if (lag.observedAt().isBefore(key.bucketStart())
                            || !lag.observedAt().isBefore(key.bucketEnd())) {
                        throw new IllegalArgumentException("a rollup lag reading is within its bucket");
                    }
                }
            }
            for (Gap gap : gaps) {
                if (gap.intervalStart().isBefore(key.bucketStart())
                        || gap.intervalEnd().isAfter(key.bucketEnd())) {
                    throw new IllegalArgumentException("a rollup gap is within its bucket");
                }
            }
        }

        public Bucket(Key key, Instant computedAt, Instant inputReadStartedAt, Instant validUntil,
                boolean requiresFinerResolution, List<Fragment> fragments, List<Gap> gaps) {
            this(key, computedAt, inputReadStartedAt, validUntil, requiresFinerResolution,
                    fragments, gaps, -1);
        }

        public boolean usableAt(Instant at) {
            Objects.requireNonNull(at, "at");
            return !requiresFinerResolution && !at.isBefore(computedAt) && at.isBefore(validUntil);
        }
    }

    /** Idempotently replaces one derived bucket under its deterministic key. */
    void upsert(Bucket bucket);

    Optional<Bucket> read(Key key);

    /** Reads a bounded, ascending prefix of one scope/resolution in {@code [from,to)}. */
    List<Bucket> readRange(String pipelineId, Scope scope, Resolution resolution,
            Instant from, Instant to, int limit);

    /** Removes only the old incarnation captured by artifact deletion. */
    void deleteIncarnation(String pipelineId, String incarnationId);

    /** Removes only upgrade-era unscoped buckets, without touching a recreated resource. */
    void deleteLegacy(String pipelineId);

    Duration retention();
}
