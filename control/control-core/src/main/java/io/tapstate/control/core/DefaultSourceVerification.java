package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResultStore;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/** One runtime's bounded association between its managed Source and a completed formal probe. */
public final class DefaultSourceVerification implements ConnectionTestService.Observer {
    public static final Duration DISPLAY_LIFETIME = Duration.ofMinutes(10);
    public enum Status { UNKNOWN, CHECKING, PASSED, FAILED, EXPIRED }
    public record Verification(Status status, Instant checkedAt, Instant validUntil) {
        public Verification {
            Objects.requireNonNull(status, "status");
            boolean completed = status == Status.PASSED || status == Status.FAILED || status == Status.EXPIRED;
            if (completed != (checkedAt != null && validUntil != null)
                    || !completed && (checkedAt != null || validUntil != null)
                    || completed && !validUntil.isAfter(checkedAt))
                throw new IllegalArgumentException("invalid verification time");
        }
        public static Verification unknown() { return new Verification(Status.UNKNOWN, null, null); }
    }
    private record Target(String id, String connector, String sourceHash, String connectionHash) {}
    private record Observation(long sequence, Target target, ConnectionTestResult result) {}
    private final Supplier<Optional<SourceResource>> currentSource;
    private final ConnectionTestResultStore results;
    private final Clock clock;
    private long sequence;
    // Immutable observation snapshots are replaced under the verification monitor.
    @SuppressWarnings("java:S3077")
    private volatile Observation observation;

    public DefaultSourceVerification(Supplier<Optional<SourceResource>> currentSource,
            ConnectionTestResultStore results, Clock clock) {
        this.currentSource = Objects.requireNonNull(currentSource, "currentSource");
        this.results = Objects.requireNonNull(results, "results");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override public ConnectionTestService.Attempt begin(ConnectionConfig connection) {
        var current = target();
        if (current.isEmpty() || !current.get().connectionHash().equals(connectionHash(connection)))
            return ConnectionTestService.Attempt.NONE;
        var target = current.get();
        final long started;
        synchronized (this) {
            started = ++sequence;
            observation = new Observation(started, target, null);
        }
        return new ConnectionTestService.Attempt() {
            private boolean completed;
            @Override public void completed(ConnectionTestResult result) {
                boolean usable = result.connectionId().equals(target.id()) && result.connectorId().equals(target.connector())
                        && result.testedAt() > 0 && result.testedAt() <= clock.millis()
                        && target().filter(target::equals).isPresent();
                synchronized (DefaultSourceVerification.this) {
                    if (sequence == started) observation = usable ? new Observation(started, target, result) : null;
                    completed = true;
                }
            }
            @Override public void close() {
                synchronized (DefaultSourceVerification.this) {
                    if (!completed && sequence == started) observation = null;
                }
            }
        };
    }

    /** Reads never execute a connector or grant storage, authentication, or Pipeline admission. */
    public Verification snapshot() {
        var observed = observation;
        if (observed == null || target().filter(observed.target()::equals).isEmpty()) return Verification.unknown();
        if (observed.result() == null) return new Verification(Status.CHECKING, null, null);
        try {
            if (results.find(observed.target().id()).filter(observed.result()::equals).isEmpty()) return Verification.unknown();
        } catch (TapstateException unavailable) {
            return Verification.unknown();
        }
        if (observation != observed || target().filter(observed.target()::equals).isEmpty()) return Verification.unknown();
        var checkedAt = Instant.ofEpochMilli(observed.result().testedAt());
        if (checkedAt.isAfter(clock.instant())) return Verification.unknown();
        var validUntil = checkedAt.plus(DISPLAY_LIFETIME);
        var status = !clock.instant().isBefore(validUntil) ? Status.EXPIRED
                : observed.result().outcome() == ConnectionTestResult.Outcome.PASSED ? Status.PASSED : Status.FAILED;
        return new Verification(status, checkedAt, validUntil);
    }

    private Optional<Target> target() {
        try {
            return currentSource.get().map(source -> new Target(source.id(), source.connector(), CanonicalHash.of(source),
                    connectionHash(new ConnectionConfig(source.id(), source.connector(), source.config()))));
        } catch (TapstateException unavailable) {
            return Optional.empty();
        }
    }
    private static String connectionHash(ConnectionConfig connection) {
        return CanonicalHash.of(new SourceResource(connection.id(), null, connection.connectorId(), connection.settings(),
                null, null, null, null));
    }
}
