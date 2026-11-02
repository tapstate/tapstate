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
    public record Execution( io.tapstate.spi.store.WorkloadOwner owner, long generation) {}
    private final Supplier<Optional<SourceResource>> currentSource;
    private final ConnectionTestResultStore results;
    private final Clock clock;
    private final String fixedSourceId;
    private final Supplier<Execution> execution;
    private final java.util.function.Predicate<Execution> live;
    private final java.util.function.Predicate<ConnectionTestResult> controlledTimeout;
    private final Duration budget;

    public DefaultSourceVerification(Supplier<Optional<SourceResource>> currentSource, ConnectionTestResultStore results, Clock clock) {
        this(currentSource, results, clock, null, new Execution(new io.tapstate.spi.store.WorkloadOwner("local", java.util.UUID.randomUUID().toString()), 1));
    }
    private DefaultSourceVerification(Supplier<Optional<SourceResource>> source, ConnectionTestResultStore store, Clock clock,
            String sourceId, Execution identity) {
        this(source, store, clock, sourceId, () -> identity, identity::equals, ignored -> false, Duration.ofSeconds(60));
    }
    public DefaultSourceVerification(Supplier<Optional<SourceResource>> source, ConnectionTestResultStore store, Clock clock,
            String sourceId, Supplier<Execution> execution, java.util.function.Predicate<Execution> live,
            java.util.function.Predicate<ConnectionTestResult> controlledTimeout, Duration budget) {
        this.currentSource = Objects.requireNonNull(source); this.results = Objects.requireNonNull(store);
        this.clock = Objects.requireNonNull(clock); this.fixedSourceId = sourceId;
        this.execution = Objects.requireNonNull(execution); this.live = Objects.requireNonNull(live);
        this.controlledTimeout = Objects.requireNonNull(controlledTimeout); this.budget = Objects.requireNonNull(budget);
        if (budget.isZero() || budget.isNegative()) throw new IllegalArgumentException("positive test budget required");
    }
    @Override public boolean manages(ConnectionConfig config) { return managesId(config.id()); }
    public boolean managesId(String id) { return fixedSourceId != null ? fixedSourceId.equals(id) : target().map(t -> t.id().equals(id)).orElse(false); }
    @Override public ConnectionTestService.Attempt begin(ConnectionConfig connection) {
        if (!manages(connection)) return ConnectionTestService.Attempt.NONE;
        var target = target().orElseThrow(DefaultSourceVerification::unavailable);
        if (!target.connectionHash().equals(connectionHash(connection)))
            throw new TapstateException(ControlError.MALFORMED_REQUEST, java.util.Map.of(), null);
        var owner = execution.get();
        if (!live.test(owner)) throw unavailable();
        long deadlineNanos = System.nanoTime() + budget.toNanos();
        long now = clock.millis();
        var started = new io.tapstate.spi.store.DefaultSourceTestAttempt(target.id(), target.connector(), target.sourceHash(),
                java.util.UUID.randomUUID().toString(), owner.owner(), owner.generation(), now, Math.addExact(now, budget.toMillis()),
                io.tapstate.spi.store.DefaultSourceTestAttempt.State.CHECKING, null, null);
        results.beginDefaultAttempt(started);
        return new ConnectionTestService.Attempt() {
            private boolean terminal;
            @Override public java.util.OptionalLong deadlineNanos() { return java.util.OptionalLong.of(deadlineNanos); }
            @Override public void completed(ConnectionTestResult result) { throw new IllegalStateException("managed result must use conditional publication"); }
            @Override public void publish(ConnectionTestResult result, ConnectionTestResultStore ignored) {
                boolean timeout = result.outcome() == ConnectionTestResult.Outcome.FAILED && controlledTimeout.test(result);
                if (!target().filter(target::equals).isPresent() || !live.test(owner) || result.testedAt() < started.acceptedAt()
                        || result.testedAt() > clock.millis() || (clock.millis() >= started.deadlineAt() || System.nanoTime() - deadlineNanos >= 0) && !timeout) throw unavailable();
                var state = result.outcome() == ConnectionTestResult.Outcome.PASSED
                        ? io.tapstate.spi.store.DefaultSourceTestAttempt.State.PASSED : io.tapstate.spi.store.DefaultSourceTestAttempt.State.FAILED;
                if (!results.finishDefaultAttempt(started.finish(state, clock.millis(), result), timeout)) throw unavailable();
                terminal = true;
            }
            @Override public void close() {
                if (!terminal) results.finishDefaultAttempt(started.finish(io.tapstate.spi.store.DefaultSourceTestAttempt.State.UNKNOWN,
                        Math.max(clock.millis(), started.acceptedAt()), null), false);
            }
        };
    }

    /** Reads never execute a connector or grant storage, authentication or Pipeline admission. */
    public Verification snapshot() {
        try {
            var target = target().orElse(null);
            if (target == null) return Verification.unknown();
            var attempt = results.findDefaultAttempt(target.id()).orElse(null);
            if (attempt == null || !attempt.sourceHash().equals(target.sourceHash()) || !attempt.connectorId().equals(target.connector())
                    || !live.test(new Execution(attempt.owner(), attempt.sessionGeneration()))) return Verification.unknown();
            if (attempt.state() == io.tapstate.spi.store.DefaultSourceTestAttempt.State.CHECKING)
                return clock.millis() < attempt.deadlineAt() ? new Verification(Status.CHECKING, null, null) : Verification.unknown();
            if (attempt.report() == null) return Verification.unknown();
            var checked = Instant.ofEpochMilli(attempt.report().testedAt());
            if (checked.isAfter(clock.instant())) return Verification.unknown();
            var valid = checked.plus(DISPLAY_LIFETIME);
            return new Verification(!clock.instant().isBefore(valid) ? Status.EXPIRED
                    : attempt.state() == io.tapstate.spi.store.DefaultSourceTestAttempt.State.PASSED ? Status.PASSED : Status.FAILED, checked, valid);
        } catch (TapstateException unavailable) { return Verification.unknown(); }
    }
    public boolean canReadReport(String id) {
        if (!managesId(id)) return true;
        var state = snapshot().status();
        return state == Status.PASSED || state == Status.FAILED || state == Status.EXPIRED;
    }
    private Optional<Target> target() {
        try {
            return currentSource.get().map(source -> new Target(source.id(), source.connector(), CanonicalHash.of(source),
                    connectionHash(new ConnectionConfig(source.id(), source.connector(), source.config()))));
        } catch (TapstateException unavailable) { return Optional.empty(); }
    }
    private static String connectionHash(ConnectionConfig connection) {
        return CanonicalHash.of(new SourceResource(connection.id(), null, connection.connectorId(), connection.settings(), null, null, null, null));
    }
    private static TapstateException unavailable() { return new TapstateException(ControlError.STORAGE_UNAVAILABLE, java.util.Map.of(), null); }
}
