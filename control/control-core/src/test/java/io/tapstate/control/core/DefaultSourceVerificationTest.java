package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.*;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

class DefaultSourceVerificationTest {
    private static final long NOW = 1_790_000_000_000L;
    private static final String URI = "mongodb://user:private-value@localhost:27017/views";
    private static SourceResource source(String uri) {
        return new SourceResource("views", null, "mongodb", Map.of("uri", uri), null, null, null, null);
    }
    private static final class MutableClock extends Clock {
        long now = NOW;
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return Instant.ofEpochMilli(now); }
    }
    private static final class Fixture {
        final MutableClock clock = new MutableClock();
        final AtomicReference<SourceResource> current = new AtomicReference<>(source(URI));
        final Map<String, ConnectionTestResult> saved = new ConcurrentHashMap<>();
        final Map<String, DefaultSourceTestAttempt> attempts = new ConcurrentHashMap<>();
        final ConnectionTestResultStore results = new ConnectionTestResultStore() {
            @Override public void save(ConnectionTestResult result) { saved.put(result.connectionId(), result); }
            @Override public Optional<ConnectionTestResult> find(String id) {
                return attempts.containsKey(id) ? Optional.ofNullable(attempts.get(id).report()) : Optional.ofNullable(saved.get(id));
            }
            @Override public void beginDefaultAttempt(DefaultSourceTestAttempt value) { attempts.put(value.connectionId(), value); }
            @Override public boolean finishDefaultAttempt(DefaultSourceTestAttempt value, boolean timeout) {
                var changed = new AtomicBoolean();
                attempts.computeIfPresent(value.connectionId(), (id, previous) -> {
                    if (!previous.attemptId().equals(value.attemptId()) || previous.state() != DefaultSourceTestAttempt.State.CHECKING) return previous;
                    changed.set(true); return value;
                });
                return changed.get();
            }
            @Override public Optional<DefaultSourceTestAttempt> findDefaultAttempt(String id) { return Optional.ofNullable(attempts.get(id)); }
        };
        final ArtifactStore artifacts = new ArtifactStore() {
            @Override public void saveAll(List<Resource> values) { throw new AssertionError("verification cannot save Source configuration"); }
            @Override public Optional<Resource> get(String id) { return Optional.ofNullable(current.get()).filter(s -> s.id().equals(id)).map(s -> s); }
            @Override public List<Resource> list() { return current.get() == null ? List.of() : List.of(current.get()); }
        };
        final DefaultSourceVerification verification = new DefaultSourceVerification(() -> Optional.ofNullable(current.get()), results, clock);
        final List<AuditRecord> audit = new ArrayList<>();
        ConnectionTestService service(io.tapstate.runtime.probe.ConnectionProbe probe) {
            return new ConnectionTestService(probe, results, new AuditGate(audit::add, clock), null,
                    new SourceConnectionResolver(artifacts), verification);
        }
        ConnectionTestResult result(ConnectionTestResult.Outcome outcome) {
            return new ConnectionTestResult("views", "mongodb", outcome, List.of(), clock.millis());
        }
        void test(ConnectionTestResult.Outcome outcome) { service(ignored -> result(outcome)).test("views", "mongodb", current.get().config(), "admin"); }
    }

    @Test void aCompletedFormalTestPublishesItsExactTimeWithoutExposingConfiguration() {
        var f = new Fixture(); f.test(ConnectionTestResult.Outcome.PASSED);
        var status = f.verification.snapshot();
        assertThat(status.status()).isEqualTo(DefaultSourceVerification.Status.PASSED);
        assertThat(status.checkedAt()).isEqualTo(Instant.ofEpochMilli(NOW));
        assertThat(status.validUntil()).isEqualTo(Instant.ofEpochMilli(NOW).plusSeconds(600));
        assertThat(status.toString()).doesNotContain(URI, "private-value");
        assertThat(f.audit).singleElement().satisfies(record -> assertThat(record.operationId()).isEqualTo("connection.test"));
    }
    @Test void aStoredResultWithoutThisRuntimeAssociationIsNotTrusted() {
        var f = new Fixture(); f.test(ConnectionTestResult.Outcome.PASSED);
        var reopened = new DefaultSourceVerification(() -> Optional.of(f.current.get()), f.results, f.clock);
        assertThat(reopened.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.UNKNOWN);
    }
    @Test void testingAnotherConfigurationUnderTheManagedIdCannotCertifyTheStoredTarget() {
        var f = new Fixture(); f.test(ConnectionTestResult.Outcome.PASSED); f.clock.now++;
        assertThatThrownBy(() -> f.service(ignored -> f.result(ConnectionTestResult.Outcome.PASSED))
                .test("views", "mongodb", Map.of("uri", "mongodb://other:27017/wrong"), "admin"))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
        assertThat(f.verification.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.PASSED);
    }
    @Test void unrelatedSourceTestsDoNotReplaceTheManagedTargetObservation() {
        var f = new Fixture(); f.test(ConnectionTestResult.Outcome.PASSED);
        f.service(config -> new ConnectionTestResult(config.id(), config.connectorId(), ConnectionTestResult.Outcome.FAILED, List.of(), NOW))
                .test("other", "mongodb", Map.of("uri", URI), "admin");
        assertThat(f.verification.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.PASSED);
    }
    @Test void changedOrMissingSourceCannotReuseAnOldPass() {
        var f = new Fixture(); f.test(ConnectionTestResult.Outcome.PASSED);
        f.current.set(source("mongodb://changed:27017/views"));
        assertThat(f.verification.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.UNKNOWN);
        f.current.set(null);
        assertThat(f.verification.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.UNKNOWN);
    }
    @Test void aSourceChangeDuringTheProbePreventsPublication() {
        var f = new Fixture();
        assertThatThrownBy(() -> f.service(ignored -> { f.current.set(source("mongodb://changed:27017/views")); return f.result(ConnectionTestResult.Outcome.PASSED); })
                .test("views", "mongodb", f.current.get().config(), "admin")).isInstanceOf(TapstateException.class);
        assertThat(f.verification.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.UNKNOWN);
    }
    @Test void aThrownRefreshClearsTheOldPassEvenWhenTheStoreStillContainsIt() {
        var f = new Fixture(); f.test(ConnectionTestResult.Outcome.PASSED);
        assertThatThrownBy(() -> f.service(ignored -> {
            assertThat(f.verification.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.CHECKING);
            throw new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
        }).test("views", "mongodb", f.current.get().config(), "admin")).isInstanceOf(TapstateException.class);
        assertThat(f.results.find("views")).isEmpty();
        assertThat(f.verification.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.UNKNOWN);
    }
    @Test void aFailedCheckPublishesFailureAndTheNewTime() {
        var f = new Fixture(); f.test(ConnectionTestResult.Outcome.PASSED); f.clock.now += 1000;
        f.test(ConnectionTestResult.Outcome.FAILED);
        assertThat(f.verification.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.FAILED);
        assertThat(f.verification.snapshot().checkedAt()).isEqualTo(f.clock.instant());
    }
    @Test void aCheckExpiresWithoutDiscardingItsActualTime() {
        var f = new Fixture(); f.test(ConnectionTestResult.Outcome.PASSED); f.clock.now += 600_000;
        assertThat(f.verification.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.EXPIRED);
        assertThat(f.verification.snapshot().checkedAt()).isEqualTo(Instant.ofEpochMilli(NOW));
    }
    @Test void aFutureDatedProbeCannotPublishSuccess() {
        var f = new Fixture();
        assertThatThrownBy(() -> f.service(ignored -> new ConnectionTestResult("views", "mongodb", ConnectionTestResult.Outcome.PASSED, List.of(), NOW + 1000))
                .test("views", "mongodb", f.current.get().config(), "admin")).isInstanceOf(TapstateException.class);
        assertThat(f.verification.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.UNKNOWN);
    }
    @Test void anOlderSuccessfulCompletionCannotReplaceANewerFailure() throws Exception {
        var f = new Fixture(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var old = executor.submit(() -> f.service(ignored -> {
                entered.countDown();
                try { if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("release missing"); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                return f.result(ConnectionTestResult.Outcome.PASSED);
            }).test("views", "mongodb", f.current.get().config(), "admin"));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                f.test(ConnectionTestResult.Outcome.FAILED);
                assertThat(f.verification.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.FAILED);
            } finally { release.countDown(); }
            assertThatThrownBy(() -> old.get(5, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TapstateException.class);
            assertThat(f.verification.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.FAILED);
            f.test(ConnectionTestResult.Outcome.PASSED);
            assertThat(f.verification.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.PASSED);
        }
    }
    @Test void aFailedAuditDoesNotProbeAndCannotLeaveOldSuccessVisible() {
        var f = new Fixture(); f.test(ConnectionTestResult.Outcome.PASSED);
        var service = new ConnectionTestService(ignored -> { throw new AssertionError("must not probe"); }, f.results,
                new AuditGate(ignored -> { throw new IllegalStateException("audit unavailable"); }, f.clock), null,
                new SourceConnectionResolver(f.artifacts), f.verification);
        assertThatThrownBy(() -> service.test("views", "mongodb", f.current.get().config(), "admin")).isInstanceOf(TapstateException.class);
        assertThat(f.verification.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.UNKNOWN);
    }
}
