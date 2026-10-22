package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationMode;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.core.validation.ValidationRootHintBatch;
import io.tapstate.core.validation.ValidationRunIdentity;
import io.tapstate.runtime.engine.validation.ValidationRootHintOutbox;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStartedListener;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ValidationCaptureHintRegistryTest {
    private static final ValidationCaptureHintTap.Limits WORK = new ValidationCaptureHintTap.Limits(1000, 1, 1_048_576);

    @Test
    void existingListenerReceivesChangesAcrossRegistrationAndRetirement() {
        var calls = new AtomicInteger(); var token = Optional.of(new SourcePosition("opaque"));
        try (var registry = registry(2); var outbox = outbox("one")) {
            CaptureListener installed = registry.install((events, position) -> {
                assertThat(position).isSameAs(token); calls.incrementAndGet();
            });
            installed.onBatch(List.of(event(1)), token);
            var binding = binding(outbox);
            var registered = registry.register(scope("opening"), binding);
            assertThat(registry.register(scope("opening"), binding)).isSameAs(registered);
            installed.onBatch(List.of(event(2)), token);
            assertThat(outbox.snapshot().packets()).isEqualTo(1);
            registered.close();
            installed.onBatch(List.of(event(3)), token);
            assertThat(calls).hasValue(3); assertThat(outbox.snapshot().packets()).isZero();
            assertThat(registered.drained()).isTrue(); assertThat(registry.reap()).isEqualTo(1);
            assertThat(registry.snapshot().registrations()).isZero();
        }
    }

    @Test
    void oldOpeningAndForeignPhysicalScopesCannotRegister() {
        try (var registry = registry(2); var outbox = outbox("one")) {
            registry.install((events, position) -> {});
            assertThatThrownBy(() -> registry.register(scope("old-opening"), binding(outbox)))
                    .isInstanceOf(TapstateException.class).hasMessageContaining("captureScopeChanged");
            var sender = outbox.sender();
            var other = new ValidationRootHintBatch.Sender(sender.registrationId(), sender.pipelineExecutionId(), sender.sourceId(),
                    sender.tableId(), sender.nodeId(), "old-boot", sender.captureEpoch(), sender.captureFence(), sender.routeRevision(),
                    sender.keyContractVersion(), sender.mappingId(), sender.outputId(), sender.domains());
            try (var stale = new ValidationRootHintOutbox(outbox.run(), other, outbox.limits(), () -> 0)) {
                assertThatThrownBy(() -> registry.register(scope("opening"), binding(stale)))
                        .isInstanceOf(TapstateException.class).hasMessageContaining("senderCaptureScope");
            }
            assertThat(registry.snapshot().registrations()).isZero();
        }
    }

    @Test
    void retirementWaitsForActualExtractionExitBeforeReleasingItsSlot() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var delivered = new AtomicInteger();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor(); var registry = registry(1);
             var outbox = outbox("one"); var next = outbox("two")) {
            var listener = registry.install((events, position) -> delivered.incrementAndGet());
            var registration = registry.register(scope("opening"), blocking(outbox, entered, release));
            var callback = executor.submit(() -> listener.onBatch(List.of(event(1)), Optional.empty()));
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            try {
                executor.submit(registration::close).get(1, TimeUnit.SECONDS);
                assertThat(registration.activeCallbacks()).isEqualTo(1);
                assertThat(outbox.drained().toCompletableFuture()).isCompleted();
                assertThat(registration.drained()).isFalse(); assertThat(registry.reap()).isZero();
                assertThatThrownBy(() -> registry.register(scope("opening"), binding(next)))
                        .isInstanceOf(TapstateException.class).hasMessageContaining("registrationCapacity");
            } finally { release.countDown(); }
            callback.get(2, TimeUnit.SECONDS);
            assertThat(registration.drained()).isTrue(); assertThat(registry.reap()).isEqualTo(1);
            assertThat(outbox.snapshot().packets()).isZero(); assertThat(delivered).hasValue(1);
            assertThat(registry.register(scope("opening"), binding(next)).accepting()).isTrue();
        } finally { release.countDown(); }
    }

    @Test
    void retirementAlsoWaitsForAnActualTransportRequestToExit() {
        try (var registry = registry(1); var outbox = outbox("one")) {
            var listener = registry.install((events, position) -> {});
            var registration = registry.register(scope("opening"), binding(outbox));
            listener.onBatch(List.of(event(1)), Optional.empty());
            var request = outbox.poll().orElseThrow();
            registration.close();
            assertThat(registration.activeCallbacks()).isZero(); assertThat(registration.drained()).isFalse();
            assertThat(registry.reap()).isZero();
            outbox.settle(request, null);
            assertThat(registration.drained()).isTrue(); assertThat(registry.reap()).isEqualTo(1);
        }
    }

    @Test
    void registrationDoesNotBlockAnAlreadyEnteredCallbackOrIncludeItRetroactively() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor(); var registry = registry(2);
             var first = outbox("one"); var second = outbox("two")) {
            var listener = registry.install((events, position) -> {});
            registry.register(scope("opening"), blocking(first, entered, release));
            var callback = executor.submit(() -> listener.onBatch(List.of(event(1)), Optional.empty()));
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            try { assertThat(executor.submit(() -> registry.register(scope("opening"), binding(second)))
                    .get(1, TimeUnit.SECONDS).accepting()).isTrue(); }
            finally { release.countDown(); }
            callback.get(2, TimeUnit.SECONDS);
            assertThat(first.snapshot().packets()).isEqualTo(1); assertThat(second.snapshot().packets()).isZero();
            listener.onBatch(List.of(event(2)), Optional.empty());
            assertThat(first.snapshot().packets()).isEqualTo(2); assertThat(second.snapshot().packets()).isEqualTo(1);
        } finally { release.countDown(); }
    }

    @Test
    void overlappingCaptureCallbacksFailCoverageWithoutWaitingOrReordering() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var delivered = new AtomicInteger();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor(); var registry = registry(1); var outbox = outbox("one")) {
            var listener = registry.install((events, position) -> delivered.incrementAndGet());
            var registration = registry.register(scope("opening"), blocking(outbox, entered, release));
            var first = executor.submit(() -> listener.onBatch(List.of(event(1)), Optional.empty()));
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            try {
                executor.submit(() -> listener.onBatch(List.of(event(2)), Optional.empty())).get(1, TimeUnit.SECONDS);
                assertThat(outbox.failure().orElseThrow()).hasMessageContaining("concurrentCaptureCallback");
                assertThat(registration.activeCallbacks()).isEqualTo(1);
            } finally { release.countDown(); }
            first.get(2, TimeUnit.SECONDS);
            assertThat(delivered).hasValue(2); assertThat(outbox.snapshot().packets()).isZero();
            registration.close(); assertThat(registration.drained()).isTrue();
        } finally { release.countDown(); }
    }

    @Test
    void overlapCannotSilentlyReverseASecondBinding() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor(); var registry = registry(2);
             var first = outbox("one"); var second = outbox("two")) {
            var listener = registry.install((events, position) -> {});
            registry.register(scope("opening"), blocking(first, entered, release));
            registry.register(scope("opening"), binding(second));
            var callback = executor.submit(() -> listener.onBatch(List.of(event(1)), Optional.empty()));
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            try {
                executor.submit(() -> listener.onBatch(List.of(event(2)), Optional.empty())).get(1, TimeUnit.SECONDS);
                assertThat(first.failure()).isPresent();
                assertThat(second.failure()).isPresent();
            } finally { release.countDown(); }
            callback.get(2, TimeUnit.SECONDS);
            assertThat(second.snapshot().packets()).isZero();
        } finally { release.countDown(); }
    }

    @Test
    void retiringALaterBindingWaitsForTheOldCallbackSnapshotToExit() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor(); var registry = registry(2);
             var first = outbox("one"); var second = outbox("two")) {
            var listener = registry.install((events, position) -> {});
            registry.register(scope("opening"), blocking(first, entered, release));
            var retiring = registry.register(scope("opening"), binding(second));
            var callback = executor.submit(() -> listener.onBatch(List.of(event(1)), Optional.empty()));
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            try {
                retiring.close();
                assertThat(retiring.activeCallbacks()).isZero();
                assertThat(retiring.drained()).isFalse();
                assertThat(registry.reap()).isZero();
            } finally { release.countDown(); }
            callback.get(2, TimeUnit.SECONDS);
            assertThat(retiring.drained()).isTrue(); assertThat(registry.reap()).isEqualTo(1);
            assertThat(second.snapshot().packets()).isZero();
        } finally { release.countDown(); }
    }

    @Test
    void futureUnrelatedCallbacksDoNotExtendAnAlreadyRetiredCutoff() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var calls = new AtomicInteger();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor(); var registry = registry(2);
             var first = outbox("one"); var second = outbox("two")) {
            var listener = registry.install((events, position) -> {});
            var retiring = registry.register(scope("opening"), binding(first));
            registry.register(scope("opening"), new ValidationCaptureHintTap.Binding(second, (kind, before, after) -> {
                if (calls.incrementAndGet() == 2) {
                    entered.countDown();
                    try { if (!release.await(3, TimeUnit.SECONDS)) throw new AssertionError("fixture deadline"); }
                    catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
                }
                return keys(kind, before, after);
            }));
            listener.onBatch(List.of(event(1)), Optional.empty());
            retiring.close();
            var later = executor.submit(() -> listener.onBatch(List.of(event(2)), Optional.empty()));
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            try {
                assertThat(retiring.drained()).isTrue();
                assertThat(registry.reap()).isEqualTo(1);
            } finally { release.countDown(); }
            later.get(2, TimeUnit.SECONDS);
        } finally { release.countDown(); }
    }

    @Test
    void sourceFailureDisablesRegistrationWithoutCompletingValidationCallbacksOnSourceThread() {
        var calls = new AtomicInteger(); var drained = new AtomicInteger(); var cause = new IllegalStateException("source failed");
        try (var registry = registry(2); var outbox = outbox("one"); var next = outbox("two")) {
            var listener = registry.install(new CaptureListener() {
                @Override public void onBatch(List<Envelope> events, Optional<SourcePosition> position) { calls.incrementAndGet(); }
                @Override public void onError(Throwable failure) { assertThat(failure).isSameAs(cause); calls.incrementAndGet(); }
            });
            var registration = registry.register(scope("opening"), binding(outbox));
            outbox.drained().thenRun(drained::incrementAndGet);
            listener.onError(cause);
            assertThat(calls).hasValue(1); assertThat(drained).hasValue(0);
            assertThat(registration.accepting()).isFalse(); assertThat(registration.failure()).contains(cause);
            assertThatThrownBy(() -> registry.register(scope("opening"), binding(next)))
                    .isInstanceOf(TapstateException.class).hasMessageContaining("captureNotOpen");
            registry.close(); assertThat(drained).hasValue(1); assertThat(registration.drained()).isTrue();
        }
    }

    @Test
    void sourceFailureCannotHideAFatalExtractorErrorThatArrivesLater() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var fatal = new AssertionError("fatal extractor"); var sourceFailure = new IllegalStateException("source failed first");
        try (var executor = Executors.newVirtualThreadPerTaskExecutor(); var registry = registry(1); var outbox = outbox("one")) {
            var listener = registry.install((events, position) -> {});
            var registration = registry.register(scope("opening"), new ValidationCaptureHintTap.Binding(outbox,
                    (kind, before, after) -> {
                        entered.countDown();
                        try { if (!release.await(3, TimeUnit.SECONDS)) throw new AssertionError("fixture deadline"); }
                        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
                        throw fatal;
                    }));
            var callback = executor.submit(() -> listener.onBatch(List.of(event(1)), Optional.empty()));
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            try { listener.onError(sourceFailure); }
            finally { release.countDown(); }
            assertThatThrownBy(() -> callback.get(2, TimeUnit.SECONDS)).hasCause(fatal);
            assertThat(registration.failure().orElseThrow()).isSameAs(fatal);
            assertThat(fatal.getSuppressed()).contains(sourceFailure);
        } finally { release.countDown(); }
    }

    @Test
    void originalSourceFailureAndOptionalCallbacksKeepTheirOriginalSemantics() {
        var calls = new AtomicInteger(); var position = new SourcePosition("anchor"); var problem = new IllegalStateException("original");
        CaptureStartedListener original = new CaptureStartedListener() {
            @Override public void onStart(SourcePosition actual) { assertThat(actual).isSameAs(position); calls.incrementAndGet(); }
            @Override public void onBatch(List<Envelope> events, Optional<SourcePosition> token) { throw problem; }
            @Override public void onAcknowledged(SourcePosition actual) { assertThat(actual).isSameAs(position); calls.incrementAndGet(); }
            @Override public void onAcknowledgeFailed(Throwable actual) { assertThat(actual).isSameAs(problem); calls.incrementAndGet(); }
        };
        try (var registry = registry(1); var outbox = outbox("one")) {
            var listener = registry.install(original); registry.register(scope("opening"), binding(outbox));
            assertThat(listener).isInstanceOf(CaptureStartedListener.class);
            ((CaptureStartedListener) listener).onStart(position); listener.onAcknowledged(position); listener.onAcknowledgeFailed(problem);
            assertThat(calls).hasValue(3); assertThat(outbox.failure()).isEmpty();
            assertThatThrownBy(() -> listener.onBatch(List.of(event(1)), Optional.empty())).isSameAs(problem);
            assertThatThrownBy(() -> registry.install(original)).isInstanceOf(TapstateException.class);
        }
        try (var registry = registry(1)) {
            assertThat(registry.install((events, token) -> {})).isNotInstanceOf(CaptureStartedListener.class);
        }
    }

    @Test
    void collisionCapacityAndScopeBudgetsDoNotSilentlyReplaceBindings() {
        try (var registry = registry(1); var first = outbox("same"); var second = outbox("same")) {
            assertThatThrownBy(() -> registry.register(scope("opening"), binding(first)))
                    .isInstanceOf(TapstateException.class).hasMessageContaining("captureNotOpen");
            registry.install((events, position) -> {}); registry.register(scope("opening"), binding(first));
            assertThatThrownBy(() -> registry.register(scope("opening"), binding(second)))
                    .isInstanceOf(TapstateException.class).hasMessageContaining("registrationCollision");
            assertThat(registry.snapshot().registrations()).isEqualTo(1);
        }
        assertThatThrownBy(() -> new ValidationCaptureHintRegistry(scope("opening"),
                new ValidationCaptureHintRegistry.Limits(1, 1, WORK))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new ValidationCaptureHintRegistry.Limits(16, 65536,
                new ValidationCaptureHintTap.Limits(1000, 1, 64L * 1024 * 1024))).isInstanceOf(TapstateException.class);
    }

    @Test
    void closePublishesRetirementAndDrainCallbacksCanReenterTheControlPath() throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor(); var registry = registry(1); var outbox = outbox("one")) {
            registry.install((events, position) -> {});
            var registration = registry.register(scope("opening"), binding(outbox));
            var callback = outbox.drained().thenApply(unused -> {
                try { return executor.submit(() -> registry.snapshot().closed()).get(1, TimeUnit.SECONDS); }
                catch (Exception failure) { throw new AssertionError(failure); }
            });
            registry.close(); registry.close();
            assertThat(callback.toCompletableFuture().get(2, TimeUnit.SECONDS)).isTrue();
            assertThat(registration.drained()).isTrue(); assertThat(registry.reap()).isEqualTo(1);
        }
    }

    private static ValidationCaptureHintTap.Binding blocking(ValidationRootHintOutbox outbox, CountDownLatch entered, CountDownLatch release) {
        return new ValidationCaptureHintTap.Binding(outbox, (kind, before, after) -> {
            entered.countDown();
            try { if (!release.await(3, TimeUnit.SECONDS)) throw new AssertionError("fixture deadline"); }
            catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
            return keys(kind, before, after);
        });
    }
    private static ValidationCaptureHintTap.Binding binding(ValidationRootHintOutbox outbox) {
        return new ValidationCaptureHintTap.Binding(outbox, ValidationCaptureHintRegistryTest::keys);
    }
    private static ValidationRootHintBatch.Change keys(ValidationRootHintBatch.ChangeKind kind, Map<String, Object> before, Map<String, Object> after) {
        return new ValidationRootHintBatch.Change(kind, before == null ? null : key(((Number) before.get("id")).longValue()),
                after == null ? null : key(((Number) after.get("id")).longValue()));
    }
    private static Envelope event(long id) { return Envelope.insert(0, "orders", Map.of("id", id), Map.of()); }
    private static ValidationKey key(long n) { return new ValidationKey(List.of(ValidationKey.Part.number(n))); }
    private static ValidationCaptureHintRegistry registry(int count) {
        return new ValidationCaptureHintRegistry(scope("opening"), new ValidationCaptureHintRegistry.Limits(count, 65536, WORK));
    }
    private static ValidationCaptureHintRegistry.Scope scope(String opening) {
        var fence = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.CAPTURE, "physical"),
                new WorkloadOwner("capture", "capture-boot"), 1, 2, 3);
        return new ValidationCaptureHintRegistry.Scope("cluster", "physical", opening, "source", "capture", "capture-boot", 1,
                fence, "route", Set.of("orders"));
    }
    private static ValidationRootHintOutbox outbox(String id) {
        var run = new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "owner", "owner-boot", 1, 1, 1),
                "task", id, ValidationMode.INCREMENTAL, "spec", 1, "rules");
        var sender = new ValidationRootHintBatch.Sender(id, "execution", "source", "orders", "capture", "capture-boot", 1, 1,
                "route", "key-contract", "mapping", "output", List.of(new ValidationRootHintBatch.Domain(ValidationKey.Kind.EXACT_NUMBER)));
        return new ValidationRootHintOutbox(run, sender, new ValidationRootHintOutbox.Limits(4, 65536, 128, 32768, 2, 0, 100), () -> 0);
    }
}
