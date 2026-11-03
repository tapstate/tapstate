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
import io.tapstate.runtime.srs.CaptureObservation;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ValidationCaptureObservationsTest {
    @Test
    void physicalCloseSignalsOnlyAndBackgroundCleanupCompletesDrain() {
        var delivered = new AtomicInteger(); var sourceClosed = new AtomicInteger(); var drainCallbacks = new AtomicInteger();
        var callback = new AtomicReference<CaptureListener>(); var context = context("first");
        try (var host = host(); var outbox = outbox("one", "route-one")) {
            var subscription = CaptureObservation.create(host, context).start((events, token) -> delivered.incrementAndGet(), listener -> {
                callback.set(listener); return sourceClosed::incrementAndGet;
            });
            callback.get().onBatch(List.of(event()), Optional.empty());
            var registration = host.register(context, binding(outbox));
            outbox.drained().thenRun(drainCallbacks::incrementAndGet);
            callback.get().onBatch(List.of(event()), Optional.empty());
            assertThat(outbox.snapshot().packets()).isOne();
            subscription.close();
            assertThat(sourceClosed).hasValue(1);
            assertThat(drainCallbacks).hasValue(0);
            assertThat(registration.accepting()).isFalse();
            callback.get().onBatch(List.of(event()), Optional.empty());
            assertThat(delivered).hasValue(3);
            host.maintain();
            assertThat(drainCallbacks).hasValue(1);
            assertThat(registration.drained()).isTrue();
            assertThat(host.status()).isEmpty();
        }
    }

    @Test
    void oldPhysicalRequestRetainsCapacityAndOldFailureCannotInvalidateReplacement() {
        try (var host = host(); var old = outbox("old", "route"); var fresh = outbox("fresh", "route")) {
            var original = context("old"); var next = context("new");
            var hooks = host.open(original); var callback = hooks.wrap((events, token) -> {});
            var registration = host.register(original, binding(old));
            callback.onBatch(List.of(event()), Optional.empty());
            var request = old.poll().orElseThrow();
            hooks.retire(null); host.maintain();
            assertThat(registration.drained()).isFalse();
            assertThatThrownBy(() -> host.open(next)).isInstanceOf(TapstateException.class).hasMessageContaining("openingCapacity");
            assertThatThrownBy(() -> host.register(original, binding(fresh))).isInstanceOf(TapstateException.class);
            old.settle(request, null); host.maintain();
            var replacement = host.open(next); replacement.wrap((events, token) -> {});
            var active = host.register(next, binding(fresh));
            host.failed(original, "retire", new IllegalStateException("late old failure"));
            assertThat(active.accepting()).isTrue();
            assertThat(host.status()).singleElement().satisfies(status -> assertThat(status.context()).isEqualTo(next));
        }
    }

    @Test
    void metadataCannotBeReapedWhileAnEnteredKeyCallbackStillOwnsIt() throws Exception {
        var entered = new CountDownLatch(1); var leave = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor(); var host = host(); var outbox = outbox("one", "route")) {
            var context = context("opening"); var hooks = host.open(context);
            var callback = hooks.wrap((events, token) -> {});
            var registration = host.register(context, new ValidationCaptureHintTap.Binding(outbox, (kind, before, after) -> {
                entered.countDown();
                try { if (!leave.await(3, TimeUnit.SECONDS)) throw new AssertionError("fixture deadline"); }
                catch (InterruptedException failure) { throw new AssertionError(failure); }
                return change();
            }));
            var delivery = executor.submit(() -> callback.onBatch(List.of(event()), Optional.empty()));
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            try {
                hooks.retire(null); host.maintain();
                assertThat(host.status()).hasSize(1); assertThat(registration.drained()).isFalse();
            } finally { leave.countDown(); }
            delivery.get(2, TimeUnit.SECONDS); host.maintain();
            assertThat(registration.drained()).isTrue(); assertThat(host.status()).isEmpty();
        } finally { leave.countDown(); }
    }

    @Test
    void admittedSendersKeepTheirOwnRouteRevisionOnTheSamePhysicalCapture() {
        try (var host = host(); var one = outbox("one", "route-one"); var two = outbox("two", "route-two")) {
            var context = context("opening"); var callback = host.open(context).wrap((events, token) -> {});
            host.register(context, binding(one)); host.register(context, binding(two));
            callback.onBatch(List.of(event()), Optional.empty());
            assertThat(one.snapshot().packets()).isOne(); assertThat(two.snapshot().packets()).isOne();
            assertThat(one.sender().routeRevision()).isEqualTo("route-one");
            assertThat(two.sender().routeRevision()).isEqualTo("route-two");
        }
    }

    @Test
    void missingMemberIdentityDoesNotInventAuthorityOrReplaceTheOriginalListener() {
        try (var host = new ValidationCaptureObservations("cluster", null, null, limits(), false)) {
            CaptureListener original = (events, token) -> {};
            var hooks = host.open(context("opening"));
            assertThat(host.available()).isFalse();
            assertThat(hooks.wrap(original)).isSameAs(original);
            assertThat(host.status()).isEmpty();
        }
    }

    @Test
    void closeRefusesNewOpeningsAndRegistrations() {
        var host = host(); var context = context("opening");
        host.open(context).wrap((events, token) -> {}); host.close();
        try (var outbox = outbox("one", "route")) {
            assertThatThrownBy(() -> host.open(context("replacement"))).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> host.register(context, binding(outbox))).isInstanceOf(TapstateException.class);
            assertThat(host.status()).isEmpty();
        }
    }

    @Test
    void racingCloseCannotMissAnOpeningPublishedAfterItsEmptySnapshot() throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int attempt = 0; attempt < 1000; attempt++) {
                var host = host(); var start = new CountDownLatch(1);
                var opening = executor.submit(() -> {
                    start.await();
                    try { host.open(context("racing")); }
                    catch (TapstateException expectedClosedAdmission) { /* Closing may win admission. */ }
                    return null;
                });
                var closing = executor.submit(() -> { start.await(); host.close(); return null; });
                start.countDown();
                opening.get(2, TimeUnit.SECONDS); closing.get(2, TimeUnit.SECONDS);
                try { assertThat(host.status()).as("all openings observed by close at attempt %s", attempt).isEmpty(); }
                finally { host.close(); }
            }
        }
    }

    private static ValidationCaptureObservations host() {
        return new ValidationCaptureObservations("cluster", "capture", "boot", limits(), false);
    }
    private static ValidationCaptureObservations.Limits limits() {
        return new ValidationCaptureObservations.Limits(1, new ValidationCaptureHintRegistry.Limits(2, 65536,
                new ValidationCaptureHintTap.Limits(1000, 1, 1_048_576)));
    }
    private static CaptureObservation.Context context(String opening) {
        return new CaptureObservation.Context("physical", opening, "chain", "source", "producer", "consumer", 1, 1,
                new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.CAPTURE, "physical"),
                        new WorkloadOwner("capture", "boot"), 1, 1, 1), List.of("orders"));
    }
    private static ValidationCaptureHintTap.Binding binding(ValidationRootHintOutbox outbox) {
        return new ValidationCaptureHintTap.Binding(outbox, (kind, before, after) -> change());
    }
    private static ValidationRootHintBatch.Change change() {
        return new ValidationRootHintBatch.Change(ValidationRootHintBatch.ChangeKind.INSERT, null,
                new ValidationKey(List.of(ValidationKey.Part.number(1))));
    }
    private static Envelope event() { return Envelope.insert(0, "orders", Map.of("id", 1), Map.of()); }
    private static ValidationRootHintOutbox outbox(String id, String route) {
        var run = new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "owner", "owner-boot", 1, 1, 1),
                "task", id, ValidationMode.INCREMENTAL, "spec", 1, "rules");
        var sender = new ValidationRootHintBatch.Sender(id, "execution", "source", "orders", "capture", "boot", 1, 1,
                route, "key-contract", "mapping", "output", List.of(new ValidationRootHintBatch.Domain(ValidationKey.Kind.EXACT_NUMBER)));
        return new ValidationRootHintOutbox(run, sender, new ValidationRootHintOutbox.Limits(4, 65536, 128, 32768, 2, 0, 100), () -> 0);
    }
}
