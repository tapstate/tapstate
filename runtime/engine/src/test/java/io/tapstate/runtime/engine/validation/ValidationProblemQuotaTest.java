package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationMode;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.core.validation.ValidationRoot;
import io.tapstate.core.validation.ValidationRunIdentity;
import io.tapstate.core.validation.ValidationRunState;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.assertj.core.api.Assertions.*;

class ValidationProblemQuotaTest {
    private static final ValidationOwner OWNER = new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 2, 3);

    @Test void fullAndIncrementalShareTheHardLimitAndOverflowClosesEveryPublicationAndJoin() {
        var quota = quota();
        var full = run("task", "full", ValidationMode.FULL);
        var inc = run("task", "inc", ValidationMode.INCREMENTAL);
        quota.join(full, () -> { });
        quota.join(inc, () -> { });
        ValidationProblemQuota.PublicationPermit earlier = null;
        for (int i = 0; i < 700; i++) earlier = quota.reserve(full, root(i)).permit().orElseThrow();
        for (int i = 0; i < 300; i++) quota.reserve(inc, root(i));
        assertThat(quota.reservedRoots()).isEqualTo(1000);
        assertThat(quota.reserve(full, root(0)).admission()).isEqualTo(ValidationProblemQuota.Admission.EXISTING);
        var writes = new AtomicInteger();
        assertThat(quota.publish(earlier, writes::incrementAndGet)).isTrue();
        var overflow = quota.reserve(inc, root(300));
        assertThat(overflow.admission()).isEqualTo(ValidationProblemQuota.Admission.LIMIT_REACHED);
        assertThat(overflow.permit()).isEmpty();
        assertThat(quota.state()).isEqualTo(ValidationProblemQuota.State.LIMIT_REACHED);
        assertThat(quota.publish(earlier, writes::incrementAndGet)).isFalse();
        assertThat(quota.reserve(full, root(0)).permit()).isEmpty();
        assertThat(quota.join(run("another", "late", ValidationMode.FULL), () -> { }))
                .isEqualTo(ValidationProblemQuota.Join.CLOSED);
        assertThat(quota.activeRuns()).containsExactlyInAnyOrder(full, inc);
        assertThat(quota.reservedRoots()).isEqualTo(1000);
        assertThat(writes).hasValue(1);
    }

    @Test void descendantsAndRepeatedIssueKindsShareARootButRunsMappingsAndOutputsStaySeparate() {
        var quota = quota();
        var first = run("one", "first", ValidationMode.FULL);
        var second = run("two", "second", ValidationMode.FULL);
        quota.join(first, () -> { });
        quota.join(second, () -> { });
        var initial = quota.reserve(first, root(1)).permit().orElseThrow();
        var numericAlias = new ValidationKey(List.of(ValidationKey.Part.number(new BigDecimal("1.00"))));
        var child = new ValidationRoot("mapping", "output", numericAlias,
                List.of(new ValidationRoot.Ancestor(List.of("items"), key(99))));
        assertThat(quota.reserve(first, child).permit()).containsSame(initial);
        quota.reserve(second, root(1));
        quota.reserve(first, new ValidationRoot("other", "output", key(1), List.of()));
        quota.reserve(first, new ValidationRoot("mapping", "other", key(1), List.of()));
        assertThat(quota.reservedRoots()).isEqualTo(4);
    }

    @Test void failedOrUnknownPublicationAndLocalSealRetainTheirReservation() {
        var quota = quota();
        var run = run("task", "full", ValidationMode.FULL);
        quota.join(run, () -> { });
        var permit = quota.reserve(run, root(1)).permit().orElseThrow();
        long charged = quota.metadataBytes();
        var unknown = new IllegalStateException("publication outcome unknown");
        assertThatThrownBy(() -> quota.publish(permit, () -> { throw unknown; })).isSameAs(unknown);
        assertThat(quota.reservedRoots()).isEqualTo(1);
        assertThat(quota.reserve(run, root(1)).permit()).containsSame(permit);
        quota.seal(run);
        assertThat(quota.publish(permit, () -> fail("sealed submission"))).isFalse();
        assertThat(quota.reserve(run, root(2)).permit()).isEmpty();
        assertThat(quota.reservedRoots()).isEqualTo(1);
        assertThat(quota.metadataBytes()).isEqualTo(charged);
    }

    @Test void knownSealedTerminalRetirementReleasesSlotsWithoutRevivingAnOverflowGeneration() {
        var quota = quota();
        var first = run("one", "first", ValidationMode.FULL);
        var second = run("two", "second", ValidationMode.FULL);
        quota.join(first, () -> { });
        quota.join(second, () -> { });
        for (int i = 0; i < 1000; i++) quota.reserve(first, root(i));
        quota.reserve(second, root(1));
        assertThatThrownBy(() -> quota.retireSealedTerminalRun(first, ValidationRunState.STOPPED))
                .isInstanceOf(IllegalStateException.class);
        quota.seal(first);
        assertThatThrownBy(() -> quota.retireSealedTerminalRun(first, ValidationRunState.STOPPING))
                .isInstanceOf(TapstateException.class);
        quota.retireSealedTerminalRun(first, ValidationRunState.STOPPED);
        assertThat(quota.reservedRoots()).isZero();
        assertThat(quota.state()).isEqualTo(ValidationProblemQuota.State.LIMIT_REACHED);
        assertThat(quota.reserve(second, root(1)).permit()).isEmpty();
        quota.seal(second);
        quota.retireSealedTerminalRun(second, ValidationRunState.STOPPED);
        assertThat(quota.activeRuns()).isEmpty();
        assertThat(quota.state()).isEqualTo(ValidationProblemQuota.State.LIMIT_REACHED);
    }

    @Test void runFailureIsIsolatedAndOldPermitsDoNotFollowAReplacementRun() {
        var quota = quota();
        var first = run("same", "first", ValidationMode.FULL);
        var other = run("other", "other", ValidationMode.FULL);
        var valid = new AtomicBoolean(true);
        quota.join(first, () -> { if (!valid.get()) throw unauthorized(); });
        quota.join(other, () -> { });
        var old = quota.reserve(first, root(1)).permit().orElseThrow();
        valid.set(false);
        assertThatThrownBy(() -> quota.publish(old, () -> fail("retired run"))).isInstanceOf(TapstateException.class);
        assertThat(quota.state()).isEqualTo(ValidationProblemQuota.State.OPEN);
        assertThat(quota.reserve(other, root(1)).permit()).isPresent();
        quota.retireSealedTerminalRun(first, ValidationRunState.FAILED);
        var replacement = run("same", "replacement", ValidationMode.FULL);
        assertThat(quota.join(replacement, () -> { })).isEqualTo(ValidationProblemQuota.Join.JOINED);
        quota.reserve(replacement, root(1));
        assertThat(quota.publish(old, () -> fail("stale permit"))).isFalse();
        assertThat(quota.reserve(first, root(2)).permit()).isEmpty();
        assertThat(quota.reservedRoots()).isEqualTo(2);
    }

    @Test void ownerLossNeverReopensEvenIfTheExternalSignalLaterRecovers() {
        var ownerLive = new AtomicBoolean(true);
        var quota = new ValidationProblemQuota(OWNER, 1, 4, 1_000_000,
                () -> { if (!ownerLive.get()) throw unauthorized(); });
        var run = run("one", "first", ValidationMode.FULL);
        quota.join(run, () -> { });
        var permit = quota.reserve(run, root(1)).permit().orElseThrow();
        ownerLive.set(false);
        assertThatThrownBy(() -> quota.publish(permit, () -> fail("old owner"))).isInstanceOf(TapstateException.class);
        ownerLive.set(true);
        assertThat(quota.publish(permit, () -> fail("revived owner"))).isFalse();
        assertThat(quota.reserve(run, root(2)).permit()).isEmpty();
        assertThat(quota.reservedRoots()).isEqualTo(1);
        assertThat(quota.state()).isEqualTo(ValidationProblemQuota.State.CLOSED);
        var newOwner = new ValidationOwner("cluster", "pipeline", "node", "new-boot", 2, 3, 3);
        var next = new ValidationProblemQuota(newOwner, 2, 4, 1_000_000, () -> { });
        assertThatThrownBy(() -> next.join(run, () -> { })).isInstanceOf(TapstateException.class);
        assertThat(next.publish(permit, () -> fail("cross-owner permit"))).isFalse();
    }

    @Test void joiningIsIdempotentAndMethodOrSpecChangesCannotCreateAnotherModeSlot() {
        var quota = quota();
        var first = run("same", "first", ValidationMode.FULL);
        assertThat(quota.join(first, () -> { })).isEqualTo(ValidationProblemQuota.Join.JOINED);
        assertThat(quota.join(first, () -> fail("must retain the original guard")))
                .isEqualTo(ValidationProblemQuota.Join.ALREADY_JOINED);
        assertThat(quota.join(run("same", "second", ValidationMode.FULL), () -> { }))
                .isEqualTo(ValidationProblemQuota.Join.MODE_BUSY);
        assertThat(quota.join(run("same", "incremental", ValidationMode.INCREMENTAL), () -> { }))
                .isEqualTo(ValidationProblemQuota.Join.JOINED);
    }

    @Test void metadataAndActiveRunBudgetsRejectBeforeRetainingTheNewItem() {
        var quota = new ValidationProblemQuota(OWNER, 1, 1, 1000, () -> { });
        var first = run("task", "first", ValidationMode.FULL);
        quota.join(first, () -> { });
        assertThatThrownBy(() -> quota.join(run("other", "other", ValidationMode.FULL), () -> { }))
                .isInstanceOf(TapstateException.class);
        long before = quota.metadataBytes();
        var large = new ValidationRoot("mapping", "output",
                new ValidationKey(List.of(ValidationKey.Part.text("x".repeat(1000)))), List.of());
        assertThatThrownBy(() -> quota.reserve(first, large)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
        assertThat(quota.reservedRoots()).isZero();
        assertThat(quota.metadataBytes()).isEqualTo(before);
        assertThat(quota.state()).isEqualTo(ValidationProblemQuota.State.OPEN);
    }

    @Test @Timeout(10)
    void concurrentNewRootsCannotOverbookAndLateCallbacksCannotPublishAfterOverflow() throws Exception {
        var quota = quota();
        var run = run("task", "run", ValidationMode.FULL);
        quota.join(run, () -> { });
        var start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(4)) {
            var results = new ArrayList<Future<ValidationProblemQuota.Reservation>>();
            for (int i = 0; i < 1008; i++) {
                int id = i;
                results.add(workers.submit(() -> { start.await(); return quota.reserve(run, root(id)); }));
            }
            start.countDown();
            int reserved = 0;
            for (var result : results) {
                var reservation = result.get(5, TimeUnit.SECONDS);
                if (reservation.admission() == ValidationProblemQuota.Admission.RESERVED) reserved++;
            }
            assertThat(reserved).isEqualTo(1000);
            assertThat(quota.reservedRoots()).isEqualTo(1000);
            for (var result : results) {
                result.get().permit().ifPresent(permit ->
                        assertThat(quota.publish(permit, () -> fail("late submit"))).isFalse());
            }
        }
    }

    @Test void anEmptyFinishedGenerationCannotBeReused() {
        var quota = quota();
        var run = run("task", "first", ValidationMode.FULL);
        quota.join(run, () -> { });
        quota.reserve(run, root(1));
        quota.seal(run);
        quota.retireSealedTerminalRun(run, ValidationRunState.COMPLETED);
        assertThat(quota.state()).isEqualTo(ValidationProblemQuota.State.CLOSED);
        assertThat(quota.join(run("task", "next", ValidationMode.FULL), () -> { }))
                .isEqualTo(ValidationProblemQuota.Join.CLOSED);
    }

    private static ValidationProblemQuota quota() { return new ValidationProblemQuota(OWNER, 1, 8, 1_000_000, () -> { }); }
    private static ValidationRunIdentity run(String task, String id, ValidationMode mode) {
        return new ValidationRunIdentity(OWNER, task, id, mode, "spec", 1, "rules");
    }
    private static ValidationRoot root(long id) { return new ValidationRoot("mapping", "output", key(id), List.of()); }
    private static ValidationKey key(long id) { return new ValidationKey(List.of(ValidationKey.Part.number(id))); }
    private static TapstateException unauthorized() {
        return new TapstateException(ValidationError.OWNER_NOT_AUTHORIZED, Map.of("pipeline", "pipeline"), null);
    }
}
