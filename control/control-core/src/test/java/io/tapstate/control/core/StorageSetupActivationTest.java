package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class StorageSetupActivationTest {
    private static final StorageSetupScope SCOPE = StorageSetupServiceTest.SCOPE;
    private static final StorageSetupService.Candidate CANDIDATE = StorageSetupServiceTest.CANDIDATE;

    @Test void explicitRetryKeepsTheOriginalGrantAfterTestStampsExpire() {
        var f = new StorageSetupServiceTest.Fixture();
        var handoff = new AtomicReference<StorageBindingCommit>(); var calls = new AtomicInteger();
        f.continuation = value -> { handoff.set(value); calls.incrementAndGet(); };
        var retry = new java.util.concurrent.atomic.AtomicBoolean();
        f.probe = new StorageConnectionProbe() {
            @Override public StorageBinding.Connection normalize(StorageBinding.Connection input) {
                if (retry.get()) throw new AssertionError("retry must use the saved binding");
                return input;
            }
            @Override public void test(StorageBinding.Connection input) {
                if (retry.get()) throw new AssertionError("retry must not repeat a network test");
            }
        };
        var service = f.service(); var stamps = f.tested(service, CANDIDATE);
        service.commit("owner", SCOPE, () -> {}, CANDIDATE, stamps);
        var original = handoff.get();
        f.time.now = f.time.now.plus(Duration.ofHours(1)); retry.set(true);
        assertThat(service.activate("owner", SCOPE, () -> {}, 1).bindingVersion()).isOne();
        assertThat(handoff.get()).isSameAs(original);
        assertThat(original.firstCreation()).isPresent();
        assertThat(calls).hasValue(2); assertThat(f.binding.inserts).isOne();
        assertThat(f.audit).extracting(AuditRecord::operationId).contains("storage.setup.activate");
        assertThat(f.target.tests).isZero();
    }

    @Test void reopeningTheSavedOperationNeverMintsFirstCacheCreationAuthority() {
        var f = new StorageSetupServiceTest.Fixture(); var original = f.service();
        original.commit("owner", SCOPE, () -> {}, CANDIDATE, f.tested(original, CANDIDATE));
        var handoff = new AtomicReference<StorageBindingCommit>(); f.continuation = handoff::set;
        var reopened = f.service();
        for (var phase : new StorageSetupStateStore.Phase[]{StorageSetupStateStore.Phase.STORAGE_BOUND, StorageSetupStateStore.Phase.COMPLETE}) {
            f.states.state = new StorageSetupStateStore.Status(SCOPE, phase, 1);
            assertThat(reopened.activate("owner", SCOPE, () -> {}, 1).setupPhase()).isEqualTo(phase);
            assertThat(handoff.get().binding()).isEqualTo(f.binding.binding);
            assertThat(handoff.get().preparation()).isEqualTo(f.binding.cachePreparation().orElseThrow());
            assertThat(handoff.get().firstCreation()).isEmpty();
        }
        assertThat(f.binding.inserts).isOne();
    }

    @Test void aChangedGenerationOrBindingVersionCannotContinueTheSavedOperation() {
        var f = new StorageSetupServiceTest.Fixture(); var calls = new AtomicInteger();
        f.continuation = ignored -> calls.incrementAndGet();
        var service = f.service(); service.commit("owner", SCOPE, () -> {}, CANDIDATE, f.tested(service, CANDIDATE));
        var renewed = new StorageSetupScope(SCOPE.clusterId(), SCOPE.installationId(), SCOPE.operationId(), 2);
        assertThatThrownBy(() -> service.activate("owner", renewed, () -> {}, 1)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> service.activate("owner", SCOPE, () -> {}, 2)).isInstanceOf(TapstateException.class);
        assertThat(calls).hasValue(1);
        assertThat(f.audit).extracting(AuditRecord::operationId).doesNotContain("storage.setup.activate");
    }

    @Test void anUncommittedPhaseCannotUseAnExistingBindingAsCreationAuthority() {
        var f = new StorageSetupServiceTest.Fixture(); var calls = new AtomicInteger();
        f.continuation = ignored -> calls.incrementAndGet();
        var service = f.service(); service.commit("owner", SCOPE, () -> {}, CANDIDATE, f.tested(service, CANDIDATE));
        f.states.state = new StorageSetupStateStore.Status(SCOPE, StorageSetupStateStore.Phase.CONFIG_PREPARED, 0);
        assertThatThrownBy(() -> service.activate("owner", SCOPE, () -> {}, 1)).isInstanceOf(TapstateException.class);
        assertThat(calls).hasValue(1);
        assertThat(f.audit).extracting(AuditRecord::operationId).doesNotContain("storage.setup.activate");
    }

    @Test void theRetainedGrantCannotBeAppliedToAReplacedPersistedBinding() {
        var f = new StorageSetupServiceTest.Fixture(); var calls = new AtomicInteger();
        f.continuation = ignored -> calls.incrementAndGet();
        var service = f.service(); service.commit("owner", SCOPE, () -> {}, CANDIDATE, f.tested(service, CANDIDATE));
        var replacement = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://other/cache", "cache", null);
        f.binding.binding = new StorageBinding(SCOPE, StorageBinding.Mode.OP, 1, "views", replacement);
        assertThatThrownBy(() -> service.activate("owner", SCOPE, () -> {}, 1)).isInstanceOf(TapstateException.class);
        assertThat(calls).hasValue(1);
        assertThat(f.audit).extracting(AuditRecord::operationId).doesNotContain("storage.setup.activate");
    }

    @Test void aRevokedCallerCannotReachTheActivationAuditOrContinuation() {
        var f = new StorageSetupServiceTest.Fixture(); var service = f.service();
        service.commit("owner", SCOPE, () -> {}, CANDIDATE, f.tested(service, CANDIDATE));
        Runnable revoked = () -> { throw new TapstateException(ControlError.BOOTSTRAP_FORBIDDEN, Map.of(), null); };
        assertThatThrownBy(() -> service.activate("owner", SCOPE, revoked, 1)).isInstanceOf(TapstateException.class);
        assertThat(f.audit).extracting(AuditRecord::operationId).doesNotContain("storage.setup.activate");
    }

    @Test void auditFailurePreservesTheBindingAndDoesNotRepeatContinuation() {
        var f = new StorageSetupServiceTest.Fixture(); var calls = new AtomicInteger();
        f.continuation = ignored -> calls.incrementAndGet(); var service = f.service();
        service.commit("owner", SCOPE, () -> {}, CANDIDATE, f.tested(service, CANDIDATE));
        var saved = f.binding.binding;
        f.onAudit = () -> { throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of(), null); };
        assertThatThrownBy(() -> service.activate("owner", SCOPE, () -> {}, 1)).isInstanceOf(TapstateException.class);
        assertThat(f.binding.binding).isSameAs(saved); assertThat(f.binding.inserts).isOne();
        assertThat(calls).hasValue(1);
    }

    @Test void noBindingMeansThereIsNothingToActivate() {
        var f = new StorageSetupServiceTest.Fixture(); var service = f.service();
        assertThatThrownBy(() -> service.activate("owner", SCOPE, () -> {}, 1)).isInstanceOf(TapstateException.class);
        assertThat(f.audit).isEmpty(); assertThat(f.binding.inserts).isZero();
    }
}
