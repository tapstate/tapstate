package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.spi.store.StorageSetupStateStore;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SetupCapabilityServiceTest {
    private static final StorageSetupScope ORIGINAL = new StorageSetupScope("cluster", "installation", "operation", 1);
    private static final class Time extends Clock {
        Instant now = Instant.parse("2026-10-20T00:00:00Z");
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }
    private static final class States implements StorageSetupStateStore {
        Status status = new Status(ORIGINAL, Phase.CONFIG_PREPARED, 0);
        int reads;
        @Override public StorageSetupStateStore guardedBy(Runnable guard) { throw new UnsupportedOperationException(); }
        @Override public Status load() { reads++; return status; }
        @Override public Status rotate(StorageSetupScope expected, Purpose purpose) {
            requireCurrent(expected, purpose);
            status = new Status(new StorageSetupScope(expected.clusterId(), expected.installationId(), expected.operationId(),
                    expected.generation()+1), status.phase(), status.bindingVersion());
            return status;
        }
        @Override public void requireCurrent(StorageSetupScope expected, Purpose purpose) {
            reads++;
            if (!status.scope().equals(expected) || (purpose == Purpose.SETUP ? status.phase() == Phase.COMPLETE : status.phase() != Phase.COMPLETE))
                throw new TapstateException(ControlError.BOOTSTRAP_FORBIDDEN, java.util.Map.of(), null);
        }
    }
    @Test void requiresTheTrustedChannelBeforeLookingAtInstallationState() {
        var states = new States(); var time = new Time(); var ownerCalls = new AtomicInteger();
        var service = new SetupCapabilityService(states, time, ownerCalls::incrementAndGet,
                () -> { throw new TapstateException(ControlError.BOOTSTRAP_FORBIDDEN, java.util.Map.of(), null); }, () -> {});
        var issued = service.issue(ORIGINAL, StorageSetupStateStore.Purpose.SETUP);
        int before = states.reads;
        assertThatThrownBy(() -> service.authorize(issued.token(), issued.scope(), issued.purpose())).isInstanceOf(TapstateException.class);
        assertThat(states.reads).isEqualTo(before); assertThat(ownerCalls.get()).isEqualTo(1);
    }
    @Test void capturedRequestCannotCommitAfterReissueOrAfterExpiry() {
        var states = new States(); var time = new Time();
        var service = new SetupCapabilityService(states, time, () -> {}, () -> {}, () -> {});
        var first = service.issue(ORIGINAL, StorageSetupStateStore.Purpose.SETUP);
        Runnable oldRequest = service.authorize(first.token(), first.scope(), first.purpose());
        var second = service.issue(first.scope(), first.purpose());
        assertThatThrownBy(oldRequest::run).isInstanceOf(TapstateException.class);
        Runnable current = service.authorize(second.token(), second.scope(), second.purpose());
        time.now = second.expiresAt();
        assertThatThrownBy(current::run).isInstanceOf(TapstateException.class);
    }
    @Test void finalGuardAlsoSeesAnotherProcessAdvancingTheAuthority() {
        var states = new States(); var time = new Time();
        var service = new SetupCapabilityService(states, time, () -> {}, () -> {}, () -> {});
        var first = service.issue(ORIGINAL, StorageSetupStateStore.Purpose.SETUP);
        Runnable pending = service.authorize(first.token(), first.scope(), first.purpose());
        states.rotate(first.scope(), first.purpose());
        assertThatThrownBy(pending::run).isInstanceOf(TapstateException.class);
    }
    @Test void bindsPurposeInstallationClusterOperationAndSecretWithoutPrintingThem() {
        var states = new States(); var service = new SetupCapabilityService(states, new Time(), () -> {}, () -> {}, () -> {});
        var issued = service.issue(ORIGINAL, StorageSetupStateStore.Purpose.SETUP);
        assertThat(issued.toString()).doesNotContain(issued.token());
        assertThatThrownBy(() -> service.authorize(issued.token(), issued.scope(), StorageSetupStateStore.Purpose.FIRST_ADMIN))
                .isInstanceOf(TapstateException.class);
        for (var scope : new StorageSetupScope[]{new StorageSetupScope("other", "installation", "operation", 2),
                new StorageSetupScope("cluster", "other", "operation", 2), new StorageSetupScope("cluster", "installation", "other", 2)})
            assertThatThrownBy(() -> service.authorize(issued.token(), scope, issued.purpose())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> service.authorize("x".repeat(43), issued.scope(), issued.purpose())).isInstanceOf(TapstateException.class);
    }
    @Test void firstAdminReadinessIsRequiredForIssueRestoreAndEveryCapturedRequest() {
        var states = new States(); var time = new Time();
        states.status = new StorageSetupStateStore.Status(ORIGINAL, StorageSetupStateStore.Phase.COMPLETE, 1);
        var healthy = new java.util.concurrent.atomic.AtomicBoolean();
        Runnable ready = () -> { if (!healthy.get()) throw new TapstateException(ControlError.UNREACHABLE, java.util.Map.of(), null); };
        var service = new SetupCapabilityService(states, time, () -> {}, () -> {}, ready);
        assertThatThrownBy(() -> service.issue(ORIGINAL, StorageSetupStateStore.Purpose.FIRST_ADMIN)).isInstanceOf(TapstateException.class);
        assertThat(states.status.scope()).isEqualTo(ORIGINAL);
        healthy.set(true);
        var issued = service.issue(ORIGINAL, StorageSetupStateStore.Purpose.FIRST_ADMIN);
        Runnable request = service.authorize(issued.token(), issued.scope(), issued.purpose());
        healthy.set(false);
        assertThatThrownBy(request::run).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> service.restore(issued)).isInstanceOf(TapstateException.class);
    }
    @Test void anOwnerFileCanBeRestoredOnlyForTheCurrentPurposeGenerationAndExpiry() {
        var states = new States(); var time = new Time();
        var first = new SetupCapabilityService(states, time, () -> {}, () -> {}, () -> {});
        var issued = first.issue(ORIGINAL, StorageSetupStateStore.Purpose.SETUP);
        var restarted = new SetupCapabilityService(states, time, () -> {}, () -> {}, () -> {});
        restarted.restore(issued);
        restarted.authorize(issued.token(), issued.scope(), issued.purpose()).run();
        time.now = issued.expiresAt();
        assertThatThrownBy(() -> restarted.restore(issued)).isInstanceOf(TapstateException.class);
    }

    @Test void aSlowFirstAdminReadinessCheckCannotExtendTheCapabilityDeadline() {
        var states = new States(); var time = new Time();
        states.status = new StorageSetupStateStore.Status(ORIGINAL, StorageSetupStateStore.Phase.COMPLETE, 1);
        var slow = new java.util.concurrent.atomic.AtomicBoolean();
        var service = new SetupCapabilityService(states, time, () -> {}, () -> {}, () -> {
            if (slow.get()) time.now = time.now.plus(SetupCapabilityService.LIFETIME);
        });
        var issued = service.issue(ORIGINAL, StorageSetupStateStore.Purpose.FIRST_ADMIN);
        var request = service.authorize(issued.token(), issued.scope(), issued.purpose());
        slow.set(true);
        assertThatThrownBy(request::run).isInstanceOf(TapstateException.class);
    }
}
