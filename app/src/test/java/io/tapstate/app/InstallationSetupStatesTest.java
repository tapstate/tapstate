package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageSetupStateStore;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class InstallationSetupStatesTest {
    @TempDir Path temporary;

    @Test void localReissueKeepsOriginalOperationAndRevokesThePreviousGeneration() {
        var files = InstallationFiles.initialize(temporary.resolve("installation"));
        var states = new InstallationSetupStates(files, Optional::empty);
        var original = states.load();
        var next = states.rotate(original.scope(), StorageSetupStateStore.Purpose.SETUP);
        assertThat(next.scope().operationId()).isEqualTo(original.scope().operationId());
        assertThat(next.scope().generation()).isEqualTo(original.scope().generation() + 1);
        assertThatThrownBy(() -> states.requireCurrent(original.scope(), StorageSetupStateStore.Purpose.SETUP)).isInstanceOf(TapstateException.class);
        states.requireCurrent(next.scope(), StorageSetupStateStore.Purpose.SETUP);
        assertThatThrownBy(() -> states.rotate(next.scope(), StorageSetupStateStore.Purpose.FIRST_ADMIN)).isInstanceOf(TapstateException.class);
    }

    @Test void completedSetupRenewalKeepsSetupPurposeAndRestrictsItToTheOriginalActivation() {
        var files = InstallationFiles.initialize(temporary.resolve("limited-activation"));
        var initial = files.stage().scope();
        var next = new io.tapstate.spi.store.StorageSetupScope(initial.clusterId(), initial.installationId(), initial.operationId(), initial.generation() + 1);
        var physical = mock(StorageSetupStateStore.class); var permitted = new java.util.concurrent.atomic.AtomicBoolean(true);
        when(physical.guardedBy(any())).thenReturn(physical);
        when(physical.load()).thenReturn(new StorageSetupStateStore.Status(initial, StorageSetupStateStore.Phase.COMPLETE, 1));
        when(physical.rotate(initial, StorageSetupStateStore.Purpose.FIRST_ADMIN)).thenAnswer(ignored -> {
            when(physical.load()).thenReturn(new StorageSetupStateStore.Status(next, StorageSetupStateStore.Phase.COMPLETE, 1));
            return new StorageSetupStateStore.Status(next, StorageSetupStateStore.Phase.COMPLETE, 1);
        });
        var states = new InstallationSetupStates(files, () -> Optional.of(physical), permitted::get);
        var capabilities = new io.tapstate.control.core.SetupCapabilityService(states, java.time.Clock.systemUTC(), () -> {}, () -> {},
                () -> { throw new AssertionError("limited activation must not mint first-admin authority"); });
        var issued = capabilities.issue(initial, StorageSetupStateStore.Purpose.SETUP);
        assertThat(issued.purpose()).isEqualTo(StorageSetupStateStore.Purpose.SETUP);
        var permission = capabilities.authorize(issued.token(), next, StorageSetupStateStore.Purpose.SETUP);
        assertThat(permission.limitedActivation()).isTrue();
        var caller = io.tapstate.control.core.StorageControlAccess.management(permission);
        for (var operation : java.util.List.of(io.tapstate.control.core.ControlOperations.STORAGE_STATUS,
                io.tapstate.control.core.ControlOperations.STORAGE_SETUP_STATUS, io.tapstate.control.core.ControlOperations.STORAGE_SETUP_ACTIVATE))
            assertThatCode(() -> caller.require(operation)).doesNotThrowAnyException();
        for (var operation : java.util.List.of(io.tapstate.control.core.ControlOperations.STORAGE_TEST,
                io.tapstate.control.core.ControlOperations.STORAGE_SETUP_COMMIT, io.tapstate.control.core.ControlOperations.STORAGE_RECOVERY_PLAN))
            assertThatThrownBy(() -> caller.require(operation)).isInstanceOf(TapstateException.class);
        permitted.set(false);
        assertThatThrownBy(permission::run).isInstanceOf(TapstateException.class);
        verify(physical, never()).rotate(any(), eq(StorageSetupStateStore.Purpose.SETUP));
    }

    @Test void missingConfigurationAfterCreationStartsDoesNotRestoreLocalAuthority() {
        var files = InstallationFiles.initialize(temporary.resolve("installation"));
        var scope = files.stage().scope();
        files.saveStage(files.stage().withHandoff(InstallationFiles.Handoff.CREATE_STARTED, "a".repeat(64)));
        var states = new InstallationSetupStates(files, Optional::empty);
        assertThatThrownBy(states::load).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> states.rotate(scope, StorageSetupStateStore.Purpose.SETUP)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> states.requireCurrent(scope, StorageSetupStateStore.Purpose.SETUP)).isInstanceOf(TapstateException.class);
        assertThat(files.stage().scope()).isEqualTo(scope);
    }

    @Test void configuredAuthorityControlsGenerationAndAnUnavailableStoreCannotFallBack() {
        var files = InstallationFiles.initialize(temporary.resolve("installation"));
        var scope = files.stage().scope();
        var configuration = mock(StorageSetupStateStore.class);
        when(configuration.guardedBy(any())).thenReturn(configuration);
        when(configuration.load()).thenThrow(StorageRuntime.unavailable());
        var states = new InstallationSetupStates(files, () -> Optional.of(configuration));
        assertThatThrownBy(states::load).isInstanceOf(TapstateException.class);
        doReturn(new StorageSetupStateStore.Status(scope, StorageSetupStateStore.Phase.CONFIG_PREPARED, 0)).when(configuration).load();
        assertThat(states.load().phase()).isEqualTo(StorageSetupStateStore.Phase.CONFIG_PREPARED);
        var foreign = new io.tapstate.spi.store.StorageSetupScope("another-cluster", scope.installationId(), scope.operationId(), scope.generation());
        when(configuration.load()).thenReturn(new StorageSetupStateStore.Status(foreign, StorageSetupStateStore.Phase.CONFIG_PREPARED, 0));
        assertThatThrownBy(states::load).isInstanceOf(TapstateException.class);
    }

    @Test void aStaleConfigurationCannotRestoreOrReissueARevokedLocalGeneration() {
        var files = InstallationFiles.initialize(temporary.resolve("installation"));
        var old = files.stage().scope();
        files.saveStage(files.stage().withScope(new io.tapstate.spi.store.StorageSetupScope(old.clusterId(), old.installationId(), old.operationId(), old.generation() + 1)));
        var configuration = mock(StorageSetupStateStore.class);
        var states = new InstallationSetupStates(files, () -> Optional.of(configuration));
        assertThatThrownBy(() -> states.requireCurrent(old, StorageSetupStateStore.Purpose.SETUP)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> states.rotate(old, StorageSetupStateStore.Purpose.SETUP)).isInstanceOf(TapstateException.class);
        verifyNoInteractions(configuration);
    }

    @Test void configurationIoDoesNotHoldTheOwnerFileLockNeededByFinalWriteGuards() throws Exception {
        var files = InstallationFiles.initialize(temporary.resolve("installation"));
        var scope = files.stage().scope();
        var configuration = mock(StorageSetupStateStore.class);
        when(configuration.guardedBy(any())).thenReturn(configuration);
        try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
            doAnswer(call -> {
                assertThat(executor.submit(files::stage).get(2, java.util.concurrent.TimeUnit.SECONDS).scope()).isEqualTo(scope);
                return new StorageSetupStateStore.View(
                        new StorageSetupStateStore.Status(scope, StorageSetupStateStore.Phase.CONFIG_PREPARED, 0),
                        Optional.empty(), Optional.empty());
            }).when(configuration).requireCurrentView(scope, StorageSetupStateStore.Purpose.SETUP);
            when(configuration.load()).thenReturn(new StorageSetupStateStore.Status(scope, StorageSetupStateStore.Phase.CONFIG_PREPARED, 0));
            var states = new InstallationSetupStates(files, () -> Optional.of(configuration));
            states.requireCurrent(scope, StorageSetupStateStore.Purpose.SETUP);
            verify(configuration).requireCurrentView(scope, StorageSetupStateStore.Purpose.SETUP);
        }
    }
}
