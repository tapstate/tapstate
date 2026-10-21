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
                return null;
            }).when(configuration).requireCurrent(scope, StorageSetupStateStore.Purpose.SETUP);
            var states = new InstallationSetupStates(files, () -> Optional.of(configuration));
            states.requireCurrent(scope, StorageSetupStateStore.Purpose.SETUP);
        }
    }
}
