package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.control.core.StorageSetupService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.spi.store.StorageSetupStateStore;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class OnPremInstallationTest {
    @TempDir Path directory;

    @Test void retainedRestartMarkerCannotSkipOriginalConfigurationIdentityValidation() {
        var files = InstallationFiles.initialize(directory.resolve("installation"));
        files.saveStage(files.stage().withHandoff(InstallationFiles.Handoff.RESTARTED, "a".repeat(64)));
        var root = mock(StorageRoot.class); var runtime = mock(StorageRuntime.class);
        var configuration = mock(MongoConfigurationDomain.class); var states = mock(StorageSetupStateStore.class);
        when(root.mode()).thenReturn(StorageBinding.Mode.OP); when(root.runtime()).thenReturn(runtime);
        when(runtime.configuration()).thenReturn(Optional.of(configuration)); when(configuration.setupStates()).thenReturn(states);
        when(states.guardedBy(any())).thenReturn(states);
        when(states.load()).thenReturn(new StorageSetupStateStore.Status(new StorageSetupScope("another-cluster", "installation", "operation", 1), StorageSetupStateStore.Phase.COMPLETE, 1));
        assertThatThrownBy(() -> new OnPremInstallation(root, files, new MongoProperties(), mock(StorageSetupService.DefaultTarget.class), Clock.systemUTC()))
                .isInstanceOf(TapstateException.class);
    }
}
