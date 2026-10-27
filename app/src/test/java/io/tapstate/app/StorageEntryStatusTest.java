package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.control.restapi.StorageEntryStatus;
import io.tapstate.spi.store.*;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class StorageEntryStatusTest {
    private final StorageRoot root = mock(StorageRoot.class);
    private final StorageRuntime runtime = mock(StorageRuntime.class);
    private final MongoConfigurationDomain configuration = mock(MongoConfigurationDomain.class);
    private final StorageSetupStateStore states = mock(StorageSetupStateStore.class);
    private final OnPremInstallation installation = mock(OnPremInstallation.class);
    @SuppressWarnings("unchecked") private final ObjectProvider<OnPremInstallation> installations = mock(ObjectProvider.class);
    private final StorageSetupScope scope = new StorageSetupScope("cluster", "install", "setup", 1);

    private StorageEntryStatus reader() {
        when(root.mode()).thenReturn(StorageBinding.Mode.OP);
        when(root.runtime()).thenReturn(runtime);
        when(runtime.configuration()).thenReturn(Optional.of(configuration));
        when(runtime.requireConfiguration()).thenReturn(configuration);
        when(configuration.setupStates()).thenReturn(states);
        when(installations.getIfAvailable()).thenReturn(installation);
        return new StorageRootConfiguration().storageEntryStatus(root, installations);
    }
    @Test void completedSetupRequiresRestartEvenWhileTheRootWaitsInSetupPhase() {
        var reader = reader();
        when(root.phase()).thenReturn(StorageRoot.Phase.SETUP_REQUIRED);
        when(states.load()).thenReturn(new StorageSetupStateStore.Status(scope, StorageSetupStateStore.Phase.COMPLETE, 1));
        when(installation.normalStartupAllowed()).thenReturn(false);
        assertThat(reader.read().entry()).isEqualTo(StorageEntryStatus.Entry.RESTART_REQUIRED);
        verify(runtime, never()).loadBound();
    }
    @Test void unreadableRetainedSetupCannotBeAdvertisedAsAnEmptyInstallation() {
        var reader = reader();
        when(root.phase()).thenReturn(StorageRoot.Phase.SETUP_REQUIRED);
        when(states.load()).thenThrow(StorageRuntime.unavailable());
        assertThat(reader.read().entry()).isEqualTo(StorageEntryStatus.Entry.UNAVAILABLE);
        verify(runtime, never()).loadBound();
    }
    @Test void cacheOutageCannotReopenInstallation() {
        var reader = reader();
        when(root.phase()).thenReturn(StorageRoot.Phase.CACHE_UNAVAILABLE);
        assertThat(reader.read().entry()).isEqualTo(StorageEntryStatus.Entry.UNAVAILABLE);
        verifyNoInteractions(states);
    }
}
