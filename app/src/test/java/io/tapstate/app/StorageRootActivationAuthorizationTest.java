package io.tapstate.app;

import io.tapstate.adapters.mongostore.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class StorageRootActivationAuthorizationTest {
    private static final StorageSetupScope SETUP = new StorageSetupScope("cluster", "installation", "setup", 1);
    private static final CachePreparation PREPARATION = new CachePreparation("setup",
            new CacheAuthScope("cluster", 1, StorageBinding.Mode.OP, "original-cache", 1));
    private static final StorageBinding.Connection CONFIG = new StorageBinding.Connection(
            StorageBinding.Connection.Kind.MONGODB, "mongodb://config/config", "config", null);
    private static final StorageBinding BINDING = new StorageBinding(SETUP, StorageBinding.Mode.OP, 1, "views",
            new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://cache/cache", "cache", null));

    private static final class Fixture implements AutoCloseable {
        final MongoConfigurationDomain configuration = mock(MongoConfigurationDomain.class);
        final StorageBindingStore bindings = mock(StorageBindingStore.class);
        final StorageSetupStateStore states = mock(StorageSetupStateStore.class);
        final MongoCacheDomain cache = mock(MongoCacheDomain.class);
        final MockedStatic<MongoCacheDomain> caches = mockStatic(MongoCacheDomain.class);
        final StorageRoot root;
        Fixture() {
            when(configuration.bindings()).thenReturn(bindings);
            when(configuration.setupStates()).thenReturn(states);
            when(configuration.recoveryCommands()).thenReturn(mock(StorageRecoveryCommands.class));
            when(configuration.databaseName()).thenReturn("config");
            when(bindings.load()).thenReturn(Optional.of(BINDING));
            when(bindings.cachePreparation()).thenReturn(Optional.of(PREPARATION));
            when(states.load()).thenReturn(new StorageSetupStateStore.Status(SETUP, StorageSetupStateStore.Phase.COMPLETE, 1));
            var artifacts = mock(ArtifactStore.class); when(configuration.artifacts()).thenReturn(artifacts);
            when(artifacts.get("views")).thenReturn(Optional.of(new SourceResource("views", null, "mongodb",
                    Map.of("uri", "mongodb://target/master"), null, null, null, null)));
            caches.when(() -> MongoCacheDomain.connect(any(), any(), anyString(), any(), any())).thenReturn(cache);
            var mongo = new MongoProperties(); mongo.setUri(null);
            root = new StorageRoot(mongo, CloudRuntimeSettings.resolve(new CloudProperties()), null, BootId.fresh(),
                    new MetricsHistoryProperties(), new RandomTokenSecrets(), Clock.systemUTC(), new byte[32]);
            root.installOnPremConfiguration(configuration, CONFIG);
        }
        StorageRuntime.Bound authenticated() {
            var bound = root.runtime().loadBound().orElseThrow(); bound.verifyAuthentication(); return bound;
        }
        @Override public void close() { try { root.close(); } finally { caches.close(); } }
    }

    @Test void onlyTheOriginalUnreadyInstallationCanAuthorizeAnActivationRetry() {
        try (var f = new Fixture()) {
            assertThat(f.root.completedInstallationActivationAllowed()).isTrue();
            verify(f.configuration).requireOriginalInitialPreparation(PREPARATION);
            verify(f.cache, never()).prepareFirstAuth(any());
            assertThat(f.root.runtime().businessStarted()).isFalse();
        }
    }

    @Test void aMissingOrRevokedPreparationCannotAuthorizeAnActivationRetry() {
        try (var f = new Fixture()) {
            when(f.bindings.cachePreparation()).thenReturn(Optional.empty());
            assertThatThrownBy(f.root::completedInstallationActivationAllowed).isInstanceOf(TapstateException.class);
            when(f.bindings.cachePreparation()).thenReturn(Optional.of(PREPARATION));
            doThrow(StorageRuntime.unavailable()).when(f.configuration).requireOriginalInitialPreparation(PREPARATION);
            assertThatThrownBy(f.root::completedInstallationActivationAllowed).isInstanceOf(TapstateException.class);
            verify(f.cache, never()).prepareFirstAuth(any());
        }
    }

    @Test void cloudOrRecoveredAuthenticationCannotAcquireInitialOpCreationRights() {
        try (var f = new Fixture()) {
            var cloud = new CachePreparation("setup", new CacheAuthScope("cluster", 1, StorageBinding.Mode.CLOUD, "original-cache", 1));
            var recovered = new CachePreparation("setup", new CacheAuthScope("cluster", 1, StorageBinding.Mode.OP, "original-cache", 2));
            when(f.bindings.cachePreparation()).thenReturn(Optional.of(cloud));
            assertThat(f.root.completedInstallationActivationAllowed()).isFalse();
            when(f.bindings.cachePreparation()).thenReturn(Optional.of(recovered));
            assertThat(f.root.completedInstallationActivationAllowed()).isFalse();
            verify(f.configuration, never()).requireOriginalInitialPreparation(any());
        }
    }

    @Test void aPublishedBusinessOwnerClosesTheInstallerRetryDoor() {
        try (var f = new Fixture()) {
            var bound = f.authenticated(); var business = mock(StorageRuntime.Business.class);
            when(business.available()).thenReturn(true);
            f.root.runtime().startBusiness(bound, ignored -> business);
            assertThat(f.root.runtime().businessStarted()).isTrue();
            assertThat(f.root.completedInstallationActivationAllowed()).isFalse();
            verify(f.configuration, never()).requireOriginalInitialPreparation(any());
        }
    }

    @Test void unconfirmedCandidateCleanupCannotBeBypassedByAnotherActivation() {
        try (var f = new Fixture()) {
            var bound = f.authenticated(); var business = mock(StorageRuntime.Business.class);
            doThrow(StorageRuntime.unavailable()).when(business).start();
            doThrow(StorageRuntime.unavailable()).when(business).close();
            assertThatThrownBy(() -> f.root.runtime().startBusiness(bound, ignored -> business)).isInstanceOf(TapstateException.class);
            assertThat(f.root.runtime().cleanupRequired()).isTrue();
            assertThat(f.root.completedInstallationActivationAllowed()).isFalse();
            verify(business).close();
            verify(f.configuration, never()).requireOriginalInitialPreparation(any());
            doNothing().when(business).close();
        }
    }
}
