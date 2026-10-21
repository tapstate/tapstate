package io.tapstate.app;

import io.tapstate.adapters.mongostore.*;
import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Exercises production continuation ownership; real database and HTTP boundaries have integration coverage. */
class CloudStorageContinuationTest {
    private static final StorageSetupScope SETUP = new StorageSetupScope("cluster", "installation", "setup", 1);
    private static final StorageBinding CONNECTION = new StorageBinding(SETUP, StorageBinding.Mode.CLOUD, 1, "atlas-store",
            new StorageBinding.Connection(StorageBinding.Connection.Kind.ATLAS, "mongodb://cache/cache?tls=true", "cache", "saved-cache-ca"));
    private static final CachePreparation PREPARATION = new CachePreparation("setup", new CacheAuthScope("cluster", 1,
            StorageBinding.Mode.CLOUD, "original-cache", 1));
    private static final SourceResource SOURCE = new SourceResource("atlas-store", null, "mongodb-atlas",
            Map.of("uri", "mongodb://target/master?tls=true", "tlsCaPem", "saved-master-ca"), null, null, null, null);

    private static final class Fixture implements AutoCloseable {
        final MongoConfigurationDomain configuration = mock(MongoConfigurationDomain.class);
        final MongoCacheDomain cache = mock(MongoCacheDomain.class);
        final StorageBindingStore bindings = mock(StorageBindingStore.class);
        final StorageSetupStateStore states = mock(StorageSetupStateStore.class);
        final StorageSetupService.DefaultTarget target = mock(StorageSetupService.DefaultTarget.class);
        final FirstCacheCreation grant = mock(FirstCacheCreation.class);
        final StorageBindingCommit committed = new StorageBindingCommit(CONNECTION, PREPARATION, Optional.of(grant));
        final AtomicReference<StorageSetupStateStore.Phase> phase = new AtomicReference<>(StorageSetupStateStore.Phase.STORAGE_BOUND);
        final MockedStatic<MongoConfigurationDomain> configurations = mockStatic(MongoConfigurationDomain.class);
        final MockedStatic<MongoCacheDomain> caches = mockStatic(MongoCacheDomain.class);
        final StorageRoot root;
        Fixture() {
            when(configuration.bindings()).thenReturn(bindings); when(configuration.setupStates()).thenReturn(states);
            when(configuration.databaseName()).thenReturn("config");
            when(bindings.load()).thenReturn(Optional.of(CONNECTION)); when(bindings.cachePreparation()).thenReturn(Optional.of(PREPARATION));
            when(bindings.guardedBy(any())).thenReturn(bindings);
            when(states.load()).thenAnswer(ignored -> new StorageSetupStateStore.Status(SETUP, phase.get(), 1));
            when(bindings.complete(SETUP)).thenAnswer(ignored -> { phase.set(StorageSetupStateStore.Phase.COMPLETE); return CONNECTION; });
            var artifacts = mock(ArtifactStore.class); when(configuration.artifacts()).thenReturn(artifacts);
            when(artifacts.get("atlas-store")).thenReturn(Optional.of(SOURCE));
            when(cache.cloudSessions(PREPARATION.authScope())).thenReturn(mock(CloudSessionStore.class));
            when(target.identity(any())).thenReturn(new StorageSetupService.ConnectorIdentity("a".repeat(64), "b".repeat(64)));
            configurations.when(() -> MongoConfigurationDomain.open(any(MongoConnectionSettings.class), eq("cluster"))).thenReturn(configuration);
            caches.when(() -> MongoCacheDomain.connect(any(), any(), anyString(), any(), any())).thenReturn(cache);
            var cloud = new CloudProperties(); cloud.setBaseUrl("https://cloud.example"); cloud.setToken("fixture-token");
            cloud.setClusterId("cluster"); cloud.setAtlasUri("mongodb://config/config");
            root = new StorageRoot(new MongoProperties(), CloudRuntimeSettings.resolve(cloud), mock(CloudSdkBridge.class),
                    BootId.fresh(), new MetricsHistoryProperties(), new RandomTokenSecrets(), Clock.systemUTC(), new byte[32]);
        }
        StorageRuntime.Business business() {
            var value = mock(StorageRuntime.Business.class); when(value.available()).thenReturn(true); return value;
        }
        void continueWith(StorageRuntime.Business business) { root.continueCloudSetup(committed, target, ignored -> business); }
        @Override public void close() { try { root.close(); } finally { caches.close(); configurations.close(); } }
    }

    @Test void completionUsesSavedTrustClosesTemporaryAuthenticationAndStartsOnlyOneBusinessOwner() {
        try (var f = new Fixture()) {
            var temporary = f.root.managedAuthentication(); var business = f.business();
            f.continueWith(business); f.continueWith(business);
            assertThat(f.root.phase()).isEqualTo(StorageRoot.Phase.BUSINESS_READY);
            assertThat(f.root.runtime().businessStarted()).isTrue();
            assertThatThrownBy(temporary.requireActive()::run).isInstanceOf(TapstateException.class);
            assertThatCode(f.root.managedAuthentication().requireActive()::run).doesNotThrowAnyException();
            var order = inOrder(f.target, f.cache, f.bindings, business);
            order.verify(f.target).test(new StorageBinding.Connection(StorageBinding.Connection.Kind.ATLAS,
                    "mongodb://target/master?tls=true", "master", "saved-master-ca"), SOURCE,
                    new StorageSetupService.ConnectorIdentity("a".repeat(64), "b".repeat(64)));
            order.verify(f.cache).prepareFirstAuth(f.grant);
            order.verify(f.cache).prepareInitialBusiness(eq(PREPARATION), any());
            order.verify(f.bindings).complete(SETUP); order.verify(business).start();
            verify(business, times(1)).start(); verify(f.cache, times(1)).prepareFirstAuth(f.grant);
            verify(f.cache, never()).close();
        }
    }

    @Test void failedSavedTargetCheckRetainsTheUnconsumedGrantAndNeverPublishesCompletion() {
        try (var f = new Fixture()) {
            doThrow(StorageRuntime.unavailable()).doNothing().when(f.target).test(any(), any(), any());
            var business = f.business();
            assertThatThrownBy(() -> f.continueWith(business)).isInstanceOf(TapstateException.class);
            verify(f.cache, never()).prepareFirstAuth(any()); verify(f.bindings, never()).complete(any());
            assertThat(f.root.runtime().business()).isEmpty();
            f.continueWith(business); verify(f.cache, times(1)).prepareFirstAuth(f.grant);
        }
    }

    @Test void uncertainFirstPreparationNeverReissuesTheConsumedCreationOnRetry() {
        try (var f = new Fixture()) {
            doThrow(StorageRuntime.unavailable()).when(f.cache).prepareFirstAuth(f.grant);
            doThrow(StorageRuntime.unavailable()).when(f.cache).resumeInitialAuth(eq(PREPARATION), any());
            var business = f.business();
            assertThatThrownBy(() -> f.continueWith(business)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.continueWith(business)).isInstanceOf(TapstateException.class);
            verify(f.cache, times(1)).prepareFirstAuth(f.grant);
            verify(f.cache).resumeInitialAuth(eq(PREPARATION), any());
            verify(f.bindings, never()).complete(any()); verify(business, never()).start();
        }
    }

    @Test void reopenedSetupCanOnlyResumeRetainedCacheFacts() {
        try (var f = new Fixture()) {
            f.root.continueCloudSetup(new StorageBindingCommit(CONNECTION, PREPARATION, Optional.empty()), f.target, ignored -> f.business());
            verify(f.cache, never()).prepareFirstAuth(any()); verify(f.cache).resumeInitialAuth(eq(PREPARATION), any());
            assertThat(f.phase.get()).isEqualTo(StorageSetupStateStore.Phase.COMPLETE);
        }
    }

    @Test void failedBusinessClosesItsOwnCandidateAndRetriesWithoutRepeatingCommittedPreparation() {
        try (var f = new Fixture()) {
            var failed = f.business(); doThrow(StorageRuntime.unavailable()).when(failed).start();
            assertThatThrownBy(() -> f.continueWith(failed)).isInstanceOf(TapstateException.class);
            verify(failed).close(); assertThat(f.phase.get()).isEqualTo(StorageSetupStateStore.Phase.COMPLETE);
            assertThatCode(f.root.managedAuthentication().requireActive()::run).doesNotThrowAnyException();
            var successor = f.business(); f.continueWith(successor);
            verify(failed, times(1)).close(); verify(f.cache, never()).close();
            verify(f.cache, times(1)).prepareInitialBusiness(eq(PREPARATION), any());
            verify(f.target, times(1)).test(any(), any(), any()); verify(successor).start();
        }
    }

    @Test void unknownCleanupKeepsTheOriginalCompletionButForbidsAnotherCandidate() {
        try (var f = new Fixture()) {
            var failed = f.business(); doThrow(StorageRuntime.unavailable()).when(failed).start();
            doThrow(StorageRuntime.unavailable()).when(failed).close();
            assertThatThrownBy(() -> f.continueWith(failed)).isInstanceOf(TapstateException.class);
            assertThat(f.root.runtime().cleanupRequired()).isTrue();
            assertThatThrownBy(() -> f.root.continueCloudSetup(f.committed, f.target, ignored -> {
                throw new AssertionError("unknown old activity must prevent another assembly");
            })).isInstanceOf(TapstateException.class);
            assertThat(f.phase.get()).isEqualTo(StorageSetupStateStore.Phase.COMPLETE);
        }
    }

    @Test void aFailureAfterCompletionCanResumeFormalAuthenticationInTheSameProcess() {
        try (var f = new Fixture()) {
            var temporary = f.root.managedAuthentication(); var probe = mock(MongoCacheDomain.class);
            f.caches.when(() -> MongoCacheDomain.connect(any(), any(), anyString(), any(), any())).thenReturn(f.cache, probe);
            doThrow(StorageRuntime.unavailable()).doNothing().when(f.configuration).requireAuthenticationPublished(PREPARATION);
            assertThatThrownBy(() -> f.continueWith(f.business())).isInstanceOf(TapstateException.class);
            assertThat(f.phase.get()).isEqualTo(StorageSetupStateStore.Phase.COMPLETE);
            assertThatThrownBy(temporary.requireActive()::run).isInstanceOf(TapstateException.class);
            assertThatCode(f.root.managedAuthentication().requireActive()::run).doesNotThrowAnyException();
            verify(f.cache, times(1)).prepareFirstAuth(f.grant);
            verify(f.bindings, times(1)).complete(SETUP);
            verify(probe).close(); verify(f.cache, never()).close();
        }
    }

    @Test void aDifferentCommittedScopeCannotActivateOrCloseTheCurrentOwner() {
        try (var f = new Fixture()) {
            var business = f.business(); f.continueWith(business);
            var other = new CachePreparation("other-operation", new CacheAuthScope("cluster", 1, StorageBinding.Mode.CLOUD, "other-cache", 1));
            assertThatThrownBy(() -> f.root.continueCloudSetup(new StorageBindingCommit(CONNECTION, other, Optional.empty()), f.target,
                    ignored -> { throw new AssertionError("must not assemble a different owner"); })).isInstanceOf(TapstateException.class);
            assertThat(f.root.runtime().businessStarted()).isTrue(); verify(business, never()).close(); verify(f.cache, never()).close();
        }
    }
}
