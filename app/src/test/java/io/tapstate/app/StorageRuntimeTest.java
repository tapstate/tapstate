package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class StorageRuntimeTest {
    private static final StorageSetupScope SETUP = new StorageSetupScope("cluster", "installation", "original", 1);
    private static final StorageBinding BINDING = new StorageBinding(SETUP, StorageBinding.Mode.OP, 1, "views",
            new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://cache/tapstate_cache", "tapstate_cache", null));
    private static final CachePreparation PREPARATION = new CachePreparation("original", new CacheAuthScope("cluster", 1,
            StorageBinding.Mode.OP, "8a5bbd9e-e330-4238-a001-98f5f104ef2c", 1));

    @Test void noConfigurationOrUnboundConfigurationNeverOpensACacheConnection() {
        var calls = new AtomicInteger();
        try (var local = new StorageRuntime(null, binding -> { calls.incrementAndGet(); throw new AssertionError(); })) {
            assertThat(local.configuration()).isEmpty();
            assertThat(local.loadBound()).isEmpty();
        }
        var config = configuration();
        when(config.bindings().load()).thenReturn(Optional.empty());
        try (var root = new StorageRuntime(config, binding -> { calls.incrementAndGet(); throw new AssertionError(); })) {
            assertThat(root.configuration()).contains(config);
            assertThat(root.loadBound()).isEmpty();
            assertThat(root.businessStarted()).isFalse();
        }
        assertThat(calls).hasValue(0);
    }

    @Test void authenticationDoesNotCreateABusinessRuntimeAndMissingBusinessFactsCannotStartOne() {
        var config = configuration(); var cache = mock(MongoCacheDomain.class); var calls = new AtomicInteger();
        try (var root = new StorageRuntime(config, binding -> cache)) {
            var bound = root.loadBound().orElseThrow(); bound.verifyAuthentication();
            verify(cache).loadAuthScope(PREPARATION.authScope());
            verify(cache, never()).prepareAuth(any(), any(), any());
            verify(cache, never()).prepareInitialBusiness(any(), any());
            doThrow(StorageRuntime.unavailable()).when(cache).loadBusinessScope(PREPARATION.businessScope());
            assertThatThrownBy(() -> root.startBusiness(bound, ignored -> { calls.incrementAndGet(); throw new AssertionError(); }))
                    .isInstanceOf(TapstateException.class);
            assertThat(calls).hasValue(0);
            assertThat(root.businessStarted()).isFalse();
        }
    }

    @Test void completedSetupAndVerifiedOriginalCacheStartOnlyOneOwnedRuntimeAndCloseInDependencyOrder() {
        var config = configuration(); var cache = mock(MongoCacheDomain.class); var business = mock(StorageRuntime.Business.class);
        var root = new StorageRuntime(config, binding -> cache);
        var bound = root.loadBound().orElseThrow(); bound.verifyAuthentication();
        root.startBusiness(bound, ignored -> business);
        root.startBusiness(bound, ignored -> { throw new AssertionError("duplicate runtime"); });
        assertThat(root.businessStarted()).isTrue(); verify(business, times(1)).start();
        root.close(); root.close();
        var ordered = inOrder(business, cache, config);
        ordered.verify(business).close(); ordered.verify(cache).close(); ordered.verify(config).close();
        assertThatThrownBy(bound::requireActive).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> root.startBusiness(bound, ignored -> business)).isInstanceOf(TapstateException.class);
    }

    @Test void failedCandidateIsClosedBeforeRetryAndUnknownCleanupPreventsAnotherAssembly() {
        var config = configuration(); var cache = mock(MongoCacheDomain.class); var candidate = mock(StorageRuntime.Business.class);
        doThrow(StorageRuntime.unavailable()).when(candidate).start();
        doThrow(StorageRuntime.unavailable()).when(candidate).close();
        try (var root = new StorageRuntime(config, binding -> cache)) {
            var bound = root.loadBound().orElseThrow(); bound.verifyAuthentication();
            assertThatThrownBy(() -> root.startBusiness(bound, ignored -> candidate)).isInstanceOf(TapstateException.class);
            verify(candidate).close();
            assertThatThrownBy(() -> root.startBusiness(bound, ignored -> { throw new AssertionError("cleanup unknown"); }))
                    .isInstanceOf(TapstateException.class);
            assertThat(root.businessStarted()).isFalse();
        }
    }

    @Test void businessCleanupErrorStillClosesCacheAndConfiguration() {
        var config = configuration(); var cache = mock(MongoCacheDomain.class); var business = mock(StorageRuntime.Business.class);
        var failure = new AssertionError("business cleanup failed"); doThrow(failure).when(business).close();
        var root = new StorageRuntime(config, binding -> cache);
        var bound = root.loadBound().orElseThrow(); bound.verifyAuthentication(); root.startBusiness(bound, ignored -> business);
        assertThat(catchThrowable(root::close)).isSameAs(failure);
        verify(cache).close(); verify(config).close();
    }

    @Test void candidateCleanupErrorPreservesTheStartupFailureAndBlocksAnotherCandidate() {
        var config = configuration(); var cache = mock(MongoCacheDomain.class); var business = mock(StorageRuntime.Business.class);
        var startup = StorageRuntime.unavailable(); var cleanup = new AssertionError("candidate cleanup failed");
        doThrow(startup).when(business).start(); doThrow(cleanup).when(business).close();
        try (var root = new StorageRuntime(config, binding -> cache)) {
            var bound = root.loadBound().orElseThrow(); bound.verifyAuthentication();
            assertThat(catchThrowable(() -> root.startBusiness(bound, ignored -> business))).isSameAs(startup);
            assertThat(startup.getSuppressed()).containsExactly(cleanup);
            assertThatThrownBy(() -> root.startBusiness(bound, ignored -> { throw new AssertionError("second candidate"); }))
                    .isInstanceOf(TapstateException.class);
        }
    }

    private static MongoConfigurationDomain configuration() {
        var config = mock(MongoConfigurationDomain.class);
        var bindings = mock(StorageBindingStore.class); var setup = mock(StorageSetupStateStore.class);
        when(config.bindings()).thenReturn(bindings); when(config.setupStates()).thenReturn(setup);
        when(bindings.load()).thenReturn(Optional.of(BINDING)); when(bindings.cachePreparation()).thenReturn(Optional.of(PREPARATION));
        when(setup.load()).thenReturn(new StorageSetupStateStore.Status(SETUP, StorageSetupStateStore.Phase.COMPLETE, 1));
        return config;
    }
}
