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
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings={"SHARED_SRS","CLUSTER"})
    void partialSharedRecoveryCanAssembleItsOwnerWhileWholeCacheRecoveryStillRequiresItsPublication(String kind) {
        var config=configuration();var cache=mock(MongoCacheDomain.class);var business=mock(StorageRuntime.Business.class);
        var auth=new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,PREPARATION.authScope().cacheIncarnation(),2);
        var preparation=new CachePreparation("recovered-auth",auth);when(config.bindings().cachePreparation()).thenReturn(Optional.of(preparation));
        var commands=mock(StorageRecoveryCommands.class);when(config.recoveryCommands()).thenReturn(commands);
        SharedSrsRecoverySnapshot shared=null;
        if(kind.equals("SHARED_SRS")) {
            var previous=new CapturePreparation(preparation.businessScope(),CapturePreparation.Mode.SHARED_SRS,"chain","capture","mc-"+"a".repeat(64),java.util.UUID.randomUUID().toString());
            shared=new SharedSrsRecoverySnapshot(previous,new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture"),new WorkloadOwner("node","boot"),1,0,1),"a".repeat(64),java.util.List.of(),java.util.Map.of());
        }
        var extent=StorageRecoveryCommands.Extent.valueOf(kind);
        var selection=shared==null?new StorageRecoveryCommands.Selection(extent,java.util.Set.of()):new StorageRecoveryCommands.Selection(extent,java.util.Set.of(),"chain","chain");
        var manifest=new StorageRecoveryCommands.Manifest(selection,java.util.Set.of(),java.util.Map.of(),"a".repeat(64),0,shared);
        var plan=new StorageRecoveryCommands.Plan(java.util.UUID.randomUUID().toString(),null,auth,manifest,"b".repeat(64),1,StorageRecoveryCommands.Phase.RUNNING);
        when(commands.current(auth)).thenReturn(Optional.of(plan));when(business.available()).thenReturn(true);
        try(var runtime=new StorageRuntime(config,ignored -> cache)) {
            var bound=runtime.loadBound().orElseThrow();bound.verifyAuthentication();
            if(shared!=null) {runtime.startBusiness(bound,ignored -> business);assertThat(runtime.businessStarted()).isTrue();verify(business).start();}
            else {assertThatThrownBy(() -> runtime.startBusiness(bound,ignored -> business)).isInstanceOf(TapstateException.class);verify(business,never()).start();}
        }
    }
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
            doThrow(StorageRuntime.unavailable()).when(cache).verifyBusinessReady(PREPARATION.businessScope());
            assertThatThrownBy(() -> root.startBusiness(bound, ignored -> { calls.incrementAndGet(); throw new AssertionError(); }))
                    .isInstanceOf(TapstateException.class);
            assertThat(calls).hasValue(0);
            assertThat(root.businessStarted()).isFalse();
        }
    }

    @Test void completedSetupAndVerifiedOriginalCacheStartOnlyOneOwnedRuntimeAndCloseInDependencyOrder() {
        var config = configuration(); var cache = mock(MongoCacheDomain.class); var business = mock(StorageRuntime.Business.class);
        var root = new StorageRuntime(config, binding -> cache);
        when(business.available()).thenReturn(true);
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

    @Test void intactAuthenticationReloadPreservesTheOriginalBusinessOwner() {
        var config = configuration(); var cache = mock(MongoCacheDomain.class); var probe = mock(MongoCacheDomain.class);
        var business = mock(StorageRuntime.Business.class); var connections = new AtomicInteger();
        when(business.available()).thenReturn(true);
        when(config.prepareAuthenticationRecovery(any(), any())).thenReturn(PREPARATION);
        try (var root = new StorageRuntime(config, ignored -> connections.getAndIncrement() == 0 ? cache : probe)) {
            var original = root.loadBound().orElseThrow(); original.verifyAuthentication();
            root.startBusiness(original, ignored -> business);
            assertThat(root.restoreAuthentication()).isSameAs(original);
            assertThat(root.businessStarted()).isTrue(); original.requireActive();
            verify(business, never()).close(); verify(cache, never()).close(); verify(probe).close();
        }
    }

    @Test void recheckingAuthenticationDoesNotTemporarilyRevokeTheOriginalValidatedOwner() throws Exception {
        var config = configuration(); var cache = mock(MongoCacheDomain.class);
        var entered = new java.util.concurrent.CountDownLatch(1); var release = new java.util.concurrent.CountDownLatch(1);
        try (var root = new StorageRuntime(config, ignored -> cache); var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
            var original = root.loadBound().orElseThrow(); original.verifyAuthentication();
            doAnswer(call -> { entered.countDown(); assertThat(release.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue(); return PREPARATION.authScope(); })
                    .when(cache).loadAuthScope(PREPARATION.authScope());
            var recheck = executor.submit(original::verifyAuthentication);
            try {
                assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                assertThatCode(original::requireAuthentication).doesNotThrowAnyException();
            } finally { release.countDown(); }
            recheck.get(5, java.util.concurrent.TimeUnit.SECONDS);
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
