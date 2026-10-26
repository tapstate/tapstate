package io.tapstate.app;

import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.servlet.HandlerExecutionChain;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class StorageRootConfigurationTest {
    @Test void installationSnapshotsStillUseTheInstallationAuthorizationPath() {
        var root = mock(StorageRoot.class);
        var installation = mock(OnPremInstallation.class);
        var provider = mock(org.springframework.beans.factory.ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(installation);
        var caller = mock(io.tapstate.control.core.StorageControlAccess.class);
        when(caller.cacheScope()).thenReturn(Optional.empty());
        Runnable active = () -> { throw new TapstateException(ControlError.BOOTSTRAP_FORBIDDEN, java.util.Map.of(), null); };
        when(installation.snapshot(caller, active)).thenAnswer(call -> {
            call.<Runnable>getArgument(1).run(); throw new AssertionError("rejected installation returned a snapshot");
        });
        assertThatThrownBy(() -> new StorageRootConfiguration().storageSnapshots(root, provider).load(caller, active))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ControlError.BOOTSTRAP_FORBIDDEN));
        verify(root, never()).snapshotProjection(any());
    }

    @Test void theRootSnapshotIncludesTheCurrentBusinessVerification() {
        var root = root();
        var cloud = mock(CloudRuntimeSettings.class);
        ReflectionTestUtils.setField(root, "cloud", cloud);
        ReflectionTestUtils.setField(root, "phase", StorageRoot.Phase.BUSINESS_READY);
        var configuration = mock(io.tapstate.adapters.mongostore.MongoConfigurationDomain.class);
        var states = mock(io.tapstate.spi.store.StorageSetupStateStore.class);
        var bindings = mock(io.tapstate.spi.store.StorageBindingStore.class);
        var recovery = mock(io.tapstate.spi.store.StorageRecoveryCommands.class);
        var setup = new io.tapstate.spi.store.StorageSetupScope("cluster", "install", "setup", 1);
        var scope = new io.tapstate.spi.store.CacheAuthScope("cluster", 1, io.tapstate.spi.store.StorageBinding.Mode.OP, "incarnation", 1);
        when(root.runtime().requireConfiguration()).thenReturn(configuration);
        when(root.runtime().businessStarted()).thenReturn(true);
        when(configuration.setupStates()).thenReturn(states);
        when(states.load()).thenReturn(new io.tapstate.spi.store.StorageSetupStateStore.Status(setup,
                io.tapstate.spi.store.StorageSetupStateStore.Phase.COMPLETE, 1));
        when(configuration.bindings()).thenReturn(bindings);
        when(bindings.cachePreparation()).thenReturn(Optional.of(new io.tapstate.spi.store.CachePreparation("setup", scope)));
        when(configuration.recoveryCommands()).thenReturn(recovery);
        when(recovery.current(scope)).thenReturn(Optional.empty());
        var business = mock(NativeBusinessContext.class);
        when(root.runtime().business()).thenReturn(Optional.of(business));
        var verified = new io.tapstate.control.core.DefaultSourceVerification.Verification(
                io.tapstate.control.core.DefaultSourceVerification.Status.PASSED,
                java.time.Instant.parse("2026-10-26T01:00:00Z"), java.time.Instant.parse("2026-10-26T01:10:00Z"));
        when(business.applicationDataVerification()).thenReturn(verified);
        var caller = io.tapstate.control.core.StorageControlAccess.ordinary(io.tapstate.spi.store.StorageBinding.Mode.OP,
                setup, scope, io.tapstate.control.core.TapstatePrincipal.humanJwt(new io.tapstate.control.core.VerifiedToken(
                        "admin", io.tapstate.control.core.Scope.ADMIN)), null, () -> {});
        var result = root.snapshot(caller, () -> {});
        assertThat(result.health().get(io.tapstate.control.core.StorageSetupService.Layer.MASTER))
                .isEqualTo(io.tapstate.control.core.StorageControlDispatcher.Health.AVAILABLE);
        assertThat(result.applicationDataVerification()).isEqualTo(verified);
        assertThat(result.allowedActions()).contains("connection.test");
        ReflectionTestUtils.setField(root, "phase", StorageRoot.Phase.AUTH_READY);
        assertThat(root.snapshot(caller, () -> {}).applicationDataVerification().status())
                .isEqualTo(io.tapstate.control.core.DefaultSourceVerification.Status.UNKNOWN);
    }
    @Test void aPublishedBusinessHandlerRetainsItsOriginalRequest() throws Exception {
        var root = root();
        var business = mock(NativeBusinessContext.class);
        when(root.runtime().businessForLiveness()).thenReturn(Optional.of(business));
        var request = new MockHttpServletRequest("GET", "/");
        var selected = new HandlerExecutionChain(new Object());
        when(business.handlerFor(request)).thenReturn(selected);

        assertThat(new StorageRootConfiguration().runtimeHttpHandlers(root).handlerFor(request)).isSameAs(selected);
        verify(business).handlerFor(request);
    }

    @Test void aCloseRequestedDuringSelectionCannotPublishABusinessHandler() throws Exception {
        var root = root();
        var business = mock(NativeBusinessContext.class);
        when(root.runtime().businessForLiveness()).thenReturn(Optional.of(business));
        var request = new MockHttpServletRequest("GET", "/");
        when(business.handlerFor(request)).thenAnswer(ignored -> {
            ReflectionTestUtils.setField(root, "closeRequested", true);
            return new HandlerExecutionChain(new Object());
        });

        assertThatThrownBy(() -> new StorageRootConfiguration().runtimeHttpHandlers(root).handlerFor(request))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE));
    }

    private static StorageRoot root() {
        var root = mock(StorageRoot.class, CALLS_REAL_METHODS);
        ReflectionTestUtils.setField(root, "runtime", mock(StorageRuntime.class));
        ReflectionTestUtils.setField(root, "installationActive", (Runnable) () -> {});
        return root;
    }
}
