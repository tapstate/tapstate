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

    @Test void managementOverviewCanBeReopenedButArbitraryRoutesRemainClosed() {
        var files = InstallationFiles.initialize(directory.resolve("reopen"));
        var root = mock(StorageRoot.class); var runtime = mock(StorageRuntime.class);
        when(root.mode()).thenReturn(StorageBinding.Mode.OP); when(root.runtime()).thenReturn(runtime);
        when(runtime.configuration()).thenReturn(Optional.empty());
        try (var installation = new OnPremInstallation(root, files, new MongoProperties(), mock(StorageSetupService.DefaultTarget.class), Clock.systemUTC())) {
            var connector = mock(org.apache.catalina.connector.Connector.class);
            when(connector.getLocalPort()).thenReturn(51599); installation.attach(connector);
            var request = mock(org.apache.catalina.connector.Request.class); var attributes = new java.util.HashMap<String,Object>();
            when(request.getConnector()).thenReturn(connector);
            doAnswer(call -> { attributes.put(call.getArgument(0), call.getArgument(1)); return null; }).when(request).setAttribute(anyString(), any());
            when(request.getAttribute(anyString())).thenAnswer(call -> attributes.get(call.getArgument(0)));
            when(request.getDispatcherType()).thenReturn(jakarta.servlet.DispatcherType.REQUEST);
            when(request.getLocalAddr()).thenReturn("127.0.0.1"); when(request.getLocalPort()).thenReturn(51599);
            when(request.getHeader("Host")).thenReturn("127.0.0.1:51599");
            when(request.getMethod()).thenReturn("GET"); when(request.getRequestURI()).thenReturn("/overview");
            installation.markRequest(request);
            assertThatCode(() -> installation.requireManagementRequest(request)).doesNotThrowAnyException();
            when(request.getRequestURI()).thenReturn("/api/sources");
            assertThatThrownBy(() -> installation.requireManagementRequest(request)).isInstanceOf(TapstateException.class);
        }
    }

    @Test void localAuthorizationGenerationIsNotReportedAsADurableConfigurationRevision() {
        var files = InstallationFiles.initialize(directory.resolve("local-revision"));
        var states = new InstallationSetupStates(files, Optional::empty);
        states.rotate(files.stage().scope(), StorageSetupStateStore.Purpose.SETUP);
        states.rotate(files.stage().scope(), StorageSetupStateStore.Purpose.SETUP);
        var root = mock(StorageRoot.class); var runtime = mock(StorageRuntime.class);
        when(root.mode()).thenReturn(StorageBinding.Mode.OP);
        when(root.runtime()).thenReturn(runtime); when(runtime.configuration()).thenReturn(Optional.empty());
        try (var installation = new OnPremInstallation(root, files, new MongoProperties(),
                mock(StorageSetupService.DefaultTarget.class), Clock.systemUTC())) {
            var snapshot = installation.snapshot(mock(io.tapstate.control.core.StorageControlAccess.class), () -> {});
            assertThat(snapshot.setupGeneration()).isGreaterThan(1);
            assertThat(snapshot.stateVersion()).as("CONFIG does not exist until the initial handoff").isZero();
            assertThat(snapshot.bindingVersion()).isZero();
            assertThat(snapshot.setupPhase()).isEqualTo("LOCAL_SETUP");
        }
    }

    @Test void losingInstallationOwnershipFencesTheRootAndPreviouslyRetainedRuntimeViews() {
        var active = new java.util.concurrent.atomic.AtomicBoolean(true);
        var mongo = new MongoProperties(); mongo.setUri(null);
        try (var root = new StorageRoot(mongo, CloudRuntimeSettings.resolve(new CloudProperties()), null, BootId.fresh(),
                new MetricsHistoryProperties(), new RandomTokenSecrets(), Clock.systemUTC(), new byte[32], () -> {
                    if (!active.get()) throw StorageRuntime.unavailable();
                })) {
            var retained = root.runtime(); root.requireActive(); assertThat(retained.current()).isEmpty();
            active.set(false);
            assertThatThrownBy(root::requireActive).isInstanceOf(TapstateException.class);
            assertThatThrownBy(root::runtime).isInstanceOf(TapstateException.class);
            assertThatThrownBy(retained::current).isInstanceOf(TapstateException.class);
        }
    }

    @Test void retainedRuntimeOwnershipChecksDoNotWaitForFileMutationMutexes() throws Exception {
        var files = InstallationFiles.initialize(directory.resolve("lock-order"));
        var held = new java.util.concurrent.CountDownLatch(1); var release = new java.util.concurrent.CountDownLatch(1);
        try (var owner = files.ownRuntime(); var runtime = new StorageRuntime(null, binding -> { throw new AssertionError("no cache is needed"); }, owner::requireActive);
             var executor = java.util.concurrent.Executors.newFixedThreadPool(2)) {
            var mutation = executor.submit(() -> files.locked(() -> {
                held.countDown();
                try { assertThat(release.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue(); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                return null;
            }));
            boolean completedWhileHeld;
            try {
                assertThat(held.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                var done = new java.util.concurrent.CountDownLatch(1);
                var read = executor.submit(() -> { try { runtime.current(); } finally { done.countDown(); } });
                completedWhileHeld = done.await(2, java.util.concurrent.TimeUnit.SECONDS);
                release.countDown(); mutation.get(5, java.util.concurrent.TimeUnit.SECONDS); read.get(5, java.util.concurrent.TimeUnit.SECONDS);
            } finally { release.countDown(); }
            assertThat(completedWhileHeld).as("runtime monitor guards must not acquire the file mutation mutex").isTrue();
        }
    }

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

    @Test void aConfirmedRecoveryCannotAdoptADifferentRegisteredConnectorOnRestart() {
        var files = InstallationFiles.initialize(directory.resolve("recovery")); var setup = files.stage().scope();
        files.saveStage(files.stage().withHandoff(InstallationFiles.Handoff.CONFIGURED, "a".repeat(64)));
        var root = mock(StorageRoot.class); var runtime = mock(StorageRuntime.class);
        var configuration = mock(MongoConfigurationDomain.class); var states = mock(StorageSetupStateStore.class);
        var bound = mock(StorageRuntime.Bound.class); var commands = mock(io.tapstate.spi.store.StorageRecoveryCommands.class);
        var source = new io.tapstate.core.model.SourceResource("views", null, "mongodb", java.util.Map.of("uri", "mongodb://target/master"), null, null, null, null);
        var scope = new io.tapstate.spi.store.CacheAuthScope(setup.clusterId(), 1, StorageBinding.Mode.OP, "incarnation", 1);
        var binding = new StorageBinding(setup, StorageBinding.Mode.OP, 1, "views", new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://cache/cache", "cache", null));
        var manifest = new io.tapstate.spi.store.StorageRecoveryCommands.Manifest(new io.tapstate.spi.store.StorageRecoveryCommands.Selection(
                io.tapstate.spi.store.StorageRecoveryCommands.Extent.CLUSTER, java.util.Set.of()), java.util.Set.of(),
                java.util.Map.of(source.id(), io.tapstate.core.model.canonical.CanonicalHash.of(source)), "f".repeat(64), 1);
        var plan = new io.tapstate.spi.store.StorageRecoveryCommands.Plan(java.util.UUID.randomUUID().toString(), null, scope, manifest, "a".repeat(64), 2,
                io.tapstate.spi.store.StorageRecoveryCommands.Phase.RUNNING);
        when(root.mode()).thenReturn(StorageBinding.Mode.OP); when(root.runtime()).thenReturn(runtime);
        when(root.coordinateInstallation(any())).thenAnswer(call -> ((java.util.function.Supplier<?>)call.getArgument(0)).get());
        when(runtime.configuration()).thenReturn(Optional.of(configuration)); when(runtime.requireConfiguration()).thenReturn(configuration);
        when(runtime.loadBound()).thenReturn(Optional.of(bound)); when(bound.preparation()).thenReturn(new io.tapstate.spi.store.CachePreparation(setup.operationId(), scope));
        when(bound.binding()).thenReturn(binding); when(configuration.setupStates()).thenReturn(states);
        when(states.guardedBy(any())).thenReturn(states); when(states.load()).thenReturn(new StorageSetupStateStore.Status(setup, StorageSetupStateStore.Phase.STORAGE_BOUND, 1));
        when(configuration.recoveryCommands()).thenReturn(commands); when(commands.guardedBy(any())).thenReturn(commands);
        when(commands.find(scope, plan.operationId())).thenReturn(Optional.of(plan)); when(configuration.initialOpRecoverySource(scope, true)).thenReturn(source);
        when(configuration.audit()).thenReturn(record -> {});
        var target = mock(StorageSetupService.DefaultTarget.class);
        when(target.identity(any())).thenReturn(new StorageSetupService.ConnectorIdentity("a".repeat(64), "b".repeat(64)));
        try (var installation = new OnPremInstallation(root, files, new MongoProperties(), target, Clock.systemUTC())) {
            var failure = catchThrowable(() -> installation.recoveryService().confirm("owner", scope, () -> {}, plan.operationId(), plan.planHash(), plan.controlRevision(), true));
            verify(target, never()).test(any(), any(), any()); verify(bound, never()).cache();
            verify(configuration, never()).startInitialOpRecovery(any(), any());
            assertThat(failure).isInstanceOf(TapstateException.class);
        }
    }
}
