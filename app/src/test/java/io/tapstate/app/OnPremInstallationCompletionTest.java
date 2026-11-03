package io.tapstate.app;

import io.tapstate.adapters.mongostore.*;
import io.tapstate.control.core.*;
import io.tapstate.control.restapi.StorageEntryStatus;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import java.nio.file.Path;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class OnPremInstallationCompletionTest {
    @TempDir Path directory;
    private static final StorageBinding.Connection CONFIG = new StorageBinding.Connection(
            StorageBinding.Connection.Kind.MONGODB, "mongodb://config/config", "config", null);

    private static final class Fixture implements AutoCloseable {
        final InstallationFiles files;
        final StorageSetupScope scope;
        final AtomicReference<StorageSetupScope> currentScope;
        final CachePreparation preparation;
        final StorageBinding binding;
        final MongoConfigurationDomain configuration = mock(MongoConfigurationDomain.class);
        final MongoCacheDomain cache = mock(MongoCacheDomain.class);
        final StorageBindingStore bindings = mock(StorageBindingStore.class);
        final StorageSetupStateStore states = mock(StorageSetupStateStore.class);
        final StorageSetupService.DefaultTarget target = mock(StorageSetupService.DefaultTarget.class);
        final StorageRuntime.Business business = mock(StorageRuntime.Business.class);
        final AtomicReference<StorageSetupStateStore.Phase> phase = new AtomicReference<>(StorageSetupStateStore.Phase.STORAGE_BOUND);
        final AtomicInteger completions = new AtomicInteger();
        final MockedStatic<MongoCacheDomain> caches = mockStatic(MongoCacheDomain.class);
        final StorageRoot root;
        final OnPremInstallation installation;
        final StorageEntryStatus entry;
        final org.apache.catalina.connector.Request request = mock(org.apache.catalina.connector.Request.class);
        Fixture(Path directory) { this(directory, 1, false); }
        Fixture(Path directory, long authGeneration) { this(directory, authGeneration, false); }
        Fixture(Path directory, long authGeneration, boolean automatic) {
            files = spy(InstallationFiles.initialize(directory)); scope = files.stage().scope(); currentScope = new AtomicReference<>(scope);
            preparation = new CachePreparation(scope.operationId(), new CacheAuthScope(scope.clusterId(), 1, StorageBinding.Mode.OP, "original-cache", authGeneration));
            binding = new StorageBinding(scope, StorageBinding.Mode.OP, 1, "views", new StorageBinding.Connection(
                    StorageBinding.Connection.Kind.MONGODB, "mongodb://cache/cache", "cache", null));
            files.saveConnection(CONFIG.uri(), null);
            files.saveStage(files.stage().withHandoff(InstallationFiles.Handoff.CONFIGURED, OnPremInstallation.configurationHash(CONFIG)));
            files.saveCapability(new SetupCapabilityService.Issued(scope, StorageSetupStateStore.Purpose.SETUP,
                    Clock.systemUTC().instant().plusSeconds(300), "a".repeat(43)));
            when(configuration.bindings()).thenReturn(bindings); when(configuration.setupStates()).thenReturn(states);
            when(configuration.databaseName()).thenReturn("config"); when(configuration.recoveryCommands()).thenReturn(mock(StorageRecoveryCommands.class));
            when(configuration.users()).thenReturn(mock(UserStore.class)); when(configuration.tokens()).thenReturn(mock(TokenStore.class));
            when(configuration.clusterIdentity()).thenReturn(mock(ClusterIdentityStore.class));
            when(configuration.audit()).thenReturn(mock(AuditStore.class));
            var firstAdmin = mock(FirstAdminStore.class); when(configuration.firstAdmin()).thenReturn(firstAdmin);
            when(firstAdmin.load()).thenAnswer(ignored -> Optional.of(new FirstAdminStore.Status(currentScope.get(), 1, FirstAdminStore.Phase.PENDING, null)));
            when(cache.sessions(preparation.authScope())).thenReturn(mock(SessionStore.class));
            when(bindings.load()).thenReturn(Optional.of(binding)); when(bindings.cachePreparation()).thenReturn(Optional.of(preparation));
            when(bindings.guardedBy(any())).thenReturn(bindings);
            when(bindings.complete(any())).thenAnswer(ignored -> { phase.set(StorageSetupStateStore.Phase.COMPLETE); return binding; });
            when(states.guardedBy(any())).thenReturn(states);
            when(states.load()).thenAnswer(ignored -> new StorageSetupStateStore.Status(currentScope.get(), phase.get(), 1));
            when(states.rotate(any(), any())).thenAnswer(call -> {
                var expected = call.<StorageSetupScope>getArgument(0);
                if (!currentScope.get().equals(expected)) throw InstallationFiles.refused();
                var next = new StorageSetupScope(scope.clusterId(), scope.installationId(), scope.operationId(), expected.generation() + 1);
                currentScope.set(next); return new StorageSetupStateStore.Status(next, phase.get(), 1);
            });
            doAnswer(call -> {
                var expected = call.<StorageSetupScope>getArgument(0); var purpose = call.<StorageSetupStateStore.Purpose>getArgument(1);
                if (!currentScope.get().equals(expected) || (phase.get() == StorageSetupStateStore.Phase.COMPLETE
                        ? purpose != StorageSetupStateStore.Purpose.FIRST_ADMIN : purpose != StorageSetupStateStore.Purpose.SETUP))
                    throw InstallationFiles.refused();
                return null;
            }).when(states).requireCurrent(any(), any());
            var artifacts = mock(ArtifactStore.class); when(configuration.artifacts()).thenReturn(artifacts);
            var source = new SourceResource("views", null, "mongodb", Map.of("uri", "mongodb://target/master"), null, null, null, null);
            when(artifacts.get("views")).thenReturn(Optional.of(source));
            if (authGeneration > 1) {
                var commands = configuration.recoveryCommands();
                var completed = mock(StorageRecoveryCommands.Plan.class);
                var manifest = new StorageRecoveryCommands.Manifest(new StorageRecoveryCommands.Selection(
                        StorageRecoveryCommands.Extent.CLUSTER, Set.of()), Set.of(),
                        Map.of("views", io.tapstate.core.model.canonical.CanonicalHash.of(source)), "a".repeat(64), 1);
                when(completed.originalScope()).thenReturn(preparation.authScope()); when(completed.operationId()).thenReturn("recovery");
                when(completed.phase()).thenReturn(StorageRecoveryCommands.Phase.COMPLETE); when(completed.manifest()).thenReturn(manifest);
                when(commands.current(preparation.authScope())).thenReturn(Optional.of(completed));
                when(commands.find(preparation.authScope(), "recovery")).thenReturn(Optional.of(completed));
                when(configuration.initialOpRecoverySource(preparation.authScope(), true)).thenReturn(source);
            }
            when(target.identity(any())).thenReturn(new StorageSetupService.ConnectorIdentity("a".repeat(64), "b".repeat(64)));
            caches.when(() -> MongoCacheDomain.connect(any(), any(), anyString(), any(), any())).thenReturn(cache);
            var mongo = new MongoProperties(); mongo.setUri(null);
            root = new StorageRoot(mongo, CloudRuntimeSettings.resolve(new CloudProperties()), null, BootId.fresh(),
                    new MetricsHistoryProperties(), new RandomTokenSecrets(), Clock.systemUTC(), new byte[32], files::requireOwner);
            root.installOnPremConfiguration(configuration, CONFIG);
            root.runtime().loadBound().orElseThrow();
            when(business.available()).thenReturn(true); root.retainInitialAssembly(ignored -> business);
            if (automatic) {
                @SuppressWarnings("unchecked") ObjectProvider<StorageSetupService.DefaultTarget> targets = mock(ObjectProvider.class);
                when(targets.getObject()).thenReturn(target);
                var environment = new org.springframework.mock.env.MockEnvironment()
                        .withProperty("tapstate.installation.automatic-restart", "true")
                        .withProperty("tapstate.installation.require-credential", "true");
                installation = new OnPremInstallationConfiguration().onPremInstallation(root, files, mongo, targets, Clock.systemUTC(),
                        mock(org.springframework.context.ConfigurableApplicationContext.class), environment);
            } else {
                installation = new OnPremInstallation(root, files, mongo, target, Clock.systemUTC());
                installation.requireCredential(true); installation.onStartupCompleted(completions::incrementAndGet);
            }
            var connector = mock(org.apache.catalina.connector.Connector.class); when(connector.getLocalPort()).thenReturn(65123);
            installation.attach(connector);
            var attributes = new HashMap<String,Object>();
            when(request.getConnector()).thenReturn(connector);
            doAnswer(call -> { attributes.put(call.getArgument(0), call.getArgument(1)); return null; }).when(request).setAttribute(anyString(), any());
            when(request.getAttribute(anyString())).thenAnswer(call -> attributes.get(call.getArgument(0)));
            when(request.getDispatcherType()).thenReturn(jakarta.servlet.DispatcherType.REQUEST);
            when(request.getLocalAddr()).thenReturn("127.0.0.1"); when(request.getLocalPort()).thenReturn(65123);
            when(request.getHeader("Host")).thenReturn("127.0.0.1:65123");
            when(request.getMethod()).thenReturn("POST"); when(request.getRequestURI()).thenReturn("/api/storage/setup/activate");
            when(request.getHeader(OnPremInstallation.CAPABILITY_HEADER)).thenAnswer(ignored -> files.capability().orElseThrow().token());
            installation.markRequest(request);
            @SuppressWarnings("unchecked") ObjectProvider<OnPremInstallation> provider = mock(ObjectProvider.class);
            when(provider.getIfAvailable()).thenReturn(installation);
            entry = new StorageRootConfiguration().storageEntryStatus(root, provider);
        }
        <T> T management(Supplier<T> action) {
            RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
            try { return action.get(); } finally { RequestContextHolder.resetRequestAttributes(); }
        }
        void settle() { org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(3)).until(() -> !root.initialActivationPending()); }
        @Override public void close() {
            try { installation.close(); root.close(); } finally { caches.close(); RequestContextHolder.resetRequestAttributes(); }
        }
    }

    private static final class InlineExecutor extends java.util.concurrent.AbstractExecutorService {
        private boolean closed;
        @Override public void shutdown() { closed = true; }
        @Override public List<Runnable> shutdownNow() { closed = true; return List.of(); }
        @Override public boolean isShutdown() { return closed; }
        @Override public boolean isTerminated() { return closed; }
        @Override public boolean awaitTermination(long timeout, java.util.concurrent.TimeUnit unit) { return closed; }
        @Override public void execute(Runnable task) { if (closed) throw new java.util.concurrent.RejectedExecutionException(); task.run(); }
    }

    private static final class EntryBridge implements AutoCloseable {
        final com.sun.net.httpserver.HttpServer server;
        final List<String> observedEntries = new java.util.concurrent.CopyOnWriteArrayList<>();
        final List<String> observedProgress = new java.util.concurrent.CopyOnWriteArrayList<>();
        EntryBridge(Fixture fixture) throws Exception {
            when(fixture.request.getMethod()).thenReturn("GET");
            when(fixture.request.getRequestURI()).thenReturn("/auth/storage-entry");
            @SuppressWarnings("unchecked") ObjectProvider<StorageEntryStatus> states = mock(ObjectProvider.class);
            @SuppressWarnings("unchecked") ObjectProvider<io.tapstate.control.restapi.InstallationHttpAccess> installations = mock(ObjectProvider.class);
            when(states.getIfAvailable()).thenReturn(fixture.entry); when(installations.getIfAvailable()).thenReturn(fixture.installation);
            var type = Class.forName("io.tapstate.control.restapi.StorageEntryController");
            var constructor = type.getDeclaredConstructor(ObjectProvider.class, ObjectProvider.class); constructor.setAccessible(true);
            var controller = constructor.newInstance(states, installations);
            var method = type.getDeclaredMethod("entry", jakarta.servlet.http.HttpServletRequest.class); method.setAccessible(true);
            server = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/auth/storage-entry", exchange -> {
                try {
                    var response = (org.springframework.http.ResponseEntity<?>) method.invoke(controller, fixture.request);
                    var body = (Map<?,?>) response.getBody();
                    observedEntries.add(String.valueOf(body.get("entry")));
                    var progress = (Map<?,?>) body.get("initializationProgress");
                    observedProgress.add(progress == null ? "absent" : String.valueOf(progress.get("state")));
                    byte[] bytes = io.tapstate.core.common.JsonWriter.write(body).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, bytes.length); try (var output = exchange.getResponseBody()) { output.write(bytes); }
                } catch (ReflectiveOperationException failure) { throw new java.io.IOException(failure); }
            });
            server.start();
            fixture.files.saveStage(fixture.files.stage().withManagementUrl("http://127.0.0.1:" + server.getAddress().getPort()));
        }
        @Override public void close() { server.stop(0); }
    }

    private static final class AutomaticIssuer implements AutoCloseable {
        final EntryBridge bridge;
        final MongoConfigurationDomain issuer = mock(MongoConfigurationDomain.class);
        final MongoCacheDomain validation = mock(MongoCacheDomain.class);
        final MockedStatic<MongoConfigurationDomain> configurations = mockStatic(MongoConfigurationDomain.class);
        AutomaticIssuer(Fixture f) throws Exception {
            bridge = new EntryBridge(f);
            var firstAdmin = f.configuration.firstAdmin(); var recoveries = f.configuration.recoveryCommands();
            when(issuer.setupStates()).thenReturn(f.states); when(issuer.bindings()).thenReturn(f.bindings);
            when(issuer.firstAdmin()).thenReturn(firstAdmin); when(issuer.recoveryCommands()).thenReturn(recoveries);
            configurations.when(() -> MongoConfigurationDomain.openExisting(any())).thenReturn(issuer);
            f.caches.when(() -> MongoCacheDomain.connect(any(StorageBinding.Connection.class), any(java.time.Duration.class), eq("installation-authorization")))
                    .thenReturn(validation);
            org.springframework.test.util.ReflectionTestUtils.setField(f.root, "activationWorker", new InlineExecutor());
            org.springframework.test.util.ReflectionTestUtils.setField(f.root, "authenticationWorker", new InlineExecutor());
        }
        @Override public void close() { try { configurations.close(); } finally { bridge.close(); } }
    }

    @Test void strictAutomaticAuthorizationPassesThroughThePublishedEntryControllerGate() throws Exception {
        try (var f = new Fixture(directory.resolve("automatic-capability"), 1, true); var issuer = new AutomaticIssuer(f)) {
            f.installation.resumePreparation(); f.settle();
            assertThat(issuer.bridge.observedEntries).isNotEmpty().containsOnly("FIRST_ADMIN_REQUIRED");
            assertThat(issuer.bridge.observedProgress).containsOnly("COMPLETE");
            var issued = f.files.capability().orElseThrow();
            assertThat(issued.purpose()).isEqualTo(StorageSetupStateStore.Purpose.FIRST_ADMIN);
            assertThat(f.installation.firstAdministratorReady()).isTrue();
            assertThat(f.root.completedInstallationActivationAllowed()).isFalse();
            f.management(() -> { f.installation.caller(f.request).require(ControlOperations.STORAGE_STATUS); return null; });
            verify(issuer.issuer).close(); verify(issuer.validation).close(); verify(f.configuration, never()).close(); verify(f.cache, never()).close();
            verify(f.business).start(); verify(f.business, never()).afterInitialReady();
            verify(f.cache, never()).prepareFirstAuth(any()); verify(f.cache).prepareInitialBusiness(eq(f.preparation), any());
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void failedAutomaticDeliveryCanBeRetriedWithoutReplayingTheCompletedHandoff(boolean writtenBeforeFailure) throws Exception {
        try (var f = new Fixture(directory.resolve("delivery-" + writtenBeforeFailure), 1, true); var issuer = new AutomaticIssuer(f)) {
            var fail = new AtomicBoolean(true); var attempted = new java.util.concurrent.CopyOnWriteArrayList<StorageSetupStateStore.Purpose>();
            doAnswer(call -> {
                var issued = call.<SetupCapabilityService.Issued>getArgument(0); attempted.add(issued.purpose());
                if (issued.purpose() == StorageSetupStateStore.Purpose.FIRST_ADMIN && fail.compareAndSet(true, false)) {
                    if (writtenBeforeFailure) call.callRealMethod();
                    throw InstallationFiles.refused();
                }
                return call.callRealMethod();
            }).when(f.files).saveCapability(any());
            f.installation.resumePreparation(); f.settle();
            assertThat(f.installation.firstAdministratorReady()).isTrue();
            assertThat(f.installation.initializationProgress()).containsEntry("state", "COMPLETE");
            assertThat(f.root.completedInstallationActivationAllowed()).isFalse();
            var previous = f.files.capability(); var generation = f.currentScope.get().generation();
            assertThat(previous.isPresent()).isEqualTo(writtenBeforeFailure);
            f.root.localAuthentication();
            var delivered = f.files.capability().orElseThrow();
            assertThat(delivered.purpose()).isEqualTo(StorageSetupStateStore.Purpose.FIRST_ADMIN);
            if (writtenBeforeFailure) {
                assertThat(delivered).isEqualTo(previous.orElseThrow());
                assertThat(f.currentScope.get().generation()).isEqualTo(generation);
                assertThat(attempted).hasSize(1);
            } else {
                assertThat(f.currentScope.get().generation()).isEqualTo(generation + 1);
                assertThat(attempted).hasSize(2);
            }
            var completedScope = f.files.stage().scope();
            f.root.localAuthentication();
            assertThat(f.files.capability()).contains(delivered); assertThat(f.files.stage().scope()).isEqualTo(completedScope);
            assertThat(attempted).containsOnly(StorageSetupStateStore.Purpose.FIRST_ADMIN);
            assertThat(issuer.bridge.observedEntries).containsOnly("FIRST_ADMIN_REQUIRED");
            assertThat(issuer.bridge.observedProgress).containsOnly("COMPLETE");
            verify(f.business).start(); verify(f.business, never()).afterInitialReady();
            verify(f.cache, never()).prepareFirstAuth(any()); verify(f.cache).resumeInitialAuth(eq(f.preparation), any());
            verify(f.cache).prepareInitialBusiness(eq(f.preparation), any()); verify(f.target, never()).test(any(), any(), any());
        }
    }

    @Test void automaticDeliveryPreservesAnExpiredIndependentCapabilityForExplicitOwnerRenewal() throws Exception {
        try (var f = new Fixture(directory.resolve("expired-delivery"), 1, true); var issuer = new AutomaticIssuer(f)) {
            var expired = new SetupCapabilityService.Issued(f.scope, StorageSetupStateStore.Purpose.FIRST_ADMIN,
                    Clock.systemUTC().instant().minusSeconds(1), "d".repeat(43));
            f.files.saveCapability(expired);
            f.installation.resumePreparation(); f.settle();
            assertThat(f.installation.firstAdministratorReady()).isTrue();
            assertThat(f.installation.initializationProgress()).containsEntry("state", "COMPLETE");
            assertThat(f.files.capability()).contains(expired); assertThat(f.currentScope.get()).isEqualTo(f.scope);
            f.root.localAuthentication();
            assertThat(f.files.capability()).contains(expired); assertThat(f.currentScope.get()).isEqualTo(f.scope);
            var renewed = InstallationCommand.authorize(f.files, new MongoProperties(), Clock.systemUTC());
            assertThat(renewed.purpose()).isEqualTo(StorageSetupStateStore.Purpose.FIRST_ADMIN);
            assertThat(renewed.scope().generation()).isEqualTo(f.scope.generation() + 1);
            assertThat(renewed.token()).isNotEqualTo(expired.token());
            f.root.localAuthentication();
            assertThat(f.files.capability()).contains(renewed); assertThat(f.currentScope.get()).isEqualTo(renewed.scope());
            verify(f.business).start(); verify(f.cache).prepareInitialBusiness(eq(f.preparation), any());
        }
    }

    @ParameterizedTest @ValueSource(strings = {"stage", "capability"})
    void explicitActivationRetriesOnlyTheUnfinishedLocalHandoff(String fault) {
        try (var f = new Fixture(directory.resolve(fault))) {
            doAnswer(ignored -> {
                if (fault.equals("stage")) doThrow(InstallationFiles.refused()).doCallRealMethod().when(f.files).saveStage(any());
                else doThrow(InstallationFiles.refused()).doCallRealMethod().when(f.files).removeCapability();
                return null;
            }).when(f.business).start();
            f.installation.resumePreparation(); f.settle();
            assertThat(f.root.runtime().businessStarted()).isTrue();
            assertThat(f.installation.firstAdministratorReady()).isFalse();
            assertThat(f.entry.read().entry()).isEqualTo(StorageEntryStatus.Entry.UNAVAILABLE);
            assertThat(f.root.completedInstallationActivationAllowed()).as("READY business must not retire unfinished installation authority").isTrue();
            var originalCaller = f.management(() -> {
                var caller = f.installation.caller(f.request);
                assertThat(f.installation.snapshot(caller, caller.guard(ControlOperations.STORAGE_STATUS)).businessPhase())
                        .isEqualTo(StorageControlDispatcher.BusinessPhase.AUTH_READY);
                caller.require(ControlOperations.STORAGE_STATUS); caller.require(ControlOperations.STORAGE_SETUP_ACTIVATE);
                assertThatThrownBy(() -> caller.require(ControlOperations.STORAGE_TEST)).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> caller.require(ControlOperations.STORAGE_SETUP_COMMIT)).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> caller.require(ControlOperations.STORAGE_RECOVERY_PLAN)).isInstanceOf(TapstateException.class);
                f.installation.setups().activate(caller.subject(), caller.setup(), caller.guard(ControlOperations.STORAGE_SETUP_ACTIVATE), 1);
                return caller;
            });
            f.settle();
            assertThat(f.installation.firstAdministratorReady()).isTrue();
            assertThat(f.entry.read().entry()).isEqualTo(StorageEntryStatus.Entry.FIRST_ADMIN_REQUIRED);
            assertThat(f.files.capability()).isEmpty(); assertThat(f.completions).hasValue(1);
            f.management(() -> {
                assertThatThrownBy(() -> originalCaller.require(ControlOperations.STORAGE_STATUS)).isInstanceOf(TapstateException.class);
                return null;
            });
            verify(f.business).start(); verify(f.business, never()).afterInitialReady();
            verify(f.cache, never()).prepareFirstAuth(any()); verify(f.cache).resumeInitialAuth(eq(f.preparation), any());
            verify(f.cache).prepareInitialBusiness(eq(f.preparation), any()); verify(f.target, never()).test(any(), any(), any());
        }
    }

    @Test void authenticationContinuationAlsoFinishesAnAlreadyPublishedBusinessOwner() {
        try (var f = new Fixture(directory.resolve("published-authentication"))) {
            doAnswer(ignored -> {
                doThrow(InstallationFiles.refused()).doCallRealMethod().when(f.files).removeCapability(); return null;
            }).when(f.business).start();
            f.installation.resumePreparation(); f.settle();
            assertThat(f.root.runtime().businessStarted()).isTrue();
            assertThat(f.installation.firstAdministratorReady()).isFalse();
            f.root.localAuthentication();
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(3)).until(f.installation::firstAdministratorReady);
            assertThat(f.entry.read().entry()).isEqualTo(StorageEntryStatus.Entry.FIRST_ADMIN_REQUIRED);
            assertThat(f.files.capability()).isEmpty(); assertThat(f.completions).hasValue(1);
            verify(f.business).start(); verify(f.business, never()).afterInitialReady();
            verify(f.cache, never()).prepareFirstAuth(any()); verify(f.cache).prepareInitialBusiness(eq(f.preparation), any());
        }
    }

    @Test void aCompletedHandoffPreservesTheIndependentFirstAdministratorCapability() {
        try (var f = new Fixture(directory.resolve("idempotent"))) {
            f.installation.resumePreparation(); f.settle();
            assertThat(f.installation.firstAdministratorReady()).isTrue();
            var firstAdmin = new SetupCapabilityService.Issued(f.scope, StorageSetupStateStore.Purpose.FIRST_ADMIN,
                    Clock.systemUTC().instant().plusSeconds(300), "b".repeat(43));
            f.files.saveCapability(firstAdmin);
            f.installation.afterNormalStartup();
            f.installation.setups().activate("owner", f.scope, () -> {}, 1); f.settle();
            assertThat(f.files.capability()).contains(firstAdmin); assertThat(f.completions).hasValue(1);
            f.management(() -> {
                var caller = f.installation.caller(f.request);
                caller.require(ControlOperations.STORAGE_STATUS);
                assertThatThrownBy(() -> caller.require(ControlOperations.STORAGE_SETUP_ACTIVATE)).isInstanceOf(TapstateException.class);
                return null;
            });
            verify(f.business).start(); verify(f.cache).prepareInitialBusiness(eq(f.preparation), any());
        }
    }

    @Test void aChangedPersistedPreparationCannotFinishTheOldLocalInstallation() {
        try (var f = new Fixture(directory.resolve("fenced"))) {
            doAnswer(ignored -> {
                doThrow(InstallationFiles.refused()).doCallRealMethod().when(f.files).saveStage(any()); return null;
            }).when(f.business).start();
            f.installation.resumePreparation(); f.settle();
            var changed = new CachePreparation(f.scope.operationId(), new CacheAuthScope(f.scope.clusterId(), 1,
                    StorageBinding.Mode.OP, "another-cache", 1));
            when(f.bindings.cachePreparation()).thenReturn(Optional.of(changed));
            assertThatThrownBy(f.root::completedInstallationActivationAllowed).isInstanceOf(TapstateException.class);
            assertThat(f.installation.firstAdministratorReady()).isFalse(); assertThat(f.completions).hasValue(0);
            assertThat(f.files.capability()).isPresent(); verify(f.business).start();
        }
    }

    @Test void retryingAFirstCreationHandoffDoesNotRepeatItsSourceTestOrCreationGrant() {
        try (var f = new Fixture(directory.resolve("first-creation"))) {
            doAnswer(ignored -> {
                doThrow(InstallationFiles.refused()).doCallRealMethod().when(f.files).removeCapability(); return null;
            }).when(f.business).start();
            var grant = mock(FirstCacheCreation.class); when(grant.preparation()).thenReturn(f.preparation);
            var committed = new StorageBindingCommit(f.binding, f.preparation, Optional.of(grant));
            f.root.handoffActivation(committed, () -> f.root.continueOnPremSetup(committed, f.target)); f.settle();
            assertThat(f.root.runtime().businessStarted()).isTrue(); assertThat(f.installation.firstAdministratorReady()).isFalse();
            f.management(() -> {
                var caller = f.installation.caller(f.request);
                f.installation.setups().activate(caller.subject(), caller.setup(), caller.guard(ControlOperations.STORAGE_SETUP_ACTIVATE), 1);
                return null;
            }); f.settle();
            assertThat(f.installation.firstAdministratorReady()).isTrue(); assertThat(f.completions).hasValue(1);
            verify(f.cache).prepareFirstAuth(grant); verify(f.cache).prepareInitialBusiness(eq(f.preparation), any());
            verify(f.cache, never()).resumeInitialAuth(any(), any()); verify(f.business).start(); verify(f.business).afterInitialReady();
            verify(f.target, never()).test(any(), any(), any());
        }
    }

    @Test void aTemporarilyUnavailablePublishedOwnerIsNotReassembledByTheInstaller() {
        try (var f = new Fixture(directory.resolve("unavailable-owner"))) {
            doAnswer(ignored -> {
                doThrow(InstallationFiles.refused()).doCallRealMethod().when(f.files).removeCapability(); return null;
            }).when(f.business).start();
            f.installation.resumePreparation(); f.settle();
            when(f.business.available()).thenReturn(false);
            f.management(() -> {
                var caller = f.installation.caller(f.request);
                f.installation.setups().activate(caller.subject(), caller.setup(), caller.guard(ControlOperations.STORAGE_SETUP_ACTIVATE), 1);
                return null;
            }); f.settle();
            assertThat(f.root.phase()).isEqualTo(StorageRoot.Phase.BUSINESS_READY);
            assertThat(f.installation.firstAdministratorReady()).isFalse(); assertThat(f.completions).hasValue(0);
            when(f.business.available()).thenReturn(true); f.root.localAuthentication();
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(3)).until(f.installation::firstAdministratorReady);
            verify(f.business).start(); verify(f.cache).resumeInitialAuth(eq(f.preparation), any());
            verify(f.cache).prepareInitialBusiness(eq(f.preparation), any()); verify(f.cache, never()).prepareFirstAuth(any());
        }
    }

    @Test void authenticationRecoveryRestoresReadyWithoutRepeatingACompletedInstallation() {
        try (var f = new Fixture(directory.resolve("completed-owner"))) {
            f.installation.resumePreparation(); f.settle();
            assertThat(f.installation.firstAdministratorReady()).isTrue();
            var firstAdmin = new SetupCapabilityService.Issued(f.scope, StorageSetupStateStore.Purpose.FIRST_ADMIN,
                    Clock.systemUTC().instant().plusSeconds(300), "c".repeat(43));
            f.files.saveCapability(firstAdmin); when(f.business.available()).thenReturn(false);
            var candidate = mock(MongoCacheDomain.class);
            f.caches.when(() -> MongoCacheDomain.connect(any(), any(), anyString(), any(), any())).thenReturn(candidate);
            // Static client boundaries are thread-local; the production continuation runs unchanged.
            org.springframework.test.util.ReflectionTestUtils.setField(f.root, "authenticationWorker", new InlineExecutor());
            doThrow(StorageRuntime.unavailable()).doThrow(StorageRuntime.unavailable()).doNothing().when(f.configuration)
                    .requireAuthenticationPublished(eq(f.preparation), any(MongoReadBudget.class));
            f.root.localAuthentication();
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(3)).until(() -> f.root.phase() == StorageRoot.Phase.AUTH_READY);
            assertThat(f.installation.firstAdministratorReady()).isFalse();
            when(f.business.available()).thenReturn(true); f.root.localAuthentication();
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(3)).untilAsserted(() -> {
                assertThat(f.root.phase()).isEqualTo(StorageRoot.Phase.BUSINESS_READY);
                assertThat(f.installation.firstAdministratorReady()).isTrue();
            });
            assertThat(f.files.capability()).contains(firstAdmin); assertThat(f.files.stage().scope()).isEqualTo(f.scope);
            assertThat(f.completions).hasValue(1); verify(f.business).start();
            verify(candidate).close();
            verify(f.cache).prepareInitialBusiness(eq(f.preparation), any()); verify(f.business, never()).afterInitialReady();
        }
    }

    @Test void theOwnerCanRenewOnlyLimitedSetupAuthorityToFinishARecoveredReadyInstallation() throws Exception {
        try (var f = new Fixture(directory.resolve("recovered"), 2)) {
            f.phase.set(StorageSetupStateStore.Phase.COMPLETE);
            var blocked = new AtomicBoolean(true);
            doAnswer(ignored -> {
                doAnswer(call -> { if (blocked.get()) throw InstallationFiles.refused(); return call.callRealMethod(); })
                        .when(f.files).saveStage(any());
                return null;
            }).when(f.business).start();
            f.root.loadExisting(ignored -> f.business, () -> f.target);
            assertThat(f.root.runtime().businessStarted()).isTrue(); assertThat(f.installation.firstAdministratorReady()).isFalse();
            blocked.set(false);
            var server = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/auth/storage-entry", exchange -> {
                var entry = f.entry.read();
                byte[] body = io.tapstate.core.common.JsonWriter.write(Map.of("mode", entry.mode(), "entry", entry.entry().name()))
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, body.length); try (var output = exchange.getResponseBody()) { output.write(body); }
            }); server.start();
            try (var configurations = mockStatic(MongoConfigurationDomain.class)) {
                configurations.when(() -> MongoConfigurationDomain.openExisting(any())).thenReturn(f.configuration);
                f.files.saveStage(f.files.stage().withManagementUrl("http://127.0.0.1:" + server.getAddress().getPort()));
                var issued = InstallationCommand.authorize(f.files, new MongoProperties(), Clock.systemUTC());
                assertThat(issued.purpose()).isEqualTo(StorageSetupStateStore.Purpose.SETUP);
                assertThat(issued.scope().generation()).isEqualTo(2);
                f.management(() -> {
                    var caller = f.installation.caller(f.request);
                    caller.require(ControlOperations.STORAGE_SETUP_ACTIVATE);
                    assertThatThrownBy(() -> caller.require(ControlOperations.STORAGE_SETUP_COMMIT)).isInstanceOf(TapstateException.class);
                    assertThatThrownBy(() -> caller.require(ControlOperations.STORAGE_RECOVERY_CONFIRM)).isInstanceOf(TapstateException.class);
                    f.installation.setups().activate(caller.subject(), caller.setup(), caller.guard(ControlOperations.STORAGE_SETUP_ACTIVATE), 1);
                    return null;
                });
                f.settle();
                assertThat(f.installation.firstAdministratorReady()).isTrue(); assertThat(f.completions).hasValue(1);
                assertThat(f.bindings.load()).contains(f.binding); assertThat(f.bindings.cachePreparation()).contains(f.preparation);
                verify(f.cache, never()).prepareFirstAuth(any()); verify(f.cache, never()).resumeInitialAuth(any(), any());
                verify(f.cache, never()).prepareInitialBusiness(any(), any()); verify(f.business).start();
            } finally { server.stop(0); }
        }
    }

    @Test void authenticationContinuationCompletesTheOriginalInstallationAfterAssemblyRecovers() {
        try (var f = new Fixture(directory.resolve("authentication"))) {
            f.phase.set(StorageSetupStateStore.Phase.COMPLETE);
            var broken = mock(StorageRuntime.Business.class); doThrow(StorageRuntime.unavailable()).when(broken).start();
            f.root.loadExisting(ignored -> broken);
            assertThat(f.root.phase()).isEqualTo(StorageRoot.Phase.AUTH_READY);
            f.root.retainInitialAssembly(ignored -> f.business);
            f.root.localAuthentication();
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(3)).untilAsserted(() -> {
                assertThat(f.root.runtime().businessStarted()).isTrue();
                assertThat(f.installation.firstAdministratorReady()).isTrue();
            });
            assertThat(f.entry.read().entry()).isEqualTo(StorageEntryStatus.Entry.FIRST_ADMIN_REQUIRED);
            assertThat(f.files.capability()).isEmpty(); assertThat(f.completions).hasValue(1);
            verify(broken).close(); verify(f.business).start(); verify(f.business, never()).afterInitialReady();
            verify(f.cache, never()).prepareFirstAuth(any()); verify(f.cache, never()).prepareInitialBusiness(any(), any());
        }
    }
}
