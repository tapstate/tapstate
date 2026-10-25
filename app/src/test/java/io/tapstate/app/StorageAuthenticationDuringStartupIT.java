package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.control.core.CredentialAuthentication;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.StorageControlAccess;
import io.tapstate.control.core.StorageControlDispatcher;
import io.tapstate.control.core.TapstatePrincipal;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Clock;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** Formal authentication must not wait for the business owner's startup monitor. */
@RequiresDocker
class StorageAuthenticationDuringStartupIT {
    @ParameterizedTest
    @EnumSource(StorageBinding.Mode.class)
    void theOriginalRecoveryResultIsReadableWhileItsBusinessOwnerIsStarting(StorageBinding.Mode mode) throws Exception {
        try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            mongo.start();
            var stored = NativeAppStorage.create(mongo, "recovery_during_start", mode, "cluster-" + UUID.randomUUID());
            try (var client = com.mongodb.client.MongoClients.create(stored.cacheUri())) {
                client.getDatabase(stored.cacheDatabase()).drop();
            }
            var mongoProperties = new MongoProperties(); mongoProperties.setUri(stored.configurationUri());
            var properties = new CloudProperties();
            if (mode == StorageBinding.Mode.CLOUD) {
                properties.setBaseUrl("https://cloud.example"); properties.setClusterId(stored.clusterId());
                properties.setAtlasUri(stored.configurationUri()); properties.setToken("fixture-runtime-token");
            }
            var cloud = CloudRuntimeSettings.resolve(properties); var clock = Clock.systemUTC();
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            var scheduled = new java.util.concurrent.atomic.AtomicBoolean();
            var executor = Executors.newFixedThreadPool(2);
            try (var root = new StorageRoot(mongoProperties, cloud,
                    mode == StorageBinding.Mode.CLOUD ? new CloudSdkBridge(cloud, new Provider(stored.clusterId(), clock)) : null,
                    BootId.fresh(), new MetricsHistoryProperties(), new RandomTokenSecrets(), clock, new byte[32])) {
                try {
                    root.loadExisting(ignored -> new StorageRuntime.Business() {
                        @Override public void start() {
                            entered.countDown();
                            try { if (!release.await(20, TimeUnit.SECONDS)) throw new AssertionError("recovery gate was not released"); }
                            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                        }
                        @Override public void close() {}
                    }, () -> org.mockito.Mockito.mock(io.tapstate.control.core.StorageSetupService.DefaultTarget.class));
                    String credential = mode == StorageBinding.Mode.CLOUD
                            ? root.managedAuthentication().service().exchangeCode("fresh", "cluster.example").token()
                            : root.localAuthentication().login().login(NativeAppStorage.USERNAME, NativeAppStorage.PASSWORD);
                    var proof = root.authentication().credentials().authenticateContext(credential).orElseThrow();
                    var scope = proof.storageScope().orElseThrow();
                    var service = root.recoveryService(clock);
                    var planned = service.plan("fixture", scope, proof.requireActive(), new io.tapstate.spi.store.StorageRecoveryCommands.Selection(
                            io.tapstate.spi.store.StorageRecoveryCommands.Extent.CLUSTER, java.util.Set.of()), null, null);
                    var monitorEntered = new CountDownLatch(1); var monitorRelease = new CountDownLatch(1);
                    var holding = executor.submit(() -> {
                        synchronized (root) {
                            monitorEntered.countDown();
                            try { if (!monitorRelease.await(20, TimeUnit.SECONDS)) throw new AssertionError("monitor gate was not released"); }
                            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                        }
                    });
                    try {
                        assertThat(monitorEntered.await(10, TimeUnit.SECONDS)).isTrue();
                        var observedService = executor.submit(() -> root.recoveryService(clock)).get(2, TimeUnit.SECONDS);
                        assertThat(observedService.status(scope, proof.requireActive(), planned.operationId()).orElseThrow()
                                .operationId()).isEqualTo(planned.operationId());
                        assertThat(executor.submit(() -> service.presentation(planned)).get(2, TimeUnit.SECONDS).current()).isTrue();
                    } finally {
                        monitorRelease.countDown(); holding.get(10, TimeUnit.SECONDS);
                    }
                    service.confirm("fixture", scope, proof.requireActive(), planned.operationId(), planned.planHash(), planned.controlRevision(), true);
                    scheduled.set(true);
                    assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                    var current = executor.submit(() -> {
                        var result = service.status(scope, proof.requireActive(), planned.operationId()).orElseThrow();
                        assertThat(result.phase()).isEqualTo(io.tapstate.spi.store.StorageRecoveryCommands.Phase.RUNNING);
                        assertThat(service.presentation(result).current()).isTrue();
                        return result;
                    }).get(2, TimeUnit.SECONDS);
                    assertThat(current.operationId()).isEqualTo(planned.operationId());
                    assertThat(release.getCount()).isEqualTo(1);
                    if (mode == StorageBinding.Mode.CLOUD) {
                        var status = new StagedCloudRuntimeConfiguration().cloudRuntimeStatusProvider(cloud, root, clock);
                        executor.submit(() -> assertThatThrownBy(status::snapshot).isInstanceOfSatisfying(TapstateException.class,
                                failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE)))
                                .get(2, TimeUnit.SECONDS);
                    }
                } finally {
                    release.countDown();
                    if (scheduled.get()) org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10)).untilAsserted(() ->
                            assertThat(root.runtime().businessStarted()).isTrue());
                }
            } finally {
                executor.shutdown(); assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(StorageBinding.Mode.class)
    void originalFormalLoginFinishesWhileBusinessStartupRemainsBlocked(StorageBinding.Mode mode) throws Exception {
        duringStartup(mode, false, false);
    }

    @ParameterizedTest
    @EnumSource(StorageBinding.Mode.class)
    void aMissingAuthenticationOwnerFailsWithinItsBudgetWithoutJoiningBusinessStartup(StorageBinding.Mode mode) throws Exception {
        duringStartup(mode, true, false);
    }

    @ParameterizedTest
    @EnumSource(StorageBinding.Mode.class)
    void theOriginalStorageRecoversAuthenticationWithoutReplacingItsBusinessOwner(StorageBinding.Mode mode) throws Exception {
        duringStartup(mode, true, true);
    }

    private static void duringStartup(StorageBinding.Mode mode, boolean invalidate, boolean recover) throws Exception {
        try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            mongo.start();
            var stored = NativeAppStorage.create(mongo, "auth_during_start", mode, "cluster-" + UUID.randomUUID());
            var mongoProperties = new MongoProperties(); mongoProperties.setUri(stored.configurationUri());
            var properties = new CloudProperties();
            if (mode == StorageBinding.Mode.CLOUD) {
                properties.setBaseUrl("https://cloud.example"); properties.setClusterId(stored.clusterId());
                properties.setAtlasUri(stored.configurationUri()); properties.setToken("fixture-runtime-token");
            }
            var cloud = CloudRuntimeSettings.resolve(properties);
            var clock = Clock.systemUTC();
            var root = new StorageRoot(mongoProperties, cloud,
                    mode == StorageBinding.Mode.CLOUD ? new CloudSdkBridge(cloud, new Provider(stored.clusterId(), clock)) : null,
                    BootId.fresh(), new MetricsHistoryProperties(), new RandomTokenSecrets(), clock, new byte[32]);
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(recover ? 0 : 1);
            var starts = new AtomicInteger();
            var closes = new AtomicInteger();
            var executor = Executors.newFixedThreadPool(2);
            var startup = executor.submit(() -> root.loadExisting(ignored -> new StorageRuntime.Business() {
                @Override public void start() {
                    starts.incrementAndGet(); entered.countDown();
                    try {
                        if (!release.await(20, TimeUnit.SECONDS)) throw new AssertionError("startup gate was not released");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt(); throw new AssertionError(interrupted);
                    }
                }
                @Override public void close() { closes.incrementAndGet(); }
            }));
            try {
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                if (recover) startup.get(10, TimeUnit.SECONDS);
                if (invalidate) root.authentication().invalidateFormal();
                var login = executor.submit(() -> {
                    String credential = mode == StorageBinding.Mode.CLOUD
                            ? root.managedAuthentication().service().exchangeCode("fresh", "cluster.example").token()
                            : root.localAuthentication().login().login(NativeAppStorage.USERNAME, NativeAppStorage.PASSWORD);
                    return root.authentication().credentials().authenticateContext(credential).orElseThrow();
                });
                if (invalidate && !recover) {
                    assertThatThrownBy(() -> login.get(12, TimeUnit.SECONDS))
                            .isInstanceOf(java.util.concurrent.ExecutionException.class)
                            .cause().isInstanceOfSatisfying(TapstateException.class,
                                    failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE));
                    assertThat(release.getCount()).isEqualTo(1);
                    assertThat(startup.isDone()).isFalse();
                    assertThat(starts).hasValue(1);
                    assertThat(closes).hasValue(0);
                } else {
                    CredentialAuthentication proof = login.get(2, TimeUnit.SECONDS);
                    assertThat(release.getCount()).isEqualTo(recover ? 0 : 1);
                    assertThat(startup.isDone()).isEqualTo(recover);
                    assertThat(proof.storageScope()).contains(stored.preparation().authScope());
                    if (mode == StorageBinding.Mode.CLOUD) assertThat(proof.organizationId()).contains("organization");
                    proof.requireActive().run();
                    assertThat(starts).hasValue(1);
                    assertThat(closes).hasValue(0);
                    var principal = mode == StorageBinding.Mode.CLOUD ? TapstatePrincipal.cloudSession(proof.token())
                            : TapstatePrincipal.humanJwt(proof.token());
                    var caller = StorageControlAccess.ordinary(mode,
                            () -> root.runtime().requireConfiguration().setupStates().load().scope(), principal, proof);
                    var status = executor.submit(() -> root.snapshot(caller, proof.requireActive())).get(2, TimeUnit.SECONDS);
                    assertThat(status.businessPhase()).isEqualTo(recover ? StorageControlDispatcher.BusinessPhase.READY
                            : StorageControlDispatcher.BusinessPhase.AUTH_READY);
                    assertThat(status.authGeneration()).isEqualTo(stored.preparation().authScope().authGeneration());
                    assertThat(release.getCount()).isEqualTo(recover ? 0 : 1);
                    var closing = executor.submit(root::close);
                    org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(2)).untilAsserted(() ->
                            assertThatThrownBy(proof.requireActive()::run).isInstanceOfSatisfying(TapstateException.class,
                                    failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE)));
                    if (recover) closing.get(10, TimeUnit.SECONDS);
                    else {
                        assertThat(closing.isDone()).isFalse();
                        assertThat(startup.isDone()).isFalse();
                    }
                }
            } finally {
                release.countDown();
                try { startup.get(10, TimeUnit.SECONDS); }
                finally {
                    executor.shutdown();
                    try { assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue(); }
                    finally { root.close(); }
                }
            }
            assertThat(closes).hasValue(1);
        }
    }

    private record Provider(String cluster, Clock clock) implements CloudSdkBridge.SdkRuntime {
        @Override public CloudSdkBridge.ExchangeResult exchange(String code) {
            return new CloudSdkBridge.ExchangeResult("fixture-jwt", "organization", cluster, "Org", "Cluster", "local");
        }
        @Override public CloudSdkBridge.VerifiedClaims verify(String jwt) {
            return new CloudSdkBridge.VerifiedClaims("user", "organization", cluster, "jti", clock.instant().plusSeconds(600),
                    "https://cloud.example", "cluster.example", List.of("workload:write"));
        }
        @Override public io.tapstate.cloud.sdk.client.ClusterEgressAddress currentEgressAddress() {
            return new io.tapstate.cloud.sdk.client.ClusterEgressAddress("organization", cluster, null, "PROVISIONING");
        }
        @Override public boolean verifyCallback(String method, String time, String nonce, String data, String signature) { return false; }
        @Override public void report(String cluster, String nonce, io.tapstate.control.core.CloudRuntimeStatus status, String token) {
            throw new AssertionError("no reporting in this authentication witness");
        }
    }
}
