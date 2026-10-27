package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class StorageAuthenticationTest {
    @Test void formalMachineEvidenceRetainsTheVerifiedRecordAndHasAPureLocalGuard() {
        withOpAuthentication(f -> {
            var record = new TokenRecord("machine", "ADMIN", f.secrets.hash("test-secret"), false, f.clock.instant());
            when(f.tokens.find("machine")).thenReturn(Optional.of(record));
            var proof = f.authentication.credentials().authenticateContext("cyxt_machine.test-secret").orElseThrow();
            var evidence = proof.evidence().orElseThrow();
            assertThat(evidence.type()).isEqualTo(CredentialType.MACHINE_TOKEN);
            assertThat(evidence.configurationTokenBinding()).contains(record.authenticationBinding());
            clearInvocations(f.configuration, f.cache, f.tokens);
            evidence.requireLocallyActive().run();
            verifyNoInteractions(f.configuration, f.cache, f.tokens);
            f.authentication.invalidateFormal();
            assertThatThrownBy(evidence.requireLocallyActive()::run).isInstanceOf(TapstateException.class);
        });
    }

    @Test void formalHumanEvidenceKeepsSignedExpiryWithoutConsultingAConfigTokenOrCacheRow() {
        withOpAuthentication(f -> {
            String jwt = new HmacTokenSigner(new byte[32], HmacTokenSigner.DEFAULT_TTL, f.clock, f.scope).issue("machine-looking-username", Scope.ADMIN);
            var proof = f.authentication.credentials().authenticateContext(jwt).orElseThrow();
            var evidence = proof.evidence().orElseThrow();
            assertThat(evidence.type()).isEqualTo(CredentialType.HUMAN_JWT);
            assertThat(evidence.configurationTokenBinding()).isEmpty();
            clearInvocations(f.configuration, f.cache, f.tokens);
            evidence.requireLocallyActive().run();
            verifyNoInteractions(f.configuration, f.cache, f.tokens);
            f.now.set(f.clock.instant().plusSeconds(16 * 60));
            assertThatThrownBy(evidence.requireLocallyActive()::run).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ControlError.UNAUTHENTICATED));
            verifyNoInteractions(f.configuration, f.cache, f.tokens);
        });
    }
    @Test void simultaneousCloudRequestsValidateOneFrozenCohort() throws Exception {
        var properties = new CloudProperties();
        properties.setBaseUrl("https://cloud.example"); properties.setClusterId("cluster");
        properties.setAtlasUri("mongodb://localhost/config"); properties.setToken("test-runtime-token");
        var clock = Clock.fixed(Instant.parse("2026-10-26T09:00:00Z"), ZoneOffset.UTC);
        var configuration = mock(MongoConfigurationDomain.class);
        var states = mock(StorageSetupStateStore.class);
        when(configuration.setupStates()).thenReturn(states);
        when(states.load()).thenReturn(new StorageSetupStateStore.Status(
                new StorageSetupScope("cluster", "installation", "operation", 1), StorageSetupStateStore.Phase.COMPLETE, 1));
        var cache = mock(io.tapstate.adapters.mongostore.MongoCacheDomain.class);
        var bound = mock(StorageRuntime.Bound.class);
        var scope = new CacheAuthScope("cluster", 1, StorageBinding.Mode.CLOUD, "incarnation", 1);
        var preparation = new CachePreparation("operation", scope);
        when(bound.preparation()).thenReturn(preparation); when(bound.cache()).thenReturn(cache);
        var sessions = mock(CloudSessionStore.class);
        when(cache.cloudSessions(scope)).thenReturn(sessions);
        when(cache.cloudSessions(eq(scope), any(io.tapstate.adapters.mongostore.MongoReadBudget.class))).thenReturn(sessions);
        var identity = new CloudSessionIdentity("https://cloud.example", CloudSdkBridge.DEPLOYMENT_ORGANIZATION, "cluster");
        var context = new CloudSessionContext("organization", "cluster", "Org", "Cluster", "region");
        when(sessions.authenticate(eq(identity), eq("jti"), anyString(), any(), any()))
                .thenReturn(Optional.of(new CloudSessionRecord(identity, "jti", "hash", "user", "WRITE", false,
                        clock.instant(), clock.instant(), clock.instant().plusSeconds(600), context)));
        try (var authentication = new StorageAuthentication(configuration, CloudRuntimeSettings.resolve(properties),
                mock(CloudSdkBridge.class), new RandomTokenSecrets(), clock, new byte[32],
                new PendingAuthenticationCohorts<>(Duration.ofSeconds(30), 4, 6));
                var executor = Executors.newFixedThreadPool(6)) {
            authentication.install(bound);
            String cookie = "tcs_" + Base64.getUrlEncoder().withoutPadding().encodeToString(
                    "jti".getBytes(java.nio.charset.StandardCharsets.UTF_8)) + ".secret";
            var ready = new CountDownLatch(6); var start = new CountDownLatch(1);
            var requests = new ArrayList<Future<CredentialAuthentication>>();
            for (int i = 0; i < 6; i++) requests.add(executor.submit(() -> {
                ready.countDown(); assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
                return authentication.credentials().authenticateContext(cookie).orElseThrow();
            }));
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue(); start.countDown();
            for (var request : requests) assertThat(request.get(5, TimeUnit.SECONDS).organizationId()).contains("organization");
            verify(sessions, times(1)).authenticate(eq(identity), eq("jti"), anyString(), any(), any());
            var checks = new ArrayList<Future<?>>();
            for (var request : requests) checks.add(executor.submit(() -> { request.get().requireActive().run(); return null; }));
            for (var check : checks) check.get(5, TimeUnit.SECONDS);
            verify(sessions, times(2)).authenticate(eq(identity), eq("jti"), anyString(), any(), any());
            var mixed = new ArrayList<Future<?>>();
            for (int i = 0; i < 3; i++) mixed.add(executor.submit(() -> authentication.credentials().authenticateContext(cookie).orElseThrow()));
            for (int i = 0; i < 3; i++) mixed.add(executor.submit(() -> { requests.getFirst().get().requireActive().run(); return null; }));
            for (var check : mixed) check.get(5, TimeUnit.SECONDS);
            verify(sessions, times(3)).authenticate(eq(identity), eq("jti"), anyString(), any(), any());
            when(sessions.authenticate(eq(identity), eq("jti"), anyString(), any(), any()))
                    .thenReturn(Optional.of(new CloudSessionRecord(identity, "jti", "hash", "different-user", "WRITE", false,
                            clock.instant(), clock.instant(), clock.instant().plusSeconds(600), context)));
            var changed = new ArrayList<Future<?>>();
            for (var request : requests) changed.add(executor.submit(() -> { request.get().requireActive().run(); return null; }));
            for (var check : changed) assertThatThrownBy(() -> check.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class).cause().isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ControlError.UNAUTHENTICATED));
            verify(sessions, times(4)).authenticate(eq(identity), eq("jti"), anyString(), any(), any());
            // A subsequent request must observe durable revocation, even for the identical cookie.
            when(sessions.authenticate(eq(identity), eq("jti"), anyString(), any(), any())).thenReturn(Optional.empty());
            // Fill a new cohort without relying on scheduler timing or a completed result cache.
            var revoked = new ArrayList<Future<java.util.Optional<CredentialAuthentication>>>();
            for (int i = 0; i < 6; i++) revoked.add(executor.submit(() -> authentication.credentials().authenticateContext(cookie)));
            for (var request : revoked) assertThat(request.get(5, TimeUnit.SECONDS)).isEmpty();
            verify(sessions, times(5)).authenticate(eq(identity), eq("jti"), anyString(), any(), any());
        }
    }

    @Test void formalCloudIdentityAndOrganizationUseOneDurableSessionValidation() {
        var properties = new CloudProperties();
        properties.setBaseUrl("https://cloud.example"); properties.setClusterId("cluster");
        properties.setAtlasUri("mongodb://localhost/config"); properties.setToken("test-runtime-token");
        var cloud = CloudRuntimeSettings.resolve(properties);
        var clock = Clock.fixed(Instant.parse("2026-10-26T09:00:00Z"), ZoneOffset.UTC);
        var configuration = mock(MongoConfigurationDomain.class);
        var states = mock(StorageSetupStateStore.class);
        when(configuration.setupStates()).thenReturn(states);
        when(states.load()).thenReturn(new StorageSetupStateStore.Status(
                new StorageSetupScope("cluster", "installation", "operation", 1), StorageSetupStateStore.Phase.COMPLETE, 1));
        var cache = mock(io.tapstate.adapters.mongostore.MongoCacheDomain.class);
        var bound = mock(StorageRuntime.Bound.class);
        var scope = new CacheAuthScope("cluster", 1, StorageBinding.Mode.CLOUD, "incarnation", 1);
        var preparation = new CachePreparation("operation", scope);
        when(bound.preparation()).thenReturn(preparation); when(bound.cache()).thenReturn(cache);
        var sessions = mock(CloudSessionStore.class);
        when(cache.cloudSessions(scope)).thenReturn(sessions);
        when(cache.cloudSessions(eq(scope), any(io.tapstate.adapters.mongostore.MongoReadBudget.class))).thenReturn(sessions);
        var identity = new CloudSessionIdentity("https://cloud.example", CloudSdkBridge.DEPLOYMENT_ORGANIZATION, "cluster");
        var context = new CloudSessionContext("organization", "cluster", "Org", "Cluster", "region");
        when(sessions.authenticate(eq(identity), eq("jti"), anyString(), any(), any()))
                .thenReturn(Optional.of(new CloudSessionRecord(identity, "jti", "hash", "user", "WRITE", false,
                        clock.instant(), clock.instant(), clock.instant().plusSeconds(600), context)));
        try (var authentication = new StorageAuthentication(configuration, cloud, mock(CloudSdkBridge.class),
                new RandomTokenSecrets(), clock, new byte[32])) {
            authentication.install(bound);
            String cookie = "tcs_" + Base64.getUrlEncoder().withoutPadding().encodeToString("jti".getBytes(java.nio.charset.StandardCharsets.UTF_8)) + ".secret";
            var proof = authentication.credentials().authenticateContext(cookie).orElseThrow();
            assertThat(proof.organizationId()).contains("organization");
            assertThat(proof.storageScope()).contains(scope);
            verify(sessions, times(1)).authenticate(eq(identity), eq("jti"), anyString(), any(), any());
            verify(configuration, times(2)).requireAuthenticationPublished(eq(preparation), any(io.tapstate.adapters.mongostore.MongoReadBudget.class));
            var evidence = proof.evidence().orElseThrow();
            assertThat(evidence.type()).isEqualTo(CredentialType.CLOUD_SESSION);
            assertThat(evidence.configurationTokenBinding()).isEmpty();
            clearInvocations(configuration, cache, sessions);
            evidence.requireLocallyActive().run();
            verifyNoInteractions(configuration, cache, sessions);
            when(sessions.authenticate(eq(identity), eq("jti"), anyString(), any(), any())).thenReturn(Optional.empty());
            doNothing().doThrow(StorageRuntime.unavailable()).when(configuration)
                    .requireAuthenticationPublished(eq(preparation), any(io.tapstate.adapters.mongostore.MongoReadBudget.class));
            assertThatThrownBy(proof.requireActive()::run).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE));
        }
    }

    @Test void cloudSetupUsesTheOriginalSdkFlowAndCompletedSetupCannotReopenTemporaryAuthentication() {
        var properties = new CloudProperties();
        properties.setBaseUrl("https://cloud.example"); properties.setClusterId("cluster");
        properties.setAtlasUri("mongodb://localhost/config"); properties.setToken("test-runtime-token");
        var cloud = CloudRuntimeSettings.resolve(properties);
        var clock = Clock.fixed(Instant.parse("2026-10-20T10:00:00Z"), ZoneOffset.UTC);
        var sdk = new CloudSdkBridge.SdkRuntime() {
            @Override public CloudSdkBridge.ExchangeResult exchange(String code) {
                return new CloudSdkBridge.ExchangeResult("signed-jwt", "organization", "cluster", "Org", "Cluster", "region");
            }
            @Override public io.tapstate.cloud.sdk.client.ClusterEgressAddress currentEgressAddress() {
                return new io.tapstate.cloud.sdk.client.ClusterEgressAddress("organization", "cluster", "192.0.2.7", "STABLE");
            }
            @Override public CloudSdkBridge.VerifiedClaims verify(String jwt) {
                return new CloudSdkBridge.VerifiedClaims("user", "organization", "cluster", "jti", clock.instant().plusSeconds(600),
                        "https://cloud.example", "cluster.example", List.of("workload:write"));
            }
            @Override public boolean verifyCallback(String method, String time, String nonce, String data, String signature) { return true; }
            @Override public void report(String cluster, String nonce, CloudRuntimeStatus status, String token) { throw new AssertionError(); }
        };
        var scope = new StorageSetupScope("cluster", "installation", "operation", 1);
        var phase = new AtomicReference<>(StorageSetupStateStore.Phase.CONFIG_PREPARED);
        var configuration = mock(MongoConfigurationDomain.class); var states = mock(StorageSetupStateStore.class);
        when(configuration.setupStates()).thenReturn(states);
        when(configuration.bindings()).thenReturn(mock(StorageBindingStore.class));
        when(states.load()).thenAnswer(ignored -> new StorageSetupStateStore.Status(scope, phase.get(),
                phase.get() == StorageSetupStateStore.Phase.CONFIG_PREPARED ? 0 : 1));
        try (var authentication = new StorageAuthentication(configuration, cloud, new CloudSdkBridge(cloud, sdk),
                new RandomTokenSecrets(), clock, new byte[32])) {
            var original = authentication.managed();
            var created = original.service().exchangeCode("code", "cluster.example");
            assertThat(original.service().clusterContextView(created.token()).orElseThrow().ec2PublicIp()).isEqualTo("192.0.2.7");
            var proof = authentication.credentials().authenticateContext(created.token()).orElseThrow();
            assertThat(proof.storageScope()).isEmpty();
            assertThat(proof.organizationId()).contains("organization");
            proof.requireActive().run();
            phase.set(StorageSetupStateStore.Phase.COMPLETE);
            assertThatThrownBy(proof.requireActive()::run).isInstanceOf(TapstateException.class);
            assertThatThrownBy(authentication::managed).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> original.service().exchangeCode("late-code", "cluster.example")).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> original.service().invalidateJwt("jti")).isInstanceOf(TapstateException.class);
            verify(configuration, never()).users();
            verify(configuration, never()).tokens();
        }
    }

    private record OpFixture(StorageAuthentication authentication, MongoConfigurationDomain configuration,
            io.tapstate.adapters.mongostore.MongoCacheDomain cache, TokenStore tokens, TokenSecrets secrets,
            CacheAuthScope scope, Clock clock, AtomicReference<Instant> now) { }

    private static void withOpAuthentication(java.util.function.Consumer<OpFixture> action) {
        var now = new AtomicReference<>(Instant.parse("2026-10-27T00:00:00Z"));
        Clock clock = new Clock() {
            @Override public ZoneId getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(ZoneId zone) { return this; }
            @Override public Instant instant() { return now.get(); }
        };
        var configuration = mock(MongoConfigurationDomain.class);
        var cache = mock(io.tapstate.adapters.mongostore.MongoCacheDomain.class);
        var scope = new CacheAuthScope("cluster", 1, StorageBinding.Mode.OP, "incarnation", 1);
        var bound = mock(StorageRuntime.Bound.class);
        when(bound.preparation()).thenReturn(new CachePreparation("operation", scope));
        when(bound.cache()).thenReturn(cache);
        when(configuration.users()).thenReturn(mock(UserStore.class));
        when(configuration.clusterIdentity()).thenReturn(mock(ClusterIdentityStore.class));
        when(cache.sessions(scope)).thenReturn(mock(SessionStore.class));
        var tokens = mock(TokenStore.class);
        when(configuration.tokens(any(io.tapstate.adapters.mongostore.MongoReadBudget.class))).thenReturn(tokens);
        var secrets = new RandomTokenSecrets();
        try (var authentication = new StorageAuthentication(configuration, CloudRuntimeSettings.resolve(new CloudProperties()), null, secrets, clock, new byte[32])) {
            authentication.install(bound);
            action.accept(new OpFixture(authentication, configuration, cache, tokens, secrets, scope, clock, now));
        }
    }
}
