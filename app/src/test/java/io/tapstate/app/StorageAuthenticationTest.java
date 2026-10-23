package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class StorageAuthenticationTest {
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
}
