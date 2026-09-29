package io.tapstate.app;

import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CloudRuntimeStatus;
import io.tapstate.control.core.Scope;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CloudSessionIdentity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudSdkBridgeTest {

    private static final Instant EXPIRY = Instant.parse("2030-01-01T00:00:00Z");
    private CloudRuntimeSettings settings;
    private ControlledSdk sdk;
    private CloudSdkBridge bridge;
    private CloudSessionIdentity deployment;

    @BeforeEach
    void setUp() {
        CloudProperties properties = new CloudProperties();
        properties.setBaseUrl("https://cloud.example");
        properties.setToken("static-token-sentinel");
        properties.setAtlasUri("mongodb+srv://user:secret@atlas.example/cluster_meta");
        properties.setClusterId("cluster-one");
        settings = CloudRuntimeSettings.resolve(properties);
        sdk = new ControlledSdk();
        bridge = new CloudSdkBridge(settings, sdk);
        deployment = new CloudSessionIdentity(
                "https://cloud.example", CloudSdkBridge.DEPLOYMENT_ORGANIZATION, "cluster-one");
    }

    @Test
    void exchangesTheRequestCodeAndRequiresTheConfiguredCluster() {
        assertThat(bridge.exchange("one-time-code", "cluster-one")).isEqualTo("signed-jwt");
        assertThat(sdk.exchangeCode).isEqualTo("one-time-code");
        assertThatThrownBy(() -> bridge.exchange("one-time-code", "other-cluster"))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code().code())
                .isEqualTo("control.cloud-auth-unavailable");
    }

    @Test
    void acceptsOnlyVerifiedClaimsForThisDeploymentAndMapsWorkloadScope() {
        var login = bridge.validate("signed-jwt", deployment).orElseThrow();
        assertThat(login.userId()).isEqualTo("stable-user");
        assertThat(login.jwtId()).isEqualTo("jwt-one");
        assertThat(login.scope()).isEqualTo(Scope.WRITE);
        assertThat(login.jwtExpiresAt()).isEqualTo(EXPIRY);
        assertThat(sdk.verifiedJwt).isEqualTo("signed-jwt");

        sdk.claims = claims("other-cluster", "https://cloud.example", "audience", List.of("workload:write"));
        assertThat(bridge.validate("signed-jwt", deployment)).isEmpty();
        sdk.claims = claims("cluster-one", "https://other.example", "cluster-one.api.tapstate.io",
                List.of("workload:write"));
        assertThat(bridge.validate("signed-jwt", deployment)).isEmpty();
        sdk.claims = claims("cluster-one", "https://cloud.example", "", List.of("workload:write"));
        assertThat(bridge.validate("signed-jwt", deployment)).isEmpty();
        sdk.claims = claims("cluster-one", "https://cloud.example", "other-cluster.api.tapstate.io",
                List.of("workload:write"));
        assertThat(bridge.validate("signed-jwt", deployment)).isEmpty();
        sdk.claims = claims("cluster-one", "https://cloud.example", "cluster-one.api.tapstate.io",
                List.of("admin"));
        assertThat(bridge.validate("signed-jwt", deployment)).isEmpty();
    }

    @Test
    void passesTheBodylessRevocationSignatureFieldsToTheSdk() {
        assertThat(bridge.verify("https://cloud.example", CloudSdkBridge.DEPLOYMENT_ORGANIZATION,
                "cluster-one", "POST", "1780000000000", "nonce-one", "jwt-one", "signature-one"))
                .isTrue();
        assertThat(sdk.callback).containsExactly(
                "POST", "1780000000000", "nonce-one", "jwt-one", "signature-one");
        assertThat(bridge.verify("https://cloud.example", CloudSdkBridge.DEPLOYMENT_ORGANIZATION,
                "other-cluster", "POST", "1780000000000", "nonce-one", "jwt-one", "signature-one"))
                .isFalse();
    }

    @Test
    void sendsCredentialFreeStatusWithTheConfiguredStaticToken() {
        CloudRuntimeStatus status = new CloudRuntimeStatus(
                "0.5.0", 1234L, 2, Instant.parse("2026-09-29T08:00:00Z"));
        bridge.send("cluster-one", "nonce-one", status);
        assertThat(sdk.reportCluster).isEqualTo("cluster-one");
        assertThat(sdk.reportNonce).isEqualTo("nonce-one");
        assertThat(sdk.reportStatus).isSameAs(status);
        assertThat(sdk.reportToken).isEqualTo("static-token-sentinel");
    }

    @Test
    void providerFailuresStayCodedAndCarryNoProviderCause() {
        sdk.failure = true;
        assertThatThrownBy(() -> bridge.exchange("one-time-code", "cluster-one"))
                .isInstanceOf(TapstateException.class)
                .hasNoCause()
                .hasMessageNotContaining("static-token-sentinel");
        assertThat(bridge.validate("signed-jwt", deployment)).isEmpty();
        assertThat(bridge.verify("https://cloud.example", CloudSdkBridge.DEPLOYMENT_ORGANIZATION,
                "cluster-one", "POST", "ts", "nonce", "jti", "signature")).isFalse();
        assertThatThrownBy(() -> bridge.send(
                "cluster-one", "nonce", new CloudRuntimeStatus("0.5.0", 1, 0, null)))
                .isInstanceOf(TapstateException.class)
                .hasNoCause()
                .hasMessageNotContaining("static-token-sentinel");
    }

    private static CloudSdkBridge.VerifiedClaims claims(
            String clusterId, String issuer, String audience, List<String> scopes) {
        return new CloudSdkBridge.VerifiedClaims(
                "stable-user", "org-one", clusterId, "jwt-one", EXPIRY, issuer, audience, scopes);
    }

    private static final class ControlledSdk implements CloudSdkBridge.SdkRuntime {
        private CloudSdkBridge.VerifiedClaims claims = claims(
                "cluster-one", "https://cloud.example", "cluster-one.api.tapstate.io",
                List.of("workload:read", "workload:write"));
        private String exchangeCode;
        private String verifiedJwt;
        private List<String> callback;
        private String reportCluster;
        private String reportNonce;
        private CloudRuntimeStatus reportStatus;
        private String reportToken;
        private boolean failure;

        @Override
        public CloudSdkBridge.ExchangeResult exchange(String code) {
            failIfRequested();
            exchangeCode = code;
            return new CloudSdkBridge.ExchangeResult("signed-jwt");
        }

        @Override
        public CloudSdkBridge.VerifiedClaims verify(String jwt) {
            failIfRequested();
            verifiedJwt = jwt;
            return claims;
        }

        @Override
        public boolean verifyCallback(String method, String timestamp, String nonce, String data, String signature) {
            failIfRequested();
            callback = List.of(method, timestamp, nonce, data, signature);
            return true;
        }

        @Override
        public void report(String clusterId, String nonce, CloudRuntimeStatus status, String token) {
            failIfRequested();
            reportCluster = clusterId;
            reportNonce = nonce;
            reportStatus = status;
            reportToken = token;
        }

        private void failIfRequested() {
            if (failure) throw new CloudSdkBridge.ProviderFailure();
        }
    }
}
