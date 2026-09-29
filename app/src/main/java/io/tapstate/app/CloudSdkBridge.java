package io.tapstate.app;

import io.tapstate.cloud.sdk.ApiClient;
import io.tapstate.cloud.sdk.ApiException;
import io.tapstate.cloud.sdk.api.C2StatusReportApi;
import io.tapstate.cloud.sdk.client.CloudControlPlaneException;
import io.tapstate.cloud.sdk.client.CloudControlPlaneSdk;
import io.tapstate.cloud.sdk.client.JwtVerificationException;
import io.tapstate.cloud.sdk.model.StatusReportRequest;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CloudCodeExchanger;
import io.tapstate.control.core.CloudJwtValidator;
import io.tapstate.control.core.CloudLoginIdentity;
import io.tapstate.control.core.CloudRuntimeStatus;
import io.tapstate.control.core.CloudSessionCallbackVerifier;
import io.tapstate.control.core.CloudStatusSender;
import io.tapstate.control.core.Scope;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CloudSessionIdentity;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Thin binding to the Cloud-owned SDK; session and status policy remain in Tapstate. */
final class CloudSdkBridge implements CloudCodeExchanger, CloudJwtValidator,
        CloudSessionCallbackVerifier, CloudStatusSender {

    static final String DEPLOYMENT_ORGANIZATION = "sdk-verified-cluster";
    private static final String AUDIENCE_SUFFIX = ".api.tapstate.io";

    private final CloudRuntimeSettings settings;
    private final SdkRuntime sdk;

    CloudSdkBridge(CloudRuntimeSettings settings) {
        this(settings, new TypedSdkRuntime(settings));
    }

    CloudSdkBridge(CloudRuntimeSettings settings, SdkRuntime sdk) {
        this.settings = Objects.requireNonNull(settings, "settings");
        if (!settings.cloud()) {
            throw new IllegalArgumentException("the Cloud SDK bridge requires Cloud mode");
        }
        this.sdk = Objects.requireNonNull(sdk, "sdk");
    }

    @Override
    public String exchange(String exchangeCode, String clusterId) {
        if (!settings.clusterId().equals(clusterId)) {
            throw CloudAuthenticationService.unavailable();
        }
        try {
            return sdk.exchange(exchangeCode).jwt();
        } catch (ProviderFailure unavailable) {
            throw CloudAuthenticationService.unavailable();
        }
    }

    @Override
    public Optional<CloudLoginIdentity> validate(String rawJwt, CloudSessionIdentity expectedDeployment) {
        if (!deploymentMatches(expectedDeployment)) {
            return Optional.empty();
        }
        try {
            VerifiedClaims claims = sdk.verify(rawJwt);
            if (!text(claims.userId()) || !text(claims.organizationId()) || !text(claims.jwtId())
                    || !settings.clusterId().equals(claims.clusterId())
                    || !settings.baseUrl().toString().equals(claims.issuer())
                    || !(settings.clusterId() + AUDIENCE_SUFFIX).equals(claims.audience())) {
                return Optional.empty();
            }
            Scope scope = scope(claims.scopes());
            if (scope == null) {
                return Optional.empty();
            }
            return Optional.of(new CloudLoginIdentity(
                    expectedDeployment, claims.userId(), claims.jwtId(), scope, claims.expiresAt()));
        } catch (ProviderFailure | IllegalArgumentException unavailable) {
            return Optional.empty();
        }
    }

    @Override
    public boolean verify(String issuer, String organizationId, String clusterId, String method,
            String timestamp, String nonce, String data, String signature) {
        if (!settings.baseUrl().toString().equals(issuer)
                || !DEPLOYMENT_ORGANIZATION.equals(organizationId)
                || !settings.clusterId().equals(clusterId)) {
            return false;
        }
        try {
            return sdk.verifyCallback(method, timestamp, nonce, data, signature);
        } catch (ProviderFailure unavailable) {
            return false;
        }
    }

    @Override
    public void send(String clusterId, String nonce, CloudRuntimeStatus status) {
        if (!settings.clusterId().equals(clusterId)) {
            throw statusUnavailable();
        }
        try {
            sdk.report(clusterId, nonce, status, settings.token());
        } catch (ProviderFailure unavailable) {
            throw statusUnavailable();
        }
    }

    private boolean deploymentMatches(CloudSessionIdentity deployment) {
        return deployment != null
                && settings.baseUrl().toString().equals(deployment.issuer())
                && DEPLOYMENT_ORGANIZATION.equals(deployment.organizationId())
                && settings.clusterId().equals(deployment.clusterId());
    }

    private static Scope scope(List<String> scopes) {
        if (scopes == null) return null;
        if (scopes.contains("workload:write")) return Scope.WRITE;
        return scopes.contains("workload:read") ? Scope.READ : null;
    }

    private static boolean text(String value) {
        return value != null && !value.isBlank();
    }

    private static TapstateException statusUnavailable() {
        return new TapstateException(BootError.CLOUD_STATUS_SDK_REQUIRED, Map.of(), null);
    }

    interface SdkRuntime {
        ExchangeResult exchange(String code);
        VerifiedClaims verify(String jwt);
        boolean verifyCallback(String method, String timestamp, String nonce, String data, String signature);
        void report(String clusterId, String nonce, CloudRuntimeStatus status, String token);
    }

    record ExchangeResult(String jwt) {
        ExchangeResult {
            if (!text(jwt)) throw new ProviderFailure();
        }
    }

    record VerifiedClaims(String userId, String organizationId, String clusterId, String jwtId,
            Instant expiresAt, String issuer, String audience, List<String> scopes) {
        VerifiedClaims {
            Objects.requireNonNull(expiresAt, "expiresAt");
            scopes = scopes == null ? null : List.copyOf(scopes);
        }
    }

    static final class ProviderFailure extends RuntimeException {
        ProviderFailure() {
            super(null, null, false, false);
        }
    }

    private static final class TypedSdkRuntime implements SdkRuntime {
        private final CloudControlPlaneSdk sdk;
        private final C2StatusReportApi status;

        TypedSdkRuntime(CloudRuntimeSettings settings) {
            sdk = CloudControlPlaneSdk.builder()
                    .baseUrl(settings.baseUrl().toString())
                    .issuer(settings.baseUrl().toString())
                    .clusterId(settings.clusterId())
                    .clusterIdentitySecret(settings.token())
                    .build();
            status = new C2StatusReportApi(new ApiClient().setBasePath(settings.baseUrl().toString()));
        }

        @Override
        public ExchangeResult exchange(String code) {
            try {
                return new ExchangeResult(sdk.exchangeForJwt(code).jwt());
            } catch (CloudControlPlaneException failure) {
                throw new ProviderFailure();
            }
        }

        @Override
        public VerifiedClaims verify(String jwt) {
            try {
                var verified = sdk.verifyJwt(jwt);
                return new VerifiedClaims(
                        verified.userId(), verified.orgId(), verified.clusterId(), verified.jti(),
                        verified.expiresAt(), verified.issuer(), verified.audience(), verified.scope());
            } catch (CloudControlPlaneException | JwtVerificationException failure) {
                throw new ProviderFailure();
            }
        }

        @Override
        public boolean verifyCallback(String method, String timestamp, String nonce, String data, String signature) {
            return sdk.verifyRequest(method, timestamp, nonce, data, signature);
        }

        @Override
        public void report(String clusterId, String nonce, CloudRuntimeStatus runtime, String token) {
            StatusReportRequest request = new StatusReportRequest()
                    .nonce(nonce)
                    .runtimeVersion(runtime.runtimeVersion())
                    .uptimeMs(runtime.uptimeMillis())
                    .activePipelines(runtime.activePipelines())
                    .lastErrorAt(runtime.lastErrorAt() == null ? null : runtime.lastErrorAt().toString());
            try {
                status.report(clusterId, request, "Bearer " + token);
            } catch (ApiException failure) {
                throw new ProviderFailure();
            }
        }
    }
}
