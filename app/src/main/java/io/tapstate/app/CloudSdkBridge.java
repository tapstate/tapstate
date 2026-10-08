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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Thin binding to the Cloud-owned SDK; session and status policy remain in Tapstate. */
final class CloudSdkBridge implements CloudCodeExchanger, CloudJwtValidator,
        CloudSessionCallbackVerifier, CloudStatusSender {

    static final String DEPLOYMENT_ORGANIZATION = "sdk-verified-cluster";
    private static final Logger LOG = LoggerFactory.getLogger(CloudSdkBridge.class);

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
            logValidationFailure("code-exchange", "configured-cluster-mismatch");
            throw CloudAuthenticationService.unavailable();
        }
        try {
            return sdk.exchange(exchangeCode).jwt();
        } catch (ProviderFailure unavailable) {
            throw CloudAuthenticationService.unavailable();
        }
    }

    @Override
    public Optional<CloudLoginIdentity> validate(
            String rawJwt, CloudSessionIdentity expectedDeployment, String expectedAudience) {
        if (!deploymentMatches(expectedDeployment)) {
            logValidationFailure("claims-validation", "deployment-mismatch");
            return Optional.empty();
        }
        if (!text(expectedAudience)) {
            logValidationFailure("claims-validation", "missing-request-audience");
            return Optional.empty();
        }
        try {
            VerifiedClaims claims = sdk.verify(rawJwt);
            String rejected = !text(claims.userId()) ? "missing-user-id"
                    : !text(claims.organizationId()) ? "missing-org-id"
                    : !text(claims.jwtId()) ? "missing-jti"
                    : !settings.clusterId().equals(claims.clusterId()) ? "cluster-id-mismatch"
                    : !settings.baseUrl().toString().equals(claims.issuer()) ? "issuer-mismatch"
                    : !expectedAudience.equals(claims.audience()) ? "audience-mismatch"
                    : null;
            if (rejected != null) {
                logValidationFailure("claims-validation", rejected);
                return Optional.empty();
            }
            Scope scope = scope(claims.scopes());
            if (scope == null) {
                logValidationFailure("claims-validation", "unsupported-scope");
                return Optional.empty();
            }
            return Optional.of(new CloudLoginIdentity(
                    expectedDeployment, claims.userId(), claims.jwtId(), scope, claims.expiresAt()));
        } catch (ProviderFailure unavailable) {
            return Optional.empty();
        } catch (IllegalArgumentException invalid) {
            logValidationFailure("claims-validation", "invalid-verified-claims");
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

    private static void logExchangeFailure(CloudControlPlaneException failure) {
        String code = failure.getErrorCode();
        String diagnostic = code != null && code.length() <= 128
                && code.matches("(?:sdk|exchange|c2)\\.[a-z0-9]+(?:-[a-z0-9]+)*")
                ? code : "unclassified";
        int status = failure.getHttpStatus();
        if (status < 0 || status > 599) status = 0;
        // Provider messages, response bodies and exception causes can contain authentication inputs.
        markFailure("code-exchange", diagnostic);
        LOG.warn("Cloud authentication rejected [request_id={}, stage=code-exchange, http={}, reason={}]",
                requestId(), status, diagnostic);
    }

    private static void logValidationFailure(String stage, String reason) {
        markFailure(stage, reason);
        LOG.warn("Cloud authentication rejected [request_id={}, stage={}, reason={}]", requestId(), stage, reason);
    }

    private static void markFailure(String stage, String reason) {
        if (MDC.get(CloudHttpDiagnosticsFilter.REQUEST_ID_MDC) != null) {
            MDC.put(CloudHttpDiagnosticsFilter.STAGE_MDC, stage);
            MDC.put(CloudHttpDiagnosticsFilter.REASON_MDC, reason);
        }
    }

    private static String requestId() {
        String id = MDC.get(CloudHttpDiagnosticsFilter.REQUEST_ID_MDC);
        return id == null ? "none" : id;
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
                    .sdkStaticToken(settings.token())
                    .build();
            status = new C2StatusReportApi(new ApiClient().setBasePath(settings.baseUrl().toString()));
        }

        @Override
        public ExchangeResult exchange(String code) {
            try {
                return new ExchangeResult(sdk.exchangeForJwt(code).jwt());
            } catch (CloudControlPlaneException failure) {
                logExchangeFailure(failure);
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
            } catch (JwtVerificationException failure) {
                String kind = failure.getKind();
                String reason = switch (kind == null ? "" : kind) {
                    case "missing-kid", "jwk-not-found", "signature-mismatch", "expired", "bad-issuer",
                            "bad-audience", "bad-claim", "parse-error" -> kind;
                    default -> "unclassified-jwt-rejection";
                };
                logValidationFailure("jwt-verification", reason);
                throw new ProviderFailure();
            } catch (CloudControlPlaneException failure) {
                logValidationFailure("jwt-verification", "jwks-provider-unavailable");
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
                var acknowledgment = status.report(clusterId, request, "Bearer " + token);
                if (acknowledgment == null || !"ok".equals(acknowledgment.getCode())
                        || acknowledgment.getData() == null
                        || !"accepted".equals(acknowledgment.getData().getStatus())) {
                    throw new ProviderFailure();
                }
            } catch (ApiException failure) {
                throw new ProviderFailure();
            }
        }
    }
}
