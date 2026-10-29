package io.tapstate.app;

import io.tapstate.cloud.sdk.ApiClient;
import io.tapstate.cloud.sdk.ApiException;
import io.tapstate.cloud.sdk.api.C2StatusReportApi;
import io.tapstate.cloud.sdk.client.CloudControlPlaneException;
import io.tapstate.cloud.sdk.client.CloudControlPlaneSdk;
import io.tapstate.cloud.sdk.client.ClusterEgressAddress;
import io.tapstate.cloud.sdk.client.JwtExchangeResult;
import io.tapstate.cloud.sdk.client.JwtVerificationException;
import io.tapstate.cloud.sdk.model.StatusReportRequest;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CloudClusterEgressAddress;
import io.tapstate.control.core.CloudClusterEgressAddressProvider;
import io.tapstate.control.core.CloudCodeExchanger;
import io.tapstate.control.core.CloudCodeExchangeResult;
import io.tapstate.control.core.CloudJwtValidator;
import io.tapstate.control.core.CloudLoginIdentity;
import io.tapstate.control.core.CloudRuntimeStatus;
import io.tapstate.control.core.CloudSessionCallbackVerifier;
import io.tapstate.control.core.CloudStatusSender;
import io.tapstate.control.core.Scope;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CloudSessionContext;
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
        CloudSessionCallbackVerifier, CloudStatusSender, CloudClusterEgressAddressProvider {

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
        return exchangeWithContext(exchangeCode, clusterId).jwt();
    }

    @Override
    public CloudCodeExchangeResult exchangeWithContext(String exchangeCode, String clusterId) {
        if (!settings.clusterId().equals(clusterId)) {
            logValidationFailure("code-exchange", "configured-cluster-mismatch");
            throw CloudAuthenticationService.unavailable();
        }
        try {
            ExchangeResult result = sdk.exchange(exchangeCode);
            return new CloudCodeExchangeResult(result.jwt(), result.context());
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
                    expectedDeployment, claims.userId(), claims.organizationId(), claims.clusterId(),
                    claims.jwtId(), scope, claims.expiresAt(), claims.userEmail()));
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

    @Override
    public Optional<CloudClusterEgressAddress> currentAddress(String organizationId, String clusterId) {
        if (!settings.clusterId().equals(clusterId)) {
            return Optional.empty();
        }
        try {
            ClusterEgressAddress current = sdk.currentEgressAddress();
            if (!organizationId.equals(current.organizationId()) || !clusterId.equals(current.clusterId())) {
                return Optional.empty();
            }
            return Optional.of(new CloudClusterEgressAddress(current.organizationId(), current.clusterId(),
                    optionalText(current.ec2PublicIp()), optionalText(current.egressAddressStatus())));
        } catch (ProviderFailure unavailable) {
            return Optional.empty();
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
        String diagnostic = reportableFailureCode(code)
                ? code : "unclassified";
        int status = failure.getHttpStatus();
        if (status < 0 || status > 599) status = 0;
        // Provider messages, response bodies and exception causes can contain authentication inputs.
        markFailure("code-exchange", diagnostic);
        LOG.warn("Cloud authentication rejected [request_id={}, stage=code-exchange, http={}, reason={}]",
                requestId(), status, diagnostic);
    }

    static boolean reportableFailureCode(String code) {
        if (code == null || code.length() > 128) return false;
        int start;
        if (code.startsWith("sdk.")) start = 4;
        else if (code.startsWith("exchange.")) start = 9;
        else if (code.startsWith("c2.")) start = 3;
        else return false;
        boolean segmentEmpty = true;
        for (int index = start; index < code.length(); index++) {
            char value = code.charAt(index);
            if (value == '-') {
                if (segmentEmpty) return false;
                segmentEmpty = true;
            } else if ((value >= 'a' && value <= 'z') || (value >= '0' && value <= '9')) {
                segmentEmpty = false;
            } else {
                return false;
            }
        }
        return !segmentEmpty;
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

    ConfigurationInitialization claimConfiguration(String bootId, String hash) {
        try {
            var result = sdk.claimConfiguration(bootId, hash);
            if (!settings.clusterId().equals(result.clusterId()) || !hash.equals(result.configurationHash())) throw new ProviderFailure();
            return result;
        } catch (ProviderFailure failure) { throw CloudAuthenticationService.unavailable(); }
    }

    void completeConfiguration(String operationId, String hash) {
        try {
            var result = sdk.completeConfiguration(operationId, hash);
            if (!settings.clusterId().equals(result.clusterId()) || !hash.equals(result.configurationHash())
                    || !operationId.equals(result.operationId()) || !"COMPLETE".equals(result.phase()) || result.creationGranted()) throw new ProviderFailure();
        } catch (ProviderFailure failure) { throw CloudAuthenticationService.unavailable(); }
    }

    record ConfigurationInitialization(String clusterId, String operationId, String phase, String configurationHash, boolean creationGranted) {
        ConfigurationInitialization {
            try {
                if (!text(clusterId) || operationId == null || !java.util.UUID.fromString(operationId).toString().equals(operationId)
                        || !List.of("CLAIMED", "COMPLETE").contains(phase) || configurationHash == null || !configurationHash.matches("[0-9a-f]{64}")
                        || creationGranted && !phase.equals("CLAIMED")) throw new ProviderFailure();
            } catch (IllegalArgumentException invalid) { throw new ProviderFailure(); }
        }
    }

    interface SdkRuntime {
        ExchangeResult exchange(String code);
        VerifiedClaims verify(String jwt);
        ClusterEgressAddress currentEgressAddress();
        boolean verifyCallback(String method, String timestamp, String nonce, String data, String signature);
        void report(String clusterId, String nonce, CloudRuntimeStatus status, String token);
        default ConfigurationInitialization claimConfiguration(String bootId, String hash) { throw new ProviderFailure(); }
        default ConfigurationInitialization completeConfiguration(String operationId, String hash) { throw new ProviderFailure(); }
    }

    record ExchangeResult(
            String jwt, String organizationId, String clusterId,
            String organizationName, String clusterName, String region,
            String ec2PublicIp, String egressAddressStatus) {
        ExchangeResult(String jwt) {
            this(jwt, null, null, null, null, null, null, null);
        }

        ExchangeResult(String jwt, String organizationId, String clusterId,
                String organizationName, String clusterName, String region) {
            this(jwt, organizationId, clusterId, organizationName, clusterName, region, null, null);
        }

        ExchangeResult {
            if (!text(jwt)) throw new ProviderFailure();
            boolean noContext = organizationId == null && clusterId == null
                    && organizationName == null && clusterName == null && region == null
                    && ec2PublicIp == null && egressAddressStatus == null;
            boolean completeContext = text(organizationId) && text(clusterId)
                    && text(organizationName) && text(clusterName) && text(region);
            if (!noContext && !completeContext) throw new ProviderFailure();
        }

        CloudSessionContext context() {
            return organizationId == null ? null
                    : new CloudSessionContext(organizationId, clusterId, organizationName, clusterName, region,
                            ec2PublicIp, egressAddressStatus);
        }
    }

    record VerifiedClaims(String userId, String organizationId, String clusterId, String jwtId,
            Instant expiresAt, String issuer, String audience, List<String> scopes, String userEmail) {
        VerifiedClaims(String userId, String organizationId, String clusterId, String jwtId,
                Instant expiresAt, String issuer, String audience, List<String> scopes) {
            this(userId, organizationId, clusterId, jwtId, expiresAt, issuer, audience, scopes, null);
        }
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

    static ExchangeResult exchangeResult(JwtExchangeResult exchanged) {
        String jwt = exchanged.jwt();
        String organizationId = exchanged.orgId();
        String clusterId = exchanged.clusterId();
        String organizationName = exchanged.organizationName();
        String clusterName = exchanged.clusterName();
        String region = exchanged.region();
        if (!text(jwt) || !text(organizationId) || !text(clusterId)
                || !text(organizationName) || !text(clusterName) || !text(region)) {
            throw new ProviderFailure();
        }
        return new ExchangeResult(jwt, organizationId, clusterId, organizationName, clusterName, region,
                optionalText(exchanged.ec2PublicIp()), optionalText(exchanged.egressAddressStatus()));
    }

    private static String optionalText(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static final class TypedSdkRuntime implements SdkRuntime {
        private final CloudControlPlaneSdk sdk;
        private final C2StatusReportApi status;
        private final CloudRuntimeSettings settings;

        TypedSdkRuntime(CloudRuntimeSettings settings) {
            this.settings = settings;
            sdk = CloudControlPlaneSdk.builder()
                    .baseUrl(settings.baseUrl().toString())
                    .issuer(settings.baseUrl().toString())
                    .clusterId(settings.clusterId())
                    .sdkStaticToken(settings.token())
                    .build();
            status = new C2StatusReportApi(new ApiClient().setBasePath(settings.baseUrl().toString()));
        }

        @Override public ConfigurationInitialization claimConfiguration(String bootId, String hash) {
            return configuration("claim", Map.of("bootId", bootId, "configurationHash", hash));
        }
        @Override public ConfigurationInitialization completeConfiguration(String operationId, String hash) {
            return configuration("complete", Map.of("operationId", operationId, "configurationHash", hash));
        }
        private ConfigurationInitialization configuration(String action, Map<String,String> body) {
            // Reuse the installed SDK transport and JSON envelope. Unknown writes are never retried or redirected.
            try (var transport = org.apache.hc.client5.http.impl.classic.HttpClients.custom()
                    .disableAutomaticRetries().disableRedirectHandling().build()) {
                var api = new ApiClient(transport).setBasePath(settings.baseUrl().toString()).setConnectTimeout(5000).setReadTimeout(10000);
                Map<String,Object> response = api.invokeAPI("/v1/api/clusters/" + api.escapeString(settings.clusterId()) + "/configuration-initialization/" + action,
                        "POST", new java.util.ArrayList<>(), new java.util.ArrayList<>(), null, body,
                        new java.util.HashMap<>(Map.of("Authorization", "Bearer " + settings.token())), new java.util.HashMap<>(),
                        new java.util.HashMap<>(), "application/json", "application/json", new String[0],
                        new com.fasterxml.jackson.core.type.TypeReference<Map<String,Object>>() {});
                if (response == null || !"ok".equals(response.get("code")) || !(response.get("data") instanceof Map<?,?> value)
                        || !(value.get("clusterId") instanceof String cluster) || !(value.get("operationId") instanceof String operation)
                        || !(value.get("phase") instanceof String phase) || !(value.get("configurationHash") instanceof String hash)
                        || !(value.get("creationGranted") instanceof Boolean granted)) throw new ProviderFailure();
                return new ConfigurationInitialization(cluster, operation, phase, hash, granted);
            } catch (ApiException | java.io.IOException failure) { throw new ProviderFailure(); }
        }

        @Override
        public ExchangeResult exchange(String code) {
            try {
                return exchangeResult(sdk.exchangeForJwt(code));
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
                        verified.expiresAt(), verified.issuer(), verified.audience(), verified.scope(), optionalText(verified.userEmail()));
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
        public ClusterEgressAddress currentEgressAddress() {
            try {
                return sdk.currentEgressAddress();
            } catch (CloudControlPlaneException failure) {
                logAddressFailure(failure);
                throw new ProviderFailure();
            }
        }

        private static void logAddressFailure(CloudControlPlaneException failure) {
            String code = failure.getErrorCode();
            String diagnostic = reportableFailureCode(code) ? code : "unclassified";
            int status = failure.getHttpStatus();
            if (status < 0 || status > 599) status = 0;
            if (LOG.isWarnEnabled()) {
                LOG.warn("Cloud Cluster address refresh failed [request_id={}, http={}, reason={}]",
                        requestId(), status, diagnostic);
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
