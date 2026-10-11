package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CloudSessionContext;
import io.tapstate.spi.store.CloudSessionIdentity;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** SDK exchange, then SDK verification, then local session creation. No later JWT refresh or validation. */
public final class CloudAuthenticationService {

    private final CloudCodeExchanger exchanger;
    private final CloudJwtValidator validator;
    private final CloudSessionService sessions;
    private final CloudAuthenticationObserver observer;

    public CloudAuthenticationService(
            CloudCodeExchanger exchanger, CloudJwtValidator validator, CloudSessionService sessions) {
        this(exchanger, validator, sessions, CloudAuthenticationObserver.NONE);
    }

    public CloudAuthenticationService(CloudCodeExchanger exchanger, CloudJwtValidator validator,
            CloudSessionService sessions, CloudAuthenticationObserver observer) {
        this.exchanger = Objects.requireNonNull(exchanger, "exchanger");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.sessions = Objects.requireNonNull(sessions, "sessions");
        this.observer = Objects.requireNonNull(observer, "observer");
    }

    public CreatedCloudSession exchangeCode(String code, String expectedAudience) {
        if (code == null || code.isBlank()) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "an exchange code is required"), null);
        }
        if (expectedAudience == null || expectedAudience.isBlank()) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "a request audience is required"), null);
        }
        observer.entering(CloudAuthenticationObserver.Stage.CODE_EXCHANGE);
        CloudCodeExchangeResult exchanged = exchanger.exchangeWithContext(code, sessions.identity().clusterId());
        String jwt = exchanged == null ? null : exchanged.jwt();
        if (jwt == null || jwt.isBlank()) {
            throw unavailable();
        }
        observer.entering(CloudAuthenticationObserver.Stage.JWT_VERIFICATION);
        CloudLoginIdentity login = validator.validate(jwt, sessions.identity(), expectedAudience)
                .orElseThrow(CloudAuthenticationService::unauthenticated);
        CloudSessionContext context = exchanged.context();
        if (context != null && (!Objects.equals(context.organizationId(), login.organizationId())
                || !Objects.equals(context.clusterId(), login.clusterId())
                || !sessions.identity().clusterId().equals(context.clusterId()))) {
            throw unauthenticated();
        }
        observer.entering(CloudAuthenticationObserver.Stage.SESSION_CREATE);
        return sessions.create(login, context).orElseThrow(CloudAuthenticationService::unauthenticated);
    }

    public Optional<VerifiedToken> authenticate(String cookie) {
        return sessions.authenticate(cookie);
    }

    public Optional<CloudSessionContext> clusterContext(String cookie) {
        return sessions.clusterContext(cookie);
    }

    /** Projects authenticated display fields without exposing a persistence record to presentation adapters. */
    public Optional<CloudClusterContextView> clusterContextView(String cookie) {
        return clusterContext(cookie).map(context -> new CloudClusterContextView(
                context.organizationId(), context.clusterId(), context.organizationName(),
                context.clusterName(), context.region()));
    }

    public boolean logout(String cookie) {
        return sessions.logout(cookie);
    }

    public CloudSessionIdentity identity() {
        return sessions.identity();
    }

    public void invalidateJwt(String jwtId) {
        sessions.invalidateJwt(jwtId);
    }

    /** Keeps the persistence identity type behind the control ring's presentation boundary. */
    public boolean authorizeCallback(CloudSessionCallbackVerifier verifier, String method,
            String timestamp, String nonce, String data, String signature) {
        CloudSessionIdentity identity = sessions.identity();
        return verifier.verify(identity.issuer(), identity.organizationId(), identity.clusterId(),
                method, timestamp, nonce, data, signature);
    }

    public static TapstateException unavailable() {
        return new TapstateException(ControlError.CLOUD_AUTH_UNAVAILABLE, Map.of(), null);
    }

    private static TapstateException unauthenticated() {
        return new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null);
    }
}
