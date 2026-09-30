package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
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

    public CreatedCloudSession exchangeCode(String code) {
        if (code == null || code.isBlank()) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "an exchange code is required"), null);
        }
        observer.entering(CloudAuthenticationObserver.Stage.CODE_EXCHANGE);
        String jwt = exchanger.exchange(code, sessions.identity().clusterId());
        if (jwt == null || jwt.isBlank()) {
            throw unavailable();
        }
        observer.entering(CloudAuthenticationObserver.Stage.JWT_VERIFICATION);
        CloudLoginIdentity login = validator.validate(jwt, sessions.identity())
                .orElseThrow(CloudAuthenticationService::unauthenticated);
        observer.entering(CloudAuthenticationObserver.Stage.SESSION_CREATE);
        return sessions.create(login).orElseThrow(CloudAuthenticationService::unauthenticated);
    }

    public Optional<VerifiedToken> authenticate(String cookie) {
        return sessions.authenticate(cookie);
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
