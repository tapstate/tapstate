package io.tapstate.control.core;

import java.util.Objects;

/** One OP request uses one issuer and session backend through password checks, exchange and response publication. */
public record LocalAuthenticationContext(LoginService login, SessionService sessions,
        ClusterIdentityService identity, Runnable requireActive) {
    public LocalAuthenticationContext {
        Objects.requireNonNull(login, "login"); Objects.requireNonNull(sessions, "sessions");
        Objects.requireNonNull(identity, "identity"); Objects.requireNonNull(requireActive, "requireActive");
    }
    @FunctionalInterface public interface Provider { LocalAuthenticationContext current(); }
}
