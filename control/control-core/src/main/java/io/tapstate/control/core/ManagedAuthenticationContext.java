package io.tapstate.control.core;

import java.util.Objects;

/** One Cloud request retains its selected local session owner through external exchange and cookie publication. */
public record ManagedAuthenticationContext(CloudAuthenticationService service, Runnable requireActive) {
    public ManagedAuthenticationContext {
        Objects.requireNonNull(service, "service"); Objects.requireNonNull(requireActive, "requireActive");
    }
    @FunctionalInterface public interface Provider { ManagedAuthenticationContext current(); }
}
