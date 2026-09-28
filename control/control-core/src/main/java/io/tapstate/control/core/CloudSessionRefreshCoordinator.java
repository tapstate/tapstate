package io.tapstate.control.core;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Keeps the Cloud credential behind a Cluster session current. A credential outside the refresh
 * window is returned without an external call. Inside the window, a provider refresh must return the
 * same user, Organization and Cluster with a future expiry; any provider failure, refusal or identity
 * change invalidates the Cluster session and is reported as an empty result to the HTTP adapter.
 */
public final class CloudSessionRefreshCoordinator {

    private final CloudTokenRefresher refresher;
    private final CloudSessionInvalidator invalidator;
    private final Clock clock;
    private final Duration refreshBefore;

    public CloudSessionRefreshCoordinator(
            CloudTokenRefresher refresher,
            CloudSessionInvalidator invalidator,
            Clock clock,
            Duration refreshBefore) {
        this.refresher = Objects.requireNonNull(refresher, "refresher");
        this.invalidator = Objects.requireNonNull(invalidator, "invalidator");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.refreshBefore = Objects.requireNonNull(refreshBefore, "refreshBefore");
        if (refreshBefore.isNegative()) {
            throw new IllegalArgumentException("refreshBefore must not be negative");
        }
    }

    public Optional<CloudUserToken> current(String sessionId, CloudUserToken current) {
        requireText(sessionId, "sessionId");
        Objects.requireNonNull(current, "current");
        Instant now = clock.instant();
        if (current.expiresAt().isAfter(now.plus(refreshBefore))) {
            return Optional.of(current);
        }
        try {
            Optional<CloudUserToken> refreshed = refresher.refresh(current);
            if (refreshed.isPresent()
                    && current.sameIdentity(refreshed.get())
                    && refreshed.get().expiresAt().isAfter(now)) {
                return refreshed;
            }
        } catch (RuntimeException unavailable) {
            // The product contract treats an unrefreshable Cloud identity as logged out. The provider
            // adapter owns the diagnostic; this coordinator owns the mandatory local invalidation.
        }
        invalidator.invalidate(sessionId);
        return Optional.empty();
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must be non-blank");
        }
    }
}
