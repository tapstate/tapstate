package io.tapstate.control.core;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/** Issues short-lived bearer credentials that never need to be persisted by a client. */
public final class AccessTokenService {

    public static final Duration DEFAULT_TTL = Duration.ofMinutes(15);

    private final TokenSigner signer;
    private final Duration ttl;
    private final Clock clock;

    public AccessTokenService(TokenSigner signer, Clock clock) {
        this(signer, DEFAULT_TTL, clock);
    }

    public AccessTokenService(TokenSigner signer, Duration ttl, Clock clock) {
        this.signer = Objects.requireNonNull(signer, "signer");
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public AccessTokenGrant issue(String principal, Scope scope) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(scope, "scope");
        return new AccessTokenGrant(signer.issue(principal, scope), clock.instant().plus(ttl), principal, scope);
    }
}
