package io.tapstate.control.restapi;

import io.tapstate.control.core.TapstatePrincipal;
import io.tapstate.control.core.CredentialAuthentication;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.Collection;
import java.util.Comparator;
import java.util.Objects;

/** A successful Spring Security authentication backed by a framework-free Tapstate principal. */
final class TapstateAuthentication extends AbstractAuthenticationToken {

    private final TapstatePrincipal principal;
    private final CredentialAuthentication original;

    TapstateAuthentication(TapstatePrincipal principal) {
        this(principal, null);
    }

    TapstateAuthentication(TapstatePrincipal principal, CredentialAuthentication original) {
        super(authoritiesFor(principal));
        this.principal = Objects.requireNonNull(principal, "principal");
        this.original = original;
        setAuthenticated(true);
    }

    CredentialAuthentication original() { return original; }

    @Override
    public Object getCredentials() {
        return "";
    }

    @Override
    public TapstatePrincipal getPrincipal() {
        return principal;
    }

    private static Collection<SimpleGrantedAuthority> authoritiesFor(TapstatePrincipal principal) {
        Objects.requireNonNull(principal, "principal");
        return principal.scopes().stream()
                .sorted(Comparator.comparing(Enum::name))
                .map(scope -> new SimpleGrantedAuthority("SCOPE_" + scope.name()))
                .toList();
    }
}
