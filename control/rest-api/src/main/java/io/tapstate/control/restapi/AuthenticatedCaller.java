package io.tapstate.control.restapi;

import io.tapstate.control.core.TapstatePrincipal;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

/** Resolves the current verified Tapstate subject for an audited REST-controller call. */
final class AuthenticatedCaller {

    private AuthenticatedCaller() {
    }

    static String subject() {
        return principal().subject();
    }

    static TapstatePrincipal principal() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null && authentication.getPrincipal() instanceof TapstatePrincipal principal) {
            return principal;
        }
        throw new IllegalStateException("an audited REST controller requires a verified Tapstate principal");
    }
}
