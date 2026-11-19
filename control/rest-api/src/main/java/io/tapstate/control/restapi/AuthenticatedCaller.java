package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.CredentialType;
import io.tapstate.control.core.TapstatePrincipal;
import io.tapstate.core.common.TapstateException;
import java.util.Map;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

/** Resolves the current verified Tapstate subject for an audited REST-controller call. */
final class AuthenticatedCaller {

    private AuthenticatedCaller() {
    }

    static String subject() {
        return principal().subject();
    }

    static boolean isCloudSession() {
        return principal().credentialType() == CredentialType.CLOUD_SESSION;
    }

    /** Captures the verified request's lifetime for work that can outlive the security context. */
    static Runnable requestGuard() {
        var authentication = SecurityContextHolder.getContext().getAuthentication();
        if (!(authentication instanceof TapstateAuthentication verified) || verified.original() == null) {
            throw new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null);
        }
        return verified.original().requireActive();
    }

    static TapstatePrincipal principal() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null && authentication.getPrincipal() instanceof TapstatePrincipal principal) {
            return principal;
        }
        throw new IllegalStateException("an audited REST controller requires a verified Tapstate principal");
    }
}
