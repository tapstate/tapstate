package io.tapstate.control.restapi;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.AuthenticationConverter;

/** Managed workload requests accept exactly one local cookie, never Cloud JWT or machine-token bearers. */
final class CloudCookieAuthenticationConverter implements AuthenticationConverter {
    @Override
    public Authentication convert(HttpServletRequest request) {
        if (request.getHeaders(HttpHeaders.AUTHORIZATION).hasMoreElements()
                || !CloudSessionCookies.permitsCookieRequest(request)) {
            return new TapstateCredentialAuthenticationToken("");
        }
        return CloudSessionCookies.read(request)
                .map(TapstateCredentialAuthenticationToken::new).orElse(null);
    }
}
