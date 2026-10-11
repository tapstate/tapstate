package io.tapstate.control.restapi;

import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.CloudSessionService;
import io.tapstate.control.core.TapstatePrincipal;
import io.tapstate.control.core.TokenService;
import io.tapstate.control.core.VerifiedToken;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;

import java.util.Objects;

/** Authenticates the credential through the implementation selected by the deployment mode. */
final class TapstateCredentialAuthenticationProvider implements AuthenticationProvider {

    private final CredentialAuthenticator credentials;

    TapstateCredentialAuthenticationProvider(CredentialAuthenticator credentials) {
        this.credentials = Objects.requireNonNull(credentials, "credentials");
    }

    @Override
    public Authentication authenticate(Authentication authentication) {
        TapstateCredentialAuthenticationToken credential = (TapstateCredentialAuthenticationToken) authentication;
        VerifiedToken verified = credentials.authenticate(credential.credential())
                .orElseThrow(() -> new BadCredentialsException("invalid bearer credential"));
        TapstatePrincipal principal = TokenService.isMachineToken(credential.credential())
                ? TapstatePrincipal.machineToken(verified)
                : CloudSessionService.isSessionToken(credential.credential())
                        ? TapstatePrincipal.cloudSession(verified) : TapstatePrincipal.humanJwt(verified);
        return new TapstateAuthentication(principal);
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return TapstateCredentialAuthenticationToken.class.isAssignableFrom(authentication);
    }
}
