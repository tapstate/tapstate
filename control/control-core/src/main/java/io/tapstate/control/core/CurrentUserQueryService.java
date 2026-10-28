package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Projects the selected mode's authenticated user and optional session email. */
public final class CurrentUserQueryService {
    private final AuthenticationMode mode;

    public CurrentUserQueryService(AuthenticationMode mode) {
        this.mode = Objects.requireNonNull(mode, "mode");
    }

    public CurrentUserView get(TapstatePrincipal principal) {
        return get(principal, null);
    }

    public CurrentUserView get(TapstatePrincipal principal, String cloudEmail) {
        Objects.requireNonNull(principal, "principal");
        CredentialType expected = mode == AuthenticationMode.CLOUD
                ? CredentialType.CLOUD_SESSION : CredentialType.HUMAN_JWT;
        if (principal.credentialType() != expected) {
            throw new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null);
        }
        var scopes = Arrays.stream(Scope.values()).filter(principal::permits)
                .map(scope -> scope.name().toLowerCase(Locale.ROOT)).toList();
        String username = mode == AuthenticationMode.ON_PREM ? principal.subject() : null;
        String email = cloudEmail;
        if (mode == AuthenticationMode.ON_PREM) {
            email = username.matches("[^@\\s]+@[^@\\s]+") ? username : null;
        }
        return new CurrentUserView(mode == AuthenticationMode.CLOUD ? "cloud" : "on-prem",
                principal.subject(), scopes, username, email);
    }
}
