package io.tapstate.control.restapi;

import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.Scope;
import io.tapstate.core.common.TapstateException;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Shared on-prem authentication mode and scope projection. */
final class AuthSupport {
    private AuthSupport() { }

    static void requireOnPrem(AuthenticationMode mode) {
        if (mode != AuthenticationMode.ON_PREM) {
            throw new TapstateException(ControlError.AUTH_MODE_UNAVAILABLE,
                    Map.of("mode", mode.name().toLowerCase(Locale.ROOT)), null);
        }
    }

    static List<String> scopesFor(Scope grade) {
        return Arrays.stream(Scope.values()).filter(grade::permits)
                .map(scope -> scope.name().toLowerCase(Locale.ROOT)).toList();
    }
}
