package io.tapstate.control.restapi;

import java.util.Objects;

/** Fixed paths and credential spelling shared by the user-session HTTP adapters. */
final class AuthWire {

    static final String LOGIN_PATH = "/auth/login";
    static final String SESSION_PATH = "/auth/session";
    static final String LOGOUT_PATH = "/auth/logout";
    static final String DISCOVERY_PATH = "/.well-known/tapstate";
    static final int LOGOUT_SUCCESS_STATUS = 204;
    static final String LOGOUT_SUCCESS_BODY = "";

    private static final String SESSION_SCHEME = "TapstateSession ";

    private AuthWire() {
    }

    static String sessionAuthorization(String sessionToken) {
        return SESSION_SCHEME + Objects.requireNonNull(sessionToken, "sessionToken");
    }
}
