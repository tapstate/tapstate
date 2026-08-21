package io.tapstate.control.restapi;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exact JSON and endpoint contracts for the user-session protocol. These fixtures are deliberately fixed
 * before the session authority exists: later controller and CLI work must consume these shapes instead of
 * inventing a second spelling, timestamp, scope, or credential scheme at either edge.
 */
class AuthWireContractTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String ACCESS_TOKEN = "access.jwt.fixture";
    private static final String ISSUER = "urn:tapstate:cluster:01J5FIXTURE";
    private static final String SESSION_TOKEN = "tss_s01.session-secret";
    private static final List<String> SCOPES = List.of("read", "write", "admin");

    @Test
    void oldLoginRequestOmitsCreateSessionAndStillDecodesAsFalse() throws Exception {
        assertThat(AuthWire.LOGIN_PATH).isEqualTo("/auth/login");
        LoginRequest decoded = JSON.readValue(golden("login-legacy-request.golden.json"), LoginRequest.class);

        assertThat(decoded.createSession()).isFalse();
        assertJsonEqualsGolden(new LoginRequest("admin", "correct horse"), "login-legacy-request.golden.json");
        assertJsonEqualsGolden(new LoginRequest("admin", "correct horse", false),
                "login-legacy-request.golden.json");
    }

    @Test
    void persistentLoginOptsInAndItsIncrementalResponseIsExact() throws Exception {
        assertJsonEqualsGolden(new LoginRequest("admin", "correct horse", true),
                "login-session-request.golden.json");
        assertJsonEqualsGolden(new LoginResponse(
                        ACCESS_TOKEN,
                        "2026-08-17T10:15:00Z",
                        ISSUER,
                        "admin",
                        SCOPES,
                        SESSION_TOKEN,
                        "2026-09-16T10:00:00Z",
                        "2026-11-15T10:00:00Z"),
                "login-session-response.golden.json");
    }

    @Test
    void accessOnlyLoginOmitsEverySessionField() throws Exception {
        assertJsonEqualsGolden(new LoginResponse(
                        ACCESS_TOKEN,
                        "2026-08-17T10:15:00Z",
                        ISSUER,
                        "admin",
                        SCOPES,
                        null,
                        null,
                        null),
                "login-access-response.golden.json");
    }

    @Test
    void sessionExchangeUsesItsOwnSchemeAndReturnsOnlyAProcessAccessToken() throws Exception {
        assertThat(AuthWire.SESSION_PATH).isEqualTo("/auth/session");
        assertThat(AuthWire.sessionAuthorization(SESSION_TOKEN))
                .isEqualTo("TapstateSession " + SESSION_TOKEN);
        assertThat(AuthWire.sessionAuthorization(SESSION_TOKEN)).doesNotStartWith("Bearer ");
        assertJsonEqualsGolden(new SessionExchangeResponse(
                        ACCESS_TOKEN,
                        "2026-08-17T10:15:00Z",
                        ISSUER,
                        "admin",
                        SCOPES),
                "session-exchange-response.golden.json");
    }

    @Test
    void logoutUsesTheSessionSchemeAndHasNoResponseBody() {
        assertThat(AuthWire.LOGOUT_PATH).isEqualTo("/auth/logout");
        assertThat(AuthWire.sessionAuthorization(SESSION_TOKEN))
                .isEqualTo("TapstateSession " + SESSION_TOKEN);
        assertThat(AuthWire.LOGOUT_SUCCESS_STATUS).isEqualTo(204);
        assertThat(AuthWire.LOGOUT_SUCCESS_BODY).isEmpty();
    }

    @Test
    void anonymousIssuerDiscoveryHasAnExactStableShape() throws Exception {
        assertThat(AuthWire.DISCOVERY_PATH).isEqualTo("/.well-known/tapstate");
        assertJsonEqualsGolden(new IssuerDiscoveryResponse(
                        ISSUER,
                        "01J5FIXTURE",
                        "tapstate/v1",
                        List.of("password", "machine_token")),
                "issuer-discovery-response.golden.json");
    }

    private static void assertJsonEqualsGolden(Object value, String fixture) throws Exception {
        assertThat(JSON.writeValueAsString(value)).isEqualTo(golden(fixture));
    }

    private static String golden(String name) throws IOException {
        try (var input = AuthWireContractTest.class.getResourceAsStream("/golden/auth/" + name)) {
            if (input == null) {
                throw new IOException("missing auth wire golden: " + name);
            }
            String file = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(file).doesNotContain("\r").endsWith("\n");
            return file.substring(0, file.length() - 1);
        }
    }
}
