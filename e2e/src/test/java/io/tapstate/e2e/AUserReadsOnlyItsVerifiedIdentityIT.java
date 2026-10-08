package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The declarative vocabulary cannot log in as a user, inspect an identity response, or mint a token. */
@RequiresDocker
class AUserReadsOnlyItsVerifiedIdentityIT {
    private static final String USER = "current-user";
    private static final String PASSWORD = "current-user-password-sentinel";
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Test
    void aRealUsersIdentitySurvivesServerReplacementButAMachineCannotReadIt() throws Exception {
        String database = "current_user_" + Long.toUnsignedString(System.nanoTime(), 16);
        String store = SharedMongo.replicaSetUrl(database);
        String userToken;
        String sessionToken;
        String machineToken;
        try (ServerHandle server = InProcessServer.start(store, database + "_operator")) {
            assertThat(post(server.baseUrl(), "/auth/bootstrap", Map.of("username", USER, "password", PASSWORD), null)
                    .statusCode()).isEqualTo(204);
            HttpResponse<String> login = post(server.baseUrl(), "/auth/login",
                    Map.of("username", USER, "password", PASSWORD, "createSession", true), null);
            assertThat(login.statusCode()).isEqualTo(200);
            userToken = (String) object(login).get("token");
            sessionToken = (String) object(login).get("sessionToken");
            assertThat(userToken).isNotBlank();
            assertThat(sessionToken).startsWith("tss_");
            assertUser(get(server.baseUrl(), userToken), userToken);
            assertAnonymous(get(server.baseUrl(), null));

            HttpResponse<String> created = post(server.baseUrl(), "/api/tokens", Map.of("scope", "read"), userToken);
            assertThat(created.statusCode()).isEqualTo(201);
            machineToken = (String) object(created).get("token");
            assertThat(machineToken).startsWith("cyxt_");
            assertAnonymous(get(server.baseUrl(), machineToken));
        }
        try (ServerHandle restarted = InProcessServer.start(store, database + "_operator")) {
            // The default local signing key is ephemeral; the persisted session mints a fresh access token.
            assertAnonymous(get(restarted.baseUrl(), userToken));
            HttpResponse<String> exchanged = http.send(HttpRequest.newBuilder(restarted.baseUrl().resolve("/auth/session"))
                    .timeout(Duration.ofSeconds(10)).header("Authorization", "TapstateSession " + sessionToken)
                    .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(exchanged.statusCode()).isEqualTo(200);
            String freshToken = (String) object(exchanged).get("token");
            assertThat(freshToken).isNotBlank();
            assertUser(get(restarted.baseUrl(), freshToken), freshToken);
            assertAnonymous(get(restarted.baseUrl(), machineToken));
        }
    }

    private HttpResponse<String> get(URI base, String token) throws Exception {
        var request = HttpRequest.newBuilder(base.resolve("/api/auth/me")).timeout(Duration.ofSeconds(10)).GET();
        if (token != null) request.header("Authorization", "Bearer " + token);
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(URI base, String path, Map<String, Object> body, String token) throws Exception {
        var request = HttpRequest.newBuilder(base.resolve(path)).timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JsonWriter.write(body)));
        if (token != null) request.header("Authorization", "Bearer " + token);
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static Map<?, ?> object(HttpResponse<String> response) {
        assertThat(JsonReader.parse(response.body())).isInstanceOf(Map.class);
        return (Map<?, ?>) JsonReader.parse(response.body());
    }

    private static void assertUser(HttpResponse<String> response, String token) {
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Cache-Control")).contains("no-store");
        assertThat(response.headers().firstValue("Set-Cookie")).isEmpty();
        assertThat(object(response)).isEqualTo(Map.of("mode", "on-prem", "principal", USER,
                "scopes", List.of("read", "write", "admin")));
        assertThat(response.body()).doesNotContain(PASSWORD, token, "password", "jwt", "jti", "tokenId");
    }

    private static void assertAnonymous(HttpResponse<String> response) {
        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(object(response).get("code")).isEqualTo("control.unauthenticated");
        assertThat(response.body()).doesNotContain(USER, PASSWORD, "cyxt_");
    }
}
