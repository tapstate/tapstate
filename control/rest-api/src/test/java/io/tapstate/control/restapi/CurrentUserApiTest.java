package io.tapstate.control.restapi;

import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.CurrentUserQueryService;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.VerifiedToken;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual HTTP/security wiring with a controlled credential verifier, not a live Cloud login witness. */
class CurrentUserApiTest {
    private static final String MACHINE = "cyxt_current-machine.machine-secret-sentinel";
    private static final String FOREIGN = "https://foreign.example";

    @ParameterizedTest
    @EnumSource(AuthenticationMode.class)
    void aCurrentUserReadReturnsOnlyVerifiedIdentityAndRejectsNonUserCredentials(AuthenticationMode mode) {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestApp.class).run(
                "--server.port=0", "--test.current-user.mode=" + mode.name())) {
            int port = ((WebServerApplicationContext) context).getWebServer().getPort();
            String origin = "http://127.0.0.1:" + port;
            RestClient client = RestClient.create(origin);
            String principal = mode == AuthenticationMode.CLOUD ? "stable-current-user" : "alice";
            String expectedMode = mode == AuthenticationMode.CLOUD ? "cloud" : "on-prem";

            for (Scope grade : Scope.values()) {
                String credential = userCredential(mode, grade);
                Map<?, ?> body = current(client, mode, credential, origin)
                        .header("X-Cloud-UserId", "forged-current-user")
                        .exchange((request, response) -> {
                            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                            assertThat(response.getHeaders().getCacheControl()).isEqualTo("no-store");
                            assertThat(response.getHeaders().get(HttpHeaders.SET_COOKIE)).isNull();
                            return response.bodyTo(Map.class);
                        });
                List<String> scopes = java.util.Arrays.stream(Scope.values()).filter(grade::permits)
                        .map(scope -> scope.name().toLowerCase(Locale.ROOT)).toList();
                assertThat(body).isEqualTo(Map.of("mode", expectedMode, "principal", principal, "scopes", scopes));
                assertThat(body.toString()).doesNotContain(credential, "forged-current-user", "password",
                        "jwt", "jti", "tokenId", "email", "displayName");
            }

            assertRefused(client.get().uri("/api/auth/me"));
            assertRefused(current(client, mode, "invalid-credential-sentinel", origin));
            assertRefused(client.get().uri("/api/auth/me").header(HttpHeaders.AUTHORIZATION, "Bearer " + MACHINE));
            assertRefused(client.get().uri("/api/auth/me")
                    .header(HttpHeaders.AUTHORIZATION, "TapstateSession tss_current.session-secret-sentinel"));
            if (mode == AuthenticationMode.CLOUD) {
                String cookie = userCredential(mode, Scope.READ);
                // Reads keep the existing same-origin browser policy; Origin matching guards writes.
                current(client, mode, cookie, FOREIGN).exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                    assertThat(response.getHeaders().getAccessControlAllowOrigin()).isNull();
                    return null;
                });
                assertRefused(client.get().uri("/api/auth/me")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + cookie));
                assertRefused(current(client, mode, cookie, origin)
                        .header(HttpHeaders.AUTHORIZATION, "Bearer human-jwt-sentinel"));
                assertRefused(client.get().uri("/api/auth/me").header(HttpHeaders.ORIGIN, origin)
                        .header(HttpHeaders.COOKIE, CloudSessionCookies.NAME + "=" + MACHINE));
            } else {
                assertRefused(client.get().uri("/api/auth/me")
                        .header(HttpHeaders.COOKIE, CloudSessionCookies.NAME + "=" + userCredential(AuthenticationMode.CLOUD, Scope.READ)));
            }
        }
    }

    private static RestClient.RequestHeadersSpec<?> current(RestClient client, AuthenticationMode mode,
            String credential, String origin) {
        var request = client.get().uri("/api/auth/me");
        if (mode == AuthenticationMode.CLOUD) {
            return request.header(HttpHeaders.COOKIE, CloudSessionCookies.NAME + "=" + credential)
                    .header(HttpHeaders.ORIGIN, origin);
        }
        return request.header(HttpHeaders.AUTHORIZATION, "Bearer " + credential);
    }

    private static void assertRefused(RestClient.RequestHeadersSpec<?> request) {
        Map<?, ?> body = request.exchange((input, response) -> {
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
            return response.bodyTo(Map.class);
        });
        assertThat(body.get("code")).isEqualTo("control.unauthenticated");
        assertThat(body.toString()).doesNotContain("secret-sentinel", "invalid-credential-sentinel",
                "human-jwt-sentinel", "stable-current-user", "current-machine");
    }

    private static String userCredential(AuthenticationMode mode, Scope grade) {
        return (mode == AuthenticationMode.CLOUD ? "tcs_current-" : "human-current-")
                + grade.name() + ".credential-secret-sentinel";
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({RestApiConfiguration.class, RestApiSecurityConfiguration.class, ApiExceptionHandler.class})
    @ComponentScan(basePackageClasses = ControlHttpFace.class, useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.REGEX,
                    pattern = "io\\.tapstate\\.control\\.restapi\\.CurrentUserController"))
    static class TestApp {
        @Bean
        AuthenticationMode mode(Environment environment) {
            return AuthenticationMode.valueOf(environment.getRequiredProperty("test.current-user.mode"));
        }

        @Bean
        OperationRegistry registry() {
            return ControlOperations.registry();
        }

        @Bean
        CurrentUserQueryService currentUserQueryService(AuthenticationMode mode) {
            return new CurrentUserQueryService(mode);
        }

        @Bean
        CredentialAuthenticator credentials(AuthenticationMode mode) {
            return new CredentialAuthenticator(presented -> {
                if (mode == AuthenticationMode.ON_PREM && MACHINE.equals(presented)) {
                    return Optional.of(new VerifiedToken("current-machine", Scope.ADMIN));
                }
                for (Scope grade : Scope.values()) {
                    if (userCredential(mode, grade).equals(presented)) {
                        return Optional.of(new VerifiedToken(
                                mode == AuthenticationMode.CLOUD ? "stable-current-user" : "alice", grade));
                    }
                }
                return Optional.empty();
            });
        }
    }
}
