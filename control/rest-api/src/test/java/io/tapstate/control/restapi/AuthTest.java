package io.tapstate.control.restapi;

import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.AccessTokenService;
import io.tapstate.control.core.ArtifactMutationService;
import io.tapstate.control.core.PlanAdvisories;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.BootstrapService;
import io.tapstate.control.core.ConnectionTestResultQueryService;
import io.tapstate.control.core.ConnectionTestService;
import io.tapstate.control.core.ClusterIdentityService;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import io.tapstate.control.core.ConnectorCatalogView;
import io.tapstate.control.core.ConnectorRegisterService;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.CreatedToken;
import io.tapstate.control.core.DataBrowserFollows;
import io.tapstate.control.core.DataBrowserService;
import io.tapstate.control.core.GeneratedSecret;
import io.tapstate.control.core.LoginService;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.PasswordHasher;
import io.tapstate.control.core.PipelineLifecycleService;
import io.tapstate.control.core.PipelineLogQueryService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.SchemaDiscoveryService;
import io.tapstate.control.core.SchemaQueryService;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.SessionService;
import io.tapstate.control.core.SourceService;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.control.core.TokenService;
import io.tapstate.control.core.TokenSigner;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.logging.LogSink;
import io.tapstate.core.logging.RingBufferLogSink;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.runtime.probe.ConnectionProbe;
import io.tapstate.runtime.probe.SchemaDiscoveryProbe;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResultStore;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SessionRecord;
import io.tapstate.spi.store.SessionStore;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.messages.MessageCatalog;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.TokenRecord;
import io.tapstate.spi.store.TokenStore;
import io.tapstate.spi.store.User;
import io.tapstate.spi.store.UserStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.client.RestClient;

import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The authentication and authorization matrix over the {@code /api} verb surface, exercised end to end
 * through a real embedded server (booted programmatically so the module stays on the reactor's JUnit
 * line). It proves Spring Security guards every verb, the two credential mechanisms both authenticate,
 * the grade check refuses an under-scoped caller, and the two pre-authentication entry points (login and
 * the zero-user bootstrap) live outside the guard and self-guard. The stores and the crypto are in-memory
 * fakes; the control-core services and the wiring are real.
 */
class AuthTest {

    /** A follow probe that is never driven: nothing here streams, and one that answered
     * would let a case pass by following instead of reading. */
    private static final io.tapstate.runtime.probe.DataBrowserTailProbe NO_FOLLOWS =
            (config, request, listener) -> {
                throw new AssertionError("no case here opens a follow");
            };

    private static final Instant NOW = Instant.parse("2026-07-08T12:00:00Z");

    private static ConfigurableApplicationContext context;
    private static int port;

    @BeforeAll
    static void startServer() {
        context = new SpringApplicationBuilder(TestApp.class).properties("server.port=0").run();
        port = ((WebServerApplicationContext) context).getWebServer().getPort();
    }

    @AfterAll
    static void stopServer() {
        if (context != null) {
            context.close();
        }
    }

    @BeforeEach
    void resetStores() {
        context.getBean(FakeUserStore.class).clear();
        context.getBean(FakeTokenStore.class).clear();
        context.getBean(FakeSessionStore.class).clear();
        context.getBean(FakeTokenSecrets.class).clear();
        context.getBean(InMemoryArtifactStore.class).clear();
    }

    private RestClient client() {
        return RestClient.create("http://localhost:" + port);
    }

    /** Mints a machine token of the given grade through the real token service and returns the bearer string. */
    private String machineToken(Scope scope) {
        return context.getBean(TokenService.class).create(scope);
    }

    // ---- an unauthenticated caller cannot reach a verb ----

    @Test
    void aVerbWithoutACredentialIsUnauthorized() {
        ApiError body = client().get().uri("/api/artifacts")
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                    assertThat(response.getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE)).isNull();
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.unauthenticated");
        // The refusal echoes nothing back, so it cannot be used to probe which credentials exist.
        assertThat(body.params()).isEmpty();
    }

    @Test
    void anInvalidCredentialIsUnauthorized() {
        ApiError body = client().get().uri("/api/artifacts")
                .header("Authorization", "Bearer not-a-real-token")
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.unauthenticated");
    }

    @Test
    void aUserSessionSchemeCanNeverAuthenticateTheApiBearerSurface() {
        ApiError body = client().get().uri("/api/artifacts")
                .header("Authorization", "TapstateSession tss_s01.session-secret")
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.unauthenticated");
        assertThat(body.params()).isEmpty();
    }

    // ---- the grade check refuses an under-scoped caller but admits a sufficient one ----

    @Test
    void aReadCredentialIsForbiddenFromAWriteVerb() {
        String readToken = machineToken(Scope.READ);

        ApiError body = client().post().uri("/api/artifacts:apply")
                .header("Authorization", "Bearer " + readToken)
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("drafts", List.of(Map.of("content", SOURCE))))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.forbidden");
        assertThat(body.params()).containsEntry("op", "artifact.apply").containsEntry("required", "write");
    }

    @Test
    void aReadCredentialMayReadAVerb() {
        HttpStatusCode status = client().get().uri("/api/artifacts")
                .header("Authorization", "Bearer " + machineToken(Scope.READ))
                .exchange((request, response) -> response.getStatusCode());

        assertThat(status).isEqualTo(HttpStatus.OK);
    }

    @Test
    void aWriteCredentialMayApply() {
        HttpStatusCode status = client().post().uri("/api/artifacts:apply")
                .header("Authorization", "Bearer " + machineToken(Scope.WRITE))
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("drafts", List.of(Map.of("content", SOURCE))))
                .exchange((request, response) -> response.getStatusCode());

        assertThat(status).isEqualTo(HttpStatus.OK);
    }

    @Test
    void anAdminCanCreateListAndRevokeASecretFreeMachineToken() {
        String admin = machineToken(Scope.ADMIN);

        Map<?, ?> created = client().post().uri("/api/tokens")
                .header("Authorization", "Bearer " + admin)
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("scope", "write"))
                .retrieve().body(Map.class);

        String tokenId = String.valueOf(created.get("tokenId"));
        String token = String.valueOf(created.get("token"));
        assertThat(tokenId).isNotBlank();
        assertThat(token).startsWith("cyxt_" + tokenId + ".");
        assertThat(created.get("scope")).isEqualTo("WRITE");

        String listed = client().get().uri("/api/tokens")
                .header("Authorization", "Bearer " + admin)
                .retrieve().body(String.class);
        assertThat(listed).contains(tokenId).contains("WRITE");
        assertThat(listed)
                .doesNotContain(token)
                .doesNotContain("secretHash")
                .doesNotContain("hash:");

        HttpStatusCode revoked = client().post().uri("/api/tokens/{id}:revoke", tokenId)
                .header("Authorization", "Bearer " + admin)
                .exchange((request, response) -> response.getStatusCode());
        assertThat(revoked).isEqualTo(HttpStatus.NO_CONTENT);
        HttpStatusCode revokedAgain = client().post().uri("/api/tokens/{id}:revoke", tokenId)
                .header("Authorization", "Bearer " + admin)
                .exchange((request, response) -> response.getStatusCode());
        assertThat(revokedAgain).isEqualTo(HttpStatus.NO_CONTENT);

        HttpStatusCode afterRevoke = client().get().uri("/api/artifacts")
                .header("Authorization", "Bearer " + token)
                .exchange((request, response) -> response.getStatusCode());
        assertThat(afterRevoke).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void tokenAdministrationRequiresAdminScopeAndRejectsAnUnknownScope() {
        HttpStatusCode forbidden = client().get().uri("/api/tokens")
                .header("Authorization", "Bearer " + machineToken(Scope.WRITE))
                .exchange((request, response) -> response.getStatusCode());
        assertThat(forbidden).isEqualTo(HttpStatus.FORBIDDEN);

        ApiError malformed = client().post().uri("/api/tokens")
                .header("Authorization", "Bearer " + machineToken(Scope.ADMIN))
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("scope", "owner"))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    return response.bodyTo(ApiError.class);
                });
        assertThat(malformed.code()).isEqualTo("control.malformed-request");
    }

    @Test
    void theBearerSchemeIsMatchedCaseInsensitively() {
        // The auth-scheme name is case-insensitive (RFC 7235), so a lowercase "bearer" carries a valid credential.
        HttpStatusCode status = client().get().uri("/api/artifacts")
                .header("Authorization", "bearer " + machineToken(Scope.READ))
                .exchange((request, response) -> response.getStatusCode());

        assertThat(status).isEqualTo(HttpStatus.OK);
    }

    @Test
    void malformedAndDuplicateBearerHeadersAreTheSameCodedUnauthenticatedRefusal() {
        String token = machineToken(Scope.READ);

        ApiError malformed = client().get().uri("/api/artifacts")
                .header("Authorization", "Bearer  " + token)
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                    return response.bodyTo(ApiError.class);
                });
        ApiError duplicate = client().get().uri("/api/artifacts")
                .header("Authorization", "Bearer " + token, "Bearer " + token)
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(malformed.code()).isEqualTo("control.unauthenticated");
        assertThat(duplicate.code()).isEqualTo("control.unauthenticated");
    }

    @Test
    void unknownApiRoutesReachMvcWhileUnknownRootRoutesRemainUnauthorized() {
        HttpStatusCode unknownApi = client().get().uri("/api/not-a-control-verb")
                .exchange((request, response) -> response.getStatusCode());
        ApiError unknownRoot = client().get().uri("/not-a-public-endpoint")
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(unknownApi).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(unknownRoot.code()).isEqualTo("control.unauthenticated");

        HttpStatusCode wrongMethod = client().put().uri("/api/sources")
                .header("Authorization", "Bearer " + machineToken(Scope.ADMIN))
                .exchange((request, response) -> response.getStatusCode());
        assertThat(wrongMethod).isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
    }

    @Test
    void theControlFaceUsesTwoStatelessSecurityChainsInsteadOfTheLegacyInterceptor() {
        assertThat(context.getBeansOfType(SecurityFilterChain.class)).hasSize(2);
    }

    @Test
    void securityConfigurationRefusesToStartWithoutTheHumanCredentialVerifier() {
        assertThatThrownBy(() -> new SpringApplicationBuilder(MissingHumanVerifierApp.class)
                .properties("server.port=0")
                .run())
                .hasStackTraceContaining("TokenSigner");
    }

    // ---- login is a pre-authentication entry point that issues a working credential ----

    @Test
    void loginIssuesASessionTokenThatAuthenticatesAVerb() {
        seedUser("alice", "s3cret", "admin");

        LoginResponse login = client().post().uri("/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new LoginRequest("alice", "s3cret"))
                .retrieve().toEntity(LoginResponse.class).getBody();

        assertThat(login.token()).isNotBlank();
        // The minted session token authenticates a verb on the guarded surface.
        HttpStatusCode status = client().get().uri("/api/artifacts")
                .header("Authorization", "Bearer " + login.token())
                .exchange((request, response) -> response.getStatusCode());
        assertThat(status).isEqualTo(HttpStatus.OK);
    }

    @Test
    void legacyAndAccessOnlyLoginDoNotCreateAPersistentSession() {
        seedUser("alice", "s3cret", "admin");

        LoginResponse legacy = client().post().uri("/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new LoginRequest("alice", "s3cret"))
                .retrieve().toEntity(LoginResponse.class).getBody();
        LoginResponse accessOnly = client().post().uri("/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new LoginRequest("alice", "s3cret", false))
                .retrieve().toEntity(LoginResponse.class).getBody();

        assertThat(legacy.token()).isNotBlank();
        assertThat(accessOnly.token()).isNotBlank();
        assertThat(legacy.accessExpiresAt()).isEqualTo("2026-07-08T12:15:00Z");
        assertThat(accessOnly.accessExpiresAt()).isEqualTo("2026-07-08T12:15:00Z");
        assertThat(legacy.sessionToken()).isNull();
        assertThat(legacy.sessionIdleExpiresAt()).isNull();
        assertThat(legacy.sessionAbsoluteExpiresAt()).isNull();
        assertThat(accessOnly.sessionToken()).isNull();
        assertThat(accessOnly.sessionIdleExpiresAt()).isNull();
        assertThat(accessOnly.sessionAbsoluteExpiresAt()).isNull();
    }

    @Test
    void persistentLoginCreatesAnOpaqueSessionThatCanMintAndRevokeAccessTokens() {
        seedUser("alice", "s3cret", "admin");

        LoginResponse login = client().post().uri("/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new LoginRequest("alice", "s3cret", true))
                .retrieve().toEntity(LoginResponse.class).getBody();

        assertThat(login.token()).isEqualTo("alice|ADMIN");
        assertThat(login.accessExpiresAt()).isEqualTo("2026-07-08T12:15:00Z");
        assertThat(login.issuer()).isEqualTo("urn:tapstate:cluster:01J5AUTHFIXTURE");
        assertThat(login.principal()).isEqualTo("alice");
        assertThat(login.scopes()).containsExactly("read", "write", "admin");
        assertThat(login.sessionToken()).isEqualTo("tss_tok-1.sec-1");
        assertThat(login.sessionIdleExpiresAt()).isEqualTo("2026-08-07T12:00:00Z");
        assertThat(login.sessionAbsoluteExpiresAt()).isEqualTo("2026-10-06T12:00:00Z");

        SessionExchangeResponse exchanged = client().post().uri(AuthWire.SESSION_PATH)
                .header(HttpHeaders.AUTHORIZATION, AuthWire.sessionAuthorization(login.sessionToken()))
                .retrieve().toEntity(SessionExchangeResponse.class).getBody();
        assertThat(exchanged.token()).isEqualTo("alice|ADMIN");
        assertThat(exchanged.accessExpiresAt()).isEqualTo("2026-07-08T12:15:00Z");
        assertThat(exchanged.issuer()).isEqualTo("urn:tapstate:cluster:01J5AUTHFIXTURE");
        assertThat(exchanged.principal()).isEqualTo("alice");
        assertThat(exchanged.scopes()).containsExactly("read", "write", "admin");

        HttpStatusCode logout = client().post().uri(AuthWire.LOGOUT_PATH)
                .header(HttpHeaders.AUTHORIZATION, AuthWire.sessionAuthorization(login.sessionToken()))
                .exchange((request, response) -> response.getStatusCode());
        HttpStatusCode logoutAgain = client().post().uri(AuthWire.LOGOUT_PATH)
                .header(HttpHeaders.AUTHORIZATION, AuthWire.sessionAuthorization(login.sessionToken()))
                .exchange((request, response) -> response.getStatusCode());
        ApiError afterLogout = client().post().uri(AuthWire.SESSION_PATH)
                .header(HttpHeaders.AUTHORIZATION, AuthWire.sessionAuthorization(login.sessionToken()))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(logout).isEqualTo(HttpStatus.NO_CONTENT);
        assertThat(logoutAgain).isEqualTo(HttpStatus.NO_CONTENT);
        assertThat(afterLogout.code()).isEqualTo("control.unauthenticated");
    }

    @Test
    void sessionExchangeAcceptsOnlyTheTapstateSessionScheme() {
        seedUser("alice", "s3cret", "admin");
        LoginResponse login = client().post().uri("/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new LoginRequest("alice", "s3cret", true))
                .retrieve().toEntity(LoginResponse.class).getBody();

        ApiError bearer = client().post().uri(AuthWire.SESSION_PATH)
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + login.sessionToken())
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(bearer.code()).isEqualTo("control.unauthenticated");

        ApiError sessionAsApiBearer = client().get().uri("/api/artifacts")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + login.sessionToken())
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                    return response.bodyTo(ApiError.class);
                });
        assertThat(sessionAsApiBearer.code()).isEqualTo("control.unauthenticated");
    }

    @Test
    void sessionEndpointsRejectMalformedDuplicateUnknownAndIssuerMismatchedCredentials() {
        seedUser("alice", "s3cret", "admin");
        LoginResponse login = client().post().uri("/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new LoginRequest("alice", "s3cret", true))
                .retrieve().toEntity(LoginResponse.class).getBody();

        assertSessionUnauthorized(AuthWire.SESSION_PATH, "TapstateSession  " + login.sessionToken());
        SessionExchangeResponse differentlyCasedScheme = client().post().uri(AuthWire.SESSION_PATH)
                .header(HttpHeaders.AUTHORIZATION, "tapstatesession " + login.sessionToken())
                .retrieve().toEntity(SessionExchangeResponse.class).getBody();
        assertThat(differentlyCasedScheme.token()).isNotBlank();
        assertSessionUnauthorized(AuthWire.SESSION_PATH, "TapstateSession missing.secret");
        ApiError duplicate = client().post().uri(AuthWire.SESSION_PATH)
                .header(HttpHeaders.AUTHORIZATION,
                        AuthWire.sessionAuthorization(login.sessionToken()),
                        AuthWire.sessionAuthorization(login.sessionToken()))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                    return response.bodyTo(ApiError.class);
                });
        assertThat(duplicate.code()).isEqualTo("control.unauthenticated");

        context.getBean(FakeSessionStore.class).replaceIssuer("tok-1", "urn:other");
        assertSessionUnauthorized(AuthWire.SESSION_PATH, AuthWire.sessionAuthorization(login.sessionToken()));
        assertSessionUnauthorized(AuthWire.LOGOUT_PATH, AuthWire.sessionAuthorization(login.sessionToken()));
    }

    private void assertSessionUnauthorized(String path, String authorization) {
        ApiError error = client().post().uri(path)
                .header(HttpHeaders.AUTHORIZATION, authorization)
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                    return response.bodyTo(ApiError.class);
                });
        assertThat(error.code()).isEqualTo("control.unauthenticated");
    }

    @Test
    void aHumanJwtCarriesItsPrincipalIntoAnAuditedWriteWithoutCreatingAServletSession() {
        seedUser("alice", "s3cret", "admin");
        LoginResponse login = client().post().uri("/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new LoginRequest("alice", "s3cret"))
                .retrieve().toEntity(LoginResponse.class).getBody();

        ResponseEntity<CreatedToken> created = client().post().uri("/api/tokens")
                .header("Authorization", "Bearer " + login.token())
                .contentType(MediaType.APPLICATION_JSON)
                .body(new TokenCreateRequest("read"))
                .retrieve().toEntity(CreatedToken.class);

        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(created.getBody().token()).startsWith("cyxt_");
        assertThat(created.getHeaders().get("Set-Cookie")).isNull();
    }

    @Test
    void loginWithAWrongPasswordIsUnauthorizedWithoutLeak() {
        seedUser("alice", "s3cret", "admin");

        ApiError body = client().post().uri("/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new LoginRequest("alice", "wrong"))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.auth-failed");
        assertThat(body.params()).as("the login failure reveals nothing about which half was wrong").isEmpty();
    }

    @Test
    void loginWithABlankCredentialIsABadRequestWithACodedBody() {
        // A missing / blank credential field is a malformed request — the client omitted a required value —
        // refused at the boundary with a coded control.malformed-request (400). This is distinct from a
        // present-but-wrong credential (the auth-failed 401), and it never reaches the login service's bare
        // argument crash.
        ApiError body = client().post().uri("/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new LoginRequest("", "s3cret"))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.malformed-request");
        assertThat(body.params()).containsKey("reason");
    }

    // ---- the zero-user bootstrap channel: loopback-only, one-shot ----

    @Test
    void bootstrapCreatesTheFirstAdminFromLoopbackThenLetsItSignIn() {
        // The test client connects over loopback, so the bootstrap channel accepts it on the empty store.
        HttpStatusCode created = client().post().uri("/auth/bootstrap")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new BootstrapRequest("root", "s3cret"))
                .exchange((request, response) -> response.getStatusCode());
        assertThat(created).isEqualTo(HttpStatus.NO_CONTENT);

        // The first admin now exists and can sign in.
        LoginResponse login = client().post().uri("/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new LoginRequest("root", "s3cret"))
                .retrieve().toEntity(LoginResponse.class).getBody();
        assertThat(login.token()).isNotBlank();
    }

    @Test
    void bootstrapIsClosedOnceAUserExists() {
        seedUser("existing", "pw", "admin");

        ApiError body = client().post().uri("/auth/bootstrap")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new BootstrapRequest("root", "s3cret"))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.bootstrap-closed");
    }

    @Test
    void bootstrapWithABlankCredentialIsABadRequestWithACodedBody() {
        // The test client is on loopback and the store is empty, so the bootstrap channel is open. A blank
        // password is still refused at the boundary with a coded control.malformed-request (400) — it would
        // otherwise be hashed into a non-blank hash and silently create an admin with an empty password.
        ApiError body = client().post().uri("/auth/bootstrap")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new BootstrapRequest("root", ""))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    return response.bodyTo(ApiError.class);
                });

        assertThat(body.code()).isEqualTo("control.malformed-request");
        assertThat(body.params()).containsKey("reason");
    }

    // ---- the liveness probe and discovery stay anonymous; topology does not ----

    @Test
    void theLivenessProbeStaysAnonymous() {
        String probe = client().get().uri("/healthz").retrieve().body(String.class);
        assertThat(probe).isEqualTo("ok");
    }

    @Test
    void issuerDiscoveryStaysAnonymous() {
        IssuerDiscoveryResponse discovery = client().get().uri(AuthWire.DISCOVERY_PATH)
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                    return response.bodyTo(IssuerDiscoveryResponse.class);
                });

        assertThat(discovery).isEqualTo(new IssuerDiscoveryResponse(
                "urn:tapstate:cluster:01J5AUTHFIXTURE",
                "01J5AUTHFIXTURE",
                "tapstate/v1",
                List.of("password", "machine_token")));
    }

    @Test
    void topologyIsBehindAuthenticationNotAnonymous() {
        // cluster.members is a read verb reserved as a 501 stub; the point here is that it is reached only
        // after authentication — an anonymous caller is turned away at the interceptor with 401, never 501.
        HttpStatusCode anonymous = client().get().uri("/api/cluster/members")
                .exchange((request, response) -> response.getStatusCode());
        assertThat(anonymous).isEqualTo(HttpStatus.UNAUTHORIZED);

        HttpStatusCode authenticated = client().get().uri("/api/cluster/members")
                .header("Authorization", "Bearer " + machineToken(Scope.READ))
                .exchange((request, response) -> response.getStatusCode());
        assertThat(authenticated).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
    }

    // ---- the unauthenticated surface is a closed allow-list (root-exit convergence) ----

    @Test
    void theOnlyEndpointsOutsideTheApiPrefixAreTheProbeAndThePreAuthEntryPoints() {
        // /api is the authenticated boundary (the API security chain guards /api/**). Everything reachable
        // anonymously must therefore be an intentional carve-out: the liveness probe, issuer discovery,
        // the two pre-auth entry points, and the framework's own error endpoint (which renders only the
        // current request's error, no application data). A future plain @Controller added at the root would
        // escape both the verb-derivation gate and the interceptor — this pins the anonymous surface to
        // exactly that set.
        Set<String> allowedRootPaths = Set.of("/healthz", AuthWire.DISCOVERY_PATH, "/auth/login",
                AuthWire.SESSION_PATH, AuthWire.LOGOUT_PATH, "/auth/bootstrap", "/error");

        RequestMappingHandlerMapping mapping =
                context.getBean("requestMappingHandlerMapping", RequestMappingHandlerMapping.class);

        List<String> unexpectedRootEndpoints = new ArrayList<>();
        mapping.getHandlerMethods().forEach((info, handler) -> {
            Set<String> patterns = info.getPathPatternsCondition() == null
                    ? Set.of()
                    : info.getPathPatternsCondition().getPatternValues();
            for (String pattern : patterns) {
                // Mirror the API security chain's /api/** exactly (segment-based), so a sibling like /api-internal
                // is treated as outside the guarded surface and must be allow-listed too, not slipped through
                // by a loose string prefix.
                boolean underApi = pattern.equals("/api") || pattern.startsWith("/api/");
                if (!underApi && !allowedRootPaths.contains(pattern)) {
                    unexpectedRootEndpoints.add(describe(handler) + " -> " + pattern);
                }
            }
        });

        assertThat(unexpectedRootEndpoints)
                .as("only the liveness probe, issuer discovery, and the pre-auth entry points may live outside /api; every "
                        + "other endpoint is a registry verb under the authenticated /api prefix")
                .isEmpty();
    }

    private static String describe(HandlerMethod handler) {
        return handler.getBeanType().getSimpleName() + "#" + handler.getMethod().getName();
    }

    private void seedUser(String username, String password, String role) {
        FakeHasher hasher = context.getBean(FakeHasher.class);
        context.getBean(FakeUserStore.class).save(new User(username, hasher.hash(password), role));
    }

    /** A source draft the apply verb accepts, so the write-path test reaches a real outcome. */
    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_ora
            connector: oracle
            config: { host: 10.20.0.15 }
            """;

    /**
     * A minimal boot config: auto-configures Web MVC + the embedded servlet container, imports the whole
     * HTTP control face as one bundle ({@link ControlHttpFace} — path prefix, security chains,
     * every verb controller, the pre-auth controller, the probe, and the advice),
     * and constructs the real control-core services over in-memory fakes. Importing the bundle rather than
     * the parts means this test exercises the exact assembly the running server uses.
     */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({ControlHttpFace.class, SourceDraftTestConfiguration.class, SourceServiceTestConfiguration.class,
            AuditedSourceServiceTestConfiguration.class})
    static class TestApp {

        @Bean
        Clock clock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }

        @Bean
        FakeUserStore userStore() {
            return new FakeUserStore();
        }

        @Bean
        FakeTokenStore tokenStore() {
            return new FakeTokenStore();
        }

        @Bean
        FakeSessionStore sessionStore() {
            return new FakeSessionStore();
        }

        @Bean
        AuditStore auditStore() {
            // A no-op is enough here; the "no audit, no execute" gate contract is proven in control-core.
            return entry -> {
            };
        }

        @Bean
        InMemoryArtifactStore artifactStore() {
            return new InMemoryArtifactStore();
        }

        @Bean
        FakeHasher passwordHasher() {
            return new FakeHasher();
        }

        @Bean
        FakeTokenSecrets tokenSecrets() {
            return new FakeTokenSecrets();
        }

        @Bean
        TokenSigner tokenSigner() {
            return new FakeSigner();
        }

        @Bean
        ClusterIdentityService clusterIdentityService() {
            ClusterIdentity identity = new ClusterIdentity("01J5AUTHFIXTURE");
            ClusterIdentityStore store = new ClusterIdentityStore() {
                @Override
                public Optional<ClusterIdentity> find() {
                    return Optional.of(identity);
                }

                @Override
                public ClusterIdentity createIfAbsent(ClusterIdentity proposed) {
                    return identity;
                }
            };
            return new ClusterIdentityService(store, () -> "unused");
        }

        @Bean
        TokenService tokenService(TokenStore store, TokenSecrets secrets, Clock clock) {
            return new TokenService(store, secrets, clock);
        }

        @Bean
        AccessTokenService accessTokenService(TokenSigner signer, Clock clock) {
            return new AccessTokenService(signer, clock);
        }

        @Bean
        SessionService sessionService(
                SessionStore store, TokenSecrets secrets, AccessTokenService accessTokens, Clock clock) {
            return new SessionService(store, secrets, accessTokens, clock);
        }

        @Bean
        LoginService loginService(UserStore users, PasswordHasher hasher, AccessTokenService accessTokens) {
            return new LoginService(users, hasher, accessTokens);
        }

        @Bean
        AuditGate auditGate(AuditStore store, Clock clock) {
            return new AuditGate(store, clock);
        }

        @Bean
        BootstrapService bootstrapService(UserStore users, PasswordHasher hasher, AuditGate gate) {
            return new BootstrapService(users, hasher, gate);
        }

        @Bean
        OperationRegistry operationRegistry() {
            return ControlOperations.registry();
        }

        @Bean
        CredentialAuthenticator credentialAuthenticator(TokenService tokens, TokenSigner signer) {
            return new CredentialAuthenticator(tokens, signer);
        }

        @Bean
        ApplyService applyService(InMemoryArtifactStore store, AuditGate auditGate) {
            return new ApplyService(TapstateCatalog::load, store, auditGate, new EmptySchemaStore(),
                    PlanAdvisories.none());
        }

        @Bean
        ArtifactQueryService artifactQueryService(InMemoryArtifactStore store) {
            return new ArtifactQueryService(store);
        }

        // The removal controller comes in with the whole ControlHttpFace bundle, so its service must be
        // present for the context to stand up. This suite exercises the auth matrix, not the removal, so
        // the dependent stores refuse rather than pretend: a reclaim reached from here is a defect.
        @Bean
        ArtifactMutationService artifactMutationService(InMemoryArtifactStore store, AuditGate auditGate) {
            return new ArtifactMutationService(
                    store, NoReclaimStores.desired(), NoReclaimStores.state(),
                    NoReclaimStores.observations(), NoReclaimStores.srsMeta(), auditGate, DataBrowserFollows.NONE);
        }

        // The connection-test controller comes in with the whole ControlHttpFace bundle, so its service must
        // be present for the context to stand up. This suite exercises the auth matrix, not the probe, so the
        // service only needs to construct — its probe and result store are inert.
        @Bean
        ConnectionTestService connectionTestService(AuditGate auditGate) {
            ConnectionProbe probe = config -> {
                throw new UnsupportedOperationException("connection.test is not exercised in this test");
            };
            ConnectionTestResultStore resultStore = new ConnectionTestResultStore() {
                @Override
                public void save(ConnectionTestResult result) {
                }

                @Override
                public Optional<ConnectionTestResult> find(String connectionId) {
                    return Optional.empty();
                }
            };
            return new ConnectionTestService(probe, resultStore, auditGate);
        }

        // The read-back controller is bundled too, so its query service must be present for the context to
        // stand up; this suite exercises the auth matrix, not the read, so the store is inert (empty).
        @Bean
        ConnectionTestResultQueryService connectionTestResultQueryService() {
            return new ConnectionTestResultQueryService(new ConnectionTestResultStore() {
                @Override
                public void save(ConnectionTestResult result) {
                }

                @Override
                public Optional<ConnectionTestResult> find(String connectionId) {
                    return Optional.empty();
                }
            });
        }

        // The discover-schema controller methods are bundled with the same controller, so their services
        // must be present for the context to stand up; their behaviour is proven in ConnectionApiTest, so
        // here they only need to construct (inert probe, empty store).
        @Bean
        SchemaDiscoveryService schemaDiscoveryService(AuditGate auditGate) {
            SchemaDiscoveryProbe probe = config -> {
                throw new UnsupportedOperationException("connection.discover-schema is not exercised in this test");
            };
            return new SchemaDiscoveryService(probe, new SchemaStore() {
                @Override
                public void save(DiscoveredSourceModel discovered) {
                }

                @Override
                public Optional<DiscoveredSourceModel> get(String connectionId) {
                    return Optional.empty();
                }
            }, auditGate, Clock.systemUTC());
        }

        @Bean
        SchemaQueryService schemaQueryService() {
            return new SchemaQueryService(new SchemaStore() {
                @Override
                public void save(DiscoveredSourceModel discovered) {
                }

                @Override
                public Optional<DiscoveredSourceModel> get(String connectionId) {
                    return Optional.empty();
                }
            });
        }

        // The three data-browser controller methods are bundled too, so their service must be present for
        // the context to stand up; this suite exercises the auth matrix, not the reads, so every probe is
        // inert (their behaviour is proven in DataBrowserApiTest).
        @Bean
        DataBrowserService dataBrowserService(InMemoryArtifactStore store) {
            return new DataBrowserService(
                    store,
                    new NoDiscoveries(),
                    config -> {
                        throw new UnsupportedOperationException(
                                "data-browser.collections is not exercised in this test");
                    },
                    (config, collection) -> {
                        throw new UnsupportedOperationException(
                                "data-browser.stats is not exercised in this test");
                    },
                    (config, query) -> {
                        throw new UnsupportedOperationException(
                                "data-browser.find is not exercised in this test");
                    }, NO_FOLLOWS);
        }

        // The connector register controller is bundled with the whole ControlHttpFace, so its service must be
        // present for the context to stand up; this suite exercises the auth matrix, not registration, so the
        // registrar is inert (never driven).
        @Bean
        ConnectorRegisterService connectorRegisterService(AuditGate auditGate) {
            io.tapstate.spi.store.ConnectorRegistrar registrar = (artifact, source) -> {
                throw new UnsupportedOperationException("connector.register is not exercised in this test");
            };
            return new ConnectorRegisterService(registrar, auditGate);
        }

        // The connector list controller comes in with the whole ControlHttpFace bundle, so its view must be
        // present for the context to stand up; this suite exercises the auth matrix, not the listing, so the
        // catalog store is inert (empty).
        @Bean
        ConnectorCatalogView connectorCatalogView() {
            io.tapstate.spi.store.ConnectorCatalogStore store = new io.tapstate.spi.store.ConnectorCatalogStore() {
                @Override
                public void upsert(io.tapstate.core.catalog.ConnectorCatalogEntry entry) {
                }

                @Override
                public Optional<io.tapstate.core.catalog.ConnectorCatalogEntry> get(String connectorId) {
                    return Optional.empty();
                }

                @Override
                public List<io.tapstate.core.catalog.ConnectorCatalogEntry> list() {
                    return List.of();
                }
            };
            return new ConnectorCatalogView(
                    TapstateCatalog.load(), store, emptySpecStore(), emptyConnectorRegistry());
        }

        @Bean
        DesiredStore desiredStore() {
            return new FakeDesiredStore();
        }

        @Bean
        PipelineLifecycleService pipelineLifecycleService(
                ArtifactQueryService artifacts, DesiredStore desired, AuditGate auditGate) {
            return new PipelineLifecycleService(artifacts, desired, auditGate);
        }

        @Bean
        ObservationStore observationStore() {
            // No read-face assertion here; the empty store is enough to bring the read controller up so the
            // full-face bundle boots. The read faces themselves are proven in PipelineObservationApiTest.
            return new ObservationStore() {
                @Override
                public void delete(String pipelineId) {
                    throw new UnsupportedOperationException("removal is not exercised by this double");
                }

                @Override
                public void save(Observation observation) {
                }

                @Override
                public Optional<Observation> read(String pipelineId) {
                    return Optional.empty();
                }
            };
        }

        @Bean
        PipelineObservationQueryService pipelineObservationQueryService(ObservationStore observations) {
            return new PipelineObservationQueryService(new ArtifactQueryService(appliedPipelines()), observations);
        }

        @Bean
        LogSink logSink() {
            // No logs assertion here; an empty node-local sink is enough to bring the logs controller up so
            // the full-face bundle boots. The logs face is proven in PipelineLogsApiTest.
            return new RingBufferLogSink(64, 200);
        }

        @Bean
        PipelineLogQueryService pipelineLogQueryService(LogSink sink) {
            return new PipelineLogQueryService(sink);
        }

    }

    /** Minimal production-security assembly that deliberately omits the human verifier. */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import(RestApiSecurityConfiguration.class)
    static class MissingHumanVerifierApp {

        @Bean
        MessageCatalog messageCatalog() {
            return MessageCatalog.bundled();
        }

        @Bean
        OperationRegistry operationRegistry() {
            return ControlOperations.registry();
        }

        @Bean
        TokenService tokenService() {
            return new TokenService(new FakeTokenStore(), new FakeTokenSecrets(), Clock.systemUTC());
        }
    }

    // ---- fakes ----

    /** An in-memory user store keyed by username. */
    private static final class FakeUserStore implements UserStore {
        private final Map<String, User> users = new LinkedHashMap<>();

        void clear() {
            users.clear();
        }

        @Override
        public Optional<User> find(String username) {
            return Optional.ofNullable(users.get(username));
        }

        @Override
        public void save(User user) {
            users.put(user.username(), user);
        }

        @Override
        public boolean isEmpty() {
            return users.isEmpty();
        }
    }

    /** An in-memory token store keyed by token id. */
    private static final class FakeTokenStore implements TokenStore {
        private final Map<String, TokenRecord> byId = new LinkedHashMap<>();

        void clear() {
            byId.clear();
        }

        @Override
        public void save(TokenRecord record) {
            byId.put(record.tokenId(), record);
        }

        @Override
        public Optional<TokenRecord> find(String tokenId) {
            return Optional.ofNullable(byId.get(tokenId));
        }

        @Override
        public void revoke(String tokenId) {
            TokenRecord existing = byId.get(tokenId);
            if (existing != null) {
                byId.put(tokenId, new TokenRecord(existing.tokenId(), existing.scope(),
                        existing.secretHash(), true, existing.createdAt()));
            }
        }

        @Override
        public List<TokenRecord> list() {
            return new ArrayList<>(byId.values());
        }
    }

    private static final class FakeSessionStore implements SessionStore {
        private final Map<String, SessionRecord> byId = new LinkedHashMap<>();

        synchronized void clear() {
            byId.clear();
        }

        synchronized void replaceIssuer(String id, String issuer) {
            SessionRecord record = byId.get(id);
            byId.put(id, new SessionRecord(record.sessionId(), record.secretHash(), record.principal(),
                    record.scope(), issuer, record.revoked(), record.createdAt(), record.lastUsedAt(),
                    record.idleExpiresAt(), record.absoluteExpiresAt()));
        }

        @Override
        public synchronized void save(SessionRecord record) {
            byId.put(record.sessionId(), record);
        }

        @Override
        public synchronized Optional<SessionRecord> find(String sessionId) {
            return Optional.ofNullable(byId.get(sessionId));
        }

        @Override
        public synchronized Optional<SessionRecord> exchange(
                String sessionId, String secretHash, String issuer, Instant now, Instant idleExpiresAt) {
            SessionRecord record = byId.get(sessionId);
            if (!valid(record, secretHash, issuer, now) || record.revoked()) {
                return Optional.empty();
            }
            SessionRecord touched = new SessionRecord(record.sessionId(), record.secretHash(), record.principal(),
                    record.scope(), record.issuer(), false, record.createdAt(), now,
                    idleExpiresAt.isBefore(record.absoluteExpiresAt()) ? idleExpiresAt : record.absoluteExpiresAt(),
                    record.absoluteExpiresAt());
            byId.put(sessionId, touched);
            return Optional.of(touched);
        }

        @Override
        public synchronized boolean revoke(String sessionId, String secretHash, String issuer, Instant now) {
            SessionRecord record = byId.get(sessionId);
            if (!valid(record, secretHash, issuer, now)) {
                return false;
            }
            byId.put(sessionId, new SessionRecord(record.sessionId(), record.secretHash(), record.principal(),
                    record.scope(), record.issuer(), true, record.createdAt(), record.lastUsedAt(),
                    record.idleExpiresAt(), record.absoluteExpiresAt()));
            return true;
        }

        private static boolean valid(SessionRecord record, String secretHash, String issuer, Instant now) {
            return record != null && record.secretHash().equals(secretHash) && record.issuer().equals(issuer)
                    && record.idleExpiresAt().isAfter(now) && record.absoluteExpiresAt().isAfter(now);
        }
    }

    /** A deterministic password hasher (hash:raw). */
    private static final class FakeHasher implements PasswordHasher {
        @Override
        public String hash(String raw) {
            return "hash:" + raw;
        }

        @Override
        public boolean matches(String raw, String storedHash) {
            return storedHash.equals(hash(raw));
        }
    }

    /** A deterministic secret minter: tok-N / sec-N with a reversible hash. */
    private static final class FakeTokenSecrets implements TokenSecrets {
        private int counter;

        void clear() {
            counter = 0;
        }

        @Override
        public GeneratedSecret generate() {
            counter++;
            return new GeneratedSecret("tok-" + counter, "sec-" + counter, "hash:sec-" + counter);
        }

        @Override
        public String hash(String presentedSecret) {
            return "hash:" + presentedSecret;
        }

        @Override
        public boolean matches(String presentedSecret, String storedHash) {
            return storedHash.equals("hash:" + presentedSecret);
        }
    }

    /** A signer whose token is a reversible {@code subject|SCOPE} encoding, so a session token round-trips. */
    private static final class FakeSigner implements TokenSigner {
        @Override
        public String issue(String subject, Scope scope) {
            return subject + "|" + scope.name();
        }

        @Override
        public Optional<VerifiedToken> verify(String token) {
            int bar = token.indexOf('|');
            if (bar < 0) {
                return Optional.empty();
            }
            return Optional.of(new VerifiedToken(token.substring(0, bar), Scope.valueOf(token.substring(bar + 1))));
        }
    }

    /** An in-memory desired store, last write wins per pipeline id — enough to bring up the lifecycle service. */
    private static final class FakeDesiredStore implements DesiredStore {
        @Override
        public void delete(String pipelineId) {
            throw new UnsupportedOperationException("removal is not exercised by this double");
        }

        private final Map<String, DesiredState> byId = new LinkedHashMap<>();

        @Override
        public void save(DesiredState desired) {
            byId.put(desired.pipelineId(), desired);
        }

        @Override
        public Optional<DesiredState> read(String pipelineId) {
            return Optional.ofNullable(byId.get(pipelineId));
        }

        @Override
        public List<String> pipelineIds() {
            return List.copyOf(byId.keySet());
        }
    }

    /**
     * An in-memory {@link io.tapstate.spi.store.ArtifactStore} holding each artifact as its canonical text,
     * mirroring the real store's write-then-parse round-trip so the apply / list verbs behave as in production.
     */
    private static final class InMemoryArtifactStore implements io.tapstate.spi.store.ArtifactStore {
        private final CanonicalWriter writer = new CanonicalWriter();
        private final DslParser parser = new DslParser();
        private final Map<String, String> byId = new LinkedHashMap<>();

        void clear() {
            byId.clear();
        }

        @Override
        public void saveAll(List<Resource> artifacts) {
            Map<String, String> staged = new LinkedHashMap<>();
            for (Resource artifact : artifacts) {
                staged.put(artifact.id(), writer.write(artifact));
            }
            byId.putAll(staged);
        }

        @Override
        public Optional<Resource> get(String id) {
            String canonical = byId.get(id);
            return canonical == null ? Optional.empty() : Optional.of(parser.parse(canonical));
        }

        @Override
        public List<Resource> list() {
            List<Resource> resources = new ArrayList<>();
            for (String canonical : byId.values()) {
                resources.add(parser.parse(canonical));
            }
            return resources;
        }
    }

    /**
     * An artifact store that answers for every id: this context is not about telling an applied pipeline
     * from an unapplied one, so every read of an unobserved pipeline stays the transient window it was
     * before the two were told apart.
     */
    private static ArtifactStore appliedPipelines() {
        return new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
            }

            @Override
            public Optional<Resource> get(String id) {
                return Optional.of(new PipelineResource(id, null, List.of("src_x"), null, null, null, null, null));
            }

            @Override
            public List<Resource> list() {
                return List.of();
            }
        };
    }

    /** Spec sources and registrations are irrelevant here: this suite drives authorization, not catalog reads. */
    private static ConnectorSpecStore emptySpecStore() {
        return new ConnectorSpecStore() {
            @Override
            public void put(String contentHash, byte[] spec) {
            }

            @Override
            public java.util.Optional<byte[]> get(String contentHash) {
                return java.util.Optional.empty();
            }
        };
    }

    private static ConnectorRegistry emptyConnectorRegistry() {
        return new ConnectorRegistry() {
            @Override
            public RegistrationOutcome register(String id, String pdkApiVersion, RegistrationSource source, byte[] artifact) {
                throw new UnsupportedOperationException();
            }

            @Override
            public java.util.List<ConnectorRegistration> list() {
                return java.util.List.of();
            }

            @Override
            public java.util.Optional<byte[]> artifact(String contentHash) {
                return java.util.Optional.empty();
            }

            @Override
            public boolean hasArtifact(String contentHash) {
                return false;
            }
        };
    }
}
