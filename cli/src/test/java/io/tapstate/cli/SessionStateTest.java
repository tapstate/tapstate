package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SessionStateTest {

    private static final URI BASE = URI.create("https://tapstate.example.com");
    private static final Instant NOW = Instant.parse("2026-08-17T10:00:00Z");
    private static final AuthSessionRecord CACHED = new AuthSessionRecord(
            1,
            UUID.fromString("5c199643-04da-4f72-9831-3a77e3590eed"),
            UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2"),
            "urn:tapstate:cluster:01J5FIXTURE",
            "admin",
            List.of("read", "write", "admin"),
            "tss_s01.session-secret",
            NOW,
            NOW.plusSeconds(30L * 24 * 60 * 60),
            NOW.plusSeconds(90L * 24 * 60 * 60));

    @Test
    void exchangesCachedSessionWhenNoProcessAccessTokenExistsAndKeepsSessionTokenOutOfTheBearer() {
        FakeClient client = new FakeClient();
        SessionState state = new SessionState(client, fixed(NOW));

        String token = state.accessToken(BASE, CACHED);

        assertThat(token).isEqualTo("jwt-access-1");
        assertThat(client.calls()).isEqualTo(1);
        assertThat(client.lastSessionToken()).isEqualTo("tss_s01.session-secret");
        assertThat(client.seenApiBearer()).isNull();
    }

    @Test
    void reusesAProcessAccessTokenUntilItHasLessThanSixtySecondsRemaining() {
        MutableClock clock = new MutableClock(NOW);
        FakeClient client = new FakeClient();
        SessionState state = new SessionState(client, clock);

        assertThat(state.accessToken(BASE, CACHED)).isEqualTo("jwt-access-1");
        clock.now = NOW.plusSeconds(14 * 60 - 1);
        assertThat(state.accessToken(BASE, CACHED)).isEqualTo("jwt-access-1");
        clock.now = NOW.plusSeconds(14 * 60);
        assertThat(state.accessToken(BASE, CACHED)).isEqualTo("jwt-access-2");
        assertThat(client.calls()).isEqualTo(2);
    }

    @Test
    void consumesTheAccessGrantReturnedByPersistentLoginBeforeAnyExchange() {
        FakeClient client = new FakeClient();
        SessionState state = new SessionState(client, fixed(NOW));

        state.remember(CACHED, "jwt-from-login", NOW.plusSeconds(900));

        assertThat(state.accessToken(BASE, CACHED)).isEqualTo("jwt-from-login");
        assertThat(client.calls()).isZero();
    }

    @Test
    void neverReusesAnAccessTokenForAnotherCachedContext() {
        FakeClient client = new FakeClient();
        SessionState state = new SessionState(client, fixed(NOW));
        AuthSessionRecord anotherContext = new AuthSessionRecord(
                1,
                UUID.fromString("6c199643-04da-4f72-9831-3a77e3590eed"),
                UUID.fromString("118f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2"),
                "urn:tapstate:cluster:01J5FIXTURE",
                "admin",
                List.of("read", "write", "admin"),
                "tss_s02.another-session-secret",
                NOW,
                NOW.plusSeconds(30L * 24 * 60 * 60),
                NOW.plusSeconds(90L * 24 * 60 * 60));

        assertThat(state.accessToken(BASE, CACHED)).isEqualTo("jwt-access-1");
        assertThat(state.accessToken(BASE, anotherContext)).isEqualTo("jwt-access-2");
        assertThat(client.calls()).isEqualTo(2);
    }

    @Test
    void concurrentRefreshIsSingleFlightInsideOneProcess() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        FakeClient client = new FakeClient(entered, release);
        SessionState state = new SessionState(client, fixed(NOW));
        CountDownLatch start = new CountDownLatch(1);

        try (var pool = Executors.newFixedThreadPool(2)) {
            var first = pool.submit(() -> {
                start.await();
                return state.accessToken(BASE, CACHED);
            });
            var second = pool.submit(() -> {
                start.await();
                return state.accessToken(BASE, CACHED);
            });
            start.countDown();
            entered.await();
            release.countDown();

            assertThat(first.get()).isEqualTo("jwt-access-1");
            assertThat(second.get()).isEqualTo("jwt-access-1");
            assertThat(client.calls()).isEqualTo(1);
        }
    }

    @Test
    void rejectedOrMismatchedExchangeFailsClosedWithCodedErrorsAndNoSecretInMessage() {
        FakeClient rejected = new FakeClient();
        rejected.outcome = new SessionExchangeOutcome.Rejected("control.auth-session-revoked", "revoked");
        SessionState rejectedState = new SessionState(rejected, fixed(NOW));
        assertThatThrownBy(() -> rejectedState.accessToken(BASE, CACHED))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code().code()).isEqualTo("cli.auth-session-rejected");
                    assertThat(error.getMessage()).doesNotContain("tss_s01.session-secret");
                });

        FakeClient mismatch = new FakeClient();
        mismatch.outcome = new SessionExchangeOutcome.Success("jwt", NOW.plusSeconds(900),
                "urn:tapstate:cluster:OTHER", "admin", List.of("read", "write", "admin"));
        SessionState mismatchState = new SessionState(mismatch, fixed(NOW));
        assertThatThrownBy(() -> mismatchState.accessToken(BASE, CACHED))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code().code()).isEqualTo("cli.auth-issuer-mismatch");
                    assertThat(error.getMessage()).doesNotContain("urn:tapstate:cluster:OTHER");
                });
    }

    @Test
    void neverRendersAnUntrustedSessionRejectionCode() {
        FakeClient rejected = new FakeClient();
        String injectedSession = "tss_s01.session-secret";
        rejected.outcome = new SessionExchangeOutcome.Rejected("control." + injectedSession, "revoked");
        assertThat(rejected.outcome.toString()).doesNotContain(injectedSession);

        assertThatThrownBy(() -> new SessionState(rejected, fixed(NOW)).accessToken(BASE, CACHED))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code().code()).isEqualTo("cli.auth-session-rejected");
                    assertThat(error.getMessage()).doesNotContain(injectedSession);
                });
    }

    @Test
    void doesNotPresentAnIdleExpiredSessionToTheServer() {
        AuthSessionRecord idleExpired = new AuthSessionRecord(
                CACHED.version(), CACHED.authRef(), CACHED.contextId(), CACHED.issuer(), CACHED.principal(),
                CACHED.scopes(), CACHED.sessionToken(), CACHED.createdAt(), NOW, CACHED.absoluteExpiresAt());
        FakeClient client = new FakeClient();

        assertThatThrownBy(() -> new SessionState(client, fixed(NOW)).accessToken(BASE, idleExpired))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo("cli.auth-session-rejected"));
        assertThat(client.calls()).isZero();
    }

    @Test
    void mismatchedPrincipalScopesOrExpiredGrantFailClosedWithoutTheSessionSecret() {
        FakeClient principalMismatch = new FakeClient();
        principalMismatch.outcome = new SessionExchangeOutcome.Success("jwt", NOW.plusSeconds(900),
                CACHED.issuer(), "other-admin", CACHED.scopes());

        assertRejectedWithoutSecret(new SessionState(principalMismatch, fixed(NOW)));

        FakeClient scopeMismatch = new FakeClient();
        scopeMismatch.outcome = new SessionExchangeOutcome.Success("jwt", NOW.plusSeconds(900),
                CACHED.issuer(), CACHED.principal(), List.of("read"));

        assertRejectedWithoutSecret(new SessionState(scopeMismatch, fixed(NOW)));

        FakeClient expiredGrant = new FakeClient();
        expiredGrant.outcome = new SessionExchangeOutcome.Success("jwt", NOW,
                CACHED.issuer(), CACHED.principal(), CACHED.scopes());

        assertRejectedWithoutSecret(new SessionState(expiredGrant, fixed(NOW)));
    }

    @Test
    void secretBearingAuthValuesRedactBearerAndOpaqueSessionTokens() {
        String accessToken = "jwt-access-token";
        AuthService.ActiveSession active = new AuthService.ActiveSession(BASE, accessToken, CACHED);
        LoginOutcome.Success login = new LoginOutcome.Success(
                accessToken, NOW.plusSeconds(900), CACHED.issuer(), CACHED.principal(), CACHED.scopes(),
                CACHED.sessionToken(), CACHED.idleExpiresAt(), CACHED.absoluteExpiresAt());

        assertThat(CACHED.toString()).doesNotContain(CACHED.sessionToken());
        assertThat(login.toString()).doesNotContain(accessToken, CACHED.sessionToken());
        assertThat(active.toString()).doesNotContain(accessToken, CACHED.sessionToken());
        assertThat(new AuthService.LoginResult.Success(active, AuthFileStore.SaveResult.PERSISTED).toString())
                .doesNotContain(accessToken, CACHED.sessionToken());
        assertThat(new AuthService.Status.SignedIn(active).toString())
                .doesNotContain(accessToken, CACHED.sessionToken());
        assertThat(new SessionExchangeOutcome.Success(accessToken, NOW.plusSeconds(900),
                CACHED.issuer(), CACHED.principal(), CACHED.scopes()).toString())
                .doesNotContain(accessToken);
        assertThat(new SessionExchangeOutcome.Rejected("control.auth-session-revoked",
                "the server echoed tss_s01.session-secret").toString())
                .doesNotContain(CACHED.sessionToken());
    }

    private static void assertRejectedWithoutSecret(SessionState state) {
        assertThatThrownBy(() -> state.accessToken(BASE, CACHED))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code().code()).isEqualTo("cli.auth-session-rejected");
                    assertThat(error.getMessage()).doesNotContain(CACHED.sessionToken());
                });
    }

    private static Clock fixed(Instant now) {
        return Clock.fixed(now, ZoneOffset.UTC);
    }

    private static final class MutableClock extends Clock {
        private Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    private static final class FakeClient implements ControlPlaneClient {

        private final AtomicInteger calls = new AtomicInteger();
        private final CountDownLatch entered;
        private final CountDownLatch release;
        private volatile SessionExchangeOutcome outcome;
        private volatile String lastSessionToken;

        private FakeClient() {
            this(null, null);
        }

        private FakeClient(CountDownLatch entered, CountDownLatch release) {
            this.entered = entered;
            this.release = release;
        }

        @Override
        public SessionExchangeOutcome exchangeSession(URI baseUrl, String sessionToken) {
            lastSessionToken = sessionToken;
            int call = calls.incrementAndGet();
            if (entered != null) {
                entered.countDown();
            }
            if (release != null) {
                try {
                    release.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            if (outcome != null) {
                return outcome;
            }
            return new SessionExchangeOutcome.Success("jwt-access-" + call, NOW.plusSeconds(900),
                    "urn:tapstate:cluster:01J5FIXTURE", "admin", List.of("read", "write", "admin"));
        }

        int calls() {
            return calls.get();
        }

        String lastSessionToken() {
            return lastSessionToken;
        }

        String seenApiBearer() {
            return null;
        }

        @Override
        public boolean isHealthy(URI baseUrl) {
            return false;
        }

        @Override
        public LoginOutcome login(URI baseUrl, String username, String password) {
            return new LoginOutcome.Unreachable();
        }

        @Override
        public ApplyOutcome apply(URI baseUrl, String credential, List<LocalDraft> drafts) {
            return new ApplyOutcome.Unreachable();
        }

        @Override
        public GetOutcome get(URI baseUrl, String credential, String id) {
            return new GetOutcome.Unreachable();
        }

        @Override
        public DeleteOutcome delete(URI baseUrl, String credential, String id, String expectedContentHash) {
            return new DeleteOutcome.Unreachable();
        }

        @Override
        public ListOutcome list(URI baseUrl, String credential, String kind) {
            return new ListOutcome.Unreachable();
        }

        @Override
        public ConnectionTestOutcome test(
                URI baseUrl, String credential, String id, String connectorId, java.util.Map<String, Object> settings) {
            return new ConnectionTestOutcome.Unreachable();
        }

        @Override
        public ConnectionTestResultOutcome testResult(URI baseUrl, String credential, String id) {
            return new ConnectionTestResultOutcome.Unreachable();
        }

        @Override
        public ConnectionDiscoverSchemaOutcome discoverSchema(
                URI baseUrl, String credential, String id, String connectorId, java.util.Map<String, Object> settings) {
            return new ConnectionDiscoverSchemaOutcome.Unreachable();
        }

        @Override
        public ConnectionSchemaOutcome schema(URI baseUrl, String credential, String id) {
            return new ConnectionSchemaOutcome.Unreachable();
        }

        @Override
        public ConnectorRegisterOutcome register(URI baseUrl, String credential, byte[] artifact) {
            return new ConnectorRegisterOutcome.Unreachable();
        }

        @Override
        public ConnectorListOutcome connectorList(URI baseUrl, String credential) {
            return new ConnectorListOutcome.Unreachable();
        }

        @Override
        public DataBrowserOutcome.Collections collections(URI baseUrl, String credential, String sourceId) {
            return new DataBrowserOutcome.Collections.Unreachable();
        }

        @Override
        public DataBrowserOutcome.Stats stats(URI baseUrl, String credential, String sourceId, String collection) {
            return new DataBrowserOutcome.Stats.Unreachable();
        }

        @Override
        public DataBrowserOutcome.Find find(URI baseUrl, String credential, String sourceId, String collection,
                                            Object filter, DataBrowserCall.Order sort, Integer limit) {
            return new DataBrowserOutcome.Find.Unreachable();
        }

        @Override
        public LifecycleOutcome lifecycle(URI baseUrl, String credential, String pipelineId, String verb) {
            return new LifecycleOutcome.Unreachable();
        }

        @Override
        public StatusOutcome status(URI baseUrl, String credential, String pipelineId) {
            return new StatusOutcome.Unreachable();
        }

        @Override
        public MetricsOutcome metrics(URI baseUrl, String credential, String pipelineId) {
            return new MetricsOutcome.Unreachable();
        }

        @Override
        public SnapshotOutcome snapshot(URI baseUrl, String credential, String pipelineId) {
            return new SnapshotOutcome.Unreachable();
        }

        @Override
        public LogsOutcome logs(URI baseUrl, String credential, String pipelineId) {
            return new LogsOutcome.Unreachable();
        }

        @Override
        public String watchStatus(URI baseUrl, String credential, String pipelineId, StatusStream sink,
                                  java.util.function.BooleanSupplier stop) {
            return null;
        }

        @Override
        public String followLogs(URI baseUrl, String credential, String pipelineId, LogStream sink,
                                 java.util.function.BooleanSupplier stop) {
            return null;
        }

        @Override
        public String tail(URI baseUrl, String credential, String sourceId, String collection, Object filter,
                           TailStream sink, java.util.function.BooleanSupplier stop) {
            return null;
        }
    }
}
