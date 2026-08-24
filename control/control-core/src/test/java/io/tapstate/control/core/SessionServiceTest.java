package io.tapstate.control.core;

import io.tapstate.spi.store.SessionRecord;
import io.tapstate.spi.store.SessionStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class SessionServiceTest {

    private static final Instant NOW = Instant.parse("2026-08-17T10:00:00Z");
    private static final String ISSUER = "urn:tapstate:cluster:01J5FIXTURE";

    @Test
    void createPersistsOnlyTheHashAndReturnsTheRawSecretOnce() {
        MemorySessionStore store = new MemorySessionStore();
        SessionService service = service(store, NOW);

        CreatedSession created = service.create("admin", Scope.ADMIN, ISSUER);

        assertThat(created.token()).isEqualTo("tss_s01.session-secret");
        assertThat(created.idleExpiresAt()).isEqualTo(NOW.plus(SessionService.IDLE_TTL));
        assertThat(created.absoluteExpiresAt()).isEqualTo(NOW.plus(SessionService.ABSOLUTE_TTL));
        SessionRecord persisted = store.records.get("s01");
        assertThat(persisted.secretHash()).isEqualTo("digest-fixture");
        assertThat(persisted.toString()).doesNotContain("session-secret");
    }

    @Test
    void exchangeDelegatesAllCredentialAndLifetimeChecksToOneAtomicStoreOperation() {
        MemorySessionStore store = new MemorySessionStore();
        SessionService service = service(store, NOW);
        service.create("admin", Scope.ADMIN, ISSUER);
        store.exchangeCalls = 0;

        Optional<AccessTokenGrant> grant = service.exchange("tss_s01.session-secret", ISSUER);

        assertThat(grant).get().extracting(AccessTokenGrant::token).isEqualTo("admin|ADMIN");
        assertThat(store.exchangeCalls).isEqualTo(1);
        assertThat(store.findCalls).isZero();
        assertThat(store.records.get("s01").lastUsedAt()).isEqualTo(NOW);
    }

    @Test
    void issuerSecretAndExpiryMismatchFailClosed() {
        MemorySessionStore store = new MemorySessionStore();
        service(store, NOW).create("admin", Scope.ADMIN, ISSUER);

        assertThat(service(store, NOW).exchange("tss_s01.wrong", ISSUER)).isEmpty();
        assertThat(service(store, NOW).exchange("tss_s01.session-secret", "urn:other")).isEmpty();
        assertThat(service(store, NOW.plus(SessionService.IDLE_TTL))
                .exchange("tss_s01.session-secret", ISSUER)).isEmpty();
    }

    @Test
    void idleRefreshIsCappedAtTheAbsoluteExpiry() {
        MemorySessionStore store = new MemorySessionStore();
        service(store, NOW).create("admin", Scope.ADMIN, ISSUER);
        Instant nearAbsolute = NOW.plus(SessionService.ABSOLUTE_TTL).minusSeconds(10);
        SessionRecord record = store.records.get("s01");
        store.records.put("s01", new SessionRecord(record.sessionId(), record.secretHash(), record.principal(),
                record.scope(), record.issuer(), false, record.createdAt(), record.lastUsedAt(),
                record.absoluteExpiresAt(), record.absoluteExpiresAt()));

        assertThat(service(store, nearAbsolute).exchange("tss_s01.session-secret", ISSUER)).isPresent();

        assertThat(store.records.get("s01").idleExpiresAt())
                .isEqualTo(NOW.plus(SessionService.ABSOLUTE_TTL));
    }

    @Test
    void absoluteExpiryRejectsASessionEvenWhenIdleExpiryIsStillInTheFuture() {
        MemorySessionStore store = new MemorySessionStore();
        service(store, NOW).create("admin", Scope.ADMIN, ISSUER);
        SessionRecord record = store.records.get("s01");
        store.records.put("s01", new SessionRecord(record.sessionId(), record.secretHash(), record.principal(),
                record.scope(), record.issuer(), false, record.createdAt(), record.lastUsedAt(),
                NOW.plusSeconds(1), NOW));

        assertThat(service(store, NOW).exchange("tss_s01.session-secret", ISSUER)).isEmpty();
        assertThat(store.records.get("s01").lastUsedAt()).isEqualTo(record.lastUsedAt());
    }

    @Test
    void logoutRejectsUnknownOrMismatchedCredentialsAndIsIdempotentForTheValidCredential() {
        MemorySessionStore store = new MemorySessionStore();
        SessionService service = service(store, NOW);
        service.create("admin", Scope.ADMIN, ISSUER);

        assertThat(service.logout("tss_missing.session-secret", ISSUER)).isFalse();
        assertThat(service.logout("tss_s01.wrong", ISSUER)).isFalse();
        assertThat(service.logout("tss_s01.session-secret", "urn:other")).isFalse();
        assertThat(service.logout("tss_s01.session-secret", ISSUER)).isTrue();
        assertThat(service.logout("tss_s01.session-secret", ISSUER)).isTrue();
        assertThat(service.exchange("tss_s01.session-secret", ISSUER)).isEmpty();
    }

    @Test
    void malformedTokensNeverReachStorage() {
        MemorySessionStore store = new MemorySessionStore();
        SessionService service = service(store, NOW);

        assertThat(service.exchange("tss_missing-separator", ISSUER)).isEmpty();
        assertThat(service.exchange("tss_id.secret.extra", ISSUER)).isEmpty();
        assertThat(service.exchange("tss_üid.secret", ISSUER)).isEmpty();
        assertThat(service.logout("Bearer tss_id.secret", ISSUER)).isFalse();
        assertThat(store.exchangeCalls).isZero();
        assertThat(store.revokeCalls).isZero();
    }

    private static SessionService service(MemorySessionStore store, Instant now) {
        TokenSecrets secrets = new TokenSecrets() {
            @Override
            public GeneratedSecret generate() {
                return new GeneratedSecret("s01", "session-secret", "digest-fixture");
            }

            @Override
            public String hash(String presentedSecret) {
                return "session-secret".equals(presentedSecret) ? "digest-fixture" : "digest-wrong";
            }

            @Override
            public boolean matches(String presentedSecret, String storedHash) {
                return storedHash.equals(hash(presentedSecret));
            }
        };
        TokenSigner signer = new TokenSigner() {
            @Override
            public String issue(String subject, Scope scope) {
                return subject + "|" + scope;
            }

            @Override
            public Optional<VerifiedToken> verify(String token) {
                return Optional.empty();
            }
        };
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        return new SessionService(store, secrets, new AccessTokenService(signer, clock), clock);
    }

    private static final class MemorySessionStore implements SessionStore {
        private final Map<String, SessionRecord> records = new LinkedHashMap<>();
        private int findCalls;
        private int exchangeCalls;
        private int revokeCalls;

        @Override
        public void save(SessionRecord record) {
            records.put(record.sessionId(), record);
        }

        @Override
        public Optional<SessionRecord> find(String sessionId) {
            findCalls++;
            return Optional.ofNullable(records.get(sessionId));
        }

        @Override
        public Optional<SessionRecord> exchange(
                String sessionId, String secretHash, String issuer, Instant now, Instant idleExpiresAt) {
            exchangeCalls++;
            SessionRecord record = records.get(sessionId);
            if (record == null || record.revoked() || !record.secretHash().equals(secretHash)
                    || !record.issuer().equals(issuer) || !record.idleExpiresAt().isAfter(now)
                    || !record.absoluteExpiresAt().isAfter(now)) {
                return Optional.empty();
            }
            SessionRecord touched = new SessionRecord(record.sessionId(), record.secretHash(), record.principal(),
                    record.scope(), record.issuer(), false, record.createdAt(), now,
                    idleExpiresAt.isBefore(record.absoluteExpiresAt()) ? idleExpiresAt : record.absoluteExpiresAt(),
                    record.absoluteExpiresAt());
            records.put(sessionId, touched);
            return Optional.of(touched);
        }

        @Override
        public boolean revoke(String sessionId, String secretHash, String issuer, Instant now) {
            revokeCalls++;
            SessionRecord record = records.get(sessionId);
            if (record == null || !record.secretHash().equals(secretHash) || !record.issuer().equals(issuer)
                    || !record.idleExpiresAt().isAfter(now) || !record.absoluteExpiresAt().isAfter(now)) {
                return false;
            }
            records.put(sessionId, new SessionRecord(record.sessionId(), record.secretHash(), record.principal(),
                    record.scope(), record.issuer(), true, record.createdAt(), record.lastUsedAt(),
                    record.idleExpiresAt(), record.absoluteExpiresAt()));
            return true;
        }
    }
}
