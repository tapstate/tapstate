package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionRecord;
import io.tapstate.spi.store.CloudSessionStore;
import io.tapstate.spi.store.IoError;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudAuthenticationServiceTest {

    private static final Instant NOW = Instant.parse("2026-09-28T10:00:00Z");
    private static final CloudSessionIdentity DEPLOYMENT =
            new CloudSessionIdentity("https://cloud.example", "org-a", "cluster-a");
    private final MutableClock clock = new MutableClock();
    private final MemoryStore store = new MemoryStore();
    private final TokenSecrets secrets = new TokenSecrets() {
        @Override public GeneratedSecret generate() { return new GeneratedSecret("unused-public-id", "secret", "hash-secret"); }
        @Override public String hash(String secret) { return "hash-" + secret; }
        @Override public boolean matches(String secret, String hash) { return hash(secret).equals(hash); }
    };
    private final CloudSessionService sessions = new CloudSessionService(store, DEPLOYMENT, secrets, clock);

    @Test
    void exchangeIsUntrustedUntilOnlineValidationThenOnlyMinimalLocalLoginFactsAreStored() {
        List<String> order = new ArrayList<>();
        CloudAuthenticationService authentication = new CloudAuthenticationService((code, cluster) -> {
            order.add("exchange");
            assertThat(cluster).isEqualTo(DEPLOYMENT.clusterId());
            return "raw-cloud-jwt-sentinel";
        }, (jwt, target) -> {
            order.add("sdk-verify");
            assertThat(jwt).isEqualTo("raw-cloud-jwt-sentinel");
            assertThat(target).isEqualTo(DEPLOYMENT);
            return Optional.of(login("jti-a", NOW.plusSeconds(900)));
        }, sessions);

        CreatedCloudSession created = authentication.exchangeCode("short-code");

        assertThat(order).containsExactly("exchange", "sdk-verify");
        assertThat(created.idleExpiresAt()).isEqualTo(NOW.plusSeconds(1800));
        assertThat(created.token()).startsWith("tcs_").doesNotContain("raw-cloud-jwt-sentinel");
        assertThat(created.toString()).doesNotContain(created.token());
        CloudSessionRecord saved = store.records.get(new Key(DEPLOYMENT, "jti-a"));
        assertThat(saved.userId()).isEqualTo("stable-user-a");
        assertThat(saved.jwtId()).isEqualTo("jti-a");
        assertThat(saved.secretHash()).isEqualTo("hash-secret");
        assertThat(saved.toString()).doesNotContain("raw-cloud-jwt-sentinel", "hash-secret");
    }

    @Test
    void aRejectedOnlineValidationNeverCreatesALocalSession() {
        CloudAuthenticationService authentication = new CloudAuthenticationService((code, cluster) -> "jwt",
                (jwt, target) -> Optional.empty(), sessions);

        assertThatThrownBy(() -> authentication.exchangeCode("code"))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ControlError.UNAUTHENTICATED));
        assertThat(store.records).isEmpty();
    }

    @Test
    void unavailableValidationHasNoSessionSideEffectAndIsNotReplacedByParsedJwtClaims() {
        CloudAuthenticationService authentication = new CloudAuthenticationService((code, cluster) -> "jwt",
                (jwt, target) -> { throw CloudAuthenticationService.unavailable(); }, sessions);
        assertThatThrownBy(() -> authentication.exchangeCode("code"))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ControlError.CLOUD_AUTH_UNAVAILABLE));
        assertThat(store.records).isEmpty();
    }

    @Test
    void initialJwtExpiryIsAVerificationWindowNotTheLifetimeOfTheLocalSession() {
        AtomicInteger exchanges = new AtomicInteger();
        AtomicInteger validations = new AtomicInteger();
        CloudAuthenticationService authentication = new CloudAuthenticationService((code, cluster) -> {
            exchanges.incrementAndGet();
            return "jwt";
        }, (jwt, target) -> {
            validations.incrementAndGet();
            return Optional.of(login("jti-a", NOW.plusSeconds(900)));
        }, sessions);
        String cookie = authentication.exchangeCode("code").token();

        clock.now = NOW.plusSeconds(16 * 60);
        assertThat(authentication.authenticate(cookie)).contains(new VerifiedToken("stable-user-a", Scope.WRITE));
        assertThat(store.find(DEPLOYMENT, "jti-a").orElseThrow().idleExpiresAt()).isEqualTo(NOW.plusSeconds(46 * 60));
        clock.now = NOW.plusSeconds(45 * 60);
        assertThat(authentication.authenticate(cookie)).isPresent();
        assertThat(store.find(DEPLOYMENT, "jti-a").orElseThrow().idleExpiresAt()).isEqualTo(NOW.plusSeconds(75 * 60));
        clock.now = NOW.plusSeconds(75 * 60);
        assertThat(authentication.authenticate(cookie)).isEmpty();
        assertThat(exchanges.get()).isEqualTo(1);
        assertThat(validations.get()).isEqualTo(1);
    }

    @Test
    void exactlyThirtyMinutesIdleExpiresAndFailureDoesNotReviveOrExtendIt() {
        String cookie = sessions.create(login("jti-a", NOW.plusSeconds(900))).orElseThrow().token();
        clock.now = NOW.plusSeconds(1800);

        assertThat(sessions.authenticate(cookie)).isEmpty();
        clock.now = NOW.plusSeconds(1801);
        assertThat(sessions.authenticate(cookie)).isEmpty();
        assertThat(store.find(DEPLOYMENT, "jti-a").orElseThrow().lastUsedAt()).isEqualTo(NOW);
    }

    @Test
    void alreadyExpiredInitialProofWrongDeploymentAndAdminScopeCannotCreateASession() {
        assertThat(sessions.create(login("expired", NOW))).isEmpty();
        assertThat(sessions.create(new CloudLoginIdentity(
                new CloudSessionIdentity(DEPLOYMENT.issuer(), "org-b", "cluster-a"),
                "stable-user-a", "wrong-org", Scope.WRITE, NOW.plusSeconds(900)))).isEmpty();
        assertThat(sessions.create(new CloudLoginIdentity(DEPLOYMENT, "stable-user-a", "admin",
                Scope.ADMIN, NOW.plusSeconds(900)))).isEmpty();
        assertThat(store.records).isEmpty();
    }

    @Test
    void jwtIdInvalidationIsIdempotentExactAndPreventsLateLoginResurrection() {
        String cookieA = sessions.create(login("jti-a", NOW.plusSeconds(900))).orElseThrow().token();
        String cookieB = sessions.create(login("jti-b", NOW.plusSeconds(900))).orElseThrow().token();
        sessions.invalidateJwt("jti-a");
        sessions.invalidateJwt("jti-a");
        assertThat(sessions.authenticate(cookieA)).isEmpty();
        assertThat(sessions.authenticate(cookieB)).isPresent();
        sessions.invalidateJwt("jti-late");
        assertThat(sessions.create(login("jti-late", NOW.plusSeconds(900)))).isEmpty();
    }

    @Test
    void localLogoutIsIdempotentAndDoesNotConsultCloud() {
        String cookie = sessions.create(login("jti-a", NOW.plusSeconds(900))).orElseThrow().token();
        assertThat(sessions.logout(cookie)).isTrue();
        assertThat(sessions.logout(cookie)).isTrue();
        assertThat(sessions.authenticate(cookie)).isEmpty();
        assertThat(sessions.create(login("jti-a", NOW.plusSeconds(900)))).isEmpty();
    }

    @Test
    void rawJwtLocalJwtMachineTokenAndMalformedCookiesAreNeverLocalCloudSessions() {
        String cookie = sessions.create(login("jti-a", NOW.plusSeconds(900))).orElseThrow().token();
        for (String invalid : List.of("raw-jwt", "cyxt_id.secret", "tss_id.secret", "tcs_", "tcs_@@.secret",
                "tcs_am5pLWE=.secret", cookie + ".extra", cookie + " ", cookie.replace("secret", "wrong"))) {
            assertThat(sessions.authenticate(invalid)).as("a credential outside the local cookie contract").isEmpty();
        }
        assertThat(sessions.authenticate(cookie)).isPresent();
    }

    @Test
    void malformedPersistedScopeIsACodedStorageDiagnosticWithoutItsUntrustedValueOrCause() {
        String cookie = sessions.create(login("jti-a", NOW.plusSeconds(900))).orElseThrow().token();
        CloudSessionRecord valid = store.records.get(new Key(DEPLOYMENT, "jti-a"));
        store.records.put(new Key(DEPLOYMENT, "jti-a"), new CloudSessionRecord(DEPLOYMENT, "jti-a",
                valid.secretHash(), valid.userId(), "stored-scope-secret-sentinel", false,
                valid.createdAt(), valid.lastUsedAt(), valid.idleExpiresAt()));
        assertThatThrownBy(() -> sessions.authenticate(cookie))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                    assertThat(failure.args()).containsEntry("field", "scope");
                    assertThat(failure.toString()).doesNotContain("stored-scope-secret-sentinel");
                    assertThat(failure.getCause()).isNull();
                });
    }

    private static CloudLoginIdentity login(String jti, Instant expiry) {
        return new CloudLoginIdentity(DEPLOYMENT, "stable-user-a", jti, Scope.WRITE, expiry);
    }

    private static final class MutableClock extends Clock {
        Instant now = NOW;
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    private record Key(CloudSessionIdentity deployment, String jwtId) { }

    private static final class MemoryStore implements CloudSessionStore {
        final Map<Key, CloudSessionRecord> records = new HashMap<>();
        final Set<Key> revoked = new HashSet<>();

        @Override public boolean create(CloudSessionRecord record) {
            Key key = new Key(record.identity(), record.jwtId());
            return !revoked.contains(key) && records.putIfAbsent(key, record) == null;
        }
        @Override public Optional<CloudSessionRecord> find(CloudSessionIdentity identity, String jti) {
            return Optional.ofNullable(records.get(new Key(identity, jti)));
        }
        @Override public Optional<CloudSessionRecord> authenticate(CloudSessionIdentity identity, String jti,
                String hash, Instant now, Instant expiry) {
            Key key = new Key(identity, jti);
            CloudSessionRecord existing = records.get(key);
            if (existing == null || revoked.contains(key) || !hash.equals(existing.secretHash())
                    || !existing.idleExpiresAt().isAfter(now)) {
                return Optional.empty();
            }
            CloudSessionRecord touched = new CloudSessionRecord(identity, jti, hash, existing.userId(), existing.scope(),
                    false, existing.createdAt(), now, expiry);
            records.put(key, touched);
            return Optional.of(touched);
        }
        @Override public boolean logout(CloudSessionIdentity identity, String jti, String hash, Instant now) {
            Key key = new Key(identity, jti);
            CloudSessionRecord record = records.get(key);
            if (record == null || !hash.equals(record.secretHash())) {
                return false;
            }
            revoked.add(key);
            return true;
        }
        @Override public void invalidate(CloudSessionIdentity identity, String jti, Instant now) {
            revoked.add(new Key(identity, jti));
        }
    }
}
