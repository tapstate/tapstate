package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.TokenRecord;
import io.tapstate.spi.store.TokenStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TokenAdminServiceTest {

    private static final Instant NOW = Instant.parse("2026-07-30T02:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void createAuditsTheKnownTokenIdBeforePersistingAndReturnsTheSecretOnce() {
        FakeTokenStore tokens = new FakeTokenStore();
        List<AuditRecord> audit = new ArrayList<>();
        TokenAdminService service = service(tokens, audit::add);

        CreatedToken created = service.create("alice", Scope.WRITE);

        assertThat(created).isEqualTo(
                new CreatedToken("tok-1", Scope.WRITE, "cyxt_tok-1.sec-1", NOW));
        assertThat(audit).containsExactly(
                new AuditRecord(NOW, "alice", "token.create", "tok-1"));
        assertThat(tokens.find("tok-1")).hasValueSatisfying(record -> {
            assertThat(record.secretHash()).isEqualTo("hash:sec-1");
            assertThat(record.secretHash()).doesNotContain("cyxt_");
        });
    }

    @Test
    void anAuditFailureRefusesCreateBeforeTheTokenIsPersisted() {
        FakeTokenStore tokens = new FakeTokenStore();
        TokenAdminService service = service(tokens, record -> {
            throw new IllegalStateException("audit unavailable");
        });

        assertThatThrownBy(() -> service.create("alice", Scope.WRITE))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo("TS0210015"));
        assertThat(tokens.list()).isEmpty();
    }

    @Test
    void listIsSecretFreeAndRevokeIsAuditedAndImmediatelyAuthoritative() {
        FakeTokenStore tokens = new FakeTokenStore();
        List<AuditRecord> audit = new ArrayList<>();
        TokenAdminService service = service(tokens, audit::add);
        CreatedToken created = service.create("alice", Scope.READ);
        audit.clear();

        assertThat(service.list()).containsExactly(
                new TokenInfo("tok-1", Scope.READ, false, NOW));

        service.revoke("alice", "tok-1");

        assertThat(audit).containsExactly(
                new AuditRecord(NOW, "alice", "token.revoke", "tok-1"));
        assertThat(service.list()).containsExactly(
                new TokenInfo("tok-1", Scope.READ, true, NOW));
        assertThat(tokens.find(created.tokenId())).hasValueSatisfying(
                record -> assertThat(record.revoked()).isTrue());
    }

    private static TokenAdminService service(TokenStore tokens, AuditStore audit) {
        TokenService tokenService = new TokenService(tokens, new FakeTokenSecrets(), CLOCK);
        return new TokenAdminService(tokenService, new AuditGate(audit, CLOCK));
    }

    private static final class FakeTokenStore implements TokenStore {
        private final Map<String, TokenRecord> records = new LinkedHashMap<>();

        @Override
        public void save(TokenRecord record) {
            records.put(record.tokenId(), record);
        }

        @Override
        public Optional<TokenRecord> find(String tokenId) {
            return Optional.ofNullable(records.get(tokenId));
        }

        @Override
        public void revoke(String tokenId) {
            TokenRecord record = records.get(tokenId);
            if (record != null) {
                records.put(tokenId, new TokenRecord(
                        record.tokenId(), record.scope(), record.secretHash(), true, record.createdAt()));
            }
        }

        @Override
        public List<TokenRecord> list() {
            return List.copyOf(records.values());
        }
    }

    private static final class FakeTokenSecrets implements TokenSecrets {
        private int counter;

        @Override
        public GeneratedSecret generate() {
            counter++;
            return new GeneratedSecret("tok-" + counter, "sec-" + counter, "hash:sec-" + counter);
        }

        @Override
        public boolean matches(String presentedSecret, String storedHash) {
            return storedHash.equals("hash:" + presentedSecret);
        }
    }
}
