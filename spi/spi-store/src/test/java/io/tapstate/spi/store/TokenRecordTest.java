package io.tapstate.spi.store;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The token record is a pure, fully validated value: the id, scope and secret hash are required and
 * non-blank and the creation instant is required, so a malformed token can never enter the store or
 * the authentication flow. The raw secret is deliberately absent from the shape — only its hash is
 * ever persisted.
 */
class TokenRecordTest {

    private static final Instant NOW = Instant.parse("2026-07-08T00:00:00Z");

    @Test
    void authenticationBindingTracksTheVerifiedRecordRatherThanOnlyItsReusableId() {
        TokenRecord original = new TokenRecord("token", "ADMIN", "original-hash", false, NOW);
        assertThat(original.authenticationBinding()).matches("[a-f0-9]{64}");
        assertThat(new TokenRecord("token", "ADMIN", "original-hash", false, NOW).authenticationBinding()).isEqualTo(original.authenticationBinding());
        assertThat(new TokenRecord("token", "ADMIN", "replacement-hash", false, NOW).authenticationBinding()).isNotEqualTo(original.authenticationBinding());
        assertThat(new TokenRecord("token", "ADMIN", "original-hash", false, NOW.plusNanos(1)).authenticationBinding()).isNotEqualTo(original.authenticationBinding());
        assertThat(new TokenRecord("token", "READ", "original-hash", false, NOW).authenticationBinding()).isNotEqualTo(original.authenticationBinding());
    }

    @Test
    void carriesItsFields() {
        TokenRecord record = new TokenRecord("tok-1", "WRITE", "hash-abc", false, NOW);

        assertThat(record.tokenId()).isEqualTo("tok-1");
        assertThat(record.scope()).isEqualTo("WRITE");
        assertThat(record.secretHash()).isEqualTo("hash-abc");
        assertThat(record.revoked()).isFalse();
        assertThat(record.createdAt()).isEqualTo(NOW);
    }

    @Test
    void rejectsABlankTokenId() {
        assertThatThrownBy(() -> new TokenRecord("  ", "WRITE", "hash-abc", false, NOW))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tokenId");
    }

    @Test
    void rejectsABlankScope() {
        assertThatThrownBy(() -> new TokenRecord("tok-1", "", "hash-abc", false, NOW))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scope");
    }

    @Test
    void rejectsABlankSecretHash() {
        assertThatThrownBy(() -> new TokenRecord("tok-1", "WRITE", null, false, NOW))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("secretHash");
    }

    @Test
    void rejectsAMissingCreatedAt() {
        assertThatThrownBy(() -> new TokenRecord("tok-1", "WRITE", "hash-abc", false, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("createdAt");
    }
}
