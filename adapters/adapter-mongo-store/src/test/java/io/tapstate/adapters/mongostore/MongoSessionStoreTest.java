package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SessionRecord;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class MongoSessionStoreTest {

    private static final Instant CREATED = Instant.parse("2026-08-17T10:00:00Z");

    @Test
    void documentContainsOnlyTheFrozenServerSideSessionFields() {
        Document document = MongoSessionStore.toDocument(record());

        assertThat(document.keySet()).containsExactlyInAnyOrder(
                "_id", "secretHash", "principal", "scope", "issuer", "revoked", "createdAt",
                "lastUsedAt", "idleExpiresAt", "absoluteExpiresAt");
        assertThat(document.values()).doesNotContain("session-secret", "tss_s01.session-secret");
        assertThat(MongoSessionStore.toRecord(document)).isEqualTo(record());
    }

    @Test
    void missingSecurityFieldFailsClosedAsUnreadable() {
        Document partial = MongoSessionStore.toDocument(record());
        partial.remove("issuer");

        Throwable thrown = catchThrowable(() -> MongoSessionStore.toRecord(partial));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
        assertThat(((TapstateException) thrown).args()).containsEntry("id", "s01");
    }

    private static SessionRecord record() {
        return new SessionRecord("s01", "sha256-fixture", "admin", "ADMIN",
                "urn:tapstate:cluster:01J5FIXTURE", false, CREATED, CREATED,
                CREATED.plusSeconds(30L * 24 * 3600), CREATED.plusSeconds(90L * 24 * 3600));
    }
}
