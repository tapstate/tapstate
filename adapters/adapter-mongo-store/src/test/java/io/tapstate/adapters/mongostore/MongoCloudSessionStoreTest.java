package io.tapstate.adapters.mongostore;

import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteError;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionRecord;
import io.tapstate.spi.store.IoError;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MongoCloudSessionStoreTest {

    private static final Instant CREATED = Instant.parse("2026-09-28T10:00:00Z");
    private static final CloudSessionIdentity IDENTITY =
            new CloudSessionIdentity("https://cloud.example", "org-one", "cluster-one");

    @Test
    void onlyMinimalLoginFactsAndTheCookieHashRoundTrip() {
        CloudSessionRecord record = record();
        Document raw = MongoCloudSessionStore.toDocument(record);

        assertThat(raw.keySet()).containsExactlyInAnyOrder("_id", "origin", "issuer", "organizationId",
                "clusterId", "jwtId", "secretHash", "userId", "scope", "revoked", "createdAt",
                "lastUsedAt", "idleExpiresAt");
        assertThat(raw.get("_id")).isInstanceOf(Binary.class);
        assertThat(raw.getString("origin")).isEqualTo("cloud");
        assertThat(raw.toJson()).doesNotContain("raw-jwt-sentinel", "raw-cookie-secret-sentinel",
                "password", "email", "absoluteExpiresAt");
        assertThat(MongoCloudSessionStore.toRecord(raw)).contains(record);
    }

    @Test
    void binaryKeysAreDeterministicAndLengthFramingSeparatesAmbiguousConcatenations() {
        Binary key = MongoCloudSessionStore.sessionKey(IDENTITY, "jwt-one");
        assertThat(key.getData()).hasSize(32);
        assertThat(key).isEqualTo(MongoCloudSessionStore.sessionKey(IDENTITY, "jwt-one"));
        assertThat(key).isNotEqualTo(MongoCloudSessionStore.sessionKey(IDENTITY, "jwt-two"));
        assertThat(MongoCloudSessionStore.sessionKey(new CloudSessionIdentity("a", "bc", "d"), "e"))
                .isNotEqualTo(MongoCloudSessionStore.sessionKey(new CloudSessionIdentity("ab", "c", "d"), "e"));
        assertThat(key).isNotEqualTo("jwt-one");
    }

    @Test
    void aRevocationOnlyMarkerIsNeverReconstructedAsALogin() {
        Document marker = MongoCloudSessionStore.toDocument(record());
        for (String field : List.of("secretHash", "userId", "scope", "createdAt", "lastUsedAt", "idleExpiresAt")) {
            marker.remove(field);
        }
        marker.put("revoked", true);

        assertThat(MongoCloudSessionStore.toRecord(marker)).isEmpty();
        marker.put("userId", "partial-login");
        assertUnreadableWithoutValues(marker);
    }

    @Test
    void malformedSecurityFieldsAndWrongContextFailClosedWithoutEchoingStoredValues() {
        for (String field : List.of("origin", "issuer", "organizationId", "clusterId", "jwtId",
                "secretHash", "userId", "scope", "revoked", "createdAt", "lastUsedAt", "idleExpiresAt")) {
            Document raw = MongoCloudSessionStore.toDocument(record());
            raw.put(field, new Document("untrusted", "stored-secret-sentinel"));
            assertUnreadableWithoutValues(raw);
        }
        Document changedContext = MongoCloudSessionStore.toDocument(record());
        changedContext.put("clusterId", "another-cluster");
        assertUnreadableWithoutValues(changedContext);
        Document changedId = MongoCloudSessionStore.toDocument(record());
        changedId.put("_id", "stored-secret-sentinel");
        assertUnreadableWithoutValues(changedId);
    }

    @Test
    void onlyADuplicateKeyIsConvertedToAnInsertRefusal() {
        MongoWriteException duplicate = writeFailure(11000);
        assertThat(new MongoCloudSessionStore(failingInsert(duplicate)).create(record())).isFalse();

        MongoWriteException other = writeFailure(13);
        assertThatThrownBy(() -> new MongoCloudSessionStore(failingInsert(other)).create(record()))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(IoError.STORE_UNAVAILABLE);
                    assertThat(failure.args().toString()).doesNotContain("driver-secret-sentinel");
                    assertThat(failure.getCause()).isNull();
                });
    }

    @Test
    void programmerFailuresAreNotHiddenAsDuplicateOrIoRefusals() {
        IllegalStateException defect = new IllegalStateException("broken insert invariant");
        assertThatThrownBy(() -> new MongoCloudSessionStore(failingInsert(defect)).create(record()))
                .isSameAs(defect);
    }

    private static void assertUnreadableWithoutValues(Document document) {
        assertThatThrownBy(() -> MongoCloudSessionStore.toRecord(document))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                    assertThat(failure.args()).containsEntry("id", "cloud-session");
                    assertThat(failure.getCause()).isNull();
                    StringWriter stack = new StringWriter();
                    failure.printStackTrace(new PrintWriter(stack));
                    assertThat(stack.toString()).doesNotContain("stored-secret-sentinel", record().secretHash());
                });
    }

    private static CloudSessionRecord record() {
        return new CloudSessionRecord(IDENTITY, "jwt-one", "sha256-cookie-fixture", "verified-user-one",
                "workload:read workload:write", false, CREATED, CREATED, CREATED.plusSeconds(30 * 60));
    }

    private static MongoWriteException writeFailure(int code) {
        return new MongoWriteException(new WriteError(code, "driver-secret-sentinel", new BsonDocument()),
                new ServerAddress(), Set.of());
    }

    @SuppressWarnings("unchecked")
    private static MongoCollection<Document> failingInsert(RuntimeException failure) {
        return (MongoCollection<Document>) Proxy.newProxyInstance(MongoCollection.class.getClassLoader(),
                new Class<?>[] {MongoCollection.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("insertOne")) {
                        throw failure;
                    }
                    throw new AssertionError("Unexpected driver operation: " + method.getName());
                });
    }
}
