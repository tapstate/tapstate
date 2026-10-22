package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class MongoAiProviderSettingsStoreTest {
    @Test void readsOnlyTheRequestedOwnerAndDecryptsPurposeBoundSettings() throws Exception {
        String owner = "org-a/cluster-a";
        byte[] key = new byte[32];
        key[0] = 7;
        String keyId = SourceConfigCipher.fingerprint(key);
        SourceConfigCipher cipher = new SourceConfigCipher(key);
        String baseUrl = "https://relay.example.com/v1";
        String model = "model";
        String secret = "sk-provider-secret";
        String context = JsonWriter.write(List.of("openai-compatible", baseUrl, model));
        String encrypted = cipher.providerSecrets().encrypt(owner, context, secret);

        Document keyring = new Document("_id", "source-config-keyring")
                .append("formatVersion", 1).append("epoch", 1L).append("activeKeyId", keyId)
                .append("keys", List.of(new Document("id", keyId).append("state", "active")
                        .append("material", Base64.getUrlEncoder().withoutPadding().encodeToString(key))));
        Document settings = new Document("_id", MongoAiProviderSettingsStore.id(owner))
                .append("kind", MongoAiProviderSettingsStore.KIND).append("owner", owner)
                .append("providerId", "openai-compatible").append("baseUrl", baseUrl)
                .append("model", model).append("secret", encrypted);
        Fixture fixture = new Fixture(keyring, settings);
        MongoAiProviderSettingsStore store = new MongoAiProviderSettingsStore(fixture.client(), fixture.database());

        assertThat(store.get(owner)).contains(new io.tapstate.spi.store.AiProviderSettingsStore.Settings(
                "openai-compatible", baseUrl, model, secret));
        assertThat(store.get("org-b/cluster-b")).isEmpty();
        SourceConfigCipher loaded = new SourceConfigKeyringStore(fixture.client(), fixture.database()).loadOrCreateCipher();
        assertThat(loaded.activeKeyId()).isEqualTo(keyId);
        assertThat(loaded.providerSecrets().decrypt(owner, context, encrypted)).isEqualTo(secret);
        assertThat(fixture.findFilters).contains(
                new Document("_id", "source-config-keyring"),
                new Document("_id", MongoAiProviderSettingsStore.id(owner)),
                new Document("_id", MongoAiProviderSettingsStore.id("org-b/cluster-b")));
        assertThat(settings.toJson()).contains(encrypted).doesNotContain(secret);
    }

    @Test void rejectsCredentialsWhoseAuthenticatedProviderContextWasModified() throws Exception {
        String owner = "org-a/cluster-a";
        byte[] key = new byte[32];
        key[0] = 9;
        String keyId = SourceConfigCipher.fingerprint(key);
        String originalBaseUrl = "https://relay.example.com/v1";
        String model = "model";
        String secret = "sk-context-secret";
        String originalContext = JsonWriter.write(List.of("openai-compatible", originalBaseUrl, model));
        String encrypted = new SourceConfigCipher(key).providerSecrets().encrypt(owner, originalContext, secret);

        Document keyring = new Document("_id", "source-config-keyring")
                .append("formatVersion", 1).append("epoch", 1L).append("activeKeyId", keyId)
                .append("keys", List.of(new Document("id", keyId).append("state", "active")
                        .append("material", Base64.getUrlEncoder().withoutPadding().encodeToString(key))));
        Document modified = new Document("_id", MongoAiProviderSettingsStore.id(owner))
                .append("kind", MongoAiProviderSettingsStore.KIND).append("owner", owner)
                .append("providerId", "openai-compatible").append("baseUrl", "https://other.example.com/v1")
                .append("model", model).append("secret", encrypted);
        Fixture fixture = new Fixture(keyring, modified);
        MongoAiProviderSettingsStore store = new MongoAiProviderSettingsStore(fixture.client(), fixture.database());

        assertThatThrownBy(() -> store.get(owner)).isInstanceOf(TapstateException.class);
    }

    @Test void scopesRecordIdsAndIncludesEverySupportedSecretRecordShapeInRetirementFilter() throws Exception {
        String expected = "ai-provider:" + java.util.HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest("org/cluster".getBytes(StandardCharsets.UTF_8)));
        assertThat(MongoAiProviderSettingsStore.id("org/cluster")).isEqualTo(expected);
        assertThat(MongoAiProviderSettingsStore.id("org/cluster"))
                .isNotEqualTo(MongoAiProviderSettingsStore.id("org/other"));
        assertThat(MongoAiProviderSettingsStore.recordsFilter()).isEqualTo(new Document("$or", List.of(
                new Document("kind", MongoAiProviderSettingsStore.KIND),
                new Document("_id", new Document("$regex", "^ai-provider:")),
                new Document("secret", new Document("$regex", "^tsai:")))));

        Document incomplete = new Document("providerId", "openai-compatible")
                .append("baseUrl", "https://relay.example.com/v1").append("model", "model")
                .append("secret", "tsai:1:payload");
        assertThatThrownBy(() -> MongoAiProviderSettingsStore.context(incomplete))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.args()).containsEntry("field", "owner"));
    }

    @Test void verifiedMongoConnectionExposesTheOwnerScopedProviderStore() throws Exception {
        Fixture fixture = new Fixture(new Document(), new Document());
        MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                "mongodb://localhost/tapstate", null, Duration.ofSeconds(1)));
        var clientField = MongoConnection.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(connection, fixture.client());
        var databaseField = MongoConnection.class.getDeclaredField("databaseName");
        databaseField.setAccessible(true);
        databaseField.set(connection, "tapstate");

        assertThat(connection.aiProviderSettings()).isInstanceOf(MongoAiProviderSettingsStore.class);
    }

    private static final class Fixture {
        private final Document keyring;
        private final Document settings;
        private final List<Document> findFilters = new java.util.ArrayList<>();

        private Fixture(Document keyring, Document settings) {
            this.keyring = keyring;
            this.settings = settings;
        }

        MongoClient client() {
            return proxy(MongoClient.class, (self, method, args) -> {
                if (method.getName().equals("getDatabase")) return database();
                throw new AssertionError("read-only settings lookup must not call MongoClient." + method.getName());
            });
        }

        MongoDatabase database() {
            return proxy(MongoDatabase.class, (self, method, args) -> {
                if (method.getName().equals("getCollection")) return collection();
                throw new AssertionError("unexpected database operation: " + method.getName());
            });
        }

        private MongoCollection<Document> collection() {
            return proxy(MongoCollection.class, (self, method, args) -> {
                if (List.of("withReadPreference", "withReadConcern", "withWriteConcern").contains(method.getName())) return self;
                if (method.getName().equals("find")) {
                    Document filter = (Document) args[0];
                    findFilters.add(filter);
                    return proxy(FindIterable.class, (iterable, findMethod, findArgs) -> {
                        if (findMethod.getName().equals("first")) {
                            Object id = filter.get("_id");
                            if (filter.containsKey("$or")) return settings;
                            if (id.equals("source-config-keyring")) return keyring;
                            return id.equals(settings.getString("_id")) ? settings : null;
                        }
                        if (findMethod.getName().equals("projection")) return iterable;
                        throw new AssertionError("unexpected find operation: " + findMethod.getName());
                    });
                }
                throw new AssertionError("unexpected collection operation: " + method.getName());
            });
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, Invocation invocation) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, (self, method, args) ->
                invocation.invoke(self, method, args));
    }

    @FunctionalInterface
    private interface Invocation {
        Object invoke(Object self, java.lang.reflect.Method method, Object[] args) throws Throwable;
    }
}
