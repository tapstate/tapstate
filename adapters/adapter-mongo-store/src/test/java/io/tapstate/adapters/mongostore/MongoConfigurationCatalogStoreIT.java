package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Configuration-only catalog encryption, BSON type fidelity, key fencing and revision transaction witnesses. */
@RequiresDocker
class MongoConfigurationCatalogStoreIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void connectionSettingsStayEncryptedAndRetainTheirValuesAcrossReopenWithoutCache() {
        withConfiguration((domain, database) -> {
            Map<String, Object> settings = new LinkedHashMap<>();
            settings.put("uri", "mongodb://private-user:private-password@database.invalid/data");
            settings.put("long", 5L);
            settings.put("integer", 5);
            settings.put("decimal", new BigDecimal("123.4500"));
            settings.put("binary", new byte[]{1, 2, 3});
            settings.put("nested", Map.of("list", java.util.Arrays.asList("secret-sentinel", null, true)));
            ConnectionConfig original = new ConnectionConfig("named", "mongodb", settings);
            domain.catalog().save(original);
            Document raw = SystemCollections.CONFIG_CONNECTIONS.on(database).find().first();
            assertThat(raw.keySet()).containsExactlyInAnyOrder("_id", "formatVersion", "envelope");
            assertThat(raw.toJson()).doesNotContain("private-user", "private-password", "secret-sentinel", "123.4500");
            assertThat(revision(database)).isOne();
            try (var reopened = MongoConfigurationDomain.open(settings(database), "cluster-a")) {
                var restored = reopened.catalog().get("named").orElseThrow();
                assertThat(restored.id()).isEqualTo("named");
                assertThat(restored.connectorId()).isEqualTo("mongodb");
                assertThat(restored.settings()).containsEntry("uri", original.settings().get("uri"))
                        .containsEntry("long", 5L).containsEntry("integer", 5)
                        .containsEntry("decimal", new BigDecimal("123.4500"))
                        .containsEntry("nested", settings.get("nested"));
                assertThat(restored.settings().get("long")).isInstanceOf(Long.class);
                assertThat(restored.settings().get("integer")).isInstanceOf(Integer.class);
                assertThat((byte[]) restored.settings().get("binary")).containsExactly((byte) 1, (byte) 2, (byte) 3);
                assertThat(reopened.catalog().list()).hasSize(1);
            }
            assertThat(database.listCollectionNames().into(new ArrayList<>()))
                    .doesNotContain("system_meta", "workload_claims", "operator_state", "sessions", "srs_meta");
        });
    }

    @Test
    void bsonLookingUserMapsRemainMapsRatherThanBecomingTypedValues() {
        withConfiguration((domain, database) -> {
            Map<String, Object> settings = Map.of(
                    "longMap", Map.of("$numberLong", "7"),
                    "dateMap", Map.of("$date", "2026-01-01T00:00:00Z"),
                    "binaryMap", Map.of("$binary", Map.of("base64", "AQID", "subType", "00")));
            domain.catalog().save(new ConnectionConfig("reserved", "mongodb", settings));
            assertThat(domain.catalog().get("reserved").orElseThrow().settings()).isEqualTo(settings);
        });
    }

    @Test
    void unrepresentableInputDoesNotExposeItsValueOrChangeConfiguration() {
        withConfiguration((domain, database) -> {
            domain.catalog().save(connection("first", "first-secret"));
            Document before = SystemCollections.CONFIG_CONNECTIONS.on(database).find().first();
            long version = revision(database);
            for (Object unsupported : List.of(new BigDecimal("123456789012345678901234567890123456789"),
                    org.bson.types.Decimal128.NaN, org.bson.types.Decimal128.POSITIVE_INFINITY, new Object())) {
                assertThatThrownBy(() -> domain.catalog().save(new ConnectionConfig("first", "mongodb", Map.of("secret", unsupported))))
                        .isInstanceOfSatisfying(TapstateException.class, error -> {
                            assertThat(error.code()).isEqualTo(IoError.DOCUMENT_INVALID);
                            assertThat(error.getMessage()).doesNotContain("123456789012345678901234567890123456789");
                            assertThat(error.getCause()).isNull();
                        });
            }
            assertThat(SystemCollections.CONFIG_CONNECTIONS.on(database).find().first()).isEqualTo(before);
            assertThat(revision(database)).isEqualTo(version);
        });
    }

    @Test
    void authenticatedNonFiniteDecimalIsUnreadableWithoutAnUncodedNormalizationFailure() {
        withConfiguration((domain, database) -> {
            domain.catalog().save(connection("first", "first-secret"));
            String material = SystemCollections.CLUSTER_KEYS.on(database).find().first().getString("material");
            SourceConfigCipher cipher = new SourceConfigCipher(Base64.getUrlDecoder().decode(material));
            var raw = new org.bson.RawBsonDocument(new Document("_id", "first").append("connectorId", "mongodb")
                    .append("settings", new Document("secret", org.bson.types.Decimal128.NaN)), new org.bson.codecs.DocumentCodec());
            byte[] payload = java.util.Arrays.copyOfRange(raw.getBackingArray(), raw.getByteOffset(), raw.getByteOffset() + raw.getByteLength());
            String envelope = cipher.encryptBytes(JsonWriter.write(List.of("connections", "first")), "connection-catalog", payload);
            SystemCollections.CONFIG_CONNECTIONS.on(database).updateOne(new Document("_id", "first"),
                    new Document("$set", new Document("envelope", envelope)));
            assertThatThrownBy(() -> domain.catalog().get("first"))
                    .satisfies(MongoConfigurationCatalogStoreIT::unreadable)
                    .hasNoCause();
        });
    }

    @Test
    void failedCatalogWriteRollsBackItsRevisionAndOriginalKeyFence() {
        withConfiguration((domain, database) -> {
            domain.catalog().save(connection("first", "first-secret"));
            long before = revision(database);
            long fence = SystemCollections.CLUSTER_KEYS.on(database).find().first().getLong("sourceWriteFence");
            SystemCollections.CONFIG_CONNECTIONS.on(database).createIndex(new Document("formatVersion", 1),
                    new IndexOptions().unique(true));
            assertThatThrownBy(() -> domain.catalog().save(connection("second", "second-secret"))).isInstanceOf(TapstateException.class);
            assertThat(domain.catalog().get("second")).isEmpty();
            assertThat(domain.catalog().get("first")).contains(connection("first", "first-secret"));
            assertThat(revision(database)).isEqualTo(before);
            assertThat(SystemCollections.CLUSTER_KEYS.on(database).find().first().getLong("sourceWriteFence")).isEqualTo(fence);
        });
    }

    @Test
    void twoIndependentClientsReplaceWholeDefinitionsAndAdvanceOneSharedRevisionEach() {
        withConfiguration((domain, database) -> {
            var first = connection("same", "first-secret");
            var second = connection("same", "second-secret");
            try (var other = MongoConfigurationDomain.open(settings(database), "cluster-a");
                    var executor = Executors.newFixedThreadPool(2)) {
                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch start = new CountDownLatch(1);
                var a = executor.submit(() -> {
                    ready.countDown();
                    if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start not released");
                    domain.catalog().save(first);
                    return null;
                });
                var b = executor.submit(() -> {
                    ready.countDown();
                    if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start not released");
                    other.catalog().save(second);
                    return null;
                });
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                start.countDown();
                a.get(20, TimeUnit.SECONDS);
                b.get(20, TimeUnit.SECONDS);
                assertThat(domain.catalog().get("same").orElseThrow()).isIn(first, second);
                assertThat(revision(database)).isEqualTo(2);
                assertThat(SystemCollections.CONFIG_CONNECTIONS.on(database).countDocuments()).isOne();
            }
        });
    }

    @Test
    void copyingCiphertextToAnotherIdentityAndPlaintextRowsAreNeverAcceptedOrRepaired() {
        withConfiguration((domain, database) -> {
            domain.catalog().save(connection("first", "first-secret"));
            domain.catalog().save(connection("second", "second-secret"));
            var rows = SystemCollections.CONFIG_CONNECTIONS.on(database);
            String envelope = rows.find(new Document("_id", "first")).first().getString("envelope");
            rows.updateOne(new Document("_id", "second"), new Document("$set", new Document("envelope", envelope)));
            assertThatThrownBy(() -> domain.catalog().get("second")).satisfies(MongoConfigurationCatalogStoreIT::unreadable);
            rows.replaceOne(new Document("_id", "second"), MongoCatalogStore.toDocument(connection("second", "plaintext-secret")));
            assertThatThrownBy(() -> domain.catalog().get("second")).satisfies(MongoConfigurationCatalogStoreIT::unreadable);
            assertThatThrownBy(() -> MongoConfigurationDomain.open(settings(database), "cluster-a"))
                    .satisfies(MongoConfigurationCatalogStoreIT::unreadable);
            assertThat(rows.find(new Document("_id", "second")).first()).containsKey("settings");
        });
    }

    @Test
    void invalidAuthenticatedPayloadDoesNotExposeRejectedSettingsInItsDiagnostic() {
        withConfiguration((domain, database) -> {
            domain.catalog().save(connection("first", "first-secret"));
            String material = SystemCollections.CLUSTER_KEYS.on(database).find().first().getString("material");
            SourceConfigCipher cipher = new SourceConfigCipher(Base64.getUrlDecoder().decode(material));
            String envelope = cipher.encrypt(JsonWriter.write(List.of("connections", "first")), "connection-catalog",
                    "{invalid-json-secret-sentinel");
            SystemCollections.CONFIG_CONNECTIONS.on(database).updateOne(new Document("_id", "first"),
                    new Document("$set", new Document("envelope", envelope)));
            assertThatThrownBy(() -> domain.catalog().get("first")).isInstanceOfSatisfying(TapstateException.class, error -> {
                assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                assertThat(error.getMessage()).doesNotContain("invalid-json-secret-sentinel");
                assertThat(error.getCause()).isNull();
            });
        });
    }

    @Test
    void missingOriginalKeyBlocksAnExistingWriterWithoutChangingItsCommittedDefinition() {
        withConfiguration((domain, database) -> {
            domain.catalog().save(connection("first", "first-secret"));
            Document original = SystemCollections.CONFIG_CONNECTIONS.on(database).find().first();
            long version = revision(database);
            SystemCollections.CLUSTER_KEYS.on(database).deleteMany(new Document());
            assertThatThrownBy(() -> domain.catalog().save(connection("first", "replacement-secret")))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_INVALID));
            assertThat(SystemCollections.CONFIG_CONNECTIONS.on(database).find().first()).isEqualTo(original);
            assertThat(revision(database)).isEqualTo(version);
            assertThat(SystemCollections.CLUSTER_KEYS.on(database).countDocuments()).isZero();
        });
    }

    private static void unreadable(Throwable error) {
        assertThat(error).isInstanceOfSatisfying(TapstateException.class,
                coded -> assertThat(coded.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    private static ConnectionConfig connection(String id, String secret) {
        return new ConnectionConfig(id, "mongodb", Map.of("password", secret, "database", id));
    }

    private static long revision(MongoDatabase database) {
        return SystemCollections.CONFIGURATION_VERSION.on(database).find().first().getLong("revision");
    }

    private static MongoConnectionSettings settings(MongoDatabase database) {
        return new MongoConnectionSettings(MONGO.getReplicaSetUrl(database.getName()), null, Duration.ofSeconds(5));
    }

    private static void withConfiguration(Scenario scenario) {
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = admin.getDatabase("config_catalog_" + UUID.randomUUID().toString().replace("-", ""));
            try (var domain = MongoConfigurationBootstrap.prepare(settings(database), "cluster-a", "installation-a", "setup-a", 1)) {
                scenario.run(domain, database);
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }

    @FunctionalInterface private interface Scenario { void run(MongoConfigurationDomain domain, MongoDatabase database) throws Exception; }
}
