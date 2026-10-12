package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.StoreError;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** A real packaged server must reject corrupt encryption state before reporting application readiness. */
@RequiresDocker
class SourceConfigStartupRefusalIT {

    private static final String SOURCE = "startup_encrypted_source";
    private static final String MARKER = "startup-config-plaintext-sentinel";
    private static final String READY = "Tapstate application is ready";
    private static final Duration STARTUP = Duration.ofSeconds(45);
    private static MongoClient client;
    private static MongoDatabase database;
    private static String uri;
    private static Document originalKeyring;
    private static Document originalSource;

    enum Damage {
        MISSING_KEYRING,
        MALFORMED_KEYRING,
        UNKNOWN_ENVELOPE_VERSION,
        TAMPERED_CIPHERTEXT,
        WRONG_CONNECTOR_CONTEXT,
        INDEXED_KIND_MISMATCH,
        PLAINTEXT_AFTER_MIGRATION
    }

    @BeforeAll
    static void createRealEncryptedStoreOnce() {
        String name = "source_startup_" + UUID.randomUUID().toString().replace("-", "");
        uri = SharedMongo.replicaSetUrl(name);
        client = MongoClients.create(uri);
        database = client.getDatabase(name);
        try (RealProcessServer seed = RealProcessServer.start(uri)) {
            seed.awaitReady();
            ControlPlane control = new ControlPlane(seed.baseUrl());
            control.bootstrapAndLogin("startup-admin", "startup-local-password");
            control.apply(Map.of("encrypted.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: %s
                    connector: mongodb
                    config:
                      uri: mongodb://fake-user:fake-password@localhost/data
                      publicValue: %s
                    """.formatted(SOURCE, MARKER)));
            originalKeyring = SystemCollections.SYSTEM_META.on(database)
                    .find(new Document("_id", "source-config-keyring")).first();
            originalSource = SystemCollections.ARTIFACTS.on(database)
                    .find(new Document("_id", SOURCE)).first();
            assertThat(originalKeyring).isNotNull();
            assertThat(originalSource).isNotNull();
            assertThat(originalSource.get("body", Document.class).getString("config")).startsWith("tscfg:1:");
            assertThat(originalSource.toJson()).doesNotContain(MARKER, "fake-password");
        }
    }

    @AfterAll
    static void closeClient() {
        if (client != null) client.close();
    }

    @ParameterizedTest
    @EnumSource(Damage.class)
    void damagedKeyringOrEncryptedSourceNeverReachesReadyAndDiagnosticsStaySafe(Damage damage) {
        try {
            corrupt(damage);
            try (RealProcessServer restarting = RealProcessServer.launching(uri)) {
                Await.until("the packaged server to reject " + damage, STARTUP,
                        () -> !restarting.isAlive() || output(restarting).contains(READY), restarting::tail);
                assertThat(restarting.isAlive()).as("corrupt encryption state cannot report ready").isFalse();
                assertThat(restarting.exitValue()).isEqualTo(1);
                String expected = damage == Damage.MISSING_KEYRING || damage == Damage.MALFORMED_KEYRING
                        ? StoreError.SOURCE_CONFIG_KEYRING_INVALID.code() : IoError.DOCUMENT_UNREADABLE.code();
                String logs = output(restarting);
                assertThat(logs).contains(expected).doesNotContain(READY, MARKER, "fake-password",
                        originalKeyring.getList("keys", Document.class).getFirst().getString("material"));
            }
        } finally {
            // No server remains alive while restoring the two fixture-owned records.
            SystemCollections.SYSTEM_META.on(database).replaceOne(new Document("_id", "source-config-keyring"),
                    originalKeyring, new com.mongodb.client.model.ReplaceOptions().upsert(true));
            SystemCollections.ARTIFACTS.on(database).replaceOne(new Document("_id", SOURCE), originalSource);
        }
    }

    @Test
    void anIndexedKindChangedAfterStartupCannotExposeSourcePlaintextThroughTheHttpList() throws Exception {
        try (RealProcessServer running = RealProcessServer.start(uri);
                HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()) {
            running.awaitReady();
            HttpRequest login = HttpRequest.newBuilder(running.baseUrl().resolve("/auth/login"))
                    .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("""
                            {"username":"startup-admin","password":"startup-local-password"}
                            """)).build();
            HttpResponse<String> signedIn = http.send(login, HttpResponse.BodyHandlers.ofString());
            assertThat(signedIn.statusCode()).isEqualTo(200);
            Map<?, ?> credentials = (Map<?, ?>) JsonReader.parse(signedIn.body());
            assertThat(credentials.get("token")).isInstanceOf(String.class).asString().isNotBlank();
            corrupt(Damage.INDEXED_KIND_MISMATCH);
            HttpRequest list = HttpRequest.newBuilder(running.baseUrl().resolve("/api/artifacts"))
                    .timeout(Duration.ofSeconds(5)).header("Authorization", "Bearer " + credentials.get("token"))
                    .GET().build();
            HttpResponse<String> response = http.send(list, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body().contains(MARKER)).as("public list contains no stored config plaintext").isFalse();
            assertThat(response.body().contains("fake-password")).isFalse();
            Map<?, ?> body = (Map<?, ?>) JsonReader.parse(response.body());
            List<?> rows = (List<?>) body.get("artifacts");
            assertThat(rows).isNotEmpty();
            List<?> matches = rows.stream().filter(row -> row instanceof Map<?, ?> value
                    && SOURCE.equals(value.get("id"))).toList();
            assertThat(matches).hasSize(1);
            Map<?, ?> unreadable = (Map<?, ?>) matches.getFirst();
            assertThat(unreadable.get("kind")).isEqualTo("pipeline");
            assertThat(unreadable.get("readable")).isEqualTo(false);
            assertThat(unreadable.get("canonicalForm")).isNull();
            assertThat(unreadable.get("contentHash")).isEqualTo(originalSource.getString("contentHash"));
        } finally {
            SystemCollections.ARTIFACTS.on(database).replaceOne(new Document("_id", SOURCE), originalSource);
        }
    }

    private static void corrupt(Damage damage) {
        if (damage == Damage.MISSING_KEYRING) {
            SystemCollections.SYSTEM_META.on(database).deleteOne(new Document("_id", "source-config-keyring"));
            return;
        }
        if (damage == Damage.MALFORMED_KEYRING) {
            SystemCollections.SYSTEM_META.on(database).updateOne(new Document("_id", "source-config-keyring"),
                    new Document("$set", new Document("keys.0.material", new Document("input", MARKER))));
            return;
        }
        String encrypted = originalSource.get("body", Document.class).getString("config");
        Object replacement = switch (damage) {
            case UNKNOWN_ENVELOPE_VERSION -> encrypted.replace("tscfg:1:", "tscfg:2:");
            case TAMPERED_CIPHERTEXT -> tamper(encrypted);
            case PLAINTEXT_AFTER_MIGRATION -> new Document("uri", "mongodb://fake-user:fake-password@localhost/data")
                    .append("publicValue", MARKER);
            case WRONG_CONNECTOR_CONTEXT -> encrypted;
            case INDEXED_KIND_MISMATCH -> encrypted;
            case MISSING_KEYRING, MALFORMED_KEYRING -> throw new AssertionError("handled above");
        };
        Document fields = new Document("body.config", replacement);
        if (damage == Damage.WRONG_CONNECTOR_CONTEXT) fields.append("body.connector", "mysql");
        if (damage == Damage.INDEXED_KIND_MISMATCH) fields.append("kind", "pipeline");
        SystemCollections.ARTIFACTS.on(database).updateOne(new Document("_id", SOURCE), new Document("$set", fields));
    }

    private static String tamper(String envelope) {
        String[] fields = envelope.split(":", -1);
        byte[] ciphertext = Base64.getUrlDecoder().decode(fields[3]);
        ciphertext[ciphertext.length - 1] ^= 1;
        fields[3] = Base64.getUrlEncoder().withoutPadding().encodeToString(ciphertext);
        return String.join(":", fields);
    }

    private static String output(RealProcessServer server) {
        try {
            return Files.readString(server.output());
        } catch (IOException failed) {
            throw new UncheckedIOException("could not read the encryption startup witness", failed);
        }
    }
}
