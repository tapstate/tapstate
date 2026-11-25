package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.control.core.ControlError;
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

import static org.assertj.core.api.Assertions.assertThat;

/** A real packaged server keeps safe diagnostics but refuses admission when encryption state is corrupt. */
@RequiresDocker
class SourceConfigStartupRefusalIT {

    private static final String SOURCE = "startup_encrypted_source";
    private static final String MARKER = "startup-config-plaintext-sentinel";
    private static final String READY = "Tapstate application is ready";
    private static final Duration STARTUP = Duration.ofSeconds(45);
    private static MongoClient client;
    private static MongoDatabase database;
    private static String uri;
    private static PreparedNativeStore storage;
    private static Document originalKeyring;
    private static Document originalSource;
    private static String originalCredential;

    enum Damage {
        MISSING_KEYRING,
        MALFORMED_KEYRING,
        UNKNOWN_ENVELOPE_VERSION,
        TAMPERED_CIPHERTEXT,
        WRONG_CONNECTOR_CONTEXT,
        INDEXED_KIND_MISMATCH,
        PLAINTEXT_SOURCE_CONFIG
    }

    @BeforeAll
    static void createRealEncryptedStoreOnce() {
        storage = PreparedNativeStore.create("startup-admin", "startup-local-password");
        uri = storage.configurationUri();
        client = MongoClients.create(uri);
        database = client.getDatabase(storage.configurationDatabase());
        try (ServerHandle seed = Tiers.REAL_PROCESS.launch(storage)) {
            ControlPlane control = new ControlPlane(seed.baseUrl());
            storage.loginAndAwaitBusiness(control);
            originalCredential = control.credential();
            control.apply(Map.of("encrypted.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: %s
                    connector: mongodb
                    config:
                      uri: mongodb://fake-user:fake-password@localhost/data
                      publicValue: %s
                    """.formatted(SOURCE, MARKER)));
            originalKeyring = SystemCollections.CLUSTER_KEYS.on(database)
                    .find(new Document("_id", "cluster")).first();
            originalSource = SystemCollections.SOURCES.on(database)
                    .find(new Document("_id", SOURCE)).first();
            assertThat(originalKeyring).isNotNull();
            assertThat(originalSource).isNotNull();
            assertThat(originalSource.get("body", Document.class).getString("config")).startsWith("tscfg:1:");
            assertThat(originalSource.toJson()).doesNotContain(MARKER, "fake-password");
        }
    }

    @AfterAll
    static void closeClient() {
        try { if (client != null) client.close(); }
        finally { if (storage != null) storage.close(); }
    }

    @ParameterizedTest
    @EnumSource(Damage.class)
    void damagedKeyringOrEncryptedSourceRefusesAdmissionAndPreservesStoredBytes(Damage damage) throws Exception {
        try {
            corrupt(damage);
            Document damagedKey = SystemCollections.CLUSTER_KEYS.on(database)
                    .find(new Document("_id", "cluster")).first();
            Document damagedSource = SystemCollections.SOURCES.on(database)
                    .find(new Document("_id", SOURCE)).first();
            try (RealProcessServer restarting = RealProcessServer.launchingPrepared(uri, "127.0.0.1",
                    port -> List.of("--tapstate.cluster.id=" + storage.clusterId(), "--tapstate.hz.member-port=0",
                            "--tapstate.hz.jet.cooperative-thread-count=2"));
                 HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()) {
                Await.until("the diagnostic process to finish startup with " + damage, STARTUP,
                        () -> output(restarting).contains(READY) && new ControlPlane(restarting.baseUrl()).healthy(),
                        restarting::tail);
                assertThat(restarting.isAlive()).as("the minimum root remains available for diagnostics").isTrue();
                var entry = http.send(HttpRequest.newBuilder(restarting.baseUrl().resolve("/auth/storage-entry"))
                        .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
                assertThat(entry.statusCode()).isEqualTo(200);
                assertThat(Document.parse(entry.body())).containsEntry("entry", "UNAVAILABLE");
                for (String path : List.of("/auth/login", "/auth/bootstrap", "/api/artifacts")) {
                    var request = HttpRequest.newBuilder(restarting.baseUrl().resolve(path))
                            .timeout(Duration.ofSeconds(5));
                    if (path.equals("/api/artifacts")) request.header("Authorization", "Bearer " + originalCredential).GET();
                    else request.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString("""
                            {"username":"startup-admin","password":"startup-local-password"}
                            """));
                    var response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
                    boolean bootstrap = path.equals("/auth/bootstrap");
                    assertThat(response.statusCode()).as(path).isEqualTo(bootstrap ? 403 : 503);
                    assertThat(ControlPlane.interpretClusterRefusal(response.statusCode(), response.body()))
                            .contains((bootstrap ? ControlError.BOOTSTRAP_FORBIDDEN : ControlError.STORAGE_UNAVAILABLE).code());
                    assertThat(response.body()).doesNotContain(MARKER, "fake-password", originalKeyring.getString("material"));
                }
                assertThat(output(restarting)).doesNotContain(MARKER, "fake-password", originalKeyring.getString("material"));
            }
            // Compare before fixture restoration, including an absent key: refusal must never repair the data.
            assertThat(SystemCollections.CLUSTER_KEYS.on(database).find(new Document("_id", "cluster")).first())
                    .as("the original damaged or absent key is preserved").isEqualTo(damagedKey);
            assertThat(SystemCollections.SOURCES.on(database).find(new Document("_id", SOURCE)).first())
                    .as("the original damaged encrypted source is preserved").isEqualTo(damagedSource);
        } finally {
            // No server remains alive while restoring the two fixture-owned records.
            SystemCollections.CLUSTER_KEYS.on(database).replaceOne(new Document("_id", "cluster"),
                    originalKeyring, new com.mongodb.client.model.ReplaceOptions().upsert(true));
            SystemCollections.SOURCES.on(database).replaceOne(new Document("_id", SOURCE), originalSource);
        }
    }

    @Test
    void anIndexedKindChangedAfterStartupCannotExposeSourcePlaintextThroughTheHttpList() throws Exception {
        try (ServerHandle running = Tiers.REAL_PROCESS.launch(storage);
                HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()) {
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
            Document damaged = SystemCollections.SOURCES.on(database).find(new Document("_id", SOURCE)).first();
            HttpRequest list = HttpRequest.newBuilder(running.baseUrl().resolve("/api/artifacts"))
                    .timeout(Duration.ofSeconds(5)).header("Authorization", "Bearer " + credentials.get("token"))
                    .GET().build();
            HttpResponse<String> response = http.send(list, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(500);
            assertThat(response.body().contains(MARKER)).as("public list contains no stored config plaintext").isFalse();
            assertThat(response.body().contains("fake-password")).isFalse();
            assertThat(ControlPlane.interpretClusterRefusal(response.statusCode(), response.body()))
                    .contains(IoError.DOCUMENT_UNREADABLE.code());
            assertThat(SystemCollections.SOURCES.on(database).find(new Document("_id", SOURCE)).first())
                    .as("the read refuses a kind that disagrees with its native table without rewriting it")
                    .isEqualTo(damaged);
        } finally {
            SystemCollections.SOURCES.on(database).replaceOne(new Document("_id", SOURCE), originalSource);
        }
    }

    private static void corrupt(Damage damage) {
        if (damage == Damage.MISSING_KEYRING) {
            SystemCollections.CLUSTER_KEYS.on(database).deleteOne(new Document("_id", "cluster"));
            return;
        }
        if (damage == Damage.MALFORMED_KEYRING) {
            SystemCollections.CLUSTER_KEYS.on(database).updateOne(new Document("_id", "cluster"),
                    new Document("$set", new Document("material", new Document("input", MARKER))));
            return;
        }
        String encrypted = originalSource.get("body", Document.class).getString("config");
        Object replacement = switch (damage) {
            case UNKNOWN_ENVELOPE_VERSION -> encrypted.replace("tscfg:1:", "tscfg:2:");
            case TAMPERED_CIPHERTEXT -> tamper(encrypted);
            case PLAINTEXT_SOURCE_CONFIG -> new Document("uri", "mongodb://fake-user:fake-password@localhost/data")
                    .append("publicValue", MARKER);
            case WRONG_CONNECTOR_CONTEXT -> encrypted;
            case INDEXED_KIND_MISMATCH -> encrypted;
            case MISSING_KEYRING, MALFORMED_KEYRING -> throw new AssertionError("handled above");
        };
        Document fields = new Document("body.config", replacement);
        if (damage == Damage.WRONG_CONNECTOR_CONTEXT) fields.append("body.connector", "mysql");
        if (damage == Damage.INDEXED_KIND_MISMATCH) fields.append("kind", "pipeline");
        SystemCollections.SOURCES.on(database).updateOne(new Document("_id", SOURCE), new Document("$set", fields));
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
