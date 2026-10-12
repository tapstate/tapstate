package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.StoredArtifact;
import io.tapstate.control.restapi.ArtifactList;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.json.JsonMapper;

import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Whole-config storage and protected HTTP reads over real Mongo; the Cloud identity is controlled. */
@RequiresDocker
class SourceConfigEncryptionIT {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final CanonicalWriter WRITER = new CanonicalWriter();
    private static final DslParser PARSER = new DslParser();
    private static final List<String> CONNECTORS = List.of(
            "mongodb", "mongodb-atlas", "mysql", "oracle", "aws-rds-mysql", "unregistered");

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @TempDir Path work;

    @ParameterizedTest
    @EnumSource(CloudRuntimeSettings.Mode.class)
    void everyConnectorAndEmptyConfigStayEncryptedWhileInternalAndHttpReadsSurviveRestart(
            CloudRuntimeSettings.Mode mode) {
        boolean cloud = mode == CloudRuntimeSettings.Mode.CLOUD;
        String database = "config_storage_" + Long.toUnsignedString(System.nanoTime(), 16);
        String uri = MONGO.getReplicaSetUrl(database);
        Map<String, Resource> expected = new LinkedHashMap<>();
        for (int index = 0; index < CONNECTORS.size(); index++) {
            String connector = CONNECTORS.get(index);
            String id = "encrypted_source_" + index;
            expected.put(id, PARSER.parse("""
                    version: tapstate/v1
                    kind: source
                    id: %s
                    connector: %s
                    config:
                      uri: mongodb://uri-user:uri-password-sentinel@host.example/data
                      password: config-password-sentinel
                      publicValue: whole-config-public-sentinel
                      nested: { tlsKey: nested-secret-sentinel, extra: unmarked-nested-sentinel }
                      flags: [true, false, 123]
                    """.formatted(id, connector)));
        }
        expected.put("empty_source", PARSER.parse("""
                version: tapstate/v1
                kind: source
                id: empty_source
                connector: unregistered
                config: {}
                """));
        Document originalKeyring;
        String credential;
        try (var context = start(cloud, database, uri)) {
            ArtifactStore store = context.getBean(ArtifactStore.class);
            // Internal storage is intentional: this also covers contracts that are not registered.
            // Source admission and PDK delivery are witnessed separately by the shared HTTP suite.
            expected.values().forEach(store::save);
            if (cloud) {
                assertThat(store.get("views")).isEmpty();
            } else {
                // The on-prem startup-owned views Source shares the full encryption/read boundary.
                expected.put("views", store.get("views").orElseThrow());
            }
            credential = verify(context, cloud, uri, database, expected, null);
            try (var raw = MongoClients.create(uri)) {
                originalKeyring = SystemCollections.SYSTEM_META.on(raw.getDatabase(database))
                        .find(new Document("_id", "source-config-keyring")).first();
                assertThat(originalKeyring).isNotNull();
            }
        }
        try (var restarted = start(cloud, database, uri)) {
            if (cloud) assertThat(restarted.getBean(ArtifactStore.class).get("views")).isEmpty();
            verify(restarted, cloud, uri, database, expected, credential);
            try (var raw = MongoClients.create(uri)) {
                assertThat(SystemCollections.SYSTEM_META.on(raw.getDatabase(database))
                        .find(new Document("_id", "source-config-keyring")).first()).isEqualTo(originalKeyring);
            }
        }
    }

    private static String verify(ConfigurableApplicationContext context, boolean cloud, String uri,
            String database, Map<String, Resource> expected, String existingCredential) {
        ArtifactStore store = context.getBean(ArtifactStore.class);
        RestClient client = RestClient.builder().baseUrl("http://127.0.0.1:"
                        + ((WebServerApplicationContext) context).getWebServer().getPort())
                .requestFactory(new JdkClientHttpRequestFactory(HttpClient.newBuilder()
                        .followRedirects(HttpClient.Redirect.NEVER).build())).build();
        // Cloud restart keeps the local cookie without redeeming the one-time handoff again.
        // On-prem retains its existing login/signing-key behavior and signs a fresh local JWT.
        String credential = cloud && existingCredential != null
                ? existingCredential : login(client, cloud, existingCredential == null);
        RestClient authorized = client.mutate().defaultHeader(cloud ? HttpHeaders.COOKIE : HttpHeaders.AUTHORIZATION,
                cloud ? credential : "Bearer " + credential).build();
        try (var raw = MongoClients.create(uri)) {
            for (var entry : expected.entrySet()) {
                Resource decrypted = store.get(entry.getKey()).orElseThrow();
                assertThat(WRITER.tree(decrypted).get("config")).isEqualTo(WRITER.tree(entry.getValue()).get("config"));
                assertThat(CanonicalHash.of(decrypted)).isEqualTo(CanonicalHash.of(entry.getValue()));
                Document document = SystemCollections.ARTIFACTS.on(raw.getDatabase(database))
                        .find(new Document("_id", entry.getKey())).first();
                assertThat(document.get("body", Document.class).get("config"))
                        .isInstanceOf(String.class).asString().startsWith("tscfg:1:");
                assertThat(document.getString("contentHash")).isEqualTo(CanonicalHash.of(entry.getValue()));
                noPlaintext(document.toJson());
                StoredArtifact view = authorized.get().uri("/api/artifacts/" + entry.getKey())
                        .retrieve().body(StoredArtifact.class);
                assertThat(view).isNotNull();
                assertThat(view.id()).isEqualTo(entry.getKey());
                assertThat(view.kind()).isEqualTo("source");
                assertThat(view.contentHash()).isEqualTo(CanonicalHash.of(entry.getValue()));
                readableProjection(view.canonicalForm(), entry.getValue());
            }
            ArtifactList list = authorized.get().uri("/api/artifacts?kind=source").retrieve().body(ArtifactList.class);
            assertThat(list).isNotNull();
            assertThat(list.artifacts()).extracting(row -> row.id()).containsExactlyInAnyOrderElementsOf(expected.keySet());
            for (var row : list.artifacts()) {
                assertThat(row.kind()).isEqualTo("source");
                assertThat(row.readable()).isTrue();
                Resource source = expected.get(row.id());
                assertThat(row.contentHash()).isEqualTo(CanonicalHash.of(source));
                readableProjection(row.canonicalForm(), source);
            }
        }
        return credential;
    }

    private static void noPlaintext(String value) {
        assertThat(value).doesNotContain("uri-user", "uri-password-sentinel", "host.example",
                "config-password-sentinel", "whole-config-public-sentinel", "nested-secret-sentinel",
                "unmarked-nested-sentinel");
    }

    private static void readableProjection(String canonical, Resource expected) {
        assertThat(canonical).isNotBlank().doesNotContain("config:", "tscfg:", "<redacted-source>");
        noPlaintext(canonical);
        Resource parsed = PARSER.parse(canonical);
        assertThat(parsed.id()).isEqualTo(expected.id());
        assertThat(parsed.kind()).isEqualTo(expected.kind());
        assertThat(WRITER.tree(parsed).get("connector")).isEqualTo(WRITER.tree(expected).get("connector"));
    }

    private ConfigurableApplicationContext start(boolean cloud, String database, String uri) {
        List<String> args = new ArrayList<>(List.of(
                "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR",
                "--tapstate.test.source-modes=true", "--SDK_STATUS_SENDER_ENABLED=false",
                "--spring.config.location=optional:classpath:/application.properties",
                "--tapstate.store.mongo.operator-state-database=" + database + "_ops",
                "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"),
                "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory()));
        if (cloud) args.addAll(List.of("--tapstate.cloud.base-url=https://cloud.example",
                "--tapstate.cloud.token=controlled-outbound-token", "--tapstate.cloud.atlas-uri=" + uri,
                "--tapstate.cloud.cluster-id=shared-source-cluster"));
        else args.add("--tapstate.store.mongo.uri=" + uri);
        StandardEnvironment environment = CloudFixtureEnvironment.isolated();
        return new SpringApplicationBuilder(SharedConnectorSourceModesIT.Assembly.class).environment(environment)
                .run(args.toArray(String[]::new));
    }

    private static String login(RestClient client, boolean cloud, boolean firstBoot) {
        if (cloud) return client.get().uri("/auth/exchange?code=controlled-code")
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode().value()).isEqualTo(302);
                    String cookie = response.getHeaders().getFirst(HttpHeaders.SET_COOKIE);
                    assertThat(cookie).contains("HttpOnly", "Secure");
                    return cookie.substring(0, cookie.indexOf(';'));
                });
        if (firstBoot) {
            client.post().uri("/auth/bootstrap").contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("username", "crypto-admin", "password", "crypto-local-password"))
                    .exchange((request, response) -> {
                        assertThat(response.getStatusCode().value()).isEqualTo(204);
                        return null;
                    });
        }
        String body = client.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "crypto-admin", "password", "crypto-local-password"))
                .retrieve().body(String.class);
        String token = JSON.readTree(body).path("token").asText();
        assertThat(token).isNotBlank();
        return token;
    }
}
