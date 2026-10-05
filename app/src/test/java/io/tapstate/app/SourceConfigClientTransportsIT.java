package io.tapstate.app;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.read.ListAppender;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.cli.Cli;
import io.tapstate.control.core.StoredArtifact;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.json.JsonMapper;

import java.io.BufferedReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual CLI and isolated stdio MCP processes over encrypted Mongo-backed HTTP. */
@RequiresDocker
class SourceConfigClientTransportsIT {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final DslParser PARSER = new DslParser();
    private static final String SECRET = "transport-stored-password-sentinel";
    private static final String REPLACEMENT = "transport-replacement-password-sentinel";
    private static final String CALLER = "transport-caller-password-sentinel";
    private static final String ENV_SECRET = "transport-env-password-sentinel";
    private static final String UNMARKED = "transport-unmarked-config-sentinel";

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @TempDir Path work;
    private int processSequence;

    @ParameterizedTest(name = "{0}")
    @MethodSource("cliFlavors")
    void cliSavedConnectionProbesReceiveTheWholeDecryptedConfigurationWithoutReturningIt(CliFlavor flavor)
            throws Exception {
        try (Fixture fixture = start(false)) {
            String token = fixture.mint("write");
            var probes = fixture.context.getBean(SharedConnectorSourceModesIT.RecordingProbes.class);
            for (String connector : List.of("mongodb", "mongodb-atlas", "mysql", "oracle", "aws-rds-mysql")) {
                String id = "cli_probe_" + connector.replace('-', '_');
                Map<String, Object> config = config(connector, SECRET);
                success(cli(fixture, flavor, token, "apply", file(source(id, connector, config)).toString()));
                Document before = fixture.document(id);
                encrypted(fixture, id, config);

                success(cli(fixture, flavor, token, "test", id, "-o", "json"));
                assertThat(probes.tested.id()).isEqualTo(id);
                assertThat(probes.tested.connectorId()).isEqualTo(connector);
                sameConfig(probes.tested.settings(), config);
                success(cli(fixture, flavor, token, "discover-schema", id, "-o", "json"));
                assertThat(probes.discovered.id()).isEqualTo(id);
                assertThat(probes.discovered.connectorId()).isEqualTo(connector);
                sameConfig(probes.discovered.settings(), config);
                assertThat(fixture.document(id)).isEqualTo(before);
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cliFlavors")
    void cliReplayPartialEditsAndExplicitReplacementKeepCiphertextAndRealCas(CliFlavor flavor) throws Exception {
        try (Fixture fixture = start(false)) {
            String token = fixture.mint("write");
            String id = "cli_source";
            Map<String, Object> original = config("mongodb-atlas", SECRET);
            success(cli(fixture, flavor, token, "apply", file(source(id, "mongodb-atlas", original)).toString()));
            encrypted(fixture, id, original);
            Document before = fixture.document(id);
            StoredArtifact displayed = fixture.view(id);
            CliReply get = cli(fixture, flavor, token, "get", id);
            success(get);
            assertThat(get.out.strip()).isEqualTo(displayed.canonicalForm().strip());
            safeProjection(get.out);
            success(cli(fixture, flavor, token, "ls", "source"));

            CliReply replay = cli(fixture, flavor, token, "apply", file(get.out).toString(),
                    "--if-match", displayed.contentHash());
            success(replay);
            assertThat(replay.out).contains("unchanged");
            assertThat(fixture.document(id)).isEqualTo(before);

            String partial = envelope(id, "mongodb-atlas") + "metadata: { description: client-edited }\n";
            success(cli(fixture, flavor, token, "apply", file(partial).toString(), "--if-match", displayed.contentHash()));
            encrypted(fixture, id, original);
            assertThat(fixture.source(id).metadata().description()).isEqualTo("client-edited");
            Document edited = fixture.document(id);
            CliReply stale = cli(fixture, flavor, token, "apply", file(partial).toString(),
                    "--if-match", displayed.contentHash());
            refusal(stale, "artifact.version-conflict");
            assertThat(fixture.document(id)).isEqualTo(edited);

            Map<String, Object> replacement = Map.of("uri", uri(REPLACEMENT));
            success(cli(fixture, flavor, token, "apply", file(source(id, "mongodb-atlas", replacement)).toString()));
            encrypted(fixture, id, replacement);
            assertThat(fixture.source(id).config()).doesNotContainKey("nested");
            success(cli(fixture, flavor, token, "apply", file(source(id, "mongodb-atlas", Map.of())).toString()));
            encrypted(fixture, id, Map.of());
        }
    }

    @Test
    void mcpWorkspaceEditsAndStoredReadsUseEncryptedConfigWithoutModelVisibleSecrets() throws Exception {
        try (Fixture fixture = start(false);
                McpSession mcp = mcp(fixture, fixture.mint("write"), true)) {
            String id = "mcp_source";
            Map<String, Object> original = config("mongodb-atlas", SECRET);
            Map<?, ?> created = mcp.success("artifact_apply", drafts(source(id, "mongodb-atlas", original), null));
            assertChange(created, id, "CREATED");
            encrypted(fixture, id, original);
            Document before = fixture.document(id);
            Map<?, ?> display = mcp.success("artifact_get", Map.of("id", id));
            String canonical = (String) display.get("canonicalForm");
            String hash = (String) display.get("contentHash");
            safeProjection(canonical);
            assertThat(hash).isEqualTo(CanonicalHash.of(fixture.source(id)));
            assertThat(hash).isNotEqualTo(CanonicalHash.of(PARSER.parse(canonical)));
            Map<?, ?> validation = mcp.success("artifact_validate", drafts(canonical, hash));
            assertThat(validation.get("valid")).isEqualTo(true);
            assertChange(validation, id, "UNCHANGED");
            assertChange(mcp.success("artifact_apply", drafts(canonical, hash)), id, "UNCHANGED");
            assertThat(fixture.document(id)).isEqualTo(before);

            String partial = envelope(id, "mongodb-atlas") + "metadata: { description: mcp-edited }\n";
            assertChange(mcp.success("artifact_apply", drafts(partial, hash)), id, "UPDATED");
            encrypted(fixture, id, original);
            Document edited = fixture.document(id);
            mcp.refused("artifact_apply", drafts(partial, hash), "artifact.version-conflict");
            assertThat(fixture.document(id)).isEqualTo(edited);

            List<Document> beforeRejectedBatch = fixture.snapshot();
            List<Document> auditBeforeRejectedBatch = fixture.audit();
            Map<String, Object> validNew = Map.of("content", source(
                    "rejected_batch_source", "mongodb-atlas", config("mongodb-atlas", REPLACEMENT)));
            Map<String, Object> staleEdit = Map.of("content", partial, "expectedContentHash", hash);
            mcp.refused("artifact_apply", Map.of("drafts", List.of(validNew, staleEdit)), "artifact.version-conflict");
            mcp.refused("artifact_apply", drafts(envelope(id, "mysql"), null), "control.malformed-request");
            mcp.refused("artifact_apply", drafts(source(id, "mongodb-atlas", Map.of("uri", "<redacted>")), null),
                    "control.malformed-request");
            assertThat(fixture.snapshot()).isEqualTo(beforeRejectedBatch);
            assertThat(fixture.audit()).isEqualTo(auditBeforeRejectedBatch);

            Map<String, Object> replacement = Map.of("uri", uri(REPLACEMENT));
            assertChange(mcp.success("artifact_apply", drafts(source(id, "mongodb-atlas", replacement), null)),
                    id, "UPDATED");
            encrypted(fixture, id, replacement);
            assertChange(mcp.success("artifact_apply", drafts(source(id, "mongodb-atlas", Map.of()), null)),
                    id, "UPDATED");
            encrypted(fixture, id, Map.of());

            String serve = "version: tapstate/v1\nkind: serve\nid: transport_serve\n";
            assertChange(mcp.success("artifact_apply", drafts(serve, null)), "transport_serve", "CREATED");
            Map<?, ?> serveView = mcp.success("artifact_get", Map.of("id", "transport_serve"));
            String serveCanonical = (String) serveView.get("canonicalForm");
            String serveHash = (String) serveView.get("contentHash");
            assertThat(serveHash).isEqualTo(CanonicalHash.of(PARSER.parse(serveCanonical)));
            Document serveDocument = fixture.document("transport_serve");
            assertChange(mcp.success("artifact_apply", drafts(serveCanonical, serveHash)), "transport_serve", "UNCHANGED");
            assertThat(fixture.document("transport_serve")).isEqualTo(serveDocument);

            // Unknown contracts and unmarked fields are safe on actual read transports too.
            for (String connector : List.of("mysql", "oracle", "aws-rds-mysql")) {
                String storedId = "inventory_" + connector.replace('-', '_');
                fixture.store.save(PARSER.parse(source(storedId, connector, config(connector, SECRET))));
                encrypted(fixture, storedId, config(connector, SECRET));
                safeProjection((String) mcp.success("artifact_get", Map.of("id", storedId)).get("canonicalForm"));
            }
            Map<?, ?> listed = mcp.success("source_list", Map.of("limit", 200));
            List<?> items = (List<?>) listed.get("items");
            assertThat(items.stream().map(item -> String.valueOf(((Map<?, ?>) item).get("id"))).toList())
                    .contains(id, "inventory_mysql", "inventory_oracle", "inventory_aws_rds_mysql");
            for (Object item : items) assertThat(((Map<?, ?>) item).containsKey("config")).isFalse();
            safe(JSON.writeValueAsString(listed));

            // Generic reads support an unknown contract without consulting its absent catalog entry;
            // typed Source views still require a known catalog. Do not conflate those two contracts.
            fixture.store.save(PARSER.parse(source("inventory_unknown", "unknown", config("unknown", SECRET))));
            encrypted(fixture, "inventory_unknown", config("unknown", SECRET));
            safeProjection((String) mcp.success("artifact_get", Map.of("id", "inventory_unknown")).get("canonicalForm"));
            success(cli(fixture, mcp.token, "get", "inventory_unknown"));
            success(cli(fixture, mcp.token, "ls", "source"));
            fixture.artifacts.updateOne(new Document("_id", "inventory_unknown"),
                    new Document("$set", new Document("body.config", "tscfg:77:bad:" + SECRET)));
            List<Document> snapshot = fixture.snapshot();
            mcp.refused("artifact_get", Map.of("id", "inventory_unknown"), "io.document-unreadable");
            Map<?, ?> afterCorruption = mcp.success("source_list", Map.of("limit", 200));
            safe(JSON.writeValueAsString(afterCorruption));
            assertThat(((List<?>) afterCorruption.get("items")).stream()
                    .map(item -> String.valueOf(((Map<?, ?>) item).get("id"))).toList())
                    .contains(id, "inventory_mysql").doesNotContain("inventory_unknown");
            assertThat(fixture.snapshot()).isEqualTo(snapshot);
        }
    }

    @Test
    void readScopedMcpDraftUsesOnlyCurrentInputAndCannotEnableReservedRevealOrWrites() throws Exception {
        try (Fixture fixture = start(false);
                Logs logs = new Logs();
                McpSession mcp = mcp(fixture, fixture.mint("read"), false)) {
            String id = "draft_source";
            fixture.store.save(PARSER.parse(source(id, "mysql", config("mysql", SECRET))));
            List<Document> artifacts = fixture.snapshot();
            List<Document> audit = fixture.audit();
            List<?> tools = (List<?>) mcp.request("tools/list", Map.of()).get("tools");
            assertThat(tools.stream().map(tool -> String.valueOf(((Map<?, ?>) tool).get("name"))).toList())
                    .contains("source_draft", "artifact_get", "source_list", "artifact_validate")
                    .doesNotContain("artifact_apply", "source_create", "source_get", "source_reveal_config");
            Map<String, Object> draftConfig = config("mysql", CALLER);
            draftConfig.put("username", "caller-owned-user");
            Map<?, ?> draft = mcp.success("source_draft", Map.of(
                    "id", id, "connector", "mysql", "config", draftConfig));
            String yaml = (String) draft.get("yaml");
            assertThat(yaml).contains("id: " + id, "host: transport-db.example").doesNotContain(SECRET, CALLER);
            assertThat(((SourceResource) PARSER.parse(yaml)).config()).doesNotContainKey("password");
            draftConfig = new LinkedHashMap<>(draftConfig);
            draftConfig.put("host", "${TAPSTATE_TRANSPORT_DRAFT_PASSWORD}");
            assertThat((String) mcp.success("source_draft", Map.of(
                    "id", id, "connector", "mysql", "config", draftConfig)).get("yaml"))
                    .contains("${TAPSTATE_TRANSPORT_DRAFT_PASSWORD}").doesNotContain(ENV_SECRET, SECRET);

            // The REST authoring exception returns caller B, never stored A, even with read scope.
            String rest = fixture.readClient(mcp.token).post().uri("/api/sources:draft")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("id", id, "connector", "mysql", "config", config("mysql", CALLER)))
                    .retrieve().body(String.class);
            assertThat(rest).contains(CALLER).doesNotContain(SECRET);
            safeProjection((String) mcp.success("artifact_get", Map.of("id", id)).get("canonicalForm"));

            mcp.unexposed("source_reveal_config", Map.of("id", id, "grant", "forged-grant"));
            // --allow-write exposes write tools but does not upgrade this actual read-scoped credential.
            try (McpSession exposed = mcp(fixture, mcp.token, true)) {
                exposed.refused("artifact_apply", drafts(source(id, "mysql", config("mysql", REPLACEMENT)), null),
                        "control.forbidden");
            }
            assertThat(fixture.snapshot()).isEqualTo(artifacts);
            assertThat(fixture.audit()).isEqualTo(audit);
            encrypted(fixture, id, config("mysql", SECRET));
            safe(logs.text());
        }
    }

    @Test
    void actualBearerClientsCannotReadOrMutateCloudEvenWithValidLocalCredentials() throws Exception {
        String localToken;
        try (Fixture onprem = start(false)) {
            localToken = onprem.mint("write");
        }
        try (Fixture cloud = start(true)) {
            String id = "cloud_transport_source";
            cloud.store.save(PARSER.parse(source(id, "mongodb-atlas", config("mongodb-atlas", SECRET))));
            assertThat(cloud.view(id).id()).isEqualTo(id); // A real local Cloud Cookie remains usable.
            List<Document> before = cloud.snapshot();
            List<Document> audit = cloud.audit();
            for (String credential : List.of(localToken, "controlled-outbound-token", cloud.credential)) {
                for (CliFlavor flavor : cliFlavors()) {
                    refusal(cli(cloud, flavor, credential, "get", id), "control.unauthenticated");
                    refusal(cli(cloud, flavor, credential, "apply", file(source(
                            "cloud_rejected_source", "mongodb-atlas", config("mongodb-atlas", REPLACEMENT))).toString()),
                            "control.unauthenticated");
                }
                try (McpSession mcp = mcp(cloud, credential, true)) {
                    mcp.refused("artifact_get", Map.of("id", id), "control.unauthenticated");
                    mcp.refused("source_list", Map.of(), "control.unauthenticated");
                    mcp.refused("artifact_apply", drafts(source(
                            "cloud_rejected_source", "mongodb-atlas", config("mongodb-atlas", REPLACEMENT)), null),
                            "control.unauthenticated");
                }
            }
            assertThat(cloud.snapshot()).isEqualTo(before);
            assertThat(cloud.audit()).isEqualTo(audit);
            encrypted(cloud, id, config("mongodb-atlas", SECRET));
        }
    }

    private static void assertChange(Map<?, ?> body, String id, String expected) {
        List<?> outcomes = (List<?>) body.get("outcomes");
        Map<?, ?> outcome = outcomes.stream().map(Map.class::cast)
                .filter(value -> id.equals(value.get("id"))).findFirst().orElseThrow();
        assertThat(outcome.get("change")).isEqualTo(expected);
        assertThat(outcome.get("contentHash")).asString().matches("[0-9a-f]{64}");
    }

    private static Map<String, Object> drafts(String content, String hash) {
        Map<String, Object> draft = new LinkedHashMap<>(Map.of("source", "transport.tap.yml", "content", content));
        if (hash != null) draft.put("expectedContentHash", hash);
        return Map.of("drafts", List.of(draft));
    }

    private static String envelope(String id, String connector) {
        return "version: tapstate/v1\nkind: source\nid: " + id + "\nconnector: " + connector + "\n";
    }

    private static String source(String id, String connector, Map<String, Object> config) {
        return envelope(id, connector) + "config: " + JSON.writeValueAsString(config) + "\n";
    }

    private static String uri(String password) {
        return "mongodb://transport-user:" + password + "@transport-db.example/rows";
    }

    private static Map<String, Object> config(String connector, String password) {
        Map<String, Object> config = new LinkedHashMap<>();
        if (connector.startsWith("mongodb")) {
            config.put("isUri", true);
            config.put("uri", uri(password));
        } else if (connector.equals("oracle")) {
            config.putAll(Map.of("thinType", "SERVICE_NAME", "host", "transport-db.example", "port", "1521",
                    "database", "transport_rows", "schema", "APP", "user", "transport-user", "password", password));
        } else {
            config.putAll(Map.of("host", "transport-db.example", "port", 3306,
                    "database", "transport_rows", "username", "transport-user", "password", password));
        }
        config.put("nested", Map.of("unmarked", UNMARKED));
        return config;
    }

    private static void encrypted(Fixture fixture, String id, Map<String, Object> expected) {
        SourceResource source = fixture.source(id);
        sameConfig(source.config(), expected);
        Document document = fixture.document(id);
        assertThat(document.get("body", Document.class).get("config"))
                .isInstanceOf(String.class).asString().startsWith("tscfg:1:");
        assertThat(document.getString("contentHash")).isEqualTo(CanonicalHash.of(source));
        safe(document.toJson());
        assertThat(document.toJson()).doesNotContain("transport-db.example", "transport_rows", UNMARKED);
    }

    private static void safe(String text) {
        assertThat(text).doesNotContain(SECRET, REPLACEMENT, CALLER, ENV_SECRET, "transport-user");
    }

    private static void sameConfig(Map<String, Object> actual, Map<String, Object> expected) {
        assertThat(JsonReader.parse(JSON.writeValueAsString(actual)))
                .isEqualTo(JsonReader.parse(JSON.writeValueAsString(expected)));
    }

    private static void safeProjection(String text) {
        safe(text);
        assertThat(text).doesNotContain("config:", "tscfg:", UNMARKED, "transport-db.example", "transport_rows");
    }

    private static void success(CliReply reply) {
        assertThat(reply.status).as("the actual CLI process succeeded; stderr=%s", reply.err).isZero();
        safe(reply.out + reply.err);
    }

    private static void refusal(CliReply reply, String code) {
        assertThat(reply.status).isNotZero();
        assertThat(reply.out + reply.err).contains(code);
        safe(reply.out + reply.err);
    }

    private Path file(String yaml) throws Exception {
        Path file = work.resolve("draft-" + (++processSequence) + ".tap.yml");
        Files.writeString(file, yaml);
        return file;
    }

    private CliReply cli(Fixture fixture, String token, String... words) throws Exception {
        return cli(fixture, CliFlavor.JVM, token, words);
    }

    private enum CliFlavor { JVM, NATIVE }

    /** An explicit native input adds real binary cases; the ordinary reactor remains JVM-only. */
    private static List<CliFlavor> cliFlavors() {
        return System.getenv().containsKey("TAPSTATE_TEST_NATIVE_CLI")
                ? List.of(CliFlavor.JVM, CliFlavor.NATIVE) : List.of(CliFlavor.JVM);
    }

    private CliReply cli(Fixture fixture, CliFlavor flavor, String token, String... words) throws Exception {
        Path home = Files.createDirectory(work.resolve("cli-" + (++processSequence)));
        Path stdout = home.resolve("stdout");
        Path stderr = home.resolve("stderr");
        List<String> command = new ArrayList<>();
        if (flavor == CliFlavor.NATIVE) {
            String configured = System.getenv("TAPSTATE_TEST_NATIVE_CLI");
            assertThat(configured).as("the native transport witness requires an explicit binary").isNotBlank();
            Path binary = Path.of(configured);
            assertThat(binary).isAbsolute().isRegularFile().isExecutable();
            command.addAll(List.of(binary.toString(), "-Duser.home=" + home));
        } else {
            String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            command.addAll(List.of(java(), "-Duser.home=" + home, "-cp", classpath, Cli.class.getName()));
        }
        command.addAll(List.of("-c", fixture.baseUrl));
        command.addAll(List.of(words));
        ProcessBuilder builder = new ProcessBuilder(command).directory(home.toFile())
                .redirectOutput(stdout.toFile()).redirectError(stderr.toFile());
        isolatedProcessEnvironment(builder);
        builder.environment().put("TAPSTATE_TOKEN", token);
        Process process = builder.start();
        try {
            assertThat(process.waitFor(30, TimeUnit.SECONDS)).as("the owned CLI process completed").isTrue();
            CliReply reply = new CliReply(process.exitValue(), Files.readString(stdout), Files.readString(stderr));
            assertThat(reply.out + reply.err).as("CLI output must not expose the supplied credential")
                    .doesNotContain(token);
            return reply;
        } finally {
            stop(process);
        }
    }

    private McpSession mcp(Fixture fixture, String token, boolean allowWrite) throws Exception {
        Path home = Files.createDirectory(work.resolve("mcp-" + (++processSequence)));
        Path stderr = home.resolve("stderr");
        String configured = System.getProperty("tapstate.app.mcp-boot-jar");
        assertThat(configured).as("the reactor supplies the isolated MCP executable").isNotBlank();
        Path jar = Path.of(configured);
        assertThat(jar).isRegularFile();
        List<String> command = new ArrayList<>(List.of(java(), "-Duser.home=" + home,
                "-jar", jar.toString()));
        if (allowWrite) command.add("--allow-write");
        ProcessBuilder builder = new ProcessBuilder(command).directory(home.toFile()).redirectError(stderr.toFile());
        isolatedProcessEnvironment(builder);
        builder.environment().put("TAPSTATE_TOKEN", token);
        builder.environment().put("TAPSTATE_SERVER_URL", fixture.baseUrl);
        builder.environment().put("TAPSTATE_TRANSPORT_DRAFT_PASSWORD", ENV_SECRET);
        McpSession session = new McpSession(builder.start(), stderr, token);
        try {
            Map<?, ?> initialized = session.request("initialize", Map.of(
                    "protocolVersion", "2025-06-18", "capabilities", Map.of(),
                    "clientInfo", Map.of("name", "source-transport-witness", "version", "1")));
            assertThat(initialized.get("protocolVersion")).isEqualTo("2025-06-18");
            session.send(Map.of("jsonrpc", "2.0", "method", "notifications/initialized"));
            return session;
        } catch (Exception | AssertionError failure) {
            try { session.close(); } catch (Exception | AssertionError cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private static String java() {
        return Path.of(System.getProperty("java.home"), "bin", "java").toString();
    }

    private static void isolatedProcessEnvironment(ProcessBuilder builder) {
        builder.environment().keySet().removeIf(name -> name.startsWith("TAPSTATE_") || name.startsWith("SPRING_")
                || name.equals("JAVA_TOOL_OPTIONS") || name.equals("JDK_JAVA_OPTIONS") || name.equals("CLUSTER_ID"));
    }

    private static void stop(Process process) throws InterruptedException {
        if (process.isAlive()) {
            process.destroy();
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                assertThat(process.waitFor(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private Fixture start(boolean cloud) {
        String database = "source_transports_" + Long.toUnsignedString(System.nanoTime(), 16);
        String uri = MONGO.getReplicaSetUrl(database);
        List<String> args = new ArrayList<>(List.of(
                "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR",
                "--tapstate.test.source-modes=true", "--SDK_STATUS_SENDER_ENABLED=false",
                "--spring.config.location=optional:classpath:/application.properties",
                "--tapstate.store.mongo.operator-state-database=" + database + "_ops",
                "--tapstate.connectors.plugins-dir=" + work.resolve("plugins-" + database),
                "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory()));
        if (cloud) args.addAll(List.of("--tapstate.cloud.base-url=https://cloud.example",
                "--tapstate.cloud.token=controlled-outbound-token", "--tapstate.cloud.atlas-uri=" + uri,
                "--tapstate.cloud.cluster-id=shared-source-cluster"));
        else args.add("--tapstate.store.mongo.uri=" + uri);
        ConfigurableApplicationContext context = new SpringApplicationBuilder(SharedConnectorSourceModesIT.Assembly.class)
                .environment(CloudFixtureEnvironment.isolated()).run(args.toArray(String[]::new));
        MongoClient raw = null;
        try {
            raw = MongoClients.create(uri);
            String baseUrl = "http://127.0.0.1:" + ((WebServerApplicationContext) context).getWebServer().getPort();
            RestClient anonymous = RestClient.builder().baseUrl(baseUrl)
                    .requestFactory(new JdkClientHttpRequestFactory(HttpClient.newBuilder()
                            .connectTimeout(Duration.ofSeconds(5)).followRedirects(HttpClient.Redirect.NEVER).build()))
                    .build();
            String credential;
            if (cloud) credential = anonymous.get().uri("/auth/exchange?code=controlled-code")
                    .exchange((request, response) -> {
                        assertThat(response.getStatusCode().value()).isEqualTo(302);
                        String cookie = response.getHeaders().getFirst(HttpHeaders.SET_COOKIE);
                        assertThat(cookie).contains("HttpOnly", "Secure");
                        return cookie.substring(0, cookie.indexOf(';'));
                    });
            else {
                anonymous.post().uri("/auth/bootstrap").contentType(MediaType.APPLICATION_JSON)
                        .body(Map.of("username", "admin", "password", "controlled-local-password"))
                        .retrieve().toBodilessEntity();
                credential = JSON.readTree(anonymous.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                        .body(Map.of("username", "admin", "password", "controlled-local-password"))
                        .retrieve().body(String.class)).path("token").asText();
            }
            assertThat(credential).isNotBlank();
            RestClient authorized = anonymous.mutate().defaultHeader(
                    cloud ? HttpHeaders.COOKIE : HttpHeaders.AUTHORIZATION,
                    cloud ? credential : "Bearer " + credential).build();
            return new Fixture(context, raw, database, baseUrl, authorized, anonymous, credential);
        } catch (RuntimeException | Error failure) {
            try { context.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            if (raw != null) try { raw.close(); } catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private record CliReply(int status, String out, String err) { }

    private static final class Fixture implements AutoCloseable {
        final ConfigurableApplicationContext context;
        final MongoClient raw;
        final MongoDatabase database;
        final MongoCollection<Document> artifacts;
        final ArtifactStore store;
        final String baseUrl;
        final RestClient client;
        final RestClient anonymous;
        final String credential;

        Fixture(ConfigurableApplicationContext context, MongoClient raw, String database, String baseUrl,
                RestClient client, RestClient anonymous, String credential) {
            this.context = context;
            this.raw = raw;
            this.database = raw.getDatabase(database);
            this.artifacts = SystemCollections.ARTIFACTS.on(this.database);
            this.store = context.getBean(ArtifactStore.class);
            this.baseUrl = baseUrl;
            this.client = client;
            this.anonymous = anonymous;
            this.credential = credential;
        }

        String mint(String scope) {
            String body = client.post().uri("/api/tokens").contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("scope", scope)).retrieve().body(String.class);
            String token = JSON.readTree(body).path("token").asText();
            assertThat(token).startsWith("cyxt_");
            return token;
        }

        RestClient readClient(String token) {
            return anonymous.mutate().defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token).build();
        }

        SourceResource source(String id) { return (SourceResource) store.get(id).orElseThrow(); }
        Document document(String id) { return artifacts.find(new Document("_id", id)).first(); }
        List<Document> snapshot() { return artifacts.find().sort(new Document("_id", 1)).into(new ArrayList<>()); }
        List<Document> audit() {
            return SystemCollections.AUDIT.on(database).find().sort(new Document("_id", 1)).into(new ArrayList<>());
        }
        StoredArtifact view(String id) {
            return client.get().uri("/api/artifacts/" + id).retrieve().body(StoredArtifact.class);
        }
        @Override public void close() { try { context.close(); } finally { raw.close(); } }
    }

    private static final class McpSession implements AutoCloseable {
        final Process process;
        final Path stderr;
        final String token;
        final Writer input;
        final BufferedReader output;
        final ExecutorService reads = Executors.newSingleThreadExecutor();
        int sequence;

        McpSession(Process process, Path stderr, String token) {
            this.process = process;
            this.stderr = stderr;
            this.token = token;
            this.input = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8);
            this.output = process.inputReader(StandardCharsets.UTF_8);
        }

        void send(Map<String, Object> message) throws Exception {
            input.write(JSON.writeValueAsString(message));
            input.write('\n');
            input.flush();
        }

        Map<?, ?> response(String method, Map<String, Object> params) throws Exception {
            int id = ++sequence;
            send(Map.of("jsonrpc", "2.0", "id", id, "method", method, "params", params));
            String line = reads.submit(output::readLine).get(15, TimeUnit.SECONDS);
            assertThat(line).as("the MCP stdout contains exactly a protocol response").isNotBlank();
            Map<?, ?> response = JSON.readValue(line, Map.class);
            assertThat(response.get("id")).isEqualTo(id);
            return response;
        }

        Map<?, ?> request(String method, Map<String, Object> params) throws Exception {
            Map<?, ?> response = response(method, params);
            assertThat(response.containsKey("error")).as("MCP protocol response: %s", response).isFalse();
            return (Map<?, ?>) response.get("result");
        }

        void unexposed(String tool, Map<String, Object> arguments) throws Exception {
            Map<?, ?> response = response("tools/call", Map.of("name", tool, "arguments", arguments));
            safe(JSON.writeValueAsString(response));
            if (response.get("error") instanceof Map<?, ?> error) {
                assertThat(((Number) error.get("code")).intValue()).isEqualTo(-32602);
                assertThat(error.get("message")).isEqualTo("Unknown tool: invalid_tool_name");
            } else {
                Map<?, ?> result = (Map<?, ?>) response.get("result");
                assertThat(result.get("isError")).isEqualTo(true);
                assertThat(JSON.writeValueAsString(result)).contains(tool).containsIgnoringCase("not found");
            }
        }

        Map<?, ?> call(String tool, Map<String, Object> arguments) throws Exception {
            Map<?, ?> response = request("tools/call", Map.of("name", tool, "arguments", arguments));
            safe(JSON.writeValueAsString(response));
            assertThat(JSON.writeValueAsString(response)).doesNotContain(token);
            return response;
        }

        Map<?, ?> success(String tool, Map<String, Object> arguments) throws Exception {
            Map<?, ?> response = call(tool, arguments);
            assertThat(response.get("isError")).as("MCP tool %s: %s", tool, response).isEqualTo(false);
            return (Map<?, ?>) response.get("structuredContent");
        }

        void refused(String tool, Map<String, Object> arguments, String code) throws Exception {
            Map<?, ?> response = call(tool, arguments);
            assertThat(response.get("isError")).isEqualTo(true);
            assertThat(((Map<?, ?>) response.get("structuredContent")).get("code")).isEqualTo(code);
        }

        @Override public void close() throws Exception {
            try {
                input.close();
                if (!process.waitFor(5, TimeUnit.SECONDS)) stop(process);
            } finally {
                stop(process);
                reads.shutdownNow();
                assertThat(reads.awaitTermination(5, TimeUnit.SECONDS)).as("owned MCP stdout reader stopped").isTrue();
                output.close();
            }
            assertThat(process.exitValue()).isZero();
            String logged = Files.readString(stderr);
            safe(logged);
            assertThat(logged).doesNotContain(token);
        }
    }

    private static final class Logs implements AutoCloseable {
        final Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        final ListAppender<ILoggingEvent> appender = new ListAppender<>();

        Logs() {
            appender.setContext(root.getLoggerContext());
            appender.start();
            root.addAppender(appender);
        }

        String text() {
            synchronized (appender) {
                return String.join("\n", appender.list.stream().map(event -> event.getFormattedMessage()
                        + (event.getThrowableProxy() == null ? "" : ThrowableProxyUtil.asString(event.getThrowableProxy())))
                        .toList());
            }
        }

        @Override public void close() {
            root.detachAppender(appender);
            appender.stop();
        }
    }
}
