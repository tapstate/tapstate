package io.tapstate.cli;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.JsonReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Process-boundary coverage for launch behavior that must happen before a REPL and its transport exist.
 */
class CliMainFreshProcessTest {

    private static final Duration PROCESS_TIMEOUT = Duration.ofSeconds(20);
    private static final Duration TERMINATION_TIMEOUT = Duration.ofSeconds(2);
    private static final UUID AUTH_REF = UUID.fromString("5c199643-04da-4f72-9831-3a77e3590eed");
    private static final UUID CONTEXT_ID = UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2");
    private static final String HUMAN_SESSION = "tss_s01.human-session-secret";
    private static final String MACHINE_TOKEN = "machine-token-secret";
    private static final String ISSUER = "urn:tapstate:cluster:TEST";

    @Test
    void offlineMainBypassesConfiguredTransportAndAuth(@TempDir Path home) throws Exception {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        Path sourceDir = Files.createDirectory(workspace.resolve("source"));
        Files.writeString(sourceDir.resolve("existing.tap.yml"), """
                version: tapstate/v1
                kind: source
                id: existing
                connector: mongodb
                config: { uri: "${MONGO_URI}" }
                """);
        try (ServerSocket recorder = new ServerSocket(0)) {
            URI seed = URI.create("http://127.0.0.1:" + recorder.getLocalPort());
            persistContextAndHumanSession(home, workspace, seed);

            ProcessResult validate = runCli(home, workspace, Map.of(), "validate", "-w", workspace.toString());
            ProcessResult scaffold = runCli(home, workspace, Map.of(), "new", "--non-interactive",
                    "--kind", "source", "--id", "src", "--connector", "mysql",
                    "--dry-run", "-w", workspace.toString());
            ProcessResult explain = runCli(home, workspace, Map.of(), "explain", "source.id");

            assertThat(validate.exitCode()).isZero();
            assertThat(scaffold.exitCode()).isZero();
            assertThat(explain.exitCode()).isZero();
            assertNoSensitiveOutput(validate);
            assertNoSensitiveOutput(scaffold);
            assertNoSensitiveOutput(explain);
            recorder.setSoTimeout(250);
            assertThatThrownByTimeout(recorder);
        }
    }

    @Test
    void environmentMachineTokenWinsWithoutConsultingHumanCache(@TempDir Path home) throws Exception {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        List<Request> requests = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> respondToMachineTokenScenario(exchange, requests));
        server.start();
        try {
            URI seed = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            awaitServerReady(seed);
            requests.clear();
            persistContextAndHumanSession(home, workspace, seed);
            Path authDirectory = home.resolve(".tapstate/auth");
            Path authFile = authDirectory.resolve(AUTH_REF + ".json");
            AuthCacheSnapshot before = snapshot(authFile, authDirectory);

            ProcessResult result = runCli(home, workspace, Map.of("TAPSTATE_TOKEN", MACHINE_TOKEN),
                    "--context", "dev", "connectors", "-o", "json");

            assertThat(result.exitCode())
                    .withFailMessage("machine-token process failed: stdout=%s stderr=%s requests=%s",
                            redacted(result.stdout()), redacted(result.stderr()), requestSummary(requests))
                    .isZero();
            assertNoSensitiveOutput(result);
            assertThat(result.stderr().isEmpty()).as("machine-token stderr is empty").isTrue();
            assertThat(JsonReader.parse(redacted(result.stdout()))).isEqualTo(Map.of("connectors", List.of()));
            assertThat(requests).containsExactly(
                    new Request("/healthz", AuthorizationKind.ABSENT),
                    new Request("/.well-known/tapstate", AuthorizationKind.ABSENT),
                    new Request("/api/connectors", AuthorizationKind.EXPECTED_MACHINE));
            assertCacheUnchanged(before, authFile, authDirectory);

            requests.clear();
            poisonHumanCache(authFile);
            AuthCacheSnapshot poisoned = snapshot(authFile, authDirectory);
            ProcessResult poisonedCacheResult = runCli(home, workspace, Map.of("TAPSTATE_TOKEN", MACHINE_TOKEN),
                    "--context", "dev", "connectors", "-o", "json");

            assertThat(poisonedCacheResult.exitCode())
                    .withFailMessage("machine-token process with poisoned cache failed: stdout=%s stderr=%s requests=%s",
                            redacted(poisonedCacheResult.stdout()), redacted(poisonedCacheResult.stderr()),
                            requestSummary(requests))
                    .isZero();
            assertNoSensitiveOutput(poisonedCacheResult);
            assertThat(poisonedCacheResult.stderr().isEmpty()).as("poisoned-cache stderr is empty").isTrue();
            assertThat(JsonReader.parse(redacted(poisonedCacheResult.stdout())))
                    .isEqualTo(Map.of("connectors", List.of()));
            assertThat(requests).containsExactly(
                    new Request("/healthz", AuthorizationKind.ABSENT),
                    new Request("/.well-known/tapstate", AuthorizationKind.ABSENT),
                    new Request("/api/connectors", AuthorizationKind.EXPECTED_MACHINE));
            assertCacheUnchanged(poisoned, authFile, authDirectory);
            assertNoAuthTransientArtifacts(authDirectory);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void environmentMachineTokenDoesNotOpenUnreadableHumanCacheOnPosix(@TempDir Path home) throws Exception {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        List<Request> requests = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> respondToMachineTokenScenario(exchange, requests));
        server.start();
        try {
            URI seed = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            awaitServerReady(seed);
            requests.clear();
            persistContextAndHumanSession(home, workspace, seed);
            Path authFile = home.resolve(".tapstate/auth").resolve(AUTH_REF + ".json");
            PosixFileAttributeView view = Files.getFileAttributeView(authFile, PosixFileAttributeView.class);
            Assumptions.assumeTrue(view != null, "requires a POSIX file system");
            Files.setPosixFilePermissions(authFile, Set.of());
            try {
                ProcessResult result = runCli(home, workspace, Map.of("TAPSTATE_TOKEN", MACHINE_TOKEN),
                        "--context", "dev", "connectors", "-o", "json");

                assertMachineTokenResult(result, requests, "unreadable human cache");
            } finally {
                Files.setPosixFilePermissions(authFile, Set.of(
                        PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
            }
        } finally {
            server.stop(0);
        }
    }

    private static void persistContextAndHumanSession(Path home, Path workspace, URI seed) throws IOException {
        ContextDefinition definition = new ContextDefinition(CONTEXT_ID, List.of(seed), new ContextTls(true), AUTH_REF);
        ContextConfig config = new ContextConfig(ContextConfig.CURRENT_VERSION, "dev", Map.of("dev", definition),
                Map.of(workspace.toRealPath().toString(), "dev"));
        ContextConfigStore.underHome(home).save(config);
        Instant createdAt = Instant.now();
        AuthSessionRecord cached = new AuthSessionRecord(AuthSessionRecord.CURRENT_VERSION, AUTH_REF, CONTEXT_ID,
                ISSUER, "admin", List.of("read"), HUMAN_SESSION, createdAt,
                createdAt.plus(Duration.ofDays(30)), createdAt.plus(Duration.ofDays(90)));
        assertThat(AuthFileStore.underHome(home).save(cached, false)).isEqualTo(AuthFileStore.SaveResult.PERSISTED);
    }

    /** Waits for the server's asynchronous accept loop before a child JVM is allowed to probe it. */
    private static void awaitServerReady(URI seed) throws IOException, InterruptedException {
        HttpResponse<Void> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(seed.resolve("/healthz")).timeout(PROCESS_TIMEOUT).GET().build(),
                HttpResponse.BodyHandlers.discarding());
        assertThat(response.statusCode()).as("fresh-process test server readiness").isEqualTo(200);
    }

    private static void respondToMachineTokenScenario(HttpExchange exchange, List<Request> requests) throws IOException {
        String authorization = exchange.getRequestHeaders().getFirst("Authorization");
        String path = exchange.getRequestURI().getPath();
        requests.add(new Request(path, AuthorizationKind.of(authorization)));
        String body = switch (path) {
            case "/healthz" -> "ok";
            case "/.well-known/tapstate" -> """
                    {"issuer":"urn:tapstate:cluster:TEST","clusterId":"TEST",
                    "apiVersion":"tapstate/v1","authModes":["password","machine_token"]}
                    """;
            case "/api/connectors" -> "{\"connectors\":[]}";
            default -> "not found";
        };
        int status = path.equals("/healthz") || path.equals("/.well-known/tapstate")
                || path.equals("/api/connectors") ? 200 : 404;
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    private static ProcessResult runCli(Path home, Path workspace, Map<String, String> environment, String... arguments)
            throws Exception {
        Path stdout = Files.createTempFile(home, "tapstate-stdout-", ".log");
        Path stderr = Files.createTempFile(home, "tapstate-stderr-", ".log");
        Process process = null;
        try {
            ProcessBuilder builder = new ProcessBuilder(cliCommand(home, arguments));
            builder.directory(workspace.toFile());
            builder.environment().remove("TAPSTATE_PASSWORD");
            builder.environment().remove("TAPSTATE_TOKEN");
            builder.environment().remove("TAPSTATE_CONTEXT");
            builder.environment().put("TAPSTATE_WORKDIR", workspace.toString());
            builder.environment().putAll(environment);
            builder.redirectOutput(stdout.toFile());
            builder.redirectError(stderr.toFile());
            process = builder.start();
            process.getOutputStream().close();
            int exitCode = awaitExit(process);
            return new ProcessResult(exitCode, Files.readString(stdout), Files.readString(stderr));
        } finally {
            if (process != null && process.isAlive()) {
                stopProcessTree(process);
            }
            Files.deleteIfExists(stdout);
            Files.deleteIfExists(stderr);
        }
    }

    private static List<String> cliCommand(Path home, String... arguments) {
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Duser.home=" + home,
                "-cp", classpath,
                Cli.class.getName()));
        command.addAll(List.of(arguments));
        return command;
    }

    private static int awaitExit(Process process) throws InterruptedException {
        try {
            if (!process.waitFor(PROCESS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                stopProcessTree(process);
                throw new AssertionError("CLI process did not exit within " + PROCESS_TIMEOUT);
            }
            return process.exitValue();
        } catch (InterruptedException interrupted) {
            stopProcessTree(process);
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }

    private static void stopProcessTree(Process process) {
        List<ProcessHandle> tree = new ArrayList<>(process.descendants().toList());
        tree.add(process.toHandle());
        tree.reversed().forEach(ProcessHandle::destroy);
        boolean interrupted = awaitTermination(tree);
        tree.reversed().stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly);
        interrupted |= awaitTermination(tree);
        while (process.isAlive()) {
            try {
                if (process.waitFor(TERMINATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    break;
                }
                process.destroyForcibly();
            } catch (InterruptedException ignored) {
                interrupted = true;
                process.destroyForcibly();
            }
        }
        List<Long> survivors = tree.stream()
                .filter(ProcessHandle::isAlive)
                .map(ProcessHandle::pid)
                .toList();
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        assertThat(survivors).as("the CLI process tree is fully terminated").isEmpty();
    }

    private static boolean awaitTermination(List<ProcessHandle> processes) {
        long deadline = System.nanoTime() + TERMINATION_TIMEOUT.toNanos();
        boolean interrupted = false;
        while (processes.stream().anyMatch(ProcessHandle::isAlive) && System.nanoTime() < deadline) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        return interrupted;
    }

    private static Set<String> directoryEntries(Path directory) throws IOException {
        try (var files = Files.list(directory)) {
            return files.map(path -> path.getFileName().toString()).collect(java.util.stream.Collectors.toSet());
        }
    }

    private static AuthCacheSnapshot snapshot(Path authFile, Path authDirectory) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(authFile, BasicFileAttributes.class);
        return new AuthCacheSnapshot(Files.readAllBytes(authFile), directoryEntries(authDirectory),
                attributes.fileKey(), attributes.lastModifiedTime());
    }

    private static void assertCacheUnchanged(AuthCacheSnapshot expected, Path authFile, Path authDirectory)
            throws IOException {
        AuthCacheSnapshot actual = snapshot(authFile, authDirectory);
        assertThat(actual.bytes()).isEqualTo(expected.bytes());
        assertThat(actual.entries()).isEqualTo(expected.entries());
        assertThat(actual.fileKey()).isEqualTo(expected.fileKey());
        assertThat(actual.lastModifiedTime()).isEqualTo(expected.lastModifiedTime());
    }

    private static void assertNoAuthTransientArtifacts(Path authDirectory) throws IOException {
        try (var files = Files.list(authDirectory)) {
            assertThat(files.map(path -> path.getFileName().toString()).filter(name ->
                    name.startsWith(".auth.tmp-") || name.startsWith(".auth.rollback-")).toList()).isEmpty();
        }
    }

    private static void assertMachineTokenResult(ProcessResult result, List<Request> requests, String scenario) {
        assertThat(result.exitCode())
                .withFailMessage("machine-token process with %s failed: stdout=%s stderr=%s requests=%s",
                        scenario, redacted(result.stdout()), redacted(result.stderr()), requestSummary(requests))
                .isZero();
        assertNoSensitiveOutput(result);
        assertThat(result.stderr().isEmpty()).as("machine-token stderr is empty").isTrue();
        assertThat(JsonReader.parse(redacted(result.stdout()))).isEqualTo(Map.of("connectors", List.of()));
        assertThat(requests).containsExactly(
                new Request("/healthz", AuthorizationKind.ABSENT),
                new Request("/.well-known/tapstate", AuthorizationKind.ABSENT),
                new Request("/api/connectors", AuthorizationKind.EXPECTED_MACHINE));
    }

    private static void assertNoSensitiveOutput(ProcessResult result) {
        String output = result.stdout() + result.stderr();
        String normalized = output.toLowerCase(Locale.ROOT);
        assertThat(output.contains(HUMAN_SESSION)).as("output contains the human session").isFalse();
        assertThat(output.contains(MACHINE_TOKEN)).as("output contains the machine token").isFalse();
        assertThat(normalized.contains("password")).as("output contains a password prompt").isFalse();
        assertThat(normalized.contains("sign in")).as("output contains an interactive sign-in prompt").isFalse();
        assertThat(normalized.contains("login")).as("output contains an interactive login prompt").isFalse();
    }

    private static void poisonHumanCache(Path authFile) throws IOException {
        Files.writeString(authFile, "not-json", StandardCharsets.UTF_8);
    }

    private static String requestSummary(List<Request> requests) {
        return requests.stream()
                .map(request -> request.path() + " authorization=" + request.authorization())
                .toList()
                .toString();
    }

    private static String redacted(String value) {
        return value.replace(HUMAN_SESSION, "[redacted-human-session]")
                .replace(MACHINE_TOKEN, "[redacted-machine-token]");
    }

    private static void assertThatThrownByTimeout(ServerSocket recorder) throws IOException {
        try {
            recorder.accept();
            throw new AssertionError("offline command opened a network connection");
        } catch (java.net.SocketTimeoutException expected) {
            // No connection reached the recorder.
        }
    }

    private enum AuthorizationKind {
        ABSENT,
        EXPECTED_MACHINE,
        OTHER_PRESENT;

        private static AuthorizationKind of(String authorization) {
            if (authorization == null) {
                return ABSENT;
            }
            return authorization.equals("Bearer " + MACHINE_TOKEN) ? EXPECTED_MACHINE : OTHER_PRESENT;
        }
    }

    private record Request(String path, AuthorizationKind authorization) { }

    private record AuthCacheSnapshot(byte[] bytes, Set<String> entries, Object fileKey,
                                     java.nio.file.attribute.FileTime lastModifiedTime) { }

    private record ProcessResult(int exitCode, String stdout, String stderr) { }
}
