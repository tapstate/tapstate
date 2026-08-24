package io.tapstate.cli;

import io.tapstate.app.Bootstrap;
import io.tapstate.core.common.JsonReader;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A real terminal login followed by separate non-terminal CLI processes against the assembled server.
 * The server uses the production Mongo-backed session store, while every CLI process shares only the
 * disposable owner-only files under its test home.
 */
@RequiresDocker
@EnabledOnOs({OS.LINUX, OS.MAC})
class SessionResumePtyIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "pty-only-secret";
    private static final Duration PROCESS_TIMEOUT = Duration.ofSeconds(35);
    private static final Duration TERMINATION_TIMEOUT = Duration.ofSeconds(5);
    private static final String SIGNED_OUT_CONNECTORS_DIAGNOSTIC = String.join(
            System.lineSeparator(),
            "error: cli.not-authenticated",
            "  The `connectors` command needs you to sign in first.",
            "  Run `login <username>`, or start with `tapstate -c <host:port> -u <user>`.",
            "");
    private static final String PTY_DRIVER = """
            import os, pty, select, signal, sys, termios, time

            data = os.environ.pop("TAPSTATE_PTY_INPUT").encode()
            wait_no_echo = os.environ.pop("TAPSTATE_PTY_WAIT_NO_ECHO", "0") == "1"
            pid, fd = pty.fork()
            if pid == 0:
                if os.environ.get("TERM", "") in ("", "dumb"):
                    os.environ["TERM"] = "linux"
                os.execvp(sys.argv[1], sys.argv[1:])

            output = bytearray()
            sent = False
            status = None
            deadline = time.time() + 30

            def no_echo():
                try:
                    return not (termios.tcgetattr(fd)[3] & termios.ECHO)
                except (OSError, termios.error):
                    return False

            while time.time() < deadline:
                readable, _, _ = select.select([fd], [], [], 0.25)
                if readable:
                    try:
                        chunk = os.read(fd, 4096)
                    except OSError:
                        chunk = b""
                    if not chunk:
                        break
                    output.extend(chunk)
                if not sent and (no_echo() if wait_no_echo else bool(output)):
                    os.write(fd, data)
                    sent = True
                done, child_status = os.waitpid(pid, os.WNOHANG)
                if done:
                    status = child_status
                    break
            else:
                os.kill(pid, signal.SIGKILL)
                _, status = os.waitpid(pid, 0)
                reason = b" waiting for terminal no-echo mode" if wait_no_echo and not sent else b""
                output.extend(b"\\nPTY timeout" + reason + b"\\n")

            try:
                while True:
                    chunk = os.read(fd, 4096)
                    if not chunk:
                        break
                    output.extend(chunk)
            except OSError:
                pass
            os.close(fd)
            sys.stdout.buffer.write(output)
            if status is None:
                _, status = os.waitpid(pid, 0)
            if os.WIFEXITED(status):
                sys.exit(os.WEXITSTATUS(status))
            sys.exit(128 + os.WTERMSIG(status))
            """;

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(MONGO_IMAGE);

    private ConfigurableApplicationContext server;

    @BeforeAll
    static void requirePythonPtySupport() throws InterruptedException {
        Process probe = null;
        boolean supported = false;
        try {
            probe = new ProcessBuilder("python3", "-c",
                    "import pty, sys, termios; assert sys.version_info.major == 3")
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .redirectError(ProcessBuilder.Redirect.DISCARD)
                    .start();
            supported = probe.waitFor(5, TimeUnit.SECONDS) && probe.exitValue() == 0;
        } catch (IOException ignored) {
            supported = false;
        } finally {
            if (probe != null && probe.isAlive()) {
                stopProcessTree(probe);
            }
        }
        assumeTrue(supported, "SessionResumePtyIT requires Python 3 with POSIX pty and termios support");
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void terminalLoginResumesAcrossProcessesAndLogoutRevokesTheSession(@TempDir Path testRoot)
            throws Exception {
        Path home = Files.createDirectory(testRoot.resolve("home"));
        Path workspace = Files.createDirectory(testRoot.resolve("orders"));
        URI seed = startServer();
        bootstrap(seed);

        ProcessResult contextSetup = runInPty(home, workspace, false,
                "1\ndev\n" + seed + "\n\n\n", "context");

        assertThat(contextSetup.exitCode()).as("context CLI exits successfully").isZero();
        assertThat(contextSetup.stdout())
                .contains("created context dev", "bound dev to " + workspace.toRealPath());
        ContextConfig config = ContextConfigStore.underHome(home).load();
        ContextDefinition context = config.contexts().get("dev");
        assertThat(context).isNotNull();
        assertThat(config.workspaceBindings()).containsExactlyEntriesOf(
                Map.of(workspace.toRealPath().toString(), "dev"));

        ProcessResult login = runInPty(home, workspace, true, PASSWORD + "\n",
                "auth", "login", USERNAME, "--context", "dev");

        assertThat(login.exitCode()).as("masked login exits successfully").isZero();
        assertThat(login.stdout().contains("signed in as " + USERNAME))
                .as("login reports the authenticated user").isTrue();
        assertThat(login.stdout().contains("session saved"))
                .as("login reports the cached session").isTrue();
        assertThat(login.stdout().contains(PASSWORD))
                .as("the PTY transcript does not echo the password").isFalse();
        AuthSessionRecord cached = AuthFileStore.underHome(home)
                .load(context.authRef(), context.id())
                .orElseThrow();

        ProcessResult resumed = runWithoutTerminal(home, workspace,
                "--context", "dev", "connectors", "-o", "json");

        assertThat(resumed.exitCode()).as("cached-session online read exits successfully").isZero();
        assertThat(resumed.stderr().isEmpty())
                .as("successful resume has no stderr diagnostic").isTrue();
        Object response = JsonReader.parse(resumed.stdout());
        assertThat(response).isInstanceOf(Map.class);
        assertThat(((Map<?, ?>) response).get("connectors")).isInstanceOf(List.class);

        ProcessResult logout = runWithoutTerminal(home, workspace,
                "auth", "logout", "--context", "dev");

        assertThat(logout.exitCode()).isZero();
        assertThat(logout.stdout()).contains("session revoked and local cache removed");
        assertScriptOutputIsClean(logout);
        assertThat(AuthFileStore.underHome(home).load(context.authRef(), context.id())).isEmpty();
        assertThat(exchange(seed, cached.sessionToken())).isEqualTo(HttpStatus.UNAUTHORIZED);

        ProcessResult signedOut = runWithoutTerminal(home, workspace,
                "--context", "dev", "connectors", "-o", "json");

        assertThat(signedOut.exitCode()).isNotZero();
        assertThat(signedOut.stdout().isEmpty()).as("signed-out command writes no stdout").isTrue();
        assertThat(signedOut.stderr().equals(SIGNED_OUT_CONNECTORS_DIAGNOSTIC))
                .as("signed-out stderr is exactly the canonical not-authenticated diagnostic")
                .isTrue();
        assertScriptOutputIsClean(signedOut);
    }

    private URI startServer() {
        String database = "session_resume_" + Long.toUnsignedString(System.nanoTime(), 16);
        server = new SpringApplicationBuilder(Bootstrap.class)
                .properties(
                        "server.address=127.0.0.1",
                        "server.port=0",
                        "tapstate.store.mongo.enabled=true",
                        "tapstate.store.mongo.uri=" + MONGO.getReplicaSetUrl(database),
                        "tapstate.store.mongo.server-selection-timeout=5s")
                .run("--server.address=127.0.0.1", "--server.port=0");
        int port = ((WebServerApplicationContext) server).getWebServer().getPort();
        return URI.create("http://127.0.0.1:" + port);
    }

    private static void bootstrap(URI seed) {
        HttpStatusCode status = RestClient.create(seed.toString())
                .post()
                .uri("/auth/bootstrap")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", USERNAME, "password", PASSWORD))
                .exchange((request, response) -> response.getStatusCode());
        assertThat(status).isEqualTo(HttpStatus.NO_CONTENT);
    }

    private static HttpStatusCode exchange(URI seed, String sessionToken) {
        return RestClient.create(seed.toString())
                .post()
                .uri("/auth/session")
                .header(HttpHeaders.AUTHORIZATION, "TapstateSession " + sessionToken)
                .exchange((request, response) -> response.getStatusCode());
    }

    private static ProcessResult runInPty(
            Path home, Path workspace, boolean waitForNoEcho, String input, String... arguments)
            throws Exception {
        List<String> cli = cliCommand(home, arguments);
        List<String> command = new ArrayList<>(List.of("python3", "-c", PTY_DRIVER));
        command.addAll(cli);
        Path transcriptFile = Files.createTempFile(home, "tapstate-pty-", ".log");
        ProcessBuilder builder = process(command, workspace).redirectErrorStream(true);
        builder.environment().put("TAPSTATE_PTY_INPUT", input);
        builder.environment().put("TAPSTATE_PTY_WAIT_NO_ECHO", waitForNoEcho ? "1" : "0");
        builder.redirectOutput(transcriptFile.toFile());
        Process process = null;
        try {
            process = builder.start();
            process.getOutputStream().close();
            int exitCode = awaitExit(process);
            String transcript = Files.readString(transcriptFile, StandardCharsets.UTF_8);
            return new ProcessResult(exitCode, transcript, "");
        } finally {
            if (process != null && process.isAlive()) {
                stopProcessTree(process);
            }
            Files.deleteIfExists(transcriptFile);
        }
    }

    private static ProcessResult runWithoutTerminal(Path home, Path workspace, String... arguments)
            throws Exception {
        Path stdoutFile = Files.createTempFile(home, "tapstate-stdout-", ".log");
        Path stderrFile = Files.createTempFile(home, "tapstate-stderr-", ".log");
        ProcessBuilder builder = process(cliCommand(home, arguments), workspace)
                .redirectOutput(stdoutFile.toFile())
                .redirectError(stderrFile.toFile());
        Process process = null;
        try {
            process = builder.start();
            process.getOutputStream().close();
            int exitCode = awaitExit(process);
            String stdout = Files.readString(stdoutFile, StandardCharsets.UTF_8);
            String stderr = Files.readString(stderrFile, StandardCharsets.UTF_8);
            return new ProcessResult(exitCode, stdout, stderr);
        } finally {
            if (process != null && process.isAlive()) {
                stopProcessTree(process);
            }
            Files.deleteIfExists(stdoutFile);
            Files.deleteIfExists(stderrFile);
        }
    }

    private static ProcessBuilder process(List<String> command, Path workspace) {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.environment().remove("TAPSTATE_PASSWORD");
        builder.environment().put("TAPSTATE_WORKDIR", workspace.toString());
        return builder;
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
        tree.reversed().stream()
                .filter(ProcessHandle::isAlive)
                .forEach(ProcessHandle::destroyForcibly);
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

    private static void assertScriptOutputIsClean(ProcessResult result) {
        String output = result.stdout() + result.stderr();
        for (String pollution : List.of(
                "Password", "Username", "signed in", "resumed", "connected to", PASSWORD)) {
            assertThat(output.contains(pollution))
                    .as("non-terminal output excludes login pollution").isFalse();
        }
    }

    private record ProcessResult(int exitCode, String stdout, String stderr) {
    }
}
