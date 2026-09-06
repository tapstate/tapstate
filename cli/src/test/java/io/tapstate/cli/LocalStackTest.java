package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The local development stack the guided first run starts when nothing listens on the default server
 * ({@code docs/first-run/README.md}, "Step 1 - which server"): what it writes under the home directory,
 * what it runs, what it fetches, how long it waits, and what it refuses. Nothing here touches Docker or
 * the network: the process runner, the downloader, the PATH lookup and the health probe are all fakes,
 * so the assertions are on the commands and the files.
 */
class LocalStackTest {

    private static final URI DEFAULT_SERVER = URI.create("http://127.0.0.1:8080");
    private static final String VERSION = Cli.VERSION_NUMBER;

    /** The compose file as written, byte for byte: three services, the CLI's own version, no secret. */
    static final String COMPOSE_GOLDEN = """
            # The local development stack the guided first run starts: the server and its managed store, and
            # nothing else. The CLI writes this file and rewrites it on every start, so edits do not survive;
            # the .env beside it is written once and kept. No sample data and no sample databases live here -
            # that is what the demo is for.
            #
            # Single-node, for development: the server keeps pipeline position in memory, so a restart
            # replays rather than resumes.
            services:
              mongo:
                image: mongo:7.0
                # A single-member replica set rather than a standalone: the store commits a batch as one
                # transaction, and MongoDB offers transactions only on a replica set.
                command: ["--replSet", "rs0", "--bind_ip_all"]
                healthcheck:
                  # Initiate the set on first boot, then hold the check red until a primary has actually been
                  # elected: rs.initiate() returns before the election completes. The member is registered
                  # under the service name, the address every client inside this network dials it by.
                  test: >
                    mongosh --quiet --eval
                    "try { rs.status().ok } catch (e) { rs.initiate({ _id: 'rs0', members: [{ _id: 0, host: 'mongo:27017' }] }) };
                     if (!db.hello().isWritablePrimary) { quit(1) }"
                  interval: 3s
                  timeout: 10s
                  retries: 40
                  start_period: 20s
                volumes:
                  # A named volume, so registered connectors and applied artifacts outlive `down`. Only
                  # `down -v` discards them.
                  - tapstate-local-mongo:/data/db

              server:
                # The same version as the CLI that wrote this file: the two are installed by different paths
                # and would drift silently otherwise.
                image: ghcr.io/tapstate/tapstate:%s
                depends_on:
                  mongo:
                    condition: service_healthy
                environment:
                  # Addressed directly rather than through topology discovery: the set has exactly one
                  # member, and its advertised address is not resolvable from outside the set's own network.
                  TAPSTATE_STORE_MONGO_URI: mongodb://mongo:27017/tapstate?directConnection=true
                  # The connector jars staged in ./connectors are registered once at boot, through the same
                  # register-if-absent path `tapstate register` uploads to. An absolute path, so it stays
                  # fixed to the mount below whatever the image's working directory is.
                  TAPSTATE_CONNECTORS_SEED_DIR: /var/lib/tapstate/connectors
                volumes:
                  - ./connectors:/var/lib/tapstate/connectors:ro
                ports:
                  # Loopback only: an unauthenticated first run must not be reachable from other machines.
                  - "127.0.0.1:8080:8080"
                # The orderly stop closes the cluster member and the store client before halting, which
                # Docker's default 10s grace can cut short.
                stop_grace_period: 30s

              # Creates the first administrator, then exits. The endpoint accepts loopback callers only, so
              # this container shares the server's network namespace - its loopback is the server's. The
              # credentials come from the .env beside this file, and there is no default: a stack cannot
              # come up with a password nobody chose.
              bootstrap:
                image: curlimages/curl:8.11.1
                depends_on:
                  server:
                    condition: service_healthy
                network_mode: "service:server"
                restart: "no"
                environment:
                  TAPSTATE_ADMIN_USER: ${TAPSTATE_ADMIN_USER:?set in .env}
                  TAPSTATE_ADMIN_PASSWORD: ${TAPSTATE_ADMIN_PASSWORD:?set in .env}
                entrypoint: ["/bin/sh", "-c"]
                # The doubled $$ keeps the values out of Compose interpolation so the container's own shell
                # reads them. A fresh store answers 204 and a bootstrapped one 409; both mean an admin now
                # exists, so a re-run never fails here.
                command:
                  - |
                    set -eu
                    code=$$(curl -s -o /dev/null -w '%%{http_code}' \\
                      -X POST http://127.0.0.1:8080/auth/bootstrap \\
                      -H 'Content-Type: application/json' \\
                      -d "{\\"username\\":\\"$${TAPSTATE_ADMIN_USER}\\",\\"password\\":\\"$${TAPSTATE_ADMIN_PASSWORD}\\"}")
                    case "$$code" in
                      204) echo "bootstrap: first admin created" ;;
                      409) echo "bootstrap: an admin already exists, nothing to do" ;;
                      *)   echo "bootstrap: failed with HTTP $$code" >&2; exit 1 ;;
                    esac

            volumes:
              tapstate-local-mongo:
            """.formatted(VERSION);

    /** A downloader that fetches nothing: it writes down the URL and leaves a marker file where the jar goes. */
    static final class FakeDownloader implements LocalStack.Downloader {
        final List<String> fetched = new ArrayList<>();

        @Override
        public void download(URI from, Path to) throws IOException {
            fetched.add(from.toString());
            Files.createDirectories(to.getParent());
            Files.writeString(to, "jar bytes from " + from);
        }
    }

    /** A stack over fakes: Docker on the PATH, every command exit 0, the probe healthy once {@code up -d} ran. */
    private static LocalStack stack(Path home, ScriptedProcessRunner runner, FakeDownloader downloads,
                                    FakeHealthProbe probe, UnaryOperator<String> env, int polls) {
        runner.when("docker compose up -d", () -> probe.healthy = true);
        return new LocalStack(home, runner, downloads, () -> true, probe, env, millis -> { }, polls);
    }

    private static LocalStack stack(Path home, ScriptedProcessRunner runner, FakeDownloader downloads,
                                    FakeHealthProbe probe) {
        return stack(home, runner, downloads, probe, name -> null, LocalStack.HEALTH_POLLS);
    }

    private static Path dir(Path home) {
        return home.resolve(".tapstate/local-stack");
    }

    @Test
    void startWritesTheStackRunsComposeFetchesTheJarsAndWaitsForHealth(@TempDir Path home) throws IOException {
        ScriptedProcessRunner runner = new ScriptedProcessRunner();
        FakeDownloader downloads = new FakeDownloader();
        FakeHealthProbe probe = new FakeHealthProbe(false);

        LocalStack.Admin admin = stack(home, runner, downloads, probe).start(null);

        Path dir = dir(home);
        assertThat(runner.calls).containsExactly(
                dir + ": docker compose version",
                dir + ": docker compose up -d");
        assertThat(Files.readString(dir.resolve("docker-compose.yml"))).isEqualTo(COMPOSE_GOLDEN);
        // the three engines the release publishes jars for, from the release location, into the seed dir
        assertThat(downloads.fetched).containsExactly(
                "https://github.com/tapstate/tapstate/releases/download/connectors-preview/mysql-connector.jar",
                "https://github.com/tapstate/tapstate/releases/download/connectors-preview/mongodb-connector.jar",
                "https://github.com/tapstate/tapstate/releases/download/connectors-preview/postgres-connector.jar");
        assertThat(dir.resolve("connectors/mysql-connector.jar")).exists();
        assertThat(dir.resolve("connectors/postgres-connector.jar")).exists();
        // the .env carries the admin the bootstrap service creates, with a password nobody chose
        Map<String, String> env = DotEnv.read(dir.resolve(".env"));
        assertThat(env).containsEntry("TAPSTATE_ADMIN_USER", "admin");
        assertThat(env.get("TAPSTATE_ADMIN_PASSWORD")).hasSizeGreaterThanOrEqualTo(24).matches("[A-Za-z0-9_-]+");
        assertThat(admin.user()).isEqualTo("admin");
        assertThat(admin.password()).isEqualTo(env.get("TAPSTATE_ADMIN_PASSWORD"));
        // probed only once compose ran, and only until it answered
        assertThat(probe.probed).containsExactly(DEFAULT_SERVER);
    }

    @Test
    void aSecondStartReusesTheEnvAndTheJarsAndStillRunsUp(@TempDir Path home) throws IOException {
        ScriptedProcessRunner runner = new ScriptedProcessRunner();
        FakeDownloader downloads = new FakeDownloader();
        FakeHealthProbe probe = new FakeHealthProbe(false);
        LocalStack first = stack(home, runner, downloads, probe);
        LocalStack.Admin admin = first.start(null);
        probe.healthy = false;

        LocalStack.Admin again = stack(home, runner, downloads, probe).start(null);

        assertThat(again.password()).as("regenerating it would lock out the admin already bootstrapped")
                .isEqualTo(admin.password());
        assertThat(runner.calls).hasSize(4).filteredOn(call -> call.endsWith("docker compose up -d")).hasSize(2);
        assertThat(downloads.fetched).as("a jar already staged is not fetched again").hasSize(3);
    }

    @Test
    void thePasswordIsReadBackFromTheEnvForAStackAlreadyRunning(@TempDir Path home) throws IOException {
        ScriptedProcessRunner runner = new ScriptedProcessRunner();
        FakeHealthProbe probe = new FakeHealthProbe(false);
        LocalStack stack = stack(home, runner, new FakeDownloader(), probe);
        assertThat(stack.savedAdmin()).isEmpty();

        LocalStack.Admin admin = stack.start(null);

        assertThat(stack.savedAdmin()).contains(admin);
    }

    @Test
    void whereItLivesAndHowToStopIt(@TempDir Path home) {
        LocalStack stack = stack(home, new ScriptedProcessRunner(), new FakeDownloader(), new FakeHealthProbe(false));

        assertThat(stack.dir()).isEqualTo(dir(home));
        assertThat(stack.stopCommand()).isEqualTo("docker compose -f " + dir(home) + "/docker-compose.yml down");
    }

    @Test
    void theConnectorLocationCanBePointedElsewhereTheWayTheQuickstartIs(@TempDir Path home) throws IOException {
        FakeDownloader downloads = new FakeDownloader();
        FakeHealthProbe probe = new FakeHealthProbe(false);
        UnaryOperator<String> env = name -> "TAPSTATE_CONNECTORS_URL".equals(name) ? "http://mirror/jars/" : null;

        stack(home, new ScriptedProcessRunner(), downloads, probe, env, LocalStack.HEALTH_POLLS).start(null);

        assertThat(downloads.fetched).containsExactly(
                "http://mirror/jars/mysql-connector.jar",
                "http://mirror/jars/mongodb-connector.jar",
                "http://mirror/jars/postgres-connector.jar");
    }

    @Test
    void withoutDockerOnThePathItRefusesBeforeWritingOrRunningAnything(@TempDir Path home) {
        ScriptedProcessRunner runner = new ScriptedProcessRunner();
        FakeDownloader downloads = new FakeDownloader();
        LocalStack stack = new LocalStack(home, runner, downloads, () -> false, new FakeHealthProbe(false),
                name -> null, millis -> { }, LocalStack.HEALTH_POLLS);

        assertThatThrownBy(() -> stack.start(null))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(CliError.DOCKER_UNAVAILABLE))
                .satisfies(e -> assertThat(((TapstateException) e).args().get("reason").toString())
                        .containsIgnoringCase("docker").containsIgnoringCase("PATH"));
        assertThat(runner.calls).isEmpty();
        assertThat(downloads.fetched).isEmpty();
        assertThat(dir(home)).doesNotExist();
    }

    @Test
    void withoutComposeItRefusesWithWhatDockerSaid(@TempDir Path home) {
        ScriptedProcessRunner runner = new ScriptedProcessRunner()
                .answer("docker compose version", new ProcessRunner.Result(1, "", "docker: 'compose' is not a docker command"));
        FakeDownloader downloads = new FakeDownloader();
        LocalStack stack = stack(home, runner, downloads, new FakeHealthProbe(false));

        assertThatThrownBy(() -> stack.start(null))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(CliError.DOCKER_UNAVAILABLE))
                .satisfies(e -> assertThat(((TapstateException) e).args().get("reason").toString())
                        .contains("docker: 'compose' is not a docker command"));
        assertThat(runner.calls).hasSize(1);
        assertThat(downloads.fetched).isEmpty();
        assertThat(dir(home)).doesNotExist();
    }

    @Test
    void aComposeThatAnswersNothingCountsAsMissing(@TempDir Path home) {
        ScriptedProcessRunner runner = new ScriptedProcessRunner()
                .answer("docker compose version", new ProcessRunner.Result(0, "", ""));
        LocalStack stack = stack(home, runner, new FakeDownloader(), new FakeHealthProbe(false));

        assertThatThrownBy(() -> stack.start(null))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(CliError.DOCKER_UNAVAILABLE));
        assertThat(runner.calls).hasSize(1);
    }

    @Test
    void aFailedUpIsRefusedWithWhatComposeSaid(@TempDir Path home) {
        ScriptedProcessRunner runner = new ScriptedProcessRunner()
                .answer("docker compose up -d", new ProcessRunner.Result(1, "", "Cannot connect to the Docker daemon"));
        FakeHealthProbe probe = new FakeHealthProbe(false);
        LocalStack stack = new LocalStack(home, runner, new FakeDownloader(), () -> true, probe,
                name -> null, millis -> { }, LocalStack.HEALTH_POLLS);

        assertThatThrownBy(() -> stack.start(null))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(CliError.DOCKER_UNAVAILABLE))
                .satisfies(e -> assertThat(((TapstateException) e).args().get("reason").toString())
                        .contains("Cannot connect to the Docker daemon"));
        assertThat(probe.probed).as("nothing to wait for").isEmpty();
    }

    @Test
    void aStackThatNeverAnswersIsRefusedNamingWhereItIsAndHowToStopIt(@TempDir Path home) {
        ScriptedProcessRunner runner = new ScriptedProcessRunner();
        FakeHealthProbe probe = new FakeHealthProbe(false);
        List<Long> slept = new ArrayList<>();
        LocalStack stack = new LocalStack(home, runner, new FakeDownloader(), () -> true, probe,
                name -> null, slept::add, 3);

        assertThatThrownBy(() -> stack.start(null))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(CliError.DOCKER_UNAVAILABLE))
                .satisfies(e -> assertThat(((TapstateException) e).args().get("reason").toString())
                        .contains(dir(home).toString())
                        .contains("docker compose -f " + dir(home) + "/docker-compose.yml down"));
        assertThat(probe.probed).hasSize(3);
        assertThat(slept).containsExactly(LocalStack.POLL_MILLIS, LocalStack.POLL_MILLIS);
    }

    @Test
    void aRunnerThatCannotStartAProcessIsRefusedTheSameWay(@TempDir Path home) {
        ScriptedProcessRunner runner = new ScriptedProcessRunner().failing(new IOException("docker: not found"));
        LocalStack stack = stack(home, runner, new FakeDownloader(), new FakeHealthProbe(false));

        assertThatThrownBy(() -> stack.start(null))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(CliError.DOCKER_UNAVAILABLE))
                .satisfies(e -> assertThat(((TapstateException) e).args().get("reason").toString())
                        .contains("docker: not found"));
    }

    @Test
    void theBundledComposeCarriesNoSecretAndExactlyOneVersionToSubstitute() {
        String bundled = LocalStack.bundledCompose();

        assertThat(bundled).contains("@TAPSTATE_VERSION@").doesNotContain(":-admin").doesNotContain("secret");
        assertThat(LocalStack.composeFile("9.9.9"))
                .doesNotContain("@TAPSTATE_VERSION@")
                .contains("image: ghcr.io/tapstate/tapstate:9.9.9");
    }

    @Test
    void retryingUntilTheBootstrapHasRunUsesTheSamePollBound() {
        List<Long> slept = new ArrayList<>();
        LocalStack stack = new LocalStack(Path.of("unused"), new ScriptedProcessRunner(), new FakeDownloader(),
                () -> true, new FakeHealthProbe(false), name -> null, slept::add, 3);
        int[] attempts = {0};

        String settled = stack.retryWhile(() -> ++attempts[0] < 3 ? "not yet" : "done", "not yet"::equals);
        String gaveUp = stack.retryWhile(() -> "not yet", "not yet"::equals);

        assertThat(settled).isEqualTo("done");
        assertThat(gaveUp).as("the last answer is handed back for the caller to report").isEqualTo("not yet");
        assertThat(slept).hasSize(2 + 2);
    }
}
