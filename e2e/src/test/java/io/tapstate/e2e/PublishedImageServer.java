package io.tapstate.e2e;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.stream.Stream;

/**
 * A build that already shipped, run as its published image.
 *
 * <p>This is the only way to run an older build of the server. Releases publish the command line as
 * a per-platform archive but the server only as an image, so a case that needs yesterday's server -
 * to write a store the way it used to, or to see what it does when handed a store from tomorrow -
 * has to run the container.
 *
 * <p>It is a {@link ServerHandle} like the other two tiers, so a case drives it with the same calls
 * and the fact that this one is a container stays here. What it cannot share is the store: a
 * container reaches the store by network name, not by a port mapped onto this host, which is what
 * {@link NetworkedMongo} exists for.
 */
final class PublishedImageServer implements ServerHandle {

    private static final int SERVER_PORT = 8080;

    /** Long enough for a first run to pull the image on a cold machine, where the pull is most of it. */
    private static final Duration STARTUP_BUDGET = Duration.ofMinutes(5);

    /** Nothing but curl, so the server image never has to carry a shell to be bootstrapped. */
    private static final DockerImageName BOOTSTRAP_IMAGE = DockerImageName.parse("curlimages/curl:8.11.1");

    /** One request against a server already answering its health probe; it does not need long. */
    private static final Duration BOOTSTRAP_BUDGET = Duration.ofMinutes(2);

    /** Where the image looks for connector artifacts to seed on the way up. */
    private static final String SEED_DIRECTORY = "/var/lib/tapstate/connectors";

    /**
     * A shell and nothing else, for the one chmod below. Deliberately not the image under test: that
     * one is whatever an older release shipped, and this must not assume anything about its insides.
     */
    private static final DockerImageName HANDOVER_IMAGE = DockerImageName.parse("busybox:1.37.0");

    private final GenericContainer<?> container;
    private final Path workspace;

    private PublishedImageServer(GenericContainer<?> container, Path workspace) {
        this.container = container;
        this.workspace = workspace;
    }

    /**
     * Runs {@code imageRef} against {@code storeUri}, with {@code connectorJar} seeded into it.
     *
     * <p>The image ships no connectors - the released stack fetches them separately - so a case that
     * applies anything real has to put one there. The harness's own connector is by construction not
     * one any release knows about, so the server is also told to accept its id; that setting is read
     * by every build this is used against, and a build that did not read it would refuse the seed
     * rather than quietly ignore it.
     */
    static PublishedImageServer start(
            String imageRef, Network network, String storeUri, Path connectorJar, Path workspace) {
        GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(imageRef))
                .withNetwork(network)
                .withExposedPorts(SERVER_PORT)
                .withEnv("TAPSTATE_STORE_MONGO_URI", storeUri)
                .withEnv("TAPSTATE_CONNECTORS_SEED_DIR", SEED_DIRECTORY)
                .withEnv("TAPSTATE_CONNECTORS_ALSO_ACCEPT_IDS", E2eConnectorJar.CONNECTOR_ID)
                .withCopyFileToContainer(
                        MountableFile.forHostPath(connectorJar),
                        SEED_DIRECTORY + "/" + connectorJar.getFileName())
                // The workspace at the same absolute path it has out here, which is what lets a
                // resource this server stores be read by a server that is not in a container. A file
                // endpoint's address is a path, and it is written into the stored resource: mounted
                // anywhere else, the resource the older build wrote would name a path the newer one
                // cannot open, and the upgrade would look like it lost the pipeline.
                .withFileSystemBind(workspace.toString(), workspace.toString(), BindMode.READ_WRITE)
                // The product's own readiness signal, for the same reason the other tiers poll it: a
                // fixed wait is either short on a loaded machine or wasted on every green run.
                .waitingFor(Wait.forHttp("/healthz").forPort(SERVER_PORT).forStatusCode(200)
                        .withStartupTimeout(STARTUP_BUDGET));
        openToTheImagesUser(workspace);
        container.start();
        return new PublishedImageServer(container, workspace);
    }

    /**
     * Widens the workspace so the image's own user can use it, before it is mounted.
     *
     * <p>A bind mount on Linux carries the host's ownership and mode straight through, and this image
     * runs as an unprivileged user of its own rather than as whoever started the test. A JUnit
     * temporary directory arrives rwx------, so that user cannot even traverse into it, and the
     * directory the server has to write its output into belongs to the host user. Neither shows up on
     * a Mac, where the file-sharing layer presents every mounted path as owned by the container's
     * user: a case that passes on a developer machine then fails on a Linux runner with nothing to
     * show for it but a source that appears to hold no tables.
     *
     * <p>Widened rather than given away, because a test process is not root and the user id the image
     * runs as does not exist on the host to chown to.
     */
    private static void openToTheImagesUser(Path workspace) {
        try (Stream<Path> tree = Files.walk(workspace)) {
            for (Path path : tree.toList()) {
                Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(
                        Files.isDirectory(path) ? "rwxrwxrwx" : "rw-rw-rw-"));
            }
        } catch (UnsupportedOperationException e) {
            // A filesystem that carries no POSIX permissions has nothing to widen, and the mount will
            // present whatever it presents. Nothing to do here rather than a failure.
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "could not open " + workspace + " to the user the image runs as", e);
        }
    }

    @Override
    public URI baseUrl() {
        return URI.create("http://" + container.getHost() + ":" + container.getMappedPort(SERVER_PORT));
    }

    /**
     * Creates the first administrator, from inside this server's own network namespace.
     *
     * <p>The endpoint that creates it answers loopback callers only, which is a safeguard against an
     * unauthenticated first run being seized over the network. A test on this host is not a loopback
     * caller as far as a container is concerned, so it cannot do this itself at all - it draws the
     * same refusal any other outside caller would, which is the safeguard working.
     *
     * <p>So it is done the way the shipped stack does it: a throwaway image with nothing but curl,
     * sharing this container's network namespace, so that its loopback is this server's. Sharing the
     * namespace rather than adding a shell to the server image is the shipped arrangement's point,
     * and copying it keeps this from testing something no deployment does.
     */
    void bootstrapFirstAdmin(String username, String password) {
        String body = "{\"username\":\"" + username + "\",\"password\":\"" + password + "\"}";
        // A fresh store answers 204 and one already bootstrapped refuses as closed; both mean an admin
        // exists by the time this returns. Anything else - including the 403 an outside caller draws -
        // is a real failure and exits non-zero with the status named.
        String script = "code=$(curl -s -o /dev/null -w '%{http_code}' -X POST"
                + " http://127.0.0.1:" + SERVER_PORT + "/auth/bootstrap"
                + " -H 'Content-Type: application/json' -d '" + body + "');"
                + " case \"$code\" in 204|409) echo \"bootstrap: $code\" ;;"
                + " *) echo \"bootstrap failed with HTTP $code\" >&2; exit 1 ;; esac";
        try (GenericContainer<?> bootstrap = new GenericContainer<>(BOOTSTRAP_IMAGE)
                .withNetworkMode("container:" + container.getContainerId())
                // Entrypoint and command are both set here rather than through withCommand, which
                // splits a single string on spaces: the script would arrive as fifty arguments and the
                // shell would run the first word of it. That failure reports only that the container
                // did not start, which says nothing about a script never having been run.
                .withCreateContainerCmdModifier(
                        command -> command.withEntrypoint("/bin/sh", "-c").withCmd(script))
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy()
                        .withTimeout(BOOTSTRAP_BUDGET))) {
            bootstrap.start();
        }
    }

    /** What it said, so a failure here carries the older build's own words rather than only a status. */
    String logs() {
        return container.getLogs();
    }

    @Override
    public void close() {
        container.stop();
        handBackTheWorkspace();
    }

    /**
     * Gives the mounted workspace back to the user running this test.
     *
     * <p>Whatever this server wrote belongs to the user the image runs as, and on Linux the host sees
     * that ownership unchanged. A case carries on afterwards against a server running as an ordinary
     * process out here, and the file endpoint rewrites a table in place rather than creating a new
     * one -- so a target this container wrote is a file that next server cannot open for writing, and
     * the case fails after the upgrade rather than during it.
     *
     * <p>Only a file's owner or root can widen it, and the test process is neither. So a throwaway
     * container does it, which is the same shape - and the same reason - as the bootstrap above:
     * something has to be done from a place this process cannot stand in.
     */
    private void handBackTheWorkspace() {
        try (GenericContainer<?> handover = new GenericContainer<>(HANDOVER_IMAGE)
                .withFileSystemBind(workspace.toString(), workspace.toString(), BindMode.READ_WRITE)
                // Entrypoint and command set here rather than through withCommand, for the reason
                // given above: that one splits a single string on spaces.
                .withCreateContainerCmdModifier(command -> command
                        .withEntrypoint("/bin/sh", "-c")
                        .withCmd("chmod -R a+rwX '" + workspace + "'"))
                // A non-zero chmod fails the start, so a workspace that could not be handed back says
                // so here rather than as an unreadable failure two phases later.
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy()
                        .withTimeout(BOOTSTRAP_BUDGET))) {
            handover.start();
        }
    }
}
