package io.tapstate.e2e;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;

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

    /** Where the image looks for connector artifacts to seed on the way up. */
    private static final String SEED_DIRECTORY = "/var/lib/tapstate/connectors";

    private final GenericContainer<?> container;

    private PublishedImageServer(GenericContainer<?> container) {
        this.container = container;
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
    static PublishedImageServer start(String imageRef, Network network, String storeUri, Path connectorJar) {
        GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(imageRef))
                .withNetwork(network)
                .withExposedPorts(SERVER_PORT)
                .withEnv("TAPSTATE_STORE_MONGO_URI", storeUri)
                .withEnv("TAPSTATE_CONNECTORS_SEED_DIR", SEED_DIRECTORY)
                .withEnv("TAPSTATE_CONNECTORS_ALSO_ACCEPT_IDS", E2eConnectorJar.CONNECTOR_ID)
                .withCopyFileToContainer(
                        MountableFile.forHostPath(connectorJar),
                        SEED_DIRECTORY + "/" + connectorJar.getFileName())
                // The product's own readiness signal, for the same reason the other tiers poll it: a
                // fixed wait is either short on a loaded machine or wasted on every green run.
                .waitingFor(Wait.forHttp("/healthz").forPort(SERVER_PORT).forStatusCode(200)
                        .withStartupTimeout(STARTUP_BUDGET));
        container.start();
        return new PublishedImageServer(container);
    }

    @Override
    public URI baseUrl() {
        return URI.create("http://" + container.getHost() + ":" + container.getMappedPort(SERVER_PORT));
    }

    /** What it said, so a failure here carries the older build's own words rather than only a status. */
    String logs() {
        return container.getLogs();
    }

    @Override
    public void close() {
        container.stop();
    }
}
