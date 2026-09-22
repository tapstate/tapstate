package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * One store that a container and this host can both reach, for the cases that run two builds of the
 * product against the same data.
 *
 * <p>{@link SharedMongo} cannot serve those: it is reached only by a mapped port on the host, and a
 * server running in a container of its own has no route to that. This one sits on a network of its
 * own and is addressed two ways - by network name from inside, by mapped port from outside.
 *
 * <p>Both addresses connect directly rather than by discovering the replica set. The set records one
 * member address for discovery, so whichever address is recorded is wrong for one of the two sides:
 * recorded as the container name, a client on this host is sent somewhere it cannot reach; recorded
 * as localhost, a client in a container is sent to its own loopback, where the refusal reads like a
 * firewall problem and is not one. Connecting directly asks the node in front of it rather than the
 * topology it advertises, which is the one thing true from both sides.
 *
 * <p>A replica set rather than a standalone for the same reason the shipped stack uses one: a write
 * path opens a session and commits a batch as a transaction, and transactions need a set.
 */
final class NetworkedMongo implements AutoCloseable {

    private static final DockerImageName IMAGE = DockerImageName.parse("mongo:7.0");

    /** What the set registers itself as, and what a container on this network dials. */
    private static final String ALIAS = "store";

    private static final int MONGO_PORT = 27017;

    private final Network network;
    private final GenericContainer<?> container;

    private NetworkedMongo(Network network, GenericContainer<?> container) {
        this.network = network;
        this.container = container;
    }

    /** Starts the store and returns once a primary has been elected, not merely once the port opens. */
    static NetworkedMongo start() {
        DockerGate.require();
        Network network = Network.newNetwork();
        GenericContainer<?> container = new GenericContainer<>(IMAGE)
                .withNetwork(network)
                .withNetworkAliases(ALIAS)
                .withExposedPorts(MONGO_PORT)
                .withCommand("--replSet", "rs0", "--bind_ip_all");
        container.start();
        initiate(container);
        return new NetworkedMongo(network, container);
    }

    /**
     * Initiates the set and holds until it can actually accept writes.
     *
     * <p>Two traps, both of which let a caller start against a set that is not ready. Evaluating
     * {@code rs.status().ok} passes on a degraded set, because a shell that evaluates a falsy value
     * still exits zero - the value has to be asserted. And initiating returns before the election
     * finishes, so stopping at "initiated" is stopping too early.
     */
    private static void initiate(GenericContainer<?> container) {
        String script = "try { rs.status().ok } catch (e) { rs.initiate({_id:'rs0',members:[{_id:0,host:'"
                + ALIAS + ":" + MONGO_PORT + "'}]}) }; if (!db.hello().isWritablePrimary) { quit(1) }";
        Await.until("the store to elect a primary",
                () -> execSucceeds(container, script),
                () -> "the set has not reported a writable primary yet");
    }

    private static boolean execSucceeds(GenericContainer<?> container, String script) {
        try {
            return container.execInContainer("mongosh", "--quiet", "--eval", script).getExitCode() == 0;
        } catch (IOException e) {
            throw new UncheckedIOException("could not ask the store whether it is ready", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the store", e);
        }
    }

    /** The network to put a containerised server on, so that {@link #uriForContainers} resolves for it. */
    Network network() {
        return network;
    }

    /** How a server running in a container on this network addresses the store. */
    String uriForContainers(String database) {
        return "mongodb://" + ALIAS + ":" + MONGO_PORT + "/" + database + "?directConnection=true";
    }

    /** How a process on this host - the shipped jar, or this test - addresses the same store. */
    String uriForThisHost(String database) {
        return "mongodb://127.0.0.1:" + container.getMappedPort(MONGO_PORT) + "/" + database
                + "?directConnection=true";
    }

    @Override
    public void close() {
        container.stop();
        network.close();
    }
}
