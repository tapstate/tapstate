package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two connectors over one jar, one closed while the other is part way through reading a resource of that
 * jar: the other's read completes.
 *
 * <p>A pipeline's two sources are two such connectors, and the one that finishes its snapshot first closes
 * while the other may still be starting - which is when a driver reads its messages, from a static
 * initializer. While the two read through one shared jar file, the close ended the other's read; the
 * driver's class then stayed unusable in its loader, and the pipeline failed with a capture failure that
 * named nothing but the initializer.
 *
 * <p>The harness connector holds its read open between two signal files, so the connectors meet in that
 * state every run rather than when a race happens to line them up. The specification vocabulary has no word
 * for holding a connector part way through a read, which is why this is Java. When it was written, the held
 * discovery failed with "Stream closed" without the fix. A running server now opens every connector over one
 * artifact through one class loader that a close leaves open, so this holds without that fix as well; it
 * stays as the guard on what a pipeline sees, whichever of the two keeps it true.
 */
class ClosingAConnectorLeavesAnotherOnItsJarReadingIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aDiscoveryReadingItsJarCompletesWhileAnotherConnectorOnThatJarCloses(@TempDir Path directory)
            throws Exception {
        Path jar = E2eConnectorJar.buildInto(directory);
        Path heldData = Files.createDirectory(directory.resolve("held"));
        Path otherData = Files.createDirectory(directory.resolve("other"));
        Files.writeString(heldData.resolve("orders.csv"), "id,amount\n7,70.00\n");
        Files.writeString(otherData.resolve("customers.csv"), "id,name\n8,ada\n");
        Path heldSignals = Files.createDirectory(directory.resolve("held-signals"));
        Path otherSignals = Files.createDirectory(directory.resolve("other-signals"));
        // The other connector reads the same resource straight through: it is told to go on before it asks.
        Files.writeString(otherSignals.resolve("resume"), "");
        String database = "e2e_shared_jar_" + UUID.randomUUID().toString().replace("-", "");

        ExecutorService background = Executors.newSingleThreadExecutor();
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl(database))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(jar));

            Future<?> held = background.submit(() -> control.discoverSchema("src_held",
                    E2eConnectorJar.CONNECTOR_ID,
                    Map.of("uri", heldData.toString(), "pause_resource_read", heldSignals.toString())));
            Await.until("the held discovery to be part way through its connector's resource",
                    () -> Files.exists(heldSignals.resolve("paused")) || held.isDone(),
                    () -> "the held discovery is " + (held.isDone() ? "already over" : "still starting"));
            assertThat(held.isDone())
                    .as("the held discovery ended before it was held; nothing below would test anything")
                    .isFalse();

            // A whole discovery over the same jar: its connector opens, reads that resource, and is closed
            // before the call returns.
            control.discoverSchema("src_other", E2eConnectorJar.CONNECTOR_ID,
                    Map.of("uri", otherData.toString(), "pause_resource_read", otherSignals.toString()));

            Files.writeString(heldSignals.resolve("resume"), "");
            try {
                held.get(60, TimeUnit.SECONDS);
            } catch (ExecutionException failed) {
                throw new AssertionError("the held discovery failed once the other connector on its jar "
                        + "closed: " + failed.getCause().getMessage(), failed.getCause());
            }
            assertThat(control.connectionSchemaTables("src_held"))
                    .as("what the held discovery found, once it could read its jar to the end")
                    .containsExactly("orders");
        } finally {
            background.shutdownNow();
        }
    }
}
