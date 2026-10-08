package io.tapstate.e2e;

import io.tapstate.e2e.connector.CsvConnector;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A running server reaches one connector artifact through one class loader, however many times it opens
 * the connector: a connection test and two discoveries initialize the connector's class once.
 *
 * <p>A connector that binds a JNI library depends on exactly that. The JVM ties the library to the first
 * loader that loads it and refuses it to every other, so when each open got a loader of its own, the Db2
 * connector's native log reader passed a connection test and then failed the pipeline after it - and a
 * pipeline on its own, which opens the connector once to find its starting position and again to read the
 * stream. No connector in this build carries a native library, so the witness is what that failure follows
 * from: the harness connector records each initialization of its class, with the jar and the loader, and a
 * class is initialized once per loader. One open per loader would leave three lines for the staged jar.
 *
 * <p>The lines are read for the jar the server staged and nothing else. Registering the artifact also
 * probes it, from a copy of its own and through a loader of its own that it closes, which is a separate
 * matter this does not judge. The specification vocabulary has no word for a class's initializations,
 * which is why this is Java.
 */
class OneConnectorArtifactIsLoadedOnceAcrossItsOpensIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aTestAndTwoDiscoveriesInitializeTheConnectorClassOnce(@TempDir Path directory) throws Exception {
        Path jar = E2eConnectorJar.buildInto(directory);
        Path orders = Files.createDirectory(directory.resolve("orders"));
        Path customers = Files.createDirectory(directory.resolve("customers"));
        Files.writeString(orders.resolve("orders.csv"), "id,amount\n7,70.00\n");
        Files.writeString(customers.resolve("customers.csv"), "id,name\n8,ada\n");
        String database = "e2e_one_loader_" + UUID.randomUUID().toString().replace("-", "");

        Set<String> before = initializations();
        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl(database))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(jar));

            control.testConnection("src_orders", E2eConnectorJar.CONNECTOR_ID, Map.of("uri", orders.toString()));
            control.discoverSchema("src_orders", E2eConnectorJar.CONNECTOR_ID, Map.of("uri", orders.toString()));
            control.discoverSchema("src_customers", E2eConnectorJar.CONNECTOR_ID,
                    Map.of("uri", customers.toString()));
            assertThat(control.connectionSchemaTables("src_customers"))
                    .as("the last of the three opens did reach the connector")
                    .containsExactly("customers");
        }

        List<String[]> staged = initializations().stream()
                .filter(line -> !before.contains(line))
                .map(line -> line.split(" ", 2))
                .filter(fields -> fields[0].contains("tapstate-e2e-plugins"))
                .toList();
        assertThat(staged)
                .as("initializations of the connector class from the jar the server staged; with none, the opens "
                        + "below were never observed and nothing here would be tested")
                .isNotEmpty();
        Map<String, Set<String>> loadersByJar = staged.stream().collect(Collectors.groupingBy(
                fields -> fields[0], Collectors.mapping(fields -> fields[1], Collectors.toSet())));
        assertThat(loadersByJar.values())
                .as("loaders the staged jar's connector class was initialized in, by jar: %s", loadersByJar)
                .isNotEmpty()
                .allSatisfy(loaders -> assertThat(loaders).hasSize(1));
    }

    private static Set<String> initializations() {
        String recorded = System.getProperty(CsvConnector.INITIALIZATIONS, "");
        return recorded.isEmpty() ? Set.of() : Set.copyOf(Arrays.asList(recorded.split("\n")));
    }
}
