package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A read after a connection's settings changed reaches the address they now name.
 *
 * <p>Temporary reads share connector instances rather than opening one each, which is the only reason
 * a preview is cheap enough to serve from a shell. What that costs is a question nothing else asks:
 * an instance was opened against particular settings, so it stays pointed wherever those pointed, and
 * a pool that hands it back for a connection whose settings have since changed answers from the old
 * address.
 *
 * <p>The failure it guards is silent, and the seeding is arranged so that it has to be. Both addresses
 * hold a collection of the same name, so a stale instance is not caught out by a missing collection or
 * a refusal - it answers, promptly and with rows, and the only thing wrong with the answer is that it
 * is the previous address's. Nothing in the response distinguishes it, which is why this has to be
 * asserted rather than noticed.
 *
 * <p>Reuse itself is not asserted here and could not usefully be: it is an in-process event, and a
 * specification that could see it would need the product to have been given a way to say so. What is
 * observable from outside is exactly this - that reuse did not outlive the settings it was valid for.
 *
 * <p>One tier. Nothing about how a connection is keyed differs by how the server was launched.
 */
class DataBrowserConnectorReuseIT {

    private static final String SOURCE_ID = "src_file";
    private static final String COLLECTION = "orders";

    /** The column the two addresses disagree on, and the only thing that tells their rows apart. */
    private static final String WHICH_ADDRESS = "held_by";

    private static final String FIRST = "the-address-declared-first";
    private static final String SECOND = "the-address-declared-second";

    @TempDir
    private Path connectorJars;

    @TempDir
    private Path firstDirectory;

    @TempDir
    private Path secondDirectory;

    private String previousConnectorsDir;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @BeforeEach
    void publishTheConnectorJar() {
        // Packaged under the browsable connector's id: the product serves row reads only to
        // connectors it knows speak the shape it asks in, and that set names real ones.
        E2eConnectorJar.buildInto(connectorJars, E2eConnectorJar.BROWSABLE_CONNECTOR_ID);
        previousConnectorsDir = System.setProperty("tapstate.e2e.connectors-dir", connectorJars.toString());
    }

    @AfterEach
    void restoreTheConnectorsDirectory() {
        if (previousConnectorsDir == null) {
            System.clearProperty("tapstate.e2e.connectors-dir");
        } else {
            System.setProperty("tapstate.e2e.connectors-dir", previousConnectorsDir);
        }
    }

    @Test
    void readsTheAddressTheConnectionNamesNowRatherThanTheOneItWasOpenedAgainst() {
        seed(firstDirectory, FIRST);
        seed(secondDirectory, SECOND);

        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_browser_reuse"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            String connector = E2eConnectorJar.BROWSABLE_CONNECTOR_ID;
            control.registerConnector(connector, ConnectorJars.bytesFor(connector));

            // The first read is what makes the second one a test of anything: it is what opens an
            // instance against these settings and leaves it where a later read could be handed it.
            control.apply(Map.of("src_file.tap.yml", sourceYaml(firstDirectory)));
            assertThat(columnOf(control.find(SOURCE_ID, COLLECTION, Map.of())))
                    .as("the rows a read of %s is answered with while its connection names the first address",
                            COLLECTION)
                    .containsExactly(FIRST);

            // The same source id, the same collection name, a different address. Only the settings moved.
            control.apply(Map.of("src_file.tap.yml", sourceYaml(secondDirectory)));
            assertThat(columnOf(control.find(SOURCE_ID, COLLECTION, Map.of())))
                    .as("the rows the very next read is answered with, the connection having been repointed")
                    .containsExactly(SECOND);
        }
    }

    /** One row saying which address it came from, in the format the connector reads. */
    private static void seed(Path directory, String address) {
        FileEndpoints.replaceTable(directory.resolve(COLLECTION + ".csv"),
                "id," + WHICH_ADDRESS + "\n1," + address + "\n");
    }

    @SuppressWarnings("unchecked")
    private static List<String> columnOf(Map<String, Object> answer) {
        assertThat(answer).containsKey("rows");
        List<String> values = new ArrayList<>();
        for (Map<String, Object> row : (List<Map<String, Object>>) answer.get("rows")) {
            values.add(String.valueOf(row.get(WHICH_ADDRESS)));
        }
        return values;
    }

    private static String sourceYaml(Path directory) {
        return """
                version: tapstate/v1
                kind: source
                id: src_file
                connector: mongodb
                config: { uri: "%s" }
                """
                .formatted(directory);
    }
}
