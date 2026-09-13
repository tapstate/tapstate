package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Nothing a caller writes in a request can move a read off the connection it was resolved from.
 *
 * <p>Driven straight at the HTTP endpoint rather than through the CLI, and that is a requirement rather
 * than convenience: an implementation that confined reads in the CLI alone would pass every test written
 * through the CLI and be wide open to anyone who dialled the port. What is asserted here is the server's
 * own behaviour, so the server is what is dialled.
 *
 * <p>Which database a read reaches follows from the source's declaration, and the request shape has no
 * room to name one — not the database, not the command to dispatch, not the collection. That absence is
 * the guarantee, so what is checked is that writing one down anyway is refused rather than tolerated: a
 * tolerated unknown field would serve an answer that quietly ignored it, and a caller could not tell that
 * answer from one that honoured it.
 *
 * <p>The last assertion is the one that makes the others mean anything. A refusal proves the request was
 * judged; it does not prove nothing happened first. Reading the whole address back afterwards and finding
 * it byte-for-byte unchanged is what separates "refused" from "refused after doing it".
 */
class DataBrowserQueryConfinementIT {

    private static final String COLLECTION = "orders";

    /** A collection this source does not hold. The name is the control plane's own, which it also is not. */
    private static final String NOT_HERE = "tokens";

    /** Reading the absent collection uses the same source, so the refusal cannot be about the source. */
    private static final String NOT_HERE_SOURCE = "src_file";

    @TempDir
    private Path workspace;

    @TempDir
    private Path connectorJars;

    @TempDir
    private Path dataDirectory;

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
    void refusesEveryFieldThatWouldAimAReadSomewhereElseAndLeavesTheAddressUntouched() {
        writeWorkspace();
        seed();
        Map<String, String> before = addressContents();

        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_browser_confinement"));
                Endpoints files = new FileEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            HttpTierBinding binding = new HttpTierBinding(
                    control, workspace, Map.of(E2eConnectorJar.BROWSABLE_CONNECTOR_ID, files), env());
            binding.registerConnector(E2eConnectorJar.BROWSABLE_CONNECTOR_ID);
            binding.applyResources(List.of("src_file.tap.yml"));

            // (1) The command a read dispatches is the server's, and there is no way to say otherwise. The
            // face pins one command name; a body naming another is not a request this shape can express.
            assertThat(control.findExpectingRefusal("src_file", COLLECTION, Map.of("command", "dropDatabase"))
                    .status())
                    .as("a body naming the command to dispatch")
                    .isEqualTo(400);
            assertThat(control.findExpectingRefusal("src_file", COLLECTION, Map.of("op", "$where")).status())
                    .as("a body carrying an operator where the shape has no operator")
                    .isEqualTo(400);

            // (2) And no way to name a database either, which is the same guarantee at the level that
            // matters most: the read runs on the connection the declaration resolved to, whatever the
            // caller writes down.
            for (String database : List.of("tapstate", "tapstate_nest")) {
                ControlPlane.Refusal refusal = control.findExpectingRefusal(
                        "src_file", COLLECTION, new LinkedHashMap<>(Map.of("database", database)));
                assertThat(refusal.status()).as("a body naming the database `%s`", database).isEqualTo(400);
            }

            // (3) A collection the source's own address does not hold is refused by code, not answered
            // with nothing. An empty answer and an absent collection are different facts.
            assertThat(control.findExpectingRefusal(NOT_HERE_SOURCE, NOT_HERE, Map.of()).code())
                    .as("reading a collection this source does not hold")
                    .isEqualTo("data-browser.unknown-collection");

            // The reads that are within the shape still work, so the refusals above are the shape refusing
            // what it has no room for rather than the face being broken.
            assertThat(control.find("src_file", COLLECTION, Map.of()).get("rows"))
                    .as("a request that stays inside the shape")
                    .isInstanceOf(List.class);
        }

        // (4) Nothing was written, moved or removed anywhere in the address the source points at. A
        // refusal that had already done its damage would pass every assertion above.
        assertThat(addressContents())
                .as("the whole of the address the source declares, read back after every refusal")
                .isEqualTo(before);
    }

    /** Every file in the declared address with its contents, so a change of any kind shows up as one. */
    private Map<String, String> addressContents() {
        Map<String, String> contents = new TreeMap<>();
        try (Stream<Path> entries = Files.list(dataDirectory)) {
            for (Path entry : (Iterable<Path>) entries::iterator) {
                contents.put(entry.getFileName().toString(), Files.readString(entry));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return contents;
    }

    private void seed() {
        FileEndpoints.replaceTable(dataDirectory.resolve(COLLECTION + ".csv"),
                "id,status\n1,paid\n2,shipped\n3,paid\n");
    }

    private void writeWorkspace() {
        write(workspace.resolve("src_file.tap.yml"), """
                version: tapstate/v1
                kind: source
                id: src_file
                connector: mongodb
                config: { uri: "${SRC_DIR}" }
                """);
    }

    private UnaryOperator<String> env() {
        return new LinkedHashMap<>(Map.of("SRC_DIR", dataDirectory.toString()))::get;
    }

    private static void write(Path file, String content) {
        try {
            Files.writeString(file, content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
