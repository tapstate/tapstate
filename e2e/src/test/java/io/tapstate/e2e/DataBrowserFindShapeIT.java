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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What one read answers with: a bounded number of rows, the size of what it was taken from, and whether
 * it was the whole of it.
 *
 * <p>The last of those is the load-bearing one. This face is one-shot and offers no way to ask for the
 * next rows, so a caller holding ten rows of twenty-five has nothing in the answer's shape to tell it
 * apart from holding a collection of ten — the two are the same object with different numbers in it.
 * Saying so explicitly is the only defence against a preview being read as a complete answer, and since
 * paging was dropped there is no continuation token left whose presence would have hinted at it.
 *
 * <p>No pipeline runs here. What is under test is the read itself, and a pipeline would only be a slower
 * way to put rows somewhere; the rows are written straight into the address the source declares.
 *
 * <p>One tier, for the same reason the listing is: nothing in this read differs by how the server was
 * launched.
 */
class DataBrowserFindShapeIT {

    /** Comfortably over the default page and under the cap, so both bounds are visible in one collection. */
    private static final int SEEDED_ROWS = 25;

    private static final int DEFAULT_PAGE = 10;

    /** Past the cap this face serves. The cap itself is the product's; this only has to be beyond it. */
    private static final int OVER_THE_CAP = 500;

    private static final String COLLECTION = "orders";

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
    void answersABoundedPageThatSaysHowBigTheCollectionIsAndThatMoreRemains() {
        writeWorkspace();
        seedSortableRows();

        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_browser_find_shape"));
                Endpoints files = new FileEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            HttpTierBinding binding = new HttpTierBinding(
                    control, workspace, Map.of(E2eConnectorJar.BROWSABLE_CONNECTOR_ID, files), env());
            binding.registerConnector(E2eConnectorJar.BROWSABLE_CONNECTOR_ID);
            binding.applyResources(List.of("src_file.tap.yml"));

            // (1) Nothing asked for: the control plane's own bound, the size of what it came from, and the
            // statement that it is not all of it. A read that quietly truncated would agree on the rows and
            // differ only here.
            Map<String, Object> unasked = control.find("src_file", COLLECTION, Map.of());
            assertThat(rowsOf(unasked))
                    .as("the rows a read that asked for nothing is answered with")
                    .hasSize(DEFAULT_PAGE);
            assertThat(numberOf(unasked, "approximateTotal"))
                    .as("the size of the collection the page was taken from")
                    .isEqualTo(SEEDED_ROWS);
            assertThat(unasked.get("moreAvailable"))
                    .as("whether the caller was told these ten are not the whole collection")
                    .isEqualTo(true);

            // (2) The whole collection asked for by number, and now there is nothing left over to report.
            Map<String, Object> everything = control.find("src_file", COLLECTION, Map.of("limit", SEEDED_ROWS));
            assertThat(rowsOf(everything)).hasSize(SEEDED_ROWS);
            assertThat(everything.get("moreAvailable"))
                    .as("a read that carries every row there is has nothing more to offer")
                    .isEqualTo(false);

            // (3) Past the cap is refused by code rather than served or silently clamped. Clamping would
            // hand back a page the caller never asked for and cannot distinguish from the one it did.
            assertThat(control.findExpectingRefusal("src_file", COLLECTION, Map.of("limit", OVER_THE_CAP)).code())
                    .as("asking for more rows than this face will serve")
                    .isEqualTo("data-browser.invalid-limit");

            // (4) An order that was asked for is an order that is applied. Descending, because ascending is
            // what several wrong implementations return by accident - the file's own order is ascending.
            Map<String, Object> descending = control.find("src_file", COLLECTION,
                    Map.of("limit", SEEDED_ROWS, "sort", Map.of("field", "status", "dir", "desc")));
            assertThat(columnOf(descending, "status"))
                    .as("the rows of a read that asked for a descending order")
                    .isSortedAccordingTo(java.util.Comparator.reverseOrder())
                    .startsWith(status(SEEDED_ROWS));
        }
    }

    /**
     * Rows carrying a column whose values order the same way as text, because that is the only order this
     * connector can honestly report: it declares every column a string.
     */
    private void seedSortableRows() {
        StringBuilder csv = new StringBuilder("id,status\n");
        for (int row = 1; row <= SEEDED_ROWS; row++) {
            csv.append(row).append(',').append(status(row)).append('\n');
        }
        FileEndpoints.replaceTable(dataDirectory.resolve(COLLECTION + ".csv"), csv.toString());
    }

    /** Zero-padded so that the text order and the numeric order are the same one. */
    private static String status(int row) {
        return String.format("s%02d", row);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> rowsOf(Map<String, Object> answer) {
        assertThat(answer).containsKey("rows");
        return (List<Map<String, Object>>) answer.get("rows");
    }

    private static List<String> columnOf(Map<String, Object> answer, String column) {
        List<String> values = new ArrayList<>();
        for (Map<String, Object> row : rowsOf(answer)) {
            values.add(String.valueOf(row.get(column)));
        }
        return values;
    }

    private static long numberOf(Map<String, Object> answer, String key) {
        assertThat(answer.get(key)).as("`%s` of %s", key, answer).isInstanceOf(Number.class);
        return ((Number) answer.get(key)).longValue();
    }

    private void writeWorkspace() {
        write("src_file.tap.yml", """
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

    private void write(String name, String content) {
        try {
            Files.writeString(workspace.resolve(name), content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
