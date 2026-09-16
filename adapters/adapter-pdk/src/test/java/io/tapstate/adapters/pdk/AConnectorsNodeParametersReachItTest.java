package io.tapstate.adapters.pdk;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A connector's spec declares two config forms, not one: the connection form a connection is authored
 * against, and the node form whose settings belong to one use of that connection inside a pipeline.
 * Both are the connector's own declaration, and a connector reads the second off the node config the
 * host hands it — there is no other way in. The host authors one settings map, so the connector's own
 * spec is what says which of those settings belong to the node.
 *
 * <p>The witness is MongoDB's {@code preImage} switch, which its spec declares under
 * {@code configOptions.node}. The connector asks its change stream for
 * {@code fullDocumentBeforeChange} only when that switch is on, and a MongoDB delete otherwise carries
 * the document key alone. A view keyed on a field of the document — the ordinary case — then has
 * nothing to build its filter from the first time anything is deleted, and the whole run fails on that
 * one event. Enabling pre-images on the collection does not help: no server-side configuration
 * supplies a before-image the client never asked for.
 *
 * <p>The seam is {@link PdkConnector#open}, the one place every connector-facing config map is built,
 * so what arrives here is what every discovery, connection test and drive sees.
 */
class AConnectorsNodeParametersReachItTest {

    /**
     * A spec in the shape connectors ship: a connection form, and a node form declaring the switch a
     * MongoDB source reads its before-image behaviour from.
     */
    private static final String SPEC = """
            {
              "properties": {"id": "demo"},
              "configOptions": {
                "connection": {
                  "type": "object",
                  "properties": {
                    "uri": {"type": "string", "x-component": "Input"}
                  }
                },
                "node": {
                  "properties": {
                    "preImage": {"type": "boolean", "x-component": "Switch"}
                  }
                }
              }
            }
            """;

    @Test
    void aParameterTheConnectorDeclaresForItsNodeArrivesAsNodeConfig(@TempDir Path dir) {
        ConnectorRef ref = new ConnectorRef(
                List.of(Synthetic.discoverableSource(dir)), "synthetic.Discoverable", "2.0.8", null, SPEC);
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("uri", "mongodb://localhost:27017/orders");
        settings.put("preImage", true);

        try (PdkConnector connector = PdkConnector.open("demo", ref, settings)) {
            assertThat(connector.context().getNodeConfig())
                    .as("a connector reads a parameter its spec declares for the node off the node "
                            + "config and nowhere else, so a null one is the setting never arriving")
                    .containsEntry("preImage", true);
        }
    }
}
