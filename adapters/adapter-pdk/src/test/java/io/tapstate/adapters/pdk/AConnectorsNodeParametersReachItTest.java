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
 * host hands it. The host authors one settings map, so the connector's own spec is what says which of
 * those settings belong to the node.
 *
 * <p>The witness is MongoDB's {@code preImage4Sink} switch, which its spec declares under
 * {@code configOptions.node} and which the connector reads as {@code getNodeConfig().get(...)} before
 * deciding whether to turn pre-images on for the collection it writes. A setting read that way has no
 * second way in: hand the connector no node config and it is silenced outright, whatever the workspace
 * wrote. {@code shardCollection} is read the same way.
 *
 * <p>A node-form setting the connector instead loads into a config bean is not a witness of this seam,
 * because the connection config is the whole authored settings map and a bean load takes the matching
 * property out of it. MongoDB's {@code preImage} is that shape: measured on a real MongoDB source, it
 * reaches the change stream with this projection and without it alike. Only the settings read off the
 * node config directly tell the two apart.
 *
 * <p>The seam is {@link PdkConnector#open}, the one place every connector-facing config map is built,
 * so what arrives here is what every discovery, connection test and drive sees.
 */
class AConnectorsNodeParametersReachItTest {

    /**
     * A spec in the shape connectors ship: a connection form, and a node form declaring the switch a
     * MongoDB target reads straight off its node config to decide about pre-images.
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
                    "preImage4Sink": {"type": "boolean", "x-component": "Switch"}
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
        settings.put("preImage4Sink", true);

        try (PdkConnector connector = PdkConnector.open("demo", ref, settings)) {
            assertThat(connector.context().getNodeConfig())
                    .as("a connector reads a parameter its spec declares for the node off the node "
                            + "config and nowhere else, so a null one is the setting never arriving")
                    .containsEntry("preImage4Sink", true);
        }
    }

    @Test
    void aConnectionParameterStaysOutOfTheNodeConfigAndInTheConnectionConfig(@TempDir Path dir) {
        ConnectorRef ref = new ConnectorRef(
                List.of(Synthetic.discoverableSource(dir)), "synthetic.Discoverable", "2.0.8", null, SPEC);
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("uri", "mongodb://localhost:27017/orders");
        settings.put("preImage4Sink", true);

        try (PdkConnector connector = PdkConnector.open("demo", ref, settings)) {
            // The node config is what the node form declares, not a second copy of everything: a
            // connector reading it to decide what this one use of the connection does must not find
            // where the database is mixed in with it.
            assertThat(connector.context().getNodeConfig()).doesNotContainKey("uri");
            assertThat(connector.context().getConnectionConfig())
                    .containsEntry("uri", "mongodb://localhost:27017/orders");
        }
    }

    @Test
    void aConnectorDeclaringNoNodeFormIsHandedAnEmptyNodeConfigRatherThanNone(@TempDir Path dir) {
        // The synthetic paths and the connectors that ship a connection form alone. A connector may
        // read its node config without first checking for one -- an upstream connector does exactly
        // that -- so handing it nothing at all is a bare crash where an empty map answers "not set".
        ConnectorRef ref = new ConnectorRef(
                List.of(Synthetic.discoverableSource(dir)), "synthetic.Discoverable", "2.0.8", null);

        try (PdkConnector connector = PdkConnector.open("demo", ref, Map.of("uri", "mongodb://host/db"))) {
            assertThat(connector.context().getNodeConfig()).isNotNull().isEmpty();
        }
    }
}
