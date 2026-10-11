package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapstate.core.common.JsonReader;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Verifies the real Atlas artifact through the host's production connector loader. */
class ManagedAtlasConnectorAssemblyIT {

    @Test
    void atlasArtifactRegistersSnapshotCdcAndTargetWriteFunctions() {
        String artifact = System.getProperty("tapstate.pdk.it.atlasJar");
        assumeTrue(artifact != null && !artifact.isBlank(),
                "no -Dtapstate.pdk.it.atlasJar — not a real Atlas artifact run");

        Path jar = Path.of(artifact);
        IntrospectedConnector introspected = new ConnectorIntrospector().introspect(List.of(jar));
        assertThat(introspected.className()).isEqualTo("io.tapdata.mongodb.MongodbAtlasConnector");
        assertThat(introspected.specPath()).isEqualTo("atlas-spec.json");
        assertThat(introspected.pdkApiVersion()).isNotBlank();

        @SuppressWarnings("unchecked")
        Map<String, Object> spec = (Map<String, Object>) JsonReader.parse(introspected.spec());
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) spec.get("properties");
        assertThat(properties.get("id")).isEqualTo("mongodb-atlas");

        ConnectorRef ref = new ConnectorRef(
                List.of(jar), introspected.className(), introspected.pdkApiVersion(), null);
        try (PdkConnector connector = PdkConnector.open("mongodb-atlas", ref, Map.of())) {
            assertThat(connector.connectorId()).isEqualTo("mongodb-atlas");
            ConnectorFunctions functions = connector.functions();
            assertThat(functions.getBatchReadFunction()).isNotNull();
            assertThat(functions.getStreamReadFunction()).isNotNull();
            assertThat(functions.getWriteRecordFunction()).isNotNull();
        }
    }
}
