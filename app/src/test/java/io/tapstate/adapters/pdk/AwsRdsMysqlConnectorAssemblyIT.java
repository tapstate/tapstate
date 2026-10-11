package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapstate.core.common.JsonReader;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Verifies the real AWS RDS MySQL source artifact through the production connector loader. */
class AwsRdsMysqlConnectorAssemblyIT {

    @Test
    void awsRdsMysqlArtifactRegistersSnapshotAndCdcReadFunctions() {
        String artifact = System.getProperty("tapstate.pdk.it.awsRdsMysqlJar");
        assumeTrue(artifact != null && !artifact.isBlank(),
                "no -Dtapstate.pdk.it.awsRdsMysqlJar — not a real AWS RDS MySQL artifact run");

        Path jar = Path.of(artifact);
        IntrospectedConnector introspected = new ConnectorIntrospector().introspect(List.of(jar));
        assertThat(introspected.className()).isEqualTo("io.tapdata.connector.rds.AWSRDSMySQLConnector");
        assertThat(introspected.specPath()).isEqualTo("aws-rds-mysql-spec.json");
        assertThat(introspected.pdkApiVersion()).isNotBlank();

        @SuppressWarnings("unchecked")
        Map<String, Object> spec = (Map<String, Object>) JsonReader.parse(introspected.spec());
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) spec.get("properties");
        assertThat(properties.get("id")).isEqualTo("aws-rds-mysql");

        ConnectorRef ref = new ConnectorRef(List.of(jar), introspected.className(),
                introspected.pdkApiVersion(), null, introspected.spec());
        try (PdkConnector connector = PdkConnector.open("aws-rds-mysql", ref, Map.of())) {
            assertThat(connector.connectorId()).isEqualTo("aws-rds-mysql");
            ConnectorFunctions functions = connector.functions();
            assertThat(functions.getBatchReadFunction()).isNotNull();
            assertThat(functions.getStreamReadFunction()).isNotNull();
        }
    }
}
