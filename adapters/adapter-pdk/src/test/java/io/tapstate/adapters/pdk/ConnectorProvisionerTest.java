package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConnectorProvisionerTest {

    @Test
    void defaultCacheIdentityIncludesConnectorMetadataAndSortedNormalizedClasspath() {
        ConnectorProvisioner provisioner = connectorId -> new ConnectorRef(
                List.of(Path.of("build", "z.jar"), Path.of("build", "..", "a.jar")),
                "example.OrdersConnector", "1.2", "beta", "{\"spec\":true}");

        assertThat(provisioner.cacheIdentity("orders"))
                .isEqualTo("example.OrdersConnector\n1.2\nbeta\n{\"spec\":true}\n"
                        + Path.of("build", "..", "a.jar").toAbsolutePath().normalize() + "\n"
                        + Path.of("build", "z.jar").toAbsolutePath().normalize());
    }

    @Test
    void defaultCacheIdentityHandlesOptionalConnectorMetadata() {
        ConnectorProvisioner provisioner = connectorId -> new ConnectorRef(
                List.of(), "example.MinimalConnector", null, null, null);

        assertThat(provisioner.cacheIdentity("minimal"))
                .isEqualTo("example.MinimalConnector\n\n\n\n");
    }
}
