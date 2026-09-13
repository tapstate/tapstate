package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** A newly seeded source must have a native CDC position before the product samples its seam. */
@RequiresDocker
class SqlServerCdcReadinessIT {
    @Test
    void seedWaitsForTheCaptureJobToPublishItsNativePosition() throws Exception {
        SqlServerEndpoints endpoints = new SqlServerEndpoints();
        EndpointAddress address = new EndpointAddress("src",
                SharedSqlServer.settings("sqlserver_cdc_readiness"));
        endpoints.seed(address, "orders", List.of(Map.of("id", 1L, "seq", 1L)));
        try (var connection = endpoints.connect(address);
             var statement = connection.createStatement();
             var results = statement.executeQuery("SELECT max(start_lsn) FROM cdc.lsn_time_mapping")) {
            assertThat(results.next()).isTrue();
            assertThat(results.getBytes(1))
                    .as("the native connector's initial CDC position must exist when seed returns")
                    .isNotNull();
        }
    }
}
