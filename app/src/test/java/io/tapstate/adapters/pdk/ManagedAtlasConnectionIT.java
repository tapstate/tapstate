package io.tapstate.adapters.pdk;

import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Drives a real Atlas connector's connection checks through the production PDK port. */
class ManagedAtlasConnectionIT {

    @Test
    void atlasConnectionChecksReturnStructuredResults() throws Throwable {
        String artifact = System.getProperty("tapstate.pdk.it.atlasJar");
        String uri = System.getenv("TAPSTATE_ATLAS_TEST_URI");
        assumeTrue(artifact != null && !artifact.isBlank() && uri != null && !uri.isBlank(),
                "real Atlas jar and controlled URI are required for this live witness");

        Path jar = Path.of(artifact);
        IntrospectedConnector introspected = new ConnectorIntrospector().introspect(List.of(jar));
        ConnectorRef ref = new ConnectorRef(
                List.of(jar), introspected.className(), introspected.pdkApiVersion(), null,
                introspected.spec());
        Map<String, Object> settings = Map.of("isUri", true, "uri", uri);
        try (PdkConnector connector = PdkConnector.open("mongodb-atlas", ref, settings)) {
            try {
                connector.underLoader(() -> {
                    connector.connector().init(connector.context());
                    return null;
                });
            } catch (Throwable failure) {
                throw new AssertionError("Atlas connector startup failed: " + causeTypes(failure));
            } finally {
                connector.stopQuietly();
            }
        }
        PdkConnectionTester tester = new PdkConnectionTester(id -> ref, Clock.systemUTC());

        ConnectionTestResult result = tester.test(new ConnectionConfig(
                "atlas-live", "mongodb-atlas", settings));
        assertThat(result.connectorId()).isEqualTo("mongodb-atlas");
        assertThat(result.items()).isNotEmpty();
        List<String> checkStatuses = result.items().stream()
                .map(item -> item.name() + ":" + item.status())
                .toList();
        assertThat(result.outcome()).as("Atlas PDK check statuses: %s", checkStatuses)
                .isEqualTo(ConnectionTestResult.Outcome.PASSED);
        assertThat(result.items()).anyMatch(item -> item.status() == ConnectionTestItem.Status.PASSED);
    }

    private static String causeTypes(Throwable failure) {
        StringBuilder types = new StringBuilder();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (!types.isEmpty()) {
                types.append(" -> ");
            }
            types.append(cause.getClass().getSimpleName());
            if (cause.getStackTrace().length > 0) {
                StackTraceElement site = cause.getStackTrace()[0];
                types.append('@').append(site.getClassName())
                        .append('.').append(site.getMethodName())
                        .append(':').append(site.getLineNumber());
            }
        }
        return types.toString();
    }
}
