package io.tapstate.adapters.pdk;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

    @Test
    void atlasResolvedHostsCanConnectAndDiscoverThroughStandardFields() throws Throwable {
        String artifact = System.getProperty("tapstate.pdk.it.atlasJar");
        String standardUri = System.getenv("TAPSTATE_ATLAS_STANDARD_URI");
        assumeTrue(artifact != null && !artifact.isBlank()
                        && standardUri != null && !standardUri.isBlank(),
                "real Atlas jar and resolved-host URI are required for this live witness");

        ConnectionString connectionString = new ConnectionString(standardUri);
        assertThat(connectionString.getCredential()).isNotNull();
        assertThat(connectionString.getCredential().getPassword()).isNotNull();
        int optionStart = standardUri.indexOf('?');
        String options = optionStart < 0 ? "" : standardUri.substring(optionStart + 1);
        String database = "ts_plan_standard_" + UUID.randomUUID().toString().substring(0, 12);
        Map<String, Object> settings = Map.of(
                "isUri", false,
                "host", String.join(",", connectionString.getHosts()),
                "database", database,
                "user", connectionString.getCredential().getUserName(),
                "password", new String(connectionString.getCredential().getPassword()),
                "additionalString", options);
        Path jar = Path.of(artifact);
        IntrospectedConnector introspected = new ConnectorIntrospector().introspect(List.of(jar));
        ConnectorRef ref = new ConnectorRef(
                List.of(jar), introspected.className(), introspected.pdkApiVersion(), null,
                introspected.spec());

        try (MongoClient raw = MongoClients.create(standardUri)) {
            try {
                MongoCollection<Document> probe = raw.getDatabase(database).getCollection("probe");
                probe.insertOne(new Document("_id", "standard-fields").append("value", 1));

                ConnectionTestResult result = new PdkConnectionTester(id -> ref, Clock.systemUTC()).test(
                        new ConnectionConfig("atlas-standard-live", "mongodb-atlas", settings));
                List<String> checkStatuses = result.items().stream()
                        .map(item -> item.name() + ":" + item.status())
                        .toList();
                assertThat(result.outcome()).as("Atlas standard-field PDK check statuses: %s", checkStatuses)
                        .isEqualTo(ConnectionTestResult.Outcome.PASSED);
                assertThat(result.items())
                        .anyMatch(item -> item.status() == ConnectionTestItem.Status.PASSED);
                assertThat(new PdkCapturePort(id -> ref)
                        .discoverSchema(new CaptureConfig("mongodb-atlas", settings, List.of()))
                        .tables())
                        .extracting(table -> table.name())
                        .contains("probe");
            } finally {
                raw.getDatabase(database).drop();
            }
        }
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
