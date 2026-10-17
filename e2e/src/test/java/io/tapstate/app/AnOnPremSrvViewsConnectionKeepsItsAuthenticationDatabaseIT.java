package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClients;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/** A read-only live witness: the specification vocabulary cannot arrange SRV/TXT authentication. */
class AnOnPremSrvViewsConnectionKeepsItsAuthenticationDatabaseIT {

    @Test
    void the_original_and_derived_connections_both_authenticate_against_admin() {
        String baseUri = System.getenv("TAPSTATE_ATLAS_TEST_URI");
        assumeTrue(baseUri != null && baseUri.startsWith("mongodb+srv://"),
                "a controlled SRV test URI is required for this live witness");
        assumeTrue(!Pattern.compile("[?&]authSource=", Pattern.CASE_INSENSITIVE).matcher(baseUri).find(),
                "this live witness requires authSource from TXT rather than an explicit query option");

        // Driver settings logs and exception messages can contain the protected test identity.
        Logger driverLog = (Logger) LoggerFactory.getLogger("org.mongodb.driver");
        Level previousLevel = driverLog.getLevel();
        driverLog.setLevel(Level.OFF);
        try {
            verifyConnections(baseUri);
        } finally {
            driverLog.setLevel(previousLevel);
        }
    }

    private static void verifyConnections(String baseUri) {
        Stage stage = Stage.METADATA_URI;
        try {
            Databases databases = databases(UUID.randomUUID());
            String metadataDatabase = databases.metadata();
            String metadataUri = inDatabase(baseUri, metadataDatabase);
            ConnectionString metadata = new ConnectionString(metadataUri);
            assertThat(metadata.getCredential()).isNotNull();
            assertThat(metadata.getCredential().getSource()).isEqualTo("admin");
            stage = Stage.METADATA_PING;
            ping(metadata);

            stage = Stage.VIEWS_URI;
            String viewsDatabase = databases.views();
            ConnectionString views = new ConnectionString(ViewStoreSeedRunner.viewsUri(metadataUri, viewsDatabase));
            assertThat(views.getDatabase()).isEqualTo(viewsDatabase);
            assertThat(views.getCredential()).isNotNull();
            assertThat(views.getCredential().getSource()).isEqualTo("admin");
            assertThat(views.getCredential().equals(metadata.getCredential()))
                    .as("the derived connection retains the metadata identity").isTrue();
            stage = Stage.VIEWS_PING;
            ping(views);
        } catch (MongoException | IllegalArgumentException failure) {
            throw failureAt(stage, failure);
        }
    }

    record Databases(String metadata, String views) {}

    static Databases databases(UUID identity) {
        String suffix = identity.toString().replace("-", "");
        // Atlas Free accepts at most 38 bytes; keep the full UUID in both distinct names.
        return new Databases("ts_m_" + suffix, "ts_v_" + suffix);
    }

    enum Stage { METADATA_URI, METADATA_PING, VIEWS_URI, VIEWS_PING }

    static AssertionError failureAt(Stage stage, RuntimeException failure) {
        String code = failure instanceof MongoException mongo ? " code=" + mongo.getCode() : "";
        return new AssertionError("Controlled SRV connection failed at " + stage + ": "
                + failure.getClass().getSimpleName() + code);
    }

    private static void ping(ConnectionString connection) {
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connection)
                .applyToClusterSettings(cluster -> cluster.serverSelectionTimeout(15, TimeUnit.SECONDS))
                .timeout(20, TimeUnit.SECONDS).build();
        try (var client = MongoClients.create(settings)) {
            assertSuccessfulPing(client.getDatabase(connection.getDatabase()).runCommand(new Document("ping", 1)));
        }
    }

    static void assertSuccessfulPing(Document response) {
        Object ok = response.get("ok");
        assertThat(ok instanceof Number).as("ping returns a numeric status").isTrue();
        assertThat(((Number) ok).doubleValue()).isEqualTo(1.0);
    }

    private static String inDatabase(String uri, String database) {
        int authorityStart = uri.indexOf("://") + 3;
        int query = uri.indexOf('?', authorityStart);
        String beforeQuery = query < 0 ? uri : uri.substring(0, query);
        int path = beforeQuery.indexOf('/', authorityStart);
        return (path < 0 ? beforeQuery : beforeQuery.substring(0, path)) + "/" + database
                + (query < 0 ? "" : uri.substring(query));
    }
}
