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
        } catch (MongoException | IllegalArgumentException failure) {
            throw new AssertionError("Controlled SRV connection failed: " + failure.getClass().getSimpleName());
        } finally {
            driverLog.setLevel(previousLevel);
        }
    }

    private static void verifyConnections(String baseUri) {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String metadataDatabase = "ts_srv_metadata_" + suffix;
        String metadataUri = inDatabase(baseUri, metadataDatabase);
        ConnectionString metadata = new ConnectionString(metadataUri);
        assertThat(metadata.getCredential()).isNotNull();
        assertThat(metadata.getCredential().getSource()).isEqualTo("admin");
        ping(metadata);

        String viewsDatabase = "ts_srv_views_" + suffix;
        ConnectionString views = new ConnectionString(ViewStoreSeedRunner.viewsUri(metadataUri, viewsDatabase));
        assertThat(views.getDatabase()).isEqualTo(viewsDatabase);
        assertThat(views.getCredential()).isNotNull();
        assertThat(views.getCredential().getSource()).isEqualTo("admin");
        assertThat(views.getCredential().equals(metadata.getCredential()))
                .as("the derived connection retains the metadata identity").isTrue();
        ping(views);
    }

    private static void ping(ConnectionString connection) {
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connection)
                .applyToClusterSettings(cluster -> cluster.serverSelectionTimeout(15, TimeUnit.SECONDS))
                .timeout(20, TimeUnit.SECONDS).build();
        try (var client = MongoClients.create(settings)) {
            assertThat(client.getDatabase(connection.getDatabase()).runCommand(new Document("ping", 1))
                    .getDouble("ok")).isEqualTo(1.0);
        }
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
