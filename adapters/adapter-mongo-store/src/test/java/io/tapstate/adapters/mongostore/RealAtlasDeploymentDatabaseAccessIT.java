package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Real Atlas topology and transactional access checks, without a full connector upload or Cloud provider. */
class RealAtlasDeploymentDatabaseAccessIT {
    @Test
    void theStartupProbeWorksOnAtlasAndCommitsNoNamespaces() {
        String base = System.getenv("TAPSTATE_ATLAS_TEST_URI");
        assumeTrue(base != null && !base.isBlank(), "a controlled Atlas URI is required");
        String metadata = "ts_plan_access_" + UUID.randomUUID().toString().substring(0, 8);
        List<String> databases = List.of(metadata, metadata + "_operator", metadata + "_views");
        int slash = base.indexOf('/', base.indexOf("://") + 3);
        int options = base.indexOf('?', slash);
        String uri = base.substring(0, slash + 1) + metadata + (options < 0 ? "" : base.substring(options));
        try (var raw = MongoClients.create(uri);
             AutoCloseable cleanup = () -> {
                 RuntimeException failed = null;
                 for (String database : databases) {
                     try { raw.getDatabase(database).drop(); }
                     catch (RuntimeException failure) {
                         if (failed == null) failed = failure;
                         else failed.addSuppressed(failure);
                     }
                 }
                 if (failed != null) throw failed;
             };
             MongoConnection connection = new MongoConnection(new MongoConnectionSettings(uri, null, Duration.ofSeconds(10)))) {
            connection.verifyConnectivity();
            connection.verifyDeploymentDatabases(databases.subList(1, 3));
            for (String database : databases) {
                assertThat(raw.getDatabase(database).listCollectionNames().into(new ArrayList<>())).isEmpty();
            }
        } catch (Exception failure) {
            throw new AssertionError("the controlled Atlas access witness failed", failure);
        }
    }
}
