package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.core.model.SourceResource;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@RequiresDocker
class PreparedNativeStoreIT {
    @Test
    void anInstallationRetainsItsDeclaredTargetAndNeverCleansUpTheBorrowedDatabase() {
        String borrowed = "borrowed_" + UUID.randomUUID().toString().replace("-", "");
        String uri = SharedMongo.replicaSetUrl(borrowed);
        var source = new SourceResource("views", null, "mongodb", Map.of("uri", uri), null, null, null, null);
        try (var client = MongoClients.create(uri)) {
            var target = client.getDatabase(borrowed);
            var marker = new Document("_id", "retained").append("value", "belongs-to-the-scenario");
            target.getCollection("ownership_witness").insertOne(marker);
            try {
                String configurationName;
                String cacheName;
                try (var installation = PreparedNativeStore.withBorrowedMaster("borrowed-target-test", "e2e", "e2e-password", source)) {
                    configurationName = installation.configurationDatabase();
                    cacheName = installation.cacheDatabase();
                    assertThat(client.listDatabaseNames().into(new ArrayList<>())).contains(configurationName, cacheName, borrowed);
                    try (var configuration = MongoConfigurationDomain.open(
                            new MongoConnectionSettings(installation.configurationUri(), null, Duration.ofSeconds(5)), installation.clusterId())) {
                        assertThat(configuration.artifacts().get("views")).contains(source);
                    }
                }
                assertThat(client.listDatabaseNames().into(new ArrayList<>())).doesNotContain(configurationName, cacheName).contains(borrowed);
                assertThat(target.getCollection("ownership_witness").find().first()).isEqualTo(marker);
            } finally {
                target.drop();
            }
        }
    }
}
