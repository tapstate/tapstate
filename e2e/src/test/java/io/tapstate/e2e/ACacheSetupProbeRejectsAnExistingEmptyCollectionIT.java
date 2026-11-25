package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoStorageConnectionProbe;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

@RequiresDocker
class ACacheSetupProbeRejectsAnExistingEmptyCollectionIT {

    @Test
    void anEmptyDatabasePassesButAnEmptyCollectionBlocksFreshCacheSetup() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String emptyDatabase = "cache_setup_empty_" + suffix;
        String occupiedDatabase = "cache_setup_occupied_" + suffix;
        try (MongoClient client = MongoClients.create(SharedMongo.replicaSetUrl("admin"))) {
            try {
                var probe = new MongoStorageConnectionProbe(Duration.ofSeconds(5));
                var empty = connection(probe, emptyDatabase);
                assertThat(probe.testCache(empty).empty()).isTrue();
                assertThat(client.getDatabase(emptyDatabase).listCollectionNames().into(new ArrayList<>())).isEmpty();

                var occupied = client.getDatabase(occupiedDatabase);
                occupied.createCollection("unrelated");
                assertThat(occupied.getCollection("unrelated").countDocuments()).isZero();

                var result = probe.testCache(connection(probe, occupiedDatabase));
                assertThat(result.empty()).isFalse();
                assertThat(result.suggestedDatabase()).matches(java.util.regex.Pattern.quote(occupiedDatabase) + "_[0-9]{6}");
                assertThat(client.getDatabase(result.suggestedDatabase())
                        .listCollectionNames().into(new ArrayList<>())).isEmpty();
            } finally {
                client.getDatabase(emptyDatabase).drop();
                client.getDatabase(occupiedDatabase).drop();
            }
        }
    }

    private static StorageBinding.Connection connection(MongoStorageConnectionProbe probe, String database) {
        return probe.normalize(new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                SharedMongo.replicaSetUrl(database), database, null));
    }
}
