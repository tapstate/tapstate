package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A changed retention is a changed index. The store is brought up twice on one collection with two
 * retentions, and what is read back is the index the server holds — not the configuration — because the
 * cheapest way to bring an index up is "create it if no index of that name exists", and under that way
 * the second start leaves the first retention on the index while everything else reads as configured.
 */
@RequiresDocker
class ChangingRetentionAltersTheExistingIndexIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static Long expiryOn(MongoDatabase database) {
        for (Document index : SystemCollections.PIPELINE_RATE_HISTORY.on(database).listIndexes()) {
            if ("observedAt_idx".equals(index.getString("name"))) {
                return index.get("expireAfterSeconds", Number.class).longValue();
            }
        }
        return null;
    }

    @Test
    void theSecondStartWritesItsRetentionOntoTheIndexTheFirstBuilt() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("history_retention_it");

            new MongoRateHistoryStore(database, SystemCollections.PIPELINE_RATE_HISTORY.on(database),
                    Duration.ofDays(15));
            assertThat(expiryOn(database)).isEqualTo(15L * 24 * 3600);

            new MongoRateHistoryStore(database, SystemCollections.PIPELINE_RATE_HISTORY.on(database),
                    Duration.ofDays(7));
            // Read off the server: the configuration says seven days whatever the index says.
            assertThat(expiryOn(database)).as("the existing index carries the new retention").isEqualTo(7L * 24 * 3600);

            new MongoRateHistoryStore(database, SystemCollections.PIPELINE_RATE_HISTORY.on(database),
                    Duration.ofDays(7));
            assertThat(expiryOn(database)).isEqualTo(7L * 24 * 3600);
        }
    }
}
