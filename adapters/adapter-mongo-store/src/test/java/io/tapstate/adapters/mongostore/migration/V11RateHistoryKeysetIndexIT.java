package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Upgrade witness for the stable equal-timestamp keyset index. */
@RequiresDocker
class V11RateHistoryKeysetIndexIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void theNewKeysetIndexReplacesTheOldRangeIndexAndIsRerunnable() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("v11_history_keyset_index");
            database.drop();
            MongoCollection<Document> collection = SystemCollections.PIPELINE_RATE_HISTORY.on(database);
            SystemCollections.IndexSpec old =
                    new SystemCollections.IndexSpec(List.of("pipelineId", "observedAt"), false);
            collection.createIndex(new Document("pipelineId", 1).append("observedAt", 1),
                    new com.mongodb.client.model.IndexOptions().name(old.indexName()));
            V11RateHistoryKeysetIndex changeset = new V11RateHistoryKeysetIndex();

            changeset.up(database, ChangeSet.Fence.HELD);
            changeset.up(database, ChangeSet.Fence.HELD);

            assertThat(collection.listIndexes()).extracting(index -> index.getString("name"))
                    .contains("pipelineId_observedAt__id_idx")
                    .doesNotContain(old.indexName());
        }
    }
}
