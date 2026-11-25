package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.testsupport.RequiresDocker;
import java.util.List;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.Test;

@RequiresDocker
class StoreDocumentsIT {
    @Test
    void nativeConsumersExcludeDetachedMembershipsAndObserveReattachment() {
        String name = "consumer_memberships_" + UUID.randomUUID().toString().replace("-", "");
        String uri = SharedMongo.replicaSetUrl(name);
        try (var client = MongoClients.create(uri); var documents = StoreDocuments.nativeSrsAt(uri)) {
            var database = client.getDatabase(name);
            try {
                SystemCollections.SRS_CHAINS.on(database).insertOne(new Document("_id", "shared"));
                var members = SystemCollections.SRS_CONSUMERS.on(database);
                var stopped = new Document("_id", new Document("chain", "shared").append("consumer", "stopped"))
                        .append("detached", true);
                members.insertMany(List.of(stopped,
                        new Document("_id", new Document("chain", "shared").append("consumer", "survivor"))
                                .append("detached", false),
                        new Document("_id", new Document("chain", "other").append("consumer", "unrelated"))
                                .append("detached", false)));

                assertThat(documents.consumersOf("shared")).containsExactly("survivor");
                assertThat(documents.consumerMembership("shared", "stopped")).isEqualTo(stopped);
                assertThat(documents.consumerOffset("shared", "stopped")).isNull();
                assertThat(documents.consumersOf("missing")).isEmpty();

                members.updateOne(new Document("_id", stopped.get("_id")),
                        new Document("$set", new Document("detached", false)));
                assertThat(documents.consumersOf("shared")).containsExactlyInAnyOrder("survivor", "stopped");
            } finally {
                database.drop();
            }
        }
    }

    @Test
    void legacyConsumersDoNotRequireANativeMembershipFlag() {
        String name = "legacy_consumers_" + UUID.randomUUID().toString().replace("-", "");
        String uri = SharedMongo.replicaSetUrl(name);
        try (var client = MongoClients.create(uri); var documents = StoreDocuments.at(uri)) {
            var database = client.getDatabase(name);
            try {
                database.getCollection(MongoStorePort.SRS_META).insertOne(new Document("_id", "shared"));
                database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS).insertMany(List.of(
                        new Document("miningChainId", "shared").append("pipelineId", "survivor"),
                        new Document("miningChainId", "other").append("pipelineId", "unrelated")));

                assertThat(documents.consumersOf("shared")).containsExactly("survivor");
                assertThat(documents.consumersOf("missing")).isEmpty();
            } finally {
                database.drop();
            }
        }
    }
}
