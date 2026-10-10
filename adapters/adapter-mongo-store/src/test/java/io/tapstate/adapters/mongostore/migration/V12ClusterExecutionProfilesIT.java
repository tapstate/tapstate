package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.MigrationError;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.TapstateException;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RequiresDocker
class V12ClusterExecutionProfilesIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void backfillPreservesCanonicalBytesAndIdentityOnASecondRun() {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("v12_incarnations");
            database.drop();
            var artifacts = SystemCollections.ARTIFACTS.on(database);
            Document legacy = new Document("_id", "orders").append("kind", "pipeline")
                    .append("body", new Document("key", "value")).append("contentHash", "same-hash");
            artifacts.insertOne(legacy);
            var change = new V12ClusterExecutionProfiles();

            change.up(database, ChangeSet.Fence.HELD);
            Document once = artifacts.find().first();
            assertThat(once.getString("incarnation")).isNotBlank();
            assertThat(once.get("body")).isEqualTo(legacy.get("body"));
            assertThat(once.getString("contentHash")).isEqualTo("same-hash");
            change.up(database, ChangeSet.Fence.HELD);
            assertThat(artifacts.find().first()).isEqualTo(once);
            assertThat(database.listCollectionNames()).contains(
                    "cluster_execution_profiles", "cluster_node_registry", "cluster_capacity_occupancy", "cluster_recovery_queue");
        }
    }

    @Test
    void liveBusinessLeaseRefusesTheColdUpgradeBeforeAnyIncarnationWrite() {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("v12_live_legacy");
            database.drop();
            var claims = SystemCollections.WORKLOAD_CLAIMS.on(database);
            claims.insertOne(new Document("_id", "legacy-pipeline").append("resourceType", "PIPELINE_ACTUATION")
                    .append("leaseUntil", new Date(Long.MAX_VALUE)));
            var artifacts = SystemCollections.ARTIFACTS.on(database);
            artifacts.insertOne(new Document("_id", "orders").append("contentHash", "same"));
            var change = new V12ClusterExecutionProfiles();

            assertThatThrownBy(() -> change.up(database, ChangeSet.Fence.HELD))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(MigrationError.COLD_UPGRADE_REQUIRED));
            assertThat(artifacts.find().first()).doesNotContainKey("incarnation");
            claims.updateMany(new Document(), List.of(new Document("$set", new Document("leaseUntil", "$$NOW"))));
            change.up(database, ChangeSet.Fence.HELD);
            assertThat(artifacts.find().first().getString("incarnation")).isNotBlank();
        }
    }
}
