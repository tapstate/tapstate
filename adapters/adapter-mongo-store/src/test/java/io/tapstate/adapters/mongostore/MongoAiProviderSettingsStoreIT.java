package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AiProviderSettingsStore.Settings;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class MongoAiProviderSettingsStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Test void encryptedCredentialsSurviveRotationAndCannotCrossWorkspaceOrEndpoint() {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("ai_" + UUID.randomUUID().toString().replace("-", ""));
            var rings = new SourceConfigKeyringStore(client, database); rings.loadOrCreateCipher();
            var store = new MongoAiProviderSettingsStore(client, database);
            var settings = new Settings("openai-compatible", "https://api.openai.com/v1", "model", "sk-integration-secret");
            store.put("org-a/cluster-a", settings);
            assertThat(store.get("org-a/cluster-a")).contains(settings);
            assertThat(store.get("org-b/cluster-b")).isEmpty();
            var meta = SystemCollections.SYSTEM_META.on(database);
            Document before = meta.find(new Document("_id", MongoAiProviderSettingsStore.id("org-a/cluster-a"))).first();
            assertThat(before.toJson()).doesNotContain("sk-integration-secret").contains("tsai:1:");
            var handle = new SourceConfigKeyringHandle(rings); handle.prepareRotation(); handle.activatePrepared();
            handle.retireReadOnlyKeys();
            assertThat(store.get("org-a/cluster-a")).contains(settings);
            Document after = meta.find(new Document("_id", before.get("_id"))).first();
            assertThat(after.getString("secret")).isNotEqualTo(before.getString("secret"));
            meta.updateOne(new Document("_id", before.get("_id")), new Document("$set", new Document("baseUrl", "https://api.deepseek.com/v1")));
            assertThatThrownBy(() -> store.get("org-a/cluster-a")).isInstanceOf(TapstateException.class);
            store.delete("org-a/cluster-a"); assertThat(store.get("org-a/cluster-a")).isEmpty();
            database.drop();
        }
    }
    @Test void missingKeyringWithExistingCredentialFailsClosed() {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("ai_missing_" + UUID.randomUUID().toString().replace("-", ""));
            SystemCollections.SYSTEM_META.on(database).insertOne(new Document("_id", "ai-provider:retained").append("secret", "tsai:1:missing:payload"));
            assertThatThrownBy(() -> new SourceConfigKeyringStore(client, database).loadOrCreateCipher()).isInstanceOf(TapstateException.class);
            database.drop();
        }
    }
}
