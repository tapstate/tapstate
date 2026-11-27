package io.tapstate.adapters.mongostore;

import com.mongodb.MongoClientSettings;
import com.mongodb.ConnectionString;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.client.MongoClients;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import java.util.ArrayList;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class MongoCacheBusinessIdentityIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test void readinessUsesOneSnapshotReadAndStillRefusesChangedIdentityAndSchema() {
        var settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                .addCommandListener(MongoCommandObservation.LISTENER).build();
        try (var client = MongoClients.create(settings)) {
            var db = client.getDatabase("identity_" + UUID.randomUUID().toString().replace("-", ""));
            var scope = new CacheBusinessScope("cluster", 1L, "cache");
            var identity = new Document("_id", "cluster").append("clusterId", "cluster")
                    .append("bindingVersion", 1L).append("cacheIncarnation", "cache")
                    .append("layoutId", MongoConfigurationDomain.LAYOUT).append("businessPhase", "READY")
                    .append("businessPreparationId", "prepare").append("claimAdmissionFence", 0L);
            var schema = new Document("_id", "business").append("installedVersion", 1).append("phase", "READY")
                    .append("operationId", "prepare").append("epoch", 1L);
            SystemCollections.CACHE_IDENTITY.on(db).insertOne(identity);
            SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(db).insertOne(schema);
            var access = new MongoCacheBusinessIdentity(db, scope);
            try (var session = client.startSession()) {
                var commands = new ArrayList<String>();
                MongoCommandObservation.CURRENT.set(commands::add);
                try {
                    session.withTransaction(() -> { access.requireReady(session); return null; },
                            TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).build());
                } finally { MongoCommandObservation.CURRENT.remove(); }
                assertThat(commands).containsExactly("aggregate", "commitTransaction");
                SystemCollections.CACHE_IDENTITY.on(db).updateOne(new Document("_id", "cluster"),
                        new Document("$set", new Document("businessPhase", "RETIRED")));
                assertThatThrownBy(() -> session.withTransaction(() -> { access.requireReady(session); return null; }))
                        .isInstanceOfSatisfying(TapstateException.class, ex -> assertThat(ex.code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED));
                SystemCollections.CACHE_IDENTITY.on(db).replaceOne(new Document("_id", "cluster"), identity);
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(db).updateOne(new Document("_id", "business"),
                        new Document("$set", new Document("operationId", "replacement")));
                assertThatThrownBy(() -> session.withTransaction(() -> { access.requireReady(session); return null; }))
                        .isInstanceOfSatisfying(TapstateException.class, ex -> assertThat(ex.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            } finally { db.drop(); }
        }
    }
}
