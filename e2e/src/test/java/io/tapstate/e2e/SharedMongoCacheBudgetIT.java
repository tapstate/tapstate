package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** The optional cache budget reaches the real daemon while replica-set writes still work. */
@RequiresDocker
class SharedMongoCacheBudgetIT {
    @Test
    void aConfiguredCacheBudgetKeepsTheSharedReplicaSetWritable() {
        String uri = SharedMongo.replicaSetUrl("cache_budget_" + UUID.randomUUID().toString().replace("-", ""));
        try (var client = MongoClients.create(uri)) {
            var database = client.getDatabase(new ConnectionString(uri).getDatabase());
            var admin = client.getDatabase("admin");
            Document hello = admin.runCommand(new Document("hello", 1));
            assertThat(hello.getString("setName")).isEqualTo("docker-rs");
            assertThat(hello.getBoolean("isWritablePrimary")).isTrue();
            var collection = database.getCollection("budget_rows");
            collection.insertOne(new Document("_id", "probe").append("value", 7));
            assertThat(collection.find(new Document("_id", "probe")).first().getInteger("value")).isEqualTo(7);

            Document options = admin.runCommand(new Document("getCmdLineOpts", 1)).get("parsed", Document.class);
            Document storage = options.get("storage", Document.class);
            Document wiredTiger = storage == null ? null : storage.get("wiredTiger", Document.class);
            Document engine = wiredTiger == null ? null : wiredTiger.get("engineConfig", Document.class);
            Number configured = engine == null ? null : engine.get("cacheSizeGB", Number.class);
            String requested = System.getProperty("tapstate.e2e.mongo.wired-tiger-cache-gb");
            if (requested == null) {
                assertThat(configured).as("the ordinary fixture retains the daemon's default cache setting").isNull();
            } else {
                BigDecimal budget = new BigDecimal(requested);
                assertThat(configured).as("the requested budget is a real mongod startup option").isNotNull();
                assertThat(new BigDecimal(configured.toString())).isEqualByComparingTo(budget);
                long maximum = admin.runCommand(new Document("serverStatus", 1))
                        .get("wiredTiger", Document.class).get("cache", Document.class)
                        .get("maximum bytes configured", Number.class).longValue();
                // The daemon realizes decimal GiB budgets in whole MiB, as verified by real readback.
                long realizedBytes = budget.multiply(BigDecimal.valueOf(1024)).longValue() * (1L << 20);
                assertThat(maximum).as("WiredTiger's actual cache ceiling matches the requested budget")
                        .isEqualTo(realizedBytes);
            }
            try {
                if (requested == null) {
                    System.setProperty("tapstate.e2e.mongo.wired-tiger-cache-gb", "0.5");
                    assertThat(SharedMongo.configuredCacheBudget())
                            .as("a late setting cannot change the fixture configuration reported after startup").isEmpty();
                } else {
                    System.clearProperty("tapstate.e2e.mongo.wired-tiger-cache-gb");
                    assertThat(SharedMongo.configuredCacheBudget())
                            .as("clearing a property cannot erase the actual startup cache budget")
                            .hasValueSatisfying(value -> assertThat(value).isEqualByComparingTo(new BigDecimal(requested)));
                }
            } finally {
                if (requested == null) { System.clearProperty("tapstate.e2e.mongo.wired-tiger-cache-gb"); }
                else { System.setProperty("tapstate.e2e.mongo.wired-tiger-cache-gb", requested); }
            }
            database.drop();
        }
    }
}
