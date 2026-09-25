package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** A real Mongo server exposes command-family deltas to an external observer. */
class BenchmarkMongoCommandSamplerIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void serverStatusCountsRealFindInsertUpdateAndDeleteCommands() {
        String uri = SharedMongo.replicaSetUrl("benchmark_command_sampler_test");
        try (MongoClient actor = MongoClients.create(uri);
                BenchmarkMongoCommandSampler sampler = BenchmarkMongoCommandSampler.open(uri)) {
            MongoCollection<Document> collection = actor.getDatabase("benchmark_command_sampler_test")
                    .getCollection("rows");
            collection.drop();

            sampler.start();
            collection.insertOne(new Document("id", 1L).append("value", 1));
            assertThat(collection.find(Filters.eq("id", 1L)).first()).isNotNull();
            assertThat(collection.updateOne(Filters.eq("id", 1L), Updates.set("value", 2))
                    .getMatchedCount()).isEqualTo(1);
            assertThat(collection.deleteOne(Filters.eq("id", 1L)).getDeletedCount()).isEqualTo(1);
            BenchmarkMongoCommandSampler.Summary result = sampler.finish();

            assertThat(result.byCommand()).containsKeys("insert", "find", "update", "delete")
                    .doesNotContainKey("serverStatus");
            assertThat(result.byFamily().get(BenchmarkMongoCommandSampler.Family.READ)).isPositive();
            assertThat(result.byFamily().get(BenchmarkMongoCommandSampler.Family.WRITE))
                    .isGreaterThanOrEqualTo(3);
            assertThat(result.totalCommands()).isGreaterThanOrEqualTo(4);
        }
    }
}
