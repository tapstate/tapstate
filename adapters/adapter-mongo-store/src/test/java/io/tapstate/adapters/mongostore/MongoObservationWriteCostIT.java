package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/** The routine latest-state offer is one fenced Mongo write with no preliminary database read. */
@RequiresDocker
class MongoObservationWriteCostIT {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void anAdvancingScopedObservationCostsOneConditionalWriteWithoutAPreRead() {
        String databaseName = "observation_write_cost_it";
        List<BsonDocument> commands = new CopyOnWriteArrayList<>();
        CommandListener listener = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                if (databaseName.equals(event.getDatabaseName())
                        && List.of("find", "aggregate", "count", "update", "findAndModify")
                                .contains(event.getCommandName())) {
                    commands.add(event.getCommand().clone());
                }
            }
        };
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                .addCommandListener(listener).build();
        try (MongoClient client = MongoClients.create(settings)) {
            var database = client.getDatabase(databaseName);
            database.drop();
            MongoCollection<Document> collection = database.getCollection(MongoStorePort.PIPELINE_OBSERVATION);
            MongoObservationStore store = new MongoObservationStore(collection);
            ObservationStore.Scope scope = new ObservationStore.Scope("inc-a", 7);
            Instant first = Instant.parse("2026-09-27T10:00:00Z");

            assertThat(store.saveScoped(observation(first), scope)).isTrue();
            assertSingleConditionalReplacement(commands, scope);
            commands.clear();

            assertThat(store.saveScoped(observation(first.plusSeconds(1)), scope)).isTrue();
            assertSingleConditionalReplacement(commands, scope);
            assertThat(store.readStored("orders").orElseThrow().observation().observedAt())
                    .isEqualTo(first.plusSeconds(1));
        }
    }

    private static void assertSingleConditionalReplacement(List<BsonDocument> commands,
            ObservationStore.Scope scope) {
        assertThat(commands).as("one Mongo update and no find/count before the offer").hasSize(1);
        BsonDocument update = commands.getFirst();
        assertThat(update.getString("update").getValue()).isEqualTo(MongoStorePort.PIPELINE_OBSERVATION);
        assertThat(update.getArray("updates")).hasSize(1);
        BsonDocument replacement = update.getArray("updates").get(0).asDocument();
        assertThat(replacement.getBoolean("upsert").getValue()).isTrue();
        assertThat(replacement.getDocument("q").getString("_id").getValue()).isEqualTo("orders");
        assertThat(replacement.getDocument("q").getArray("$or")).isNotEmpty();
        assertThat(replacement.getDocument("u").getString("pipelineIncarnationId").getValue())
                .isEqualTo(scope.pipelineIncarnationId());
        assertThat(replacement.getDocument("u").getInt64("executionGeneration").getValue())
                .isEqualTo(scope.executionGeneration());
    }

    private static Observation observation(Instant at) {
        return new Observation("orders", PipelineState.RUNNING, Map.of("records.out", 10L),
                Map.of(), Map.of(), null, at);
    }
}
