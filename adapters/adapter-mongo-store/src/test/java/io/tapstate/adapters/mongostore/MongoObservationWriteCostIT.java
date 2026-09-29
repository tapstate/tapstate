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
            MongoObservationStore store = new MongoObservationStore(client, collection,
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
            ObservationStore.Scope scope = new ObservationStore.Scope("inc-a", 7);
            Instant first = Instant.parse("2026-09-27T10:00:00Z");

            assertThat(store.saveScoped(observation(first), scope)).isTrue();
            commands.clear();

            assertThat(store.saveScoped(observation(first.plusSeconds(1)), scope)).isTrue();
            assertSingleConditionalReplacement(commands, scope);
            assertThat(store.readStored("orders").orElseThrow().observation().observedAt())
                    .isEqualTo(first.plusSeconds(1));
        }
    }

    @Test
    void preparedChunksAreMajorityJournaledBeforeManifestPromotion() {
        String databaseName = "observation_chunk_durability_it";
        List<BsonDocument> commands = new CopyOnWriteArrayList<>();
        CommandListener listener = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                if (databaseName.equals(event.getDatabaseName()) && "insert".equals(event.getCommandName())) {
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
            MongoObservationStore store = new MongoObservationStore(client,
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
            Observation large = new Observation("orders", PipelineState.RUNNING, Map.of(), Map.of(),
                    Map.of("orders", "x".repeat(3 * 1024 * 1024)), null,
                    Instant.parse("2026-09-27T10:00:00Z"));

            assertThat(store.saveScoped(large, new ObservationStore.Scope("inc-a", 7))).isTrue();

            List<BsonDocument> chunkInserts = commands.stream()
                    .filter(command -> MongoStorePort.PIPELINE_OBSERVATION_CHUNKS.equals(
                            command.getString("insert").getValue()))
                    .toList();
            assertThat(chunkInserts).isNotEmpty().allSatisfy(command -> {
                BsonDocument concern = command.getDocument("writeConcern");
                assertThat(concern.getString("w").getValue()).isEqualTo("majority");
                assertThat(concern.getBoolean("j").getValue()).isTrue();
            });
        }
    }

    @Test
    void anAdvancingNonemptyChunkHasOneReservationOneInsertAndOnePromotion() {
        String databaseName = "observation_nonempty_chunk_cost_it";
        List<CostCommand> commands = new CopyOnWriteArrayList<>();
        CommandListener listener = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                if (databaseName.equals(event.getDatabaseName())) {
                    commands.add(new CostCommand(event.getCommandName(), event.getCommand().clone()));
                }
            }
        };
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl(databaseName)))
                .addCommandListener(listener).build();
        try (MongoClient client = MongoClients.create(settings)) {
            var database = client.getDatabase(databaseName);
            database.drop();
            var manifests = database.getCollection(MongoStorePort.PIPELINE_OBSERVATION);
            var chunks = database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS);
            MongoObservationStore store = new MongoObservationStore(client, manifests, chunks);
            var owner = new ObservationStore.Scope("inc-chunk-cost", 9);
            Instant first = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
            assertThat(store.saveScoped(observation(first), owner)).isTrue();
            Observation large = new Observation("orders", PipelineState.RUNNING, Map.of(), Map.of(),
                    Map.of("orders", "x".repeat(600 * 1024)), null, first.plusMillis(1));
            commands.clear();

            assertThat(store.saveScoped(large, owner)).isTrue();
            if ("latest-extra-read".equals(System.getProperty("tapstate.cost-gate.family-mutation"))) {
                manifests.find(new Document()).limit(1).first();
            }
            List<CostCommand> captured = List.copyOf(commands);
            List<CostCommand> heartbeats = captured.stream().filter(MongoObservationWriteCostIT::heartbeat).toList();
            // A single chunk has one heartbeat check before its write and one before promotion.
            // Preserve those real IOs without making the cost gate depend on finishing within ten seconds.
            assertThat(heartbeats.size()).as("bounded lease renewal commands").isBetween(0, 2);
            List<CostCommand> publication = captured.stream().filter(command -> !heartbeat(command)).toList();
            assertThat(publication.stream().map(CostCommand::name).toList())
                    .as("one reservation, one nonempty chunk insert, and one manifest promotion")
                    .containsExactly("findAndModify", "insert", "update");
            assertThat(publication.get(0).body().getString("findAndModify").getValue())
                    .isEqualTo(MongoStorePort.PIPELINE_OBSERVATION);
            BsonDocument insert = publication.get(1).body();
            assertThat(insert.getString("insert").getValue()).isEqualTo(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS);
            assertThat(insert.getArray("documents").size()).isEqualTo(1);
            BsonDocument chunk = insert.getArray("documents").getFirst().asDocument();
            assertThat(chunk.getNumber("ordinal").longValue()).isZero();
            assertThat(chunk.getBinary("payload").getData().length).isBetween(600 * 1024, 1024 * 1024);
            assertThat(insert.getDocument("writeConcern").getString("w").getValue()).isEqualTo("majority");
            assertThat(insert.getDocument("writeConcern").getBoolean("j").getValue()).isTrue();
            assertThat(publication.get(2).body().getString("update").getValue()).isEqualTo(MongoStorePort.PIPELINE_OBSERVATION);
            System.out.printf("observation-nonempty-chunk-cost publicationCommands=%d leaseRenewalCommands=%d"
                            + " physicalChunks=1 payloadBytes=%d%n", publication.size(), heartbeats.size(),
                    chunk.getBinary("payload").getData().length);

            assertThat(store.readStored("orders").orElseThrow().observation()).isEqualTo(large);
            assertThat(chunks.countDocuments()).isEqualTo(1);
        }
    }

    private record CostCommand(String name, BsonDocument body) {}

    private static boolean heartbeat(CostCommand observed) {
        if (!observed.name().equals("update")) { return false; }
        BsonDocument command = observed.body();
        if (!command.getString("update").getValue().equals(MongoStorePort.PIPELINE_OBSERVATION)
                || command.getArray("updates").size() != 1) {
            return false;
        }
        var update = command.getArray("updates").getFirst().asDocument().get("u");
        if (!update.isArray() || update.asArray().size() != 1) { return false; }
        var stage = update.asArray().getFirst().asDocument();
        return stage.size() == 1 && stage.containsKey("$set")
                && stage.getDocument("$set").keySet().equals(java.util.Set.of("pending.publishUntil", "revision"));
    }

    private static void assertSingleConditionalReplacement(List<BsonDocument> commands,
            ObservationStore.Scope scope) {
        assertThat(commands).as("one Mongo update and no find/count before the offer").hasSize(1);
        BsonDocument update = commands.getFirst();
        assertThat(update.getString("update").getValue()).isEqualTo(MongoStorePort.PIPELINE_OBSERVATION);
        assertThat(update.getArray("updates")).hasSize(1);
        BsonDocument replacement = update.getArray("updates").get(0).asDocument();
        assertThat(replacement.getBoolean("upsert", org.bson.BsonBoolean.FALSE).getValue()).isFalse();
        assertThat(replacement.getDocument("q").getBinary("_id").getData()).hasSize(32);
        assertThat(replacement.getDocument("q").getArray("$and")).hasSize(2);
        assertThat(replacement.getArray("u")).isNotEmpty();
        assertThat(update.getDocument("writeConcern").getString("w").getValue()).isEqualTo("majority");
        assertThat(update.getDocument("writeConcern").getBoolean("j").getValue()).isTrue();
        assertThat(replacement.toJson()).contains(scope.pipelineIncarnationId())
                .contains(Long.toString(scope.executionGeneration()))
                .contains("inlinePayload");
    }

    private static Observation observation(Instant at) {
        return new Observation("orders", PipelineState.RUNNING, Map.of("records.out", 10L),
                Map.of(), Map.of(), null, at);
    }
}
