package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@RequiresDocker
class MongoStructureInspectionIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final SystemCollections CAPTURE = SystemCollections.CAPTURE_PREPARATIONS;
    private static final SystemCollections STATE = SystemCollections.CACHE_PIPELINE_STATE;
    private static final SystemCollections LAZY = SystemCollections.PDK_NOTES;

    @Test void anHourlyPassReportsExactDifferencesWithoutChangingTheCatalog() {
        var commands = new ArrayList<String>();
        var listener = new CommandListener() {
            @Override public void commandStarted(CommandStartedEvent event) { commands.add(event.getCommandName()); }
        };
        try (var writer = MongoClients.create(MONGO.getReplicaSetUrl());
                var reader = MongoClients.create(MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build())) {
            var database = writer.getDatabase("structure_inspection");
            database.drop();
            try {
                CAPTURE.prepareCollectionOn(database);
                database.createView(STATE.collectionName(), CAPTURE.collectionName(), List.of());
                var observed = reader.getDatabase(database.getName());
                var expected = MongoStructureInspection.namespaces(List.of(CAPTURE, STATE, LAZY),
                        List.of(CAPTURE, STATE), Duration.ofDays(15));

                var result = MongoStructureInspection.inspect(observed, expected, new MongoReadBudget(Duration.ofSeconds(5)));

                assertThat(commands).containsExactly("listCollections", "listIndexes");
                assertThat(result).hasSize(2);
                assertThat(result.getFirst()).satisfies(problem -> {
                    assertThat(problem.problem()).isEqualTo(MongoStructureInspection.Problem.INDEX_MISSING);
                    assertThat(problem.repairCommand()).contains("db.getSiblingDB(\"structure_inspection\")",
                            ".getCollection(\"capture_preparations\")", "capture_preparation_history", ".createIndex(");
                    assertThat(problem.repairCommand().getBytes(java.nio.charset.StandardCharsets.UTF_8)).hasSizeLessThanOrEqualTo(512);
                });
                assertThat(result.getLast().requiresSupport()).isTrue();
                assertThat(result.getLast().problem()).isEqualTo(MongoStructureInspection.Problem.COLLECTION_TYPE);
                assertThat(database.listCollectionNames().into(new ArrayList<>())).doesNotContain(LAZY.collectionName());
                assertThat(CAPTURE.on(database).listIndexes().into(new ArrayList<>())).hasSize(1);

                var original = CAPTURE.indexes().getFirst();
                CAPTURE.on(database).createIndex(original.keyDocument(), new IndexOptions().name(original.indexName()));
                database.getCollection(STATE.collectionName()).drop();
                STATE.prepareCollectionOn(database);
                assertThat(MongoStructureInspection.inspect(observed, expected, new MongoReadBudget(Duration.ofSeconds(5)))).isEmpty();

                CAPTURE.on(database).dropIndex(original.indexName());
                CAPTURE.on(database).createIndex(new Document("expires", 1), new IndexOptions()
                        .name("unexpected_ttl").expireAfter(600L, java.util.concurrent.TimeUnit.SECONDS));
                var damaged = MongoStructureInspection.inspect(observed, expected, new MongoReadBudget(Duration.ofSeconds(5)));
                assertThat(damaged).hasSize(2).allSatisfy(problem -> assertThat(problem.repairCommand()).isNull());
                assertThat(damaged).extracting(MongoStructureInspection.Difference::problem)
                        .containsExactly(MongoStructureInspection.Problem.INDEX_MISSING, MongoStructureInspection.Problem.UNEXPECTED_CONSTRAINT);

                CAPTURE.on(database).drop();
                var absent = MongoStructureInspection.inspect(observed, expected, new MongoReadBudget(Duration.ofSeconds(5)));
                assertThat(absent).singleElement().satisfies(problem -> {
                    assertThat(problem.problem()).isEqualTo(MongoStructureInspection.Problem.COLLECTION_MISSING);
                    assertThat(problem.repairCommand()).isNull();
                });
            } finally { database.drop(); }
        }
    }
    @Test void alertCollectionOptionsAndCollatedIndexesDoNotReportHealthy() {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("structure_alert_contract");
            var alerts = SystemCollections.ALERT_RULES;
            try {
                alerts.prepareCollectionOn(database);
                var indexes = alerts.indexes();
                for (var index : indexes) IndexEnsure.createIfAbsent(alerts.on(database), index);
                var expected = MongoStructureInspection.namespaces(List.of(alerts), List.of(alerts), Duration.ofDays(15));
                assertThat(MongoStructureInspection.inspect(database, expected, new MongoReadBudget(Duration.ofSeconds(5))))
                        .isEmpty();

                var replaced = indexes.getFirst();
                alerts.on(database).dropIndex(replaced.indexName());
                alerts.on(database).createIndex(replaced.keyDocument(), new IndexOptions().name(replaced.indexName())
                        .unique(replaced.unique()).collation(com.mongodb.client.model.Collation.builder().locale("en").build()));
                database.runCommand(new Document("collMod", alerts.collectionName())
                        .append("validator", new Document("fixtureField", new Document("$exists", true))));
                assertThat(MongoStructureInspection.inspect(database, expected, new MongoReadBudget(Duration.ofSeconds(5))))
                        .extracting(MongoStructureInspection.Difference::problem)
                        .contains(MongoStructureInspection.Problem.COLLECTION_OPTIONS, MongoStructureInspection.Problem.INDEX_DEFINITION);
            } finally { database.drop(); }
        }
    }
    @Test void aRealMongoWriteFailureChecksOnlyTheOriginalViewAfterAbort() {
        String name = "structure_targeted_view";
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl());
                var connection = MongoConnection.forConfiguration(
                        new MongoConnectionSettings(MONGO.getReplicaSetUrl(name), null, Duration.ofSeconds(5)))) {
            var database = admin.getDatabase(name);
            try {
                LAZY.prepareCollectionOn(database);
                database.createView(CAPTURE.collectionName(), LAZY.collectionName(), List.of());
                connection.verifyConnectivity();
                try (var scope = io.tapstate.core.common.OperationErrorDiagnostics.open();
                     var measurement = MongoHttpDiagnosticsScope.open()) {
                    org.assertj.core.api.Assertions.assertThatThrownBy(() -> new MongoReadBudget(Duration.ofSeconds(5))
                            .transaction(connection.client(), session -> {
                                CAPTURE.on(connection.database()).insertOne(session, new Document("_id", "fixture"));
                                return null;
                            })).isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                                error -> assertThat(error.code()).isEqualTo(io.tapstate.spi.store.IoError.STORE_UNAVAILABLE));
                    assertThat(io.tapstate.core.common.OperationErrorDiagnostics.appendTo("original"))
                            .contains("original", name + ".capture_preparations", "COLLECTION_TYPE")
                            .doesNotContain("createIndex", "getSiblingDB");
                    var groups = (List<?>) measurement.snapshot().get("mongo_groups");
                    assertThat(groups.stream().map(row -> (String) ((java.util.Map<?, ?>) row).get("command")))
                            .containsExactly("insert", "abortTransaction", "listCollections");
                    assertThat(measurement.snapshot()).containsEntry("complete", true);
                }
                assertThat(LAZY.on(database).countDocuments()).isZero();
            } finally { database.drop(); }
        }
    }
}
