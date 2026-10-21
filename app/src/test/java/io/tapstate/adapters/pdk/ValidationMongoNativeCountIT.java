package io.tapstate.adapters.pdk;

import com.mongodb.client.MongoClients;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Real native count dispatch and scalar delivery; not a validation lifecycle or stable-window proof. */
class ValidationMongoNativeCountIT {
    private static final String DATABASE = "validation_native_count";
    private static final String COLLECTION = "records";
    private static final String EMPTY = "empty_records";

    @Test void isolatedConnectorCountsCurrentRowsNativelyWithoutBusinessRowDelivery() throws Throwable {
        String configured = System.getProperty("tapstate.pdk.it.mongodb-jar");
        assumeTrue(configured != null && !configured.isBlank(), "a real MongoDB connector jar is required");
        var classpath = List.of(Path.of(configured));
        var artifact = new ConnectorIntrospector().introspect(classpath);
        var ref = new ConnectorRef(classpath, artifact.className(), artifact.pdkApiVersion(), null, artifact.spec());
        try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            mongo.start();
            String uri = mongo.getReplicaSetUrl(DATABASE);
            try (var observer = MongoClients.create(uri)) {
                var database = observer.getDatabase(DATABASE);
                var rows = database.getCollection(COLLECTION);
                rows.insertMany(List.of(new Document("_id", 1).append("qty", 10),
                        new Document("_id", 2).append("qty", 20), new Document("_id", 3).append("qty", 30)));
                database.createCollection(EMPTY);
                assertThat(database.listCollectionNames().into(new ArrayList<>())).contains(COLLECTION, EMPTY);
                database.runCommand(new Document("profile", 2));
                try (var connector = PdkConnector.open("mongodb", ref,
                        Map.of("uri", uri, "database", DATABASE, "isUri", true))) {
                    try {
                        connector.underLoader(() -> { connector.connector().init(connector.context()); return null; });
                        assertThat(connector.functions().getExecuteCommandFunction()).isNotNull();
                        assertThat(count(connector, COLLECTION, Map.of())).isEqualTo(3);
                        assertThat(count(connector, COLLECTION, Map.of("qty", Map.of("$gte", 20)))).isEqualTo(2);
                        assertThat(count(connector, EMPTY, Map.of())).isZero();
                        rows.deleteOne(eq("_id", 2));
                        assertThat(count(connector, COLLECTION, Map.of())).isEqualTo(2);
                    } finally { connector.stopQuietly(); }
                }
                assertThat(database.listCollectionNames().into(new ArrayList<>())).contains(COLLECTION, EMPTY);
                var profiles = database.getCollection("system.profile").find().into(new ArrayList<>());
                var aggregateCounts = profiles.stream().map(record -> record.get("command", Document.class))
                        .filter(command -> command != null && (COLLECTION.equals(command.get("aggregate"))
                                || EMPTY.equals(command.get("aggregate"))))
                        .toList();
                assertThat(aggregateCounts).as("countDocuments uses native aggregation").hasSize(4);
                assertThat(aggregateCounts).allSatisfy(command ->
                        assertThat(command.getList("pipeline", Document.class))
                                .anySatisfy(stage -> assertThat(stage).containsKey("$group")));
                assertThat(profiles).noneSatisfy(record -> {
                    var command = record.get("command", Document.class);
                    assertThat(command).isNotNull();
                    assertThat(COLLECTION.equals(command.get("find")) || COLLECTION.equals(command.get("count"))
                            || EMPTY.equals(command.get("find")) || EMPTY.equals(command.get("count"))).isTrue();
                });
            }
        }
    }

    private static long count(PdkConnector connector, String table, Map<String, Object> predicate) throws Throwable {
        return connector.underLoader(() -> {
            var command = TapExecuteCommand.create().command("count").params(Map.of(
                    "database", DATABASE, "collection", table, "filter", predicate));
            return PdkValidationCounter.callback(table, sink -> connector.functions().getExecuteCommandFunction()
                    .execute(connector.context(), command, result -> sink.accept(result)), () -> {
                if (!connector.isAlive()) throw new io.tapstate.core.common.TapstateException(
                        io.tapstate.core.validation.ValidationError.READ_CANCELLED, Map.of("table", table), null);
            });
        });
    }
}
