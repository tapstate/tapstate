package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.assertThat;

/** A sampled document model is not a field census or a declaration of every later value's type. */
class MongoSampledDocumentRestartIT {
    private static final String PIPELINE = "sampled_document_restart";

    @Test void offlineSampleChangesAndLaterMixedValuesSurviveRepeatedRetainedRestarts() {
        DockerGate.require(); RealConnectorGate.require("mongodb");
        try (var mongo = mongo(); var storage = PreparedNativeStore.createOn(mongo);
                var client = MongoClients.create(mongo.getReplicaSetUrl("input"))) {
            String input = mongo.getReplicaSetUrl("input");
            String output = mongo.getReplicaSetUrl("output");
            var source = client.getDatabase("input").getCollection("orders");
            var target = client.getDatabase("output").getCollection("orders");
            String longValue = "x".repeat(4096);
            var initial = new Document("_id", 1L).append("name", longValue).append("earlier", "present");
            source.insertOne(initial);
            try (var first = InProcessServer.startPrepared(storage)) {
                var control = new ControlPlane(first.baseUrl()); storage.loginAndAwaitBusiness(control);
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                control.apply(workspace(input, output));
                control.discoverSchema("sampled_input", "mongodb", Map.of("uri", input, "database", "input"));
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                delivered(control, target, initial);
                Await.until("the original snapshot to land", () -> !control.snapshotTables(PIPELINE).isEmpty()
                                && control.snapshotTables(PIPELINE).values().stream().allMatch(t -> t.landed()),
                        () -> "snapshot=" + control.snapshotTables(PIPELINE));
                var live = new Document("_id", 2L).append("name", longValue).append("earlier", "present");
                source.insertOne(live); delivered(control, target, live);
            }

            // Every remaining source document now has different sampled membership, order, kind and width.
            source.deleteMany(new Document());
            var offline = new Document("_id", 3L).append("later", "present").append("name", 7L);
            source.insertOne(offline);
            try (var second = InProcessServer.startPrepared(storage)) {
                var control = new ControlPlane(second.baseUrl()); storage.loginAndAwaitBusiness(control);
                delivered(control, target, offline);
                Await.until("the original documents' offline deletes to arrive", () -> target.countDocuments() == 1,
                        () -> "rows=" + target.find().into(new ArrayList<>()) + ", failure=" + control.failure(PIPELINE));
                // The new CDC value disagrees with the just-discovered numeric sample as well.
                var mixed = new Document("_id", 4L).append("name", "a").append("later", "live");
                source.insertOne(mixed); delivered(control, target, mixed);
            }

            source.deleteMany(new Document());
            var shorter = new Document("_id", 5L).append("name", "short");
            source.insertOne(shorter);
            try (var third = InProcessServer.startPrepared(storage)) {
                var control = new ControlPlane(third.baseUrl()); storage.loginAndAwaitBusiness(control);
                delivered(control, target, shorter);
                Await.until("the second retained restart to keep only the actual current document",
                        () -> target.countDocuments() == 1, () -> "failure=" + control.failure(PIPELINE));
                var later = new Document("_id", 6L).append("new_field", true).append("name", longValue + "x");
                source.insertOne(later); delivered(control, target, later);
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void originalTargetPreparationIsNotReplacedByAnotherSample(boolean cdcOnly) {
        DockerGate.require(); RealConnectorGate.require("mongodb");
        try (var mongo=mongo();var storage=PreparedNativeStore.createOn(mongo);
                var client=MongoClients.create(mongo.getReplicaSetUrl("input"))) {
            String input=mongo.getReplicaSetUrl("input"),output=mongo.getReplicaSetUrl("output");
            var source=client.getDatabase("input").getCollection("orders");
            var target=client.getDatabase("output").getCollection("orders");
            source.insertOne(new Document("_id", 1L).append("name","a"));
            String longer="x".repeat(4096);
            try(var first=InProcessServer.startPrepared(storage)) {
                var control=new ControlPlane(first.baseUrl());storage.loginAndAwaitBusiness(control);
                control.registerConnector("mongodb",ConnectorJars.bytesFor("mongodb"));
                var resources=new java.util.HashMap<>(workspace(input,output));
                if(cdcOnly)resources.put("pipeline.tap.yml",resources.get("pipeline.tap.yml").replace("snapshot_and_cdc","cdc_only"));
                control.apply(resources);
                control.discoverSchema("sampled_input","mongodb",Map.of("uri",input,"database","input"));
                // A snapshot pin records this later sample, not the discovery used for initial planning.
                if(!cdcOnly)source.updateOne(new Document("_id", 1L),new Document("$set",new Document("name",longer)));
                control.lifecycle(PIPELINE,LifecycleVerb.START);
                Await.until("the original run to start",() -> control.state(PIPELINE)
                                .filter(io.tapstate.core.lifecycle.PipelineState.RUNNING::equals).isPresent(),
                        () -> "failure="+control.failure(PIPELINE));
                if(!cdcOnly)delivered(control,target,new Document("_id", 1L).append("name",longer));
                var live=new Document("_id", 2L).append("name",cdcOnly?"b":longer);
                source.insertOne(live);delivered(control,target,live);
                control.stop(PIPELINE,false);
                Await.until("the original run to stop without clearing its state",() -> control.state(PIPELINE)
                                .filter(io.tapstate.core.lifecycle.PipelineState.STOPPED::equals).isPresent(),
                        () -> "state="+control.state(PIPELINE));
            }
            var offline=new Document("_id", 3L).append("name",longer);source.insertOne(offline);
            try(var second=InProcessServer.startPrepared(storage)) {
                var control=new ControlPlane(second.baseUrl());storage.loginAndAwaitBusiness(control);
                if(cdcOnly)control.discoverSchema("sampled_input","mongodb",Map.of("uri",input,"database","input"));
                control.lifecycle(PIPELINE,LifecycleVerb.START);
                delivered(control,target,offline);
            }
        }
    }

    private static void delivered(ControlPlane control, MongoCollection<Document> target, Document expected) {
        Await.until("the complete actual document " + expected.get("_id") + " to arrive", () -> {
            assertThat(control.failure(PIPELINE)).as("actual processing must not silently fail").isEmpty();
            return expected.equals(target.find(new Document("_id", expected.get("_id"))).first());
        }, () -> "actual=" + target.find(new Document("_id", expected.get("_id"))).first()
                + ", state=" + control.state(PIPELINE) + ", failure=" + control.failure(PIPELINE));
    }

    private static MongoDBContainer mongo() {
        var mongo = new MongoDBContainer(DockerImageName.parse(System.getProperty("tapstate.e2e.mongo-image", "mongo:7.0")))
                .withTmpFs(Map.of("/data/db", "rw,size=1g", "/data/configdb", "rw,size=16m"));
        var command = new ArrayList<>(Arrays.asList(mongo.getCommandParts()));
        command.addAll(List.of("--wiredTigerCacheSizeGB", "0.25", "--oplogSize", "16"));
        mongo.withCommand(command.toArray(String[]::new));
        String owner = System.getProperty("tapstate.e2e.resource-owner");
        if (owner != null && !owner.isBlank()) mongo.withLabel(
                System.getProperty("tapstate.e2e.resource-owner-label", "io.tapstate.e2e.resource-owner"), owner);
        mongo.start(); return mongo;
    }

    private static Map<String, String> workspace(String input, String output) {
        return Map.of("input.tap.yml", """
                version: tapstate/v1
                kind: source
                id: sampled_input
                connector: mongodb
                config: { uri: "%s", database: input }
                mode: cdc
                tables: [orders]
                """.formatted(input), "output.tap.yml", """
                version: tapstate/v1
                kind: source
                id: sampled_output
                connector: mongodb
                config: { uri: "%s", database: output }
                """.formatted(output), "pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: sampled_document_restart
                source: sampled_input
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: rows, from: [orders], type: filter, expr: "true" }
                serve:
                  from: rows
                  sync:
                    - source: sampled_output
                """);
    }
}
