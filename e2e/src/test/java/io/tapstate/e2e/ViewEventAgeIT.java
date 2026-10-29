package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.core.common.*;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.model.SourceResource;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

/** Real Source-to-managed-View data and target ACK precede any monitoring assertions. */
class ViewEventAgeIT {
    @BeforeAll static void requirements(){DockerGate.require();RealConnectorGate.require("mongodb");}
    @Test void aStoppedPublisherCannotLeaveTheLastSampleDisplayedAsCurrent() throws Exception {
        String suffix=UUID.randomUUID().toString().replace("-","");
        String sourceDb="view_age_source_"+suffix,targetDb="view_age_target_"+suffix;
        String sourceUri=SharedMongo.replicaSetUrl(sourceDb),targetUri=SharedMongo.replicaSetUrl(targetDb);
        var target=new SourceResource("views",null,"mongodb",Map.of("uri",targetUri,"database",targetDb),null,null,null,null);
        try(var source=MongoClients.create(sourceUri);var targetClient=MongoClients.create(targetUri);
                var storage=PreparedNativeStore.withBorrowedMaster("view-age-"+suffix,"e2e","e2e-password",target);
                var server=InProcessServer.startPrepared(storage)){
            var collection=source.getDatabase(sourceDb).getCollection("orders");collection.insertOne(new Document("_id",1).append("id",1).append("name","before"));
            var control=new ControlPlane(server.baseUrl());storage.loginAndAwaitBusiness(control);
            control.registerConnector("mongodb",ConnectorJars.bytesFor("mongodb"));
            var resources=new LinkedHashMap<String,String>();
            resources.put("source.tap.yml","""
                    version: tapstate/v1
                    kind: source
                    id: src_age
                    connector: mongodb
                    config: { uri: "%s", database: "%s" }
                    mode: cdc
                    tables: [orders]
                    """.formatted(sourceUri,sourceDb));
            resources.put("pipeline.tap.yml","""
                    version: tapstate/v1
                    kind: pipeline
                    id: view_age
                    source: src_age
                    settings: { read_mode: snapshot_and_cdc }
                    view:
                      id: order_state
                      from: orders
                      primary_key: id
                    """);
            control.apply(resources);control.discoverSchema("src_age","mongodb",Map.of("uri",sourceUri,"database",sourceDb));
            control.lifecycle("view_age",LifecycleVerb.START);
            var landed=targetClient.getDatabase(targetDb).getCollection("order_state");
            Await.until("the real managed View snapshot",Duration.ofSeconds(90),()->landed.countDocuments()==1,()->control.viewsJson());
            Instant changedAt=Instant.now();collection.updateOne(new Document("_id",1),new Document("$set",new Document("name","after")));
            Await.until("the real source change to reach the managed View",Duration.ofSeconds(90),()->landed.countDocuments(new Document("name","after"))==1,()->control.viewsJson());
            Await.until("the target-acked source position",Duration.ofSeconds(90),()->control.durablePosition("view_age","orders").isPresent(),()->control.metricsJson("view_age"));
            Instant targetConfirmedAt=Instant.now();
            String before=control.viewsJson(),metrics=control.metricsJson("view_age");
            assertThat(server.stopPublishingObservations()).isEqualTo(1);
            Await.until("the retained observation to age beyond the read freshness window",Duration.ofSeconds(90),()->control.observedAgeMillis("view_age").orElse(0L)>=65000,()->control.viewsJson());
            String after=control.viewsJson();
            var receipt=new LinkedHashMap<String,Object>();receipt.put("sourceChangeRequestedAt",changedAt.toString());receipt.put("targetConfirmedAt",targetConfirmedAt.toString());
            receipt.put("actualTargetDocument",landed.find(new Document("name","after")).first().toJson());receipt.put("sourcePositionPresent",true);
            receipt.put("before",JsonReader.parse(before));receipt.put("after",JsonReader.parse(after));receipt.put("metrics",JsonReader.parse(metrics));
            Path evidence=Path.of(System.getProperty("tapstate.t8.view-receipt","/tmp/w4-t8-view-receipt.json"));Files.writeString(evidence,JsonWriter.write(receipt));
            assertThat(after).as("the actual View API after the publisher stopped: %s",after).contains("\"state\":\"stale\"");
        }finally{
            try(var cleanup=MongoClients.create(sourceUri)){cleanup.getDatabase(sourceDb).drop();cleanup.getDatabase(targetDb).drop();}
        }
    }
}
