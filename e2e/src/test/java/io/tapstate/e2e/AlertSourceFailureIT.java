package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.model.SourceResource;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

/** Actual source outage and same-binding retry, with CONFIG and the managed target remaining live. */
class AlertSourceFailureIT {
    @BeforeAll static void requirements(){DockerGate.require();RealConnectorGate.require("mongodb");}
    @Test void anActualSourceReadFailureAndSameCapabilityRetryReachTheOriginalAlertCenter() throws Exception {exercise(false);}
    @Test void aNecessaryMasterWriteFailureRecoversOnlyAfterTheOriginalTargetWritesAgain() throws Exception {exercise(true);}
    private void exercise(boolean masterFailure) throws Exception {
        String suffix=UUID.randomUUID().toString().replace("-","");String sourceDb="alert_source_"+suffix,targetDb="alert_target_"+suffix;
        try(var source=NetworkedMongo.start();var master=masterFailure?NetworkedMongo.start():null) {
            String uri=source.uriForThisHost(sourceDb)+"&serverSelectionTimeoutMS=1000&socketTimeoutMS=2500";
            String targetUri=masterFailure?master.uriForThisHost(targetDb)+"&serverSelectionTimeoutMS=1000&socketTimeoutMS=2500":SharedMongo.replicaSetUrl(targetDb);
            var target=new SourceResource("views",null,"mongodb",Map.of("uri",targetUri,"database",targetDb),null,null,null,null);
            try(var sourceClient=MongoClients.create(source.uriForThisHost(sourceDb));var targetClient=MongoClients.create(targetUri);
                    var storage=PreparedNativeStore.withBorrowedMaster("alert-source-"+suffix,"e2e","e2e-password",target);
                    var server=InProcessServer.startPrepared(storage,List.of("--logging.level.io.tapstate.app.NativeBusinessContext=DEBUG","--logging.level.io.tapstate.app.BuiltinResourceSignals=DEBUG"))) {
                var collection=sourceClient.getDatabase(sourceDb).getCollection("orders");collection.insertOne(new Document("_id",1).append("name","before"));
                var control=new ControlPlane(server.baseUrl());storage.loginAndAwaitBusiness(control);control.registerConnector("mongodb",ConnectorJars.bytesFor("mongodb"));
                control.apply(Map.of("source.tap.yml","""
                        version: tapstate/v1
                        kind: source
                        id: src_alarm
                        connector: mongodb
                        config: { uri: "%s", database: "%s" }
                        mode: cdc
                        tables: [orders]
                        """.formatted(uri,sourceDb),"pipeline.tap.yml","""
                        version: tapstate/v1
                        kind: pipeline
                        id: source_alarm
                        source: src_alarm
                        settings: { read_mode: snapshot_and_cdc }
                        view: { id: source_result, from: orders, primary_key: _id }
                        """));
                control.discoverSchema("src_alarm","mongodb",Map.of("uri",uri,"database",sourceDb));
                for(var pair:Map.of("source-failure","source.connection-failed","pipeline-failure","pipeline.failed").entrySet())
                    control.alert("configuration/save",Map.of("kind","RULE","id",pair.getKey(),"etag","0","form",Map.of("name",pair.getValue(),"typeId",pair.getValue(),
                            "resourceKinds",List.of(pair.getKey().equals("source-failure")?"source":"pipeline"),"allResources",true,"resources",List.of(),"parameters",Map.of(),"enabled",true)));
                if(masterFailure)control.alert("configuration/save",Map.of("kind","RULE","id","master-failure","etag","0","form",Map.of("name","Master storage access","typeId","cluster.storage-unavailable",
                        "resourceKinds",List.of("cluster-storage"),"allResources",true,"resources",List.of(),"parameters",Map.of(),"enabled",true)));
                control.lifecycle("source_alarm",LifecycleVerb.START);var landed=targetClient.getDatabase(targetDb).getCollection("source_result");
                Await.until("the actual source snapshot to reach the managed target",Duration.ofSeconds(90),()->landed.countDocuments()==1,control::viewsJson);
                collection.updateOne(new Document("_id",1),new Document("$set",new Document("name","streaming")));
                Await.until("actual CDC before the source fault",Duration.ofSeconds(60),()->landed.countDocuments(new Document("name","streaming"))==1,control::viewsJson);
                var unavailable=masterFailure?master:source;
                String failureType=masterFailure?"cluster.storage-unavailable":"source.connection-failed";
                unavailable.freeze();Map<String,Object> during;
                if(masterFailure)collection.updateOne(new Document("_id",1),new Document("$set",new Document("name","target-unavailable")));
                try {
                    Await.until("actual Source and Pipeline failures in the authoritative alert center",Duration.ofSeconds(45),()->{
                        String center=JsonWriter.write(control.alert("center",Map.of()));return center.contains(failureType) && center.contains("pipeline.failed");
                    },()->JsonWriter.write(control.alert("center",Map.of())));
                    during=control.alert("center",Map.of());
                }finally{unavailable.thaw();}
                collection.updateOne(new Document("_id",1),new Document("$set",new Document("name","after")));
                control.stop("source_alarm",false);
                Await.until("the explicit retained-state stop",Duration.ofSeconds(30),()->control.state("source_alarm").filter(io.tapstate.core.lifecycle.PipelineState.STOPPED::equals).isPresent(),control::viewsJson);
                assertThat(JsonWriter.write(control.alert("center",Map.of()))).contains(failureType);
                control.lifecycle("source_alarm",LifecycleVerb.START);
                Await.until("the original source to resume actual CDC",Duration.ofSeconds(90),()->landed.countDocuments(new Document("name","after"))==1,control::viewsJson);
                collection.insertOne(new Document("_id",2).append("name","new-after-retry"));
                Await.until("a new event on the recovered original stream",Duration.ofSeconds(60),()->landed.countDocuments(new Document("_id",2))==1,control::viewsJson);
                Await.until("Source original-capability recovery in the alert center",Duration.ofSeconds(30),()->!
                        JsonWriter.write(control.alert("center",Map.of())).contains(failureType),()->JsonWriter.write(control.alert("center",Map.of())));
                var history=control.alert("center",Map.of("view","history"));assertThat(history.toString()).contains(failureType,"RECOVERED");
                var evidence=Map.of("during",during,"after",control.alert("center",Map.of()),"history",history,"targetCount",landed.countDocuments(),"sameSourceUri",true,"storageRole",masterFailure?"MASTER":"BUSINESS_SOURCE");
                Files.writeString(Path.of(System.getProperty("tapstate.t8.source-receipt",masterFailure?"/tmp/w4-t8-master-receipt.json":"/tmp/w4-t8-source-receipt.json")),JsonWriter.write(evidence));
            }finally{try(var cleanup=MongoClients.create(targetUri)){cleanup.getDatabase(targetDb).drop();}}
        }
    }
}
