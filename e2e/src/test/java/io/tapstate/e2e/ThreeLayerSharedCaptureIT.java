package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/** Two real Mongo consumers retain one native capture while one explicitly replaces its own state. */
@RequiresDocker
class ThreeLayerSharedCaptureIT {
    @Container private static final MongoDBContainer CONFIG = mongo();
    @Container private static final MongoDBContainer CACHE = mongo();
    @Container private static final MongoDBContainer DATA = mongo()
            .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--setParameter", "enableTestCommands=1");
    private static final Duration BOUND = Duration.ofSeconds(90);
    @TempDir Path work;

    @BeforeAll static void requireConnector() { RealConnectorGate.require("mongodb"); }

    @Test @Timeout(180)
    void aRealMongoColumnUsesTheNewModelOnlyAfterAnExplicitClearStateFullLoad() throws Exception {
        Duration columnBound=Duration.ofSeconds(60);String suffix=UUID.randomUUID().toString().replace("-","");
        String cluster="column-"+suffix,cacheDb="column_cache_"+suffix,configUri=CONFIG.getReplicaSetUrl("column_cfg_"+suffix);
        String sourceUri=DATA.getReplicaSetUrl("column_source_"+suffix),targetUri=DATA.getReplicaSetUrl("column_target_"+suffix);
        NativeMongoStorageFixture.prepare(configUri,new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                CACHE.getReplicaSetUrl(cacheDb),cacheDb,null),targetUri,cluster);
        var source=EndpointAddress.uri(sourceUri);var target=EndpointAddress.uri(targetUri);String pipeline="column_reader";
        try(var mongo=new MongoEndpoints();var cacheClient=MongoClients.create(CACHE.getReplicaSetUrl());
                var configClient=MongoClients.create(CONFIG.getReplicaSetUrl());var server=lossServer(configUri,cluster)) {
            mongo.seed(source,"orders",SeedRows.generated(8));var cache=cacheClient.getDatabase(cacheDb);
            var generations=configClient.getDatabase("column_cfg_"+suffix).getCollection("operator_generations");
            var control=new ControlPlane(server.baseUrl());control.login(NativeMongoStorageFixture.USER,NativeMongoStorageFixture.PASSWORD);
            control.registerConnector("mongodb",ConnectorJars.bytesFor("mongodb"));
            control.apply(Map.of("views.tap.yml",control.artifact("views").orElseThrow().canonicalForm(),
                    "input.tap.yml",source("input",sourceUri,"orders"),"column_reader.tap.yml",pipeline(pipeline,List.of("input"),"views",List.of("orders"))));
            control.discoverSchema("input","mongodb",Map.of("isUri",true,"uri",sourceUri));
            control.lifecycle(pipeline,LifecycleVerb.START);awaitRows(mongo,source,target,columnBound);
            String initialAck=Await.answered("the original Mongo snapshot acknowledgement",columnBound,() -> control.durablePosition(pipeline,"orders"));
            mongo.update(source,"orders",Map.of("id",2L),Map.of("seq",202L));awaitRows(mongo,source,target,columnBound);
            Await.answered("the original Mongo CDC acknowledgement",columnBound,() -> control.durablePosition(pipeline,"orders").filter(value -> !value.equals(initialAck)));
            assertThat(pinnedColumns(cache,pipeline,pipeline+"_step",1)).contains("id","seq").doesNotContain("note");
            assertThat(mongo.documents(target,"orders")).allSatisfy(row -> assertThat(row).doesNotContainKey("note"));
            String chain=membership(cache,pipeline,"input").get("_id",Document.class).getString("chain");
            var oldModels=cache.getCollection("srs_models").find(new Document("_id.chain",chain)).into(new ArrayList<>());
            assertThat(oldModels).isNotEmpty();
            control.stop(pipeline,false);Await.until("the old Mongo reader to stop",columnBound,
                    () -> control.state(pipeline).filter(PipelineState.STOPPED::equals).isPresent(),() -> control.failureCode(pipeline).toString());
            for(long id=1;id<=8;id++)mongo.update(source,"orders",Map.of("id",id),Map.of("note","new-full-"+id));
            control.discoverSchema("input","mongodb",Map.of("isUri",true,"uri",sourceUri));
            long began=System.nanoTime();control.stop(pipeline,true);
            Await.until("the explicit new Mongo state generation",columnBound,() -> {
                var row=generations.find(new Document("_id",pipeline)).first();return row!=null && Long.valueOf(2).equals(row.get("stateGeneration"))
                        && "ACTIVE".equals(row.getString("phase")) && control.state(pipeline).filter(PipelineState.STOPPED::equals).isPresent();
            },() -> String.valueOf(generations.find(new Document("_id",pipeline)).first()));
            control.lifecycle(pipeline,LifecycleVerb.START);
            Await.until("the new Mongo model and all full values",columnBound,
                    () -> columnRows(mongo,source).equals(columnRows(mongo,target)) && pinnedColumns(cache,pipeline,pipeline+"_step",2).contains("note"),
                    () -> "target="+columnRows(mongo,target)+", failure="+control.failureCode(pipeline));
            var fullDuration=Duration.ofNanos(System.nanoTime()-began);assertThat(fullDuration).isLessThan(columnBound);
            System.out.println("FORMAL_MONGO_COLUMN_FULL elapsedMs="+fullDuration.toMillis());
            Await.until("the new full's exact snapshot count",columnBound,() -> Long.valueOf(8).equals(control.snapshotRowsRead(pipeline).get("orders")),
                    () -> control.snapshotRowsRead(pipeline).toString());
            var newOffset=offset(cache,chain,pipeline,"input");assertThat(newOffset).containsEntry("stateGeneration",2L);
            var snapshot=newOffset.get("snapshot",Document.class);assertThat(snapshot).isNotNull();
            var modelKey=snapshot.getList("models",Document.class).stream().filter(value -> "orders".equals(value.getString("table")))
                    .findFirst().orElseThrow().get("model",Document.class);
            var model=cache.getCollection("srs_models").find(new Document("_id",modelKey)).first();assertThat(model).isNotNull();
            assertThat(model.get("schema",Document.class).getList("fields",Document.class)).extracting(field -> field.getString("name")).contains("id","seq","note");
            assertThat(modelKey.get("captureContext",Document.class).getLong("epoch")).isEqualTo(snapshot.getLong("snapshotEpoch"));
            assertThat(newOffset.get("progress",Document.class).getLong("snapshotEpoch")).isEqualTo(snapshot.getLong("snapshotEpoch"));
            String fullAck=Await.answered("the new full's durable acknowledgement",columnBound,() -> control.durablePosition(pipeline,"orders"));
            mongo.update(source,"orders",Map.of("id",3L),Map.of("seq",303L,"note","new-cdc"));
            Await.until("real CDC after the new Mongo full",columnBound,() -> columnRows(mongo,source).equals(columnRows(mongo,target)),
                    () -> "target="+columnRows(mongo,target));
            Await.answered("the new-model CDC acknowledgement",columnBound,() -> control.durablePosition(pipeline,"orders").filter(value -> !value.equals(fullAck)));
            Await.until("the real checkpoint to consume the pending model handoff",columnBound,() -> {
                var current=cache.getCollection("srs_chains").find(new Document("_id",chain)).first();
                return current!=null && current.get("resumeOverride")==null && current.get("checkpoint") instanceof Document checkpoint
                        && Boolean.TRUE.equals(checkpoint.get("positionEditsRestricted"));
            },() -> "model handoff checkpoint is not yet committed");
            var root=cache.getCollection("srs_chains").find(new Document("_id",chain)).first();
            assertThat(root.get("resumeOverride")).isNull();
            var checkpoint=root.get("checkpoint",Document.class);assertThat(checkpoint).containsEntry("positionEditsRestricted",true);
            assertThat(checkpoint.getLong("epoch")).isGreaterThanOrEqualTo(snapshot.getLong("snapshotEpoch"));
            var tableCheckpoint=checkpoint.getList("tables",Document.class).stream().filter(value -> "orders".equals(value.getString("table"))).findFirst().orElseThrow();
            assertThat(tableCheckpoint.get("model")).isEqualTo(modelKey);
            assertThat(tableCheckpoint.getLong("sequence")).isGreaterThanOrEqualTo(model.getLong("ddlSequence"));
            assertThat(root).containsEntry("consumerCount",1L);
            for(var old:oldModels) {
                var retained=cache.getCollection("srs_models").find(new Document("_id",old.get("_id"))).first();
                if(retained!=null) {assertThat(retained).isEqualTo(old);continue;}
                var contextId=new Document(old.get("_id",Document.class));contextId.remove("modelVersion");
                var context=cache.getCollection("srs_model_contexts").find(new Document("_id",contextId)).first();assertThat(context).isNotNull();
                var nextId=new Document(modelKey);nextId.remove("modelVersion");
                assertThat(context).containsEntry("nextContext",nextId).containsEntry("nextSequence",model.getLong("ddlSequence"));
                var bounds=cache.getCollection("srs_log_bounds").find(new Document("_id","srs-log-bounds:"+old.getString("ringName"))).first();assertThat(bounds).isNotNull();
                assertThat(bounds.getLong("trimmedThrough")).isGreaterThanOrEqualTo(context.getLong("nextSequence")-1);
            }
            assertThat(membership(cache,pipeline,"input")).containsEntry("stateGeneration",2L);
        }
    }

    private static List<String> pinnedColumns(MongoDatabase cache,String pipeline,String step,long generation) {
        var row=cache.getCollection("derived_schemas").find(new Document("_id",pipeline+'.'+step).append("scope.stateGeneration",generation)).first();
        if(row==null || !(row.get("pin") instanceof Number pin))return List.of();
        return row.getList("versions",Document.class).get(pin.intValue()).getList("columns",Document.class).stream().map(field -> field.getString("name")).toList();
    }
    private static Map<Long,List<Object>> columnRows(MongoEndpoints mongo,EndpointAddress endpoint) {
        var result=new TreeMap<Long,List<Object>>();
        for(var row:mongo.documents(endpoint,"orders"))assertThat(result.put(((Number)row.get("id")).longValue(),
                java.util.Arrays.asList(((Number)row.get("seq")).longValue(),row.get("note")))).isNull();
        return result;
    }

    @Test @Timeout(300)
    void aLostSharedModelRecoversOnlyItsActualReadersWhileAnUnrelatedCaptureKeepsItsValues() throws Exception {
        String suffix=UUID.randomUUID().toString().replace("-","");String cluster="shared-loss-"+suffix;
        String configDb="loss_cfg_"+suffix,cacheDb="loss_cache_"+suffix,sourceDb="loss_source_"+suffix;
        String secondDb="loss_second_"+suffix,configUri=CONFIG.getReplicaSetUrl(configDb);
        String sourceUri=DATA.getReplicaSetUrl(sourceDb),secondUri=DATA.getReplicaSetUrl(secondDb);
        String targetAUri=DATA.getReplicaSetUrl("loss_a_"+suffix),targetBUri=DATA.getReplicaSetUrl("loss_b_"+suffix),targetCUri=DATA.getReplicaSetUrl("loss_c_"+suffix);
        NativeMongoStorageFixture.prepare(configUri,new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                CACHE.getReplicaSetUrl(cacheDb),cacheDb,null),targetAUri,cluster);
        var source=EndpointAddress.uri(sourceUri);var second=EndpointAddress.uri(secondUri);
        var a=EndpointAddress.uri(targetAUri);var b=EndpointAddress.uri(targetBUri);var c=EndpointAddress.uri(targetCUri);
        try(var mongo=new MongoEndpoints();var cacheClient=MongoClients.create(CACHE.getReplicaSetUrl());var configClient=MongoClients.create(CONFIG.getReplicaSetUrl())) {
            mongo.seed(source,"orders",SeedRows.generated(8));mongo.seed(second,"items",SeedRows.generated(5));
            var cache=cacheClient.getDatabase(cacheDb);var config=configClient.getDatabase(configDb);
            var generations=config.getCollection("operator_generations");String canonical,replacement,operation;long firstPid;
            try(var server=lossServer(configUri,cluster)) {
                firstPid=server.pid();
                var control=new ControlPlane(server.baseUrl());control.login(NativeMongoStorageFixture.USER,NativeMongoStorageFixture.PASSWORD);
                control.registerConnector("mongodb",ConnectorJars.bytesFor("mongodb"));
                var resources=new LinkedHashMap<String,String>();resources.put("views.tap.yml",control.artifact("views").orElseThrow().canonicalForm());
                resources.put("input.tap.yml",source("input",sourceUri,"orders"));resources.put("second.tap.yml",source("second",secondUri,"items"));
                resources.put("target_b.tap.yml",target("target_b",targetBUri));resources.put("target_c.tap.yml",target("target_c",targetCUri));
                resources.put("reader_a.tap.yml",pipeline("reader_a",List.of("input","second"),"views",List.of("orders","items")));
                resources.put("reader_b.tap.yml",pipeline("reader_b",List.of("input"),"target_b",List.of("orders")));
                resources.put("reader_c.tap.yml",pipeline("reader_c",List.of("second"),"target_c",List.of("items")));
                control.apply(resources);
                control.discoverSchema("input","mongodb",Map.of("isUri",true,"uri",sourceUri));
                control.discoverSchema("second","mongodb",Map.of("isUri",true,"uri",secondUri));
                control.lifecycle("reader_c",LifecycleVerb.START);awaitRows(mongo,second,c,"items",BOUND);
                control.lifecycle("reader_a",LifecycleVerb.START);awaitRows(mongo,source,a,BOUND);awaitRows(mongo,second,a,"items",BOUND);
                control.lifecycle("reader_b",LifecycleVerb.START);awaitRows(mongo,source,b,BOUND);
                var first=membership(cache,"reader_b","input");canonical=first.get("_id",Document.class).getString("chain");
                String kept=membership(cache,"reader_c","second").get("_id",Document.class).getString("chain");
                assertThat(kept).isNotEqualTo(canonical);
                var keptRoot=cache.getCollection("srs_chains").find(new Document("_id",kept)).first();
                var keptModels=cache.getCollection("srs_models").find(new Document("_id.chain",kept)).sort(new Document("_id",1)).into(new ArrayList<>());
                long cExecution=Await.answered("C's actual original execution",BOUND,() -> control.executionGenerationOf("reader_c"));
                String cAck=Await.answered("C's original target confirmation",BOUND,() -> control.durablePosition("reader_c","items"));
                var cMember=membership(cache,"reader_c","second");var cOffset=offset(cache,kept,"reader_c","second");
                long bExecution=execution(cache,"reader_b");var bRows=rows(mongo,b);
                for(String id:List.of("reader_a","reader_b")) {
                    control.stop(id,false);Await.until(id+" to stop before shared recovery",BOUND,
                            () -> control.state(id).filter(PipelineState.STOPPED::equals).isPresent(),() -> control.failureCode(id).toString());
                }
                var lost=cache.getCollection("srs_models").find(new Document("_id.chain",canonical)).into(new ArrayList<>());
                assertThat(lost).isNotEmpty();for(var row:lost)assertThat(cache.getCollection("srs_models").deleteOne(new Document("_id",row.get("_id"))).getDeletedCount()).isOne();
                Map<?,?> preview;
                try {preview=storage(control,"plan",Map.of("scope","SHARED_SRS","pipelineIds",List.of(),"canonicalChain",canonical,"physicalChain",canonical));}
                catch(RuntimeException|Error failed) {
                    captureDiagnostic(config,cache,canonical);
                    FailureScene.writeMemberLogs(cluster,Map.of("runtime",server.output()));FailureScene.writeThreads(cluster,server);throw failed;
                }
                assertThat(preview.get("affectedPipelineIds")).isEqualTo(List.of("reader_a","reader_b"));
                assertThat(preview.get("preflightCurrent")).isEqualTo(true);
                for(String id:List.of("reader_a","reader_b","reader_c"))assertThat(generations.find(new Document("_id",id)).first()).containsEntry("stateGeneration",1L);
                mongo.update(second,"items",Map.of("id",2L),Map.of("seq",222L));awaitRows(mongo,second,c,"items",BOUND);
                Await.answered("C's original target confirmation to advance during the shared preflight",BOUND,
                        () -> control.durablePosition("reader_c","items").filter(value -> !value.equals(cAck)));
                storage(control,"confirm",Map.of("operationId",preview.get("operationId"),"planHash",preview.get("planHash"),
                        "controlRevision",preview.get("controlRevision"),"confirmed",true));
                Await.until("the original shared recovery to publish its exact replacement",BOUND,() -> {
                    var status=storage(control,"status",Map.of("operationId",preview.get("operationId")));
                    if(status.get("executionErrorCode")!=null)throw new AssertionError("shared recovery failed: "+status);
                    return "COMPLETE".equals(status.get("phase"));
                },() -> String.valueOf(storage(control,"status",Map.of("operationId",preview.get("operationId")))));
                operation=(String)preview.get("operationId");
                var bindings=config.getCollection("capture_preparations").find(new Document("currentRecovery.operationId",operation)).into(new ArrayList<>());
                assertThat(bindings).hasSize(1);var current=bindings.getFirst().get("currentRecovery",Document.class);
                assertThat(current).containsEntry("phase","READY").containsEntry("operationId",operation);
                replacement=current.getString("physicalChain");assertThat(replacement).isNotBlank().isNotEqualTo(canonical);
                assertThat(cache.getCollection("srs_chains").find(new Document("_id",canonical)).first().get("recoveryControl",Document.class))
                        .containsEntry("phase","RETIRED").containsEntry("operationId",operation).containsEntry("replacementChain",replacement);
                for(String id:List.of("reader_a","reader_b")) {
                    assertThat(control.state(id)).contains(PipelineState.STOPPED);
                    assertThat(generations.find(new Document("_id",id)).first()).containsEntry("stateGeneration",1L);
                    var refusal=control.startExpectingRefusal(id);assertThat(refusal.status()).isEqualTo(409);
                    assertThat(refusal.code()).isEqualTo(io.tapstate.spi.store.IoError.PIPELINE_STATE_REBUILD_REQUIRED.code());
                }
                assertThat(execution(cache,"reader_b")).isEqualTo(bExecution);
                assertThat(control.state("reader_c")).contains(PipelineState.RUNNING);assertThat(control.executionGenerationOf("reader_c")).contains(cExecution);
                for(String field:List.of("captureId","captureFence","epoch"))assertThat(cache.getCollection("srs_chains").find(new Document("_id",kept)).first().get(field)).isEqualTo(keptRoot.get(field));
                assertThat(cache.getCollection("srs_models").find(new Document("_id.chain",kept)).sort(new Document("_id",1)).into(new ArrayList<>())).isEqualTo(keptModels);
                for(String id:List.of("reader_a","reader_b")) {
                    control.stop(id,true);
                    Await.until(id+" to finish its separately confirmed clear",BOUND,() -> {
                        var mapping=generations.find(new Document("_id",id)).first();return mapping!=null && Long.valueOf(2).equals(mapping.get("stateGeneration"))
                                && "ACTIVE".equals(mapping.getString("phase")) && control.state(id).filter(PipelineState.STOPPED::equals).isPresent();
                    },() -> String.valueOf(generations.find(new Document("_id",id)).first()));
                    control.lifecycle(id,LifecycleVerb.START);awaitRows(mongo,source,id.equals("reader_a")?a:b,BOUND);
                    if(id.equals("reader_a")) {
                        awaitRows(mongo,second,a,"items",BOUND);assertThat(control.state("reader_b")).contains(PipelineState.STOPPED);
                        mongo.update(source,"orders",Map.of("id",2L),Map.of("seq",202L));awaitRows(mongo,source,a,BOUND);
                        mongo.update(second,"items",Map.of("id",3L),Map.of("seq",333L));awaitRows(mongo,second,c,"items",BOUND);
                        assertThat(rows(mongo,b)).isEqualTo(bRows);assertThat(execution(cache,"reader_b")).isEqualTo(bExecution);
                    }
                    kept(cache,kept,keptRoot,keptModels,cMember,cOffset);
                    assertThat(membership(cache,id,"input")).containsEntry("stateGeneration",2L);
                    assertThat(membership(cache,id,"input").get("_id",Document.class)).containsEntry("chain",replacement);
                }
                mongo.update(source,"orders",Map.of("id",4L),Map.of("seq",444L));awaitRows(mongo,source,a,BOUND);awaitRows(mongo,source,b,BOUND);
                awaitRows(mongo,second,a,"items",BOUND);awaitRows(mongo,second,c,"items",BOUND);
                assertThat(generations.find(new Document("_id","reader_c")).first()).containsEntry("stateGeneration",1L);
                assertThat(control.executionGenerationOf("reader_c")).contains(cExecution);
                assertThat(membership(cache,"reader_a","input").get("_id",Document.class)).containsEntry("chain",replacement);
                assertThat(membership(cache,"reader_b","input").get("_id",Document.class)).containsEntry("chain",replacement);
                assertThat(membership(cache,"reader_a","second").get("_id",Document.class).getString("chain")).isEqualTo(kept);
            }
            try(var reopened=lossServer(configUri,cluster)) {
                assertThat(reopened.pid()).isNotEqualTo(firstPid);
                var control=new ControlPlane(reopened.baseUrl());control.login(NativeMongoStorageFixture.USER,NativeMongoStorageFixture.PASSWORD);
                for(String id:List.of("reader_a","reader_b","reader_c"))Await.until(id+" to resume its original new scope after process replacement",BOUND,
                        () -> control.state(id).filter(PipelineState.RUNNING::equals).isPresent(),() -> control.failureCode(id).toString());
                mongo.update(source,"orders",Map.of("id",5L),Map.of("seq",555L));mongo.update(second,"items",Map.of("id",4L),Map.of("seq",444L));
                awaitRows(mongo,source,a,BOUND);awaitRows(mongo,source,b,BOUND);awaitRows(mongo,second,a,"items",BOUND);awaitRows(mongo,second,c,"items",BOUND);
                assertThat(generations.find(new Document("_id","reader_a")).first()).containsEntry("stateGeneration",2L);
                assertThat(generations.find(new Document("_id","reader_b")).first()).containsEntry("stateGeneration",2L);
                assertThat(generations.find(new Document("_id","reader_c")).first()).containsEntry("stateGeneration",1L);
                for(String id:List.of("reader_a","reader_b"))assertThat(membership(cache,id,"input").get("_id",Document.class)).containsEntry("chain",replacement);
                assertThat(config.getCollection("capture_preparations").find(new Document("currentRecovery.operationId",operation)).first()
                        .get("currentRecovery",Document.class)).containsEntry("physicalChain",replacement).containsEntry("operationId",operation);
            }
        }
    }

    private RealProcessServer lossServer(String configurationUri,String cluster) {
        return RealProcessServer.startPrepared(configurationUri,"127.0.0.1",port -> List.of("--tapstate.cluster.id="+cluster,
                "--tapstate.cluster.node-id=shared-loss-node","--tapstate.hz.member-port=0","--tapstate.hz.jet.cooperative-thread-count=2",
                "--SDK_STATUS_SENDER_ENABLED=false","--logging.level.io.tapstate.app.HazelcastOperatorReadiness=DEBUG",
                "--tapstate.connectors.seed-dir="+work.resolve("no-loss-seeds")));
    }
    private static Map<?,?> storage(ControlPlane control,String action,Map<String,?> request) {
        return (Map<?,?>)io.tapstate.core.common.JsonReader.parse(control.sourceRequest("POST","/api/storage/recovery/"+action,request,null,200).body());
    }
    private static void captureDiagnostic(MongoDatabase config,MongoDatabase cache,String canonical) {
        var root=config.getCollection("storage_recovery").find(new Document("_id","cluster")).first();
        var pending=root==null?null:root.get("pendingRecovery",Document.class);
        var snapshot=pending==null?null:pending.get("sharedSrs",Document.class);
        var expected=snapshot==null?null:snapshot.get("captureFence",Document.class);
        String capture=io.tapstate.core.model.canonical.SourceCaptureIdentity.sharedCapture(canonical);
        var projection=new Document("_id",0).append("leaseUntil",1).append("observedLeased",1);
        var fixed=new Document();
        for(String field:List.of("clusterId","resourceType","resourceId","ownerNodeId","ownerBootId",
                "claimGeneration","executionGeneration","topologyRevision")) {
            projection.append(field,1);if(expected!=null)fixed.append(field,expected.get(field));
        }
        var actual=cache.getCollection("workload_claims").aggregate(List.of(
                new Document("$match",new Document("resourceType","CAPTURE").append("resourceId",capture)),
                new Document("$set",new Document("observedLeased",new Document("$gt",List.of("$leaseUntil","$$NOW")))),
                new Document("$project",projection))).first();
        System.err.println("SHARED_RECOVERY_CAPTURE_DIAGNOSTIC "+new Document("expectedFence",fixed).append("actualClaim",actual).toJson());
    }
    private static Document membership(MongoDatabase cache,String pipeline,String source) {
        var members=cache.getCollection("srs_consumers").find(new Document("pipelineId",pipeline).append("sourceId",source).append("detached",false)).into(new ArrayList<>());
        assertThat(members).hasSize(1);return members.getFirst();
    }
    private static Document offset(MongoDatabase cache,String chain,String pipeline,String source) {
        var row=cache.getCollection("srs_consumer_offsets").find(new Document("_id.chain",chain)
                .append("_id.consumer",io.tapstate.spi.store.SrsConsumerId.of(pipeline,source).value())).first();
        assertThat(row).isNotNull();return row;
    }
    private static long execution(MongoDatabase cache,String pipeline) {
        var row=cache.getCollection("workload_claims").find(new Document("resourceType","PIPELINE_ACTUATION").append("resourceId",pipeline)).first();
        assertThat(row).isNotNull();return row.getLong("executionGeneration");
    }
    private static void kept(MongoDatabase cache,String chain,Document root,List<Document> models,Document member,Document cursor) {
        var actualRoot=cache.getCollection("srs_chains").find(new Document("_id",chain)).first();assertThat(actualRoot).isNotNull();
        for(String field:List.of("captureId","captureFence","epoch"))assertThat(actualRoot.get(field)).as("unaffected capture %s",field).isEqualTo(root.get(field));
        assertThat(actualRoot.getLong("checkpointRevision")).isGreaterThanOrEqualTo(root.getLong("checkpointRevision"));
        assertThat(cache.getCollection("srs_models").find(new Document("_id.chain",chain)).sort(new Document("_id",1)).into(new ArrayList<>())).isEqualTo(models);
        var actual=membership(cache,"reader_c","second");
        for(String field:List.of("_id","pipelineId","sourceId","stateGeneration","commandFence"))assertThat(actual.get(field)).as("unaffected C %s",field).isEqualTo(member.get(field));
        var next=offset(cache,chain,"reader_c","second");assertThat(next.get("snapshot")).isEqualTo(cursor.get("snapshot"));
        assertThat(next.getLong("progressRevision")).isGreaterThanOrEqualTo(cursor.getLong("progressRevision"));
        var before=cursor.get("progress",Document.class);var after=next.get("progress",Document.class);
        for(String field:List.of("snapshotEpoch","cdcStartPosition"))assertThat(after.get(field)).isEqualTo(before.get(field));
        assertThat(after.getList("snapshotCompletedTables",String.class)).containsAll(before.getList("snapshotCompletedTables",String.class));
        for(var old:before.getList("perTableSeq",Document.class)) {
            var now=after.getList("perTableSeq",Document.class).stream().filter(value -> value.getString("table").equals(old.getString("table"))).findFirst().orElseThrow();
            assertThat(now.getLong("sequence")).isGreaterThanOrEqualTo(old.getLong("sequence"));
        }
        if(before.get("sinkAcked") instanceof Document ack) {
            var nextAck=after.get("sinkAcked",Document.class);assertThat(nextAck).isNotNull();
            assertThat(new io.tapstate.core.event.SourceOrder(nextAck.getLong("epoch"),nextAck.getLong("sequence")))
                    .isGreaterThanOrEqualTo(new io.tapstate.core.event.SourceOrder(ack.getLong("epoch"),ack.getLong("sequence")));
        }
    }
    private static String source(String id,String uri,String table) {
        return "version: tapstate/v1\nkind: source\nid: "+id+"\nconnector: mongodb\nconfig: { isUri: true, uri: \""+uri+"\" }\nmode: cdc\ntables: [ "+table+" ]\n";
    }
    private static String target(String id,String uri) {
        return "version: tapstate/v1\nkind: source\nid: "+id+"\nconnector: mongodb\nconfig: { isUri: true, uri: \""+uri+"\" }\n";
    }
    private static String pipeline(String id,List<String> sources,String target,List<String> tables) {
        String references=sources.stream().map(source -> "{ id: "+source+", srs: true }").collect(java.util.stream.Collectors.joining(", "));
        return "version: tapstate/v1\nkind: pipeline\nid: "+id+"\nsource: [ "+references+" ]\nsettings: { read_mode: snapshot_and_cdc }\n"
                +"transforms:\n  - { id: "+id+"_step, from: [ "+String.join(", ",tables)+" ], type: filter, expr: \"op != 'x'\" }\n"
                +"serve:\n  from: "+id+"_step\n  sync:\n    - source: "+target+"\n      on_full_load: clear\n";
    }

    @Test @Timeout(300)
    void oneExplicitFullKeepsTheOtherConsumersLiveCdcModelsAndOriginalGeneration() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "shared-" + suffix, cacheDb = "shared_cache_" + suffix, sourceDb = "shared_source_" + suffix;
        String configDb = "shared_cfg_" + suffix, configUri = CONFIG.getReplicaSetUrl(configDb);
        String sourceUri = DATA.getReplicaSetUrl(sourceDb), targetAUri = DATA.getReplicaSetUrl("shared_a_" + suffix),
                targetBUri = DATA.getReplicaSetUrl("shared_b_" + suffix), appName = "shared-source-" + suffix;
        String taggedSource = sourceUri + (sourceUri.contains("?") ? "&" : "?") + "appName=" + appName;
        NativeMongoStorageFixture.prepare(configUri, new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                CACHE.getReplicaSetUrl(cacheDb), cacheDb, null), targetAUri, cluster);
        var source = EndpointAddress.uri(sourceUri); var targetA = EndpointAddress.uri(targetAUri); var targetB = EndpointAddress.uri(targetBUri);
        try (var mongo = new MongoEndpoints(); var dataClient = MongoClients.create(DATA.getReplicaSetUrl());
             var cacheClient = MongoClients.create(CACHE.getReplicaSetUrl()); var configClient = MongoClients.create(CONFIG.getReplicaSetUrl())) {
            mongo.seed(source, "orders", SeedRows.generated(8));
            try (var server = RealProcessServer.startPrepared(configUri, "127.0.0.1", port -> List.of(
                    "--tapstate.cluster.id=" + cluster, "--tapstate.cluster.node-id=shared-node", "--tapstate.hz.member-port=0",
                    "--tapstate.hz.jet.cooperative-thread-count=2", "--SDK_STATUS_SENDER_ENABLED=false",
                    "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds")))) {
                var control = new ControlPlane(server.baseUrl()); control.login(NativeMongoStorageFixture.USER, NativeMongoStorageFixture.PASSWORD);
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                var resources = new LinkedHashMap<String, String>();
                resources.put("views.tap.yml", control.artifact("views").orElseThrow().canonicalForm());
                resources.put("input.tap.yml", """
                        version: tapstate/v1
                        kind: source
                        id: input
                        connector: mongodb
                        config: { isUri: true, uri: "%s" }
                        mode: cdc
                        tables: [ orders ]
                        """.formatted(taggedSource));
                resources.put("target_b.tap.yml", """
                        version: tapstate/v1
                        kind: source
                        id: target_b
                        connector: mongodb
                        config: { isUri: true, uri: "%s" }
                        """.formatted(targetBUri));
                resources.put("reader_a.tap.yml", pipeline("reader_a", "views"));
                resources.put("reader_b.tap.yml", pipeline("reader_b", "target_b"));
                control.apply(resources); control.discoverSchema("input", "mongodb", Map.of("isUri", true, "uri", taggedSource));
                control.lifecycle("reader_a", LifecycleVerb.START); awaitRows(mongo, source, targetA, BOUND);
                control.lifecycle("reader_b", LifecycleVerb.START); awaitRows(mongo, source, targetB, BOUND);
                String beforeAck = Await.answered("the second consumer's initial target confirmation", BOUND,
                        () -> control.durablePosition("reader_b", "orders"));
                long beforeExecution = Await.answered("the second consumer's original execution", BOUND,
                        () -> control.executionGenerationOf("reader_b"));
                var cache = cacheClient.getDatabase(cacheDb); var generations = configClient.getDatabase(configDb).getCollection("operator_generations");
                var chains = cache.getCollection("srs_chains"); assertThat(chains.countDocuments()).isOne();
                var originalChain = chains.find().first();
                assertConsumer(cache, "reader_b", 1, originalChain.get("_id"));
                var models = cache.getCollection("srs_models").find().sort(new Document("_id", 1)).into(new ArrayList<>());
                assertThat(models).isNotEmpty();
                control.stop("reader_a", true);
                // Runtime reads remain fenced while CONFIG is publishing the replacement scope.
                Await.until("only the first consumer's explicit full replacement to complete", BOUND,
                        () -> {
                            var mapping = generations.find(new Document("_id", "reader_a")).first();
                            return mapping != null && Long.valueOf(2).equals(mapping.get("stateGeneration"))
                                    && "ACTIVE".equals(mapping.getString("phase"))
                                    && control.state("reader_a").filter(PipelineState.STOPPED::equals).isPresent();
                        }, () -> "mapping=" + generations.find(new Document("_id", "reader_a")).first());
                MongoDatabase admin = dataClient.getDatabase("admin");
                long entered = ((Number) admin.runCommand(new Document("configureFailPoint", "failCommand")
                        .append("mode", new Document("times", 1)).append("data", new Document("failCommands", List.of("find"))
                                .append("namespace", sourceDb + ".orders").append("appName", appName)
                                .append("blockConnection", true).append("blockTimeMS", 15000))).get("count")).longValue();
                var fullCount = new Document("_id.scope.pipelineId", "reader_a").append("_id.scope.stateGeneration", 2L);
                try (var starter = Executors.newSingleThreadExecutor()) {
                    var start = starter.submit(() -> control.lifecycle("reader_a", LifecycleVerb.START));
                    try {
                        admin.runCommand(new Document("waitForFailPoint", "failCommand").append("timesEntered", entered + 1).append("maxTimeMS", 15000));
                        Object held = Await.answered("the first consumer's real source read to be held", Duration.ofSeconds(5),
                                () -> heldRead(admin, appName, sourceDb)).get("opid");
                        assertThat(cache.getCollection("snapshot_counts").countDocuments(fullCount)).isZero();
                        mongo.update(source, "orders", Map.of("id", 2L), Map.of("seq", 222L));
                        awaitRows(mongo, source, targetB, Duration.ofSeconds(8));
                        Await.answered("the unaffected consumer's target confirmation during the held full start", Duration.ofSeconds(5),
                                () -> control.durablePosition("reader_b", "orders").filter(value -> !value.equals(beforeAck)));
                        assertThat(heldRead(admin, appName, sourceDb).map(op -> op.get("opid"))).contains(held);
                        assertThat(cache.getCollection("snapshot_counts").countDocuments(fullCount)).isZero();
                        assertConsumer(cache, "reader_b", 1, originalChain.get("_id"));
                        assertThat(control.state("reader_b")).contains(PipelineState.RUNNING);
                    } finally { admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
                    start.get(60, TimeUnit.SECONDS);
                }
                awaitRows(mongo, source, targetA, BOUND);
                Await.until("the replacement generation's actual full read to finish", BOUND,
                        () -> cache.getCollection("snapshot_counts").countDocuments(new Document(fullCount).append("rows", 8L)) == 1,
                        () -> "snapshot counts=" + cache.getCollection("snapshot_counts").find(fullCount).into(new ArrayList<>()));
                mongo.update(source, "orders", Map.of("id", 4L), Map.of("seq", 444L));
                awaitRows(mongo, source, targetA, BOUND); awaitRows(mongo, source, targetB, BOUND);
                assertThat(generations.find(new Document("_id", "reader_b")).first()).containsEntry("stateGeneration", 1L);
                assertThat(control.executionGenerationOf("reader_b")).contains(beforeExecution);
                assertConsumer(cache, "reader_a", 2, originalChain.get("_id"));
                assertConsumer(cache, "reader_b", 1, originalChain.get("_id"));
                assertThat(chains.countDocuments()).isOne();
                assertThat(chains.find().first()).containsEntry("_id", originalChain.get("_id")).containsEntry("epoch", originalChain.get("epoch"));
                assertThat(cache.getCollection("srs_models").find().sort(new Document("_id", 1)).into(new ArrayList<>())).isEqualTo(models);
                assertThat(control.state("reader_a")).contains(PipelineState.RUNNING);
                assertThat(control.state("reader_b")).contains(PipelineState.RUNNING);
            }
        }
    }

    private static Optional<Document> heldRead(MongoDatabase admin, String appName, String database) {
        return admin.runCommand(new Document("currentOp", 1).append("$all", true)).getList("inprog", Document.class).stream()
                .filter(op -> appName.equals(op.getString("appName")))
                .filter(op -> op.get("command") instanceof Document command && "orders".equals(command.getString("find"))
                        && database.equals(command.getString("$db"))).findFirst();
    }

    private static void assertConsumer(MongoDatabase cache, String pipeline, long generation, Object chain) {
        var consumers = cache.getCollection("srs_consumers").find(new Document("pipelineId", pipeline)
                .append("stateGeneration", generation).append("detached", false)).into(new ArrayList<>());
        assertThat(consumers).hasSize(1);
        assertThat(consumers.getFirst().get("_id", Document.class)).containsEntry("chain", chain);
    }

    private static void awaitRows(MongoEndpoints mongo, EndpointAddress source, EndpointAddress target, Duration bound) {
        awaitRows(mongo,source,target,"orders",bound);
    }
    private static void awaitRows(MongoEndpoints mongo,EndpointAddress source,EndpointAddress target,String table,Duration bound) {
        Await.until("all actual target values to match the source",bound,() -> rows(mongo,source,table).equals(rows(mongo,target,table)),
                () -> "target="+rows(mongo,target,table));
    }
    private static Map<Long, Long> rows(MongoEndpoints mongo, EndpointAddress endpoint) {
        return rows(mongo,endpoint,"orders");
    }
    private static Map<Long, Long> rows(MongoEndpoints mongo, EndpointAddress endpoint,String table) {
        var result = new TreeMap<Long, Long>();
        for (var row : mongo.documents(endpoint, table))
            assertThat(result.put(((Number) row.get("id")).longValue(), ((Number) row.get("seq")).longValue())).isNull();
        return result;
    }
    private static String pipeline(String id, String target) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ { id: input, srs: true } ]
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: orders
                  sync:
                    - source: %s
                      on_full_load: clear
                """.formatted(id, target);
    }
    private static MongoDBContainer mongo() {
        return new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withMemory(768L * 1024 * 1024).withNanoCPUs(1_000_000_000L));
    }
}
