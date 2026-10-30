package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.pdk.ConnectorArtifactRegistrar;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TokenService;
import io.tapstate.core.model.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;

/** Authenticated native HTTP through the real registered connector, independent metadata and CONFIG store. */
@RequiresDocker
class NativeValidationApiIT {
    @Container static final MongoDBContainer MONGO=NativeValidationOwnerReservationsIT.MONGO;
    @TempDir Path work;
    @Test @Timeout(120) void fullMethodsAndObservedDifferencesUseTheActiveChildAndRealRegisteredPdk()throws Exception{
        var storage=NativeAppStorage.createOnPremWithAdmin(MONGO,"validation_http","admin","validation-password");
        try(var schema=io.tapstate.adapters.mongostore.MongoConfigurationDomain.prepareValidationSchema(
                new io.tapstate.adapters.mongostore.MongoConnectionSettings(storage.configurationUri(),null,Duration.ofSeconds(5)),storage.clusterId(),UUID.randomUUID());
                var cache=io.tapstate.adapters.mongostore.MongoCacheDomain.connect(new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,storage.cacheUri(),storage.cacheDatabase(),null),Duration.ofSeconds(5),"validation-http-fixture")){
            schema.prepareCache(cache);while(!schema.advance(20).ready()){}
        }
        var sourceDb="http_source_"+UUID.randomUUID().toString().replace("-","");var targetDb="http_target_"+UUID.randomUUID().toString().replace("-","");
        try(var observer=MongoClients.create(storage.configurationUri())){
            try(var app=new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                    "--server.address=127.0.0.1","--server.port=0","--logging.level.root=ERROR","--tapstate.hz.member-port=0",
                    "--tapstate.hz.jet.cooperative-thread-count=2","--tapstate.store.mongo.uri="+storage.configurationUri(),
                    "--tapstate.connectors.seed-dir="+work.resolve("no-seeds"),"--tapstate.connectors.plugins-dir="+work.resolve("plugins"),
                    "--tapstate.control.auth.jwt-secret=validation-http-fixture-signing-key")){
                NativeAppStorage.awaitBusiness(app);var child=NativeAppStorage.business(app);
                var artifact=Path.of(Objects.requireNonNull(System.getProperty("tapstate.pdk.it.mongodb-jar")));
                var registered=child.getBean(ConnectorArtifactRegistrar.class).register(artifact,RegistrationSource.REGISTER);
                var connector=registered.registration().connectorId();
                var stableRow=new Document("id",1L).append("payload","same");
                observer.getDatabase(sourceDb).getCollection("orders").insertOne(stableRow);
                observer.getDatabase(targetDb).getCollection("checked_orders").insertOne(new Document(stableRow));
                var artifacts=child.getBean(ArtifactStore.class);
                artifacts.create(new SourceResource("validation_input",null,connector,Map.of("uri",MONGO.getReplicaSetUrl(sourceDb),"database",sourceDb,"isUri",true),
                        SourceMode.SNAPSHOT,List.of(TableRef.literal("orders")),null,null));
                artifacts.create(new SourceResource("validation_target",null,connector,Map.of("uri",MONGO.getReplicaSetUrl(targetDb),"database",targetDb,"isUri",true),null,null,null,null));
                artifacts.create(new PipelineResource("validation_flow",null,List.of(SourceRef.spec("validation_input",false)),null,null,
                        new ServeBlock.Inline("serve",FromClause.list(FromRef.literal("validation_input.orders")),List.of(new SyncElement("out","validation_target",null,new RenameSpec(null,null,"checked_",null),null)),null,null),null,null));
                var http=RestClient.create("http://127.0.0.1:"+((WebServerApplicationContext)app).getWebServer().getPort());
                var input=Map.of("pipelineId","validation_flow","taskId","validation_task","taskGeneration",UUID.randomUUID().toString(),"expectedRevision","0","method","COUNT",
                        "mappings",List.of(Map.of("mappingId","orders","outputId","out","sourceId","validation_input","table","orders")),"confirmations",Map.of());
                assertThat(http.post().uri("/api/validation:evaluate").contentType(MediaType.APPLICATION_JSON).body(input)
                        .<Integer>exchange((request,response)->response.getStatusCode().value())).isEqualTo(401);
                var token=child.getBean(TokenService.class).create(Scope.READ);
                assertThat(http.post().uri("/api/validation:evaluate").header("Authorization","Bearer "+token).contentType(MediaType.APPLICATION_JSON).body(input)
                        .<Integer>exchange((request,response)->response.getStatusCode().value())).isEqualTo(403);
                var write=child.getBean(TokenService.class).create(Scope.WRITE);
                var response=http.post().uri("/api/validation:evaluate").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(input)
                        .retrieve().toEntity(Map.class);var evaluated=response.getBody();assertThat(evaluated).isNotNull();
                assertThat(response.getHeaders().getCacheControl()).isEqualTo("no-store");assertThat(evaluated.get("fingerprint")).isInstanceOf(String.class);
                assertThat(evaluated.get("configurationRevision")).isInstanceOf(String.class);assertThat(evaluated.get("premise")).isEqualTo("INITIAL_SYNC_FINISHED_AND_BOTH_SIDES_STABLE");
                var saved=Map.of("input",input,"fingerprint",evaluated.get("fingerprint"),"commandId",UUID.randomUUID().toString(),"historyLimit",10,"scheduleEnabled",false);
                var result=http.post().uri("/api/validation:save").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(saved).retrieve().body(Map.class);
                assertThat(result).containsEntry("outcome","APPLIED");
                var read=http.get().uri("/api/pipelines/validation_flow/validation-tasks/validation_task").header("Authorization","Bearer "+token).retrieve().body(Map.class);
                assertThat(read).containsEntry("revision","1").containsEntry("method","COUNT").containsEntry("nextStartSequence","1");
                for(var revision:List.of(1,1.5,true)){
                    var invalid=new HashMap<String,Object>(input);invalid.put("expectedRevision",revision);
                    assertThat(http.post().uri("/api/validation:evaluate").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(invalid)
                            .<Integer>exchange((request,httpResponse)->httpResponse.getStatusCode().value())).isEqualTo(400);
                }
                for(var bad:List.of(Map.of("historyLimit",1.5),Map.of("historyLimit","10"),Map.of("scheduleEnabled","false"),Map.of("unknown",true))){
                    var invalid=new HashMap<String,Object>(saved);invalid.putAll(bad);
                    assertThat(http.post().uri("/api/validation:save").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(invalid)
                            .<Integer>exchange((request,httpResponse)->httpResponse.getStatusCode().value())).isEqualTo(400);
                }
                var stale=new HashMap<String,Object>(input);stale.put("taskGeneration",read.get("taskGeneration"));stale.put("expectedRevision","2");
                assertThat(http.post().uri("/api/validation:evaluate").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(stale)
                        .<Integer>exchange((request,httpResponse)->httpResponse.getStatusCode().value())).isEqualTo(409);
                for(var malformed:List.of("{\"pipelineId\":\"first\",\"pipelineId\":\"second\"}"," ".repeat(256*1024+1))){
                    assertThat(http.post().uri("/api/validation:evaluate").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(malformed)
                            .<Integer>exchange((request,httpResponse)->httpResponse.getStatusCode().value())).isEqualTo(400);
                }
                assertThat(http.post().uri("/api/validation:schedule-preview").header("Authorization","Bearer "+token).contentType(MediaType.APPLICATION_JSON)
                        .body(Map.of("inputRevision","1")).<Integer>exchange((request,httpResponse)->httpResponse.getStatusCode().value())).isEqualTo(400);
                var schedule=Map.of("kind","CRON","expression","*/5 * * * *","zoneId","Asia/Shanghai");
                var preview=http.post().uri("/api/validation:schedule-preview").header("Authorization","Bearer "+token).contentType(MediaType.APPLICATION_JSON)
                        .body(Map.of("schedule",schedule,"inputRevision","9007199254740993")).retrieve().body(Map.class);
                assertThat(preview).containsEntry("inputRevision","9007199254740993");assertThat((List<?>)preview.get("occurrences")).hasSize(5);
                var checkedFrames=ValidationMetadataSlots.acquire(child.getBean(ValidationMemberResources.Member.class),4,Duration.ofSeconds(2));
                try { assertThat(child.getBean(ValidationMemberResources.Member.class).snapshot().metadataFrames()).isEqualTo(4); }
                finally { checkedFrames.forEach(ValidationMemberResources.Metadata::close); }
                assertThat(child.getBean(com.hazelcast.core.HazelcastInstance.class).getJet().getJobs()).isEmpty();
                var operation=Map.of("context",evaluated.get("context"),"pipeline","validation_flow","task","validation_task","taskGeneration",read.get("taskGeneration"),
                        "revision","1","sequence","1","command",UUID.randomUUID().toString());
                assertThat(http.post().uri("/api/validation:start").header("Authorization","Bearer "+token).contentType(MediaType.APPLICATION_JSON).body(operation)
                        .<Integer>exchange((request,httpResponse)->httpResponse.getStatusCode().value())).isEqualTo(403);
                var started=http.post().uri("/api/validation:start").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(operation).retrieve().body(Map.class);
                assertThat(started.get("outcome")).isIn("PENDING","ACCEPTED");
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100)).untilAsserted(()->{
                    var progress=http.post().uri("/api/validation:progress").header("Authorization","Bearer "+token).contentType(MediaType.APPLICATION_JSON).body(operation).retrieve().body(Map.class);
                    assertThat(progress).containsEntry("state","COMPLETED");
                });
                var retried=http.post().uri("/api/validation:start").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(operation).retrieve().body(Map.class);
                assertThat(retried).containsEntry("state","COMPLETED");
                var stopped=http.post().uri("/api/validation:stop").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(operation).retrieve().body(Map.class);
                assertThat(stopped).containsEntry("state","COMPLETED");
                var after=http.get().uri("/api/pipelines/validation_flow/validation-tasks/validation_task").header("Authorization","Bearer "+token).retrieve().body(Map.class);
                assertThat(after).containsEntry("nextStartSequence","2");
                var returnedFrames=ValidationMetadataSlots.acquire(child.getBean(ValidationMemberResources.Member.class),4,Duration.ofSeconds(5));
                try {
                    assertThat(child.getBean(ValidationMemberResources.Member.class).snapshot().metadataFrames()).isEqualTo(4);
                    assertThat(child.getBean(ValidationMemberResources.Member.class).snapshot().processReserved().bytes()).isEqualTo(2L*1024*1024);
                } finally { returnedFrames.forEach(ValidationMemberResources.Metadata::close); }
                assertThat(child.getBean(com.hazelcast.core.HazelcastInstance.class).getJet().getJobs()).hasSize(1);
                var history=http.get().uri("/api/pipelines/validation_flow/validation-tasks/validation_task/reports").header("Authorization","Bearer "+token).retrieve().body(Map.class);
                assertThat(history).containsEntry("total","1");var items=(List<?>)history.get("items");var first=(Map<?,?>)items.getFirst();var ref=(Map<?,?>)first.get("reference");
                var reportPath="/api/pipelines/validation_flow/validation-tasks/validation_task/reports/"+ref.get("run")+"?generation="+ref.get("generation")+"&sequence="+ref.get("sequence");
                assertThat(http.get().uri(reportPath.substring(0,reportPath.indexOf('?'))).header("Authorization","Bearer "+token)
                        .<Integer>exchange((request,httpResponse)->httpResponse.getStatusCode().value())).isEqualTo(400);
                assertThat(http.get().uri("/api/pipelines/validation_flow/validation-tasks/validation_task/reports/absent/roots?generation="+ref.get("generation")+"&sequence=1")
                        .header("Authorization","Bearer "+token).<Integer>exchange((request,httpResponse)->httpResponse.getStatusCode().value())).isEqualTo(404);
                var report=completedReport(http,token,reportPath);
                assertThat(report).containsEntry("method","COUNT").containsEntry("result","COMPLETE_NO_ISSUES_OBSERVED").containsEntry("physicalDrain","UNKNOWN").containsEntry("premiseVerifiedBySystem",false);
                var counts=(List<?>)report.get("counts");var count=(Map<?,?>)counts.getFirst();assertThat(count.get("source")).isEqualTo("1");assertThat(count.get("target")).isEqualTo("1");assertThat(count.get("difference")).isEqualTo("0");
                var occupied=ValidationMetadataSlots.acquire(child.getBean(ValidationMemberResources.Member.class),4,Duration.ofSeconds(2));
                try{
                    assertThat(http.post().uri("/api/validation:progress").header("Authorization","Bearer "+token).contentType(MediaType.APPLICATION_JSON).body(operation)
                            .<Integer>exchange((request,httpResponse)->httpResponse.getStatusCode().value())).isEqualTo(429);
                    assertThat(http.get().uri(reportPath).header("Authorization","Bearer "+token).<Integer>exchange((request,httpResponse)->httpResponse.getStatusCode().value())).isEqualTo(429);
                }finally{occupied.forEach(ValidationMemberResources.Metadata::close);}
                for(var method:List.of("UNIQUE_KEY","FULL_COLUMN")){
                    var task=method.equals("UNIQUE_KEY")?"validation_keys":"validation_columns";
                    var confirmed=Map.of("orders",Map.of("id",UUID.randomUUID().toString(),"fields",List.of(Map.of("source","id","target","id"))));
                    var selected=new LinkedHashMap<String,Object>(input);selected.put("taskId",task);selected.put("taskGeneration",UUID.randomUUID().toString());
                    selected.put("method",method);selected.put("confirmations",confirmed);
                    var checked=http.post().uri("/api/validation:evaluate").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(selected).retrieve().body(Map.class);
                    var saveSelected=Map.of("input",selected,"fingerprint",checked.get("fingerprint"),"commandId",UUID.randomUUID().toString(),"historyLimit",10,"scheduleEnabled",false);
                    var serverSaved=http.post().uri("/api/validation:save").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(saveSelected).retrieve().body(Map.class);
                    assertThat(serverSaved).containsEntry("outcome","APPLIED");assertThat(serverSaved.get("taskGeneration")).isNotEqualTo(selected.get("taskGeneration"));
                    var original=Map.of("context",checked.get("context"),"pipeline","validation_flow","task",task,"taskGeneration",serverSaved.get("taskGeneration"),
                            "revision","1","sequence","1","command",UUID.randomUUID().toString());
                    var complete=runAndReport(http,write,token,original,task);
                    assertThat(complete).containsEntry("method",method).containsEntry("result","COMPLETE_NO_ISSUES_OBSERVED");
                    if(method.equals("FULL_COLUMN")){
                        observer.getDatabase(targetDb).getCollection("checked_orders").updateOne(new Document("id",1L),new Document("$set",new Document("payload","changed")));
                        var changed=new HashMap<String,Object>(original);changed.put("sequence","2");changed.put("command",UUID.randomUUID().toString());
                        var differences=runAndReport(http,write,token,changed,task);assertThat(differences).containsEntry("result","COMPLETE_WITH_OBSERVATIONS").containsEntry("rootCount","1");
                        var reference=(Map<?,?>)differences.get("reference");
                        var rootPath="/api/pipelines/validation_flow/validation-tasks/"+task+"/reports/"+reference.get("run")+"/roots";
                        var query="?generation="+reference.get("generation")+"&sequence="+reference.get("sequence");
                        var roots=http.get().uri(rootPath+query).header("Authorization","Bearer "+token).retrieve().body(Map.class);
                        assertThat(roots).containsEntry("total","1");var root=(Map<?,?>)((List<?>)roots.get("items")).getFirst();
                        String detail=http.get().uri(rootPath+"/"+root.get("id")+query).header("Authorization","Bearer "+token).retrieve().body(String.class);
                        assertThat(detail).contains("same","changed","codecBinding","observation");
                    }
                }
                var finalFrames=ValidationMetadataSlots.acquire(child.getBean(ValidationMemberResources.Member.class),4,Duration.ofSeconds(5));
                try { assertThat(child.getBean(ValidationMemberResources.Member.class).snapshot().metadataFrames()).isEqualTo(4); }
                finally { finalFrames.forEach(ValidationMemberResources.Metadata::close); }
                assertThat(child.getBean(com.hazelcast.core.HazelcastInstance.class).getJet().getJobs()).hasSize(4);
                var wall=new LinkedHashMap<String,Object>(input);wall.put("taskId","scheduled_wall");wall.put("taskGeneration",UUID.randomUUID().toString());
                var wallCheck=http.post().uri("/api/validation:evaluate").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(wall).retrieve().body(Map.class);
                var anchor=java.time.Instant.now().minusSeconds(50).toString();
                var scheduleInput=Map.of("kind","INTERVAL","every","1","unit","MINUTES","anchor",anchor,"zoneId","UTC");
                var wallSave=Map.of("input",wall,"fingerprint",wallCheck.get("fingerprint"),"commandId",UUID.randomUUID().toString(),"historyLimit",10,"schedule",scheduleInput,"scheduleEnabled",true);
                var wallSaved=http.post().uri("/api/validation:save").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(wallSave).retrieve().body(Map.class);
                assertThat(wallSaved).containsEntry("outcome","APPLIED");
                var nextDue=new java.util.concurrent.atomic.AtomicReference<java.time.Instant>();
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(()->{
                    var scheduled=http.get().uri("/api/pipelines/validation_flow/validation-tasks/scheduled_wall").header("Authorization","Bearer "+token).retrieve().body(Map.class);
                    var observation=(Map<?,?>)scheduled.get("scheduleObservation");assertThat(observation).isNotNull();
                    nextDue.set(java.time.Instant.parse((String)observation.get("nextDue")));
                });
                var remaining=Duration.between(java.time.Instant.now(),nextDue.get());assertThat(remaining).isLessThanOrEqualTo(Duration.ofMinutes(1));
                org.awaitility.Awaitility.await().atMost((remaining.isNegative()?Duration.ZERO:remaining).plusSeconds(15)).pollInterval(Duration.ofMillis(200)).untilAsserted(()->{
                    var observed=http.get().uri("/api/pipelines/validation_flow/validation-tasks/scheduled_wall/reports").header("Authorization","Bearer "+token).retrieve().body(Map.class);
                    assertThat(observed).containsEntry("total","1");var completed=(Map<?,?>)((List<?>)observed.get("items")).getFirst();assertThat(completed.get("terminal")).isEqualTo("COMPLETED");
                });
                var wallOff=Map.of("pipeline","validation_flow","task","scheduled_wall","taskGeneration",wallSaved.get("taskGeneration"),"expectedRevision","1","command",UUID.randomUUID().toString());
                assertThat(http.post().uri("/api/validation:schedule-disable").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(wallOff).retrieve().body(Map.class))
                        .containsEntry("outcome","APPLIED");
                var columns=http.get().uri("/api/pipelines/validation_flow/validation-tasks/validation_columns").header("Authorization","Bearer "+token).retrieve().body(Map.class);
                var configured=(SourceResource)artifacts.get("validation_input").orElseThrow();var unavailable=new HashMap<>(configured.config());unavailable.put("uri","mongodb://127.0.0.1:1/unavailable?serverSelectionTimeoutMS=10");
                artifacts.replace(configured.id(),io.tapstate.core.model.canonical.CanonicalHash.of(configured),new SourceResource(configured.id(),configured.metadata(),configured.connector(),unavailable,
                        configured.mode(),configured.tables(),configured.srs(),configured.execution(),configured.experimental()));
                var off=Map.of("pipeline","validation_flow","task","validation_columns","taskGeneration",columns.get("taskGeneration"),"expectedRevision","1","command",UUID.randomUUID().toString());
                assertThat(http.post().uri("/api/validation:schedule-disable").header("Authorization","Bearer "+token).contentType(MediaType.APPLICATION_JSON).body(off)
                        .<Integer>exchange((request,httpResponse)->httpResponse.getStatusCode().value())).isEqualTo(403);
                var disabled=http.post().uri("/api/validation:schedule-disable").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(off).retrieve().body(Map.class);
                assertThat(disabled).containsEntry("outcome","APPLIED").containsEntry("receiptRevision","2").containsEntry("scheduleEnabled",false);
                assertThat(http.post().uri("/api/validation:schedule-disable").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(off).retrieve().body(Map.class))
                        .containsEntry("outcome","DUPLICATE");
                assertThat(child.getBean(com.hazelcast.core.HazelcastInstance.class).getJet().getJobs()).hasSize(5);

            }finally{
                for(var name:List.of(sourceDb,targetDb,storage.configurationDatabase(),storage.cacheDatabase(),storage.masterDatabase()))observer.getDatabase(name).drop();
            }
        }
    }
    private static Map runAndReport(RestClient http,String write,String read,Map<String,?> original,String task){
        var start=http.post().uri("/api/validation:start").header("Authorization","Bearer "+write).contentType(MediaType.APPLICATION_JSON).body(original).retrieve().body(Map.class);
        assertThat(start.get("outcome")).isIn("PENDING","ACCEPTED");
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100)).untilAsserted(()->{
            var progress=http.post().uri("/api/validation:progress").header("Authorization","Bearer "+read).contentType(MediaType.APPLICATION_JSON).body(original).retrieve().body(Map.class);
            assertThat(progress).containsEntry("state","COMPLETED");
        });
        var path="/api/pipelines/validation_flow/validation-tasks/"+task+"/reports/"+start.get("run")+"?generation="+original.get("taskGeneration")+"&sequence="+original.get("sequence");
        return completedReport(http,read,path);
    }

    private static Map completedReport(RestClient http,String token,String path){
        var completed=new java.util.concurrent.atomic.AtomicReference<Map>();
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100))
                .ignoreExceptionsMatching(failure->failure instanceof org.springframework.web.client.HttpClientErrorException.NotFound||failure instanceof org.springframework.web.client.HttpClientErrorException.TooManyRequests).untilAsserted(()->{
                    var report=http.get().uri(path).header("Authorization","Bearer "+token).retrieve().body(Map.class);
                    assertThat(report).containsEntry("terminal","COMPLETED").containsEntry("closed",true);completed.set(report);
                });
        return completed.get();
    }

}
