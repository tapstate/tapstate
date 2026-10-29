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
    @Test @Timeout(120) void countEvaluationAndSaveUseTheActiveChildAndRealRegisteredPdk()throws Exception{
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
                observer.getDatabase(sourceDb).getCollection("orders").insertOne(new Document("id",1L).append("payload","same"));
                observer.getDatabase(targetDb).getCollection("checked_orders").insertOne(new Document("id",1L).append("payload","same"));
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
                var stale=new HashMap<String,Object>(input);stale.put("expectedRevision","2");
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
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).untilAsserted(()->assertThat(child.getBean(ValidationMemberResources.Member.class).snapshot().metadataFrames()).isZero());
                assertThat(child.getBean(com.hazelcast.core.HazelcastInstance.class).getJet().getJobs()).isEmpty();
            }finally{
                for(var name:List.of(sourceDb,targetDb,storage.configurationDatabase(),storage.cacheDatabase(),storage.masterDatabase()))observer.getDatabase(name).drop();
            }
        }
    }
}
