package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.*;
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

/** Real HTTP control remains reachable while two actual native count commands are held by Mongo. */
@RequiresDocker
@SuppressWarnings({"rawtypes", "unchecked"})
class NativeValidationControlHeadroomIT {
    @Container static final MongoDBContainer MONGO = NativeValidationOwnerReservationsIT.MONGO;
    @Container static final MongoDBContainer BUSINESS = new MongoDBContainer("mongo:7.0")
            .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--setParameter", "enableTestCommands=1");
    @TempDir Path work;

    @Test @Timeout(120) void twoBlockedCountPipelinesKeepHttpProgressAndStopCapacity() throws Exception {
        var storage=NativeAppStorage.createOnPremWithAdmin(MONGO,"validation_headroom","admin","headroom-fixture-password");
        try(var schema=MongoConfigurationDomain.prepareValidationSchema(new MongoConnectionSettings(storage.configurationUri(),null,Duration.ofSeconds(5)),storage.clusterId(),UUID.randomUUID());
            var cache=MongoCacheDomain.connect(new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,storage.cacheUri(),storage.cacheDatabase(),null),Duration.ofSeconds(5),"validation-headroom")){
            schema.prepareCache(cache);while(!schema.advance(20).ready()){}
        }
        var data="headroom_data_"+UUID.randomUUID().toString().replace("-","");
        try(var observer=MongoClients.create(storage.configurationUri());var business=MongoClients.create(BUSINESS.getReplicaSetUrl())){
            business.getDatabase(data).getCollection("orders").insertOne(new Document("id",1L));
            business.getDatabase(data).getCollection("checked_orders").insertOne(new Document("id",1L));
            var admin=business.getDatabase("admin");
            try(var app=new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                    "--server.address=127.0.0.1","--server.port=0","--logging.level.root=ERROR","--tapstate.hz.member-port=0",
                    "--tapstate.hz.jet.cooperative-thread-count=4","--tapstate.store.mongo.uri="+storage.configurationUri(),
                    "--tapstate.connectors.seed-dir="+work.resolve("no-seeds"),"--tapstate.connectors.plugins-dir="+work.resolve("plugins"),
                    "--tapstate.control.auth.jwt-secret=validation-headroom-fixture-signing-key")){
                NativeAppStorage.awaitBusiness(app);var child=NativeAppStorage.business(app);
                var connector=child.getBean(ConnectorArtifactRegistrar.class).register(Path.of(Objects.requireNonNull(System.getProperty("tapstate.pdk.it.mongodb-jar"))),RegistrationSource.REGISTER).registration().connectorId();
                var artifacts=child.getBean(ArtifactStore.class);
                artifacts.create(new SourceResource("input",null,connector,Map.of("uri",BUSINESS.getReplicaSetUrl(data),"database",data,"isUri",true),SourceMode.SNAPSHOT,List.of(TableRef.literal("orders")),null,null));
                artifacts.create(new SourceResource("target",null,connector,Map.of("uri",BUSINESS.getReplicaSetUrl(data),"database",data,"isUri",true),null,null,null,null));
                var http=RestClient.create("http://127.0.0.1:"+((WebServerApplicationContext)app).getWebServer().getPort());
                var token=child.getBean(TokenService.class).create(Scope.WRITE);var operations=new ArrayList<Map<String,Object>>();
                var nativeOperations=new ArrayList<io.tapstate.runtime.probe.ValidationOperation>();
                for(var pipeline:List.of("flow_a","flow_b")){
                    artifacts.create(new PipelineResource(pipeline,null,List.of(SourceRef.spec("input",false)),null,null,
                            new ServeBlock.Inline("serve",FromClause.list(FromRef.literal("input.orders")),List.of(new SyncElement("out","target",null,new RenameSpec(null,null,"checked_",null),null)),null,null),null,null));
                    var input=Map.of("pipelineId",pipeline,"taskId","full","expectedRevision","0","method","COUNT",
                            "mappings",List.of(Map.of("mappingId","orders","outputId","out","sourceId","input","table","orders")),"confirmations",Map.of());
                    var evaluated=post(http,token,"evaluate",input);
                    var saved=post(http,token,"save",Map.of("input",input,"fingerprint",evaluated.get("fingerprint"),"commandId",UUID.randomUUID().toString(),"historyLimit",10,"scheduleEnabled",false));
                    assertThat(saved).containsEntry("outcome","APPLIED");
                    var command=UUID.randomUUID();
                    operations.add(Map.of("context",evaluated.get("context"),"pipeline",pipeline,"task","full","taskGeneration",saved.get("taskGeneration"),"revision","1","sequence","1","command",command.toString()));
                    nativeOperations.add(new io.tapstate.runtime.probe.ValidationOperation(child.getBean(NativeValidationStartCoordinator.class).context(),pipeline,"full",UUID.fromString((String)saved.get("taskGeneration")),1,1,command));
                }
                // Scheduled admission has separate coverage. Freeze its background activity so the
                // two original manual reservations, rather than an unrelated tick, own this scenario.
                child.getBean(NativeValidationScheduler.class).close();
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(()->child.getBean(ValidationMemberResources.Member.class).snapshot().metadataFrames()==0);
                long before=((Number)admin.runCommand(new Document("configureFailPoint","failCommand").append("mode",new Document("times",2))
                        .append("data",new Document("failCommands",List.of("aggregate")).append("namespace",data+".orders").append("blockConnection",true).append("blockTimeMS",20000))).get("count")).longValue();
                try{
                    // Admit the actual production Jobs directly so transient START response frames cannot
                    // decide this control-headroom scenario before both long-lived reads exist.
                    var owners=child.getBean(NativeValidationOwnerReservations.class);var runs=child.getBean(NativeValidationRuns.class);
                    for(var original:nativeOperations){
                        var request=new ValidationOwnerRouter.Request(original.pipeline(),original.task(),original.taskGeneration(),1,1,"FULL",original.command(),NativeValidationStartCoordinator.runId(original));
                        var accepted=owners.reserve(request);assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
                        var start=owners.original(request,accepted.owner());var reservation=owners.take(start);
                        runs.start(start,new NativeValidationFullJob(reservation,child.getBean(io.tapstate.runtime.engine.validation.ValidationProcessorRegistry.class),
                                child.getBean(com.hazelcast.core.HazelcastInstance.class),child.getBean(ValidationMemberResources.class),runs,child.getBean(NativeValidationFullJob.Factory.class)));
                        // A single server-observed native command belongs to each admitted Pipeline.
                        admin.runCommand(new Document("waitForFailPoint","failCommand").append("timesEntered",before+nativeOperations.indexOf(original)+1).append("maxTimeMS",10000));
                    }
                    // This server-owned barrier proves both native count commands entered before HTTP control.
                    admin.runCommand(new Document("waitForFailPoint","failCommand").append("timesEntered",before+2).append("maxTimeMS",10000));
                    var resources=child.getBean(ValidationMemberResources.Member.class);
                    var first=operations.getFirst();
                    var progress=http.post().uri("/api/validation:progress").header("Authorization","Bearer "+token).contentType(MediaType.APPLICATION_JSON).body(first)
                            .exchange((request,response)->Map.entry(response.getStatusCode().value(),response.bodyTo(Map.class)));
                    assertThat(progress.getKey()).as("actual control response: %s",progress.getValue()).isEqualTo(200);
                    assertThat(progress.getValue()).containsEntry("admitted",true).containsEntry("controlUnknown",false);
                    assertThat(resources.snapshot().metadataFrames()).isEqualTo(2);
                    assertThat(post(http,token,"stop",first)).containsEntry("admitted",true).containsEntry("controlUnknown",false);
                    // The other blocked native command must still belong to its original run.
                    assertThat(post(http,token,"progress",operations.getLast())).containsEntry("admitted",true).containsEntry("state","RUNNING");
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(40)).untilAsserted(()->{
                        assertThat(post(http,token,"progress",first)).containsEntry("state","STOPPED").containsEntry("drain","DRAINED");
                        assertThat(post(http,token,"progress",operations.getLast())).containsEntry("state","COMPLETED").containsEntry("drain","DRAINED");
                        assertThat(resources.snapshot().metadataFrames()).isZero();
                    });
                }finally{admin.runCommand(new Document("configureFailPoint","failCommand").append("mode","off"));}
            }finally{business.getDatabase(data).drop();for(var name:List.of(storage.configurationDatabase(),storage.cacheDatabase(),storage.masterDatabase()))observer.getDatabase(name).drop();}
        }
    }
    private static Map<String,Object> post(RestClient http,String token,String action,Map<String,?> body){
        return http.post().uri("/api/validation:"+action).header("Authorization","Bearer "+token).contentType(MediaType.APPLICATION_JSON).body(body).retrieve().body(Map.class);
    }
}
