package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.pdk.ConnectorArtifactRegistrar;
import io.tapstate.control.core.*;
import io.tapstate.core.model.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.*;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;

import static org.assertj.core.api.Assertions.*;

/** Actual registered relational PDK through configuration, native HTTP, Jet and Mongo reports; not Cloud acceptance. */
@RequiresDocker
@SuppressWarnings({"rawtypes", "unchecked"})
class NativeValidationRelationalApiIT {
    @Container static final MongoDBContainer MONGO=NativeValidationOwnerReservationsIT.MONGO;
    private static final String DB="validation_http",PASSWORD="validation-http-fixture";
    @TempDir Path work;
    @Test @Timeout(180) void mysqlMappingsAndAllMethodsUseTheRealHttpAndScheduledRun()throws Exception{verify(true);}
    @Test @Timeout(180) void postgresMappingsAndAllMethodsUseTheRealHttpAndScheduledRun()throws Exception{verify(false);}

    private void verify(boolean mysql)throws Exception{
        int port=mysql?3306:5432;String kind=mysql?"mysql":"postgres";
        try(var sql=new GenericContainer<>(mysql?"mysql:8.0":"postgres:16").withExposedPorts(port).withStartupTimeout(Duration.ofSeconds(75))){
            if(mysql)sql.withEnv("MYSQL_ROOT_PASSWORD",PASSWORD).withEnv("MYSQL_DATABASE",DB).withEnv("MYSQL_USER","validation").withEnv("MYSQL_PASSWORD",PASSWORD)
                    .withCommand("--innodb-buffer-pool-size=64M","--max-connections=20").waitingFor(Wait.forLogMessage(".*mysqld: ready for connections\\..*port: 3306.*\\n",1));
            else sql.withEnv("POSTGRES_PASSWORD",PASSWORD).withEnv("POSTGRES_USER","validation").withEnv("POSTGRES_DB",DB)
                    .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n",2));
            sql.start();String statement="CREATE TABLE orders (id BIGINT PRIMARY KEY, payload VARCHAR(32), amount BIGINT); INSERT INTO orders VALUES (1,'same',9007199254740993),(2,'two',9007199254740993); "
                    +"CREATE TABLE items (id BIGINT PRIMARY KEY, secondary BIGINT, alias_id BIGINT, alias_secondary BIGINT, payload VARCHAR(32)); INSERT INTO items VALUES(9,90,9,90,'item'),(11,110,11,110,'tail');";
            var seeded=mysql?sql.execInContainer("mysql","--protocol=TCP","--host=127.0.0.1","--user=validation","--password="+PASSWORD,"--batch",DB,"-e",statement)
                    :sql.execInContainer("psql","-U","validation","-d",DB,"-c",statement);
            assertThat(seeded.getExitCode()).as(seeded.getStderr()).isZero();
            var source=new LinkedHashMap<String,Object>();source.put("host",sql.getHost());source.put("port",sql.getMappedPort(port));source.put("database",DB);
            source.put(mysql?"username":"user","validation");source.put("password",PASSWORD);source.put("timezone","+00:00");source.put("oldVersionTimezone",false);
            if(mysql)source.put("addtionalString","useSSL=false&allowPublicKeyRetrieval=true");else source.put("schema","public");
            exercise(kind,source);
        }
    }

    private void exercise(String kind,Map<String,Object> source)throws Exception{
        var storage=NativeAppStorage.createOnPremWithAdmin(MONGO,"relational_http","admin",PASSWORD);
        try(var schema=io.tapstate.adapters.mongostore.MongoConfigurationDomain.prepareValidationSchema(
                new io.tapstate.adapters.mongostore.MongoConnectionSettings(storage.configurationUri(),null,Duration.ofSeconds(5)),storage.clusterId(),UUID.randomUUID());
                var cache=io.tapstate.adapters.mongostore.MongoCacheDomain.connect(new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,storage.cacheUri(),storage.cacheDatabase(),null),Duration.ofSeconds(5),"relational-http-fixture")){
            schema.prepareCache(cache);while(!schema.advance(20).ready()){}
        }
        var targetName="relational_http_"+UUID.randomUUID().toString().replace("-","");
        try(var observer=MongoClients.create(storage.configurationUri())){
            var target=observer.getDatabase(targetName);
            target.getCollection("checked_orders").insertMany(List.of(new Document("id",1L).append("payload","same").append("amount",9007199254740993L),
                    new Document("id",2L).append("payload","two").append("amount",9007199254740993L)));
            target.getCollection("checked_items").insertMany(List.of(item(9,90,"item"),item(11,110,"tail")));
            try(var app=new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                    "--server.address=127.0.0.1","--server.port=0","--logging.level.root=ERROR","--tapstate.hz.member-port=0","--tapstate.hz.jet.cooperative-thread-count=2",
                    "--tapstate.store.mongo.uri="+storage.configurationUri(),"--tapstate.connectors.seed-dir="+work.resolve("no-seeds"),
                    "--tapstate.connectors.plugins-dir="+work.resolve("plugins"),"--tapstate.control.auth.jwt-secret=relational-validation-http-signing-key")){
                NativeAppStorage.awaitBusiness(app);var child=NativeAppStorage.business(app);var registrar=child.getBean(ConnectorArtifactRegistrar.class);
                var sourceConnector=registrar.register(artifact(kind),RegistrationSource.REGISTER).registration().connectorId();
                var targetConnector=registrar.register(artifact("mongodb"),RegistrationSource.REGISTER).registration().connectorId();
                var artifacts=child.getBean(ArtifactStore.class);
                artifacts.create(new SourceResource("input",null,sourceConnector,source,SourceMode.SNAPSHOT,List.of(TableRef.literal("orders"),TableRef.literal("items")),null,null));
                artifacts.create(new SourceResource("target",null,targetConnector,Map.of("uri",MONGO.getReplicaSetUrl(targetName),"database",targetName,"isUri",true),null,null,null,null));
                artifacts.create(new PipelineResource("flow",null,List.of(SourceRef.spec("input",false)),null,null,
                        new ServeBlock.Inline("serve",FromClause.list(FromRef.literal("input.orders"),FromRef.literal("input.items")),
                                List.of(new SyncElement("out","target",null,new RenameSpec(null,null,"checked_",null),null)),null,null),null,null));
                var tokens=child.getBean(TokenService.class);String write=tokens.create(Scope.WRITE),read=tokens.create(Scope.READ);
                var http=RestClient.create("http://127.0.0.1:"+((WebServerApplicationContext)app).getWebServer().getPort());
                var operations=new LinkedHashMap<String,Map<String,Object>>();var inputs=new LinkedHashMap<String,Map<String,Object>>();
                for(var method:List.of("COUNT","UNIQUE_KEY","FULL_COLUMN")){
                    var task=method.toLowerCase(Locale.ROOT);var input=selection(task,method);inputs.put(method,input);
                    var evaluated=post(http,write,"evaluate",input);
                    var saved=post(http,write,"save",Map.of("input",input,"fingerprint",evaluated.get("fingerprint"),"commandId",UUID.randomUUID().toString(),"historyLimit",10,"scheduleEnabled",false));
                    assertThat(saved).containsEntry("outcome","APPLIED");
                    var operation=new LinkedHashMap<String,Object>(Map.of("context",evaluated.get("context"),"pipeline","flow","task",task,"taskGeneration",saved.get("taskGeneration"),
                            "revision","1","sequence","1","command",UUID.randomUUID().toString()));operations.put(method,operation);
                    var report=run(http,write,read,operation);assertThat(report).containsEntry("method",method).containsEntry("result","COMPLETE_NO_ISSUES_OBSERVED").containsEntry("rootCount","0").containsEntry("premiseVerifiedBySystem",false);
                    assertThat((List<?>)report.get("mappings")).hasSize(2);
                    if(method.equals("COUNT"))assertThat((List<Map>)report.get("counts")).hasSize(2).allSatisfy(count->assertThat(count).containsEntry("source","2").containsEntry("target","2").containsEntry("difference","0"));
                }
                // All writes have stopped before the next comparisons. Deliberate faults exercise distinct methods.
                target.getCollection("checked_orders").deleteOne(new Document("id",2L));
                target.getCollection("checked_orders").insertOne(new Document("id",3L).append("payload","extra").append("amount",9007199254740993L));
                target.getCollection("checked_orders").updateOne(new Document("id",1L),new Document("$set",new Document("amount",9007199254740992L)));
                target.getCollection("checked_items").insertOne(item(9,90,"duplicate"));
                for(var method:operations.keySet()){
                    var operation=new LinkedHashMap<>(operations.get(method));operation.put("sequence","2");operation.put("command",UUID.randomUUID().toString());
                    var report=run(http,write,read,operation);assertThat(report).containsEntry("result","COMPLETE_WITH_OBSERVATIONS");
                    if(method.equals("COUNT")){
                        assertThat(report).containsEntry("rootCount","0");assertThat((List<Map>)report.get("counts")).anySatisfy(count->assertThat(count).containsEntry("source","2").containsEntry("target","3").containsEntry("difference","-1"));
                    }else{
                        var ref=(Map)report.get("reference");var rootPath="/api/pipelines/flow/validation-tasks/"+operation.get("task")+"/reports/"+ref.get("run")+"/roots";
                        String query="?generation="+ref.get("generation")+"&sequence="+ref.get("sequence");var roots=get(http,read,rootPath+query);
                        assertThat(Long.parseLong((String)roots.get("total"))).isGreaterThanOrEqualTo(3);
                        var details=new StringBuilder();for(var root:(List<Map>)roots.get("items"))details.append(get(http,read,rootPath+"/"+root.get("id")+query));
                        assertThat(details.toString()).contains("TARGET_MISSING","TARGET_EXTRA","TARGET_DUPLICATE");
                        if(method.equals("FULL_COLUMN"))assertThat(details.toString()).contains("TARGET_VALUE","amount","9007199254740993","9007199254740992");
                        else assertThat(details.toString()).doesNotContain("TARGET_VALUE");
                    }
                }
                var input=new LinkedHashMap<>(inputs.get("COUNT"));input.put("expectedRevision","1");input.put("taskGeneration",operations.get("COUNT").get("taskGeneration"));
                var checked=post(http,write,"evaluate",input);
                var interval=Map.of("kind","INTERVAL","every","1","unit","MINUTES","anchor",Instant.now().minusSeconds(50).toString(),"zoneId","UTC");
                assertThat(post(http,write,"save",Map.of("input",input,"fingerprint",checked.get("fingerprint"),"commandId",UUID.randomUUID().toString(),"historyLimit",10,"scheduleEnabled",true,"schedule",interval))).containsEntry("outcome","APPLIED");
                var nextDue=new java.util.concurrent.atomic.AtomicReference<Instant>();
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(()->{
                    var observation=(Map)get(http,read,"/api/pipelines/flow/validation-tasks/count").get("scheduleObservation");
                    assertThat(observation).isNotNull();nextDue.set(Instant.parse((String)observation.get("nextDue")));
                });
                var untilDue=Duration.between(Instant.now(),nextDue.get());assertThat(untilDue).isLessThanOrEqualTo(Duration.ofMinutes(1));
                var wait=(untilDue.isNegative()?Duration.ZERO:untilDue).plusSeconds(15);
                org.awaitility.Awaitility.await().atMost(wait).pollInterval(Duration.ofMillis(200)).untilAsserted(()->{
                    var history=get(http,read,"/api/pipelines/flow/validation-tasks/count/reports");assertThat(history).containsEntry("total","3");
                    assertThat((List<Map>)history.get("items")).allSatisfy(row->assertThat(row).containsEntry("terminal","COMPLETED"));
                });
                assertThat(post(http,write,"schedule-disable",Map.of("pipeline","flow","task","count","taskGeneration",input.get("taskGeneration"),"expectedRevision","2","command",UUID.randomUUID().toString()))).containsEntry("outcome","APPLIED");
                assertThat(child.getBean(com.hazelcast.core.HazelcastInstance.class).getJet().getJobs()).hasSize(7);
            }finally{for(var name:List.of(targetName,storage.configurationDatabase(),storage.cacheDatabase(),storage.masterDatabase()))observer.getDatabase(name).drop();}
        }
    }
    private static Document item(long id,long secondary,String payload){return new Document("id",id).append("secondary",secondary).append("alias_id",id).append("alias_secondary",secondary).append("payload",payload);}
    private static Path artifact(String kind){return Path.of(Objects.requireNonNull(System.getProperty("tapstate.pdk.it."+kind+"-jar")));}
    private static Map<String,Object> selection(String task,String method){
        Map<String,Object> confirmations=method.equals("COUNT")?Map.of():Map.of("orders",Map.of("id",UUID.randomUUID().toString(),"fields",List.of(Map.of("source","id","target","id"))),
                "items",Map.of("id",UUID.randomUUID().toString(),"fields",List.of(Map.of("source","id","target","alias_id"),Map.of("source","secondary","target","alias_secondary"))));
        return Map.of("pipelineId","flow","taskId",task,"expectedRevision","0","method",method,"mappings",List.of(
                Map.of("mappingId","orders","outputId","out","sourceId","input","table","orders"),Map.of("mappingId","items","outputId","out","sourceId","input","table","items")),"confirmations",confirmations);
    }
    private static Map<String,Object> run(RestClient http,String write,String read,Map<String,Object> operation){
        var start=post(http,write,"start",operation);assertThat(start.get("outcome")).isIn("PENDING","ACCEPTED");
        var completed=new java.util.concurrent.atomic.AtomicReference<Map<String,Object>>();
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(25)).pollInterval(Duration.ofMillis(100))
                .ignoreExceptionsMatching(failure->failure instanceof org.springframework.web.client.HttpClientErrorException.NotFound||failure instanceof org.springframework.web.client.HttpClientErrorException.TooManyRequests).untilAsserted(()->{
                    var report=get(http,read,"/api/pipelines/flow/validation-tasks/"+operation.get("task")+"/reports/"+start.get("run")+"?generation="+operation.get("taskGeneration")+"&sequence="+operation.get("sequence"));
                    assertThat(report).containsEntry("terminal","COMPLETED").containsEntry("closed",true);completed.set(report);
                });
        assertThat(post(http,read,"progress",operation)).containsEntry("state","COMPLETED");return completed.get();
    }
    private static Map<String,Object> post(RestClient http,String token,String action,Map<String,?> body){return http.post().uri("/api/validation:"+action).header("Authorization","Bearer "+token).contentType(MediaType.APPLICATION_JSON).body(body).retrieve().body(Map.class);}
    private static Map<String,Object> get(RestClient http,String token,String path){return http.get().uri(path).header("Authorization","Bearer "+token).retrieve().body(Map.class);}
}
