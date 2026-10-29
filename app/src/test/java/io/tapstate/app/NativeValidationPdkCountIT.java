package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.PdkValidationReads;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.ValidationProcessorRegistry;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.*;

/** Saved native definitions to actual isolated PDK counts, original owner Job and CONFIG report. */
@RequiresDocker
class NativeValidationPdkCountIT {
    @Container static final MongoDBContainer MONGO=NativeValidationOwnerReservationsIT.MONGO;
    private static final String DB="validation_factory",PASSWORD="pdk-count-fixture";
    @Test @Timeout(180) void mysqlSavedMappingsRunThroughTheProductionCountFactory()throws Exception{verify(true,ValidationMethod.COUNT);}
    @Test @Timeout(180) void postgresSavedMappingsRunThroughTheProductionCountFactory()throws Exception{verify(false,ValidationMethod.COUNT);}
    @Test @Timeout(180) void mysqlSavedMappingsRunThroughTheProductionKeyFactory()throws Exception{verify(true,ValidationMethod.UNIQUE_KEY);}
    @Test @Timeout(180) void postgresSavedMappingsRunThroughTheProductionKeyFactory()throws Exception{verify(false,ValidationMethod.UNIQUE_KEY);}
    @Test @Timeout(180) void mysqlSavedMappingsRunThroughTheProductionFullColumnFactory()throws Exception{verify(true,ValidationMethod.FULL_COLUMN);}
    @Test @Timeout(180) void postgresSavedMappingsRunThroughTheProductionFullColumnFactory()throws Exception{verify(false,ValidationMethod.FULL_COLUMN);}
    @Test @Timeout(180) void canonicalLookingStringsCannotPassAsNativeGeneratedIdentifiers()throws Exception{verify(true,ValidationMethod.FULL_COLUMN,true);}
    private void verify(boolean mysql,ValidationMethod method)throws Exception{
        verify(mysql,method,false);
    }
    private void verify(boolean mysql,ValidationMethod method,boolean stringIdentifiers)throws Exception{
        boolean count=method==ValidationMethod.COUNT;
        var kind=mysql?"mysql":"postgres";var sourceArtifact=artifact(kind);var targetArtifact=artifact("mongodb");int port=mysql?3306:5432;
        try(var sql=new GenericContainer<>(DockerImageName.parse(mysql?"mysql:8.0":"postgres:16")).withExposedPorts(port).withStartupTimeout(Duration.ofSeconds(75))){
            if(mysql)sql.withEnv("MYSQL_ROOT_PASSWORD",PASSWORD).withEnv("MYSQL_DATABASE",DB).withEnv("MYSQL_USER","validation")
                    .withEnv("MYSQL_PASSWORD",PASSWORD).withCommand("--innodb-buffer-pool-size=64M","--max-connections=20")
                    .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections\\..*port: 3306.*\\n",1));
            else sql.withEnv("POSTGRES_PASSWORD",PASSWORD).withEnv("POSTGRES_USER","validation").withEnv("POSTGRES_DB",DB)
                    .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n",2));
            sql.start();var statement=count
                    ?"CREATE TABLE orders (id BIGINT PRIMARY KEY); CREATE TABLE empty_orders (id BIGINT PRIMARY KEY); CREATE TABLE items (id BIGINT PRIMARY KEY); INSERT INTO orders VALUES (1),(2),(3); INSERT INTO items VALUES(9);"
                    :"CREATE TABLE orders (id BIGINT, payload VARCHAR(32), amount BIGINT DEFAULT 9007199254740993); CREATE TABLE empty_orders (id BIGINT PRIMARY KEY); CREATE TABLE items (id BIGINT PRIMARY KEY, secondary BIGINT, alias_id BIGINT, alias_secondary BIGINT, payload VARCHAR(32)); INSERT INTO orders(id,payload) VALUES (0,'head'),(1,'same'),(2,'two'),(3,'missing'),(5,'duplicate-a'),(5,'duplicate-b'),(6,'expected'),(7,'numeric'),(12,NULL),(13,''); INSERT INTO items VALUES(9,90,9,90,'item'),(11,110,11,110,'tail');";
            var seeded=mysql?sql.execInContainer("mysql","--protocol=TCP","--host=127.0.0.1","--user=validation","--password="+PASSWORD,"--batch",DB,"-e",statement)
                    :sql.execInContainer("psql","-U","validation","-d",DB,"-c",statement);
            assertThat(seeded.getExitCode()).as(seeded.getStderr()).isZero();
            var sourceSettings=new LinkedHashMap<String,Object>();sourceSettings.put("host",sql.getHost());sourceSettings.put("port",sql.getMappedPort(port));
            sourceSettings.put("database",DB);sourceSettings.put(mysql?"username":"user","validation");sourceSettings.put("password",PASSWORD);
            sourceSettings.put("timezone","+00:00");sourceSettings.put("oldVersionTimezone",false);
            if(mysql)sourceSettings.put("addtionalString","useSSL=false&allowPublicKeyRetrieval=true");else sourceSettings.put("schema","public");
            try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
                var processors=new ValidationProcessorRegistry(f.member,()->{})){
                String destination="validation_target_"+f.suffix;var target=f.raw.getDatabase(destination);
                var documents=count?List.of(new Document("id",1L),new Document("id",2L))
                        :List.of(new Document("id",1L).append("payload","same").append("extra_only",Date.from(java.time.Instant.parse("2026-10-21T01:02:03.123Z"))),new Document("id",2L).append("payload","two-a"),
                                new Document("id",2L).append("payload","two-b"),new Document("id",4L).append("payload","extra"),
                                new Document("id",5L).append("payload","duplicate-a"),new Document("id",5L).append("payload","duplicate-b"),
                                new Document("id",6L).append("payload","wrong").append("extra",true),new Document("id",7L).append("payload","numeric"),
                                new Document("id",8L).append("payload","target-tail"),new Document("id",12L),new Document("id",13L).append("payload"," "));
                if(!count)documents.forEach(document->document.put("amount",document.getLong("id")==7?9007199254740992L:9007199254740993L));
                if(stringIdentifiers)documents.forEach(document->document.put("_id",new org.bson.types.ObjectId().toHexString()));
                target.getCollection("checked_orders").insertMany(documents);
                var item=new Document("id",9L).append("secondary",90L).append("alias_id",9L).append("alias_secondary",90L).append("payload",count?"item":"wrong");
                if(stringIdentifiers)item.put("_id",new org.bson.types.ObjectId().toHexString());
                target.getCollection("checked_items").insertOne(item);
                target.createCollection("checked_empty_orders");target.runCommand(new Document("profile",2));
                try{
                    var input=new SourceResource("input",null,kind,sourceSettings,SourceMode.SNAPSHOT,
                            List.of(TableRef.literal("orders"),TableRef.literal("empty_orders"),TableRef.literal("items")),null,null);
                    assertThat(f.configuration.artifacts().replace("input",CanonicalHash.of(f.configuration.artifacts().get("input").orElseThrow()),input)).isEqualTo(ArtifactMutation.REPLACED);
                    var dest=new SourceResource("dest",null,"mongodb",Map.of("uri",MONGO.getReplicaSetUrl(destination),"database",destination,"isUri",true),null,null,null,null);
                    assertThat(f.configuration.artifacts().create(dest)).isEqualTo(ArtifactMutation.CREATED);
                    var pipeline=new PipelineResource("flow",null,List.of(SourceRef.spec("input",false)),null,null,
                            new ServeBlock.Inline("serving",FromClause.list(FromRef.literal("input.orders"),FromRef.literal("input.empty_orders"),FromRef.literal("input.items")),
                                    List.of(new SyncElement("out","dest",null,new RenameSpec(null,null,"checked_",null),null)),null,null),null,null);
                    assertThat(f.configuration.artifacts().replace("flow",CanonicalHash.of(f.configuration.artifacts().get("flow").orElseThrow()),pipeline)).isEqualTo(ArtifactMutation.REPLACED);
                    var hashes=new TreeMap<String,String>();for(var resource:List.of(input,dest,pipeline))hashes.put(resource.id(),CanonicalHash.of(resource));
                    var mappings=new ArrayList<ValidationTaskSpec.Mapping>();
                    try(var query=java.util.concurrent.Executors.newSingleThreadExecutor();var decode=java.util.concurrent.Executors.newSingleThreadExecutor()){
                    for(var table:count?List.of("orders","empty_orders"):List.of("orders","items")){
                        var sourceTable=new ValidationTaskSpec.Table("input","input",DB,mysql?null:"public",table,null,null);
                        var targetTable=new ValidationTaskSpec.Table("out","dest",destination,null,"checked_"+table,null,null);
                        var fields=List.of(new ValidationKeyBinding.Component(List.of("id"),ValidationKey.Kind.EXACT_NUMBER,Set.of("exact-number")));
                        var targetFields=fields;
                        if(!count&&table.equals("items")){
                            fields=List.of(fields.getFirst(),new ValidationKeyBinding.Component(List.of("secondary"),ValidationKey.Kind.EXACT_NUMBER,Set.of("exact-number")));
                            targetFields=List.of(new ValidationKeyBinding.Component(List.of("alias_id"),ValidationKey.Kind.EXACT_NUMBER,Set.of("exact-number")),
                                    new ValidationKeyBinding.Component(List.of("alias_secondary"),ValidationKey.Kind.EXACT_NUMBER,Set.of("exact-number")));
                        }
                        if(!count){
                            sourceTable=bind(input,sourceArtifact,sourceTable,method,fields,query,decode);
                            targetTable=bind(dest,targetArtifact,targetTable,method,targetFields,query,decode);
                        }
                        mappings.add(new ValidationTaskSpec.Mapping("mapping_"+table,ValidationTaskSpec.OutputKind.SYNC,"out",sourceTable,targetTable,
                                count?null:new ValidationTaskSpec.Keys("fixture-confirmed",fields,targetFields)));
                    }
                    }
                    var comparison=new ValidationTaskSpec("flow",hashes,2,"rules",mappings);
                    assertThat(f.tasks.update(new ValidationTaskStore.Update(f.task,1,comparison,ValidationTaskStore.Settings.defaults(method,1000),UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.APPLIED);
                    var request=new ValidationOwnerRouter.Request("flow","task",f.task.generation(),2,1,"FULL",UUID.randomUUID(),UUID.randomUUID().toString());
                    var accepted=f.owners.reserve(request);assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
                    var spec=new ValidationRunSpec(comparison,ValidationMode.FULL,method);
                    var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),ValidationMode.FULL,spec.fingerprint(),comparison.mappingRevision(),"rules");
                    var start=new ValidationReportAdmission.Start(f.task,2,1,request.command().toString(),identity);var reservation=f.owners.take(start);
                    ConnectorProvisioner provisioner=id->{
                        if(id.equals(kind))return sourceArtifact;if(id.equals("mongodb"))return targetArtifact;
                        throw new AssertionError("a connector outside the saved closure was requested");
                    };
                    var factory=new NativeBusinessContext.Wiring().nativeValidationFullPlans(provisioner,Clock.systemUTC());
                    var actualPlan=new java.util.concurrent.atomic.AtomicReference<NativeValidationFullJob.Plan>();
                    runs.start(start,new NativeValidationFullJob(reservation,processors,f.member,f.process,runs,original->{
                        var plan=factory.prepare(original);actualPlan.set(plan);return plan;
                    }));
                    var stored=f.raw.getDatabase(f.cacheName).getCollection("validation_runs");
                    var exactRun=new Document("_id.kind","run").append("_id.clusterId",identity.owner().clusterId())
                            .append("_id.pipelineId",identity.owner().pipelineId()).append("_id.taskId",identity.taskId())
                            .append("_id.mode",identity.mode().name()).append("_id.runId",identity.runId());
                    var terminal=new java.util.concurrent.atomic.AtomicReference<Document>();
                    // Inspect this original fixture run even after its live owner has retired.
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(45)).until(()->{
                        var state=stored.find(exactRun).first();
                        if(state==null||!Boolean.TRUE.equals(state.getBoolean("retired"))||!ValidationRunState.valueOf(state.getString("state")).terminal())return false;
                        if(f.tasks.read("flow","task").orElseThrow().full().permission().phase()!=ValidationTaskStore.Phase.CLOSED)return false;
                        terminal.set(state);return true;
                    });
                    assertThat(terminal.get()).as("factory failure: %s",actualPlan.get()==null?null:actualPlan.get().failure())
                            .containsEntry("state",ValidationRunState.COMPLETED.name());
                    NativeValidationFullJobIT.await(()->f.resources.snapshot().metadataFrames()==0&&f.resources.snapshot().processReserved().isEmpty());
                    var report=f.raw.getDatabase(f.configName).getCollection("validation_reports").find().first();
                    assertThat(report).containsEntry("closed",true).containsEntry("rootCount",count?0L:method==ValidationMethod.UNIQUE_KEY?7L:13L);
                    if(count){
                    var summaries=report.getList("countSummaries",Document.class);assertThat(summaries).hasSize(2);
                    assertThat(summaries).anySatisfy(row->assertThat(row).containsEntry("mappingId","mapping_orders").containsEntry("sourceCount",3L).containsEntry("targetCount",2L));
                    assertThat(summaries).anySatisfy(row->assertThat(row).containsEntry("mappingId","mapping_empty_orders").containsEntry("sourceCount",0L).containsEntry("targetCount",0L));
                    assertThat(f.raw.getDatabase(f.configName).getCollection("validation_differences").countDocuments()).isZero();
                    }else{
                        var differences=f.raw.getDatabase(f.configName).getCollection("validation_differences").find().into(new ArrayList<>());
                        assertThat(differences).hasSize(method==ValidationMethod.UNIQUE_KEY?7:13);
                        assertThat(differences.toString()).contains("SOURCE_DUPLICATE","TARGET_DUPLICATE","TARGET_MISSING","TARGET_EXTRA");
                        if(method==ValidationMethod.UNIQUE_KEY)assertThat(differences.toString()).doesNotContain("TARGET_VALUE","TARGET_TYPE");
                        else{
                            assertThat(differences.toString()).contains("TARGET_VALUE","extra_only");
                            assertThat(root(differences,"mapping_orders",1).get("payload",Document.class).get("counts",Document.class))
                                    .containsExactlyInAnyOrderEntriesOf(stringIdentifiers?Map.of("TARGET_EXTRA",1L,"TARGET_TYPE",1L):Map.of("TARGET_EXTRA",1L));
                            assertThat(root(differences,"mapping_orders",7).get("payload",Document.class).get("counts",Document.class))
                                    .containsExactlyInAnyOrderEntriesOf(stringIdentifiers?Map.of("TARGET_VALUE",1L,"TARGET_TYPE",1L):Map.of("TARGET_VALUE",1L));
                            assertThat(root(differences,"mapping_orders",7).toString()).contains("9007199254740993","9007199254740992");
                            assertThat(root(differences,"mapping_orders",12).get("payload",Document.class).get("counts",Document.class))
                                    .containsExactlyInAnyOrderEntriesOf(stringIdentifiers?Map.of("TARGET_MISSING",1L,"TARGET_TYPE",1L):Map.of("TARGET_MISSING",1L));
                            assertThat(root(differences,"mapping_orders",13).get("payload",Document.class).get("counts",Document.class))
                                    .containsExactlyInAnyOrderEntriesOf(stringIdentifiers?Map.of("TARGET_VALUE",1L,"TARGET_TYPE",1L):Map.of("TARGET_VALUE",1L));
                            if(stringIdentifiers)assertThat(root(differences,"mapping_orders",1).get("payload",Document.class).getList("findings",Document.class))
                                    .anySatisfy(finding->{assertThat(finding).containsEntry("kind","TARGET_TYPE");
                                        assertThat(finding.get("location",Document.class)).containsEntry("fieldPath",List.of("_id"));});
                            var composite=root(differences,"mapping_items",9).get("payload",Document.class).getList("samples",Document.class).getFirst();
                            assertThat(composite.get("source",Document.class).get("identity",Document.class).get("keyFields"))
                                    .isEqualTo(List.of(List.of("id"),List.of("secondary")));
                            assertThat(composite.get("target",Document.class).get("identity",Document.class).get("keyFields"))
                                    .isEqualTo(List.of(List.of("alias_id"),List.of("alias_secondary")));
                        }
                        assertThat(root(differences,"mapping_orders",8).get("payload",Document.class).get("counts",Document.class)).containsEntry("TARGET_EXTRA",1L);
                        assertThat(root(differences,"mapping_items",11).get("payload",Document.class).get("counts",Document.class)).containsEntry("TARGET_MISSING",1L);
                        var duplicate=root(differences,"mapping_orders",5).get("payload",Document.class);
                        assertThat(duplicate.get("counts",Document.class)).containsEntry("SOURCE_DUPLICATE",1L).containsEntry("TARGET_DUPLICATE",1L)
                                .doesNotContainKeys("TARGET_VALUE","TARGET_TYPE");
                        assertThat(duplicate.getList("findings",Document.class)).hasSize(2).allSatisfy(finding->
                                assertThat(finding.get("multiplicity",Document.class)).containsEntry("occurrences",2L).containsEntry("exact",true));
                    }
                    assertThat(f.member.getJet().getJobs()).singleElement().satisfies(job->assertThat(job.getConfig().isAutoScaling()).isFalse());
                    var commands=target.getCollection("system.profile").find().into(new ArrayList<>()).stream().map(row->row.get("command",Document.class)).filter(Objects::nonNull).toList();
                    if(count){
                        assertThat(commands).filteredOn(command->command.containsKey("aggregate")).hasSize(2);
                        assertThat(commands).noneSatisfy(command->assertThat(command.containsKey("find")||command.containsKey("count")||command.toJson().contains("$sample")).isTrue());
                    }else assertThat(commands).anySatisfy(command->assertThat(command).containsKey("find"));
                }finally{target.drop();}
            }
        }
    }
    private static Document root(List<Document> differences,String mapping,long key){
        return differences.stream().filter(row->{
            var root=row.get("root",Document.class);if(!mapping.equals(root.getString("mappingId")))return false;
            var value=root.getList("key",Document.class).getFirst().get("value",Document.class);
            return new java.math.BigDecimal(new java.math.BigInteger(value.getString("unscaled")),value.getInteger("scale")).longValueExact()==key;
        }).findFirst().orElseThrow();
    }
    private static ValidationTaskSpec.Table bind(SourceResource resource,ConnectorRef artifact,ValidationTaskSpec.Table table,ValidationMethod method,
            List<ValidationKeyBinding.Component> fields,
            java.util.concurrent.ExecutorService query,java.util.concurrent.ExecutorService decode)throws Exception{
        try(var read=PdkValidationReads.inspect(new ConnectionConfig(resource.id(),resource.connector(),resource.config()),artifact,CanonicalHash.of(resource),
                table.table(),method==ValidationMethod.FULL_COLUMN?List.of():fields.stream().map(component->component.path().getFirst()).toList(),()->{},query,decode,new ValidationValues.Limits(64*1024,8192,32))){
            var metadata=read.result();read.close();read.workersExited().toCompletableFuture().get(5,java.util.concurrent.TimeUnit.SECONDS);
            read.rowsReleased().toCompletableFuture().get(5,java.util.concurrent.TimeUnit.SECONDS);
            return new ValidationTaskSpec.Table(table.nodeId(),table.sourceId(),table.database(),table.schema(),table.table(),
                    metadata.binding().modelVersion(),metadata.binding().codecBinding());
        }
    }
    static ConnectorRef artifact(String kind)throws Exception{
        var configured=System.getProperty("tapstate.pdk.it."+kind+"-jar");assertThat(configured).as("real connector artifact for %s",kind).isNotBlank();
        var path=Path.of(configured);var hash=MessageDigest.getInstance("SHA-256");
        try(var input=Files.newInputStream(path)){var buffer=new byte[8192];for(int read;(read=input.read(buffer))!=-1;)hash.update(buffer,0,read);}
        var metadata=new ConnectorIntrospector().introspect(List.of(path));
        return new ConnectorRef(List.of(path),metadata.className(),metadata.pdkApiVersion(),null,metadata.spec(),HexFormat.of().formatHex(hash.digest()),false);
    }
}
