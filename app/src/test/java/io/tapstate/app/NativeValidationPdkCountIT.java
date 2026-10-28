package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.ConnectorRef;
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
    @Test @Timeout(180) void mysqlSavedMappingsRunThroughTheProductionCountFactory()throws Exception{verify(true);}
    @Test @Timeout(180) void postgresSavedMappingsRunThroughTheProductionCountFactory()throws Exception{verify(false);}
    private void verify(boolean mysql)throws Exception{
        var kind=mysql?"mysql":"postgres";var sourceArtifact=artifact(kind);var targetArtifact=artifact("mongodb");int port=mysql?3306:5432;
        try(var sql=new GenericContainer<>(DockerImageName.parse(mysql?"mysql:8.0":"postgres:16")).withExposedPorts(port).withStartupTimeout(Duration.ofSeconds(75))){
            if(mysql)sql.withEnv("MYSQL_ROOT_PASSWORD",PASSWORD).withEnv("MYSQL_DATABASE",DB).withEnv("MYSQL_USER","validation")
                    .withEnv("MYSQL_PASSWORD",PASSWORD).withCommand("--innodb-buffer-pool-size=64M","--max-connections=20")
                    .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections\\..*port: 3306.*\\n",1));
            else sql.withEnv("POSTGRES_PASSWORD",PASSWORD).withEnv("POSTGRES_USER","validation").withEnv("POSTGRES_DB",DB)
                    .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n",2));
            sql.start();var statement="CREATE TABLE orders (id BIGINT PRIMARY KEY); CREATE TABLE empty_orders (id BIGINT PRIMARY KEY); INSERT INTO orders VALUES (1),(2),(3);";
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
                target.getCollection("checked_orders").insertMany(List.of(new Document("_id",1),new Document("_id",2)));
                target.createCollection("checked_empty_orders");target.runCommand(new Document("profile",2));
                try{
                    var input=new SourceResource("input",null,kind,sourceSettings,SourceMode.SNAPSHOT,
                            List.of(TableRef.literal("orders"),TableRef.literal("empty_orders")),null,null);
                    assertThat(f.configuration.artifacts().replace("input",CanonicalHash.of(f.configuration.artifacts().get("input").orElseThrow()),input)).isEqualTo(ArtifactMutation.REPLACED);
                    var dest=new SourceResource("dest",null,"mongodb",Map.of("uri",MONGO.getReplicaSetUrl(destination),"database",destination,"isUri",true),null,null,null,null);
                    assertThat(f.configuration.artifacts().create(dest)).isEqualTo(ArtifactMutation.CREATED);
                    var pipeline=new PipelineResource("flow",null,List.of(SourceRef.spec("input",false)),null,null,
                            new ServeBlock.Inline("serving",FromClause.list(FromRef.literal("input.orders"),FromRef.literal("input.empty_orders")),
                                    List.of(new SyncElement("out","dest",null,new RenameSpec(null,null,"checked_",null),null)),null,null),null,null);
                    assertThat(f.configuration.artifacts().replace("flow",CanonicalHash.of(f.configuration.artifacts().get("flow").orElseThrow()),pipeline)).isEqualTo(ArtifactMutation.REPLACED);
                    var hashes=new TreeMap<String,String>();for(var resource:List.of(input,dest,pipeline))hashes.put(resource.id(),CanonicalHash.of(resource));
                    var mappings=new ArrayList<ValidationTaskSpec.Mapping>();
                    for(var table:List.of("orders","empty_orders"))mappings.add(new ValidationTaskSpec.Mapping("mapping_"+table,ValidationTaskSpec.OutputKind.SYNC,"out",
                            new ValidationTaskSpec.Table("input","input",DB,mysql?null:"public",table,null,null),
                            new ValidationTaskSpec.Table("out","dest",destination,null,"checked_"+table,null,null),null));
                    var comparison=new ValidationTaskSpec("flow",hashes,2,"rules",mappings);
                    assertThat(f.tasks.update(new ValidationTaskStore.Update(f.task,1,comparison,ValidationTaskStore.Settings.defaults(ValidationMethod.COUNT,1000),UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.APPLIED);
                    var request=new ValidationOwnerRouter.Request("flow","task",f.task.generation(),2,1,"FULL",UUID.randomUUID(),UUID.randomUUID().toString());
                    var accepted=f.owners.reserve(request);assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
                    var spec=new ValidationRunSpec(comparison,ValidationMode.FULL,ValidationMethod.COUNT);
                    var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),ValidationMode.FULL,spec.fingerprint(),comparison.mappingRevision(),"rules");
                    var start=new ValidationReportAdmission.Start(f.task,2,1,request.command().toString(),identity);var reservation=f.owners.take(start);
                    ConnectorProvisioner provisioner=id->{
                        if(id.equals(kind))return sourceArtifact;if(id.equals("mongodb"))return targetArtifact;
                        throw new AssertionError("a connector outside the saved closure was requested");
                    };
                    var factory=new NativeBusinessContext.Wiring().nativeValidationFullPlans(provisioner,Clock.systemUTC());
                    runs.start(start,new NativeValidationFullJob(reservation,processors,f.member,f.process,runs,factory));
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
                    assertThat(terminal.get()).containsEntry("state",ValidationRunState.COMPLETED.name());
                    NativeValidationFullJobIT.await(()->f.resources.snapshot().metadataFrames()==0&&f.resources.snapshot().processReserved().isEmpty());
                    var report=f.raw.getDatabase(f.configName).getCollection("validation_reports").find().first();
                    assertThat(report).containsEntry("closed",true).containsEntry("rootCount",0L);
                    var summaries=report.getList("countSummaries",Document.class);assertThat(summaries).hasSize(2);
                    assertThat(summaries).anySatisfy(row->assertThat(row).containsEntry("mappingId","mapping_orders").containsEntry("sourceCount",3L).containsEntry("targetCount",2L));
                    assertThat(summaries).anySatisfy(row->assertThat(row).containsEntry("mappingId","mapping_empty_orders").containsEntry("sourceCount",0L).containsEntry("targetCount",0L));
                    assertThat(f.raw.getDatabase(f.configName).getCollection("validation_differences").countDocuments()).isZero();
                    assertThat(f.member.getJet().getJobs()).singleElement().satisfies(job->assertThat(job.getConfig().isAutoScaling()).isFalse());
                    var commands=target.getCollection("system.profile").find().into(new ArrayList<>()).stream().map(row->row.get("command",Document.class)).filter(Objects::nonNull).toList();
                    assertThat(commands).filteredOn(command->command.containsKey("aggregate")).hasSize(2);
                    assertThat(commands).noneSatisfy(command->assertThat(command.containsKey("find")||command.containsKey("count")||command.toJson().contains("$sample")).isTrue());
                }finally{target.drop();}
            }
        }
    }
    private static ConnectorRef artifact(String kind)throws Exception{
        var configured=System.getProperty("tapstate.pdk.it."+kind+"-jar");assertThat(configured).as("real connector artifact for %s",kind).isNotBlank();
        var path=Path.of(configured);var hash=MessageDigest.getInstance("SHA-256");
        try(var input=Files.newInputStream(path)){var buffer=new byte[8192];for(int read;(read=input.read(buffer))!=-1;)hash.update(buffer,0,read);}
        var metadata=new ConnectorIntrospector().introspect(List.of(path));
        return new ConnectorRef(List.of(path),metadata.className(),metadata.pdkApiVersion(),null,metadata.spec(),HexFormat.of().formatHex(hash.digest()),false);
    }
}
