package io.tapstate.adapters.pdk;

import com.mongodb.client.MongoClients;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Real scalar gateway reads, independently of a pipeline, Job or production acceptance environment. */
@RequiresDocker
class ValidationNativeCountGatewayIT {
    private static final String DB="validation_counts", PASSWORD="count-fixture-only";
    @Test @Timeout(120) void mysqlAndMongoUseExactNativeCountsWithoutValueDiscovery()throws Exception { verify(true); }
    @Test @Timeout(120) void postgresAndMongoUseExactNativeCountsWithoutValueDiscovery()throws Exception { verify(false); }
    @Test @Timeout(60) void nativeIdentifierKeysRequireTheirOwnConfirmedDomain()throws Exception{
        var artifact=artifact("mongodb");var identifier=new org.bson.types.ObjectId("0123456789abcdef01234567");
        var limits=new io.tapstate.core.validation.ValidationValues.Limits(64*1024,8192,32);
        try(var mongo=new MongoDBContainer(DockerImageName.parse("mongo:7.0"))){
            mongo.start();var settings=Map.<String,Object>of("uri",mongo.getReplicaSetUrl(DB),"database",DB,"isUri",true);
            try(var client=MongoClients.create(mongo.getReplicaSetUrl(DB))){
                client.getDatabase(DB).getCollection("identifiers").insertOne(new Document("_id",identifier).append("business_text",identifier.toHexString()));
            }
            var connection=new ConnectionConfig("target","document-alias",settings);
            var resource=new io.tapstate.core.model.SourceResource("target",null,connection.connectorId(),settings,null,null,null,null);
            var configurationHash=io.tapstate.core.model.canonical.CanonicalHash.of(resource);
            var idKeys=List.of(new io.tapstate.core.validation.ValidationKeyBinding.Component(List.of("_id"),
                    io.tapstate.core.validation.ValidationKey.Kind.TEXT,Set.of("object-id")));
            var textKeys=List.of(new io.tapstate.core.validation.ValidationKeyBinding.Component(List.of("_id"),
                    io.tapstate.core.validation.ValidationKey.Kind.TEXT,Set.of("text")));
            try(var query=java.util.concurrent.Executors.newSingleThreadExecutor();var decode=java.util.concurrent.Executors.newSingleThreadExecutor()){
                final PdkValidationReads.Binding binding;
                try(var metadata=PdkValidationReads.inspect(connection,artifact,configurationHash,"identifiers",List.of("_id"),()->{},query,decode,limits)){
                    binding=metadata.result().binding();
                }
                try(var reader=PdkValidationReads.scan(connection,artifact,configurationHash,"identifiers",binding,idKeys,false,()->{},query,decode,2,limits)){
                    try(var row=reader.next().orElseThrow()){
                        assertThat((io.tapstate.core.validation.ValidationValues.Typed)row.values().fields().get("_id"))
                                .satisfies(value->{assertThat(value.logicalType()).isEqualTo("object-id");assertThat(value.value()).isEqualTo(identifier.toHexString());});
                    }
                    assertThat(reader.next()).isEmpty();
                }
                try(var reader=PdkValidationReads.scan(connection,artifact,configurationHash,"identifiers",binding,textKeys,false,()->{},query,decode,2,limits)){
                    assertThatThrownBy(reader::next).isInstanceOfSatisfying(TapstateException.class,
                            failure->assertThat(failure.code()).isEqualTo(io.tapstate.core.validation.ValidationError.RULE_UNSUPPORTED));
                }
            }
        }
    }
    @Test @Timeout(120) void stableDocumentMetadataHasNoSamplingOrderDependentBinding()throws Exception{
        var artifact=artifact("mongodb");var limits=new io.tapstate.core.validation.ValidationValues.Limits(64*1024,8192,32);
        try(var mongo=new MongoDBContainer(DockerImageName.parse("mongo:7.0"))){
            mongo.start();var settings=Map.<String,Object>of("uri",mongo.getReplicaSetUrl(DB),"database",DB,"isUri",true);
            try(var client=MongoClients.create(mongo.getReplicaSetUrl(DB))){
                var rows=new ArrayList<Document>();
                for(int index=1;index<=7;index++)rows.add(new Document("id",(long)index).append("payload","same"));
                rows.get(5).append("extra",true);client.getDatabase(DB).getCollection("records").insertMany(rows);
            }
            Object baseline=null;
            try(var query=java.util.concurrent.Executors.newSingleThreadExecutor();var decode=java.util.concurrent.Executors.newSingleThreadExecutor()){
                for(int attempt=0;attempt<20;attempt++){
                    var captured=new java.util.concurrent.atomic.AtomicReference<Object>();
                    try(var reader=PdkValidationSession.inspect("mongodb",artifact,settings,"records",List.of(),
                            model->captured.set(PdkValidationModelVersion.canonicalModel(model,artifact)),()->{},query,decode,limits)){
                        assertThat(reader.next()).isEmpty();
                    }
                    if(baseline==null)baseline=captured.get();
                    var paths=new ArrayList<String>();changedPaths(baseline,captured.get(),"model",paths);
                    assertThat(paths).as("canonical property paths changed; no metadata values are logged").isEmpty();
                    if(attempt==0)try(var flush=MongoClients.create(mongo.getReplicaSetUrl(DB))){
                        flush.getDatabase("admin").runCommand(new Document("fsync",1));
                    }
                }
            }
        }
    }
    private static void changedPaths(Object first,Object second,String path,List<String> changed){
        if(first instanceof Map<?,?> left&&second instanceof Map<?,?> right){
            var keys=new TreeSet<String>();left.keySet().forEach(key->keys.add(key.toString()));right.keySet().forEach(key->keys.add(key.toString()));
            for(var key:keys)changedPaths(left.get(key),right.get(key),path+"/"+key,changed);
        }else if(first instanceof List<?> left&&second instanceof List<?> right){
            if(left.size()!=right.size()){changed.add(path+"/length");return;}
            for(int index=0;index<left.size();index++)changedPaths(left.get(index),right.get(index),path+"/"+index,changed);
        }else if(!Objects.equals(first,second))changed.add(path);
    }
    private void verify(boolean mysql)throws Exception {
        var kind=mysql?"mysql":"postgres";var sourceRef=artifact(kind);var targetRef=artifact("mongodb");int port=mysql?3306:5432;
        try(var sql=new GenericContainer<>(DockerImageName.parse(mysql?"mysql:8.0":"postgres:16"))
                    .withExposedPorts(port).withStartupTimeout(Duration.ofSeconds(75));
            var mongo=new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            if(mysql)sql.withEnv("MYSQL_ROOT_PASSWORD",PASSWORD).withEnv("MYSQL_DATABASE",DB).withEnv("MYSQL_USER","validation")
                    .withEnv("MYSQL_PASSWORD",PASSWORD).withCommand("--innodb-buffer-pool-size=64M","--max-connections=20")
                    .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections\\..*port: 3306.*\\n",1));
            else sql.withEnv("POSTGRES_PASSWORD",PASSWORD).withEnv("POSTGRES_USER","validation").withEnv("POSTGRES_DB",DB)
                    .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n",2));
            sql.start();mongo.start();
            var statement="CREATE TABLE records (id BIGINT PRIMARY KEY); CREATE TABLE empty_records (id BIGINT PRIMARY KEY); INSERT INTO records VALUES (1),(2),(3);";
            var seeded=mysql?sql.execInContainer("mysql","--protocol=TCP","--host=127.0.0.1","--user=validation","--password="+PASSWORD,"--batch",DB,"-e",statement)
                    :sql.execInContainer("psql","-U","validation","-d",DB,"-c",statement);
            assertThat(seeded.getExitCode()).as(seeded.getStderr()).isZero();
            var settings=new LinkedHashMap<String,Object>();settings.put("host",sql.getHost());settings.put("port",sql.getMappedPort(port));
            settings.put("database",DB);settings.put(mysql?"username":"user","validation");settings.put("password",PASSWORD);
            settings.put("timezone","+00:00");settings.put("oldVersionTimezone",false);
            if(mysql)settings.put("addtionalString","useSSL=false&allowPublicKeyRetrieval=true");else settings.put("schema","public");
            var source=new ConnectionConfig("source",kind,settings);
            var target=new ConnectionConfig("target","independent-document-alias",Map.of("uri",mongo.getReplicaSetUrl(DB),"database",DB,"isUri",true));
            try(var observer=MongoClients.create(mongo.getReplicaSetUrl(DB))) {
                var database=observer.getDatabase(DB);database.getCollection("records").insertMany(List.of(new Document("_id",1),new Document("_id",2)));
                database.createCollection("empty_records");database.runCommand(new Document("profile",2));
                assertThat(count(source,sourceRef,"records",PdkValidationCounts.Operation.NATIVE_PROTOCOL)).isEqualTo(3);
                assertThat(count(target,targetRef,"records",PdkValidationCounts.Operation.NATIVE_PROTOCOL)).isEqualTo(2);
                assertThat(count(source,sourceRef,"empty_records",PdkValidationCounts.Operation.NATIVE_PROTOCOL)).isZero();
                assertThat(count(target,targetRef,"empty_records",PdkValidationCounts.Operation.NATIVE_PROTOCOL)).isZero();
                assertThatThrownBy(()->count(source,sourceRef,"missing_records",PdkValidationCounts.Operation.NATIVE_PROTOCOL)).isInstanceOf(TapstateException.class);
                assertThatThrownBy(()->count(target,targetRef,"missing_records",PdkValidationCounts.Operation.NATIVE_PROTOCOL)).isInstanceOf(TapstateException.class);
                var commands=database.getCollection("system.profile").find().into(new ArrayList<>()).stream()
                        .map(row->row.get("command",Document.class)).filter(Objects::nonNull).toList();
                assertThat(commands).filteredOn(command->"records".equals(command.get("aggregate"))||"empty_records".equals(command.get("aggregate")))
                        .hasSize(2).allSatisfy(command->assertThat(command.getList("pipeline",Document.class)).anySatisfy(stage->assertThat(stage).containsKey("$group")));
                assertThat(commands).noneSatisfy(command->{
                    assertThat(command.containsKey("find")||command.containsKey("count")||command.toJson().contains("$sample")).isTrue();
                });
            }
        }
    }
    private static long count(ConnectionConfig connection,ConnectorRef artifact,String table,PdkValidationCounts.Operation operation) {
        try(var count=new PdkValidationCounts(connection,artifact,new PdkValidationCounts.Request(table,DB,operation),()->{})) {
            long value=count.count();assertThat(count.drained()).isTrue();assertThat(count.callExited().toCompletableFuture()).isCompleted();return value;
        }
    }
    private static ConnectorRef artifact(String kind)throws Exception {
        var configured=System.getProperty("tapstate.pdk.it."+kind+"-jar");
        assertThat(configured).as("real connector artifact for %s",kind).isNotBlank();
        var paths=List.of(Path.of(configured));var inspected=new ConnectorIntrospector().introspect(paths);
        var digest=java.security.MessageDigest.getInstance("SHA-256");
        try(var input=java.nio.file.Files.newInputStream(paths.getFirst())){var buffer=new byte[8192];for(int read;(read=input.read(buffer))!=-1;)digest.update(buffer,0,read);}
        return new ConnectorRef(paths,inspected.className(),inspected.pdkApiVersion(),null,inspected.spec(),HexFormat.of().formatHex(digest.digest()),false);
    }
}
