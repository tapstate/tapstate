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
            var target=new ConnectionConfig("target","mongodb",Map.of("uri",mongo.getReplicaSetUrl(DB),"database",DB,"isUri",true));
            try(var observer=MongoClients.create(mongo.getReplicaSetUrl(DB))) {
                var database=observer.getDatabase(DB);database.getCollection("records").insertMany(List.of(new Document("_id",1),new Document("_id",2)));
                database.createCollection("empty_records");database.runCommand(new Document("profile",2));
                assertThat(count(source,sourceRef,"records",PdkValidationCounts.Operation.PARTITION_FILTER)).isEqualTo(3);
                assertThat(count(target,targetRef,"records",PdkValidationCounts.Operation.DOCUMENT_COUNT)).isEqualTo(2);
                assertThat(count(source,sourceRef,"empty_records",PdkValidationCounts.Operation.PARTITION_FILTER)).isZero();
                assertThat(count(target,targetRef,"empty_records",PdkValidationCounts.Operation.DOCUMENT_COUNT)).isZero();
                assertThatThrownBy(()->count(source,sourceRef,"missing_records",PdkValidationCounts.Operation.PARTITION_FILTER)).isInstanceOf(TapstateException.class);
                assertThatThrownBy(()->count(target,targetRef,"missing_records",PdkValidationCounts.Operation.DOCUMENT_COUNT)).isInstanceOf(TapstateException.class);
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
    private static ConnectorRef artifact(String kind) {
        var configured=System.getProperty("tapstate.pdk.it."+kind+"-jar");
        assertThat(configured).as("real connector artifact for %s",kind).isNotBlank();
        var paths=List.of(Path.of(configured));var inspected=new ConnectorIntrospector().introspect(paths);
        return new ConnectorRef(paths,inspected.className(),inspected.pdkApiVersion(),null,inspected.spec());
    }
}
