package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.pdk.ConnectorArtifactRegistrar;
import io.tapstate.adapters.pdk.LocalConnectorArtifacts;
import io.tapstate.control.core.*;
import io.tapstate.core.model.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
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

    @Test
    @Timeout(180)
    void mysqlCustomerBalanceCanBeValidatedAfterCopyToAtlas() throws Exception {
        String table = "bmsql_customer";
        String ddl = "CREATE TABLE bmsql_customer (c_w_id INT NOT NULL, c_d_id INT NOT NULL, "
                + "c_id INT NOT NULL, c_balance DECIMAL(12,2) NOT NULL, PRIMARY KEY (c_w_id,c_d_id,c_id))";
        var seeds = Files.createDirectory(work.resolve("seeds"));
        for (var connector : List.of("mysql", "mongodb-atlas")) {
            Files.copy(artifact(connector), seeds.resolve(connector + "-connector.jar"));
        }
        try (var mysql = new GenericContainer<>("mysql:8.0").withExposedPorts(3306)
                .withEnv("MYSQL_ROOT_PASSWORD", PASSWORD).withEnv("MYSQL_DATABASE", DB)
                .withCommand("--innodb-buffer-pool-size=64M", "--max-connections=20")
                .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections\\..*port: 3306.*\\n", 1))
                .withStartupTimeout(Duration.ofSeconds(75))) {
            mysql.start();
            var seed = mysql.execInContainer("mysql", "--protocol=TCP", "--host=127.0.0.1", "--user=root",
                    "--password=" + PASSWORD, DB, "-e", ddl + "; INSERT INTO bmsql_customer VALUES (1,1,1,-10.00)");
            assertThat(seed.getExitCode()).as(seed.getStderr()).isZero();
            var inputSettings = Map.<String, Object>of("host", mysql.getHost(), "port", mysql.getMappedPort(3306),
                    "database", DB, "username", "root", "password", PASSWORD, "timezone", "+00:00",
                    "oldVersionTimezone", false, "addtionalString", "useSSL=false&allowPublicKeyRetrieval=true");
            var storage = NativeAppStorage.createOnPremWithAdmin(MONGO, "customer_balance", "admin", PASSWORD);
            try (var raw = MongoClients.create(storage.configurationUri())) {
                try (var schema = io.tapstate.adapters.mongostore.MongoConfigurationDomain.prepareValidationSchema(
                        new io.tapstate.adapters.mongostore.MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5)),
                        storage.clusterId(), UUID.randomUUID());
                        var cache = io.tapstate.adapters.mongostore.MongoCacheDomain.connect(
                                new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                                        storage.cacheUri(), storage.cacheDatabase(), null),
                                Duration.ofSeconds(5), "customer-balance-fixture")) {
                    schema.prepareCache(cache);
                    while (!schema.advance(20).ready()) {}
                }
                var collection = raw.getDatabase(storage.masterDatabase()).getCollection(table);
                try (var app = new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                        "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR",
                        "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                        "--tapstate.store.mongo.uri=" + storage.configurationUri(),
                        "--tapstate.connectors.seed-dir=" + seeds,
                        "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"),
                        "--tapstate.control.auth.jwt-secret=customer-balance-http-signing-key")) {
                    NativeAppStorage.awaitBusiness(app);
                    var child = NativeAppStorage.business(app);
                    var artifacts = child.getBean(ArtifactStore.class);
                    assertThat(artifacts.create(new SourceResource("input", null, "mysql", inputSettings,
                            SourceMode.SNAPSHOT, List.of(TableRef.literal(table)), null, null))).isEqualTo(ArtifactMutation.CREATED);
                    assertThat(artifacts.create(new SourceResource("target", null, "mongodb-atlas",
                            Map.of("uri", storage.masterUri(), "database", storage.masterDatabase(), "isUri", true),
                            null, null, null, null))).isEqualTo(ArtifactMutation.CREATED);
                    assertThat(artifacts.create(new PipelineResource("balance_flow", null, List.of(SourceRef.spec("input", false)),
                            null, null, new ServeBlock.Inline("serve", FromClause.list(FromRef.literal("input." + table)),
                            List.of(new SyncElement("out", "target", WriteMode.UPSERT, null, null)), null, null),
                            null, null))).isEqualTo(ArtifactMutation.CREATED);
                    var tokens = child.getBean(TokenService.class);
                    String write = tokens.create(Scope.WRITE), read = tokens.create(Scope.READ);
                    var http = RestClient.create("http://127.0.0.1:" + ((WebServerApplicationContext) app).getWebServer().getPort());
                    http.post().uri("/api/connections:discover-schema").header("Authorization", "Bearer " + write)
                            .contentType(MediaType.APPLICATION_JSON).body(Map.of(
                                    "id", "input", "connectorId", "mysql", "settings", inputSettings))
                            .retrieve().toBodilessEntity();
                    http.post().uri("/api/pipelines/balance_flow:start").header("Authorization", "Bearer " + write)
                            .retrieve().toBodilessEntity();
                    try {
                        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                                assertThat(collection.find().first()).isNotNull().containsKey("c_balance"));
                    } catch (org.awaitility.core.ConditionTimeoutException timedOut) {
                        throw new AssertionError("Copy did not write the customer row. Pipeline status: "
                                + get(http, read, "/api/pipelines/balance_flow/status"), timedOut);
                    }

                    var selection = Map.of("pipelineId", "balance_flow", "taskId", "balance", "expectedRevision", "0",
                            "mappings", List.of(Map.of("mappingId", "customer", "outputId", "out", "sourceId", "input", "table", table)));
                    var input = new LinkedHashMap<String, Object>(selection);
                    input.put("method", "UNIQUE_KEY");
                    input.put("confirmations", Map.of());
                    var models = post(http, write, "evaluate", input);
                    input.put("method", "FULL_COLUMN");
                    var fields = List.of("c_w_id", "c_d_id", "c_id").stream()
                            .map(field -> Map.of("source", field, "target", field)).toList();
                    input.put("confirmations", Map.of("customer", Map.of("id", UUID.randomUUID().toString(), "fields", fields)));
                    String taskPath = "/api/pipelines/balance_flow/validation-tasks/balance";
                    Map<String, Object> evaluated;
                    try {
                        evaluated = post(http, write, "evaluate", input);
                    } catch (RestClientResponseException refused) {
                        int taskStatus = http.get().uri(taskPath).header("Authorization", "Bearer " + read)
                                .exchange((request, response) -> response.getStatusCode().value());
                        var connectors = child.getBean(io.tapstate.adapters.pdk.ConnectorProvisioner.class);
                        throw new AssertionError("Expected FULL_COLUMN evaluation of the copied customer balance. Runtime: "
                                + get(http, read, "/version") + "; MySQL SHA-256 " + connectors.resolve("mysql").contentHash()
                                + "; Atlas SHA-256 " + connectors.resolve("mongodb-atlas").contentHash() + "; DDL: " + ddl
                                + "; BSON: " + collection.find().first().toJson() + "; live models: " + models
                                + "; evaluate HTTP " + refused.getStatusCode().value() + ": " + refused.getResponseBodyAsString()
                                + "; save was not reached; task reload HTTP " + taskStatus, refused);
                    }
                    assertThat(evaluated.get("fingerprint")).isInstanceOf(String.class);
                    var saved = post(http, write, "save", Map.of("input", input, "fingerprint", evaluated.get("fingerprint"),
                            "commandId", UUID.randomUUID().toString(), "historyLimit", 10, "scheduleEnabled", false));
                    assertThat(saved).containsEntry("outcome", "APPLIED");
                    var task = get(http, read, taskPath);
                    assertThat(task).containsEntry("revision", "1").containsEntry("taskGeneration", saved.get("taskGeneration"));
                    assertThat(collection.updateOne(new Document("c_id", 1), new Document("$set",
                            new Document("c_balance", new Decimal128(new BigDecimal("-11.00"))))).getModifiedCount()).isEqualTo(1);
                    var report = run(http, write, read, Map.of("context", task.get("context"), "pipeline", "balance_flow",
                            "task", "balance", "taskGeneration", task.get("taskGeneration"), "revision", task.get("revision"),
                            "sequence", "1", "command", UUID.randomUUID().toString()));
                    assertThat(report).containsEntry("result", "COMPLETE_WITH_OBSERVATIONS").containsEntry("rootCount", "1");
                    var reference = (Map<?, ?>) report.get("reference");
                    String rootsPath = taskPath + "/reports/" + reference.get("run") + "/roots";
                    String query = "?generation=" + reference.get("generation") + "&sequence=" + reference.get("sequence");
                    var roots = get(http, read, rootsPath + query);
                    assertThat((List<Map>) roots.get("items")).singleElement().satisfies(root -> {
                        var detail = get(http, read, rootsPath + "/" + root.get("id") + query);
                        var observation = (Map<?, ?>) detail.get("observation");
                        assertThat((List<Map>) reportField(observation, "samples").get("value")).anySatisfy(sample -> {
                            var finding = reportField(sample, "finding");
                            assertThat(reportField(finding, "kind").get("value")).isEqualTo("TARGET_VALUE");
                            var path = reportField(reportField(finding, "location"), "fieldPath");
                            assertThat((List<Map>) path.get("value")).extracting(part -> part.get("value"))
                                    .containsExactly("c_balance");
                            assertBalanceSample(reportField(sample, "source"), new BigDecimal("-10.00"));
                            assertBalanceSample(reportField(sample, "target"), new BigDecimal("-11.00"));
                        });
                    });
                } finally {
                    for (String database : List.of(storage.configurationDatabase(), storage.cacheDatabase(), storage.masterDatabase())) {
                        raw.getDatabase(database).drop();
                    }
                }
            }
        }
    }

    private static Map<?, ?> reportField(Map<?, ?> object, String name) {
        var fields = (List<Map<?, ?>>) object.get("value");
        return fields.stream().filter(field -> name.equals(field.get("name")))
                .map(field -> (Map<?, ?>) field.get("value"))
                .findFirst().orElseThrow(() -> new AssertionError("Missing report field " + name));
    }

    private static void assertBalanceSample(Map<?, ?> side, BigDecimal expected) {
        var row = reportField(side, "row");
        var entries = (List<Map<?, ?>>) reportField(row, "entries").get("value");
        assertThat(entries).anySatisfy(entry -> {
            assertThat(reportField(entry, "name").get("value")).isEqualTo("c_balance");
            var balance = reportField(reportField(entry, "value"), "value");
            assertThat(reportField(balance, "kind").get("value")).isEqualTo("DECIMAL");
            assertThat(reportField(balance, "scale").get("value")).isEqualTo(Integer.toString(expected.scale()));
            assertThat(reportField(balance, "unscaled").get("value")).isEqualTo(expected.unscaledValue().toString());
        });
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings={"UNIQUE_KEY","FULL_COLUMN"})
    @Timeout(180) void smallPostgresPairsFitTheOriginalProcessCapacityAndReadBothSides(String method)throws Exception{
        var seeds = Files.createDirectory(work.resolve("seeds"));
        Files.copy(artifact("postgres"), seeds.resolve("postgres-connector.jar"));
        try(var sql=new GenericContainer<>("postgres:16").withExposedPorts(5432).withEnv("POSTGRES_PASSWORD",PASSWORD)
                .withEnv("POSTGRES_USER","validation").withEnv("POSTGRES_DB",DB).withStartupTimeout(Duration.ofSeconds(75))
                .withCommand("postgres","-c","log_statement=all")
                .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n",2))){
            sql.start();var seed=sql.execInContainer("psql","-U","validation","-d",DB,"-c",
                    "CREATE TABLE orders(id BIGINT PRIMARY KEY,payload VARCHAR(32)); INSERT INTO orders VALUES(1,'same'); "
                    +"CREATE TABLE checked_orders(id BIGINT PRIMARY KEY,payload VARCHAR(32)); INSERT INTO checked_orders VALUES(1,'same');");
            assertThat(seed.getExitCode()).as(seed.getStderr()).isZero();
            var connection=Map.<String,Object>of("host",sql.getHost(),"port",sql.getMappedPort(5432),"database",DB,"schema","public","user","validation","password",PASSWORD);
            var storage=NativeAppStorage.createOnPremWithAdmin(MONGO,"postgres_pair_http","admin",PASSWORD);
            try(var schema=io.tapstate.adapters.mongostore.MongoConfigurationDomain.prepareValidationSchema(
                    new io.tapstate.adapters.mongostore.MongoConnectionSettings(storage.configurationUri(),null,Duration.ofSeconds(5)),storage.clusterId(),UUID.randomUUID());
                    var cache=io.tapstate.adapters.mongostore.MongoCacheDomain.connect(new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,storage.cacheUri(),storage.cacheDatabase(),null),Duration.ofSeconds(5),"postgres-pair-fixture")){
                schema.prepareCache(cache);while(!schema.advance(20).ready()){}
            }
            try(var app=new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                    "--server.address=127.0.0.1","--server.port=0","--logging.level.root=ERROR","--logging.level.io.tapstate.app.NativeValidationOwnerReservations=WARN","--tapstate.hz.member-port=0","--tapstate.hz.jet.cooperative-thread-count=2",
                    "--tapstate.store.mongo.uri="+storage.configurationUri(),"--tapstate.connectors.seed-dir="+seeds,
                    "--tapstate.connectors.plugins-dir="+work.resolve("plugins"),"--tapstate.control.auth.jwt-secret=postgres-pair-http-signing-key")){
                NativeAppStorage.awaitBusiness(app);var child=NativeAppStorage.business(app);
                var connector="postgres";
                var artifacts=child.getBean(ArtifactStore.class);
                assertThat(artifacts.create(new SourceResource("input",null,connector,connection,SourceMode.SNAPSHOT,List.of(TableRef.literal("orders")),null,null))).isEqualTo(ArtifactMutation.CREATED);
                assertThat(artifacts.create(new SourceResource("target",null,connector,connection,null,null,null,null))).isEqualTo(ArtifactMutation.CREATED);
                assertThat(artifacts.create(new PipelineResource("flow",null,List.of(SourceRef.spec("input",false)),null,null,
                        new ServeBlock.Inline("serve",FromClause.list(FromRef.literal("input.orders")),List.of(new SyncElement("out","target",null,new RenameSpec(null,null,"checked_",null),null)),null,null),null,null))).isEqualTo(ArtifactMutation.CREATED);
                var tokens=child.getBean(TokenService.class);String write=tokens.create(Scope.WRITE),read=tokens.create(Scope.READ);
                var http=RestClient.create("http://127.0.0.1:"+((WebServerApplicationContext)app).getWebServer().getPort());
                var input=Map.of("pipelineId","flow","taskId","pair","expectedRevision","0","method",method,
                        "mappings",List.of(Map.of("mappingId","orders","outputId","out","sourceId","input","table","orders")),
                        "confirmations",Map.of("orders",Map.of("id",UUID.randomUUID().toString(),"fields",List.of(Map.of("source","id","target","id")))));
                var evaluated=post(http,write,"evaluate",input);
                var mapping = (Map) ((List) evaluated.get("mappings")).getFirst();
                for (String side : List.of("source", "target")) {
                    var model = (Map) mapping.get(side);
                    assertThat(model.get("modelVersion")).isInstanceOf(String.class);
                    assertThat((List<Map>) model.get("fields")).anySatisfy(field ->
                            assertThat(field).containsEntry("name", "id").containsEntry("keyDomain", "EXACT_NUMBER"));
                }
                String taskPath = "/api/pipelines/flow/validation-tasks/pair";
                if (method.equals("UNIQUE_KEY")) {
                    var incompatible = new LinkedHashMap<String, Object>(input);
                    incompatible.put("confirmations", Map.of("orders", Map.of("id", UUID.randomUUID().toString(),
                            "fields", List.of(Map.of("source", "id", "target", "payload")))));
                    assertThatThrownBy(() -> post(http, write, "evaluate", incompatible))
                            .isInstanceOfSatisfying(RestClientResponseException.class, refused -> {
                                assertThat(refused.getStatusCode().value()).isEqualTo(400);
                                assertThat(refused.getResponseBodyAsString()).contains("TS0300007", "key-domain-mismatch");
                            });
                    assertThatThrownBy(() -> post(http, write, "save", Map.of("input", incompatible,
                            "fingerprint", evaluated.get("fingerprint"), "commandId", UUID.randomUUID().toString(),
                            "historyLimit", 10, "scheduleEnabled", false)))
                            .isInstanceOfSatisfying(RestClientResponseException.class, refused -> {
                                assertThat(refused.getStatusCode().value()).isEqualTo(409);
                                assertThat(refused.getResponseBodyAsString()).contains("CONFLICT");
                            });
                    assertThatThrownBy(() -> get(http, read, taskPath))
                            .isInstanceOf(org.springframework.web.client.HttpClientErrorException.NotFound.class);
                }
                var saved=post(http,write,"save",Map.of("input",input,"fingerprint",evaluated.get("fingerprint"),"commandId",UUID.randomUUID().toString(),"historyLimit",10,"scheduleEnabled",false));
                assertThat(saved).containsEntry("outcome","APPLIED");
                var reloaded = get(http, read, taskPath);
                assertThat(reloaded).containsEntry("revision", "1").containsEntry("taskGeneration", saved.get("taskGeneration"));
                var savedKeys = (Map) ((Map) ((List) reloaded.get("mappings")).getFirst()).get("keys");
                assertThat(savedKeys.get("fields")).isEqualTo(List.of(Map.of("source", List.of("id"), "target", List.of("id"))));
                assertThat(child.getBean(ValidationMemberResources.Member.class).snapshot().data().capacity().bytes()).isEqualTo(256L*1024*1024);
                if(method.equals("FULL_COLUMN"))try(var query=java.util.concurrent.Executors.newSingleThreadExecutor();var decode=java.util.concurrent.Executors.newSingleThreadExecutor()){
                    var resource=StoredArtifacts.requireSource(artifacts,"input");
                    var ref=child.getBean(io.tapstate.adapters.pdk.ConnectorProvisioner.class).resolve(connector);
                    var config=new ConnectionConfig(resource.id(),resource.connector(),resource.config());
                    var hash=io.tapstate.core.model.canonical.CanonicalHash.of(resource);var limits=new io.tapstate.core.validation.ValidationValues.Limits(64L*1024,8192,32);
                    io.tapstate.adapters.pdk.PdkValidationReads.Metadata metadata;
                    try(var inspected=io.tapstate.adapters.pdk.PdkValidationReads.inspect(config,ref,hash,"orders",List.of(),()->{},query,decode,limits)){metadata=inspected.result();}
                    assertThat(metadata.model().fields()).hasSize(2);
                    var keys=List.of(new io.tapstate.core.validation.ValidationKeyBinding.Component(List.of("id"),io.tapstate.core.validation.ValidationKey.Kind.EXACT_NUMBER,Set.of("exact-number")));
                    try(var rejected=io.tapstate.adapters.pdk.PdkValidationReads.scan(config,ref,hash,"orders",metadata.binding(),keys,true,()->{},query,decode,2,limits,1)){
                        assertThatThrownBy(rejected::next).isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                                failure->{assertThat(failure.code()).isEqualTo(io.tapstate.core.validation.ValidationError.VALUE_LIMIT_EXCEEDED);assertThat(failure.args()).containsEntry("limit","validation.nativeFields");});
                        rejected.close();io.tapstate.adapters.pdk.PdkValidationReads.cleanupCompleted(rejected).toCompletableFuture().get(10,java.util.concurrent.TimeUnit.SECONDS);
                    }
                    assertThat(sql.getLogs().toUpperCase(Locale.ROOT)).doesNotContain("LIMIT 64");
                    try(var admitted=io.tapstate.adapters.pdk.PdkValidationReads.scan(config,ref,hash,"orders",metadata.binding(),keys,true,()->{},query,decode,2,limits,2)){
                        try(var row=admitted.next().orElseThrow()){assertThat(row.values().fields()).containsOnlyKeys("id","payload");}
                        assertThat(admitted.next()).isEmpty();
                    }
                    assertThat(sql.getLogs().toUpperCase(Locale.ROOT)).contains("LIMIT 64");
                }
                for(int sequence=1;sequence<=2;sequence++){
                    var operation=Map.<String,Object>of("context",evaluated.get("context"),"pipeline","flow","task","pair","taskGeneration",saved.get("taskGeneration"),"revision","1","sequence",Integer.toString(sequence),"command",UUID.randomUUID().toString());
                    var start=post(http,write,"start",operation);var status=new java.util.concurrent.atomic.AtomicReference<Map<String,Object>>();var retried=new java.util.concurrent.atomic.AtomicBoolean();
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                            .ignoreExceptionsMatching(failure->failure instanceof org.springframework.web.client.HttpClientErrorException.NotFound||failure instanceof org.springframework.web.client.HttpClientErrorException.TooManyRequests).until(()->{
                        var observed=post(http,read,"progress",operation);status.set(observed);
                        if("ACCEPTED".equals(observed.get("outcome"))&&Boolean.FALSE.equals(observed.get("admitted"))&&Boolean.FALSE.equals(observed.get("controlUnknown"))&&"STARTING".equals(observed.get("state"))&&retried.compareAndSet(false,true))post(http,write,"start",operation);
                        return "COMPLETED".equals(observed.get("state"))||"FAILED".equals(observed.get("state"))||"STOPPED".equals(observed.get("state"));
                    });
                    assertThat(status.get()).as("supported PostgreSQL pair must complete inside its unchanged capacity").containsEntry("state","COMPLETED");
                    var report=get(http,read,"/api/pipelines/flow/validation-tasks/pair/reports/"+start.get("run")+"?generation="+saved.get("taskGeneration")+"&sequence="+sequence);
                    assertThat(report).containsEntry("method",method).containsEntry("closed",true).containsEntry("rootCount",sequence==1?"0":method.equals("UNIQUE_KEY")?"2":"1");
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).until(()->"DRAINED".equals(post(http,read,"progress",operation).get("drain"))&&child.getBean(ValidationMemberResources.Member.class).snapshot().data().runs()==0);
                    if(sequence==1){var fault=sql.execInContainer("psql","-U","validation","-d",DB,"-c",method.equals("UNIQUE_KEY")?"UPDATE checked_orders SET id=2;":"UPDATE checked_orders SET payload='different';");assertThat(fault.getExitCode()).isZero();}
                }
            }finally{try(var raw=MongoClients.create(storage.configurationUri())){for(var database:List.of(storage.configurationDatabase(),storage.cacheDatabase(),storage.masterDatabase()))raw.getDatabase(database).drop();}}
        }
    }

    @Test @Timeout(180) void mysqlAndMongoTextKeysUseTheSameOrdering()throws Exception{verify(true,true);}
    private void verify(boolean mysql)throws Exception{verify(mysql,false);}
    @Test @Timeout(180) void postgresWideNativePageIsChargedBeforeQueryAndPreservesAllRows()throws Exception{verify(false,false,true);}
    private void verify(boolean mysql,boolean textOnly)throws Exception{verify(mysql,textOnly,false);}
    @Test @Timeout(180) void postgresStricterResultBufferFailsAsIncompleteWithoutChangingSavedSource()throws Exception{verify(false,false,true,true);}
    private void verify(boolean mysql,boolean textOnly,boolean wide)throws Exception{verify(mysql,textOnly,wide,false);}
    @Test @Timeout(180) void postgresDuplicateIdentityAcrossNativePagesRetainsExactMultiplicity()throws Exception{verify(false,false,false,false,true);}
    private void verify(boolean mysql,boolean textOnly,boolean wide,boolean smallerCap)throws Exception{verify(mysql,textOnly,wide,smallerCap,false);}
    @Test @Timeout(180) void bsonDataChangesAfterSaveAreComparedInsteadOfChangingTheReadSchema()throws Exception{verify(false,false,false,false,false,"inference");}
    @Test @Timeout(180) void postgresNativeArrayExpansionIsRefusedBeforeQueryButScalarKeysAndCountRemainAvailable()throws Exception{verify(false,false,false,false,false,"array");}
    @Test @Timeout(180) void postgresJsonbScalarCodecRemainsBoundedWithoutBlockingCountOrKeys()throws Exception{verify(false,false,false,false,false,"json");}
    private void verify(boolean mysql,boolean textOnly,boolean wide,boolean smallerCap,boolean duplicatePages)throws Exception{verify(mysql,textOnly,wide,smallerCap,duplicatePages,null);}
    private void verify(boolean mysql,boolean textOnly,boolean wide,boolean smallerCap,boolean duplicatePages,String nested)throws Exception{
        int port=mysql?3306:5432;String kind=mysql?"mysql":"postgres";
        try(var sql=new GenericContainer<>(mysql?"mysql:8.0":"postgres:16").withExposedPorts(port).withStartupTimeout(Duration.ofSeconds(75))){
            if(mysql)sql.withEnv("MYSQL_ROOT_PASSWORD",PASSWORD).withEnv("MYSQL_DATABASE",DB).withEnv("MYSQL_USER","validation").withEnv("MYSQL_PASSWORD",PASSWORD)
                    .withCommand("--innodb-buffer-pool-size=64M","--max-connections=20").waitingFor(Wait.forLogMessage(".*mysqld: ready for connections\\..*port: 3306.*\\n",1));
            else sql.withEnv("POSTGRES_PASSWORD",PASSWORD).withEnv("POSTGRES_USER","validation").withEnv("POSTGRES_DB",DB)
                    .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n",2));
            sql.start();String statement="CREATE TABLE orders (id BIGINT PRIMARY KEY, payload VARCHAR(32), amount BIGINT); INSERT INTO orders VALUES (1,'same',9007199254740993),(2,'two',9007199254740993); "
                    +"CREATE TABLE items (id BIGINT PRIMARY KEY, secondary BIGINT, alias_id BIGINT, alias_secondary BIGINT, payload VARCHAR(32)); INSERT INTO items VALUES(9,90,9,90,'item'),(11,110,11,110,'tail');";
            if(wide)statement="CREATE TABLE orders (id BIGINT PRIMARY KEY,payload TEXT,amount BIGINT); INSERT INTO orders SELECT n,repeat(chr(30028),20000),9007199254740993 FROM generate_series(1,1000) AS n; "
                    +"CREATE TABLE items (id BIGINT PRIMARY KEY, secondary BIGINT, alias_id BIGINT, alias_secondary BIGINT, payload VARCHAR(32)); INSERT INTO items VALUES(9,90,9,90,'item'),(11,110,11,110,'tail');";
            if(duplicatePages)statement="CREATE TABLE orders (id BIGINT PRIMARY KEY,payload VARCHAR(32),amount BIGINT); INSERT INTO orders SELECT n,'same',CASE WHEN n BETWEEN 50 AND 90 THEN 50 ELSE n END FROM generate_series(1,100) AS n; "
                    +"CREATE TABLE items (id BIGINT PRIMARY KEY, secondary BIGINT, alias_id BIGINT, alias_secondary BIGINT, payload VARCHAR(32)); INSERT INTO items VALUES(9,90,9,90,'item'),(11,110,11,110,'tail');";
            if(nested!=null&&!nested.equals("inference")){
                String type=nested.equals("array")?"TEXT[]":"JSONB";
                String value=nested.equals("array")?"array_fill('a'::text,ARRAY[100000])":"to_jsonb(array_fill('a'::text,ARRAY[100000]))";
                statement="CREATE TABLE orders(id BIGINT PRIMARY KEY,payload "+type+",amount BIGINT); INSERT INTO orders SELECT n,"+value+",9007199254740993 FROM generate_series(1,2) AS n; "
                        +"CREATE TABLE items (id BIGINT PRIMARY KEY, secondary BIGINT, alias_id BIGINT, alias_secondary BIGINT, payload VARCHAR(32)); INSERT INTO items VALUES(9,90,9,90,'item'),(11,110,11,110,'tail');";
            }
            if(mysql)statement+=" CREATE TABLE text_orders (id VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci PRIMARY KEY); INSERT INTO text_orders VALUES ('a'),('B');";
            var seeded=mysql?sql.execInContainer("mysql","--protocol=TCP","--host=127.0.0.1","--user=validation","--password="+PASSWORD,"--batch",DB,"-e",statement)
                    :sql.execInContainer("psql","-U","validation","-d",DB,"-c",statement);
            assertThat(seeded.getExitCode()).as(seeded.getStderr()).isZero();
            var source=new LinkedHashMap<String,Object>();source.put("host",sql.getHost());source.put("port",sql.getMappedPort(port));source.put("database",DB);
            source.put(mysql?"username":"user","validation");source.put("password",PASSWORD);source.put("timezone","+00:00");source.put("oldVersionTimezone",false);
            if(mysql)source.put("addtionalString","useSSL=false&allowPublicKeyRetrieval=true");else source.put("schema","public");
            if(textOnly){
                var ordered=sql.execInContainer("mysql","--protocol=TCP","--host=127.0.0.1","--user=validation","--password="+PASSWORD,"--batch","--skip-column-names",DB,"-e","SELECT id FROM text_orders ORDER BY id");
                assertThat(ordered.getExitCode()).isZero();assertThat(ordered.getStdout().strip()).isEqualTo("a\nB");
            }
            if(smallerCap)source.put("extParams","?maxResultBuffer=1000000");
            exercise(kind,source,textOnly,wide,smallerCap,duplicatePages,nested,sql);
        }
    }

    private void exercise(String kind,Map<String,Object> source,boolean textOnly,boolean wide,boolean smallerCap,boolean duplicatePages,String nested,GenericContainer sql)throws Exception{
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
            if(wide){
                target.getCollection("checked_orders").deleteMany(new Document());
                var rows=new ArrayList<Document>();var payload=String.valueOf((char)30028).repeat(20000);
                for(long id=1;id<=1000;id++)rows.add(new Document("id",id).append("payload",payload).append("amount",9007199254740993L));
                target.getCollection("checked_orders").insertMany(rows);
            }
            if(duplicatePages){
                target.getCollection("checked_orders").deleteMany(new Document());var rows=new ArrayList<Document>();
                for(long id=1;id<=100;id++)rows.add(new Document("id",id).append("payload","same").append("amount",id>=50&&id<=90?50L:id));
                target.getCollection("checked_orders").insertMany(rows);
            }
            if(textOnly)target.getCollection("checked_text_orders").insertMany(List.of(new Document("id","a"),new Document("id","B")));
            target.getCollection("checked_items").insertMany(List.of(item(9,90,"item"),item(11,110,"tail")));
            try(var app=new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                    "--server.address=127.0.0.1","--server.port=0","--logging.level.root=ERROR","--logging.level.io.tapstate.app.NativeValidationOwnerReservations=WARN","--tapstate.hz.member-port=0","--tapstate.hz.jet.cooperative-thread-count=2",
                    "--tapstate.store.mongo.uri="+storage.configurationUri(),"--tapstate.connectors.seed-dir="+work.resolve("no-seeds"),
                    "--tapstate.connectors.plugins-dir="+work.resolve("plugins"),"--tapstate.control.auth.jwt-secret=relational-validation-http-signing-key")){
                NativeAppStorage.awaitBusiness(app);var child=NativeAppStorage.business(app);
                var local=child.getBean(LocalConnectorArtifacts.class);
                var sourceConnector=local.register(Files.readAllBytes(artifact(kind)),RegistrationSource.REGISTER)
                        .registration().connectorId();
                var targetConnector=local.register(Files.readAllBytes(artifact("mongodb")),RegistrationSource.REGISTER)
                        .registration().connectorId();
                var artifacts=child.getBean(ArtifactStore.class);
                artifacts.create(new SourceResource("input",null,sourceConnector,source,SourceMode.SNAPSHOT,textOnly?List.of(TableRef.literal("orders"),TableRef.literal("items"),TableRef.literal("text_orders")):List.of(TableRef.literal("orders"),TableRef.literal("items")),null,null));
                artifacts.create(new SourceResource("target",null,targetConnector,Map.of("uri",MONGO.getReplicaSetUrl(targetName),"database",targetName,"isUri",true),null,null,null,null));
                artifacts.create(new PipelineResource("flow",null,List.of(SourceRef.spec("input",false)),null,null,
                        new ServeBlock.Inline("serve",FromClause.list(FromRef.literal("input.orders"),FromRef.literal("input.items")),
                                List.of(new SyncElement("out","target",null,new RenameSpec(null,null,"checked_",null),null)),null,null),null,null));
                var tokens=child.getBean(TokenService.class);String write=tokens.create(Scope.WRITE),read=tokens.create(Scope.READ);
                var http=RestClient.create("http://127.0.0.1:"+((WebServerApplicationContext)app).getWebServer().getPort());
                if(textOnly){
                    artifacts.create(new PipelineResource("text_flow",null,List.of(SourceRef.spec("input",false)),null,null,
                            new ServeBlock.Inline("serve",FromClause.list(FromRef.literal("input.text_orders")),List.of(new SyncElement("out","target",null,new RenameSpec(null,null,"checked_",null),null)),null,null),null,null));
                    for(var method:List.of("UNIQUE_KEY","FULL_COLUMN")){
                        String task=method.toLowerCase(Locale.ROOT);
                        var input=Map.of("pipelineId","text_flow","taskId",task,"expectedRevision","0","method",method,
                                "mappings",List.of(Map.of("mappingId","text","outputId","out","sourceId","input","table","text_orders")),
                                "confirmations",Map.of("text",Map.of("id",UUID.randomUUID().toString(),"fields",List.of(Map.of("source","id","target","id")))));
                        var evaluated=post(http,write,"evaluate",input);
                        var saved=post(http,write,"save",Map.of("input",input,"fingerprint",evaluated.get("fingerprint"),"commandId",UUID.randomUUID().toString(),"historyLimit",10,"scheduleEnabled",false));
                        var operation=Map.<String,Object>of("context",evaluated.get("context"),"pipeline","text_flow","task",task,
                                "taskGeneration",saved.get("taskGeneration"),"revision","1","sequence","1","command",UUID.randomUUID().toString());
                        assertThat(run(http,write,read,operation)).containsEntry("result","COMPLETE_NO_ISSUES_OBSERVED").containsEntry("rootCount","0");
                    }
                    return;
                }
                if("inference".equals(nested)){
                    var input=selection("full_column","FULL_COLUMN");var evaluated=post(http,write,"evaluate",input);
                    var saved=post(http,write,"save",Map.of("input",input,"fingerprint",evaluated.get("fingerprint"),"commandId",UUID.randomUUID().toString(),"historyLimit",10,"scheduleEnabled",false));
                    // These are data changes between saving and running, followed by a stable read window.
                    // MongoDB has no column DDL here; numerically equal integer widths retain identity.
                    for(String name:List.of("checked_orders","checked_items"))for(var row:target.getCollection(name).find()){
                        var update=new Document("id",((Number)row.get("id")).intValue());
                        if(name.equals("checked_orders"))update.append("payload","changed-".repeat(40));
                        target.getCollection(name).updateOne(new Document("_id",row.get("_id")),new Document("$set",update));
                    }
                    target.getCollection("checked_items").insertOne(item(99,990,"extra-tail"));
                    var operation=Map.<String,Object>of("context",evaluated.get("context"),"pipeline","flow","task","full_column","taskGeneration",saved.get("taskGeneration"),"revision","1","sequence","1","command",UUID.randomUUID().toString());
                    var report=run(http,write,read,operation);
                    assertThat(report).containsEntry("terminal","COMPLETED").containsEntry("failure",null)
                            .containsEntry("result","COMPLETE_WITH_OBSERVATIONS").containsEntry("rootCount","3");
                    var ref=(Map)report.get("reference");String query="?generation="+ref.get("generation")+"&sequence="+ref.get("sequence");
                    var path="/api/pipelines/flow/validation-tasks/full_column/reports/"+ref.get("run")+"/roots";var roots=get(http,read,path+query);
                    assertThat(roots).containsEntry("total","3");var details=new StringBuilder();
                    for(var root:(List<Map>)roots.get("items"))details.append(get(http,read,path+"/"+root.get("id")+query));
                    assertThat(details.toString()).contains("TARGET_VALUE","payload","TARGET_EXTRA").doesNotContain("TARGET_TYPE");
                    return;
                }
                if(nested!=null){
                    var emptyKeys=new LinkedHashMap<String,Object>(selection("unique_key","UNIQUE_KEY"));emptyKeys.put("confirmations",Map.of());
                    var candidates=new java.util.concurrent.atomic.AtomicReference<Map<String,Object>>();
                    assertThatCode(()->candidates.set(post(http,write,"evaluate",emptyKeys))).doesNotThrowAnyException();
                    var sourceSide=(Map)((Map)((List)candidates.get().get("mappings")).getFirst()).get("source");
                    assertThat((List<Map>)sourceSide.get("candidates")).anySatisfy(candidate->assertThat(candidate.get("fields")).isEqualTo(List.of("id")));
                    if(nested.equals("array"))assertThatThrownBy(()->post(http,write,"evaluate",selection("full_column","FULL_COLUMN")))
                            .isInstanceOfSatisfying(org.springframework.web.client.HttpClientErrorException.class,refused->{
                                assertThat(refused.getStatusCode().value()).isEqualTo(400);
                                assertThat(refused.getResponseBodyAsString()).contains("TS0300007","native-nested-codec-expansion-unproven","payload");
                            });
                    else{
                        var input=selection("full_column","FULL_COLUMN");var evaluated=post(http,write,"evaluate",input);
                        var saved=post(http,write,"save",Map.of("input",input,"fingerprint",evaluated.get("fingerprint"),"commandId",UUID.randomUUID().toString(),"historyLimit",10,"scheduleEnabled",false));
                        var operation=Map.<String,Object>of("context",evaluated.get("context"),"pipeline","flow","task","full_column","taskGeneration",saved.get("taskGeneration"),"revision","1","sequence","1","command",UUID.randomUUID().toString());
                        var report=runWithFailure(http,write,read,operation);
                        assertThat(report).containsEntry("terminal","FAILED");assertThat(report.get("result").toString()).startsWith("INCOMPLETE");
                    }
                    for(String method:List.of("COUNT","UNIQUE_KEY")){
                        String task=method.toLowerCase(Locale.ROOT);var input=selection(task,method);var evaluated=post(http,write,"evaluate",input);
                        var saved=post(http,write,"save",Map.of("input",input,"fingerprint",evaluated.get("fingerprint"),"commandId",UUID.randomUUID().toString(),"historyLimit",10,"scheduleEnabled",false));
                        var operation=Map.<String,Object>of("context",evaluated.get("context"),"pipeline","flow","task",task,"taskGeneration",saved.get("taskGeneration"),"revision","1","sequence","1","command",UUID.randomUUID().toString());
                        assertThat(run(http,write,read,operation)).containsEntry("result","COMPLETE_NO_ISSUES_OBSERVED").containsEntry("rootCount","0");
                    }
                    assertThat(StoredArtifacts.requireSource(artifacts,"input").config()).isEqualTo(source);
                    return;
                }
                if(duplicatePages){
                    var input=new LinkedHashMap<String,Object>(selection("unique_key","UNIQUE_KEY"));
                    var confirmations=new LinkedHashMap<String,Object>((Map<String,Object>)input.get("confirmations"));
                    confirmations.put("orders",Map.of("id",UUID.randomUUID().toString(),"fields",List.of(Map.of("source","amount","target","amount"))));input.put("confirmations",confirmations);
                    var evaluated=post(http,write,"evaluate",input);
                    var saved=post(http,write,"save",Map.of("input",input,"fingerprint",evaluated.get("fingerprint"),"commandId",UUID.randomUUID().toString(),"historyLimit",10,"scheduleEnabled",false));
                    var operation=Map.<String,Object>of("context",evaluated.get("context"),"pipeline","flow","task","unique_key","taskGeneration",saved.get("taskGeneration"),"revision","1","sequence","1","command",UUID.randomUUID().toString());
                    var report=run(http,write,read,operation);assertThat(report).containsEntry("result","COMPLETE_WITH_OBSERVATIONS").containsEntry("rootCount","1");
                    var ref=(Map)report.get("reference");String query="?generation="+ref.get("generation")+"&sequence="+ref.get("sequence");
                    var path="/api/pipelines/flow/validation-tasks/unique_key/reports/"+ref.get("run")+"/roots";
                    var roots=get(http,read,path+query);assertThat(roots).containsEntry("total","1");
                    var detail=get(http,read,path+"/"+((Map)((List)roots.get("items")).getFirst()).get("id")+query);
                    assertThat(detail.toString()).contains("SOURCE_DUPLICATE","TARGET_DUPLICATE");
                    var multiplicities=new ArrayList<String>();collectMultiplicity(detail,multiplicities);
                    assertThat(multiplicities).hasSizeGreaterThanOrEqualTo(2).allMatch("41"::equals);
                    return;
                }
                if(wide){
                    var input=selection("full_column","FULL_COLUMN");var evaluated=post(http,write,"evaluate",input);
                    var saved=post(http,write,"save",Map.of("input",input,"fingerprint",evaluated.get("fingerprint"),"commandId",UUID.randomUUID().toString(),"historyLimit",10,"scheduleEnabled",false));
                    var operation=new LinkedHashMap<String,Object>(Map.of("context",evaluated.get("context"),"pipeline","flow","task","full_column","taskGeneration",saved.get("taskGeneration"),"revision","1","sequence","1","command",UUID.randomUUID().toString()));
                    try(var executor=java.util.concurrent.Executors.newSingleThreadExecutor();var locked=new PostgresTableLock(sql)){
                        var result=executor.submit(()->smallerCap?runWithFailure(http,write,read,operation):run(http,write,read,operation,Duration.ofSeconds(90)));
                        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(()->{
                            var waiting=sql.execInContainer("psql","-U","validation","-d",DB,"-t","-A","-c","SELECT count(*) FROM pg_locks WHERE relation='orders'::regclass AND NOT granted;");
                            assertThat(waiting.getExitCode()).isZero();assertThat(Integer.parseInt(waiting.getStdout().strip())).isPositive();
                        });
                        // The query is blocked before fetching any rows; the immutable run has already paid its native window.
                        var budget=child.getBean(ValidationMemberResources.Member.class).snapshot();
                        // This fixture has three native columns. Keep raw/UTF-16, both metadata
                        // windows and non-reader space, without an unrelated 8192-column map charge.
                        long minimumWindow=3*NativeValidationPdkRows.POSTGRES_RESULT_BYTES+48L*1024*1024+64L*3*256;
                        try{assertThat(budget.data().reserved().bytes()).isGreaterThanOrEqualTo(minimumWindow);
                            assertThat(budget.data().capacity().bytes()).isEqualTo(256L*1024*1024);}
                        finally{locked.close();}
                        var report=result.get(90,java.util.concurrent.TimeUnit.SECONDS);
                        if(smallerCap){
                            assertThat(report).containsEntry("terminal","COMPLETED").containsEntry("result","COMPLETE_WITH_FAILURES");
                            assertThat((List<?>)report.get("tableFailures")).singleElement().satisfies(value->{
                                var failure=(Map<?,?>)value;
                                assertThat(failure.get("mapping")).isEqualTo("orders");
                                assertThat(failure.get("side")).isEqualTo("SOURCE");
                                assertThat(((Map<?,?>)failure.get("failure")).get("code")).isEqualTo("TS0300015");
                            });
                            assertThat(StoredArtifacts.requireSource(artifacts,"input").config()).isEqualTo(source);
                        }else assertThat(report).containsEntry("result","COMPLETE_NO_ISSUES_OBSERVED").containsEntry("rootCount","0");
                        assertThat((List<?>)report.get("mappings")).hasSize(2);
                        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(()->assertThat(child.getBean(ValidationMemberResources.Member.class).snapshot().data().runs()).isZero());
                    }
                    return;
                }
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
    private static void collectMultiplicity(Object value,List<String> counts){
        if(value instanceof Map<?,?> map){
            if("occurrences".equals(map.get("name"))&&map.get("value") instanceof Map<?,?> scalar)counts.add(String.valueOf(scalar.get("value")));
            for(var child:map.values())collectMultiplicity(child,counts);
        }else if(value instanceof List<?> items)for(var child:items)collectMultiplicity(child,counts);
    }
    private static final class PostgresTableLock implements AutoCloseable {
        private final java.io.PipedOutputStream commands=new java.io.PipedOutputStream();
        private final java.io.PipedInputStream input;
        private final java.io.ByteArrayOutputStream output=new java.io.ByteArrayOutputStream();
        private final com.github.dockerjava.api.async.ResultCallback.Adapter<com.github.dockerjava.api.model.Frame> running;
        private boolean closed;
        PostgresTableLock(GenericContainer sql)throws Exception{
            input=new java.io.PipedInputStream(commands);
            var docker=org.testcontainers.DockerClientFactory.instance().client();
            var exec=docker.execCreateCmd(sql.getContainerId()).withAttachStdin(true).withAttachStdout(true).withAttachStderr(true)
                    .withCmd("psql","-U","validation","-d",DB,"-X","-q","-t","-A","-v","ON_ERROR_STOP=1").exec();
            running=docker.execStartCmd(exec.getId()).withStdIn(input).exec(new com.github.dockerjava.api.async.ResultCallback.Adapter<com.github.dockerjava.api.model.Frame>(){@Override public void onNext(com.github.dockerjava.api.model.Frame frame){output.writeBytes(frame.getPayload());}});
            commands.write("BEGIN; LOCK TABLE orders IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));commands.flush();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).until(()->output.toString(java.nio.charset.StandardCharsets.UTF_8).contains("locked"));
        }
        public void close()throws Exception{
            if(closed)return;closed=true;
            commands.write("COMMIT;\n\\q\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));commands.flush();commands.close();
            assertThat(running.awaitCompletion(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();running.close();input.close();
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
    private static Map<String,Object> runWithFailure(RestClient http,String write,String read,Map<String,Object> operation){
        var start=post(http,write,"start",operation);assertThat(start.get("outcome")).isIn("PENDING","ACCEPTED");
        var completed=new java.util.concurrent.atomic.AtomicReference<Map<String,Object>>();
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100))
                .ignoreExceptionsMatching(failure->failure instanceof org.springframework.web.client.HttpClientErrorException.NotFound||failure instanceof org.springframework.web.client.HttpClientErrorException.TooManyRequests)
                .untilAsserted(()->{
                    var report=get(http,read,"/api/pipelines/flow/validation-tasks/"+operation.get("task")+"/reports/"+start.get("run")+"?generation="+operation.get("taskGeneration")+"&sequence="+operation.get("sequence"));
                    assertThat(report.get("terminal")).isIn("COMPLETED","FAILED","STOPPED");completed.set(report);
                });
        return completed.get();
    }
    private static Map<String,Object> run(RestClient http,String write,String read,Map<String,Object> operation){return run(http,write,read,operation,Duration.ofSeconds(25));}
    private static Map<String,Object> run(RestClient http,String write,String read,Map<String,Object> operation,Duration timeout){
        var start=post(http,write,"start",operation);assertThat(start.get("outcome")).isIn("PENDING","ACCEPTED");
        var completed=new java.util.concurrent.atomic.AtomicReference<Map<String,Object>>();var retried=new java.util.concurrent.atomic.AtomicBoolean();
        try{org.awaitility.Awaitility.await().atMost(timeout).pollInterval(Duration.ofMillis(100))
                .ignoreExceptionsMatching(failure->failure instanceof org.springframework.web.client.HttpClientErrorException.NotFound||failure instanceof org.springframework.web.client.HttpClientErrorException.TooManyRequests).untilAsserted(()->{
                    var current=post(http,read,"progress",operation);assertThat(current.get("original")).isEqualTo(operation);
                    // Passive receipt recovery does not activate. The caller explicitly retries only this original once.
                    if("ACCEPTED".equals(current.get("outcome"))&&Boolean.FALSE.equals(current.get("admitted"))
                            &&Boolean.FALSE.equals(current.get("controlUnknown"))&&"STARTING".equals(current.get("state"))&&retried.compareAndSet(false,true)){
                        var retry=post(http,write,"start",operation);assertThat(retry.get("original")).isEqualTo(operation);assertThat(retry.get("run")).isEqualTo(start.get("run"));
                    }
                    var report=get(http,read,"/api/pipelines/"+operation.get("pipeline")+"/validation-tasks/"+operation.get("task")+"/reports/"+start.get("run")+"?generation="+operation.get("taskGeneration")+"&sequence="+operation.get("sequence"));
                    assertThat(report).containsEntry("terminal","COMPLETED").containsEntry("closed",true);completed.set(report);
                });}catch(org.awaitility.core.ConditionTimeoutException failed){
            throw new AssertionError("original="+operation+"; initial="+start+"; progress="+post(http,read,"progress",operation)
                    +"; task="+get(http,read,"/api/pipelines/"+operation.get("pipeline")+"/validation-tasks/"+operation.get("task")),failed);
        }
        assertThat(post(http,read,"progress",operation)).containsEntry("state","COMPLETED");return completed.get();
    }
    private static Map<String,Object> post(RestClient http,String token,String action,Map<String,?> body){return http.post().uri("/api/validation:"+action).header("Authorization","Bearer "+token).contentType(MediaType.APPLICATION_JSON).body(body).retrieve().body(Map.class);}
    private static Map<String,Object> get(RestClient http,String token,String path){return http.get().uri(path).header("Authorization","Bearer "+token).retrieve().body(Map.class);}
}
