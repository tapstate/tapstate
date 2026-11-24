package io.tapstate.app;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.*;
import io.tapstate.core.common.*;
import io.tapstate.core.model.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** The same native NEST fixture measures steady idle work and then captures real new DML. */
@RequiresDocker
class NativeIdlePipelineCostIT {
    @TempDir Path work;
    @Test void idleNestRemainsBoundedAndStillCapturesChanges()throws Exception{
        var args=new ArrayList<String>();args.add(Path.of(System.getProperty("java.home"),"bin","java").toString());
        args.addAll(List.of("-Xmx1536m","-XX:ActiveProcessorCount=2","--add-opens","java.base/java.lang=ALL-UNNAMED"));
        for(String property:List.of("api.version","tapstate.test.idle.connectors","tapstate.test.cloud-release-dir")){
            String value=System.getProperty(property);if(value!=null)args.add("-D"+property+"="+value);
        }
        args.addAll(List.of("-cp",System.getProperty("java.class.path"),Application.class.getName(),work.toString()));
        Path log=Path.of("target","failsafe-reports","idle-native-"+UUID.randomUUID()+".log");Files.createDirectories(log.getParent());
        var process=new ProcessBuilder(args).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try{
            boolean complete=process.waitFor(240,TimeUnit.SECONDS);String output=Files.readString(log);System.out.print(output);
            assertThat(complete).as("isolated native fixture completed").isTrue();assertThat(process.exitValue()).as(output).isZero();
        }finally{if(process.isAlive())process.destroyForcibly();}
    }
    @Test
    void warmNestObservationPublishesBatchedFactsAndAdvancesAfterCdc() throws Exception {
        verifyNativeFixture(false);
    }

    @Test void singleNodeWorkloadSwitchDeliversFullAndCdcNestState() throws Exception {
        verifyNativeFixture(false, true);
    }

    public static class Application {
        public static void main(String[] args)throws Exception{
            // Keep the fixture runnable against the baseline before this startup default existed.
            try{Bootstrap.class.getDeclaredMethod("configureWorkerParallelism").invoke(null);}catch(NoSuchMethodException baseline){ }
            var fixture=new NativeIdlePipelineCostIT();fixture.work=Path.of(args[0]);fixture.verifyNativeFixture(true);
        }
    }
    private Path connectorDirectory() throws java.io.IOException {
        String local = System.getProperty("tapstate.test.idle.connectors");
        if (local != null) return Path.of(local);
        Path publishedMongo = CloudConnectorTestInputs.seedDirectory().resolve("mongodb-connector.jar");
        Path seeds = Files.createDirectory(work.resolve("seeds"));
        Files.copy(publishedMongo, seeds.resolve(publishedMongo.getFileName()));
        return seeds;
    }
    private void verifyNativeFixture(boolean measure) throws Exception {
        verifyNativeFixture(measure, false);
    }
    private void verifyNativeFixture(boolean measure, boolean exerciseChildEdits) throws Exception {
        try(var mongo=new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withTmpFs(Map.of("/data/db","rw","/data/configdb","rw"))
                .withCreateContainerCmdModifier(cmd->cmd.getHostConfig().withMemory(768L*1024*1024).withNanoCPUs(1_000_000_000L))
                .withCommand("--replSet","docker-rs","--bind_ip_all","--wiredTigerCacheSizeGB","0.25")) {
            mongo.start();
            var storage=NativeAppStorage.createOnPremWithAdmin(mongo,"idle1022","admin","isolated-idle-password");
            try(var raw=MongoClients.create(storage.configurationUri());
                    var app=new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated())
                        .properties(exerciseChildEdits ? Map.<String,Object>of("tapstate.cluster.profile", "SINGLE",
                                "tapstate.cluster.node-session-ttl", "2s", "tapstate.cluster.node-session-renew-interval", "500ms") : Map.<String,Object>of()).run(
                        "--server.address=127.0.0.1","--server.port=0","--logging.level.root=ERROR",
                        "--tapstate.store.mongo.uri="+storage.configurationUri(),"--tapstate.hz.member-port=0",
                        "--tapstate.hz.jet.cooperative-thread-count=2","--tapstate.connectors.seed-dir="+connectorDirectory(),
                        "--tapstate.connectors.plugins-dir="+work.resolve("plugins"),"--tapstate.control.auth.jwt-secret=idle-diagnostic-signing-key")) {
                NativeAppStorage.awaitBusiness(app);
                String base="http://127.0.0.1:"+((WebServerApplicationContext)app).getWebServer().getPort();
                try(var http=HttpClient.newHttpClient()) {
                    String token=String.valueOf(request(http,base,null,"POST","/auth/login",Map.of("username","admin","password","isolated-idle-password"),200).get("token"));
                    var data=raw.getDatabase(storage.masterDatabase());
                    for(int i=1;i<=5;i++) {
                        data.getCollection("orders").insertOne(new Document("_id",(long)i).append("parent_id",(long)i));
                        data.getCollection("lines").insertOne(new Document("_id",(long)i).append("parent_id",(long)i));
                    }
                    if (exerciseChildEdits) {
                        for (String table : List.of("orders", "lines"))
                            data.runCommand(new Document("collMod", table)
                                    .append("changeStreamPreAndPostImages", new Document("enabled", true)));
                    }
                    var port=NativeAppStorage.business(app).getBean(StorePort.class);
                    port.artifacts().save(new SourceResource("idle-input",null,"mongodb",Map.of("isUri",true,"uri",storage.masterUri(),"preImage",exerciseChildEdits),
                            SourceMode.CDC,List.of(TableRef.literal("orders"),TableRef.literal("lines")),null,null));
                    var fields=List.of(new SourceField("_id","int",TapstateType.INT64),new SourceField("parent_id","int",TapstateType.INT64));
                    port.schemas().save(new DiscoveredSourceModel("idle-input","mongodb",System.currentTimeMillis(),new SourceModel(List.of(
                            new SourceTable("orders",fields,List.of("_id"),List.of()),new SourceTable("lines",fields,List.of("_id"),List.of())))));
                    var draft=Map.of("pipelineId","idle-orders","mode","wizard","name","Idle orders","wizard",Map.of(
                            "root",Map.of("id","root","sourceId","idle-input","table","orders","key",List.of("_id"),"preTransforms",List.of()),
                            "related",List.of(Map.of("id","lines","parentId","root","sourceId","idle-input","table","lines","preTransforms",List.of(),
                                "relation",Map.of("on",List.of(Map.of("childField","parent_id","parentField","_id")),"shape","array","path","lines","key",List.of("_id"),"arrayKey",List.of("_id")))),
                            "transforms",List.of(),"output",Map.of("kind","source","config",Map.of("sourceId","views","table","idle_order_views","writeMode","upsert","onFullLoad","append"))));
                    request(http,base,token,"POST","/api/pipelines/idle-orders/draft",draft,201);
                    request(http,base,token,"POST","/api/pipelines/idle-orders/draft:preview",Map.of("revision",1),200);
                    request(http,base,token,"POST","/api/pipelines/idle-orders/draft:publish",Map.of("revision",1),200);
                    request(http,base,token,"POST","/api/pipelines/idle-orders:start",Map.of(),200);
                    await().atMost(Duration.ofSeconds(90)).until(()->data.getCollection("idle_order_views").countDocuments()==5);
                    Files.writeString(work.resolve("ready.json"),JsonWriter.write(request(http,base,token,"GET","/api/pipelines/idle-orders/status",null,200)));
                    var results = new ArrayList<Map<String, Object>>();
                    if (measure) {
                        measureIdle(raw, results);
                    } else {
                        // The batched observation path requires the deferred inspection to have completed.
                        var runtime = app.getBean(StorageRoot.class).runtime();
                        var original = runtime.current().orElseThrow();
                        assertThat(runtime.requireConfiguration().inspectStructure(new MongoReadBudget(Duration.ofSeconds(30)))).isEmpty();
                        assertThat(original.cache().inspectStructure(original.preparation().authScope(),
                                new MongoReadBudget(Duration.ofSeconds(30)))).isEmpty();
                        verifyWarmObservations(raw, storage);
                    }
                    var health=NativeAppStorage.business(app).getBean(PipelineAlertRuntime.class).health();
                    Files.writeString(work.resolve("health.txt"),health.toString());assertThat(health.closed()).isFalse();assertThat(health.degraded()).as(health.toString()).isFalse();
                    assertThat(data.getCollection("orders").countDocuments()).isEqualTo(5);
                    assertThat(data.getCollection("lines").countDocuments()).isEqualTo(5);
                    data.getCollection("lines").insertOne(new Document("_id",6L).append("parent_id",6L));
                    data.getCollection("orders").insertOne(new Document("_id",6L).append("parent_id",6L));
                    await().atMost(Duration.ofSeconds(30)).untilAsserted(()->{
                        var landed=data.getCollection("idle_order_views").find(new Document("_id",6L)).first();
                        assertThat(landed).isNotNull();assertThat(landed.getList("lines",Document.class)).hasSize(1);
                    });
                    var claims = raw.getDatabase(storage.cacheDatabase()).getCollection("workload_claims");
                    assertThat(claims.countDocuments(new Document("_id.resourceType", "NODE_SESSION")
                            .append("leaseUntil", java.util.Date.from(io.tapstate.spi.store.ProcessNodeSession.PROCESS_OWNED)))).isOne();
                    for (String kind : List.of("CAPTURE", "PIPELINE_ACTUATION")) {
                        var filter = new Document("_id.resourceType", kind);
                        filter.append("leaseUntil", new java.util.Date(-1)).append("nodeLeaseGeneration", new Document("$gt", 0L));
                        assertThat(claims.countDocuments(filter)).as("active %s lease mode", kind).isPositive();
                    }
                    if (exerciseChildEdits) {
                        var views = data.getCollection("idle_order_views");
                        var phases = "/api/pipelines/idle-orders";
                        request(http, base, token, "POST", phases + ":pause", Map.of(), 200);
                        awaitActual(http, base, token, phases, "PAUSED");
                        data.getCollection("lines").updateOne(new Document("_id", 6L), new Document("$set", new Document("parent_id", 1L)));
                        request(http, base, token, "POST", phases + ":resume", Map.of(), 200);
                        awaitActual(http, base, token, phases, "RUNNING");
                        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                                assertThat(views.find(new Document("_id", 1L)).first().getList("lines", Document.class)).hasSize(2));
                        data.getCollection("lines").deleteOne(new Document("_id", 6L));
                        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                                assertThat(views.find(new Document("_id", 1L)).first().getList("lines", Document.class))
                                        .extracting(child -> child.get("_id")).containsExactly(1L));
                        request(http, base, token, "POST", phases + ":stop", Map.of("purgeState", false), 200);
                        awaitActual(http, base, token, phases, "STOPPED");
                    }
                    System.out.println("IDLE_CDC_AFTER_WINDOW_PASSED");
                    for(var result:results) {
                        @SuppressWarnings("unchecked") var ops=(Map<String,Long>)result.get("opcounters");
                        double seconds=(double)result.get("seconds");
                        double perSecond=ops.values().stream().mapToLong(Long::longValue).sum()/seconds;
                        assertThat(perSecond).as("idle command rate in window %s",result.get("window")).isLessThan(100);
                        @SuppressWarnings("unchecked") var transactions=(Map<String,Long>)result.get("transactions");
                        assertThat(transactions.get("totalStarted")/seconds).as("idle transaction rate").isLessThanOrEqualTo(10);
                        assertThat((long)result.get("newThreads")).as("steady idle thread starts").isLessThanOrEqualTo(20);
                    }
                }
            }
        }
    }
    private static void awaitActual(HttpClient http, String base, String token, String path, String expected) {
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            var status = request(http, base, token, "GET", path + "/status", null, 200);
            assertThat(status.get("state")).isEqualTo(expected);
            assertThat(status.get("observedState")).isEqualTo(expected);
        });
    }

    private void measureIdle(com.mongodb.client.MongoClient raw, List<Map<String, Object>> results) throws Exception {
        Thread.sleep(30000);
        var admin=raw.getDatabase("admin");
        var bean=(com.sun.management.OperatingSystemMXBean)java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        var threads=java.lang.management.ManagementFactory.getThreadMXBean();
        for(int window=1;window<=2;window++){
            var before=admin.runCommand(new Document("serverStatus",1));long started=System.nanoTime();
            long cpu=bean.getProcessCpuTime(),startedThreads=threads.getTotalStartedThreadCount();
            Thread.sleep(30000);var after=admin.runCommand(new Document("serverStatus",1));
            double seconds=(System.nanoTime()-started)/1e9;var row=new LinkedHashMap<String,Object>();row.put("window",window);row.put("seconds",seconds);
            row.put("processVcpu",(bean.getProcessCpuTime()-cpu)/1e9/seconds);row.put("newThreads",threads.getTotalStartedThreadCount()-startedThreads);
            for(String section:List.of("opcounters","transactions")){
                var low=before.get(section,Document.class);var high=after.get(section,Document.class);var delta=new TreeMap<String,Long>();
                high.forEach((key,value)->{if(value instanceof Number n&&low.get(key) instanceof Number old)delta.put(key,n.longValue()-old.longValue());});row.put(section,delta);
            }
            results.add(row);System.out.println("IDLE_MEASUREMENT "+JsonWriter.write(row));
        }
        Files.writeString(work.resolve("windows.json"),JsonWriter.write(results));
    }

    private static void verifyWarmObservations(com.mongodb.client.MongoClient raw, NativeAppStorage storage) {
        var cache = raw.getDatabase(storage.cacheDatabase());
        var observations = cache.getCollection("pipeline_observation");
        var pipeline = new Document("pipelineId", "idle-orders").append("state", "RUNNING");
        await().atMost(Duration.ofSeconds(30)).until(() -> observations.countDocuments(pipeline) == 1);
        var original = observations.find(pipeline).first().getDate("observedAt");
        var configuration = raw.getDatabase(storage.configurationDatabase());
        configuration.runCommand(new Document("profile", 1)
                .append("filter", new Document("command.aggregate", "configuration_version")));
        try {
            var profile = configuration.getCollection("system.profile");
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(profile.countDocuments(new Document("command.aggregate", "configuration_version")))
                        .as("warm observation CONFIG end checks").isGreaterThanOrEqualTo(2);
                var observation = observations.find(pipeline).first();
                assertThat(observation).isNotNull();
                assertThat(observation.getDate("observedAt")).isAfter(original);
                assertThat(observation.get("snapshot", Document.class)).hasSize(2)
                        .allSatisfy((table, value) -> {
                            assertThat(value).isInstanceOf(Document.class);
                            var snapshot = (Document) value;
                            assertThat(snapshot.getBoolean("landed")).isTrue();
                            assertThat(snapshot.getLong("rowsDone")).isEqualTo(5L);
                        });
            });
        } finally {
            configuration.runCommand(new Document("profile", 0).append("filter", "unset"));
        }
    }

    @SuppressWarnings("unchecked")
    static Map<String,Object> request(HttpClient http,String base,String token,String method,String path,Object body,int expected)throws Exception {
        var b=HttpRequest.newBuilder(URI.create(base+path)).timeout(Duration.ofSeconds(60)).header("Content-Type","application/json");
        if(token!=null)b.header("Authorization","Bearer "+token);
        var response=http.send(b.method(method,body==null?HttpRequest.BodyPublishers.noBody():HttpRequest.BodyPublishers.ofString(JsonWriter.write(body))).build(),HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(path+": "+response.body()).isEqualTo(expected);
        return (Map<String,Object>)JsonReader.parse(response.body());
    }
}
