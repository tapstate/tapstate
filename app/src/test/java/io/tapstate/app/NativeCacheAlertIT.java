package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.*;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.net.URI;
import java.net.http.*;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/** A necessary CACHE write fails while CONFIG stays writable, including the original role's restart recovery. */
@RequiresDocker
class NativeCacheAlertIT {
    @Container static final MongoDBContainer MONGO=new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet","docker-rs","--setParameter","enableTestCommands=1");
    @TempDir Path work;
    @Test void anActualRequiredCacheCommandFailureCanBeAcceptedAfterTheDataMemberFailsClosed() throws Exception {
        var storage=NativeAppStorage.createOnPremWithAdmin(MONGO,"native_cache_alert","cache-admin","isolated-cache-password");
        var settings=new MongoConnectionSettings(storage.configurationUri(),null,Duration.ofSeconds(5));
        try(var observer=MongoClients.create(storage.configurationUri())) {
            var config=observer.getDatabase(storage.configurationDatabase());var admin=observer.getDatabase("admin");
            String[] args={"--server.address=127.0.0.1","--server.port=0","--logging.level.root=ERROR","--tapstate.store.mongo.uri="+storage.configurationUri(),
                    "--tapstate.hz.member-port=0","--tapstate.hz.jet.cooperative-thread-count=2","--tapstate.cluster.node-session-ttl=6s","--tapstate.cluster.node-session-renew-interval=1s",
                    "--tapstate.connectors.seed-dir="+work.resolve("no-seeds"),"--tapstate.connectors.plugins-dir="+work.resolve("plugins"),"--tapstate.control.auth.jwt-secret=cache-alert-fixture-signing"};
            AlarmKey originalKey;
            try(var app=new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(args)) {
                NativeAppStorage.awaitBusiness(app);var client=HttpClient.newHttpClient();String base="http://127.0.0.1:"+((WebServerApplicationContext)app).getWebServer().getPort();
                var login=post(client,base,"/auth/login",null,Map.of("username","cache-admin","password","isolated-cache-password"));
                String token=String.valueOf(login.get("token"));
                post(client,base,"/api/alerts/configuration/save",token,Map.of("kind","RULE","id","cache-required-write","etag","0","form",Map.of("name","Required CACHE access","typeId","cluster.storage-unavailable",
                        "resourceKinds",List.of("cluster-storage"),"allResources",true,"resources",List.of(),"parameters",Map.of(),"enabled",true)));
                await().atMost(Duration.ofSeconds(10)).until(()->config.getCollection("alert_node_lifecycles").countDocuments()==1);
                await().atMost(Duration.ofSeconds(10)).untilAsserted(()->{
                    var healthy=config.getCollection("alert_instances").find(new Document("_id.resourceKind","cluster-storage").append("_id.resourceId","cache")).first();
                    assertThat(healthy).isNotNull();assertThat(healthy).containsEntry("status","CLEAR");
                });
                var child=NativeAppStorage.business(app);var runtime=child.getBean(PipelineAlertRuntime.class);
                var entered=new java.util.concurrent.CountDownLatch(2);var release=new java.util.concurrent.CountDownLatch(1);
                Runnable block=()->{entered.countDown();try{if(!release.await(10,java.util.concurrent.TimeUnit.SECONDS))throw new AssertionError("bounded fixture gate expired");}
                    catch(InterruptedException stopped){Thread.currentThread().interrupt();}};
                org.springframework.test.util.ReflectionTestUtils.invokeMethod(runtime,"submit",null,0,block);
                org.springframework.test.util.ReflectionTestUtils.invokeMethod(runtime,"submit",null,0,block);
                assertThat(entered.await(5,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                admin.runCommand(new Document("configureFailPoint","failCommand").append("mode",new Document("times",1))
                        .append("data",new Document("namespace",storage.cacheDatabase()+".workload_claims").append("failCommands",List.of("findAndModify")).append("errorCode",8)));
                try {
                    await().atMost(Duration.ofSeconds(8)).until(()->!child.getBean(NodeSessionLease.class).active());
                    release.countDown();
                    await().atMost(Duration.ofSeconds(15)).untilAsserted(()->{
                        var raw=config.getCollection("alert_instances").find(new Document("_id.resourceKind","cluster-storage").append("_id.resourceId","cache")).first();
                        assertThat(raw).isNotNull();assertThat(raw).containsEntry("status","FIRING");
                    });
                } finally {release.countDown();admin.runCommand(new Document("configureFailPoint","failCommand").append("mode","off"));}
                try(var domain=MongoConfigurationDomain.open(settings,storage.clusterId())) {
                    var setup=domain.bindings().load().orElseThrow().setup();var scope=new AlarmKey.Scope(setup.installationId(),setup.clusterId());
                    var store=domain.alerts(new AlertStore.Host(scope,setup,AlertChannelPolicy.Deployment.ON_PREM,()->{}),BuiltinAlarmTypes.catalog(),Clock.systemUTC());
                    var head=store.active(null,10,new AlertStore.ReadAccess(true,Set.of(),()->{})).rows().stream()
                            .map(value->value.value()).filter(value->value.snapshot().rule().key().resource().id().equals("cache")).findFirst().orElseThrow();
                    originalKey=head.snapshot().rule().key();assertThat(head.operationalEvidence().failedCapabilities()).isEqualTo(1<<OperationalEvidence.Capability.CACHE_WRITE.ordinal());
                }
            }
            await().atMost(Duration.ofSeconds(10)).until(()->{
                var row=observer.getDatabase(storage.cacheDatabase()).getCollection("workload_claims").find(new Document("_id.resourceId",storage.clusterId())).first();
                return row==null || row.getDate("leaseUntil").before(Date.from(Instant.now()));
            });
            try(var restarted=new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(args);
                    var domain=MongoConfigurationDomain.open(settings,storage.clusterId())) {
                NativeAppStorage.awaitBusiness(restarted);var setup=domain.bindings().load().orElseThrow().setup();
                var store=domain.alerts(new AlertStore.Host(originalKey.scope(),setup,AlertChannelPolicy.Deployment.ON_PREM,()->{}),BuiltinAlarmTypes.catalog(),Clock.systemUTC());
                await().atMost(Duration.ofSeconds(40)).untilAsserted(()->{
                    var head=store.state(originalKey,new AlertStore.ReadAccess(true,Set.of(),()->{})).orElseThrow().value();
                    assertThat(head.snapshot().status()).isEqualTo(AlertSnapshot.Status.RESOLVED);
                    assertThat(head.episode().value().endReason()).isEqualTo(AlertSnapshot.EndReason.RECOVERED);
                });
            }
        }finally{
            try(var cleanup=MongoClients.create(storage.configurationUri())){cleanup.getDatabase(storage.configurationDatabase()).drop();cleanup.getDatabase(storage.cacheDatabase()).drop();cleanup.getDatabase(storage.masterDatabase()).drop();}
        }
    }
    private static Map<String,Object> post(HttpClient client,String base,String path,String token,Map<String,?> body)throws Exception {
        var builder=HttpRequest.newBuilder(URI.create(base+path)).header("Content-Type","application/json");if(token!=null)builder.header("Authorization","Bearer "+token);
        var reply=client.send(builder.POST(HttpRequest.BodyPublishers.ofString(JsonWriter.write(body))).build(),HttpResponse.BodyHandlers.ofString());
        assertThat(reply.statusCode()).as(reply.body()).isEqualTo(200);return AlertHttpModel.object(JsonReader.parse(reply.body()));
    }
}
