package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.testsupport.RequiresDocker;
import java.net.URI;
import java.net.http.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/** Actual admitted child, original human authentication, HTTP dispatch and CONFIG authoring. */
@RequiresDocker
class NativeAlertManagementIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @TempDir Path work;
    @Test void originalNativeHumanSessionOwnsAlertAuthoringWithoutCallerSelectedScope() throws Exception {
        var storage=NativeAppStorage.createOnPremWithAdmin(MONGO,"native_alert","alert-operator","isolated-native-password");
        try(var observer=MongoClients.create(storage.configurationUri())){
            var configuration=observer.getDatabase(storage.configurationDatabase());
            var setup=configuration.getCollection("storage_setup").find(new Document("_id","cluster")).first();
            var oldMinute=java.time.Instant.now().minusSeconds(7200).truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
            var expiredQuotaId=new Document("authorityId",setup.getString("installationId")).append("clusterId",storage.clusterId()).append("minute",oldMinute.toString());
            configuration.getCollection("alert_test_quotas").insertOne(new Document("_id",expiredQuotaId).append("formatVersion",1).append("recordRevision",1L)
                    .append("cleanupAt",java.util.Date.from(oldMinute.plusSeconds(3660))).append("body",new Document("actors",List.of(new Document("actor","retention-probe").append("accepted",1)))));
            try(var application=new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                    "--server.address=127.0.0.1","--server.port="+Integer.getInteger("tapstate.alert.native-browser-port",0),"--logging.level.root=ERROR","--tapstate.store.mongo.uri="+storage.configurationUri(),
                    "--tapstate.hz.member-port=0","--tapstate.hz.jet.cooperative-thread-count=2","--tapstate.alerting.retention.ended-episode=P7D","--tapstate.connectors.seed-dir="+work.resolve("no-seeds"),
                    "--tapstate.connectors.plugins-dir="+work.resolve("plugins"),"--tapstate.control.auth.jwt-secret=native-alert-test-signing-key")){
                NativeAppStorage.awaitBusiness(application);int port=((WebServerApplicationContext)application).getWebServer().getPort();var client=HttpClient.newHttpClient();String root="http://127.0.0.1:"+port;
                var login=post(client,root,"/auth/login",null,Map.of("username","alert-operator","password","isolated-native-password"));assertThat(login.statusCode()).isEqualTo(200);String token=(String)AlertHttpModel.object(JsonReader.parse(login.body())).get("token");assertThat(token).isNotBlank();
                await().atMost(Duration.ofSeconds(20)).untilAsserted(()->{var catalog=post(client,root,"/api/alerts/catalog",token,Map.of());assertThat(catalog.statusCode()).isEqualTo(200);assertThat(AlertHttpModel.object(JsonReader.parse(catalog.body()))).containsEntry("clusterId",storage.clusterId()).containsEntry("deployment","ON_PREM");});
                await().atMost(Duration.ofSeconds(30)).untilAsserted(()->{
                    var read=post(client,root,"/api/alerts/health",token,Map.of());assertThat(read.statusCode()).isEqualTo(200);
                    assertThat(AlertHttpModel.object(AlertHttpModel.object(JsonReader.parse(read.body())).get("maintenance"))).containsEntry("capacitySampleStale",false);
                });
                var form=Map.of("name","Native UI rule","typeId","pipeline.failed","typeVersion",1,"resourceKinds",List.of("pipeline"),"allResources",true,"resources",List.of(),"parameters",Map.of(),"severity","DEFAULT","enabled",true);
                var saved=post(client,root,"/api/alerts/configuration/save",token,Map.of("kind","RULE","id","native-rule","etag","0","form",form));assertThat(saved.statusCode()).isEqualTo(200);
                var list=post(client,root,"/api/alerts/configuration/list",token,Map.of("kind","RULE"));assertThat(list.statusCode()).isEqualTo(200);assertThat(list.body()).contains("Native UI rule").doesNotContain("isolated-native-password",token);
                var raw=observer.getDatabase(storage.configurationDatabase()).getCollection("alert_rules").find(new Document("_id.id","native-rule")).first();assertThat(raw).isNotNull();assertThat(raw.toJson()).contains("alert-operator");
                assertThat(post(client,root,"/api/alerts/center",token,Map.of("authorityId","foreign")).statusCode()).isEqualTo(400);
                await().atMost(Duration.ofSeconds(30)).untilAsserted(()->{
                    var health=post(client,root,"/api/alerts/health",token,Map.of());assertThat(health.statusCode()).isEqualTo(200);
                    var body=AlertHttpModel.object(JsonReader.parse(health.body()));assertThat(body).containsEntry("cleanupAvailable",true);
                    var maintenance=AlertHttpModel.object(body.get("maintenance"));assertThat(maintenance.get("lastSuccess")).isNotNull();
                    assertThat(((Number)maintenance.get("changed")).longValue()).isPositive();
                    assertThat(configuration.getCollection("alert_test_quotas").find(new Document("_id",expiredQuotaId)).first()).isNull();
                    assertThat(maintenance).containsEntry("capacitySampleStale",false);assertThat(((Number)maintenance.get("logicalBytes")).longValue()).isGreaterThanOrEqualTo(0);
                });
                assertThat(NativeAppStorage.business(application).getBean(PipelineAlertRuntime.class).managementServices().evaluator().retentionPolicy().duration(io.tapstate.core.alerting.AlertRetentionPolicy.Setting.ENDED_EPISODE)).isEqualTo(Duration.ofDays(7));
                assertThat(NativeAppStorage.business(application).getBean(PipelineAlertRuntime.class).managementServices().store().maintenance().orElseThrow().inventory().logicalBytes()).isPositive();
                String receipt=System.getProperty("tapstate.alert.maintenance-receipt");
                if(receipt!=null){var read=post(client,root,"/api/alerts/health",token,Map.of());var evidence=new LinkedHashMap<String,Object>();
                    evidence.put("mode","actual-native-op");evidence.put("clusterId",storage.clusterId());evidence.put("expiredQuotaAbsent",configuration.getCollection("alert_test_quotas").find(new Document("_id",expiredQuotaId)).first()==null);
                    evidence.put("maintenance",AlertHttpModel.object(JsonReader.parse(read.body())).get("maintenance"));java.nio.file.Files.writeString(Path.of(receipt),JsonWriter.write(evidence));}
                String control=System.getProperty("tapstate.alert.native-browser-control");
                if(control!=null){var directory=Path.of(control);java.nio.file.Files.createDirectories(directory);AlertDeliveryNetworkIT.temp=work;AlertDeliveryNetworkIT.tls();
                    try(var smtp=new AlertDeliveryNetworkIT.SmtpPeer(true)){
                        java.nio.file.Files.write(directory.resolve("smtp-ca.pem"),AlertDeliveryNetworkIT.pem);
                        var browserData=new NativeAlertBrowserData(application,storage,(operation,input)->{try{var reply=post(client,root,"/api/alerts/"+operation,token,input);assertThat(reply.statusCode()).as(reply.body()).isEqualTo(200);}catch(Exception failure){throw new AssertionError(failure);}});
                        if(Boolean.getBoolean("tapstate.alert.native-route-probe")){
                            var receiver=Map.of("name","Native route probe","enabled",true,"transport",Map.of("channelType","email","host","localhost","port",smtp.port(),"encryption","IMPLICIT_TLS","authentication","NONE","from","alerts@example.test","recipients",List.of("a@example.test"),"trust","PRIVATE_CA"),"privateCa",new String(AlertDeliveryNetworkIT.pem,java.nio.charset.StandardCharsets.US_ASCII));
                            assertThat(post(client,root,"/api/alerts/configuration/save",token,Map.of("kind","RECEIVER","id","probe-receiver","etag","0","form",receiver)).statusCode()).isEqualTo(200);
                            var failureTransport=new LinkedHashMap<String,Object>(AlertHttpModel.object(receiver.get("transport")));failureTransport.put("trust","SYSTEM");var failureReceiver=new LinkedHashMap<String,Object>(receiver);failureReceiver.put("name","Native TLS rejection");failureReceiver.put("transport",failureTransport);failureReceiver.remove("privateCa");
                            assertThat(post(client,root,"/api/alerts/configuration/save",token,Map.of("kind","RECEIVER","id","probe-untrusted","etag","0","form",failureReceiver)).statusCode()).isEqualTo(200);
                            var destinations=new ArrayList<Map<String,Object>>();destinations.add(Map.of("channelType","email","receivers",List.of("probe-receiver","probe-untrusted")));
                            String peerDirectory=System.getProperty("tapstate.alert.native-probe-peers");if(peerDirectory!=null){var peers=Path.of(peerDirectory);var peer=AlertHttpModel.object(JsonReader.parse(java.nio.file.Files.readString(peers.resolve("ready.json"))));
                                var hook=Map.of("name","Native rejected Webhook","enabled",true,"transport",Map.of("channelType","webhook","authentication","NONE","trust","PRIVATE_CA"),"endpoint","https://localhost:"+peer.get("httpsPort")+"/rejected","privateCa",java.nio.file.Files.readString(peers.resolve("ca.pem")));
                                assertThat(post(client,root,"/api/alerts/configuration/save",token,Map.of("kind","RECEIVER","id","probe-webhook","etag","0","form",hook)).statusCode()).isEqualTo(200);destinations.add(Map.of("channelType","webhook","receivers",List.of("probe-webhook")));}
                            var route=Map.of("name","Native route probe","enabled",true,"resourceKinds",List.of("pipeline"),"allResources",true,"resources",List.of(),"channels",destinations,"groupBy",List.of("RESOURCE"),"initialWaitSeconds",0,"inhibition",List.of(Map.of("rootType","cluster.storage-unavailable","dependentType","pipeline.failed")));
                            var savedRoute=post(client,root,"/api/alerts/configuration/save",token,Map.of("kind","SUBSCRIPTION","id","probe-route","etag","0","form",route));assertThat(savedRoute.statusCode()).as(savedRoute.body()).isEqualTo(200);
                            var services=NativeAppStorage.business(application).getBean(PipelineAlertRuntime.class).managementServices();
                            await().atMost(Duration.ofSeconds(30)).untilAsserted(()->assertThat(services.store().deliveries(null,100,null,null,new io.tapstate.spi.store.AlertStore.ReadAccess(true,Set.of(),()->{})).rows()).isNotEmpty());
                        }
                        java.nio.file.Files.writeString(directory.resolve("ready.json"),JsonWriter.write(Map.of("port",port,"smtpPort",smtp.port(),"smtpHost","localhost","clusterId",storage.clusterId(),"username","alert-operator","mode","actual-native-op","transport","real-local-tls-smtp")));
                        long until=System.nanoTime()+Duration.ofMinutes(60).toNanos();while(!java.nio.file.Files.exists(directory.resolve("stop"))&&System.nanoTime()<until){if(java.nio.file.Files.exists(directory.resolve("recover"))&&!java.nio.file.Files.exists(directory.resolve("recovered"))){browserData.recover();java.nio.file.Files.createFile(directory.resolve("recovered"));}
                            if(java.nio.file.Files.exists(directory.resolve("diagnostic"))&&!java.nio.file.Files.exists(directory.resolve("diagnostic-refreshed"))){browserData.refreshDiagnostic();java.nio.file.Files.createFile(directory.resolve("diagnostic-refreshed"));}Thread.sleep(100);}
                        java.nio.file.Files.writeString(directory.resolve("smtp-receipts.json"),JsonWriter.write(Map.of("count",smtp.bodies.size(),"sha256",smtp.bodies.stream().map(body->{try{return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(body.getBytes(java.nio.charset.StandardCharsets.US_ASCII)));}catch(java.security.NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}}).toList())));
                    }
                }

            } finally {observer.getDatabase(storage.configurationDatabase()).drop();observer.getDatabase(storage.cacheDatabase()).drop();observer.getDatabase(storage.masterDatabase()).drop();}
        }
    }
    private static HttpResponse<String> post(HttpClient client,String root,String path,String token,Map<String,?> body) throws Exception {
        var request=HttpRequest.newBuilder(URI.create(root+path)).header("Content-Type","application/json").timeout(Duration.ofSeconds(20));if(token!=null)request.header("Authorization","Bearer "+token);
        return client.send(request.POST(HttpRequest.BodyPublishers.ofString(JsonWriter.write(body))).build(),HttpResponse.BodyHandlers.ofString());
    }
}
