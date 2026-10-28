package io.tapstate.app;

import io.tapstate.cloud.sdk.client.ClusterEgressAddress;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.annotation.*;
import static org.assertj.core.api.Assertions.*;

/** Original Cloud cookie, admitted business context and real alert HTTP over isolated local storage. */
@RequiresDocker
class NativeCloudAlertManagementIT {
    static final String CLUSTER="browser-managed-cluster",ORG="browser-managed-organization",CLOUD="https://cloud.example.test";
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @TempDir Path work;
    @Test void originalCloudSessionSeesLocalAlertsAndCannotAuthorDirectOutboundChannels() throws Exception {
        var storage=NativeAppStorage.create(MONGO,"native_cloud_alert",StorageBinding.Mode.CLOUD,CLUSTER);
        try(var observer=com.mongodb.client.MongoClients.create(storage.configurationUri())){
            try(var application=new SpringApplicationBuilder(Bootstrap.class,ControlledSdk.class).environment(CloudFixtureEnvironment.isolated()).run(
                    "--server.address=127.0.0.1","--server.port="+Integer.getInteger("tapstate.alert.cloud-browser-port",0),"--logging.level.root=ERROR",
                    "--tapstate.store.mongo.uri="+storage.configurationUri(),"--tapstate.cloud.atlas-uri="+storage.configurationUri(),
                    "--tapstate.cloud.base-url="+CLOUD,"--tapstate.cloud.token=controlled-cloud-machine-token","--tapstate.cloud.cluster-id="+CLUSTER,
                    "--tapstate.hz.member-port=0","--tapstate.hz.jet.cooperative-thread-count=2","--SDK_STATUS_SENDER_ENABLED=false",
                    "--tapstate.connectors.seed-dir="+CloudConnectorTestInputs.seedDirectory(),"--tapstate.connectors.plugins-dir="+work.resolve("plugins"),
                    "--spring.web.resources.static-locations="+System.getProperty("tapstate.alert.cloud-browser-assets","classpath:/static/"))){
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(()->assertThat(application.getBean(StorageRoot.class).runtime().business()).as("phase="+application.getBean(StorageRoot.class).phase()).isPresent());
                NativeAppStorage.awaitBusiness(application);int port=((WebServerApplicationContext)application).getWebServer().getPort();String base="http://localhost:"+port;
                var client=HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();
                var exchanged=client.send(HttpRequest.newBuilder(URI.create(base+"/auth/exchange?code=browser-code")).GET().build(),HttpResponse.BodyHandlers.ofString());
                assertThat(exchanged.statusCode()).isEqualTo(302);assertThat(exchanged.headers().firstValue("location")).hasValue("/");String cookie=exchanged.headers().firstValue("set-cookie").orElseThrow().split(";",2)[0];
                java.util.function.BiFunction<String,Map<String,Object>,HttpResponse<String>> post=(path,body)->{try{return client.send(HttpRequest.newBuilder(URI.create(base+"/api/alerts/"+path)).header("Cookie",cookie).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString(JsonWriter.write(body))).build(),HttpResponse.BodyHandlers.ofString());}catch(Exception failure){throw new AssertionError(failure);}};
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(()->{var reply=post.apply("catalog",Map.of());assertThat(reply.statusCode()).isEqualTo(200);assertThat(reply.body()).contains("CLOUD_MANAGED");});
                assertThat(post.apply("center",Map.of()).statusCode()).isEqualTo(200);
                var saved=post.apply("configuration/save",Map.of("kind","RULE","id","managed-rule","etag","0","form",Map.of("name","Managed rule","typeId","pipeline.failed","resourceKinds",List.of("pipeline"),"allResources",true,"resources",List.of(),"parameters",Map.of(),"enabled",true)));
                assertThat(saved.statusCode()).as(saved.body()).isEqualTo(200);
                assertThat(post.apply("configuration/list",Map.of("kind","RULE")).body()).contains("Managed rule");
                var denied=post.apply("configuration/list",Map.of("kind","RECEIVER"));assertThat(denied.statusCode()).isEqualTo(409);assertThat(denied.body()).contains("alert.stale-action","managed-external-configuration").doesNotContain("credentialId","ciphertext");
                String control=System.getProperty("tapstate.alert.cloud-browser-control");if(control!=null){var directory=Path.of(control);Files.createDirectories(directory);Files.writeString(directory.resolve("ready.json"),JsonWriter.write(Map.of("port",port,"mode","actual-native-cloud","sdk","controlled-local-authentication","clusterId",CLUSTER,"organizationId",ORG)));
                    long until=System.nanoTime()+Duration.ofMinutes(45).toNanos();while(!Files.exists(directory.resolve("stop"))&&System.nanoTime()<until)Thread.sleep(100);}
            }finally{observer.getDatabase(storage.configurationDatabase()).drop();observer.getDatabase(storage.cacheDatabase()).drop();observer.getDatabase(storage.masterDatabase()).drop();}
        }
    }
    @Configuration(proxyBeanMethods=false) static class ControlledSdk {
        @Bean @Primary CloudSdkBridge browserSdk(CloudRuntimeSettings settings){return new CloudSdkBridge(settings,new CloudSdkBridge.SdkRuntime(){
            public CloudSdkBridge.ExchangeResult exchange(String code){if(!code.startsWith("browser-code"))throw new CloudSdkBridge.ProviderFailure();return new CloudSdkBridge.ExchangeResult("controlled-cloud-browser-jwt-"+code,ORG,CLUSTER,"Browser organization","Browser Cloud Cluster","local","203.0.113.42","STABLE");}
            public CloudSdkBridge.VerifiedClaims verify(String jwt){if(!jwt.startsWith("controlled-cloud-browser-jwt-browser-code"))throw new CloudSdkBridge.ProviderFailure();return new CloudSdkBridge.VerifiedClaims("cloud-browser-operator",ORG,CLUSTER,"browser-jti-"+jwt.substring("controlled-cloud-browser-jwt-".length()),Instant.now().plusSeconds(3600),CLOUD,"localhost",List.of("workload:write"));}
            public ClusterEgressAddress currentEgressAddress(){return new ClusterEgressAddress(ORG,CLUSTER,"203.0.113.42","STABLE");}
            public boolean verifyCallback(String method,String timestamp,String nonce,String data,String signature){throw new AssertionError("no callback");}
            public void report(String cluster,String nonce,io.tapstate.control.core.CloudRuntimeStatus status,String token){throw new AssertionError("status reporting disabled");}
        });}
    }
}
