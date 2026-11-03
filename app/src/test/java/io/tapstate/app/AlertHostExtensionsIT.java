package io.tapstate.app;

import com.sun.net.httpserver.*;
import io.tapstate.adapters.alerting.AlarmTypeText;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.*;
import io.tapstate.core.lifecycle.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.math.BigDecimal;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.KeyStore;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import javax.net.ssl.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static io.tapstate.app.AlertHttpModel.*;

/** Test-only static types traverse the actual bounded host, CONFIG, HTTP management and original network channels. */
@RequiresDocker
class AlertHostExtensionsIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    static final String METRIC="test.queue-high",TYPED="test.replica-unavailable";
    static final String ORG="212345678901234567890123",CLUSTER="312345678901234567890123";
    @TempDir static Path work;
    static AlarmTypeCatalog catalog() {
        var types=new ArrayList<>(BuiltinAlarmTypes.catalog().types());
        types.add(new AlarmTypeCatalog.Type(METRIC,1,AlarmTypeCatalog.Kind.STATEFUL,Set.of("pipeline"),"pipeline-observations","threshold",
                List.of(new AlarmTypeCatalog.Parameter("threshold",AlarmTypeCatalog.ParameterKind.DECIMAL,"1",BigDecimal.ZERO,BigDecimal.valueOf(1000),BigDecimal.TEN),
                        new AlarmTypeCatalog.Parameter("recovery-threshold",AlarmTypeCatalog.ParameterKind.DECIMAL,"1",BigDecimal.ZERO,BigDecimal.valueOf(1000),BigDecimal.valueOf(5))),
                AlarmSeverity.MINOR,false,METRIC+".condition",METRIC+".recovery",METRIC+".guidance"));
        types.add(new AlarmTypeCatalog.Type(TYPED,1,AlarmTypeCatalog.Kind.STATEFUL,Set.of("cluster"),"test-replica-health","condition",List.of(),
                AlarmSeverity.MAJOR,false,TYPED+".condition",TYPED+".recovery",TYPED+".guidance"));
        var template=new MetricAlertEvaluator.Rule("test.queue",MetricType.GAUGE,"1",Map.of(),MetricAlertEvaluator.Direction.ABOVE,BigDecimal.TEN,BigDecimal.valueOf(5));
        return new AlarmTypeCatalog(types,Map.of(METRIC,new AlarmTypeCatalog.MetricPreset(template,"threshold","recovery-threshold")));
    }
    static AlarmTypeText descriptions() {
        var values=new HashMap<String,String>();
        for(String type:List.of(METRIC,TYPED)) {
            values.put(type+".condition","Test-only original condition");values.put(type+".recovery","Test-only same-owner recovery");values.put(type+".guidance","Inspect the test-only original input");
        }
        return AlarmTypeText.english().with(values);
    }
    @BeforeAll static void certificates() throws Exception {AlertDeliveryNetworkIT.temp=work;AlertDeliveryNetworkIT.tls();}
    @Test void onPremTypesReuseCenterEmailAndWebhook() throws Exception {exercise(false);}
    @Test void cloudTypesReuseCenterAndRealSdkEventReceipts() throws Exception {exercise(true);}
    private void exercise(boolean cloud) throws Exception {
        var tls=AlertDeliveryNetworkIT.serverTls;
        var accepted=new CopyOnWriteArrayList<Map<String,Object>>();var errors=new CopyOnWriteArrayList<Throwable>();
        var server=HttpsServer.create(new InetSocketAddress("localhost",0),0);var executor=Executors.newFixedThreadPool(2);
        server.setHttpsConfigurator(new HttpsConfigurator(tls));server.setExecutor(executor);
        server.createContext("/",exchange->{try {
            var body=object(JsonReader.parse(new String(exchange.getRequestBody().readAllBytes(),StandardCharsets.UTF_8)));
            accepted.add(body);String response="{}";
            if(cloud) {
                assertThat(exchange.getRequestURI().getPath()).isEqualTo("/v1/api/clusters/"+CLUSTER+"/events");
                assertThat(exchange.getRequestHeaders().getFirst("Authorization")).isEqualTo("Bearer fixture-extension-token");
                response=JsonWriter.write(map("code","ok","opId","extension-request","data",map("id",String.format("%024x",accepted.size()),"orgId",ORG,
                        "source","cluster","type",body.get("type"),"dedupeKey",body.get("dedupeKey"),"schemaVersion",1,"payload",map("clusterId",CLUSTER))));
            }
            byte[] bytes=response.getBytes(StandardCharsets.UTF_8);exchange.getResponseHeaders().set("Content-Type","application/json");exchange.sendResponseHeaders(200,bytes.length);exchange.getResponseBody().write(bytes);
        }catch(Throwable failure){errors.add(failure);}finally{exchange.close();}});
        server.start();var origin=URI.create("https://localhost:"+server.getAddress().getPort());
        try(var smtp=cloud?null:new AlertDeliveryNetworkIT.SmtpPeer(true)) {
            var scope=cloud?new AlarmKey.Scope(ORG,CLUSTER):AlertEvaluationHostIT.SCOPE;
            AlertEvaluationHostIT.withFixture(false,cloud,scope,original->{try{run(original,cloud,origin,smtp,accepted);}catch(Exception failure){throw new AssertionError(failure);}});
            assertThat(errors).isEmpty();assertThat(accepted).hasSize(4);
            if(cloud)assertThat(accepted.stream().map(value->value.get("type")).toList()).containsExactlyInAnyOrder(
                    "cluster.alert."+METRIC+".fault","cluster.alert."+TYPED+".fault","cluster.alert."+METRIC+".recovery","cluster.alert."+TYPED+".recovery");
            else assertThat(smtp.bodies).hasSize(4);
        }finally{server.stop(0);executor.shutdownNow();assertThat(executor.awaitTermination(3,TimeUnit.SECONDS)).isTrue();}
    }
    private void run(AlertEvaluationHostIT.Fixture f,boolean cloud,URI origin,AlertDeliveryNetworkIT.SmtpPeer smtp,List<Map<String,Object>> received) throws Exception {
        f.now().set(Instant.now());var catalog=catalog();var deployment=f.storeHost().deployment();
        var properties=new CloudProperties();if(cloud){properties.setBaseUrl(origin.toString());properties.setClusterId(CLUSTER);properties.setToken("fixture-extension-token");properties.setAtlasUri(f.settings().uri());}
        var cloudSettings=CloudRuntimeSettings.resolve(properties);var capabilities=AlertChannels.capabilities(deployment,cloudSettings);
        var before=f.storeHost();String managed=cloud?AlertNotificationPlanner.managedBinding(origin,before.scope(),before.setup()):null;
        var host=new AlertStore.Host(before.scope(),before.setup(),deployment,()->{},cloud?new AlertIntent.SystemRoute("managed-events",managed,1):null,
                cloud?new AlertIntent.SdkTarget(managed,CLUSTER,managed):null,r->Optional.empty(),capabilities);
        var store=f.configuration().alerts(host,catalog,f.clock());
        var evaluator=new AlertEvaluationHost(store,catalog,deployment,f.clock(),"extension-evaluator",()->{});
        var planner=new AlertNotificationPlanner(store,f.clock(),"extension-planner",cloud?origin:null,()->{});evaluator.withPlanner(planner);
        var service=new AlertManagement(()->new PipelineAlertRuntime.Services(store,evaluator,catalog,null,null,
                Set.of("pipeline-lifecycle","pipeline-observations","test-replica-health"),descriptions()),
                caller->cloud?AlertStore.Access.cloud("operator",ORG,"ADMIN",()->{},()->{}):AlertNotificationPlannerIT.access(),
                ()->new PipelineAlertRuntime.Health(false,false,0,null,Map.of(),0,0),host.scope(),deployment,f.clock(),new io.tapstate.control.core.AuditGate(r->{},f.clock()));
        try(var app=new SpringApplicationBuilder(io.tapstate.control.restapi.AlertTestHttpFace.class)
                .initializers(context->context.getBeanFactory().registerSingleton("alertControlDispatcher",service)).run("--server.port=0","--server.address=127.0.0.1","--logging.level.root=ERROR")) {
            var client=HttpClient.newHttpClient();String base="http://127.0.0.1:"+((WebServerApplicationContext)app).getWebServer().getPort();
            var catalogReply=post(client,base,"catalog",Map.of());assertThat(catalogReply.toString()).contains(METRIC,TYPED,"Test-only original condition");
            for(String type:List.of(METRIC,TYPED))post(client,base,"configuration/save",map("kind","RULE","id",type.equals(METRIC)?"extension-metric":"extension-typed","etag","0",
                    "form",map("name",type,"typeId",type,"resourceKinds",List.of(type.equals(METRIC)?"pipeline":"cluster"),"allResources",true,"resources",List.of(),"parameters",Map.of(),"enabled",true)));
            if(!cloud)routes(client,base,origin,smtp);
            var trust=trust();var channels=new AlertChannels(deployment,cloudSettings,f.clock(),()->{},cloud?ORG:null,
                    hostname->new InetAddress[]{InetAddress.getLoopbackAddress()},trust);
            try(var worker=new AlertDeliveryRuntime(store,f.clock(),"extension-sender",()->{},channels,authority->AlertNotificationPlannerIT.access());
                    var runtime=new PipelineAlertRuntime(()->new PipelineAlertRuntime.Services(store,evaluator,catalog,null,null,Set.of("pipeline-lifecycle","pipeline-observations","test-replica-health"),descriptions()),
                            f.configuration().intents(),(scope,fence)->Optional.of(()->{}),()->{},f.clock())) {
                runtime.tick();await().atMost(Duration.ofSeconds(5)).until(()->!runtime.health().degraded() && runtime.health().queuedInputs()==0);
                var pass=org.mockito.Mockito.mock(PipelineConvergencePass.class);org.mockito.Mockito.when(pass.scope()).thenReturn(f.intent().scope());
                org.mockito.Mockito.when(pass.claim()).thenReturn(WorkloadClaimFence.from(f.claim()));org.mockito.Mockito.when(pass.intent()).thenReturn(f.intent());
                var typed=new ReplicaProvider(runtime,new AlarmKey.Resource("cluster",host.scope().clusterId(),host.setup().installationId()));
                publish(runtime,pass,f.clock().instant(),11);typed.report(f.clock().instant(),false);
                await().atMost(Duration.ofSeconds(5)).until(()->runtime.health().queuedInputs()==0);
                assertThat(post(client,base,"center",Map.of())).containsEntry("total",2L);
                planner.tick();deliver(worker,store,channels,f.now(),cloud?2:4);assertThat(received).hasSize(2);
                f.now().set(Instant.now().plusSeconds(2));publish(runtime,pass,f.clock().instant(),4);typed.report(f.clock().instant(),true);
                await().atMost(Duration.ofSeconds(5)).until(()->runtime.health().queuedInputs()==0);planner.tick();deliver(worker,store,channels,f.now(),cloud?4:8);
                assertThat(post(client,base,"center",Map.of())).containsEntry("total",0L);
                var history=post(client,base,"center",Map.of("view","history"));assertThat((List<?>)history.get("rows")).hasSize(2);
                var deliveries=store.deliveries(null,100,null,null,f.reads()).rows();assertThat(deliveries).hasSize(cloud?4:8)
                        .allSatisfy(value->{assertThat(value.value().state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
                            if(cloud){assertThat(value.value().state().acceptedReceipt().source()).isEqualTo("sdk-event-center");assertThat(value.value().state().acceptedReceipt().remoteId()).isNotBlank();}});
            }
        }
    }
    private static void routes(HttpClient client,String base,URI origin,AlertDeliveryNetworkIT.SmtpPeer smtp) throws Exception {
        String ca=new String(AlertDeliveryNetworkIT.pem,StandardCharsets.US_ASCII);
        post(client,base,"configuration/save",map("kind","RECEIVER","id","extension-email","etag","0","form",map("name","Extension SMTP","enabled",true,
                "transport",map("channelType","email","host","localhost","port",smtp.port(),"encryption","IMPLICIT_TLS","authentication","NONE","from","alerts@example.test","recipients",List.of("one@example.test"),"trust","PRIVATE_CA"),"privateCa",ca)));
        post(client,base,"configuration/save",map("kind","RECEIVER","id","extension-webhook","etag","0","form",map("name","Extension Webhook","enabled",true,
                "transport",map("channelType","webhook","authentication","NONE","trust","PRIVATE_CA"),"endpoint",origin+"/webhook","privateCa",ca)));
        post(client,base,"configuration/save",map("kind","SUBSCRIPTION","id","extension-route","etag","0","form",map("name","Extension route","enabled",true,
                "resourceKinds",List.of("pipeline","cluster"),"allResources",true,"resources",List.of(),"typeIds",List.of(METRIC,TYPED),"groupBy",List.of("TYPE"),"initialWaitSeconds",0,
                "channels",List.of(map("channelType","email","receivers",List.of("extension-email")),map("channelType","webhook","receivers",List.of("extension-webhook"))))));
    }
    private static Map<String,Object> post(HttpClient client,String base,String operation,Map<String,?> body) throws Exception {
        var reply=client.send(HttpRequest.newBuilder(URI.create(base+"/api/alerts/"+operation)).header("Authorization","Bearer cyxt_alert-browser.fixture")
                .header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString(JsonWriter.write(body))).build(),HttpResponse.BodyHandlers.ofString());
        assertThat(reply.statusCode()).as(reply.body()).isEqualTo(200);return object(JsonReader.parse(reply.body()));
    }
    private static void publish(PipelineAlertRuntime runtime,PipelineConvergencePass pass,Instant at,long value) {
        runtime.published(pass,new Observation(pass.scope().pipelineId(),PipelineState.RUNNING,Map.of(),Map.of(),Map.of(),null,at,
                List.of(MetricFact.single("test.queue",MetricType.GAUGE,"1",MetricPoint.reading(Map.of(),at,value)))),null);
    }
    private record ReplicaProvider(PipelineAlertRuntime runtime,AlarmKey.Resource resource) {
        void report(Instant at,boolean available){runtime.resourceInput(resource,TYPED,0,"test-replica-original",()->{},
                new AlertInput.Conditions(at,available?AlertSignal.Condition.GOOD:AlertSignal.Condition.BAD,null));}
    }
    private static void deliver(AlertDeliveryRuntime worker,AlertStore store,AlertChannels channels,java.util.concurrent.atomic.AtomicReference<Instant> now,int expected) {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(()->{now.updateAndGet(previous->{var real=Instant.now();return real.isAfter(previous)?real:previous.plusMillis(1);});worker.tick();AlertDeliveryRuntimeIT.settled(worker);
            var rows=store.deliveries(null,100,null,null,new AlertStore.ReadAccess(true,Set.of(),()->{})).rows();
            var dns=org.springframework.test.util.ReflectionTestUtils.getField(channels,"dns");
            assertThat(rows).hasSize(expected);
            assertThat(rows.stream().map(row->row.value().state().summary()).toList())
                    .as("worker=%s DNS=%s protocols=%s",worker.failure(),dns,rows.stream().flatMap(row->row.value().state().attempts().stream()).map(AlertIntent.Attempt::protocol).toList())
                    .allMatch(value->value==DeliveryEvidence.Outcome.ACCEPTED);
        });
    }
    private static SSLContext trust() throws Exception {
        var certificate=java.security.cert.CertificateFactory.getInstance("X.509").generateCertificate(new java.io.ByteArrayInputStream(AlertDeliveryNetworkIT.pem));
        var store=KeyStore.getInstance(KeyStore.getDefaultType());store.load(null);store.setCertificateEntry("fixture",certificate);
        var managers=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());managers.init(store);
        var context=SSLContext.getInstance("TLS");context.init(null,managers.getTrustManagers(),null);return context;
    }
}
