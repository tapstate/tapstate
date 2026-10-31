package io.tapstate.app;

import com.mongodb.client.MongoClients;
import com.sun.net.httpserver.*;
import io.tapstate.adapters.alerting.WebhookChannel;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.app.AlertHttpModel.*;
import static io.tapstate.app.AlertNotificationPlannerIT.*;

/** A new test-only registration traverses ordinary HTTP authoring, CONFIG, planning, permission and pinned TLS IO. */
@RequiresDocker
class AlertHttpsJsonExtensionIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @TempDir static Path work;
    static final String TYPE="test.sink";
    static AlertChannelRegistry registry(){
        var descriptor=new AlertChannelCatalog.Descriptor(new AlertChannelPolicy.Capability(TYPE,1,AlertChannelPolicy.Family.OP_EXTERNAL),"Test sink",
                AlertChannelCatalog.TransportFamily.HTTPS_JSON,1,List.of(new AlertChannelCatalog.Field("topic","Topic",64,true,null)));
        return new AlertChannelRegistry(List.of(new AlertChannelRegistry.Registration(descriptor,c->new WebhookChannel(c.deployment(),c.binding(),c.endpoint(),
                Set.of(new WebhookChannel.Origin(c.endpoint().getHost(),c.endpoint().getPort())),c.tls(),c.bearer(),c.signingKey(),c.clock(),c.addresses(),c.capability()))));
    }
    @BeforeAll static void tls() throws Exception {AlertDeliveryNetworkIT.temp=work;AlertDeliveryNetworkIT.tls();}
    private static AlertReceiver genericReceiver(AlertEvaluationHostIT.Fixture f,AlertStore store){
        var author=new AlertAuthoring(store,f.storeHost().scope(),f.storeHost().deployment(),BuiltinAlarmTypes.catalog(),f.clock(),registry().catalog());
        return (AlertReceiver)author.save(map("kind","RECEIVER","id","generic","etag","0","form",map("name","Generic","enabled",true,
                "transport",map("channelType",TYPE,"family","HTTPS_JSON","transportVersion",1,"authentication","NONE","trust","SYSTEM","parameters",Map.of("topic","orders")),
                "endpoint","https://notify.example.test/private-hook")),access(),false);
    }
    @Test void uninstalledNodeCannotWriteAnOptionalFormatReceiver(){
        AlertEvaluationHostIT.withFixture(f->{var capable=f.configuration().alerts(f.storeHost().withChannels(AlertChannels.capabilities(f.storeHost().deployment(),null,registry())),BuiltinAlarmTypes.catalog(),f.clock());
            var receiver=genericReceiver(f,capable);var absent=f.configuration().alerts(f.storeHost(),BuiltinAlarmTypes.catalog(),f.clock());
            assertThatThrownBy(()->absent.saveReceiver(receiver,receiver.edit("Renamed",true,receiver.transport(),receiver.credentials(),T,"operator"),null,true,access()))
                    .isInstanceOfSatisfying(TapstateException.class,error->assertThat(error.code()).isEqualTo(AlertError.CHANNEL_UNAVAILABLE));
        });
    }
    @Test void uninstalledPlannerKeepsGenericWorkPendingWithoutBlockingCompatibleEmail(){
        AlertEvaluationHostIT.withFixture(f->{var capable=f.configuration().alerts(f.storeHost().withChannels(AlertChannels.capabilities(f.storeHost().deployment(),null,registry())),BuiltinAlarmTypes.catalog(),f.clock());
            var receiver=genericReceiver(f,capable);route(f,Duration.ZERO);
            var route=new AlertConfiguration.Subscription(AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.SUBSCRIPTION,f.storeHost().scope(),"generic-route"),f.storeHost().deployment(),T,"operator"),"Generic",
                    new AlertConfiguration.Match(f.rule().resources(),Set.of(),Set.of()),true,List.of(new AlertConfiguration.RouteChannel(TYPE,List.of(receiver.header().identity()),MessageSettings.empty())),Set.of(),
                    new AlertGroupingClock.Policy(Duration.ZERO,Duration.ofSeconds(1),Duration.ofHours(1)),List.of());
            capable.saveDefinition(null,route,access());bad(f);
            var limited=f.configuration().alerts(f.storeHost().withChannels(AlertChannels.capabilities(f.storeHost().deployment(),null)),BuiltinAlarmTypes.catalog(),f.clock());
            new AlertNotificationPlanner(limited,f.clock(),"missing-extension-planner",null,ACTIVE).tick();
            assertThat(limited.deliveries(null,100,null,null,f.reads()).rows()).hasSize(2).allSatisfy(row->assertThat(row.value().frozen().channel().typeId()).isEqualTo("email"));
            new AlertNotificationPlanner(capable,f.clock(),"installed-extension-planner",null,ACTIVE).tick();
            assertThat(capable.deliveries(null,100,TYPE,null,f.reads()).rows()).hasSize(1);
        });
    }
    @Test void missingCapabilityStillRetiresAnEpisodeThatRecoveredBeforeItsFirstIntent(){
        AlertEvaluationHostIT.withFixture(f->{var capable=f.configuration().alerts(f.storeHost().withChannels(AlertChannels.capabilities(f.storeHost().deployment(),null,registry())),BuiltinAlarmTypes.catalog(),f.clock());
            var receiver=genericReceiver(f,capable);
            var route=new AlertConfiguration.Subscription(AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.SUBSCRIPTION,f.storeHost().scope(),"waiting-generic"),f.storeHost().deployment(),T,"operator"),"Waiting generic",
                    new AlertConfiguration.Match(f.rule().resources(),Set.of(),Set.of()),true,List.of(new AlertConfiguration.RouteChannel(TYPE,List.of(receiver.header().identity()),MessageSettings.empty())),Set.of(),
                    new AlertGroupingClock.Policy(Duration.ofSeconds(60),Duration.ofSeconds(1),Duration.ofHours(1)),List.of());
            capable.saveDefinition(null,route,access());bad(f);new AlertNotificationPlanner(capable,f.clock(),"original-waiting",null,ACTIVE).tick();
            assertThat(capable.planning().schedules()).hasSize(1);assertThat(capable.deliveries(null,100,null,null,f.reads()).rows()).isEmpty();
            f.now().set(T.plusSeconds(31));recover(f);
            var absent=f.configuration().alerts(f.storeHost(),BuiltinAlarmTypes.catalog(),f.clock());new AlertNotificationPlanner(absent,f.clock(),"ending-without-adapter",null,ACTIVE).tick();
            assertThat(absent.planning().schedules()).isEmpty();
            try(var mongo=MongoClients.create(f.settings().uri())){
                var row=mongo.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase()).getCollection("alert_notification_schedules")
                        .find(new Document("_id.channelType",TYPE)).first();
                assertThat(row).isNotNull();assertThat(row.get("retiredAt")).isNotNull();assertThat(row.getList("members",Document.class)).isEmpty();
            }
            assertThat(absent.deliveries(null,100,null,null,f.reads()).rows()).isEmpty();
        });
    }
    @Test void genericUnsavedCandidateUsesTheOriginalSingleAttemptEncryptedCommandAndCannotReplayWithOtherParameters(){
        AlertEvaluationHostIT.withFixture(f->{var host=f.storeHost().withChannels(AlertChannels.capabilities(f.storeHost().deployment(),null,registry()));
            var store=f.configuration().alerts(host,BuiltinAlarmTypes.catalog(),f.clock());var endpoint=URI.create("https://notify.example.test/private-hook");
            var transport=AlertReceiver.HttpsJson.fromEndpoint(TYPE,1,endpoint,AlertReceiver.Authentication.NONE,null,AlertReceiver.Trust.SYSTEM,Map.of("topic","orders"));
            var id=new AlertTestCommand.ClientId(T,"a".repeat(32));var commands=new AlertDeliveryCommands(store,host.scope(),host.deployment(),f.clock());
            AlertStore.Stored<AlertIntent.Record> original;
            try(var material=new AlertStore.PrivateMaterial(null,null,endpoint)){original=commands.test(id,"generic-candidate",transport,AlertReceiver.httpsTargetId(transport),null,material,Set.of(),null,access());}
            assertThat(original.value().state().candidate()).isNotNull();assertThat(original.value().frozen().retry().maximumTotalAttempts()).isOne();
            try(var domain=MongoConfigurationDomain.open(f.settings(),host.scope().clusterId())){
                var reopened=domain.alerts(host,BuiltinAlarmTypes.catalog(),f.clock());var channels=new AlertDeliveryRuntimeIT.Channels(reopened);
                try(var worker=AlertDeliveryRuntimeIT.runtime(f,reopened,channels,"generic-test-sender")){worker.tick();AlertDeliveryRuntimeIT.settled(worker);worker.tick();AlertDeliveryRuntimeIT.settled(worker);}
                assertThat(channels.calls).hasValue(1);assertThat(channels.requests.getFirst().purpose()).isEqualTo(io.tapstate.spi.alerting.NotificationChannel.Purpose.CHANNEL_TEST);
                assertThat(new String(channels.requests.getFirst().payload(),StandardCharsets.UTF_8)).contains("channel-test");
                var retained=reopened.intent(original.value().frozen().identity(),access()).orElseThrow();assertThat(retained.value().state().attemptsConsumed()).isOne();
                assertThat(retained.value().state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
                f.now().set(T.plusSeconds(301));reopened.maintenance().orElseThrow().maintain(new AlertMaintenance.Cursor(0,null,null),200,4L*1024*1024,Duration.ofSeconds(5));
                assertThat(reopened.intent(original.value().frozen().identity(),access()).orElseThrow().value().state().candidate()).isNull();
            }
            var changed=AlertReceiver.HttpsJson.fromEndpoint(TYPE,1,endpoint,AlertReceiver.Authentication.NONE,null,AlertReceiver.Trust.SYSTEM,Map.of("topic","other"));
            try(var material=new AlertStore.PrivateMaterial(null,null,endpoint)){assertThatThrownBy(()->commands.test(id,"generic-candidate",changed,AlertReceiver.httpsTargetId(changed),null,material,Set.of(),null,access()))
                    .isInstanceOfSatisfying(TapstateException.class,error->assertThat(error.code()).isEqualTo(AlertError.COMMAND_CONFLICT));}
            assertThat(store.definitions(AlertConfiguration.Kind.RECEIVER,null,10,access()).rows()).isEmpty();assertThat(store.active(null,10,f.reads()).total()).isZero();
        });
    }
    @Test void genericManualRetryUsesOriginalBytesAndBudgetAcrossTakeoverAndCleanupButNeverRevivesRevocation(){
        for(boolean revoke:List.of(false,true))AlertEvaluationHostIT.withFixture(f->{var host=f.storeHost().withChannels(AlertChannels.capabilities(f.storeHost().deployment(),null,registry()));
            var store=f.configuration().alerts(host,BuiltinAlarmTypes.catalog(),f.clock());var receiver=genericReceiver(f,store);
            var route=new AlertConfiguration.Subscription(AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.SUBSCRIPTION,host.scope(),"manual-generic"),host.deployment(),T,"operator"),"Manual generic",
                    new AlertConfiguration.Match(f.rule().resources(),Set.of(),Set.of()),true,List.of(new AlertConfiguration.RouteChannel(TYPE,List.of(receiver.header().identity()),MessageSettings.empty())),Set.of(),
                    new AlertGroupingClock.Policy(Duration.ZERO,Duration.ofSeconds(1),Duration.ofHours(1)),List.of());store.saveDefinition(null,route,access());
            var planner=new AlertNotificationPlanner(store,f.clock(),"manual-generic-planner",null,ACTIVE);f.evaluator().withPlanner(planner);bad(f);planner.tick();
            var original=store.deliveries(null,10,TYPE,null,f.reads()).rows().getFirst();var identity=original.value().frozen().identity();var channels=new AlertDeliveryRuntimeIT.Channels(store);
            channels.answer=request->new io.tapstate.spi.alerting.NotificationChannel.Result(DeliveryEvidence.Outcome.NOT_ACCEPTED,io.tapstate.spi.alerting.NotificationChannel.Action.STOP,null,io.tapstate.spi.alerting.NotificationChannel.Phase.FINAL_RESPONSE,403,"webhook");
            try(var worker=AlertDeliveryRuntimeIT.runtime(f,store,channels,"generic-rejected")){worker.tick();AlertDeliveryRuntimeIT.settled(worker);}
            var before=store.intent(identity,null).orElseThrow();var manual=AlertDeliveryProgress.manual(before.value(),f.clock().instant());
            if(revoke){f.now().set(T.plusSeconds(1));store.saveDefinition(route,route.edit(route.name(),route.match(),false,route.channels(),route.groupBy(),route.grouping(),route.inhibition(),f.clock().instant(),"operator"),access());planner.tick();
                var cancelled=store.intent(identity,null).orElseThrow();assertThatThrownBy(()->AlertDeliveryProgress.manual(cancelled.value(),f.clock().instant())).isInstanceOf(TapstateException.class);
                assertThatThrownBy(()->store.commitAsCaller(new AlertStore.Commit(AlertStore.CommitKind.PLAN,null,List.of(new AlertStore.IntentChange(cancelled,manual)),List.of()),access())).isInstanceOf(TapstateException.class);
                assertThat(store.intent(identity,null).orElseThrow().value().state().attemptsConsumed()).isOne();return;
            }
            var absent=f.configuration().alerts(host.withChannels(Set.of()),BuiltinAlarmTypes.catalog(),f.clock());
            assertThatThrownBy(()->absent.commitAsCaller(new AlertStore.Commit(AlertStore.CommitKind.PLAN,null,List.of(new AlertStore.IntentChange(before,manual)),List.of()),access()))
                    .isInstanceOfSatisfying(TapstateException.class,error->assertThat(error.code()).isEqualTo(AlertError.CHANNEL_UNAVAILABLE));
            assertThat(store.intent(identity,null).orElseThrow().value().state().manualCycles()).isZero();
            assertThat(store.commitAsCaller(new AlertStore.Commit(AlertStore.CommitKind.PLAN,null,List.of(new AlertStore.IntentChange(before,manual)),List.of()),access())).isEqualTo(AlertStore.Outcome.STAGED);
            f.now().set(T.plusSeconds(31));
            try(var domain=MongoConfigurationDomain.open(f.settings(),host.scope().clusterId())){
                var reopened=domain.alerts(host,BuiltinAlarmTypes.catalog(),f.clock());var sender=new AlertDeliveryRuntimeIT.Channels(reopened);
                sender.answer=request->new io.tapstate.spi.alerting.NotificationChannel.Result(DeliveryEvidence.Outcome.ACCEPTED,io.tapstate.spi.alerting.NotificationChannel.Action.STOP,null,io.tapstate.spi.alerting.NotificationChannel.Phase.FINAL_RESPONSE,202,"webhook");
                try(var worker=AlertDeliveryRuntimeIT.runtime(f,reopened,sender,"generic-manual-takeover")){worker.tick();AlertDeliveryRuntimeIT.settled(worker);}
                assertThat(sender.calls).hasValue(1);assertThat(sender.requests.getFirst().payload()).isEqualTo(original.value().state().payload().bytes());
                var accepted=reopened.intent(identity,null).orElseThrow().value();assertThat(accepted.state().manualCycles()).isOne();assertThat(accepted.state().attemptsConsumed()).isEqualTo(2);
                assertThat(accepted.frozen()).isEqualTo(original.value().frozen());assertThat(accepted.state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
                f.now().set(accepted.state().retained().payloadUntil());var withoutAdapter=domain.alerts(host.withChannels(Set.of()),BuiltinAlarmTypes.catalog(),f.clock());
                withoutAdapter.maintenance().orElseThrow().maintain(new AlertMaintenance.Cursor(0,null,null),200,4L*1024*1024,Duration.ofSeconds(5));
                var pruned=reopened.intent(identity,null).orElseThrow().value();assertThat(pruned.state().payload()).isNull();assertThat(pruned.state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
                assertThat(pruned.state().attemptsConsumed()).isEqualTo(2);
            }
        });
    }
    @Test void registrationOnlyAddsADescriptorAndAdapterWhileOriginalPermitsAndRecoveryRemainDurable() throws Exception {
        var received=new CopyOnWriteArrayList<String>();var failures=new CopyOnWriteArrayList<Throwable>();
        var executor=Executors.newFixedThreadPool(2);var server=HttpsServer.create(new InetSocketAddress("localhost",0),0);
        server.setHttpsConfigurator(new HttpsConfigurator(AlertDeliveryNetworkIT.serverTls));server.setExecutor(executor);
        server.createContext("/private-hook",exchange->{try{
            var body=exchange.getRequestBody().readNBytes(65537);assertThat(body.length).isLessThanOrEqualTo(65536);
            received.add(new String(body,StandardCharsets.UTF_8));exchange.sendResponseHeaders(202,-1);
        }catch(Throwable failure){failures.add(failure);}finally{exchange.close();}});server.start();
        try {AlertEvaluationHostIT.withFixture(f->{
            var registry=registry();var original=f.storeHost();var host=original.withChannels(AlertChannels.capabilities(original.deployment(),null,registry));
            var store=f.configuration().alerts(host,BuiltinAlarmTypes.catalog(),f.clock());var planner=new AlertNotificationPlanner(store,f.clock(),"generic-planner",null,ACTIVE);
            var evaluator=new AlertEvaluationHost(store,BuiltinAlarmTypes.catalog(),host.deployment(),f.clock(),"generic-evaluator",ACTIVE);evaluator.withPlanner(planner);
            var services=new PipelineAlertRuntime.Services(store,evaluator,BuiltinAlarmTypes.catalog(),null,null,Set.of("pipeline-lifecycle"),io.tapstate.adapters.alerting.AlarmTypeText.english(),null,registry.catalog());
            var management=new AlertManagement(()->services,c->access(),()->new PipelineAlertRuntime.Health(false,false,0,null,Map.of(),0,0),host.scope(),host.deployment(),f.clock(),new io.tapstate.control.core.AuditGate(r->{},f.clock()));
            try(var app=new SpringApplicationBuilder(io.tapstate.control.restapi.AlertTestHttpFace.class).initializers(c->c.getBeanFactory().registerSingleton("alertControlDispatcher",management))
                    .run("--server.port=0","--server.address=127.0.0.1","--logging.level.root=ERROR");var client=HttpClient.newHttpClient()){
                String base="http://127.0.0.1:"+((WebServerApplicationContext)app).getWebServer().getPort();
                assertThat(post(client,base,"catalog",Map.of()).toString()).contains(TYPE,"Topic");
                var form=map("name","Generic sink","enabled",true,"transport",map("channelType",TYPE,"family","HTTPS_JSON","transportVersion",1,
                        "authentication","NONE","trust","PRIVATE_CA","parameters",Map.of("topic","orders")),"endpoint","https://localhost:"+server.getAddress().getPort()+"/private-hook",
                        "privateCa",new String(AlertDeliveryNetworkIT.pem,StandardCharsets.US_ASCII));
                var saved=post(client,base,"configuration/save",map("kind","RECEIVER","id","generic","etag","0","form",form));assertThat(saved.toString()).doesNotContain("private-hook","BEGIN CERTIFICATE");
                post(client,base,"configuration/save",map("kind","SUBSCRIPTION","id","generic-route","etag","0","form",map("name","Generic route","enabled",true,
                        "resourceKinds",List.of("pipeline"),"allResources",true,"resources",List.of(),"typeIds",List.of("pipeline.failed"),"groupBy",List.of("TYPE"),"initialWaitSeconds",0,
                        "channels",List.of(map("channelType",TYPE,"receivers",List.of("generic"))))));
                var binding=f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
                assertThat(evaluator.accept(binding,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);planner.tick();
                var record=store.dueIntents(T.plusSeconds(1),null,10).rows().getFirst();var identity=record.value().frozen().identity();
                assertThat(record.value().frozen().retry()).isEqualTo(ChannelRetryPolicy.webhookDefaults());
                var frozen=record.value().frozen();var payload=record.value().state().payload().bytes();
                f.now().set(T.plusSeconds(31));
                var withoutAdapter=f.configuration().alerts(host.withChannels(AlertChannels.capabilities(host.deployment(),null)),BuiltinAlarmTypes.catalog(),f.clock());
                new AlertNotificationPlanner(withoutAdapter,f.clock(),"paused-original-plan",null,ACTIVE).tick();
                var stillPending=store.intent(identity,null).orElseThrow().value();
                assertThat(stillPending.state().permitsStopped()).isFalse();
                assertThat(stillPending.frozen()).isEqualTo(frozen);
                try(var mongo=MongoClients.create(f.settings().uri())){
                    var database=mongo.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase());
                    assertThat(database.getCollection("alert_receivers").find(new Document("_id.id","generic")).first().getInteger("formatVersion")).isEqualTo(2);
                    assertThat(database.getCollection("alert_delivery_intents").find(new Document("_id.deliveryId",identity.deliveryId())).first().getInteger("formatVersion")).isEqualTo(3);
                }
                // A node with the original registrations cannot spend this new channel's budget.
                try(var worker=new AlertDeliveryRuntime(store,f.clock(),"missing-generic",ACTIVE,channels(f,AlertChannelRegistry.defaults()),a->access())){worker.tick();AlertDeliveryRuntimeIT.settled(worker);}
                assertThat(received).isEmpty();assertThat(store.intent(identity,null).orElseThrow().value().state().attemptsConsumed()).isZero();
                try(var reopened=MongoConfigurationDomain.open(f.settings(),host.scope().clusterId())){
                    var view=reopened.alerts(host,BuiltinAlarmTypes.catalog(),f.clock());assertThat(view.intent(identity,null).orElseThrow().value().frozen()).isEqualTo(frozen);
                    try(var worker=new AlertDeliveryRuntime(view,f.clock(),"generic-sender",ACTIVE,channels(f,registry),a->access())){worker.tick();AlertDeliveryRuntimeIT.settled(worker);worker.tick();AlertDeliveryRuntimeIT.settled(worker);}
                    assertThat(view.intent(identity,null).orElseThrow().value().state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
                }
                assertThat(received).containsExactly(new String(payload,StandardCharsets.UTF_8));
                f.now().set(T.plusSeconds(32));evaluator.accept(binding,2,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.GOOD,null));
                f.now().set(T.plusSeconds(62));planner.tick();
                try(var worker=new AlertDeliveryRuntime(store,f.clock(),"generic-recovery",ACTIVE,channels(f,registry),a->access())){worker.tick();AlertDeliveryRuntimeIT.settled(worker);}
                assertThat(received).hasSize(2);assertThat(received.getLast()).contains("recovery");
                assertThat(post(client,base,"center",Map.of())).containsEntry("total",0L);
                assertThat((List<?>)post(client,base,"deliveries",Map.of("channel",TYPE)).get("rows")).hasSize(2);
                assertThat(store.intent(identity,null).orElseThrow().value().frozen()).isEqualTo(frozen);
            }catch(Exception failure){throw new AssertionError(failure);}
        });assertThat(failures).isEmpty();}
        finally {server.stop(0);executor.shutdownNow();assertThat(executor.awaitTermination(3,TimeUnit.SECONDS)).isTrue();}
    }
    static AlertChannels channels(AlertEvaluationHostIT.Fixture f,AlertChannelRegistry registry){
        return new AlertChannels(f.storeHost().deployment(),CloudRuntimeSettings.resolve(new CloudProperties()),f.clock(),ACTIVE,null,
                host->new InetAddress[]{InetAddress.getLoopbackAddress()},AlertChannels.trust(AlertDeliveryNetworkIT.pem),true,registry);
    }
    private static Map<String,Object> post(HttpClient client,String base,String operation,Map<String,?> body)throws Exception{
        var response=client.send(HttpRequest.newBuilder(URI.create(base+"/api/alerts/"+operation)).header("Authorization","Bearer cyxt_alert-browser.fixture")
                .header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString(JsonWriter.write(body))).build(),HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);return object(JsonReader.parse(response.body()));
    }
}
