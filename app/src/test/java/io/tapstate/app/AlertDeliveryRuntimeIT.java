package io.tapstate.app;

import io.tapstate.adapters.alerting.EmailMime;
import io.tapstate.adapters.alerting.NotificationRenderer;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.alerting.NotificationChannel;
import io.tapstate.spi.store.AlertStore;
import io.tapstate.testsupport.RequiresDocker;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static io.tapstate.app.AlertNotificationPlannerIT.*;

@RequiresDocker
class AlertDeliveryRuntimeIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    static final class Channels implements AlertDeliveryRuntime.Channels {
        final AlertStore store;final AtomicInteger calls=new AtomicInteger();final List<NotificationChannel.Request> requests=new CopyOnWriteArrayList<>();
        volatile boolean supported=true;volatile Function<NotificationChannel.Request,NotificationChannel.Result> answer=r->accepted();
        volatile java.util.function.Predicate<AlertIntent.Frozen> available=f->true;
        Channels(AlertStore store){this.store=store;}
        public boolean supports(AlertIntent.Frozen f){return supported&&available.test(f);}
        public Object prepare(AlertIntent.Frozen f){return this;}
        public NotificationChannel open(AlertIntent.Frozen f,AlertStore.PrivateMaterial material,Object prepared){return new NotificationChannel(){
            public AlertChannelPolicy.Capability capability(){return f.channel();}
            public Result send(Request r){
                var stored=store.intent(f.identity(),null).orElseThrow().value();
                assertThat(stored.state().attempts()).anyMatch(a->a.permit().attemptId().equals(r.attemptId()));
                assertThat(stored.state().payload().bytes()).isEqualTo(r.payload());
                assertThat(r.deadline()).isBeforeOrEqualTo(f.deadline());
                requests.add(r);calls.incrementAndGet();return answer.apply(r);
            }
        };}
        public void close(){}
    }
    static NotificationChannel.Result accepted(){return new NotificationChannel.Result(DeliveryEvidence.Outcome.ACCEPTED,NotificationChannel.Action.STOP,null,NotificationChannel.Phase.FINAL_RESPONSE,250,"smtp");}
    static AlertDeliveryRuntime runtime(AlertEvaluationHostIT.Fixture f,AlertStore store,Channels channels,String name){return new AlertDeliveryRuntime(store,f.clock(),name,ACTIVE,channels,a->access());}
    static AlertReceiver.Email testEmail(){return new AlertReceiver.Email("smtp.example.test",465,AlertReceiver.Encryption.IMPLICIT_TLS,
            AlertReceiver.Authentication.NONE,null,"alerts@example.test",List.of("a@example.test","b@example.test"),AlertReceiver.Trust.SYSTEM);}
    static AlertStore.Stored<AlertIntent.Record> seedLegacyTestPayload(AlertEvaluationHostIT.Fixture f,
            AlertTestCommand.ClientId clientId,AlertReceiver.Email transport,String target){
        var originalAccess=access();var key=new AlertTestCommand.Key(f.storeHost().scope(),originalAccess.actor().principalKey(),clientId);
        var payload=new NotificationRenderer.Snapshot(NotificationRenderer.TEMPLATE_VERSION,"en","[TEST] Email",
                "Legacy channel-test notification","<!doctype html><html lang=\"en\"><body><pre>Legacy channel-test notification</pre></body></html>",
                "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var frozenMime=EmailMime.freeze(key.testId(),clientId.createdAt(),transport.from(),target,payload);
        assertThat(frozenMime.successful()).isTrue();
        var originalPayload=new AlertIntent.Payload(frozenMime.bytes());
        var form=new AlertTestCommand.Parameters("email",new AlertTestCommand.Candidate("candidate","0".repeat(64)),null,
                target,"candidate",null,originalPayload.sha256());
        var prepared=f.store().prepareCandidate(key,form,transport,null,0,null,Set.of(),originalAccess);
        var capability=new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL);
        var accepted=AlertTestCommand.admit(AlertChannelPolicy.Deployment.ON_PREM,capability,key,prepared.parameters(),null,f.clock().instant()).command();
        var retention=AlertIntent.Policy.capture(AlertRetentionPolicy.defaults(AlertChannelPolicy.Deployment.ON_PREM));
        var frozen=new AlertIntent.Frozen(new AlertIntent.Test(accepted),capability,null,prepared.target(),
                new AlertIntent.Content("message/rfc822",originalPayload.sha256(),
                        new AlertIntent.Template("channel-test",NotificationRenderer.TEMPLATE_VERSION,1,"en",MessageSettings.empty()),false),
                List.of(),ChannelRetryPolicy.singleAttempt(Duration.ofMinutes(5),Duration.ofSeconds(60)),retention,
                accepted.acceptedAt(),accepted.deadline(),originalAccess.actor());
        var retained=AlertIntent.Retained.capture(DeliveryRetention.begin(retention.value(),DeliveryRetention.Kind.CHANNEL_TEST,
                accepted.acceptedAt(),accepted.deadline()));
        var record=new AlertIntent.Record(frozen,new AlertIntent.State(AlertOwnership.unclaimed(),0,false,null,accepted.acceptedAt(),0,
                List.of(),DeliveryEvidence.Outcome.NOT_ATTEMPTED,retained,originalPayload,prepared.encrypted(),null,null,null));
        return sendingStore(f).admitTest(record,originalAccess);
    }
    static void seed(AlertEvaluationHostIT.Fixture f){route(f,Duration.ZERO);var planner=planner(f,"planner");f.evaluator().withPlanner(planner);bad(f);planner.tick();}
    static List<AlertIntent.Identity> ids(AlertEvaluationHostIT.Fixture f){return intents(f).stream().map(v->v.value().frozen().identity()).toList();}
    static void settled(AlertDeliveryRuntime runtime){await().atMost(Duration.ofSeconds(10)).until(()->runtime.occupied()==0);}
    @Test void aCommittedPermitPrecedesEveryCallAndReopenNeverResendsAccepted(){
        AlertEvaluationHostIT.withFixture(f->{seed(f);var store=sendingStore(f);var channels=new Channels(store);var ids=ids(f);
            try(var a=runtime(f,store,channels,"worker-a")){a.tick();settled(a);assertThat(channels.calls).hasValue(2);}
            try(var b=runtime(f,store,channels,"worker-b")){b.tick();settled(b);assertThat(channels.calls).hasValue(2);}
            for(var id:ids)assertThat(store.intent(id,null).orElseThrow().value().state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
        });
    }
    @Test void twoWorkersCompeteForOriginalPermitsWithoutDuplicateExternalCalls(){
        AlertEvaluationHostIT.withFixture(f->{seed(f);var store=sendingStore(f);var channels=new Channels(store);
            try(var a=runtime(f,store,channels,"worker-a");var b=runtime(f,store,channels,"worker-b")){
                a.tick();b.tick();settled(a);settled(b);assertThat(channels.calls).hasValue(2);
                assertThat(channels.requests.stream().map(NotificationChannel.Request::deliveryId).distinct()).hasSize(2);
            }
        });
    }
    @Test void retryUsesOriginalContentIdAndPersistedLowerBoundAcrossReopen(){
        AlertEvaluationHostIT.withFixture(f->{seed(f);var store=sendingStore(f);var channels=new Channels(store);var ids=ids(f);
            channels.answer=r->new NotificationChannel.Result(DeliveryEvidence.Outcome.UNKNOWN,NotificationChannel.Action.RETRY,null,NotificationChannel.Phase.DEADLINE,0,"smtp");
            try(var a=runtime(f,store,channels,"worker-a")){a.tick();settled(a);assertThat(channels.calls).hasValue(2);}
            var original=store.intent(ids.getFirst(),null).orElseThrow().value();var next=original.state().nextAttemptAt();
            assertThat(next).isAfterOrEqualTo(T.plusSeconds(1800));
            f.now().set(ids.stream().map(id->store.intent(id,null).orElseThrow().value().state().nextAttemptAt()).min(Instant::compareTo).orElseThrow().minusNanos(1));
            try(var b=runtime(f,store,channels,"worker-b")){b.tick();settled(b);assertThat(channels.calls).hasValue(2);}
            f.now().set(T.plusSeconds(2200));channels.answer=r->accepted();
            try(var c=runtime(f,store,channels,"worker-c")){c.tick();settled(c);assertThat(channels.calls).hasValue(4);}
            assertThat(original.state().payload().bytes()).isEqualTo(channels.requests.stream().filter(r->r.deliveryId().equals(original.frozen().identity().deliveryId())).toList().getLast().payload());
            assertThat(store.intent(ids.getFirst(),null).orElseThrow().value().state().attemptsConsumed()).isEqualTo(2);
        });
    }
    @Test void lateRetryAfterAndPermanentRejectionReconcileOriginalOrphanProgress() {
        AlertEvaluationHostIT.withFixture(f->{seed(f);var store=sendingStore(f);var first=intents(f).getFirst();var pending=grant(f,first);var permit=pending.state().attempts().getFirst().permit();
            f.now().set(permit.deadline().plusSeconds(1));var before=store.intent(first.value().frozen().identity(),null).orElseThrow();
            var orphan=AlertDeliveryProgress.result(before.value(),permit,DeliveryEvidence.Outcome.UNKNOWN,new AlertIntent.Protocol(AlertIntent.Phase.DEADLINE,AlertIntent.Action.RETRY,0,"orphan-attempt",null),null,f.clock().instant(),0);
            assertThat(store.commit(new AlertStore.Commit(AlertStore.CommitKind.RESULT,null,List.of(new AlertStore.IntentChange(before,orphan)),List.of()))).isEqualTo(AlertStore.Outcome.STAGED);
            before=store.intent(first.value().frozen().identity(),null).orElseThrow();Instant until=T.plusSeconds(4000);
            var limited=AlertDeliveryProgress.result(before.value(),permit,DeliveryEvidence.Outcome.UNKNOWN,new AlertIntent.Protocol(AlertIntent.Phase.FINAL_RESPONSE,AlertIntent.Action.RETRY,429,"smtp",until),null,f.clock().instant(),0);
            assertThat(limited.state().nextAttemptAt()).isEqualTo(until);
            assertThat(store.commit(new AlertStore.Commit(AlertStore.CommitKind.RESULT,null,List.of(new AlertStore.IntentChange(before,limited)),List.of()))).isEqualTo(AlertStore.Outcome.STAGED);
            before=store.intent(first.value().frozen().identity(),null).orElseThrow();
            var stopped=AlertDeliveryProgress.result(before.value(),permit,DeliveryEvidence.Outcome.NOT_ACCEPTED,new AlertIntent.Protocol(AlertIntent.Phase.FINAL_RESPONSE,AlertIntent.Action.STOP,535,"smtp",until),null,f.clock().instant(),0);
            assertThat(stopped.state().nextAttemptAt()).isNull();
            assertThat(store.commit(new AlertStore.Commit(AlertStore.CommitKind.RESULT,null,List.of(new AlertStore.IntentChange(before,stopped)),List.of()))).isEqualTo(AlertStore.Outcome.STAGED);
            assertThat(stopped.state().summary()).isEqualTo(DeliveryEvidence.Outcome.UNKNOWN);
            var manual=AlertDeliveryProgress.manual(stopped,f.clock().instant());assertThat(manual.state().nextAttemptAt()).isEqualTo(until);
            before=store.intent(first.value().frozen().identity(),null).orElseThrow();
            assertThat(store.commitAsCaller(new AlertStore.Commit(AlertStore.CommitKind.PLAN,null,List.of(new AlertStore.IntentChange(before,manual)),List.of()),access())).isEqualTo(AlertStore.Outcome.STAGED);
        });
    }
    @Test void concurrentIdenticalTestCommandsReconcileTheWinnersFrozenServerTimestamp() {
        AlertEvaluationHostIT.withFixture(f->{f.now().set(T.plusSeconds(1));var store=sendingStore(f);var prepared=new AtomicInteger();var barrier=new CyclicBarrier(2);
            var wrapped=(AlertStore)java.lang.reflect.Proxy.newProxyInstance(AlertStore.class.getClassLoader(),new Class<?>[]{AlertStore.class},(proxy,method,args)->{
                try{var result=method.invoke(store,args);if(method.getName().equals("prepareCandidate")&&prepared.incrementAndGet()<=2)barrier.await(10,TimeUnit.SECONDS);return result;}
                catch(java.lang.reflect.InvocationTargetException failure){throw failure.getCause();}
            });
            var command=new AlertTestCommand.ClientId(Instant.ofEpochSecond(T.getEpochSecond()),"1".repeat(32));
            var email=new AlertReceiver.Email("smtp.example.test",465,AlertReceiver.Encryption.IMPLICIT_TLS,AlertReceiver.Authentication.NONE,null,"alerts@example.test",List.of("a@example.test"),AlertReceiver.Trust.SYSTEM);
            try(var threads=Executors.newFixedThreadPool(2)) {
                var futures=List.of(T,T.plusSeconds(1)).stream().map(time->threads.submit(()->new AlertDeliveryCommands(wrapped,f.storeHost().scope(),AlertChannelPolicy.Deployment.ON_PREM,Clock.fixed(time,ZoneOffset.UTC))
                        .test(command,"candidate",email,"a@example.test",null,null,Set.of(),null,access()))).toList();
                try{var first=futures.getFirst().get(15,TimeUnit.SECONDS);var second=futures.getLast().get(15,TimeUnit.SECONDS);assertThat(first.value().frozen()).isEqualTo(second.value().frozen());}
                catch(Exception failure){throw new AssertionError(failure);}
            }
            assertThat(store.dueIntents(T.plusSeconds(300),null,10).rows()).hasSize(1);
            var channels=new Channels(store);channels.answer=r->new NotificationChannel.Result(DeliveryEvidence.Outcome.UNKNOWN,NotificationChannel.Action.RETRY,null,NotificationChannel.Phase.DEADLINE,0,"smtp");
            try(var worker=runtime(f,store,channels,"test-sender")){worker.tick();settled(worker);worker.tick();settled(worker);assertThat(channels.calls).hasValue(1);}
            var record=store.test(new AlertTestCommand.Key(f.storeHost().scope(),access().actor().principalKey(),command),access()).orElseThrow().value();
            assertThat(record.state().attemptsConsumed()).isOne();assertThat(record.state().summary()).isEqualTo(DeliveryEvidence.Outcome.UNKNOWN);assertThat(record.state().nextAttemptAt()).isNull();
            assertThat(f.store().active(null,10,f.reads()).total()).isZero();
        });
    }
    @Test void replayReturnsTheOriginalFrozenPayloadAfterTheHtmlRendererChanges() {
        AlertEvaluationHostIT.withFixture(f->{
            var email=testEmail();var command=new AlertTestCommand.ClientId(T,"4".repeat(32));
            var original=seedLegacyTestPayload(f,command,email,"a@example.test");
            byte[] oldPayload=original.value().state().payload().bytes();
            assertThat(new String(oldPayload,java.nio.charset.StandardCharsets.US_ASCII)).contains("text/html","base64");
            var replay=new AlertDeliveryCommands(f.store(),f.storeHost().scope(),AlertChannelPolicy.Deployment.ON_PREM,f.clock())
                    .test(command,"candidate",email,"a@example.test",null,null,Set.of(),null,access());
            assertThat(replay).isEqualTo(original);
            assertThat(replay.value().state().payload().bytes()).isEqualTo(oldPayload);
            assertThat(replay.value().frozen().identity()).isEqualTo(original.value().frozen().identity());
        });
    }
    @Test void replayStillConflictsWhenItsTargetOrTransportChanges() {
        AlertEvaluationHostIT.withFixture(f->{
            var email=testEmail();var command=new AlertTestCommand.ClientId(T,"5".repeat(32));
            seedLegacyTestPayload(f,command,email,"a@example.test");
            var commands=new AlertDeliveryCommands(f.store(),f.storeHost().scope(),AlertChannelPolicy.Deployment.ON_PREM,f.clock());
            assertThatThrownBy(()->commands.test(command,"candidate",email,"b@example.test",null,null,Set.of(),null,access()))
                    .isInstanceOfSatisfying(TapstateException.class,failure->assertThat(failure.code()).isEqualTo(AlertError.COMMAND_CONFLICT));
            var changedTransport=new AlertReceiver.Email("smtp.other.example.test",465,AlertReceiver.Encryption.IMPLICIT_TLS,
                    AlertReceiver.Authentication.NONE,null,email.from(),email.recipients(),AlertReceiver.Trust.SYSTEM);
            assertThatThrownBy(()->commands.test(command,"candidate",changedTransport,"a@example.test",null,null,Set.of(),null,access()))
                    .isInstanceOfSatisfying(TapstateException.class,failure->assertThat(failure.code()).isEqualTo(AlertError.COMMAND_CONFLICT));
        });
    }
    @Test void untrustedManagedOriginOrMalformedTokenCannotBecomeAnAvailableChannel() {
        AlertEvaluationHostIT.withFixture(false,true,new AlarmKey.Scope("112345678901234567890123","012345678901234567890123"),f->{
            var planner=new AlertNotificationPlanner(f.store(),f.clock(),"planner",java.net.URI.create("https://cloud.example"),ACTIVE);f.evaluator().withPlanner(planner);bad(f);planner.tick();
            var frozen=f.store().dueIntents(T.plusSeconds(1),null,10).rows().getFirst().value().frozen();
            for(boolean insecureOrigin:List.of(true,false)){
                var properties=new CloudProperties();properties.setBaseUrl(insecureOrigin?"http://cloud.example":"https://cloud.example");properties.setToken(insecureOrigin?"token":"token\r\ninjected");properties.setAtlasUri("mongodb://config.invalid/config");properties.setClusterId(f.storeHost().scope().clusterId());
                try(var channels=new AlertChannels(AlertChannelPolicy.Deployment.CLOUD_MANAGED,CloudRuntimeSettings.resolve(properties),f.clock(),ACTIVE,f.storeHost().scope().authorityId(),host->{throw new AssertionError("untrusted configuration must not resolve DNS");})){
                    assertThat(channels.supports(frozen)).isFalse();
                }
            }
        });
    }
    @Test void disabledDeploymentOutboundDoesNotResolveOrConsumeAnyPermit() {
        AlertEvaluationHostIT.withFixture(f->{seed(f);var store=sendingStore(f);
            try(var factory=new AlertChannels(AlertChannelPolicy.Deployment.ON_PREM,CloudRuntimeSettings.resolve(new CloudProperties()),f.clock(),ACTIVE,null,false);
                    var worker=new AlertDeliveryRuntime(store,f.clock(),"disabled-sender",ACTIVE,factory,a->access())){
                worker.tick();settled(worker);assertThat(worker.failure()).isNotNull();
                for(var id:ids(f))assertThat(store.intent(id,null).orElseThrow().value().state().attemptsConsumed()).isZero();
                assertThat(f.store().active(null,10,f.reads()).total()).isOne();
            }
            assertThat(AlertChannels.outboundEnabled(null)).isTrue();assertThat(AlertChannels.outboundEnabled("false")).isFalse();
            assertThatThrownBy(()->AlertChannels.outboundEnabled("invalid")).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        });
    }
    @Test void managedFactoryNeverRedirectsOriginalOpWorkAndAnOriginalInFlightReceiptKeepsItsIdentity(){
        AlertEvaluationHostIT.withFixture(f->{seed(f);var store=sendingStore(f);var originals=intents(f);var identity=originals.getFirst().value().frozen().identity();
            var properties=new CloudProperties();properties.setBaseUrl("https://cloud.example.test");properties.setToken("controlled-machine-token");
            properties.setClusterId(f.storeHost().scope().clusterId());properties.setAtlasUri("mongodb://config.invalid/config");
            var resolutions=new java.util.concurrent.atomic.AtomicInteger();
            try(var factory=new AlertChannels(AlertChannelPolicy.Deployment.CLOUD_MANAGED,CloudRuntimeSettings.resolve(properties),f.clock(),ACTIVE,f.storeHost().scope().authorityId(),
                    host->{resolutions.incrementAndGet();throw new AssertionError("an OP snapshot cannot resolve a managed destination");});
                    var worker=new AlertDeliveryRuntime(store,f.clock(),"managed-mode-factory",ACTIVE,factory,a->access())){
                worker.tick();settled(worker);assertThat(resolutions).hasValue(0);
                for(var row:originals)assertThat(store.intent(row.value().frozen().identity(),null).orElseThrow()).isEqualTo(row);
                var pending=grant(f,originals.getFirst());worker.tick();settled(worker);assertThat(resolutions).hasValue(0);
                assertThat(store.intent(identity,null).orElseThrow().value()).isEqualTo(pending);
                AlertNotificationPlannerIT.accepted(f,identity);
                var retained=store.intent(identity,null).orElseThrow().value();assertThat(retained.frozen()).isEqualTo(originals.getFirst().value().frozen());
                assertThat(retained.state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);assertThat(retained.state().attemptsConsumed()).isOne();
                worker.tick();settled(worker);assertThat(resolutions).hasValue(0);
                assertThat(store.deliveries(null,10,null,null,f.reads()).rows()).hasSize(2).allSatisfy(row->assertThat(row.value().frozen().identity().channelType()).isEqualTo("email"));
                assertThat(store.intent(originals.getLast().value().frozen().identity(),null).orElseThrow()).isEqualTo(originals.getLast());
            }
        });
    }
    @Test void missingAdapterDoesNotConsumeBudgetAndOtherTargetsStillRun(){
        AlertEvaluationHostIT.withFixture(f->{seed(f);var store=sendingStore(f);var channels=new Channels(store);channels.supported=false;var ids=ids(f);
            try(var a=runtime(f,store,channels,"worker-a")){a.tick();settled(a);assertThat(channels.calls).hasValue(0);assertThat(a.failure()).isNotNull();}
            for(var id:ids)assertThat(store.intent(id,null).orElseThrow().value().state().attemptsConsumed()).isZero();
            channels.supported=true;
            try(var b=runtime(f,store,channels,"worker-b")){b.tick();settled(b);assertThat(channels.calls).hasValue(2);}
        });
    }
    @Test void revocationBeforePermitPreventsEveryExternalCall(){
        AlertEvaluationHostIT.withFixture(f->{seed(f);var store=sendingStore(f);var channels=new Channels(store);var route=(AlertConfiguration.Subscription)store.definition(AlertConfiguration.Kind.SUBSCRIPTION,"route",access()).orElseThrow();
            store.saveDefinition(route,route.edit(route.name(),route.match(),false,route.channels(),route.groupBy(),route.grouping(),route.inhibition(),T,"operator"),access());
            try(var a=runtime(f,store,channels,"worker-a")){a.tick();settled(a);assertThat(channels.calls).hasValue(0);}
            for(var id:ids(f))assertThat(store.intent(id,null).orElseThrow().value().state().attemptsConsumed()).isZero();
        });
    }
    @Test void missingCapabilitySurvivesReopenAndMaintenanceWithoutBlockingCompatibleWorkOrRevivingRevocation(){
        AlertEvaluationHostIT.withFixture(f->{var route=route(f,Duration.ZERO);var store=sendingStore(f);
            AlertManagementIT.call(AlertManagementIT.service(f),"configuration.save",AlertHttpModel.map("kind","RECEIVER","id","compatible-webhook","etag","0","form",
                    AlertHttpModel.map("name","Compatible webhook","enabled",true,"transport",AlertHttpModel.map("channelType","webhook","authentication","NONE","trust","SYSTEM"),"endpoint","https://notify.example.test/hook")));
            var receiver=(AlertReceiver)store.definition(AlertConfiguration.Kind.RECEIVER,"compatible-webhook",access()).orElseThrow();
            var routes=new ArrayList<>(route.channels());routes.add(new AlertConfiguration.RouteChannel("webhook",List.of(receiver.header().identity()),MessageSettings.empty()));
            store.saveDefinition(route,route.edit(route.name(),route.match(),true,routes,route.groupBy(),route.grouping(),route.inhibition(),T,"operator"),access());
            var planner=planner(f,"capability-planner");f.evaluator().withPlanner(planner);bad(f);planner.tick();
            var original=intents(f).stream().filter(row->row.value().frozen().identity().channelType().equals("email")).toList();assertThat(original).hasSize(2);
            var absent=f.configuration().alerts(f.storeHost().withChannels(Set.of(new AlertChannelPolicy.Capability("webhook",1,AlertChannelPolicy.Family.OP_EXTERNAL))),BuiltinAlarmTypes.catalog(),f.clock());
            var channels=new Channels(absent);channels.available=frozen->frozen.identity().channelType().equals("webhook");
            try(var worker=runtime(f,absent,channels,"partial-capability")){worker.tick();settled(worker);assertThat(channels.calls).hasValue(1);}
            assertThat(channels.requests.getFirst().destination().channelType()).isEqualTo("webhook");
            for(var row:original)assertThat(absent.intent(row.value().frozen().identity(),null).orElseThrow()).isEqualTo(row);
            var paused=original.getFirst();var unavailable=paused.value().frozen().identity();
            var permit=DeliveryAttempt.grant(unavailable.scope(),unavailable.deliveryId(),"unsupported-attempt",1,1,"unsupported-worker",paused.value().frozen().retry(),T,paused.value().frozen().deadline(),T).orElseThrow();
            var state=paused.value().state();
            var pending=new AlertIntent.Record(paused.value().frozen(),new AlertIntent.State(new AlertOwnership(1,"unsupported-worker",T.plusSeconds(120)),0,false,null,null,1,
                    List.of(AlertIntent.Attempt.pending(permit)),DeliveryEvidence.Outcome.UNKNOWN,state.retained(),state.payload(),null,null,null,null));
            assertThatThrownBy(()->absent.commit(new AlertStore.Commit(AlertStore.CommitKind.PERMIT,null,List.of(new AlertStore.IntentChange(paused,pending)),List.of())))
                    .isInstanceOfSatisfying(TapstateException.class,error->assertThat(error.code()).isEqualTo(AlertError.CHANNEL_UNAVAILABLE));
            f.now().set(T.plusSeconds(31));
            for(int collection=0;collection<12;collection++)absent.maintenance().orElseThrow()
                    .maintain(new io.tapstate.spi.store.AlertMaintenance.Cursor(collection,null,null),200,4L*1024*1024,Duration.ofSeconds(5));
            try(var reopened=io.tapstate.adapters.mongostore.MongoConfigurationDomain.open(f.settings(),f.storeHost().scope().clusterId())){
                var view=reopened.alerts(f.storeHost().withChannels(Set.of(new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL))),BuiltinAlarmTypes.catalog(),f.clock());
                assertThat(view.intent(unavailable,null).orElseThrow()).isEqualTo(paused);
                var restored=new Channels(view);
                try(var worker=runtime(f,view,restored,"restored-capability")){worker.tick();settled(worker);assertThat(restored.calls).hasValue(2);}
                var request=restored.requests.stream().filter(value->value.deliveryId().equals(unavailable.deliveryId())).findFirst().orElseThrow();
                assertThat(request.payload()).isEqualTo(paused.value().state().payload().bytes());
                assertThat(view.intent(unavailable,null).orElseThrow().value().state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
            }
        });
        AlertEvaluationHostIT.withFixture(f->{seed(f);var store=sendingStore(f);var channels=new Channels(store);channels.supported=false;
            var original=intents(f);try(var worker=runtime(f,store,channels,"missing-before-revocation")){worker.tick();settled(worker);}
            var route=(AlertConfiguration.Subscription)store.definition(AlertConfiguration.Kind.SUBSCRIPTION,"route",access()).orElseThrow();
            store.saveDefinition(route,route.edit(route.name(),route.match(),false,route.channels(),route.groupBy(),route.grouping(),route.inhibition(),T,"operator"),access());
            channels.supported=true;try(var worker=runtime(f,store,channels,"restored-after-revocation")){worker.tick();settled(worker);}
            assertThat(channels.calls).hasValue(0);
            for(var row:original)assertThat(store.intent(row.value().frozen().identity(),null).orElseThrow().value().state().attemptsConsumed()).isZero();
        });
    }
    @Test void orphanAttemptWaitsOnlyOriginalDeadlineAndCannotBeReissuedOnTakeover(){
        AlertEvaluationHostIT.withFixture(f->{seed(f);var store=sendingStore(f);var first=intents(f).getFirst();var pending=grant(f,first);var permit=pending.state().attempts().getFirst().permit();
            var channels=new Channels(store);
            f.now().set(permit.deadline().minusNanos(1));
            try(var a=runtime(f,store,channels,"worker-a")){a.tick();settled(a);assertThat(channels.requests).noneMatch(r->r.deliveryId().equals(first.value().frozen().identity().deliveryId()));}
            f.now().set(permit.deadline().plusSeconds(100));
            try(var b=runtime(f,store,channels,"worker-b")){b.tick();settled(b);}
            var restored=store.intent(first.value().frozen().identity(),null).orElseThrow().value();
            assertThat(restored.state().attemptsConsumed()).isOne();assertThat(restored.state().attempts().getFirst().endedAt()).isEqualTo(permit.deadline());
            assertThat(restored.state().nextAttemptAt()).isBetween(permit.deadline().plusSeconds(1800),permit.deadline().plusSeconds(2160));
        });
    }
}
