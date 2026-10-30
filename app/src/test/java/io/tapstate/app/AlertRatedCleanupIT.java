package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.AlertStore;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import static io.tapstate.app.AlertNotificationPlannerIT.*;
import static org.assertj.core.api.Assertions.*;

/** Opt-in, wall-clock ingress with the production planner, SMTP sender and cleanup round driver. */
@RequiresDocker
@EnabledIfSystemProperty(named="tapstate.test.alert-rated-cleanup",matches="true")
class AlertRatedCleanupIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @TempDir static Path work;
    @BeforeAll static void tls()throws Exception{AlertDeliveryNetworkIT.temp=work;AlertDeliveryNetworkIT.tls();}
    @Test void sustainedIngressReclaimsEligiblePayloadsAndQuotasWithoutRemovingProtectedFacts()throws Exception{
        int minutes=Integer.getInteger("tapstate.test.alert-rated-minutes",60);
        assertThat(minutes).isBetween(1,60);
        try(var smtp=new AlertDeliveryNetworkIT.SmtpPeer(true)){
            AlertEvaluationHostIT.withFixture(original->{
                long clockOrigin=System.nanoTime();
                Clock continuous=new Clock(){public ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(ZoneId zone){return this;}
                    public Instant instant(){return T.plusNanos(System.nanoTime()-clockOrigin);}};
                var host=original.storeHost().withChannels(Set.of(new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL),new AlertChannelPolicy.Capability("webhook",1,AlertChannelPolicy.Family.OP_EXTERNAL)));
                var live=original.configuration().alerts(host,BuiltinAlarmTypes.catalog(),continuous);
                var f=new AlertEvaluationHostIT.Fixture(original.configuration(),original.cache(),original.settings(),original.business(),original.intent(),original.claim(),live,host,original.rule(),
                        new AlertEvaluationHost(live,BuiltinAlarmTypes.catalog(),host.deployment(),continuous,"live-protected-evaluator",ACTIVE),continuous,original.now());
                // This active, unrouted rule is independent of the repeated critical workload.
                bad(f);var protectedKey=f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE)).key();
                var route=route(f,Duration.ZERO);
                var transport=new AlertReceiver.Email("localhost",smtp.port(),AlertReceiver.Encryption.IMPLICIT_TLS,
                        AlertReceiver.Authentication.NONE,null,"alerts@example.test",List.of("a@example.test","b@example.test"),AlertReceiver.Trust.PRIVATE_CA);
                AlertDeliveryNetworkIT.receiver(f,transport,null);
                var onlyCritical=new AlertConfiguration.Match(route.match().resources(),route.match().typeIds(),Set.of(AlarmSeverity.CRITICAL));
                assertThat(f.store().saveDefinition(route,route.edit(route.name(),onlyCritical,true,route.channels(),route.groupBy(),route.grouping(),route.inhibition(),T,"operator"),access())).isEqualTo(AlertStore.Outcome.STAGED);
                var header=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,f.storeHost().scope(),"rated-rule"),f.storeHost().deployment(),T,"operator");
                var rule=new AlertConfiguration.Rule(header,"Rated failures","pipeline.failed",1,f.rule().resources(),Map.of(),AlarmSeverity.CRITICAL,true,null).validated(BuiltinAlarmTypes.catalog()).enrolled(null);
                assertThat(f.store().saveDefinition(null,rule,access())).isEqualTo(AlertStore.Outcome.STAGED);
                var settings=new AlertRetentionSettings();settings.setRetention(Map.of("accepted-payload","PT15M"));var policy=settings.resolve(f.storeHost().deployment());
                var evaluator=new AlertEvaluationHost(f.store(),BuiltinAlarmTypes.catalog(),f.storeHost().deployment(),f.clock(),"rated-evaluator",ACTIVE,policy);
                var binding=new AlertEvaluationHost.Binding(rule,new AlarmKey.Resource("pipeline","orders","1"),"rated-producer",0,ACTIVE,()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
                var planner=planner(f,"rated-planner");evaluator.withPlanner(planner);var store=f.store();
                var candidate=AlertDeliveryRuntimeIT.seedLegacyTestPayload(f,new AlertTestCommand.ClientId(T,"d".repeat(32)),AlertDeliveryRuntimeIT.testEmail(),"a@example.test");
                var unknownChannel=new AlertDeliveryRuntimeIT.Channels(store);unknownChannel.answer=request->new io.tapstate.spi.alerting.NotificationChannel.Result(DeliveryEvidence.Outcome.UNKNOWN,io.tapstate.spi.alerting.NotificationChannel.Action.STOP,null,io.tapstate.spi.alerting.NotificationChannel.Phase.DEADLINE,0,"controlled-unknown");
                try(var unknown=AlertDeliveryRuntimeIT.runtime(f,store,unknownChannel,"protected-unknown")){unknown.tick();AlertDeliveryRuntimeIT.settled(unknown);}
                assertThat(store.intent(candidate.value().frozen().identity(),null).orElseThrow().value().state().summary()).isEqualTo(DeliveryEvidence.Outcome.UNKNOWN);
                try(var client=MongoClients.create(f.settings().uri());var maintenance=new AlertMaintenanceRuntime(store.maintenance().orElseThrow(),f.clock(),ACTIVE);
                        var sender=new AlertDeliveryRuntime(store,f.clock(),"rated-sender",ACTIVE,AlertDeliveryNetworkIT.channels(f,new AtomicInteger()),a->access())){
                    var db=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase());
                    var future=new Document("_id",new Document("authorityId",f.storeHost().scope().authorityId()).append("clusterId",f.storeHost().scope().clusterId()).append("purpose","business").append("deliveryId","rated-future-format"))
                            .append("formatVersion",99).append("cleanupAt",Date.from(T.minusSeconds(1))).append("body",new Document("evidence","retain-unknown"));
                    var payloadDeadlines=new LinkedHashMap<AlertIntent.Identity,Instant>();
                    var quotaDeadlines=new LinkedHashMap<Document,Instant>();
                    var probe=new ReclamationProbe(f,store,db,payloadDeadlines,quotaDeadlines);
                    maintenance.start();org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(()->assertThat(maintenance.health()).containsEntry("capacitySampleStale",false));
                    long started=System.nanoTime();Instant workloadEpoch=f.clock().instant();int sequence=0;var admissions=new AtomicInteger();
                    for(int minute=0;minute<minutes;minute++){
                        waitUntil(probe,started,(long)minute*60);
                        assertThat(Duration.between(workloadEpoch.plusSeconds(minute*60L),f.clock().instant())).isLessThan(Duration.ofSeconds(10));
                        var quotaMinute=f.clock().instant().truncatedTo(java.time.temporal.ChronoUnit.MINUTES).minusSeconds(3660);
                        var id=new Document("authorityId",f.storeHost().scope().authorityId()).append("clusterId",f.storeHost().scope().clusterId()).append("minute",quotaMinute.toString());
                        var eligible=quotaMinute.plusSeconds(3660);quotaDeadlines.put(id,eligible);
                        db.getCollection("alert_test_quotas").insertOne(new Document("_id",id).append("formatVersion",1).append("recordRevision",1L).append("cleanupAt",Date.from(eligible))
                                .append("body",new Document("actors",List.of(new Document("actor","rated-actor").append("accepted",1)))));
                        accept(store,evaluator,binding,++sequence,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.BAD,null),admissions);
                        pump(probe,planner,sender,smtp,minute*4+2);
                        waitUntil(probe,started,(long)minute*60+3);
                        accept(store,evaluator,binding,++sequence,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.GOOD,null),admissions);
                        pump(probe,planner,sender,smtp,minute*4+4);
                        if(minute==0)db.getCollection("alert_delivery_intents").insertOne(future);
                        var deliveries=store.deliveries(null,100,null,null,f.reads());String after=null;
                        do{
                            deliveries=store.deliveries(after,100,null,null,f.reads());
                            for(var row:deliveries.rows())if(row.value().frozen().identity() instanceof AlertIntent.Business&&row.value().frozen().members().stream().anyMatch(member->member.evaluation().key().equals(binding.key()))){
                                assertThat(row.value().state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
                                assertThat(row.value().frozen().retention().value().acceptedPayload()).isEqualTo(Duration.ofMinutes(15));
                                var acceptedAt=row.value().state().attempts().getFirst().acceptedRecordedAt();assertThat(acceptedAt).isNotNull();
                                assertThat(row.value().state().retained().acceptedAt()).isEqualTo(acceptedAt);
                                var eligiblePayload=acceptedAt.plusSeconds(900);
                                assertThat(row.value().state().retained().payloadUntil()).isEqualTo(eligiblePayload);
                                payloadDeadlines.putIfAbsent(row.value().frozen().identity(),eligiblePayload);
                            }
                            after=deliveries.nextId();
                        }while(after!=null);
                        probe.observe();
                        assertThat(store.state(protectedKey,f.reads()).orElseThrow().value().snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
                        assertThat(store.intent(candidate.value().frozen().identity(),null).orElseThrow().value().state().summary()).isEqualTo(DeliveryEvidence.Outcome.UNKNOWN);
                        assertThat(db.getCollection("alert_delivery_intents").find(new Document("_id",future.get("_id"))).first()).isEqualTo(future);
                        assertThat(maintenance.health()).containsEntry("failures",0L);
                        System.out.println("RATED_CLEANUP minute="+(minute+1)+" smtp="+smtp.bodies.size()+" changed="+maintenance.health().get("changed"));
                    }
                    waitUntil(probe,started,(long)minutes*60);
                    assertThat(smtp.bodies).hasSize(minutes*4);
                    assertThat(payloadDeadlines).hasSize(minutes*4);
                    var distribution=new TreeMap<String,Integer>();for(var identity:payloadDeadlines.keySet()){
                        var key=((AlertIntent.Business)identity).key();distribution.merge(key.notification().kind().name()+":"+key.targetId(),1,Integer::sum);}
                    assertThat(distribution).containsExactlyInAnyOrderEntriesOf(Map.of("FAULT:a@example.test",minutes,"FAULT:b@example.test",minutes,"RECOVERY:a@example.test",minutes,"RECOVERY:b@example.test",minutes));
                    var matured=payloadDeadlines.entrySet().stream().filter(entry->!f.clock().instant().isBefore(entry.getValue().plusSeconds(900))).map(Map.Entry::getKey).toList();
                    assertThat(probe.pruned).containsAll(matured);
                    if(minutes==60){assertThat(matured).hasSizeGreaterThanOrEqualTo(120);assertThat(probe.deleted).hasSizeGreaterThanOrEqualTo(45);
                        assertThat(maintenance.health()).containsEntry("isolatedRecords",true);
                        assertThat(((Number)((Map<?,?>)maintenance.health().get("skipped")).get("unreadable")).longValue()).isPositive();}
                    var currentQuota=new Document("authorityId",f.storeHost().scope().authorityId()).append("clusterId",f.storeHost().scope().clusterId()).append("minute",((AlertIntent.Test)candidate.value().frozen().identity()).command().acceptedAt().truncatedTo(java.time.temporal.ChronoUnit.MINUTES).toString());
                    assertThat(f.clock().instant()).isBefore(((AlertIntent.Test)candidate.value().frozen().identity()).command().acceptedAt().truncatedTo(java.time.temporal.ChronoUnit.MINUTES).plusSeconds(3660));
                    assertThat(db.getCollection("alert_test_quotas").find(new Document("_id",currentQuota)).first()).isNotNull();
                    long elapsed=TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-started);
                    assertThat(elapsed).isGreaterThanOrEqualTo(minutes*60_000L);
                    var evidence=new LinkedHashMap<String,Object>();evidence.put("minutes",minutes);evidence.put("wallMilliseconds",elapsed);evidence.put("acceptanceDuration",minutes==60);evidence.put("clockScale",1);evidence.put("clockMode","continuous-monotonic");evidence.put("maintenanceDriver","production-scheduled");evidence.put("admissionCalls",admissions.get());evidence.put("quotaFixture","historical quota rows introduced at eligibility; natural aging is separately covered");evidence.put("smtpAccepted",smtp.bodies.size());evidence.put("businessIntents",payloadDeadlines.size());evidence.put("reclaimedPayloads",probe.pruned.size());evidence.put("reclaimedQuotas",probe.deleted.size());evidence.put("maximumObservedReclamationMillis",probe.maximumMillis);evidence.put("health",maintenance.health());
                    String output=System.getProperty("tapstate.test.alert-rated-receipt");if(output!=null)try{Files.writeString(Path.of(output),io.tapstate.core.common.JsonWriter.write(evidence));}catch(java.io.IOException failure){throw new AssertionError(failure);}
                }
            });
        }
    }
    private static void accept(AlertStore store,AlertEvaluationHost evaluator,AlertEvaluationHost.Binding binding,int sequence,AlertInput input,AtomicInteger calls){
        org.awaitility.Awaitility.await().pollInterval(Duration.ofMillis(100)).atMost(Duration.ofSeconds(10)).untilAsserted(()->{
            calls.incrementAndGet();var result=evaluator.accept(binding,sequence,input);
            assertThat(result).isIn(AlertEvaluationHost.Result.ACCEPTED,AlertEvaluationHost.Result.IGNORED);
            if(result==AlertEvaluationHost.Result.IGNORED){var retained=store.state(binding.key(),new AlertStore.ReadAccess(true,Set.of(),ACTIVE)).orElseThrow().value().snapshot();
                assertThat(retained.lastSequence()).isEqualTo(sequence);assertThat(retained.lastObservedAt()).isEqualTo(input.observedAt());}
        });
    }
    private static void waitUntil(ReclamationProbe probe,long started,long seconds){
        while(true){probe.observe();
            long remaining=TimeUnit.SECONDS.toNanos(seconds)-(System.nanoTime()-started);if(remaining<=0)return;
            try{TimeUnit.NANOSECONDS.sleep(Math.min(remaining,TimeUnit.SECONDS.toNanos(1)));}catch(InterruptedException failure){Thread.currentThread().interrupt();throw new AssertionError(failure);}}
    }
    private static void pump(ReclamationProbe probe,AlertNotificationPlanner planner,
            AlertDeliveryRuntime sender,AlertDeliveryNetworkIT.SmtpPeer smtp,int expected){
        long deadline=System.nanoTime()+Duration.ofSeconds(10).toNanos();
        do{probe.observe();planner.tick();sender.tick();AlertDeliveryRuntimeIT.settled(sender);
            if(sender.failure()!=null)assertThat(sender.failure().code()).isEqualTo(AlertError.CHANNEL_UNAVAILABLE.code());
            if(smtp.bodies.size()==expected)return;
            try{Thread.sleep(100);}catch(InterruptedException failure){Thread.currentThread().interrupt();throw new AssertionError(failure);}
        }while(System.nanoTime()<deadline);
        assertThat(smtp.bodies).hasSize(expected);
    }
    private static final class ReclamationProbe {
        final AlertEvaluationHostIT.Fixture fixture;final AlertStore store;final com.mongodb.client.MongoDatabase database;
        final Map<AlertIntent.Identity,Instant> payloads;final Map<Document,Instant> quotas;
        final Set<AlertIntent.Identity> pruned=new HashSet<>();final Set<Document> deleted=new HashSet<>();long maximumMillis;
        ReclamationProbe(AlertEvaluationHostIT.Fixture fixture,AlertStore store,com.mongodb.client.MongoDatabase database,Map<AlertIntent.Identity,Instant> payloads,Map<Document,Instant> quotas){this.fixture=fixture;this.store=store;this.database=database;this.payloads=payloads;this.quotas=quotas;}
        void observe(){
            for(var entry:payloads.entrySet())if(!pruned.contains(entry.getKey())&&!fixture.clock().instant().isBefore(entry.getValue())){
                var record=store.intent(entry.getKey(),null).orElseThrow().value();boolean absent=record.state().payload()==null;
                check(entry.getValue(),fixture.clock().instant(),absent);if(absent)pruned.add(entry.getKey());}
            for(var entry:quotas.entrySet())if(!deleted.contains(entry.getKey())&&!fixture.clock().instant().isBefore(entry.getValue())){
                boolean absent=database.getCollection("alert_test_quotas").countDocuments(new Document("_id",entry.getKey()))==0;
                check(entry.getValue(),fixture.clock().instant(),absent);if(absent)deleted.add(entry.getKey());}
        }
        void check(Instant eligible,Instant now,boolean absent){long age=Duration.between(eligible,now).toMillis();
            if(absent){assertThat(age).isLessThanOrEqualTo(900_000);maximumMillis=Math.max(maximumMillis,age);}
            else assertThat(now).isBefore(eligible.plusSeconds(900));}
    }
}
