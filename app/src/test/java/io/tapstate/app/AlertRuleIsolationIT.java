package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AlertStore;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static io.tapstate.app.AlertNotificationPlannerIT.*;
import static org.assertj.core.api.Assertions.*;

/** Independent rule capabilities cannot revoke old facts or stop unrelated rules. */
@RequiresDocker
class AlertRuleIsolationIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @org.junit.jupiter.api.Test void globalSchemaFailureIsNeverReportedAsALocalRuleCapabilityNotice(){
        AlertEvaluationHostIT.withFixture(f->{AlertDeliveryRuntimeIT.seed(f);var original=intents(f);var service=AlertManagementIT.service(f);
            try(var client=MongoClients.create(f.settings().uri())){
                var database=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase());var schemas=database.getCollection("schema_migrations");
                var saved=schemas.find(new Document("_id","config")).first();var future=Document.parse(saved.toJson());future.get("alerting",Document.class).put("installedVersion",999);
                schemas.replaceOne(new Document("_id","config"),future);
                try{assertThatThrownBy(()->AlertManagementIT.call(service,"deliveries",Map.of())).isInstanceOfSatisfying(TapstateException.class,error->assertThat(error.code()).isEqualTo(io.tapstate.spi.store.IoError.DOCUMENT_UNREADABLE));}
                finally{schemas.replaceOne(new Document("_id","config"),saved);}
                assertThat(f.store().deliveries(null,100,null,null,f.reads()).rows()).isEqualTo(original);
            }
        });
    }
    @org.junit.jupiter.api.Test void aRegisteredReceiptIsStillMergedWithoutInterpretingTheNowOpaqueRule(){
        AlertEvaluationHostIT.withFixture(f->{AlertDeliveryRuntimeIT.seed(f);var rows=intents(f);var original=rows.getFirst();grant(f,original);
            var head=f.store().active(null,10,f.reads()).rows().getFirst();
            try(var client=MongoClients.create(f.settings().uri())){
                var rules=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase()).getCollection("alert_rules");
                var saved=rules.find(new Document("_id.id","rule")).first();var opaque=Document.parse(saved.toJson());opaque.put("formatVersion",999);opaque.put("body",new Document("unknown","never-interpret"));rules.replaceOne(new Document("_id",saved.get("_id")),opaque);
                try{accepted(f,original.value().frozen().identity());var receipt=f.store().intent(original.value().frozen().identity(),null).orElseThrow().value();
                    assertThat(receipt.frozen()).isEqualTo(original.value().frozen());assertThat(receipt.state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);assertThat(receipt.state().attemptsConsumed()).isOne();
                    assertThat(f.store().active(null,10,f.reads()).rows()).containsExactly(head);assertThat(f.store().intent(rows.getLast().value().frozen().identity(),null).orElseThrow()).isEqualTo(rows.getLast());
                    assertThat(rules.find(new Document("_id",saved.get("_id"))).first()).isEqualTo(opaque);
                }finally{rules.replaceOne(new Document("_id",saved.get("_id")),saved);}
            }
        });
    }
    @org.junit.jupiter.api.Test void anImmutableMixedBatchWaitsForItsCapabilityWhileIndependentKnownWorkContinues(){
        AlertEvaluationHostIT.withFixture(f->{var catalog=AlertTypeCapabilityIT.catalog(1);var host=f.storeHost().withChannels(Set.of(new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL)));
            var capable=f.configuration().alerts(host,catalog,f.clock());var route=route(f,Duration.ofSeconds(10));
            capable.saveDefinition(route,route.edit(route.name(),route.match(),true,route.channels(),Set.of(AlertConfiguration.GroupBy.RESOURCE),route.grouping(),route.inhibition(),T,"operator"),access());
            var unknown=rule(f,"mixed-unavailable","test.pipeline-condition",1,null,catalog);capable.saveDefinition(null,unknown,access());
            var originalPlanner=new AlertNotificationPlanner(capable,f.clock(),"mixed-owner",null,ACTIVE);var evaluator=new AlertEvaluationHost(capable,catalog,host.deployment(),f.clock(),"mixed-evaluator",ACTIVE).withPlanner(originalPlanner);
            evaluator.accept(binding(f,unknown),1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null));evaluator.accept(binding(f,f.rule()),1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null));
            f.now().set(T.plusSeconds(10));originalPlanner.tick();var mixed=capable.deliveries(null,100,null,null,f.reads()).rows();assertThat(mixed).hasSize(2).allSatisfy(row->assertThat(row.value().frozen().members()).hasSize(2));
            var old=f.configuration().alerts(host,BuiltinAlarmTypes.catalog(),f.clock());var oldPlanner=new AlertNotificationPlanner(old,f.clock(),"known-owner",null,ACTIVE);
            var oldEvaluator=new AlertEvaluationHost(old,BuiltinAlarmTypes.catalog(),host.deployment(),f.clock(),"known-evaluator",ACTIVE).withPlanner(oldPlanner);
            f.now().set(T.plusSeconds(31));assertThat(oldEvaluator.accept(binding(f,f.rule()),2,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.BAD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
            var independent=rule(f,"independent-known","pipeline.failed",1,null,BuiltinAlarmTypes.catalog());old.saveDefinition(null,independent,access());
            assertThat(oldEvaluator.accept(binding(f,independent),1,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.BAD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
            f.now().set(T.plusSeconds(41));try{oldPlanner.tick();}catch(TapstateException unavailable){assertThat(unavailable.code()).isEqualTo(io.tapstate.spi.store.IoError.DOCUMENT_UNREADABLE);}
            var channels=new AlertDeliveryRuntimeIT.Channels(old);try(var sender=AlertDeliveryRuntimeIT.runtime(f,old,channels,"known-only")){sender.tick();AlertDeliveryRuntimeIT.settled(sender);}
            assertThat(channels.calls).hasValue(2);for(var row:mixed)assertThat(old.intent(row.value().frozen().identity(),null).orElseThrow()).isEqualTo(row);
            var restored=new AlertDeliveryRuntimeIT.Channels(capable);try(var sender=AlertDeliveryRuntimeIT.runtime(f,capable,restored,"mixed-restored")){sender.tick();AlertDeliveryRuntimeIT.settled(sender);}
            assertThat(restored.calls).hasValue(2);for(var row:mixed)assertThat(capable.intent(row.value().frozen().identity(),null).orElseThrow().value().frozen()).isEqualTo(row.value().frozen());
        });
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void productionRefreshKeepsKnownInputsRunningWithoutEndingAnUnavailableRulesOccurrence(boolean futureFormat){
        AlertEvaluationHostIT.withFixture(f->{var catalog=AlertTypeCapabilityIT.catalog(1);var capable=f.configuration().alerts(f.storeHost(),catalog,f.clock());var original=rule(f,"unavailable-refresh","test.pipeline-condition",1,null,catalog);
            capable.saveDefinition(null,original,access());var originalBinding=binding(f,original);
            new AlertEvaluationHost(capable,catalog,f.storeHost().deployment(),f.clock(),"before-refresh",ACTIVE).accept(originalBinding,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null));
            var before=capable.state(originalBinding.key(),f.reads()).orElseThrow();
            try(var client=MongoClients.create(f.settings().uri())){
                var rules=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase()).getCollection("alert_rules");var saved=rules.find(new Document("_id.id","unavailable-refresh")).first();
                if(futureFormat){var future=Document.parse(saved.toJson());future.put("formatVersion",999);future.put("body",new Document("opaque",true));rules.replaceOne(new Document("_id",saved.get("_id")),future);}
                var old=f.configuration().alerts(f.storeHost(),BuiltinAlarmTypes.catalog(),f.clock());var evaluator=new AlertEvaluationHost(old,BuiltinAlarmTypes.catalog(),f.storeHost().deployment(),f.clock(),"known-refresh",ACTIVE);
                try(var runtime=new PipelineAlertRuntime(()->new PipelineAlertRuntime.Services(old,evaluator),f.configuration().intents(),(scope,fence)->Optional.of(ACTIVE),ACTIVE,f.clock())){
                    runtime.tick();org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).until(()->org.springframework.test.util.ReflectionTestUtils.getField(runtime,"services")!=null);
                    var pass=org.mockito.Mockito.mock(PipelineConvergencePass.class);org.mockito.Mockito.when(pass.scope()).thenReturn(f.intent().scope());org.mockito.Mockito.when(pass.intent()).thenReturn(f.intent());org.mockito.Mockito.when(pass.claim()).thenReturn(io.tapstate.spi.store.WorkloadClaimFence.from(f.claim()));
                    runtime.published(pass,new io.tapstate.core.lifecycle.Observation("orders",io.tapstate.core.lifecycle.PipelineState.FAILED,Map.of(),Map.of(),Map.of(),null,T,List.of()),null);
                    var known=f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE)).key();
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(()->assertThat(old.state(known,f.reads())).isPresent());
                    assertThat(old.state(originalBinding.key(),f.reads()).orElseThrow()).isEqualTo(before);assertThat(runtime.health().degraded()).isTrue();
                }finally{if(futureFormat)rules.replaceOne(new Document("_id",saved.get("_id")),saved);}
            }
        });
    }
    @ParameterizedTest @ValueSource(strings={"unknown-type","type-version","record-format"})
    void anUnavailableRuleKeepsItsOriginalWorkWhileKnownRulesStillAcceptAndPlan(String scenario){
        AlertEvaluationHostIT.withFixture(f->{boolean format=scenario.equals("record-format");int version=scenario.equals("type-version")?2:1;
            var creator=format?BuiltinAlarmTypes.catalog():AlertTypeCapabilityIT.catalog(version);
            var reader=scenario.equals("type-version")?AlertTypeCapabilityIT.catalog(1):BuiltinAlarmTypes.catalog();
            var host=f.storeHost().withChannels(Set.of(new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL)));
            var capable=f.configuration().alerts(host,creator,f.clock());var originalRoute=route(f,Duration.ZERO);
            var major=new AlertConfiguration.Match(originalRoute.match().resources(),Set.of(),Set.of(AlarmSeverity.MAJOR));
            assertThat(capable.saveDefinition(originalRoute,originalRoute.edit("Original route",major,true,originalRoute.channels(),originalRoute.groupBy(),originalRoute.grouping(),originalRoute.inhibition(),T,"operator"),access())).isEqualTo(AlertStore.Outcome.STAGED);
            var otherRoute=new AlertConfiguration.Subscription(AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.SUBSCRIPTION,host.scope(),"compatible-route"),host.deployment(),T,"operator"),"Compatible route",
                    new AlertConfiguration.Match(originalRoute.match().resources(),Set.of(),Set.of(AlarmSeverity.CRITICAL)),true,originalRoute.channels(),originalRoute.groupBy(),originalRoute.grouping(),originalRoute.inhibition());
            assertThat(capable.saveDefinition(null,otherRoute,access())).isEqualTo(AlertStore.Outcome.STAGED);
            var original=rule(f,"unavailable",format?"pipeline.failed":"test.pipeline-condition",version,null,creator);
            var compatible=rule(f,"compatible","pipeline.failed",1,AlarmSeverity.CRITICAL,creator);
            assertThat(capable.saveDefinition(null,original,access())).isEqualTo(AlertStore.Outcome.STAGED);
            assertThat(capable.saveDefinition(null,compatible,access())).isEqualTo(AlertStore.Outcome.STAGED);
            var originalBinding=binding(f,original);var originalPlanner=new AlertNotificationPlanner(capable,f.clock(),"original-rule-owner",null,ACTIVE);
            var evaluator=new AlertEvaluationHost(capable,creator,host.deployment(),f.clock(),"original-rule-evaluator",ACTIVE).withPlanner(originalPlanner);
            assertThat(evaluator.accept(originalBinding,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);originalPlanner.tick();
            var before=capable.deliveries(null,100,null,null,f.reads()).rows();assertThat(before).hasSize(2);
            var originalHead=capable.state(originalBinding.key(),f.reads()).orElseThrow();
            try(var client=MongoClients.create(f.settings().uri())){
                var database=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase());var rules=database.getCollection("alert_rules");
                var retained=rules.find(new Document("_id.id","unavailable")).first();var opaque=Document.parse(retained.toJson());
                if(format){opaque.put("formatVersion",999);opaque.put("body",new Document("uninterpretable","x".repeat(512*1024)).append("private-input","must-not-echo"));rules.replaceOne(new Document("_id",retained.get("_id")),opaque);}
                var old=f.configuration().alerts(host,reader,f.clock());var oldPlanner=new AlertNotificationPlanner(old,f.clock(),"old-rule-owner",null,ACTIVE);
                var oldEvaluator=new AlertEvaluationHost(old,reader,host.deployment(),f.clock(),"known-rule-evaluator",ACTIVE).withPlanner(oldPlanner);
                var channels=new AlertDeliveryRuntimeIT.Channels(old);
                try(var sender=AlertDeliveryRuntimeIT.runtime(f,old,channels,"known-rule-sender")){
                    var service=new AlertManagement(()->new PipelineAlertRuntime.Services(old,oldEvaluator,reader,null,sender),caller->access(),
                            ()->new PipelineAlertRuntime.Health(false,false,0,null,Map.of(),0,0),host.scope(),host.deployment(),f.clock(),new io.tapstate.control.core.AuditGate(r->{},f.clock()));
                    var listing=AlertManagementIT.call(service,"deliveries",Map.of());
                    assertThat((List<?>)listing.get("rows")).hasSize(2).allSatisfy(value->assertThat(AlertHttpModel.object(value)).containsEntry("displayState","PAUSED"));
                    if(format){assertThat(io.tapstate.core.common.JsonWriter.write(listing)).doesNotContain("uninterpretable","must-not-echo");
                        assertThat(AlertHttpModel.object(AlertHttpModel.object(((List<?>)listing.get("rows")).getFirst()).get("capability"))).containsEntry("reason","UNSUPPORTED_RECORD_VERSION").containsEntry("recordVersion",999);}
                    f.now().set(T.plusSeconds(31));
                    assertThat(oldEvaluator.accept(binding(f,compatible),1,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.BAD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
                    var roots=old.planning().schedules().stream().filter(row->row.value().batch()==null&&row.value().retiredAt()==null).toList();int first=0;
                    for(int i=0;i<roots.size();i++)if(roots.get(i).value().members().stream().anyMatch(member->member.ruleIdentity().id().equals("unavailable"))){first=i;break;}
                    org.springframework.test.util.ReflectionTestUtils.setField(oldPlanner,"offset",first);
                    try{oldPlanner.tick();}catch(TapstateException degraded){assertThat(degraded.code()).isIn(io.tapstate.spi.store.IoError.DOCUMENT_UNREADABLE,AlertError.INVALID_CONFIGURATION);}
                    var planned=old.deliveries(null,100,null,null,f.reads()).rows();assertThat(planned).hasSize(4);
                    for(int round=0;round<3;round++){sender.tick();AlertDeliveryRuntimeIT.settled(sender);}
                    assertThat(channels.calls).hasValue(2);assertThat(channels.requests).allSatisfy(request->assertThat(before).noneMatch(row->row.value().frozen().identity().deliveryId().equals(request.deliveryId())));
                    for(var row:before)assertThat(old.intent(row.value().frozen().identity(),null).orElseThrow()).isEqualTo(row);
                    assertThat(old.state(originalBinding.key(),f.reads()).orElseThrow()).isEqualTo(originalHead);
                    for(int resource=0;resource<12;resource++)old.maintenance().orElseThrow().maintain(new io.tapstate.spi.store.AlertMaintenance.Cursor(resource,null,null),200,4L*1024*1024,Duration.ofSeconds(5));
                    assertThat(rules.find(new Document("_id",retained.get("_id"))).first()).isEqualTo(format?opaque:retained);
                }
                if(format)rules.replaceOne(new Document("_id",retained.get("_id")),retained);
                var restored=new AlertDeliveryRuntimeIT.Channels(capable);
                try(var sender=AlertDeliveryRuntimeIT.runtime(f,capable,restored,"restored-rule-sender")){sender.tick();AlertDeliveryRuntimeIT.settled(sender);sender.tick();AlertDeliveryRuntimeIT.settled(sender);}
                assertThat(restored.calls).hasValue(2);
                for(var row:before){var accepted=capable.intent(row.value().frozen().identity(),null).orElseThrow().value();assertThat(accepted.frozen()).isEqualTo(row.value().frozen());assertThat(accepted.state().attemptsConsumed()).isOne();assertThat(accepted.state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);}
            }
        });
    }
    private static AlertConfiguration.Rule rule(AlertEvaluationHostIT.Fixture f,String id,String type,int version,AlarmSeverity severity,AlarmTypeCatalog catalog){
        return new AlertConfiguration.Rule(AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,f.storeHost().scope(),id),f.storeHost().deployment(),T,"operator"),id,type,version,f.rule().resources(),Map.of(),severity,true,null).validated(catalog).enrolled(null);
    }
    private static AlertEvaluationHost.Binding binding(AlertEvaluationHostIT.Fixture f,AlertConfiguration.Rule rule){return new AlertEvaluationHost.Binding(rule,new AlarmKey.Resource("pipeline","orders","1"),"isolation-producer",0,ACTIVE,()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));}
}
