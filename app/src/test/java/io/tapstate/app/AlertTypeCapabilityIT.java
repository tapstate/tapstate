package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.AlertStore;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import static io.tapstate.app.AlertNotificationPlannerIT.*;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class AlertTypeCapabilityIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    static AlarmTypeCatalog catalog(int version){var types=new ArrayList<>(BuiltinAlarmTypes.catalog().types());
        types.add(new AlarmTypeCatalog.Type("test.pipeline-condition",version,AlarmTypeCatalog.Kind.STATEFUL,Set.of("pipeline"),"pipeline-lifecycle","condition",List.of(),
                AlarmSeverity.MAJOR,false,"test.condition","test.recovery","test.guidance"));return new AlarmTypeCatalog(types);}
    @Test void readersAndWritersCanInspectSupportedDeliveriesWithoutConfigurationPrivileges(){
        AlertEvaluationHostIT.withFixture(f->{AlertDeliveryRuntimeIT.seed(f);var store=sendingStore(f);var queued=intents(f).getFirst();var channels=new AlertDeliveryRuntimeIT.Channels(store);
            try(var worker=AlertDeliveryRuntimeIT.runtime(f,store,channels,"read-capability")){
                for(var permission:List.of(io.tapstate.control.core.Scope.READ,io.tapstate.control.core.Scope.WRITE)){
                    var token=new io.tapstate.spi.store.TokenRecord("operator",permission.name(),"isolated-test-hash",false,T);f.configuration().tokens().save(token);
                    var readAccess=new AlertStore.Access(new AlertTestAuthority(AlertTestAuthority.Mechanism.MACHINE_TOKEN,"installation-a","operator",permission.name(),token.authenticationBinding(),null),ACTIVE,ACTIVE,()->"");
                    var service=new AlertManagement(()->new PipelineAlertRuntime.Services(store,f.evaluator(),BuiltinAlarmTypes.catalog(),null,worker),caller->readAccess,
                            ()->new PipelineAlertRuntime.Health(false,false,0,null,Map.of(),0,0),f.storeHost().scope(),f.storeHost().deployment(),f.clock(),new io.tapstate.control.core.AuditGate(r->{},f.clock()));
                    var caller=AlertManagementIT.caller(permission,ACTIVE);
                    var listing=service.dispatch("alert.deliveries",caller,Map.of());assertThat((List<?>)listing.get("rows")).hasSize(2);
                    var detail=service.dispatch("alert.deliveries",caller,Map.of("id",queued.value().frozen().identity().deliveryId()));
                    assertThat(detail).containsEntry("displayState","QUEUED").doesNotContainKey("capability");
                    assertThat(store.delivery(queued.value().frozen().identity().deliveryId(),f.reads()).orElseThrow()).isEqualTo(queued);
                }
            }
        });
    }
    @Test void anObsoleteUnknownFrozenTypeDoesNotTakeTheSlotsOfLaterKnownWork(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var catalog=catalog(1);var host=f.storeHost().withChannels(Set.of(new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL)));
            var capable=f.configuration().alerts(host,catalog,f.clock());
            var rule=new AlertConfiguration.Rule(AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,host.scope(),"obsolete-type"),host.deployment(),T,"operator"),"Obsolete type","test.pipeline-condition",1,f.rule().resources(),Map.of(),null,true,null).validated(catalog).enrolled(null);
            capable.saveDefinition(null,rule,access());var originalPlanner=new AlertNotificationPlanner(capable,f.clock(),"obsolete-planner",null,ACTIVE);
            var evaluator=new AlertEvaluationHost(capable,catalog,host.deployment(),f.clock(),"obsolete-evaluator",ACTIVE).withPlanner(originalPlanner);
            var binding=new AlertEvaluationHost.Binding(rule,new AlarmKey.Resource("pipeline","orders","1"),"obsolete-producer",0,ACTIVE,()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
            evaluator.accept(binding,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null));originalPlanner.tick();var originals=capable.deliveries(null,10,null,null,f.reads()).rows();assertThat(originals).hasSize(2);
            f.now().set(T.plusSeconds(1));f.evaluator().withPlanner(originalPlanner);bad(f);originalPlanner.tick();assertThat(capable.deliveries(null,10,null,null,f.reads()).rows()).hasSize(4);
            f.now().set(T.plusSeconds(2));var replacement=new AlertConfiguration.Rule(rule.header().next(f.clock().instant(),"operator"),"New semantics","pipeline.failed",1,rule.resources(),Map.of(),null,true,null);
            assertThat(capable.saveDefinition(rule,replacement,access())).isEqualTo(AlertStore.Outcome.STAGED);
            var old=f.configuration().alerts(host,BuiltinAlarmTypes.catalog(),f.clock());var channels=new AlertDeliveryRuntimeIT.Channels(old);
            try(var sender=AlertDeliveryRuntimeIT.runtime(f,old,channels,"old-catalog-sender")){sender.tick();AlertDeliveryRuntimeIT.settled(sender);assertThat(channels.calls).hasValue(2);}
            for(var original:originals)assertThat(old.intent(original.value().frozen().identity(),null).orElseThrow()).isEqualTo(original);
        });
    }
    @Test void unsupportedTypeOrVersionCannotGrantAnOriginalTaskAndRestoredSupportUsesItsOriginalIdentity(){
        for(int version:List.of(1,2))for(boolean exhausted:List.of(false,true))AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var catalog=catalog(version);var host=f.storeHost().withChannels(Set.of(new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL)));
            var supported=f.configuration().alerts(host,catalog,f.clock());
            var header=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,host.scope(),"capability-rule"),host.deployment(),T,"operator");
            var rule=new AlertConfiguration.Rule(header,"Capability rule","test.pipeline-condition",version,f.rule().resources(),Map.of(),null,true,null).validated(catalog).enrolled(null);
            assertThat(supported.saveDefinition(null,rule,access())).isEqualTo(AlertStore.Outcome.STAGED);
            var planner=new AlertNotificationPlanner(supported,f.clock(),"capable-planner",null,ACTIVE);
            var evaluator=new AlertEvaluationHost(supported,catalog,host.deployment(),f.clock(),"capable-evaluator",ACTIVE).withPlanner(planner);
            var binding=new AlertEvaluationHost.Binding(rule,new AlarmKey.Resource("pipeline","orders","1"),"capability-producer",0,ACTIVE,()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
            assertThat(evaluator.accept(binding,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);planner.tick();
            var originals=supported.deliveries(null,10,null,null,f.reads()).rows();assertThat(originals).hasSize(2);
            if(exhausted){var temporary=new AlertDeliveryRuntimeIT.Channels(supported);
                temporary.answer=request->new io.tapstate.spi.alerting.NotificationChannel.Result(DeliveryEvidence.Outcome.UNKNOWN,io.tapstate.spi.alerting.NotificationChannel.Action.RETRY,null,io.tapstate.spi.alerting.NotificationChannel.Phase.DEADLINE,0,"controlled-unknown");
                try(var worker=AlertDeliveryRuntimeIT.runtime(f,supported,temporary,"before-type-loss")){
                    for(int round=0;round<12;round++){worker.tick();AlertDeliveryRuntimeIT.settled(worker);
                        var next=supported.deliveries(null,10,null,null,f.reads()).rows().stream().map(row->row.value().state().nextAttemptAt()).filter(Objects::nonNull).min(java.time.Instant::compareTo);
                        if(next.isEmpty())break;f.now().set(next.orElseThrow().plusSeconds(1));
                    }
                }
                originals=supported.deliveries(null,10,null,null,f.reads()).rows();assertThat(originals).allSatisfy(row->{assertThat(row.value().state().attemptsConsumed()).isEqualTo(5);assertThat(row.value().state().nextAttemptAt()).isNull();assertThat(row.value().state().permitsStopped()).isFalse();});
            }
            var oldCatalog=version==1?BuiltinAlarmTypes.catalog():catalog(1);var old=f.configuration().alerts(host,oldCatalog,f.clock());
            var channels=new AlertDeliveryRuntimeIT.Channels(old);
            try(var worker=AlertDeliveryRuntimeIT.runtime(f,old,channels,"unsupported-type-worker")){worker.tick();AlertDeliveryRuntimeIT.settled(worker);
                var service=new AlertManagement(()->new PipelineAlertRuntime.Services(old,new AlertEvaluationHost(old,oldCatalog,host.deployment(),f.clock(),"unsupported-read",ACTIVE),oldCatalog,null,worker),caller->access(),
                        ()->new PipelineAlertRuntime.Health(false,false,0,null,Map.of(),0,0),host.scope(),host.deployment(),f.clock(),new io.tapstate.control.core.AuditGate(r->{},f.clock()));
                for(var original:originals){var projected=AlertManagementIT.call(service,"deliveries",Map.of("id",original.value().frozen().identity().deliveryId()));
                    assertThat(projected).containsEntry("displayState","PAUSED").containsEntry("canRetry",false);
                    assertThat(AlertHttpModel.object(projected.get("capability"))).containsEntry("typeId",rule.typeId()).containsEntry("typeVersion",version==1?null:version)
                            .containsEntry("reason",version==1?"UNKNOWN_TYPE":"UNSUPPORTED_TYPE_VERSION");}
            }
            assertThat(channels.calls).hasValue(0);
            if(exhausted)for(var original:originals)assertThatThrownBy(()->new AlertDeliveryCommands(old,host.scope(),host.deployment(),f.clock()).retry(original.value().frozen().identity(),original.revision(),access())).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            for(var original:originals)assertThat(old.intent(original.value().frozen().identity(),null).orElseThrow()).isEqualTo(original);
            f.now().set(f.clock().instant().plusSeconds(31));for(int collection=0;collection<12;collection++)old.maintenance().orElseThrow().maintain(new io.tapstate.spi.store.AlertMaintenance.Cursor(collection,null,null),200,4L*1024*1024,Duration.ofSeconds(5));
            if(exhausted)for(var original:originals)assertThat(new AlertDeliveryCommands(supported,host.scope(),host.deployment(),f.clock()).retry(original.value().frozen().identity(),original.revision(),access())).isEqualTo(AlertStore.Outcome.STAGED);
            var restored=new AlertDeliveryRuntimeIT.Channels(supported);
            try(var worker=AlertDeliveryRuntimeIT.runtime(f,supported,restored,"restored-type-worker")){worker.tick();AlertDeliveryRuntimeIT.settled(worker);}
            assertThat(restored.calls).hasValue(2);
            for(var original:originals){var after=supported.intent(original.value().frozen().identity(),null).orElseThrow().value();
                assertThat(after.frozen()).isEqualTo(original.value().frozen());assertThat(after.state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);assertThat(after.state().attemptsConsumed()).isEqualTo(exhausted?6:1);}
        });
    }
}
