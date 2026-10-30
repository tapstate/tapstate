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
    @Test void unsupportedTypeOrVersionCannotGrantAnOriginalTaskAndRestoredSupportUsesItsOriginalIdentity(){
        for(int version:List.of(1,2))AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var catalog=catalog(version);var host=f.storeHost().withChannels(Set.of(new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL)));
            var supported=f.configuration().alerts(host,catalog,f.clock());
            var header=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,host.scope(),"capability-rule"),host.deployment(),T,"operator");
            var rule=new AlertConfiguration.Rule(header,"Capability rule","test.pipeline-condition",version,f.rule().resources(),Map.of(),null,true,null).validated(catalog).enrolled(null);
            assertThat(supported.saveDefinition(null,rule,access())).isEqualTo(AlertStore.Outcome.STAGED);
            var planner=new AlertNotificationPlanner(supported,f.clock(),"capable-planner",null,ACTIVE);
            var evaluator=new AlertEvaluationHost(supported,catalog,host.deployment(),f.clock(),"capable-evaluator",ACTIVE).withPlanner(planner);
            var binding=new AlertEvaluationHost.Binding(rule,new AlarmKey.Resource("pipeline","orders","1"),"capability-producer",0,ACTIVE,()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
            assertThat(evaluator.accept(binding,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);planner.tick();
            var originals=supported.deliveries(null,10,null,null,f.reads()).rows();assertThat(originals).hasSize(2);
            var old=f.configuration().alerts(host,version==1?BuiltinAlarmTypes.catalog():catalog(1),f.clock());
            var channels=new AlertDeliveryRuntimeIT.Channels(old);
            try(var worker=AlertDeliveryRuntimeIT.runtime(f,old,channels,"unsupported-type-worker")){worker.tick();AlertDeliveryRuntimeIT.settled(worker);}
            assertThat(channels.calls).hasValue(0);
            for(var original:originals)assertThat(old.intent(original.value().frozen().identity(),null).orElseThrow()).isEqualTo(original);
            f.now().set(T.plusSeconds(31));for(int collection=0;collection<12;collection++)old.maintenance().orElseThrow().maintain(new io.tapstate.spi.store.AlertMaintenance.Cursor(collection,null,null),200,4L*1024*1024,Duration.ofSeconds(5));
            var restored=new AlertDeliveryRuntimeIT.Channels(supported);
            try(var worker=AlertDeliveryRuntimeIT.runtime(f,supported,restored,"restored-type-worker")){worker.tick();AlertDeliveryRuntimeIT.settled(worker);}
            assertThat(restored.calls).hasValue(2);
            for(var original:originals){var after=supported.intent(original.value().frozen().identity(),null).orElseThrow().value();
                assertThat(after.frozen()).isEqualTo(original.value().frozen());assertThat(after.state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);assertThat(after.state().attemptsConsumed()).isOne();}
        });
    }
}
