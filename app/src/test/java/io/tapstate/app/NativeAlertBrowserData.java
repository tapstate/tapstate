package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import org.springframework.context.ConfigurableApplicationContext;

/** Synthetic classified observations over the real admitted store, solely for opt-in browser verification. */
final class NativeAlertBrowserData {
    private final AlertEvaluationHost evaluator;
    private final PipelineAlertRuntime runtime;
    private Object slot;
    private final List<AlertEvaluationHost.Binding> bindings=new ArrayList<>();
    private final Map<AlertEvaluationHost.Binding,java.util.concurrent.atomic.AtomicLong> sequences=new HashMap<>();
    NativeAlertBrowserData(ConfigurableApplicationContext application,NativeAppStorage storage,
            java.util.function.BiConsumer<String,Map<String,Object>> request){
        var child=NativeAppStorage.business(application);
        var configuration=application.getBean(StorageRoot.class).runtime().requireConfiguration();
        var services=child.getBean(PipelineAlertRuntime.class).managementServices();
        evaluator=services.evaluator();
        runtime=child.getBean(PipelineAlertRuntime.class);
        String pipelineId="browser-orders";
        { String id=pipelineId;var pipeline=new PipelineResource(id,null,List.of(),null,null,null,null,null);
            configuration.artifacts().create(pipeline);var business=storage.preparation().businessScope();
            var scope=new PipelineIntentScope(business.clusterId(),business.bindingVersion(),business.cacheIncarnation(),id,1);
            configuration.intents().compareAndSet(scope,0,new DesiredState(id,PipelineState.STOPPED,CanonicalHash.of(pipeline)));
            child.getBean(PipelineFirstStatePreparer.class).prepare(scope);
        }
        for(int i=0;i<6;i++){
            String id="browser-rule-"+i;
            request.accept("configuration/save",Map.of("kind","RULE","id",id,"etag","0","form",Map.of(
                    "name","Browser synthetic "+i,"typeId","pipeline.failed","typeVersion",1,"resourceKinds",List.of("pipeline"),
                    "allResources",false,"resources",List.of(Map.of("kind","pipeline","id",pipelineId,"incarnation","1")),
                    "parameters",Map.of(),"severity",i==0?"CRITICAL":i==1?"WARNING":"MAJOR","enabled",true)));
            var rule=(AlertConfiguration.Rule)services.store().evaluationRules(null,100).rows().stream().filter(r->r.header().identity().id().equals(id)).findFirst().orElseThrow();
            var slots=(Map<?,?>)org.springframework.test.util.ReflectionTestUtils.getField(runtime,"slots");
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).until(()->slots.containsKey(pipelineId));
            slot=slots.get(pipelineId);
            AlertEvaluationHost.Binding binding=org.springframework.test.util.ReflectionTestUtils.invokeMethod(runtime,"binding",slot,rule);
            sequences.put(binding,(java.util.concurrent.atomic.AtomicLong)org.springframework.test.util.ReflectionTestUtils.getField(slot,"sequence"));
            bindings.add(binding);evaluate(binding,AlertSignal.Condition.BAD,"java.lang.IllegalStateException: synthetic browser failure\n\tat example.Pipeline.run(Pipeline.java:12)");
        }
    }
    private void evaluate(AlertEvaluationHost.Binding binding,AlertSignal.Condition condition,String trace){
        var completion=new java.util.concurrent.CompletableFuture<AlertEvaluationHost.Result>();
        Runnable action=()->{try{completion.complete(evaluator.accept(binding,sequences.get(binding).getAndIncrement(),new AlertInput.Conditions(Instant.now(),condition,trace==null?null:new AlarmDiagnostics.SafeTrace(trace,1,false,false))));}catch(Throwable failure){completion.completeExceptionally(failure);}};
        org.springframework.test.util.ReflectionTestUtils.invokeMethod(runtime,"submit",slot,0,action);
        try {var result=completion.get(30,java.util.concurrent.TimeUnit.SECONDS);if(result!=AlertEvaluationHost.Result.ACCEPTED)throw new AssertionError(result);}
        catch(Exception failure){throw new AssertionError(failure);}
    }

    void recover(){evaluate(bindings.getFirst(),AlertSignal.Condition.GOOD,null);}
    void refreshDiagnostic(){evaluate(bindings.get(1),AlertSignal.Condition.BAD,"java.lang.IllegalStateException: newer synthetic evidence\n\tat example.Pipeline.retry(Pipeline.java:24)");}
    String capabilityProbe(ConfigurableApplicationContext application){
        var configuration=application.getBean(StorageRoot.class).runtime().requireConfiguration();
        var services=NativeAppStorage.business(application).getBean(PipelineAlertRuntime.class).managementServices();
        var host=services.store().planning().host();var now=Clock.systemUTC();
        var token=new TokenRecord("capability-seed","ADMIN","isolated-browser-seed",false,now.instant());configuration.tokens().save(token);
        var retainedToken=configuration.tokens().find(token.tokenId()).orElseThrow();
        var access=new AlertStore.Access(new AlertTestAuthority(AlertTestAuthority.Mechanism.MACHINE_TOKEN,host.setup().installationId(),retainedToken.tokenId(),"ADMIN",retainedToken.authenticationBinding(),null),()->{},()->{},()->"");
        var types=new ArrayList<>(BuiltinAlarmTypes.catalog().types());String type="test.browser-capability";
        types.add(new AlarmTypeCatalog.Type(type,2,AlarmTypeCatalog.Kind.STATEFUL,Set.of("pipeline"),"pipeline-lifecycle","condition",List.of(),AlarmSeverity.MAJOR,false,"test.condition","test.recovery","test.guidance"));
        var catalog=new AlarmTypeCatalog(types);var store=configuration.alerts(host,catalog,now);var original=bindings.getFirst();
        var header=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,host.scope(),"browser-capability"),host.deployment(),now.instant(),token.tokenId());
        var rule=new AlertConfiguration.Rule(header,"Unavailable type fixture",type,2,new AlertConfiguration.Resources(Set.of("pipeline"),false,Set.of(original.resource())),Map.of(),null,true,null).validated(catalog).enrolled(null);
        if(store.saveDefinition(null,rule,access)!=AlertStore.Outcome.STAGED)throw new AssertionError("capability fixture rule");
        var planner=new AlertNotificationPlanner(store,now,"capability-browser-planner",null,()->{});
        var evaluator=new AlertEvaluationHost(store,catalog,host.deployment(),now,"capability-browser-evaluator",()->{}).withPlanner(planner);
        var binding=new AlertEvaluationHost.Binding(rule,original.resource(),"capability-browser-producer",original.sourceBindingRevision(),original.requireCurrent(),original.proof());
        org.awaitility.Awaitility.await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(60)).untilAsserted(()->{
            var result=evaluator.accept(binding,1,new AlertInput.Conditions(now.instant(),AlertSignal.Condition.BAD,null));
            if(result!=AlertEvaluationHost.Result.ACCEPTED&&result!=AlertEvaluationHost.Result.IGNORED)throw new AssertionError("capability fixture state: "+result);
            var retained=store.state(binding.key(),new AlertStore.ReadAccess(true,Set.of(),()->{})).orElseThrow();
            if(retained.value().snapshot().status()!=AlertSnapshot.Status.FIRING||retained.value().snapshot().lastSequence()!=1)throw new AssertionError("capability fixture state not retained");
        });
        var delivery=new java.util.concurrent.atomic.AtomicReference<String>();
        org.awaitility.Awaitility.await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(60)).untilAsserted(()->{
            try{planner.tick();}catch(io.tapstate.core.common.TapstateException conflict){if(conflict.code()!=AlertError.COMMAND_CONFLICT)throw conflict;}
            var found=store.deliveries(null,100,null,null,new AlertStore.ReadAccess(true,Set.of(),()->{})).rows().stream()
                    .filter(row->row.value().frozen().members().stream().anyMatch(member->member.evaluation().key().typeId().equals(type)))
                    .map(row->row.value().frozen().identity().deliveryId()).findFirst();
            if(found.isEmpty())throw new AssertionError("capability fixture is not planned");delivery.set(found.orElseThrow());
        });return delivery.get();
    }
}
