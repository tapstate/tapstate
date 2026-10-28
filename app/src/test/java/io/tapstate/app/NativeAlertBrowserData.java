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
}
