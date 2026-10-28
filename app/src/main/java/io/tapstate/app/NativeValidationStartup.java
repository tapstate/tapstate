package io.tapstate.app;

import com.hazelcast.jet.core.JobStatus;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.PipelineDagBuilder;
import io.tapstate.spi.store.*;
import java.util.*;

/** Native metadata preflight before a validation owner claim; no Job, reader or hint is created. */
final class NativeValidationStartup implements NativeValidationOwnerReservations.Startup {
    private final Engine engine;private final ValidationPipelineInitials initials;
    private final MongoConfigurationDomain configuration;private final MongoCacheDomain cache;
    private final CacheBusinessScope scope;private final Runnable active;
    NativeValidationStartup(Engine engine,ValidationPipelineInitials initials,MongoConfigurationDomain configuration,MongoCacheDomain cache,CacheBusinessScope scope,Runnable active){
        this.engine=Objects.requireNonNull(engine);this.initials=Objects.requireNonNull(initials);this.configuration=Objects.requireNonNull(configuration);
        this.cache=Objects.requireNonNull(cache);this.scope=Objects.requireNonNull(scope);this.active=Objects.requireNonNull(active);
    }
    @Override public NativeValidationOwnerReservations.StartupGate inspect(ValidationSavedDeclarations saved,ValidationRunSpec spec){
        active.run();
        // An independent full scan does not inherit a joint-start readiness barrier.
        if(spec.mode()==ValidationMode.FULL)return NativeValidationOwnerReservations.StartupGate.ready();
        return inspectJoint(saved,spec);
    }
    NativeValidationOwnerReservations.StartupGate inspectJoint(ValidationSavedDeclarations saved,ValidationRunSpec spec){
        active.run();var pipeline=spec.task().pipelineId();var intent=configuration.intents().guardedBy(active).load(pipeline);
        if(intent.isEmpty()||intent.orElseThrow().deleted()||intent.orElseThrow().desired().targetState()!=PipelineState.RUNNING){
            throw invalid("startup.pipelineNotRunning");
        }
        var originalIntent=intent.orElseThrow();var prepared=configuration.preparations().read(pipeline).orElseThrow(()->invalid("startup.originalPipelineUnavailable"));
        if(prepared.phase()==PipelinePreparation.Phase.COMPLETE){
            var state=cache.validationPipelineState(prepared.scope(),active);
            if(originalIntent.desired().targetStateAt(state)!=PipelineState.RUNNING)throw invalid("startup.pipelineNotRunning");
            if(originalIntent.desired().rebuildOwedAt(state))throw invalid("startup.pipelineStartNotObserved");
        }
        var found=engine.captureRun(pipeline).orElseThrow(()->invalid("startup.pipelineStartNotObserved"));
        if(!(found.operatorManifest().executionIdentity() instanceof ExecutionFence original)||original.scope()==null
                ||!scope.equals(new CacheBusinessScope(original.scope().clusterId(),original.scope().bindingVersion(),original.scope().cacheIncarnation()))
                ||!originalIntent.scope().equals(original.scope()))throw invalid("startup.originalPipelineUnavailable");
        if(found.status().isTerminal())throw invalid("startup.pipelineNotRunning");
        if(found.status()!=JobStatus.RUNNING&&found.status()!=JobStatus.STARTING)throw invalid("startup.pipelineNotRunning");
        var outputs=new HashSet<String>();
        for(var mapping:spec.task().mappings()){
            var inspected=saved.mapping(mapping.mappingId());
            outputs.add(mapping.outputKind()==ValidationTaskSpec.OutputKind.VIEW?PipelineDagBuilder.viewVertex(inspected.view()):PipelineDagBuilder.serveVertex(inspected.sync(),0));
        }
        var selected=Set.copyOf(outputs);var first=read(original,selected);
        long job=found.jobId();
        return new NativeValidationOwnerReservations.StartupGate(first!=ValidationRunLifecycle.Readiness.READY,()->{
            active.run();var currentIntent=configuration.intents().guardedBy(active).load(pipeline).orElseThrow(()->invalid("startup.originalPipelineEnded"));
            if(!currentIntent.equals(originalIntent))throw invalid("startup.originalPipelineChanged");
            var current=engine.captureRun(pipeline).orElseThrow(()->invalid("startup.originalPipelineEnded"));
            if(current.jobId()!=job||!original.equals(current.operatorManifest().executionIdentity()))throw invalid("startup.originalPipelineChanged");
            var status=current.status();if(status!=JobStatus.RUNNING&&status!=JobStatus.STARTING)throw invalid("startup.originalPipelineEnded");
            var ready=read(original,selected);
            if(first==ValidationRunLifecycle.Readiness.READY&&ready==ValidationRunLifecycle.Readiness.WAITING_FOR_INITIAL_LOAD)throw invalid("startup.initialReadinessChanged");
            return ready;
        });
    }
    private ValidationRunLifecycle.Readiness read(ExecutionFence original,Set<String> outputs){
        var result=initials.read(original,outputs);active.run();
        if(result.state()==ValidationInitialLoadState.State.READY)return ValidationRunLifecycle.Readiness.READY;
        if(result.state()==ValidationInitialLoadState.State.WAITING)return ValidationRunLifecycle.Readiness.WAITING_FOR_INITIAL_LOAD;
        if(Set.of("initial.lookup-unknown","initial.lookup-interrupted").contains(result.reason()))return ValidationRunLifecycle.Readiness.UNKNOWN;
        throw invalid(result.reason());
    }
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field",field),null);}
}
