package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.spi.store.*;
import java.util.Map;
import java.util.Objects;

/** Independent native CONFIG birth, CACHE tenure and alias membership checks on control IO. */
final class NativeValidationCaptureAuthority implements NativeValidationCaptureRegistrar.Authority {
    private final MongoConfigurationDomain configuration;private final MongoCacheDomain cache;private final CacheBusinessScope scope;
    private final SrsStoreViews sources;private final ExecutionAuthorization authorization;private final Runnable active;
    NativeValidationCaptureAuthority(MongoConfigurationDomain configuration,MongoCacheDomain cache,CacheBusinessScope scope,
            SrsStoreViews sources,ExecutionAuthorization authorization,Runnable active){
        this.configuration=Objects.requireNonNull(configuration);this.cache=Objects.requireNonNull(cache);this.scope=Objects.requireNonNull(scope);
        this.sources=Objects.requireNonNull(sources);this.authorization=Objects.requireNonNull(authorization);this.active=Objects.requireNonNull(active);
    }
    @Override public void run(ValidationReportAdmission.Start original,ExecutionFence production){
        active.run();var run=original.run();var physical=production.scope();
        if(physical==null||!scope.clusterId().equals(run.owner().clusterId())||!run.owner().pipelineId().equals(production.pipelineId())
                ||!scope.equals(new CacheBusinessScope(physical.clusterId(),physical.bindingVersion(),physical.cacheIncarnation())))throw invalid("capture.runScope");
        authorization.require(production);
        var task=configuration.validationTasks(active).read(original.task().pipelineId(),original.task().taskId()).orElseThrow(()->invalid("capture.originalBirth"));
        var permission=task.mode(run.mode()).permission();
        if(!task.task().equals(original.task())||task.closed()||permission==null||!permission.start().equals(original)
                ||permission.phase()!=ValidationTaskStore.Phase.BORN)throw invalid("capture.originalBirth");
        var held=configuration.loadValidationAccess(production.pipelineId(),scope,cache,active).claims().read(ValidationClaims.key(scope.clusterId(),production.pipelineId())).orElseThrow(()->invalid("capture.validationOwner"));
        if(!held.leased()||!ValidationClaims.owner(production.pipelineId(),held.claim()).equals(run.owner()))throw invalid("capture.validationOwner");
        authorization.require(production);active.run();
    }
    @Override public void capture(ValidationCaptureRoute route){
        active.run();authorization.require(route.execution());authorization.requireCapture(scope,route.capture());
        if(!scope.equals(route.preparation().scope()))throw invalid("capture.physicalScope");
        var current=cache.capturePreparations(scope,active).load(route.preparation());
        if(!current.preparation().equals(route.preparation())||!current.claim().equals(route.capture()))throw invalid("capture.physicalBirth");
        var execution=route.execution();var claim=new WorkloadClaimFence(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,execution.pipelineId()),
                new WorkloadOwner(execution.ownerNodeId(),execution.ownerBootId()),execution.claimGeneration(),execution.executionGeneration(),execution.topologyRevision());
        if(route.membershipRevision()>0){
            var chain=current.chain().orElseThrow(()->invalid("capture.physicalChain"));
            if(chain.epoch()!=route.epoch()||!chain.miningChainId().equals(route.channel())||!chain.servingTables().contains(route.table()))throw invalid("capture.physicalChain");
            var key=new SrsConsumerKey(route.channel(),execution.pipelineId(),route.alias());
            var member=sources.srs(scope,active).consumers().find(key).orElseThrow(()->invalid("capture.aliasMembership"));
            if(member.detached()||!member.context().equals(new SrsConsumerContext(key,execution.scope().stateGeneration()))
                    ||member.revision()!=route.membershipRevision()||!member.commandFence().equals(claim)||!member.tables().contains(route.table()))throw invalid("capture.aliasMembership");
        }else{
            var context=new DirectChannelContext(execution.scope(),route.channel(),route.alias(),route.preparation().captureId());
            var direct=sources.directChannels(scope,active).load(context);var window=direct.current();
            if(window==null||direct.epoch()!=route.epoch()||window.epoch()!=route.epoch()||!window.captureFence().equals(route.capture())
                    ||!window.pipelineFence().equals(claim)||!direct.tables().contains(route.table()))throw invalid("capture.aliasChannel");
        }
        authorization.require(route.execution());authorization.requireCapture(scope,route.capture());active.run();
    }
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field",field),null);}
}
