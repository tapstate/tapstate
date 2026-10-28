package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.spi.store.*;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/** Read-only native route lookup. A reply is metadata; the receiving capture revalidates authority. */
final class NativeValidationCaptureRoutes implements AutoCloseable {
    static final String CONTEXT="tapstate.validation-capture-routes",EXECUTOR=CONTEXT;
    private final HazelcastInstance member;
    private final CacheBusinessScope scope;
    private final ScopedPipelineCaptureCoordinator captures;
    private final Runnable active;
    private final Consumer<ExecutionFence> authorized;
    private final ValidationMemberResources.Member resources;
    private volatile boolean closed;
    NativeValidationCaptureRoutes(HazelcastInstance member,CacheBusinessScope scope,ScopedPipelineCaptureCoordinator captures,
            Runnable active,Consumer<ExecutionFence> authorized,ValidationMemberResources.Member resources){
        this.member=Objects.requireNonNull(member);this.scope=Objects.requireNonNull(scope);this.captures=Objects.requireNonNull(captures);
        this.active=Objects.requireNonNull(active);this.authorized=Objects.requireNonNull(authorized);
        this.resources=Objects.requireNonNull(resources);
        if(member.getUserContext().putIfAbsent(CONTEXT,this)!=null)throw invalid("capture.routeAlreadyBound");
    }
    ValidationCaptureRoute read(ExecutionFence original,String alias,String table){
        try(var paid=resources.admitMetadata()){
            return readPaid(original,alias,table,paid);
        }
    }
    ValidationCaptureRoute readPaid(ExecutionFence original,String alias,String table,ValidationMemberResources.Metadata paid){
        paid.requireRetained(resources);
        require(original);ValidationCaptureRoute.identifier(alias,"capture.alias");ValidationCaptureRoute.identifier(table,"capture.table");
        var targets=member.getCluster().getMembers().stream().filter(value->matches(value,original)).toList();
        if(targets.size()!=1)throw invalid("capture.routeOwnerUnavailable");var target=targets.getFirst();
        if(target.localMember())return localPaid(original,alias,table,paid);
        try{
            var wire=member.getExecutorService(EXECUTOR).submitToMember(new Query(original,alias,table,target.getUuid()),target).get(3,TimeUnit.SECONDS);
            require(original);var route=wire.route();
            if(!route.execution().equals(original)||!route.alias().equals(alias)||!route.table().equals(table))throw invalid("capture.routeReply");
            return route;
        }catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","capture.routeInterrupted"),interrupted);}
        catch(ExecutionException|TimeoutException unknown){throw new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","capture.routeUnknown"),unknown);}
    }
    private ValidationCaptureRoute localPaid(ExecutionFence original,String alias,String table,ValidationMemberResources.Metadata paid){
        paid.requireRetained(resources);
        require(original);if(!matches(member.getCluster().getLocalMember(),original))throw invalid("capture.routeReceiver");
        var route=captures.validationRoute(original,alias,table);require(original);return route;
    }
    private void require(ExecutionFence original){
        if(closed)throw invalid("capture.routeClosed");active.run();var held=Objects.requireNonNull(original).scope();
        if(held==null||!scope.equals(new CacheBusinessScope(held.clusterId(),held.bindingVersion(),held.cacheIncarnation())))throw invalid("capture.routeScope");
        authorized.accept(original);
    }
    private static boolean matches(Member member,ExecutionFence original){return !member.isLiteMember()
            &&original.ownerNodeId().equals(member.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE))
            &&original.ownerBootId().equals(member.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE));}
    @Override public void close(){closed=true;member.getUserContext().remove(CONTEXT,this);}
    private static final class Query implements Callable<Wire>,Serializable,HazelcastInstanceAware{
        private static final long serialVersionUID=1L;
        private final ExecutionFence original;private final String alias,table;private final UUID receiver;private transient HazelcastInstance local;
        Query(ExecutionFence original,String alias,String table,UUID receiver){this.original=original;this.alias=alias;this.table=table;this.receiver=receiver;}
        @Override public void setHazelcastInstance(HazelcastInstance value){local=value;}
        @Override public Wire call(){if(!local.getCluster().getLocalMember().getUuid().equals(receiver))throw invalid("capture.routeReceiver");
            var context=local.getUserContext().get(CONTEXT);if(!(context instanceof NativeValidationCaptureRoutes routes))throw invalid("capture.routeUnavailable");
            try(var paid=routes.resources.admitMetadata()){return Wire.of(routes.localPaid(original,alias,table,paid));}}
    }
    /** Flatten only immutable coordinates: native preparation/claim objects do not cross the wire. */
    private record Wire(ExecutionFence execution,String alias,String table,String mode,String chain,String capture,String identity,
            String preparation,String canonical,String birth,String node,String boot,long claim,long executionGeneration,long topology,
            String channel,long epoch,long membership) implements Serializable {
        static Wire of(ValidationCaptureRoute route){var p=route.preparation();var c=route.capture();return new Wire(route.execution(),route.alias(),route.table(),p.mode().name(),p.chainId(),p.captureId(),p.sourceIdentityHash(),p.preparationId(),p.canonicalChainId(),p.birthPreparationId(),c.owner().nodeId(),c.owner().bootId(),c.claimGeneration(),c.executionGeneration(),c.topologyRevision(),route.channel(),route.epoch(),route.membershipRevision());}
        ValidationCaptureRoute route(){var s=execution.scope();var p=new CapturePreparation(new CacheBusinessScope(s.clusterId(),s.bindingVersion(),s.cacheIncarnation()),CapturePreparation.Mode.valueOf(mode),chain,capture,identity,preparation,canonical,birth);
            return new ValidationCaptureRoute(execution,alias,table,p,new WorkloadClaimFence(new WorkloadClaimKey(s.clusterId(),WorkloadClaimType.CAPTURE,capture),new WorkloadOwner(node,boot),claim,executionGeneration,topology),channel,epoch,membership);}
    }
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field",field),null);}
}
