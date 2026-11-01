package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.PdkExternalResourceRetirement;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

/** Persistent original clear obligations outlive the Pipeline command that relinquished them. */
final class NativeCaptureExternalRetirement implements AutoCloseable {
    private static final org.slf4j.Logger LOG=org.slf4j.LoggerFactory.getLogger(NativeCaptureExternalRetirement.class);
    private final CacheBusinessScope business;
    private final CaptureExternalRetirements configuration;
    private final CaptureClearCache cache;
    private final RequiredNoteStore notes;
    private final PdkExternalResourceRetirement source;
    private final ConnectorProvisioner connectors;
    private final Supplier<Optional<WorkloadClaimFence>> node;
    private final Runnable active;
    private final HazelcastInstance member;
    private final ScopedPipelineCaptureCoordinator captures;
    private static final String LOCAL="tapstate.native.capture.external-retirement";
    private final Map<String,Work> running=new HashMap<>();
    private boolean closed;
    private final ScheduledExecutorService executor=Executors.newSingleThreadScheduledExecutor(task -> {
        var thread=new Thread(task,"tapstate-external-capture-reconciliation");thread.setDaemon(true);return thread;
    });
    private record Work(CaptureClearPreparation original,CaptureExternalRetirements.Attempt attempt,
            CompletableFuture<PdkExternalResourceRetirement.Result> answer) {}
    NativeCaptureExternalRetirement(CacheBusinessScope business,CaptureExternalRetirements configuration,
            CaptureClearCache cache,RequiredNoteStore notes,ConnectorProvisioner connectors,
            Supplier<Optional<WorkloadClaimFence>> node,Runnable active,HazelcastInstance member,ScopedPipelineCaptureCoordinator captures) {
        this.business=business;this.configuration=configuration;this.cache=cache;this.notes=notes;
        this.source=new PdkExternalResourceRetirement(connectors,notes);this.node=node;this.active=active;this.member=member;
        this.connectors=connectors;
        this.captures=captures;member.getUserContext().put(LOCAL,this);
        executor.scheduleWithFixedDelay(this::maintain,1,1,TimeUnit.SECONDS);
    }

    synchronized void relinquished(PipelineRebuildPreparation original,List<SrsConsumerMembership> previous,
            Map<String,io.tapstate.core.model.SourceResource> desired) {
        active.run();if(closed)throw ThreeLayerStorePort.refused();
        var aliases=aliases(original.request().originalScope(),original.request().operationId(),desired);
        configuration.relinquishAliases(original,aliases);
        schedule(original.request().originalScope().pipelineId(),previous,aliases,(observations,related,name,origin,automatic,members) -> configuration.reserve(original,observations,related,name,origin,automatic,members));
    }
    synchronized void deleted(PipelineRetirementReceipt receipt,List<SrsConsumerMembership> previous) {
        active.run();if(closed)throw ThreeLayerStorePort.refused();var original=receipt.preparation();
        var aliases=aliases(original.request().scope(),original.request().operationId(),null);
        configuration.relinquishAliases(original,aliases);
        schedule(original.request().scope().pipelineId(),previous,aliases,(observations,related,name,origin,automatic,members) -> configuration.reserve(receipt,observations,related,name,origin,automatic,members));
    }
    private List<CaptureAliasObservation> aliases(PipelineIntentScope original,String operation,Map<String,io.tapstate.core.model.SourceResource> desired) {
        var aliases=new ArrayList<CaptureAliasObservation>();
        var aliasInventory=configuration.resourceNotes(business);
        var shared=configuration.sharedCaptures(business).stream().map(CapturePreparation::canonicalChainId).collect(java.util.stream.Collectors.toSet());
        for(var context:notes.privateContexts(original,operation)) {
            if(context.completed() || context.origin().isEmpty() || !context.scope().owner().hasPurpose(io.tapstate.core.model.PipelineNode.Purpose.CAPTURE))continue;
            if(!source.declaresPolicy(context.origin().orElseThrow()))continue;
            if(desired!=null) {
                var definition=desired.get(context.scope().owner().namespace());if(definition==null)continue;
                var expected=io.tapstate.adapters.pdk.ConnectorNoteOrigin.capture(definition.connector(),connectors.resolve(definition.connector()).contentHash(),definition.config());
                if(!context.origin().orElseThrow().equals(expected))continue;
            }
            {
                var observed=aliasInventory.stream().filter(n -> n.scope().equals(context.scope())).findFirst().orElseThrow(ThreeLayerStorePort::refused);
                var resource=source.slot(observed,aliasInventory);
                if(resource!=null && aliasInventory.stream().anyMatch(candidate -> candidate.scope().owner().kind()==RequiredNoteOwner.Kind.CHAIN
                        && shared.contains(candidate.scope().owner().ownerId()) && candidate.origin().equals(observed.origin())
                        && resource.equals(source.savedSlot(candidate,aliasInventory))))aliases.add(new CaptureAliasObservation(observed,resource));
            }
        }
        return List.copyOf(aliases);
    }
    @FunctionalInterface private interface Reservation {
        void reserve(List<CaptureClearObservation> observations,List<CaptureResourceNotes> related,String name,RequiredNoteOrigin origin,boolean automatic,List<CaptureRetirementMember> members);
    }
    private void schedule(String pipeline,List<SrsConsumerMembership> previous,List<CaptureAliasObservation> relinquished,Reservation reservation) {
        var chains=new HashSet<String>();previous.forEach(value -> chains.add(value.context().key().miningChainId()));
        if(chains.isEmpty() && relinquished.isEmpty())return;
        var captures=configuration.sharedCaptures(business);var handled=new HashSet<String>();
        for(var prepared:captures) {
            if(handled.contains(prepared.captureId()))continue;
            var owner=new RequiredNoteOwner(business.bindingVersion(),"pdk.chain."+prepared.canonicalChainId(),
                    RequiredNoteOwner.Kind.CHAIN,prepared.canonicalChainId(),0);
            var scope=notes.open(owner);var origin=notes.origin(scope);
            if(!source.supported(origin,scope))continue;
            source.requireOriginal(prepared,origin);
            var inventory=configuration.resourceNotes(business);
            var own=inventory.stream().filter(value -> value.scope().equals(scope)).findFirst().orElseThrow(ThreeLayerStorePort::refused);
            String slot=source.savedSlot(own,inventory);
            if(slot==null)continue;
            if(!chains.contains(prepared.chainId()) && relinquished.stream().noneMatch(alias -> alias.resourceName().equals(slot)))continue;
            var related=new ArrayList<CaptureResourceNotes>();
            for(var observed:inventory) {
                if(!source.declaresPolicy(observed.origin()))continue;
                if(slot.equals(source.slot(observed,inventory))) {
                    if(!origin.equals(observed.origin()))throw pending(null,"same-slot-context-independence-unproven");
                    related.add(observed);
                }
            }
            if(related.stream().anyMatch(CaptureResourceNotes::resumable))continue;
            var cohort=new ArrayList<CaptureClearObservation>();boolean retained=false;
            for(var root:captures) {
                if(related.stream().noneMatch(value -> value.scope().owner().kind()==RequiredNoteOwner.Kind.CHAIN
                        && value.scope().owner().ownerId().equals(root.canonicalChainId())))continue;
                var observation=cache.inspect(root);cohort.add(observation);handled.add(root.captureId());
                if(!observation.snapshot().consumers().isEmpty() || configuration.retainedPeer(root,pipeline))retained=true;
            }
            if(retained)continue;
            reservation.reserve(cohort,related,CaptureResourceNotes.nameHash(slot),origin,source.automatic(origin),roster());
            // The durable obligation is independent from private G+1 publication. Its CACHE freeze
            // and external work proceed through reconciliation rather than holding the clear command.
        }
    }

    private synchronized void maintain() {
        if(closed)return;
        try {
            active.run();
            for(var pending:configuration.pending(business)) {
                try {reconcile(pending);}
                catch(TapstateException waiting) {
                    if(waiting.code()!=IoError.CAPTURE_EXTERNAL_RETIREMENT_PENDING)
                        LOG.warn("Capture retirement {} remains pending [{}]",pending.preparation().operationId(),waiting.code().code());
                }
            }
        }catch(TapstateException unavailable) {
            // Losing access never retires evidence or turns an old source call into an ended call.
        }catch(RuntimeException bug) {
            LOG.error("Original capture retirement reconciliation failed",bug);
        }
    }
    private void reconcile(CaptureExternalRetirements.Pending pending) {
        var original=pending.preparation();String key=original.operationId();
        switch(pending.phase()) {
            case "RESERVED" -> {
                try {
                    var receipt=cache.freeze(original,node());
                    if(receipt.outcome()==CaptureClearCache.Outcome.FROZEN)configuration.frozen(original,receipt);
                }catch(TapstateException changed) {
                    if("attachment-raced".equals(changed.args().get("reason")))configuration.cancelling(original);
                    else throw changed;
                }
            }
            case "FROZEN" -> {
                retireEveryMember(original);
                var permit=configuration.beginExternal(original,node());
                if(permit.isPresent()) {
                    var attempt=permit.orElseThrow();running.put(key,new Work(original,attempt,source.begin(original,attempt)));
                }
            }
            case "STARTED","UNKNOWN" -> {
                var work=running.get(key);
                if(work==null){
                    if(pending.phase().equals("STARTED"))configuration.recoveredUnknown(original);
                    else configuration.beginExternal(original,node()).ifPresent(attempt -> running.put(key,new Work(original,attempt,source.begin(original,attempt))));
                    return;
                }
                if(!work.answer().isDone())return;
                var answer=work.answer().join();
                if(answer.absent())configuration.verified(work.original(),work.attempt());
                else work.attempt().unknown(answer.reason());
                running.remove(key);
            }
            case "VERIFIED" -> {
                var receipts=new ArrayList<CaptureClearCache.Receipt>();
                for(var participant:configuration.cohort(original))receipts.add(cache.applied(participant).orElseGet(() -> cache.apply(participant,node())));
                configuration.complete(original,receipts);
            }
            case "CANCELLING" -> configuration.cancelled(original,cache.cancel(original,node()));
            case "READY" -> {
                if(cache.reclaim(original,node())) {
                    var prefix="srs."+original.observation().previous().chainId()+".";
                    for(var object:member.getDistributedObjects())
                        if(object instanceof Ringbuffer<?> && object.getName().startsWith(prefix))object.destroy();
                    configuration.cleaned(original);
                }
            }
            default -> throw new IllegalStateException("unexpected capture retirement phase: "+pending.phase());
        }
    }
    private WorkloadClaimFence node(){active.run();return node.get().orElseThrow(ThreeLayerStorePort::refused);}
    private List<CaptureRetirementMember> roster() {
        return member.getCluster().getMembers().stream().map(value -> new CaptureRetirementMember(
                value.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE),value.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE),value.getUuid().toString()))
                .sorted(Comparator.comparing(CaptureRetirementMember::nodeId)).toList();
    }
    private void retireEveryMember(CaptureClearPreparation original) {
        if(!roster().equals(original.members()))throw pending(original,"member-roster-changed");
        for(String action:List.of("STOP","VERIFY")) {
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
            var targets=List.copyOf(member.getCluster().getMembers());
            var replies=member.getExecutorService("tapstate-native-capture-retirement").submitToMembers(
                    new Retire(original.originalScope().clusterId(),original.originalScope().bindingVersion(),original.originalScope().cacheIncarnation(),
                            original.operationId(),original.sequence(),original.requestHash(),action),targets);
            for(var target:targets) {
                try {
                    if(!Boolean.TRUE.equals(replies.get(target).get(Math.max(1,deadline-System.nanoTime()),TimeUnit.NANOSECONDS)))
                        throw pending(original,"member-physical-exit-unproven");
                }catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw pending(original,"member-exit-interrupted");}
                catch(ExecutionException|TimeoutException unavailable){throw pending(original,"member-exit-unavailable");}
            }
            if(!roster().equals(original.members()))throw pending(original,"member-roster-changed");
        }
    }
    private static final class Retire implements Callable<Boolean>,java.io.Serializable,com.hazelcast.core.HazelcastInstanceAware {
        private final String cluster,incarnation,operation,hash,action;
        private final long binding,sequence;
        private transient HazelcastInstance member;
        private Retire(String cluster,long binding,String incarnation,String operation,long sequence,String hash,String action) {
            this.cluster=cluster;this.binding=binding;this.incarnation=incarnation;this.operation=operation;
            this.sequence=sequence;this.hash=hash;this.action=action;
        }
        @Override public void setHazelcastInstance(HazelcastInstance value){member=value;}
        @Override public Boolean call() {
            if(member==null || !(member.getUserContext().get(LOCAL)instanceof NativeCaptureExternalRetirement local))return false;
            local.active.run();
            if(!local.business.equals(new CacheBusinessScope(cluster,binding,incarnation)))return false;
            var original=local.configuration.pending(local.business).stream().map(CaptureExternalRetirements.Pending::preparation)
                    .filter(p -> p.operationId().equals(operation) && p.sequence()==sequence && p.requestHash().equals(hash)).findFirst().orElseThrow(ThreeLayerStorePort::refused);
            if(!local.roster().equals(original.members()))return false;
            original.requireCurrent();
            if(local.cache.frozen(original).filter(r -> r.outcome()==CaptureClearCache.Outcome.FROZEN).isEmpty())return false;
            if(action.equals("STOP"))local.captures.stopSharedLocally(original.observation().previous(),original.observation().captureFence());
            else if(action.equals("VERIFY"))local.captures.requireSharedRetired(original.observation().previous(),original.observation().captureFence());
            else throw new IllegalStateException("unexpected capture retirement action");
            original.requireCurrent();return local.roster().equals(original.members());
        }
    }
    private static TapstateException pending(CaptureClearPreparation original,String reason) {
        return new TapstateException(IoError.CAPTURE_EXTERNAL_RETIREMENT_PENDING,Map.of("capture",original==null?"unknown":original.replacement().captureId(),
                "operation",original==null?"unknown":original.operationId(),"reason",reason),null);
    }
    @Override public synchronized void close() {
        closed=true;executor.shutdownNow();
        member.getUserContext().remove(LOCAL,this);
        for(var work:running.values()) {
            try {work.attempt().unknown("executor-closing");}catch(TapstateException unavailable){/* Preserve the original persisted STARTED. */}
        }
        running.clear();
    }
}
