package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.function.*;

/** One member's volatile exclusion, bound to the original durable shared recovery attempt. */
final class SharedSrsPreviewMember implements AutoCloseable {
    private final StorageRecoveryCommands commands;
    private final CacheAuthScope auth;
    private final PipelineActuationOwnership ownership;
    private final WorkloadClaimStore claims;
    private final Supplier<WorkloadOwner> localOwner;
    private final LongSupplier revision;
    private final Runnable active;
    private final BiConsumer<PipelineIntentScope,WorkloadClaimFence> isolated;
    private final Map<PipelineIntentScope,PipelineActuationOwnership.Control> controls=new LinkedHashMap<>();
    private StorageRecoveryCommands.Plan held;
    private long highestRevision=-1;
    private boolean closed;

    SharedSrsPreviewMember(StorageRecoveryCommands commands,CacheAuthScope auth,PipelineActuationOwnership ownership,
            WorkloadClaimStore claims,Supplier<WorkloadOwner> localOwner,LongSupplier revision,Runnable active,
            BiConsumer<PipelineIntentScope,WorkloadClaimFence> isolated) {
        this.commands=Objects.requireNonNull(commands).guardedBy(active);this.auth=Objects.requireNonNull(auth);
        this.ownership=Objects.requireNonNull(ownership);this.claims=Objects.requireNonNull(claims);
        this.localOwner=Objects.requireNonNull(localOwner);this.revision=Objects.requireNonNull(revision);
        this.active=Objects.requireNonNull(active);this.isolated=Objects.requireNonNull(isolated);
    }

    synchronized StorageRecoveryCommands.Plan original(CacheAuthScope scope,String operation,String hash,long controlRevision) {
        requireActive();if(!auth.equals(scope))throw fenced();
        var plan=commands.find(auth,operation).orElseThrow(SharedSrsPreviewMember::fenced);
        if(!plan.planHash().equals(hash) || plan.controlRevision()!=controlRevision)throw fenced();
        return requireCurrent(plan);
    }

    synchronized void freeze(StorageRecoveryCommands.Plan requested) {
        var current=requireCurrent(requested);
        if(current.controlRevision()<highestRevision)throw fenced();
        highestRevision=current.controlRevision();reconcile(current);held=current;
        for(var pipeline:current.manifest().sharedSrs().pipelines().values()) {
            var existing=controls.get(pipeline.scope());
            if(existing==null)controls.put(pipeline.scope(),ownership.freezeControl(pipeline.scope(),pipeline.claim()));
            else if(!pipeline.claim().equals(existing.claim()))throw fenced();
        }
        // A delayed read cannot acknowledge an installation after another CONFIG mutation.
        requireCurrent(current);requireHeld(current);
    }

    synchronized void endOwnedTenures(StorageRecoveryCommands.Plan requested) {
        requireCurrent(requested);requireHeld(requested);var owner=localOwner.get();
        for(var pipeline:requested.manifest().sharedSrs().pipelines().values()) {
            isolated.accept(pipeline.scope(),pipeline.claim());
            if(pipeline.claim().owner().equals(owner))controls.get(pipeline.scope()).endTenure();
        }
        requireCurrent(requested);
    }

    synchronized void requireEnded(StorageRecoveryCommands.Plan requested) {
        requireCurrent(requested);requireHeld(requested);
        for(var pipeline:requested.manifest().sharedSrs().pipelines().values()) {
            isolated.accept(pipeline.scope(),pipeline.claim());
            var observed=claims.readPipeline(pipeline.scope()).orElseThrow(SharedSrsPreviewMember::fenced);
            if(observed.leased() || !pipeline.claim().equals(WorkloadClaimFence.from(observed.claim())))throw fenced();
        }
        requireCurrent(requested);
    }

    /** Local checks only: safe for a CONFIG transaction guard, without CACHE, RPC or connector IO. */
    synchronized void requireHeld(StorageRecoveryCommands.Plan requested) {
        requireActive();
        if(held==null || !valid(requested) || !sameAttempt(held,requested))throw fenced();
        for(var pipeline:requested.manifest().sharedSrs().pipelines().values()) {
            var control=controls.get(pipeline.scope());
            if(control==null || !control.claim().equals(pipeline.claim()))throw fenced();
            control.requireHeld();
        }
    }

    @org.springframework.scheduling.annotation.Scheduled(fixedDelay=1000)
    synchronized void maintain() {
        if(closed || held==null)return;
        try {
            active.run();var current=commands.current(auth);
            if(current.isEmpty() || !valid(current.orElseThrow()) || !sameRecovery(held,current.orElseThrow())) {clear();return;}
            var selected=current.orElseThrow();
            if(selected.phase()!=StorageRecoveryCommands.Phase.RUNNING
                    && revision.getAsLong()!=selected.controlRevision()+selected.phase().ordinal()) {clear();return;}
            highestRevision=Math.max(highestRevision,selected.controlRevision());reconcile(selected);
        } catch(TapstateException unavailable) {
            // Unknown CONFIG evidence cannot release the original exclusion or authorize confirmation.
        }
    }

    private StorageRecoveryCommands.Plan requireCurrent(StorageRecoveryCommands.Plan requested) {
        requireActive();
        if(!valid(requested))throw fenced();
        var current=commands.find(auth,requested.operationId()).orElseThrow(SharedSrsPreviewMember::fenced);
        if(!valid(current) || !sameAttempt(current,requested)
                || current.phase()!=StorageRecoveryCommands.Phase.RUNNING
                    && revision.getAsLong()!=current.controlRevision()+current.phase().ordinal())throw fenced();
        return current;
    }
    private boolean valid(StorageRecoveryCommands.Plan plan) {
        return plan!=null && auth.equals(plan.originalScope()) && plan.manifest().selection().extent()==StorageRecoveryCommands.Extent.SHARED_SRS
                && plan.phase()!=StorageRecoveryCommands.Phase.COMPLETE;
    }
    private static boolean sameRecovery(StorageRecoveryCommands.Plan left,StorageRecoveryCommands.Plan right) {
        return left.operationId().equals(right.operationId()) && left.originalScope().equals(right.originalScope())
                && left.manifest().selection().equals(right.manifest().selection())
                && left.manifest().sharedSrs().previous().equals(right.manifest().sharedSrs().previous());
    }
    private static boolean sameAttempt(StorageRecoveryCommands.Plan left,StorageRecoveryCommands.Plan right) {
        return sameRecovery(left,right) && Objects.equals(left.previousOperationId(),right.previousOperationId())
                && left.controlRevision()==right.controlRevision() && left.planHash().equals(right.planHash()) && left.manifest().equals(right.manifest());
    }
    private void reconcile(StorageRecoveryCommands.Plan current) {
        if(held!=null && !sameRecovery(held,current)){clear();return;}
        var expected=current.manifest().sharedSrs().pipelines();
        var iterator=controls.entrySet().iterator();
        while(iterator.hasNext()) {
            var entry=iterator.next();var pipeline=expected.get(entry.getKey().pipelineId());
            if(pipeline==null || !entry.getKey().equals(pipeline.scope()) || !entry.getValue().claim().equals(pipeline.claim())) {
                entry.getValue().close();iterator.remove();
            }
        }
    }
    private void requireActive(){active.run();if(closed)throw fenced();}
    private void clear(){controls.values().forEach(PipelineActuationOwnership.Control::close);controls.clear();held=null;}
    @Override public synchronized void close(){closed=true;clear();}
    private static TapstateException fenced(){return ThreeLayerStorePort.refused();}
}
