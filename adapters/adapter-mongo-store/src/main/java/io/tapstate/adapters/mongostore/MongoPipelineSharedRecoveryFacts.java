package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import org.bson.Document;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** A scope-bound requirement for explicit full, never capture creation or execution authority. */
final class MongoPipelineSharedRecoveryFacts {
    record Pending(String operationId,PipelineIntentScope originalScope) {
        Pending {
            if(operationId==null || !UUID.fromString(operationId).toString().equals(operationId) || originalScope==null)
                throw new IllegalArgumentException("invalid shared recovery scope gate");
        }
    }
    private MongoPipelineSharedRecoveryFacts() {}

    static Optional<Pending> read(Document mapping) {
        if(mapping==null || !mapping.containsKey("sharedRecovery"))return Optional.empty();
        try {
            var row=mapping.get("sharedRecovery",Document.class);
            if(row==null || !row.keySet().equals(Set.of("formatVersion","operationId","originalScope"))
                    || !Integer.valueOf(1).equals(row.get("formatVersion")))throw unreadable();
            var pending=new Pending(row.getString("operationId"),MongoPipelinePreparationFacts.scope(row.get("originalScope",Document.class)));
            var original=pending.originalScope();
            if(!original.pipelineId().equals(mapping.get("_id")) || !Long.valueOf(original.stateGeneration()).equals(mapping.get("stateGeneration")))throw unreadable();
            var preparation=mapping.get("cachePreparation",Document.class);
            if(preparation==null || !"COMPLETE".equals(preparation.get("phase"))
                    || !original.equals(MongoPipelinePreparationFacts.scope(preparation.get("scope",Document.class))))throw unreadable();
            if(!encode(pending).equals(row))throw unreadable();return Optional.of(pending);
        }catch(IllegalArgumentException|NullPointerException|ClassCastException invalid){throw unreadable();}
    }

    static Document encode(Pending pending) {
        return new Document("formatVersion",1).append("operationId",pending.operationId())
                .append("originalScope",MongoPipelinePreparationFacts.scope(pending.originalScope()));
    }

    static boolean ready(ClientSession session,MongoDatabase database,Pending pending) {
        if(!session.hasActiveTransaction())throw new IllegalStateException("shared recovery readiness requires its CONFIG transaction");
        var original=pending.originalScope();var scope=new CacheBusinessScope(original.clusterId(),original.bindingVersion(),original.cacheIncarnation());
        var retained=MongoCaptureStatePreparations.currentRecoveryForOperation(session,database,scope,pending.operationId());
        return retained.phase()==MongoCapturePreparationFacts.RecoveryPhase.READY;
    }

    static void requireReady(ClientSession session,MongoDatabase database,Document mapping,PipelineIntentScope original) {
        var pending=read(mapping);
        if(pending.isEmpty())return;
        if(!pending.orElseThrow().originalScope().equals(original))throw unreadable();
        if(!ready(session,database,pending.orElseThrow()))throw required(original.pipelineId());
    }

    static void requireIntent(ClientSession session,MongoDatabase database,Document mapping,PipelineIntentScope scope,
            PipelineIntentSnapshot previous,DesiredState desired) {
        var pending=read(mapping);
        if(pending.isEmpty())return;
        if(!pending.orElseThrow().originalScope().equals(scope))throw unreadable();
        if(desired!=null && desired.targetState()==PipelineState.STOPPED && !desired.purgeState())return;
        if(desired==null || desired.purgeState())requireReady(session,database,mapping,scope);
        if(desired==null || desired.targetState()==PipelineState.STOPPED && desired.purgeState())return;
        if(desired.targetState()==PipelineState.RUNNING && desired.purgeState() && previous!=null && !previous.deleted() && previous.desired().purgeState()
                && Set.of(PipelineState.STOPPED,PipelineState.RUNNING).contains(previous.desired().targetState()))return;
        throw required(scope.pipelineId());
    }

    static void removeOnVerifiedReplacement(Document original,Document replacement,PipelineIntentScope previous) {
        var pending=read(original);
        if(pending.isEmpty())return;
        if(!pending.orElseThrow().originalScope().equals(previous))throw unreadable();
        replacement.remove("sharedRecovery");
    }

    static TapstateException required(String pipeline) {
        return new TapstateException(IoError.PIPELINE_STATE_REBUILD_REQUIRED,Map.of("pipeline",pipeline),null);
    }
    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","pipeline-shared-recovery","field","scopeGate"),null);
    }
}
