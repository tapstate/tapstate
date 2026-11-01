package io.tapstate.adapters.pdk;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.SharedNotes;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Pinned PostgreSQL cleanup and a positive cluster-unique slot-name absence check. */
public final class PdkExternalResourceRetirement {
    public record Result(boolean absent,String reason) {}
    private final ConnectorProvisioner connectors;
    private final RequiredNoteStore notes;
    public PdkExternalResourceRetirement(ConnectorProvisioner connectors,RequiredNoteStore notes) {
        this.connectors=java.util.Objects.requireNonNull(connectors);this.notes=java.util.Objects.requireNonNull(notes);
    }
    /** Pure policy inspection; it neither initializes a connector nor replaces retained settings. */
    public boolean automatic(RequiredNoteOrigin origin) {
        var ref=connectors.resolve(origin.connectorId(),origin.artifactHash());
        var settings=ConfigTypeCoercion.coerce(origin.connectorId(),ref.spec(),ConnectorNoteOrigin.settings(origin));
        var value=settings.get("autoClearSlot");
        if(value==null)return true;
        if(value instanceof Boolean flag)return flag;
        throw refusal(null,"original-policy-unreadable");
    }
    public boolean supported(RequiredNoteOrigin origin,RequiredNoteScope scope) {
        var reference=connectors.resolve(origin.connectorId(),origin.artifactHash());
        if(reference.spec()!=null && declaresSlotPolicy(io.tapstate.core.common.JsonReader.parse(reference.spec())))return true;
        var local=notes.read(scope,"tapdata_pg_slot");
        if(local.bytes()!=null)return ConnectorStateCodec.decode(local.bytes()) instanceof String;
        for(var carry:notes.carries(scope)) {
            var value=notes.readCarried(carry,"tapdata_pg_slot");
            if(value.bytes()!=null && ConnectorStateCodec.decode(value.bytes()) instanceof String)return true;
        }
        return false;
    }
    public void requireOriginal(CapturePreparation preparation,RequiredNoteOrigin origin) {
        if(!preparation.sourceIdentityHash().equals(io.tapstate.core.model.canonical.SourceCaptureIdentity.physical(
                origin.connectorId(),ConnectorNoteOrigin.settings(origin))))throw refusal(null,"original-capture-context-mismatch");
    }
    public boolean declaresPolicy(RequiredNoteOrigin origin) {
        var reference=connectors.resolve(origin.connectorId(),origin.artifactHash());
        return reference.spec()!=null && declaresSlotPolicy(io.tapstate.core.common.JsonReader.parse(reference.spec()));
    }
    public String resourceNameHash(RequiredNoteOrigin origin,RequiredNoteScope scope) {
        if(!declaresPolicy(origin))return null;
        var own=notes.read(scope,"tapdata_pg_slot");String slot=decodedSlot(own);
        if(slot==null && own.revision()==0 && scope.owner().kind()==RequiredNoteOwner.Kind.CHAIN && !notes.carryBlocked(scope,"tapdata_pg_slot")) {
            for(var carry:notes.carries(scope)) {
                var candidate=decodedSlot(notes.readCarried(carry,"tapdata_pg_slot"));if(candidate==null)continue;
                if(slot!=null && !slot.equals(candidate))throw refusal(null,"carry-slot-disagreement");
                slot=candidate;
            }
        }
        if(slot==null && ConnectorNoteOrigin.settings(origin).get("customSlotName")instanceof String custom && !custom.isBlank())slot=custom;
        return CaptureResourceNotes.nameHash(slot);
    }
    private static String decodedSlot(RequiredNoteValue value) {
        if(value.bytes()==null)return null;
        if(ConnectorStateCodec.decode(value.bytes())instanceof String slot && !slot.isBlank())return slot;
        throw refusal(null,"slot-note-unreadable");
    }
    /** Note bytes remain connector-owned. Metadata stores never guess their serialized values. */
    public String slot(CaptureResourceNotes observed,List<CaptureResourceNotes> all) {
        String saved=savedSlot(observed,all);if(saved!=null)return saved;
        var custom=ConnectorNoteOrigin.settings(observed.origin()).get("customSlotName");
        return custom instanceof String value && !value.isBlank()?value:null;
    }
    public String savedSlot(CaptureResourceNotes observed,List<CaptureResourceNotes> all) {
        String own=ownSlot(observed);
        if(own!=null)return own;
        String carried=null;
        for(var token:observed.mayCarrySlot()?observed.carries():List.<RequiredNoteScope>of()) {
            var source=all.stream().filter(value -> samePhysical(value.scope(),token)).findFirst()
                    .orElseThrow(() -> refusal(null,"carry-source-not-observed"));
            String value=ownSlot(source);if(value==null)continue;
            if(carried!=null && !carried.equals(value))throw refusal(null,"carry-slot-disagreement");
            carried=value;
        }
        return carried;
    }
    public String ownSlot(CaptureResourceNotes observed) {
        return decodedSlot(observed.slot());
    }
    private static boolean samePhysical(RequiredNoteScope first,RequiredNoteScope second) {
        return first.owner().bindingVersion()==second.owner().bindingVersion() && first.owner().namespace().equals(second.owner().namespace())
                && first.generation()==second.generation();
    }
    private static boolean declaresSlotPolicy(Object value) {
        if(value instanceof Map<?,?> map) {
            var field=map.get("autoClearSlot");
            if(field instanceof Map<?,?> property && "boolean".equals(property.get("type")))return true;
            return map.values().stream().anyMatch(PdkExternalResourceRetirement::declaresSlotPolicy);
        }
        return value instanceof List<?> list && list.stream().anyMatch(PdkExternalResourceRetirement::declaresSlotPolicy);
    }

    public CompletableFuture<Result> begin(CaptureClearPreparation original,CaptureExternalRetirements.Attempt permit) {
        permit.consume();var result=new CompletableFuture<Result>();
        var worker=new Thread(() -> {
            try {result.complete(retire(original,permit));}
            catch(VirtualMachineError fatal){result.completeExceptionally(fatal);throw fatal;}
            catch(TapstateException refused){
                String reason=String.valueOf(refused.args().getOrDefault("reason","source-retirement-refused"));
                if("source-stop-unproven".equals(reason))permit.unknown(reason);else permit.ended();
                result.complete(new Result(false,reason));
            }
            catch(Throwable unexpected){result.completeExceptionally(unexpected);}
        },"tapstate-original-slot-retirement");
        worker.setDaemon(true);worker.start();return result;
    }
    private Result retire(CaptureClearPreparation original,CaptureExternalRetirements.Attempt permit) throws Throwable {
        var origin=notes.origin(original.notes());var ref=connectors.resolve(origin.connectorId(),origin.artifactHash());
        requireOriginal(original.observation().previous(),origin);
        var settings=ConnectorNoteOrigin.settings(origin);
        var node=new PipelineNode(original.originalScope().pipelineId(),"original-source");
        var shared=new SharedNotes(original.observation().previous().canonicalChainId(),List.of());
        var config=new CaptureConfig(origin.connectorId(),settings,List.of(),node,shared);
        var scopes=scopes(original);
        String slot;boolean alreadyAbsent;
        try(var reader=reader(origin,ref,config,scopes);var stopping=stop(reader)) {
            var name=reader.context().getStateMap().get("tapdata_pg_slot");
            if(name==null)name=settings.get("customSlotName");
            if(!(name instanceof String saved) || saved.isBlank())throw refusal(original,"slot-name-unproven");
            slot=saved;
            if(reader.functions().getReleaseExternalFunction()==null)throw refusal(original,"release-function-missing");
            long present=count(reader,slot);
            alreadyAbsent=present==0;
            if(present>1)throw refusal(original,"slot-identity-not-unique");
            if(present==1) {
                boolean physical="physical".equals(settings.get("logPluginName"));
                String expectedType=physical?"physical":"logical";
                var database=settings.get("database");
                if(!physical && !(database instanceof String))throw refusal(original,"original-database-unproven");
                String metadata=" AND slot_type="+literal(expectedType)+(physical?" AND database IS NULL":" AND database="+literal((String)database));
                if(rawCount(reader,query(slot)+metadata)!=1)throw refusal(original,"slot-metadata-mismatch");
            }
        }
        if(alreadyAbsent){permit.ended();return new Result(true,"confirmed-absent-before-call");}
        var port=PdkCapturePort.withRequiredNotes(connectors,null,scopes);
        var released=port.beginObservedRelease(config);
        released.facade().thenAccept(answer -> {if(answer.isPresent() && !released.terminal().isDone())permit.unknown("release-still-in-flight");});
        // A facade timeout leaves the original worker alive. Only its actual completion can proceed.
        if(!released.terminal().get()) {permit.unknown("release-or-cleanup-unproven");return new Result(false,"release-or-cleanup-unproven");}
        try(var reader=reader(origin,ref,config,scopes);var stopping=stop(reader)) {
            long count=count(reader,slot);
            if(count!=0) {permit.ended();return new Result(false,"original-slot-remains");}
        }
        permit.ended();return new Result(true,"confirmed-absent");
    }

    private ConnectorNoteScopes scopes(CaptureClearPreparation original) {
        var global=notes.open(new RequiredNoteOwner(original.notes().owner().bindingVersion(),"pdk.global-state",
                RequiredNoteOwner.Kind.GLOBAL,original.originalScope().clusterId(),0));
        return ConnectorNoteScopes.of(notes,namespace -> {
            if(namespace.equals(original.notes().owner().namespace()))return original.notes();
            if(namespace.equals(global.owner().namespace()))return global;
            throw refusal(original,"note-namespace");
        },() -> {});
    }
    private static PdkConnector reader(RequiredNoteOrigin origin,ConnectorRef ref,CaptureConfig config,ConnectorNoteScopes scopes) throws Throwable {
        var connector=PdkConnector.open(origin.connectorId(),ref,config.settings(),config.node(),null,config.sharedNotes(),scopes);
        try {connector.underLoader(() -> {connector.connector().init(connector.context());return null;});return connector;}
        catch(Throwable failure){try{connector.stopForCleanup();}finally{connector.close();}throw failure;}
    }
    private static AutoCloseable stop(PdkConnector connector) {
        return () -> {
            try {connector.stopForCleanup();}
            catch(Error bug){throw bug;}
            catch(Throwable failure){throw refusal(null,"source-stop-unproven");}
        };
    }
    private static long count(PdkConnector reader,String slot) throws Throwable {return rawCount(reader,query(slot));}
    static String query(String slot) {
        return "SELECT COUNT(*) AS slot_count FROM pg_catalog.pg_replication_slots WHERE slot_name="+literal(slot);
    }
    private static String literal(String value) {return "'"+value.replace("'","''")+"'";}
    private static long rawCount(PdkConnector reader,String query) throws Throwable {
        var function=reader.functions().getRunRawCommandFunction();
        if(function==null)throw refusal(null,"verification-function-missing");
        var table=new TapTable("slot_verification").add(new TapField("slot_count","bigint"));reader.fillFieldTypes(table);
        var answer=new CompletableFuture<Long>();
        var worker=new Thread(() -> {
            try {
                var events=new ArrayList<TapEvent>();var callbacks=new int[1];
                reader.underLoader(() -> {function.run(reader.context(),query,table,2,rows -> {
                    callbacks[0]++;if(rows==null)throw refusal(null,"verification-empty");events.addAll(rows);
                });return null;});
                answer.complete(scalar(callbacks[0],events));
            }catch(Throwable failure){answer.completeExceptionally(failure);}
        },"tapstate-slot-verification");
        worker.setDaemon(true);worker.start();
        try {return answer.get(5,TimeUnit.SECONDS);}
        catch(TimeoutException slow) {
            // Retain this worker and source handle until the real query ends; it is not an absence.
            try {answer.get();}catch(ExecutionException ignored){/* The original result remains unknown. */}
            throw refusal(null,"verification-deadline");
        }catch(ExecutionException failed){throw refusal(null,"verification-query-refused");}
    }
    static long scalar(int callbacks,List<TapEvent> events) {
        if(callbacks!=1 || events.size()!=1 || !(events.getFirst()instanceof TapInsertRecordEvent row)
                || row.getAfter()==null || !(row.getAfter().get("slot_count")instanceof Long value) || value<0)
            throw refusal(null,"verification-scalar-unproven");
        return value;
    }
    private static TapstateException refusal(CaptureClearPreparation original,String reason) {
        return new TapstateException(IoError.CAPTURE_EXTERNAL_RETIREMENT_PENDING,Map.of(
                "capture",original==null?"original":original.observation().previous().captureId(),
                "operation",original==null?"verification":original.operationId(),"reason",reason),null);
    }
}
