package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.pdk.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.store.*;
import java.util.Map;
import java.util.Objects;

/** Member-local CONFIG notes and CACHE receipts, resolved only for the coordinates shipped by the original run. */
final class ScopedSinkStateProvider {
    private static final String KEY="tapstate.sink.configured-state";
    private final PipelineStoreViews cache;
    private final RequiredNoteStore notes;
    ScopedSinkStateProvider(PipelineStoreViews cache,RequiredNoteStore notes){this.cache=Objects.requireNonNull(cache,"cache");this.notes=Objects.requireNonNull(notes,"notes");}
    static void bind(Config config,PipelineStoreViews cache,RequiredNoteStore notes){if(config.getUserContext().putIfAbsent(KEY,new ScopedSinkStateProvider(cache,notes))!=null)throw fenced();}
    static ScopedSinkStateProvider of(HazelcastInstance member){if(member.getUserContext().get(KEY)instanceof ScopedSinkStateProvider provider)return provider;throw fenced();}
    @FunctionalInterface interface Operations {io.tapstate.spi.capture.SourceOperationObserver open(ConfiguredSinkRun run,SinkConfig config,String slot,Runnable guard,Runnable local);}
    private volatile Operations operations=(run,config,slot,guard,local)->io.tapstate.spi.capture.SourceOperationObserver.NONE;
    void observeOperations(Operations observer){operations=Objects.requireNonNull(observer);}
    PdkSinkPort open(HazelcastInstance member,ConfiguredSinkRun run,SinkConfig config,ConnectorProvisioner provisioner,SharedSinkConnectors sharing) {
        return open(member,run,config,provisioner,sharing,"writer:0:0");
    }
    PdkSinkPort open(HazelcastInstance member,ConfiguredSinkRun run,SinkConfig config,ConnectorProvisioner provisioner,SharedSinkConnectors sharing,String slot) {
        var authorization=ExecutionAuthorization.of(member);var original=authorization.require(run.original());
        Runnable active=() -> {if(!original.equals(authorization.require(run.original())))throw fenced();};
        active.run();if(!run.node().equals(config.node()))throw fenced();
        var fixed=Map.of(run.primary().namespace(),run.primary().token(),run.global().namespace(),run.global().token());
        var scopes=ConnectorNoteScopes.of(notes,namespace -> {var token=fixed.get(namespace);if(token==null)throw fenced();return token;},active);
        ConnectorProvisioner exact=id -> {active.run();if(!config.connectorId().equals(id))throw fenced();var ref=provisioner.resolve(id,run.artifactHash());active.run();return ref;};
        var receipts=new PdkSinkPreparations(cache.sinkPreparations(run.original().scope(),original,active),run.original().scope(),run.node(),run.purpose(),config.onFullLoad(),config.fullLoad());
        Runnable local=()->{if(!original.equals(authorization.requireLocal(run.original())))throw fenced();};
        return PdkSinkPort.withScopedState(exact,sharing,run.purpose(),scopes,original,receipts)
                .withOperationObserver(operations.open(run,config,slot,active,local))
                .withSharedInitialization(()->operations.open(run,config,"shared-init:"+(slot.startsWith("writer:")?slot.substring(7,slot.lastIndexOf(':')):slot),active,local));
    }
    io.tapstate.spi.capture.SourceOperationObserver observeOriginal(ConfiguredSinkRun run,SinkConfig config,String slot,Runnable guard,Runnable local) {
        return operations.open(run,config,slot,guard,local);
    }
    static ScopedSinkStateProvider local() {
        var members=com.hazelcast.core.Hazelcast.getAllHazelcastInstances();
        if(members.size()!=1)throw fenced();return of(members.iterator().next());
    }
    private static TapstateException fenced(){return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);}
}
