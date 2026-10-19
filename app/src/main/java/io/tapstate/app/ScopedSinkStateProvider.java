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
    PdkSinkPort open(HazelcastInstance member,ConfiguredSinkRun run,SinkConfig config,ConnectorProvisioner provisioner,SharedSinkConnectors sharing) {
        var authorization=ExecutionAuthorization.of(member);var original=authorization.require(run.original());
        Runnable active=() -> {if(!original.equals(authorization.require(run.original())))throw fenced();};
        active.run();if(!run.node().equals(config.node()))throw fenced();
        var fixed=Map.of(run.primary().namespace(),run.primary().token(),run.global().namespace(),run.global().token());
        var scopes=ConnectorNoteScopes.of(notes,namespace -> {var token=fixed.get(namespace);if(token==null)throw fenced();return token;},active);
        ConnectorProvisioner exact=id -> {active.run();if(!config.connectorId().equals(id))throw fenced();var ref=provisioner.resolve(id,run.artifactHash());active.run();return ref;};
        var receipts=new PdkSinkPreparations(cache.sinkPreparations(run.original().scope(),original,active),run.original().scope(),run.node(),run.purpose(),config.onFullLoad(),config.fullLoad());
        return PdkSinkPort.withScopedState(exact,sharing,run.purpose(),scopes,original,receipts);
    }
    private static TapstateException fenced(){return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);}
}
