package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.capture.SourceOperationObserver;
import io.tapstate.spi.store.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.function.*;

/** Bridges original operation results to the bounded host. It owns no store, probe or sender. */
final class BuiltinResourceSignals {
    private final Supplier<PipelineAlertRuntime> runtime;
    private final Supplier<StorageBinding> storage;
    private final CacheAuthScope cache;
    private final Runnable active;
    private final Clock clock;
    private final Map<AlarmKey.Resource,AlertStore.ResourceEvidence> evidence = new java.util.concurrent.ConcurrentHashMap<>();
    BuiltinResourceSignals(Supplier<PipelineAlertRuntime> runtime,Supplier<StorageBinding> storage,
            CacheAuthScope cache,Runnable active,Clock clock){
        this.runtime=runtime;this.storage=storage;this.cache=cache;this.active=active;this.clock=clock;
    }
    private static final class Connection {
        final String id=UUID.randomUUID().toString();boolean closed;int pending;
    }
    private final Map<String,Connection> connections=new java.util.concurrent.ConcurrentHashMap<>();
    SourceOperationObserver source(SourceBindingSnapshot original,Runnable operation){return source(original,"legacy-source",operation,active);}
    SourceOperationObserver source(SourceBindingSnapshot original,String logical,Runnable operation,Runnable localProof) {
        var id=original.identity();
        boolean master=storage.get().defaultSourceId().equals(id.sourceId());
        var sourceResource=new AlarmKey.Resource("source",id.sourceId(),id.incarnation());
        var storageResource=new AlarmKey.Resource("cluster-storage",id.sourceId(),id.incarnation()+":"+id.bindingRevision());
        if(master) {
            if(!evidence.containsKey(storageResource) && evidence.size()>=256){runtime.get().collectionUnavailable("resource-proof-budget");return SourceOperationObserver.NONE;}
            evidence.put(storageResource,new AlertStore.StorageEvidence(storageResource,AlertStore.StorageRole.MASTER,null,id));
        }
        String base=digest(id.sourceId(),id.incarnation(),Long.toString(id.bindingRevision()),logical);
        var connection=new Connection();
        synchronized(connections) {
            if(!connections.containsKey(base) && connections.size()>=8192){runtime.get().collectionUnavailable("connection-proof-budget");return SourceOperationObserver.NONE;}
            connections.put(base,connection);
        }
        Runnable local=()->{
            active.run();localProof.run();
            if(connections.get(base)!=connection)throw ThreeLayerStorePort.refused();
        };
        Runnable settled=()->{synchronized(connection){if(--connection.pending<0)throw new IllegalStateException("negative pending operation count");
            if(connection.closed && connection.pending==0)connections.remove(base,connection);}};
        return new SourceOperationObserver() {
            @Override public void completed(Capability capability,Throwable failure){completed("connection",capability,failure);}
            @Override public void completed(String access,Capability capability,Throwable failure) {
                if(failure!=null)org.slf4j.LoggerFactory.getLogger(BuiltinResourceSignals.class).debug("Operational failure classification [master={},capability={},classified={}]",master,capability,connectionFailure(failure));
                if(failure!=null && !connectionFailure(failure))return;
                synchronized(connection){
                    if(connection.closed || connections.get(base)!=connection)return;
                    connection.pending+=master?2:1;
                }
                String domain=digest(base,access);
                String binding=digest(id.sourceId(),id.incarnation(),Long.toString(id.bindingRevision()),domain);
                var input=new AlertInput.Operational(clock.instant(),binding,OperationalEvidence.Capability.valueOf(capability.name()),failure==null,domain);
                runtime.get().resourceInput(sourceResource,"source.connection-failed",id.bindingRevision(),binding,operation,local,input,settled);
                if(master)runtime.get().resourceInput(storageResource,"cluster.storage-unavailable",0,binding,operation,local,input,settled);
            }
            @Override public SourceOperationObserver fork(String phase){close();return BuiltinResourceSignals.this.source(original,logical+":"+phase,operation,localProof);}
            @Override public void close(){synchronized(connection){connection.closed=true;if(connection.pending==0)connections.remove(base,connection);}}
        };
    }
    private volatile Function<AlarmKey.Resource,Optional<AlertStore.NodeEvidence>> nodeProofs=resource->Optional.empty();
    void nodeProofs(Function<AlarmKey.Resource,Optional<AlertStore.NodeEvidence>> proofs){nodeProofs=Objects.requireNonNull(proofs);}
    Optional<AlertStore.ResourceEvidence> resourceEvidence(AlarmKey.Resource resource){
        if(resource.kind().equals("cluster-node"))return nodeProofs.apply(resource).map(value->value);
        if(resource.kind().equals("cluster-storage")&&resource.id().equals("cache")){
            var proof=new AlertStore.StorageEvidence(new AlarmKey.Resource("cluster-storage","cache",cache.cacheIncarnation()+":"+cache.bindingVersion()),AlertStore.StorageRole.CACHE,cache,null);
            return proof.resource().equals(resource)?Optional.of(proof):Optional.empty();
        }
        return Optional.ofNullable(evidence.get(resource));
    }
    void nodeEvidence(AlertStore.NodeEvidence value){
        if(!evidence.containsKey(value.resource())&&evidence.size()>=256){runtime.get().collectionUnavailable("resource-proof-budget");return;}
        evidence.put(value.resource(),value);
    }
    void cacheOperation(boolean write,Throwable failure){
        if(failure!=null&&(!(failure instanceof TapstateException e)||e.code()!=IoError.STORE_UNAVAILABLE&&e.code()!=IoError.STORE_UNAUTHORIZED))return;
        String binding=digest(cache.clusterId(),cache.cacheIncarnation(),Long.toString(cache.bindingVersion()),Long.toString(cache.authGeneration()));
        runtime.get().resourceInput(new AlarmKey.Resource("cluster-storage","cache",cache.cacheIncarnation()+":"+cache.bindingVersion()),
                "cluster.storage-unavailable",0,binding,active,new AlertInput.Operational(clock.instant(),binding,
                        write?OperationalEvidence.Capability.CACHE_WRITE:OperationalEvidence.Capability.CACHE_READ,failure==null));
    }
    static boolean connectionFailure(Throwable failure){
        var seen=Collections.newSetFromMap(new IdentityHashMap<Throwable,Boolean>());
        for(Throwable cause=failure;cause!=null&&seen.size()<16&&seen.add(cause);cause=cause.getCause()){
            if(cause instanceof TapstateException coded && (coded.code() instanceof IoError
                    || coded.code() instanceof io.tapstate.runtime.engine.EngineError || coded.code() instanceof ActuationError))return false;
            if(cause instanceof java.sql.SQLException sql){String state=sql.getSQLState();
                if(state!=null&&(state.startsWith("08")||state.startsWith("28")||state.equals("42501")))return true;
            }
            if(cause instanceof java.net.ConnectException||cause instanceof java.net.NoRouteToHostException
                    ||cause instanceof java.net.UnknownHostException||cause instanceof java.net.SocketTimeoutException)return true;
            for(Class<?> type=cause.getClass();type!=null;type=type.getSuperclass()){
                if(Set.of("com.mongodb.MongoSocketException","com.mongodb.MongoTimeoutException").contains(type.getName()))return true;
                if(type.getName().equals("com.mongodb.MongoException")){
                    try {Object code=type.getMethod("getCode").invoke(cause);if(code instanceof Number n&&Set.of(13,18,8000).contains(n.intValue()))return true;}
                    catch(ReflectiveOperationException unavailable){return false;}
                }
            }
        }
        return false;
    }
    static String digest(String...parts){
        try {var hash=java.security.MessageDigest.getInstance("SHA-256");for(String part:parts){byte[] bytes=part.getBytes(StandardCharsets.UTF_8);hash.update(ByteBuffer.allocate(4).putInt(bytes.length).array());hash.update(bytes);}return HexFormat.of().formatHex(hash.digest());}
        catch(java.security.NoSuchAlgorithmException impossible){throw new IllegalStateException(impossible);}
    }
}
