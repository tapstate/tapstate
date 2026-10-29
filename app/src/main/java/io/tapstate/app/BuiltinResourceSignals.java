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
    SourceOperationObserver source(SourceBindingSnapshot original,Runnable originalOperation){
        var id=original.identity();
        if(storage.get().defaultSourceId().equals(id.sourceId())){
            var resource=new AlarmKey.Resource("cluster-storage",id.sourceId(),id.incarnation()+":"+id.bindingRevision());
            if(!evidence.containsKey(resource)&&evidence.size()>=256){runtime.get().collectionUnavailable("resource-proof-budget");return SourceOperationObserver.NONE;}
            evidence.put(resource,new AlertStore.StorageEvidence(resource,AlertStore.StorageRole.MASTER,null,id));
        }
        String digest=digest(id.sourceId(),id.incarnation(),Long.toString(id.bindingRevision()));
        return (capability,failure)->{
            try { originalOperation.run(); }
            catch(TapstateException ended){return;}
            if(failure!=null&&!connectionFailure(failure))return;
            var input=new AlertInput.Operational(clock.instant(),digest,
                    OperationalEvidence.Capability.valueOf(capability.name()),failure==null);
            runtime.get().resourceInput(new AlarmKey.Resource("source",id.sourceId(),id.incarnation()),
                    "source.connection-failed",id.bindingRevision(),digest,active,input);
            StorageBinding bound=storage.get();
            if(bound.defaultSourceId().equals(id.sourceId())){
                runtime.get().resourceInput(new AlarmKey.Resource("cluster-storage",id.sourceId(),id.incarnation()+":"+id.bindingRevision()),
                        "cluster.storage-unavailable",0,digest,active,input);
            }
        };
    }
    Optional<AlertStore.ResourceEvidence> resourceEvidence(AlarmKey.Resource resource){
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
