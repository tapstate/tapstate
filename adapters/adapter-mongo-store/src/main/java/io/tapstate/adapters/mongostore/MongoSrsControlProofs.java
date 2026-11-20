package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.CacheBusinessScope;
import java.util.LinkedHashMap;

/** Bounded read-only reference proofs; current identity and control rows are always read again. */
final class MongoSrsControlProofs {
    private record Key(CacheBusinessScope scope,String chain,String fingerprint) { }
    private record Proof(String fingerprint,MongoSrsLease owner,MongoSrsLease.Lease lease) { }
    private long generation;
    private final LinkedHashMap<Key,Proof> proofs=new LinkedHashMap<>(16,0.75f,true);
    synchronized boolean matches(CacheBusinessScope scope,String chain,String fingerprint){
        var key=new Key(scope,chain,fingerprint);var proof=proofs.get(key);
        if(proof==null)return false;
        if(!proof.owner().stands(proof.lease())){proofs.remove(key);return false;}
        return proof.fingerprint().equals(fingerprint);
    }
    synchronized void remember(CacheBusinessScope scope,String chain,String fingerprint,MongoSrsLease owner,MongoSrsLease.Lease lease,long observedGeneration){
        if(observedGeneration!=generation)return;
        if(lease==null||!owner.stands(lease))return;
        proofs.put(new Key(scope,chain,fingerprint),new Proof(fingerprint,owner,lease));
        if(proofs.size()>256)proofs.remove(proofs.keySet().iterator().next());
    }
    synchronized long generation(){return generation;}
    synchronized void clear(){generation++;proofs.clear();}
}
