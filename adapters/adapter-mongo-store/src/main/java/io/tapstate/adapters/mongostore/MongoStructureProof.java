package io.tapstate.adapters.mongostore;

import java.util.EnumMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Physical metadata only; live identity, claim and transaction fences are never cached here. */
final class MongoStructureProof {
    enum Kind { SRS, SRS_RUNTIME, CLAIMS, PIPELINE_STATE, CHECKPOINT_READ, OBSERVATION, UNPREPARED_VALIDATION }
    interface Check extends Runnable { void invalidate(); long remainingNanos(); }
    private final EnumMap<Kind,Long> verified=new EnumMap<>(Kind.class);
    private final LongSupplier nanos;
    MongoStructureProof(){this(System::nanoTime);}
    MongoStructureProof(LongSupplier nanos){this.nanos=nanos;}
    Check check(Kind kind,Runnable verify){
        return new Check(){
            public void run(){require(kind,verify);}
            public void invalidate(){clear();}
            public long remainingNanos(){return remaining(kind);}
        };
    }
    synchronized void require(Kind kind,Runnable verify){
        long started=nanos.getAsLong();var deadline=verified.get(kind);
        if(deadline!=null&&started-deadline<0)return;
        try{
            verify.run();
            verified.put(kind,started+TimeUnit.SECONDS.toNanos(10));
        }catch(RuntimeException|Error failure){verified.clear();throw failure;}
    }
    synchronized long remaining(Kind kind){return Math.max(0L,verified.getOrDefault(kind,nanos.getAsLong())-nanos.getAsLong());}
    synchronized long remainingSrs(){return Math.max(remaining(Kind.SRS),remaining(Kind.SRS_RUNTIME));}
    synchronized void clear(){verified.clear();}
    static void invalidate(Runnable check){if(check instanceof Check owned)owned.invalidate();}
}
