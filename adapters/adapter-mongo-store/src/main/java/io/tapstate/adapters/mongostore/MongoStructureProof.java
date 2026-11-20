package io.tapstate.adapters.mongostore;

import java.util.EnumMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Physical metadata only; live identity, claim and transaction fences are never cached here. */
final class MongoStructureProof {
    enum Kind { SRS, SRS_RUNTIME, SRS_CONTROL_SNAPSHOT, SRS_CONTROL_READINESS, SRS_RECLAIM_REFERENCES, CLAIMS, PIPELINE_STATE, OBSERVATION, UNPREPARED_VALIDATION }
    interface Check extends Runnable { void invalidate(); long remainingNanos(); default long generation() { return 0L; } }
    private final EnumMap<Kind,Long> verified=new EnumMap<>(Kind.class);
    private final LongSupplier nanos;
    private long generation;
    MongoStructureProof(){this(System::nanoTime);}
    MongoStructureProof(LongSupplier nanos){this.nanos=nanos;}
    Check check(Kind kind,Runnable verify){
        return new Check(){
            public void run(){require(kind,verify);}
            public void invalidate(){clear();}
            public long remainingNanos(){return remaining(kind);}
            public long generation(){return currentGeneration();}
        };
    }
    void require(Kind kind,Runnable verify){
        final long started;
        final long observedGeneration;
        synchronized(this){
            started=nanos.getAsLong();var deadline=verified.get(kind);
            if(deadline!=null&&started-deadline<0)return;
            observedGeneration=generation;
        }
        try{
            verify.run();
            synchronized(this){
                if(generation==observedGeneration)verified.put(kind,started+TimeUnit.SECONDS.toNanos(10));
            }
        }catch(RuntimeException|Error failure){clear();throw failure;}
    }
    synchronized long currentGeneration(){return generation;}
    synchronized long remaining(Kind kind){return Math.max(0L,verified.getOrDefault(kind,nanos.getAsLong())-nanos.getAsLong());}
    synchronized long remainingSrs(){return Math.max(remaining(Kind.SRS),remaining(Kind.SRS_RUNTIME));}
    synchronized void clear(){generation++;verified.clear();}
    static void invalidate(Runnable check){if(check instanceof Check owned)owned.invalidate();}
}
