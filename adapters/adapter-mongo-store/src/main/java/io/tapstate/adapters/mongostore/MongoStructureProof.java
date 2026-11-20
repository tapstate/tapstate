package io.tapstate.adapters.mongostore;

import java.util.EnumMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/** Physical metadata only; live identity, claim and transaction fences are never cached here. */
final class MongoStructureProof {
    enum Kind { SRS, SRS_RUNTIME, SRS_CONTROL_SNAPSHOT, SRS_CONTROL_READINESS, SRS_RECLAIM_REFERENCES, CLAIMS, PIPELINE_STATE, CHECKPOINT_READ, OBSERVATION, UNPREPARED_VALIDATION }
    interface Check extends Runnable { void invalidate(); long remainingNanos(); default long generation() { return 0L; } }
    record Verdict(boolean eligible, long deadlineNanos) {}
    private static final long VALID_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final EnumMap<Kind,Verdict> verified = new EnumMap<>(Kind.class);
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
    void require(Kind kind, Runnable verify) {
        audit(kind, () -> {
            verify.run();
            return true;
        });
    }

    boolean permits(Kind kind, BooleanSupplier verify) {
        return stands(kind, audit(kind, verify));
    }

    Verdict audit(Kind kind, BooleanSupplier verify) {
        final long started;
        final long observedGeneration;
        synchronized (this) {
            started = nanos.getAsLong();
            var retained = verified.get(kind);
            if (live(retained, started)) return retained;
            observedGeneration = generation;
        }
        try {
            boolean eligible = verify.getAsBoolean();
            synchronized (this) {
                long now = nanos.getAsLong();
                if (generation != observedGeneration) return null;
                var retained = verified.get(kind);
                if (live(retained, now)) return retained;
                if (now - started >= VALID_NANOS) return null;
                var verdict = new Verdict(eligible, started + VALID_NANOS);
                verified.put(kind, verdict);
                return verdict;
            }
        } catch (RuntimeException | Error failure) {
            clear();
            throw failure;
        }
    }

    synchronized boolean stands(Kind kind, Verdict verdict) {
        return verdict != null && verdict.eligible() && verified.get(kind) == verdict && live(verdict, nanos.getAsLong());
    }

    private static boolean live(Verdict verdict, long now) {
        return verdict != null && now - verdict.deadlineNanos() < 0;
    }

    synchronized long currentGeneration(){return generation;}
    synchronized long remaining(Kind kind) {
        var verdict = verified.get(kind);
        return verdict == null ? 0L : Math.max(0L, verdict.deadlineNanos() - nanos.getAsLong());
    }
    synchronized long remainingSrs(){return Math.max(remaining(Kind.SRS),remaining(Kind.SRS_RUNTIME));}
    synchronized void clear(){generation++;verified.clear();}
    static void invalidate(Runnable check){if(check instanceof Check owned)owned.invalidate();}
}
