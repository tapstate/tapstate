package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/** Process-local accounting only. It neither executes work nor grants storage or runtime authority. */
public final class ValidationMetadataBudget {
    public static final int FRAMES=4;
    public static final long RESERVED_BYTES=64L*1024*1024;
    private final Semaphore slots=new Semaphore(FRAMES);
    private enum Kind { CONTROL,WORK,MAINTENANCE,RETAINED }
    private int backgroundFrames,workFrames;
    public Lease acquire(Object owner){return acquire(owner,Kind.CONTROL,1).getFirst();}
    /** Retryable discovery and history leave room for a foreground response and native probe. */
    public Lease acquireMaintenance(Object owner){return acquire(owner,Kind.MAINTENANCE,1).getFirst();}
    /** Long-lived definitions and quotas share the background allowance, preserving control headroom. */
    public Lease acquireRetained(Object owner){return acquireRetainedBundle(owner,1).getFirst();}
    /** Preparation and native probes keep a control slot even when their cleanup remains unresolved. */
    public Lease acquireWork(Object owner){return acquire(owner,Kind.WORK,1).getFirst();}
    /** All required retained frames are admitted together; no partial holder can wait for its peer. */
    public List<Lease> acquireRetainedBundle(Object owner,int frames){return acquire(owner,Kind.RETAINED,frames);}
    private synchronized List<Lease> acquire(Object owner,Kind kind,int frames){
        Objects.requireNonNull(owner,"metadata owner");
        if(frames<1||frames>FRAMES-2||frames!=1&&kind!=Kind.RETAINED)throw new IllegalArgumentException("invalid metadata frame bundle");
        boolean background=kind==Kind.MAINTENANCE||kind==Kind.RETAINED,work=kind!=Kind.CONTROL;
        if((background&&backgroundFrames>FRAMES-2-frames)||(work&&workFrames>FRAMES-1-frames)
                ||(kind==Kind.MAINTENANCE&&slots.availablePermits()<3)||!slots.tryAcquire(frames))
            throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation.metadataFrames"),null);
        if(background)backgroundFrames+=frames;if(work)workFrames+=frames;
        var leases=new ArrayList<Lease>(frames);for(int index=0;index<frames;index++)leases.add(new Lease(owner,kind));
        return List.copyOf(leases);
    }
    public int inUse(){return FRAMES-slots.availablePermits();}
    public final class Lease implements AutoCloseable {
        private final AtomicReference<Object> owner;
        private final Kind kind;
        private Lease(Object owner,Kind kind){this.owner=new AtomicReference<>(owner);this.kind=kind;}
        public void requireRetained(Object expected){
            if(expected==null||owner.get()!=expected)throw new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","resources.metadataOwner"),null);
        }
        /** The caller first relinquishes every decoded value, in-flight body and serialized response. */
        @Override public void close(){if(owner.getAndSet(null)!=null)synchronized(ValidationMetadataBudget.this){if(kind==Kind.MAINTENANCE||kind==Kind.RETAINED)backgroundFrames--;if(kind!=Kind.CONTROL)workFrames--;slots.release();}}
    }
}
