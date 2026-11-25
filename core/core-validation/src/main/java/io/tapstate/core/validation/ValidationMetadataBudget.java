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
    private int retainedFrames,workFrames,maintenanceFrames,pendingBundleFrames;
    private long wakeGeneration;
    public Lease acquire(Object owner){return acquire(owner,Kind.CONTROL,1).getFirst();}
    /** Retryable discovery and history leave room for a foreground response and native probe. */
    public Lease acquireMaintenance(Object owner){return acquire(owner,Kind.MAINTENANCE,1).getFirst();}
    /** Long-lived definitions and shared quotas leave at least one foreground control slot. */
    public Lease acquireRetained(Object owner){return acquireRetainedBundle(owner,1).getFirst();}
    /** Preparation and native probes keep a control slot even when their cleanup remains unresolved. */
    public Lease acquireWork(Object owner){return acquire(owner,Kind.WORK,1).getFirst();}
    /** All required retained frames are admitted together; no partial holder can wait for its peer. */
    public List<Lease> acquireRetainedBundle(Object owner,int frames){return acquire(owner,Kind.RETAINED,frames);}
    /** Only transient response/discovery holders may defer a first bundle; native work still refuses immediately. */
    public List<Lease> acquireRetainedBundleAfterControls(Object owner,int frames,long timeoutNanos,Runnable active)throws InterruptedException{
        Objects.requireNonNull(active,"active metadata context");
        if(timeoutNanos<1||timeoutNanos>java.util.concurrent.TimeUnit.SECONDS.toNanos(30))throw new IllegalArgumentException("invalid metadata admission deadline");
        long deadline=System.nanoTime()+timeoutNanos;boolean pending=false;
        try{
            for(;;){
                long observed;synchronized(this){observed=wakeGeneration;}
                active.run();
                synchronized(this){
                    if(System.nanoTime()-deadline>=0)throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation.metadataFrames"),null);
                    if(observed!=wakeGeneration)continue;
                    try{return acquire(owner,Kind.RETAINED,frames,pending);}
                    catch(TapstateException refused){
                        if(!transientControlsOnly(refused,pending))throw refused;
                        long remaining=deadline-System.nanoTime();if(remaining<=0)throw refused;
                        pendingBundleFrames=frames;pending=true;
                        java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(this,remaining);
                    }
                }
            }
        }finally{if(pending)synchronized(this){pendingBundleFrames=0;notifyAll();}}
    }
    private synchronized boolean transientControlsOnly(TapstateException refused,boolean pending){
        return refused.code()==ValidationError.VALUE_LIMIT_EXCEEDED&&retainedFrames==0&&workFrames==maintenanceFrames
                &&(pending||pendingBundleFrames==0);
    }
    /** Retirement invalidates a waiter; waking it does not grant or return a lease. */
    public synchronized void wakeWaiters(){wakeGeneration++;notifyAll();}
    private List<Lease> acquire(Object owner,Kind kind,int frames){return acquire(owner,kind,frames,false);}
    private synchronized List<Lease> acquire(Object owner,Kind kind,int frames,boolean pending){
        Objects.requireNonNull(owner,"metadata owner");
        if(frames<1||frames>FRAMES-2||frames!=1&&kind!=Kind.RETAINED)throw new IllegalArgumentException("invalid metadata frame bundle");
        boolean retained=kind==Kind.RETAINED,work=kind!=Kind.CONTROL;
        if((pendingBundleFrames!=0&&!pending&&(kind!=Kind.CONTROL||slots.availablePermits()<=pendingBundleFrames))
                ||(retained&&retainedFrames>FRAMES-1-frames)||(work&&workFrames>FRAMES-1-frames)
                ||(kind==Kind.MAINTENANCE&&slots.availablePermits()<3)||!slots.tryAcquire(frames))
            throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation.metadataFrames"),null);
        if(retained)retainedFrames+=frames;if(work)workFrames+=frames;if(kind==Kind.MAINTENANCE)maintenanceFrames+=frames;
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
        @Override public void close(){if(owner.getAndSet(null)!=null)synchronized(ValidationMetadataBudget.this){if(kind==Kind.RETAINED)retainedFrames--;if(kind!=Kind.CONTROL)workFrames--;if(kind==Kind.MAINTENANCE)maintenanceFrames--;slots.release();ValidationMetadataBudget.this.notifyAll();}}
    }
}
