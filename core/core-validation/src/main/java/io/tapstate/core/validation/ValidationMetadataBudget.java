package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/** Process-local accounting only. It neither executes work nor grants storage or runtime authority. */
public final class ValidationMetadataBudget {
    public static final int FRAMES=4;
    public static final long RESERVED_BYTES=64L*1024*1024;
    private final Semaphore slots=new Semaphore(FRAMES);
    public Lease acquire(Object owner){
        Objects.requireNonNull(owner,"metadata owner");
        if(!slots.tryAcquire())throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation.metadataFrames"),null);
        return new Lease(owner);
    }
    public int inUse(){return FRAMES-slots.availablePermits();}
    public final class Lease implements AutoCloseable {
        private final AtomicReference<Object> owner;
        private Lease(Object owner){this.owner=new AtomicReference<>(owner);}
        public void requireRetained(Object expected){
            if(expected==null||owner.get()!=expected)throw new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","resources.metadataOwner"),null);
        }
        /** The caller first relinquishes every decoded value, in-flight body and serialized response. */
        @Override public void close(){if(owner.getAndSet(null)!=null)slots.release();}
    }
}
