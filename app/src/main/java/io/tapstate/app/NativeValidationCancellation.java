package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFailureLatch;
import java.util.Map;

/** Local cancellation only; authority, native read and cleanup failures retain their own diagnosis. */
final class NativeValidationCancellation extends TapstateException {
    private static final int MAX_CONTEXT_DEPTH=32,MAX_CONTEXT_VISITS=128;
    NativeValidationCancellation(String node) {
        super(ValidationError.EXECUTION_CANCELLED, Map.of("node", node), null);
    }
    static void record(ValidationFailureLatch failures,Throwable reported,boolean requested) {
        record(failures,reported,requested,0,new Traversal(reported));
    }
    private static void record(ValidationFailureLatch failures,Throwable reported,boolean requested,int depth,Traversal traversal){
        if(traversal.exhausted)return;
        if(--traversal.remaining<0){traversal.exhausted=true;retain(failures,traversal.root);return;}
        if(traversal.completed.contains(reported))return;
        if(depth>=MAX_CONTEXT_DEPTH||!traversal.active.add(reported)){retain(failures,reported);return;}
        try{recordEntered(failures,reported,requested,depth,traversal);}
        finally{traversal.active.remove(reported);traversal.completed.add(reported);}
    }
    private static void recordEntered(ValidationFailureLatch failures,Throwable reported,boolean requested,int depth,Traversal traversal){
        var wrappers=new java.util.ArrayList<Throwable>();
        var seen=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable,Boolean>());
        var original=reported;
        for(int index=0;index<MAX_CONTEXT_DEPTH&&seen.add(original)&&original.getCause()!=null&&carrier(original);index++){
            wrappers.add(original);original=original.getCause();
        }
        recordOriginal(failures,original,requested,depth,traversal);
        for(int index=wrappers.size()-1;index>=0&&!traversal.exhausted;index--)
            companions(failures,wrappers.get(index),requested,depth,traversal);
    }
    private static void recordOriginal(ValidationFailureLatch failures,Throwable reported,boolean requested,int depth,Traversal traversal){
        if(!requested||!signal(reported)||!controlCauses(reported.getCause())){retain(failures,reported);return;}
        // One stopped read can report independent query/decode cancellation signals. Their pure
        // control chains are not faults; every companion cleanup diagnosis remains independently owned.
        for(var current=reported;current!=null&&!traversal.exhausted;current=current.getCause())
            companions(failures,current,true,depth,traversal);
    }
    private static void companions(ValidationFailureLatch failures,Throwable value,boolean requested,int depth,Traversal traversal){
        for(var secondary:value.getSuppressed()){
            record(failures,secondary,requested,depth+1,traversal);
            if(traversal.exhausted)return;
        }
    }
    private static final class Traversal {
        final Throwable root;
        final java.util.Set<Throwable> active=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable,Boolean>());
        final java.util.Set<Throwable> completed=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable,Boolean>());
        int remaining=MAX_CONTEXT_VISITS;boolean exhausted;
        Traversal(Throwable root){this.root=root;}
    }
    private static boolean controlCauses(Throwable cause){
        var seen=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable,Boolean>());
        for(int depth=0;cause!=null&&depth<MAX_CONTEXT_DEPTH;depth++){
            if(!seen.add(cause))return false;
            if(carrier(cause)&&cause.getCause()==null)return false;
            if(!signal(cause)&&!(cause instanceof InterruptedException)&&!carrier(cause))return false;
            cause=cause.getCause();
        }
        return cause==null;
    }
    private static boolean signal(Throwable value){
        return value instanceof java.util.concurrent.CancellationException||value instanceof TapstateException coded
                &&(coded.code()==ValidationError.EXECUTION_CANCELLED||coded.code()==ValidationError.READ_CANCELLED);
    }
    private static boolean carrier(Throwable value){
        return value instanceof java.util.concurrent.CompletionException||value instanceof java.util.concurrent.ExecutionException
                ||value instanceof com.hazelcast.jet.JetException;
    }
    private static void retain(ValidationFailureLatch failures,Throwable reported){
        failures.record(reported);ValidationFailureLatch.addContext(failures.get(),reported);
    }
}
