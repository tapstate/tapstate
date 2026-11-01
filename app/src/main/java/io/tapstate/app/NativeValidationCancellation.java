package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFailureLatch;
import java.util.Map;

/** Local cancellation only; authority, native read and cleanup failures retain their own diagnosis. */
final class NativeValidationCancellation extends TapstateException {
    NativeValidationCancellation(String node) {
        super(ValidationError.EXECUTION_CANCELLED, Map.of("node", node), null);
    }
    static void record(ValidationFailureLatch failures,Throwable reported,boolean requested) {
        var wrappers=new java.util.ArrayList<Throwable>();
        var seen=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable,Boolean>());
        var original=reported;
        for(int depth=0;depth<32&&seen.add(original)&&original.getCause()!=null
                &&(original instanceof java.util.concurrent.CompletionException||original instanceof java.util.concurrent.ExecutionException
                ||original instanceof com.hazelcast.jet.JetException);depth++){
            wrappers.add(original);original=original.getCause();
        }
        recordOriginal(failures,original,requested);
        for(int index=wrappers.size()-1;index>=0;index--)for(var secondary:wrappers.get(index).getSuppressed())recordOriginal(failures,secondary,false);
    }
    private static void recordOriginal(ValidationFailureLatch failures,Throwable reported,boolean requested){
        var cause=reported.getCause();
        boolean cancellationCause=cause==null||((cause instanceof java.util.concurrent.CancellationException||cause instanceof InterruptedException)
                &&cause.getCause()==null&&cause.getSuppressed().length==0);
        boolean signal=reported instanceof java.util.concurrent.CancellationException||reported instanceof TapstateException coded
                &&(coded.code()==ValidationError.EXECUTION_CANCELLED||coded.code()==ValidationError.READ_CANCELLED);
        if(requested&&signal&&cancellationCause){
            // Cancellation is control flow; its unwind may still carry genuine cleanup failures.
            for(var secondary:reported.getSuppressed())record(failures,secondary,false);
        }else{
            failures.record(reported);ValidationFailureLatch.addContext(failures.get(),reported);
        }
    }
}
