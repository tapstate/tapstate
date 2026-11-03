package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.core.validation.ValidationReportMetadata;
import io.tapstate.messages.MessageCatalog;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/** Bounded first-cause facts, redacted at the existing process secret boundary before retention. */
final class NativeValidationReportFailure {
    private NativeValidationReportFailure(){}
    static ValidationReportMetadata.Failure capture(Throwable failure,SecretRedactor redactor){
        if(failure==null)return null;
        var params=new LinkedHashMap<String,String>();String code=null;var coded=codedCause(failure);
        String message=failure.getMessage()==null?"":failure.getMessage();
        if(coded!=null){
            code=coded.code().code();var renderParams=new LinkedHashMap<String,Object>();int arguments=0;
            for(var entry:coded.args().entrySet()){
                if(arguments++==32)break;
                var value=entry.getValue();
                String text=value==null?"null":value instanceof String||value instanceof Number||value instanceof Boolean?String.valueOf(value):value.getClass().getName();
                var bounded=safe(text,128,redactor);
                params.put(safe(entry.getKey(),64,redactor),bounded);
                renderParams.put(clip(entry.getKey(),64),bounded);
            }
            message=Catalog.VALUE.render(code,renderParams).message();
        }
        var details=new StringBuilder();var seen=Collections.newSetFromMap(new IdentityHashMap<Throwable,Boolean>());
        append(failure,details,seen,redactor,0);
        return new ValidationReportMetadata.Failure(code,params,clip(failure.getClass().getName(),256),
                safe(message,1024,redactor),clip(details.toString(),4096),failure instanceof Error);
    }
    static String code(Throwable failure){var coded=codedCause(failure);return coded==null?null:coded.code().code();}
    /** Unwrap only execution carriers; a programmer failure remains the diagnosis in its own right. */
    private static TapstateException codedCause(Throwable failure){
        var seen=Collections.newSetFromMap(new IdentityHashMap<Throwable,Boolean>());var current=failure;
        for(int depth=0;current!=null&&depth<32&&seen.add(current);depth++){
            if(current instanceof TapstateException coded)return coded;
            if(!(current instanceof CompletionException||current instanceof ExecutionException||current instanceof com.hazelcast.jet.JetException))return null;
            current=current.getCause();
        }
        return null;
    }
    private static final class Catalog {private static final MessageCatalog VALUE=MessageCatalog.bundled();}
    private static void append(Throwable failure,StringBuilder target,Set<Throwable> seen,SecretRedactor redactor,int depth){
        if(failure==null||depth==8||target.length()>=4096||!seen.add(failure))return;
        target.append(clip(failure.getClass().getName(),256)).append(": ")
                .append(safe(failure.getMessage()==null?"":failure.getMessage(),512,redactor)).append('\n');
        var frames=failure.getStackTrace();
        for(int index=0;index<Math.min(8,frames.length)&&target.length()<4096;index++)target.append("  at ").append(safe(frames[index].toString(),256,redactor)).append('\n');
        append(failure.getCause(),target,seen,redactor,depth+1);
        for(var secondary:failure.getSuppressed())append(secondary,target,seen,redactor,depth+1);
    }
    private static String safe(String value,int size,SecretRedactor redactor){return clip(redactor.redactLog(value),size);}
    private static String clip(String value,int size){
        if(value.length()<=size)return value;
        int end=Character.isHighSurrogate(value.charAt(size-1))?size-1:size;
        return value.substring(0,end);
    }
}
