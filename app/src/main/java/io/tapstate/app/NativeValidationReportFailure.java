package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.core.validation.ValidationReportMetadata;
import java.util.*;

/** Bounded first-cause facts, redacted at the existing process secret boundary before retention. */
final class NativeValidationReportFailure {
    private NativeValidationReportFailure(){}
    static ValidationReportMetadata.Failure capture(Throwable failure,SecretRedactor redactor){
        if(failure==null)return null;
        var params=new LinkedHashMap<String,String>();String code=null;
        if(failure instanceof TapstateException coded){
            code=coded.code().code();
            for(var entry:coded.args().entrySet()){
                if(params.size()==32)break;
                var value=entry.getValue();
                String text=value==null?"null":value instanceof String||value instanceof Number||value instanceof Boolean?String.valueOf(value):value.getClass().getName();
                params.put(safe(entry.getKey(),64,redactor),safe(text,128,redactor));
            }
        }
        var details=new StringBuilder();var seen=Collections.newSetFromMap(new IdentityHashMap<Throwable,Boolean>());
        append(failure,details,seen,redactor,0);
        return new ValidationReportMetadata.Failure(code,params,clip(failure.getClass().getName(),256),
                safe(failure.getMessage()==null?"":failure.getMessage(),1024,redactor),clip(details.toString(),4096),failure instanceof Error);
    }
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
