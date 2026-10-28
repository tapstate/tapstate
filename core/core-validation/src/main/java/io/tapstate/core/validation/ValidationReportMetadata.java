package io.tapstate.core.validation;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Bounded retained report facts. A window is an observation interval, never a database snapshot. */
public final class ValidationReportMetadata {
    private ValidationReportMetadata(){}
    public record Window(Instant startedAt,Instant finishedAt){
        public Window{
            Objects.requireNonNull(startedAt);
            if(finishedAt!=null&&finishedAt.isBefore(startedAt))throw ValidationError.invalid("report.window");
        }
    }
    /** Already redacted by the assembly; no Throwable, credentials or connector objects cross this port. */
    public record Failure(String code,Map<String,String> params,String exceptionType,String message,String technical,boolean fatal){
        public Failure{
            if(code!=null)text(code,256);Objects.requireNonNull(params);if(params.size()>32)throw limit();
            for(var entry:params.entrySet()){text(entry.getKey(),128);text(entry.getValue(),512);}
            params=Map.copyOf(params);text(exceptionType,256);text(message,2048);text(technical,8192);
            long characters=(code==null?0:code.length())+exceptionType.length()+message.length()+technical.length();
            for(var entry:params.entrySet())characters+=entry.getKey().length()+entry.getValue().length();
            if(characters>12*1024)throw limit();
        }
    }
    private static void text(String value,int maximum){if(value==null||value.length()>maximum)throw limit();}
    private static io.tapstate.core.common.TapstateException limit(){return new io.tapstate.core.common.TapstateException(
            ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation.reportMetadata"),null);}
}
