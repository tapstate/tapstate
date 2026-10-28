package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;

/** Explicit safe HTTP projections; no persisted record or credential is serialized generically. */
final class AlertHttpModel {
    private AlertHttpModel() { }
    static Map<String,Object> object(Object value) {
        if (!(value instanceof Map<?,?> m) || m.keySet().stream().anyMatch(k -> !(k instanceof String))) throw invalid("object");
        @SuppressWarnings("unchecked") var result=(Map<String,Object>)m;return result;
    }
    static Map<String,Object> map(Object... pairs) { var m=new LinkedHashMap<String,Object>();for(int i=0;i<pairs.length;i+=2)m.put((String)pairs[i],pairs[i+1]);return m; }
    static String text(Map<String,Object> m,String key) { var v=m.get(key);if(!(v instanceof String s)||s.isBlank()||s.length()>8192)throw invalid(key);return s; }
    static String optional(Map<String,Object> m,String key) {return m.get(key)==null?null:text(m,key);}
    static String privateInput(Map<String,Object> m,String key){return m.get(key) instanceof String s&&s.isEmpty()?null:optional(m,key);}
    static String literal(Map<String,Object> m,String key){var v=m.getOrDefault(key,"");if(!(v instanceof String s))throw invalid(key);return s;}
    static long number(Map<String,Object> m,String key,long fallback){var v=m.get(key);if(v==null)return fallback;try{return new java.math.BigDecimal(v.toString()).longValueExact();}catch(NumberFormatException|ArithmeticException e){throw invalid(key);}}
    static int integer(Map<String,Object> m,String key,int fallback){long value=number(m,key,fallback);if(value<Integer.MIN_VALUE||value>Integer.MAX_VALUE)throw invalid(key);return (int)value;}
    static boolean bool(Map<String,Object> m,String key,boolean fallback){var v=m.get(key);if(v==null)return fallback;if(!(v instanceof Boolean b))throw invalid(key);return b;}
    static List<?> list(Map<String,Object> m,String key){var v=m.getOrDefault(key,List.of());if(!(v instanceof List<?> l)||l.size()>100)throw invalid(key);return l;}
    static Set<String> strings(Map<String,Object> m,String key){var out=new LinkedHashSet<String>();for(var v:list(m,key)){if(!(v instanceof String s)||!out.add(s))throw invalid(key);}return Set.copyOf(out);}
    static <E extends Enum<E>> E value(Class<E> type,String value){try{return Enum.valueOf(type,value);}catch(IllegalArgumentException e){throw invalid("enum");}}
    static void fields(Map<String,Object> m,String... allowed){if(!Set.of(allowed).containsAll(m.keySet()))throw invalid("unknown-field");}
    static TapstateException invalid(String field){return new TapstateException(AlertError.INVALID_CONFIGURATION,Map.of("field",field,"reason","invalid-http-input"),null);}
    static TapstateException stale(String field){return new TapstateException(AlertError.STALE_ACTION,Map.of("action",field),null);}
    static String instant(Instant v){return v==null?null:v.toString();}
    static Map<String,Object> resource(AlarmKey.Resource r){return map("kind",r.kind(),"id",r.id(),"incarnation",r.incarnation());}
    static AlarmKey.Resource resource(Map<String,Object> m){fields(m,"kind","id","incarnation");return new AlarmKey.Resource(text(m,"kind"),text(m,"id"),text(m,"incarnation"));}
    static String key(AlarmKey k){return encode(map("resource",resource(k.resource()),"typeId",k.typeId(),"qualifier",k.qualifier()));}
    static AlarmKey key(String token,AlarmKey.Scope scope){var m=decode(token);fields(m,"resource","typeId","qualifier");return new AlarmKey(scope,resource(object(m.get("resource"))),text(m,"typeId"),text(m,"qualifier"));}
    static String encode(Map<String,Object> m){return Base64.getUrlEncoder().withoutPadding().encodeToString(JsonWriter.write(m).getBytes(StandardCharsets.UTF_8));}
    static Map<String,Object> decode(String s){if(s==null||s.length()>8192)throw invalid("cursor");try{return object(JsonReader.parse(new String(Base64.getUrlDecoder().decode(s),StandardCharsets.UTF_8)));}catch(IllegalArgumentException e){throw invalid("cursor");}}
    static Map<String,Object> episode(AlertStateRecord.Episode e){var v=e.value();return map("key",key(e.originalBinding().key()),"resource",resource(e.originalBinding().key().resource()),"typeId",e.originalBinding().key().typeId(),"episode",v.number(),"severity",v.severity().name(),"firedAt",instant(v.firedAt()),"endedAt",instant(v.endedAt()),"recoveredAt",instant(v.recoveredAt()),"endReason",v.endReason()==null?null:v.endReason().name(),"status",v.endedAt()==null?"ACTIVE":v.endReason()==AlertSnapshot.EndReason.RECOVERED?"RECOVERED":"ENDED");}
    static Map<String,Object> head(AlertStateRecord.Head h){var m=h.episode()==null?map("key",key(h.snapshot().rule().key())):episode(h.episode());m.put("quality",h.snapshot().quality().name());m.put("observedAt",instant(h.snapshot().lastObservedAt()));return m;}
    static Map<String,Object> diagnostics(AlertStateRecord.Diagnostics d){var m=map("status",d.status().name());if(d.history()!=null){var c=d.history().latest();m.put("text",c.trace().text());m.put("observedAt",instant(c.occurrence().observedAt()));m.put("truncated",c.trace().truncated());m.put("redacted",c.trace().redacted());}return m;}
    static Map<String,Object> delivery(io.tapstate.spi.store.AlertStore.Stored<AlertIntent.Record> stored,boolean detail){
        var r=stored.value();var f=r.frozen();var s=r.state();var m=map("candidateId",f.identity() instanceof AlertIntent.Test test?test.command().parameters().candidate().id():null,"id",f.identity().deliveryId(),"etag",Long.toString(stored.revision()),"channel",f.channel().typeId(),"outcome",s.summary().name(),"createdAt",instant(f.createdAt()),"deadline",instant(f.deadline()),"nextAttemptAt",instant(s.nextAttemptAt()),"attempts",s.attempts().size(),"title",title(r),"target",deliveryTarget(f),"resources",f.members().stream().map(member->member.evaluation().key().resource().id()).distinct().limit(20).toList(),"stopped",s.permitsStopped(),"source",null,"eventId",null);
        m.put("stopReason",s.stopReason()==null?null:s.stopReason().name());m.put("displayState",displayState(r));if(s.acceptedReceipt()!=null){m.put("source",s.acceptedReceipt().source());m.put("eventId",s.acceptedReceipt().remoteId());}
        if(detail){m.put("members",f.members().stream().map(v->map("key",key(v.evaluation().key()),"episode",v.episode())).toList());m.put("attemptHistory",s.attempts().stream().map(a->map("number",a.permit().ordinal(),"outcome",a.outcome().name(),"endedAt",instant(a.endedAt()),"phase",a.protocol()==null?null:a.protocol().phase().name(),"status",a.protocol()==null?null:a.protocol().status())).toList());}
        return m;
    }
    private static String deliveryTarget(AlertIntent.Frozen frozen){
        var transport=frozen.target() instanceof AlertIntent.ReceiverTarget receiver?receiver.transport():frozen.target() instanceof AlertIntent.CandidateTarget candidate?candidate.transport():null;
        return transport instanceof AlertReceiver.Email?frozen.identity().targetId():transport instanceof AlertReceiver.Webhook webhook?webhook.origin().toASCIIString():null;
    }
    private static String displayState(AlertIntent.Record record){var s=record.state();
        if(s.summary()==DeliveryEvidence.Outcome.ACCEPTED)return "ACCEPTED";
        if(s.permitsStopped()){if(Set.of(AlertIntent.StopReason.CANCELLED,AlertIntent.StopReason.REVOKED,AlertIntent.StopReason.DISABLED).contains(s.stopReason()))return "CANCELLED";
            if(s.stopReason()==AlertIntent.StopReason.EXHAUSTED)return "EXHAUSTED";if(s.summary()==DeliveryEvidence.Outcome.UNKNOWN)return "UNKNOWN";
            return s.stopReason()==AlertIntent.StopReason.EXPIRED||s.stopReason()==AlertIntent.StopReason.AGE_LIMIT?"EXPIRED":"NOT_ACCEPTED";}
        if(s.attempts().stream().anyMatch(a->a.endedAt()==null))return "IN_FLIGHT";
        if(s.attemptsConsumed()==0)return "QUEUED";
        if(s.nextAttemptAt()!=null)return "WAITING_RETRY";
        if(s.currentCycleConsumed()>=record.frozen().retry().attemptsPerCycle())return "EXHAUSTED";
        if(!s.attempts().isEmpty()&&s.attempts().getLast().protocol()!=null&&s.attempts().getLast().protocol().action()==AlertIntent.Action.PAUSE)return "PAUSED";
        return s.summary()==DeliveryEvidence.Outcome.UNKNOWN?"UNKNOWN":"NOT_ACCEPTED";
    }
    private static String title(AlertIntent.Record r){
        if(r.state().payload()==null)return null;var bytes=r.state().payload().bytes();
        try{
            if(r.frozen().channel().typeId().equals("email")){
                String header=new String(bytes,0,Math.min(bytes.length,16384),StandardCharsets.ISO_8859_1);int end=header.indexOf("\r\n\r\n");if(end<0)return null;
                var match=java.util.regex.Pattern.compile("(?m)^Subject: ([^\r\n]*)").matcher(header.substring(0,end).replaceAll("\r\n[ \t]+"," "));if(!match.find())return null;
                var words=java.util.regex.Pattern.compile("=\\?UTF-8\\?B\\?([A-Za-z0-9+/=]+)\\?=",java.util.regex.Pattern.CASE_INSENSITIVE).matcher(match.group(1));var out=new StringBuilder();while(words.find())out.append(new String(Base64.getDecoder().decode(words.group(1)),StandardCharsets.UTF_8));return out.isEmpty()?null:out.substring(0,Math.min(1024,out.length()));
            }
            if(bytes.length>64*1024)return null;var object=object(JsonReader.parse(new String(bytes,StandardCharsets.UTF_8)));Object title=object.getOrDefault("title",object.get("subject"));return title instanceof String text?text.substring(0,Math.min(text.length(),1024)):null;
        }catch(IllegalArgumentException unreadable){return null;}
    }
    static MessageSettings messages(Map<String,Object> m){fields(m,"titlePrefix","titleSuffix","messageNote","footerText");return new MessageSettings(literal(m,"titlePrefix"),literal(m,"titleSuffix"),literal(m,"messageNote"),literal(m,"footerText"));}
    static Map<String,Object> messages(MessageSettings s){return map("titlePrefix",s.titlePrefix(),"titleSuffix",s.titleSuffix(),"messageNote",s.messageNote(),"footerText",s.footerText());}
}
