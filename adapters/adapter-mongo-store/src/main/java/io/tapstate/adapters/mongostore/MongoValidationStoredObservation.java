package io.tapstate.adapters.mongostore;

import io.tapstate.core.validation.*;
import io.tapstate.spi.store.IoError;
import io.tapstate.core.common.TapstateException;
import java.math.*;
import java.time.*;
import java.util.*;
import org.bson.Document;
import org.bson.types.Binary;

/** Strict read validation of the existing explicit observation encoding. */
final class MongoValidationStoredObservation {
    private MongoValidationStoredObservation(){}
    static ValidationRoot validate(Document root,Document payload,ValidationRunSpec spec){
        try{return validateBody(root,payload,spec);}catch(RuntimeException bad){throw unreadable();}
    }
    private static ValidationRoot validateBody(Document rawRoot,Document payload,ValidationRunSpec spec){
        var root=root(rawRoot);if(!root.ancestors().isEmpty())throw unreadable();
        var mapping=spec.task().mappings().stream().filter(item->item.mappingId().equals(root.mappingId())&&item.outputId().equals(root.outputId())).findFirst().orElseThrow(MongoValidationStoredObservation::unreadable);
        fields(payload,"method","participation","targetPresence","checks","counts","findings","notCompared","notApplicableScopes","reads","samples","samplesTruncated","confirmation");
        var method=ValidationMethod.valueOf(string(payload,"method"));
        if(method!=spec.method()||method==ValidationMethod.COUNT||!"LOCAL_OBSERVATION".equals(payload.get("confirmation")))throw unreadable();
        var participation=ValidationCheckEvent.Participation.valueOf(string(payload,"participation"));
        var presence=ValidationRootCheck.TargetPresence.valueOf(string(payload,"targetPresence"));
        var checks=document(payload,"checks");var statuses=new EnumMap<ValidationRootCheck.Check,ValidationRootCheck.Status>(ValidationRootCheck.Check.class);
        for(var entry:checks.entrySet())statuses.put(ValidationRootCheck.Check.valueOf(entry.getKey()),ValidationRootCheck.Status.valueOf((String)entry.getValue()));
        if(statuses.size()!=4||method==ValidationMethod.UNIQUE_KEY&&statuses.get(ValidationRootCheck.Check.VALUES)!=ValidationRootCheck.Status.NOT_APPLICABLE)throw unreadable();
        var reads=new HashSet<String>();boolean sourceRead=false,targetRead=false;
        for(var item:list(payload,"reads",1024)){
            var read=(Document)item;fields(read,"side","readId","contractVersion","startedAt","finishedAt");
            var side=ValidationRootCheck.Side.valueOf(string(read,"side"));
            var boundary=new ValidationCheckEvent.ReadBoundary(string(read,"readId"),string(read,"contractVersion"),instant(document(read,"startedAt")),instant(document(read,"finishedAt")));
            if(!reads.add(side.name()+":"+boundary.readId()))throw unreadable();
            sourceRead|=side==ValidationRootCheck.Side.SOURCE;targetRead|=side==ValidationRootCheck.Side.TARGET;
        }
        if(!sourceRead||targetRead!=(presence!=ValidationRootCheck.TargetPresence.NOT_READ))throw unreadable();
        var findings=new HashSet<ValidationRootCheck.Finding>();var totals=new EnumMap<ValidationRootCheck.Kind,Long>(ValidationRootCheck.Kind.class);
        for(var item:list(payload,"findings",1024)){
            var row=(Document)item;fields(row,"kind","location","multiplicity","count");
            var finding=finding(row,root);long count=positive(row,"count");if(!findings.add(finding))throw unreadable();
            totals.merge(finding.kind(),count,Math::addExact);
            if(method==ValidationMethod.UNIQUE_KEY&&(finding.kind()==ValidationRootCheck.Kind.TARGET_TYPE||finding.kind()==ValidationRootCheck.Kind.TARGET_VALUE))throw unreadable();
        }
        var counts=document(payload,"counts");if(counts.size()!=totals.size())throw unreadable();
        for(var entry:counts.entrySet())if(!Objects.equals(totals.get(ValidationRootCheck.Kind.valueOf(entry.getKey())),positive(counts,entry.getKey())))throw unreadable();
        for(var name:List.of("notCompared","notApplicableScopes"))for(var item:list(payload,name,1024)){
            var row=(Document)item;fields(row,"check","location","cause");var check=ValidationRootCheck.Check.valueOf(string(row,"check"));
            var location=location(document(row,"location"),root);var cause=finding(document(row,"cause"),root);if(!findings.contains(cause))throw unreadable();
            if(name.equals("notCompared")){new ValidationRootCheck.Blocked(check,location,cause);if(statuses.get(check)!=ValidationRootCheck.Status.BLOCKED)throw unreadable();}
            else new ValidationRootCheck.Skipped(check,location,cause);
        }
        for(var item:list(payload,"samples",256)){
            var row=(Document)item;fields(row,"finding","source","target");var finding=finding(document(row,"finding"),root);if(!findings.contains(finding))throw unreadable();
            if(row.get("source")==null&&row.get("target")==null)throw unreadable();
            if(row.get("source")!=null)sample(document(row,"source"),ValidationRootCheck.Side.SOURCE,reads,mapping.source());
            if(row.get("target")!=null)sample(document(row,"target"),ValidationRootCheck.Side.TARGET,reads,mapping.target());
        }
        if(!(payload.get("samplesTruncated") instanceof Boolean))throw unreadable();
        if(participation==ValidationCheckEvent.Participation.UNKNOWN&&presence==ValidationRootCheck.TargetPresence.NOT_READ
                &&statuses.get(ValidationRootCheck.Check.STRUCTURE)!=ValidationRootCheck.Status.BLOCKED)throw unreadable();
        return root;
    }
    private static void sample(Document row,ValidationRootCheck.Side side,Set<String> reads,ValidationTaskSpec.Table expected){
        fields(row,"identity","readId","row");if(!reads.contains(side.name()+":"+string(row,"readId")))throw unreadable();
        var id=document(row,"identity");fields(id,"sourceId","database","schema","table","keyFields","actualKey");
        var keyFields=new ArrayList<List<String>>();for(var path:list(id,"keyFields",32)){
            if(!(path instanceof List<?> parts))throw unreadable();var names=new ArrayList<String>();for(var name:parts)names.add((String)name);keyFields.add(names);
        }
        var identity=new ValidationPhysicalIdentity(string(id,"sourceId"),(String)id.get("database"),(String)id.get("schema"),string(id,"table"),keyFields,key(list(id,"actualKey",32)));
        if(!expected.sourceId().equals(identity.sourceId())||!Objects.equals(expected.database(),identity.database())
                ||!Objects.equals(expected.schema(),identity.schema())||!expected.table().equals(identity.table()))throw unreadable();
        var value=value(document(row,"row"),0);if(!(value instanceof Map<?,?>))throw unreadable();
    }
    private static ValidationRootCheck.Finding finding(Document row,ValidationRoot expected){
        var allowed=new HashSet<>(row.keySet());allowed.remove("count");if(!allowed.equals(Set.of("kind","location","multiplicity")))throw unreadable();
        Optional<ValidationRootCheck.Multiplicity> multiplicity=Optional.empty();
        if(row.get("multiplicity")!=null){var repeated=document(row,"multiplicity");fields(repeated,"occurrences","exact");
            multiplicity=Optional.of(new ValidationRootCheck.Multiplicity(positive(repeated,"occurrences"),(Boolean)repeated.get("exact")));}
        return new ValidationRootCheck.Finding(ValidationRootCheck.Kind.valueOf(string(row,"kind")),location(document(row,"location"),expected),multiplicity);
    }
    private static ValidationRootCheck.Location location(Document row,ValidationRoot expected){
        fields(row,"scope","fieldPath");var scope=root(document(row,"scope"));
        if(!scope.mappingId().equals(expected.mappingId())||!scope.outputId().equals(expected.outputId())||!scope.rootKey().equals(expected.rootKey()))throw unreadable();
        return new ValidationRootCheck.Location(scope,list(row,"fieldPath",64));
    }
    private static ValidationRoot root(Document row){
        fields(row,"mappingId","outputId","key","ancestors");var ancestors=new ArrayList<ValidationRoot.Ancestor>();
        for(var item:list(row,"ancestors",32)){var ancestor=(Document)item;fields(ancestor,"fieldPath","key");
            var path=new ArrayList<String>();for(var name:list(ancestor,"fieldPath",32))path.add((String)name);
            ancestors.add(new ValidationRoot.Ancestor(path,key(list(ancestor,"key",32))));}
        var root=new ValidationRoot(string(row,"mappingId"),string(row,"outputId"),key(list(row,"key",32)),ancestors);
        if(!MongoValidationObservationCodec.root(root).equals(row))throw unreadable();return root;
    }
    private static ValidationKey key(List<?> encoded){
        var parts=new ArrayList<ValidationKey.Part>();for(var item:encoded){var row=(Document)item;fields(row,"kind","binarySubtype","value");
            var kind=ValidationKey.Kind.valueOf(string(row,"kind"));int subtype=(Integer)row.get("binarySubtype");
            Object value=kind==ValidationKey.Kind.BINARY?row.get("value"):value(document(row,"value"),0);
            var part=switch(kind){
                case EXACT_NUMBER -> ValidationKey.Part.number((BigDecimal)value);case TEXT -> ValidationKey.Part.text((String)value);
                case BOOLEAN -> ValidationKey.Part.bool((Boolean)value);case BINARY -> ValidationKey.Part.binary(subtype,((Binary)value).getData());
                case DATE -> ValidationKey.Part.date((LocalDate)value);case TIME -> ValidationKey.Part.time((LocalTime)value);
                case LOCAL_DATE_TIME -> ValidationKey.Part.localDateTime((LocalDateTime)value);case INSTANT -> ValidationKey.Part.instant((Instant)value);
            };if(subtype!=part.binarySubtype())throw unreadable();parts.add(part);
        }return new ValidationKey(parts);
    }
    private static Object value(Document row,int depth){
        if(depth>64)throw unreadable();var kind=string(row,"kind");Object answer=switch(kind){
            case "NULL" -> null;case "TYPED" -> new ValidationValues.Typed(string(row,"logicalType"),(String)row.get("nativeType"),string(row,"codecBinding"),string(row,"conversionOrigin"),value(document(row,"value"),depth+1));
            case "BINARY" -> new ValidationValues.Binary((Integer)row.get("subtype"),((Binary)row.get("value")).getData());
            case "DECIMAL" -> new BigDecimal(new BigInteger(string(row,"unscaled")),(Integer)row.get("scale"));
            case "INTEGER" -> new BigInteger(string(row,"value"));case "BYTE" -> {int number=(Integer)row.get("value");if(number<Byte.MIN_VALUE||number>Byte.MAX_VALUE)throw unreadable();yield (byte)number;}
            case "SHORT" -> {int number=(Integer)row.get("value");if(number<Short.MIN_VALUE||number>Short.MAX_VALUE)throw unreadable();yield (short)number;}
            case "INT" -> (Integer)row.get("value");case "LONG" -> (Long)row.get("value");
            case "FLOAT_BITS" -> Float.intBitsToFloat((Integer)row.get("value"));case "DOUBLE_BITS" -> Double.longBitsToDouble((Long)row.get("value"));
            case "TEXT" -> string(row,"value");case "BOOLEAN" -> (Boolean)row.get("value");case "INSTANT" -> instant(document(row,"value"));
            case "DATE" -> LocalDate.parse(string(row,"value"));case "TIME" -> LocalTime.parse(string(row,"value"));case "LOCAL_DATE_TIME" -> LocalDateTime.parse(string(row,"value"));
            case "OBJECT" -> {var map=new LinkedHashMap<String,Object>();for(var item:list(row,"entries",65536)){var entry=(Document)item;fields(entry,"name","value");
                var name=string(entry,"name");if(map.containsKey(name))throw unreadable();map.put(name,value(document(entry,"value"),depth+1));}yield map;}
            case "ARRAY" -> {var items=new ArrayList<Object>();for(var item:list(row,"values",65536))items.add(value((Document)item,depth+1));yield items;}
            default -> throw unreadable();
        };if(!MongoValidationObservationCodec.value(answer,depth).equals(row))throw unreadable();return answer;
    }
    private static Instant instant(Document row){fields(row,"seconds","nanos");if(!(row.get("seconds") instanceof Long seconds)||!(row.get("nanos") instanceof Integer nanos)||nanos<0||nanos>999999999)throw unreadable();return Instant.ofEpochSecond(seconds,nanos);}
    private static Document document(Document row,String name){if(!(row.get(name) instanceof Document value))throw unreadable();return value;}
    private static String string(Document row,String name){if(!(row.get(name) instanceof String value))throw unreadable();return value;}
    private static List<Object> list(Document row,String name,int max){if(!(row.get(name) instanceof List<?> value)||value.size()>max)throw unreadable();return new ArrayList<>(value);}
    private static long positive(Document row,String name){if(!(row.get(name) instanceof Long value)||value<1)throw unreadable();return value;}
    private static void fields(Document row,String... names){if(!row.keySet().equals(Set.of(names)))throw unreadable();}
    private static TapstateException unreadable(){return new TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","validation","field","root-observation"),null);}
}
