package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import java.math.*;
import java.time.*;
import java.util.*;

/** Explicit portable-value wire forms. No numeric, temporal or binary interpretation is delegated to JSON coercion. */
public final class ValidationWireValues {
    private ValidationWireValues(){}
    public record Value(String type,Object value,Map<String,String> metadata) {}
    public record Entry(String name,Value value) {}
    public record KeyPart(String domain,Value value) {}
    public record Ancestor(List<String> fieldPath,List<KeyPart> key) {}
    public record Root(String mappingId,String outputId,List<KeyPart> key,List<Ancestor> ancestors) {}
    /** One encoder/budget belongs to the complete response, including every item of a page. */
    public static final class Encoder {
        private static final long MAX_BYTES=32L*1024*1024;
        private long bytes;private int nodes;
        public Value value(Object original){return value(original,1,new IdentityHashMap<>());}
        public Root root(ValidationRoot root){
            node();charge(256);text(root.mappingId());text(root.outputId());var key=key(root.rootKey());var ancestors=new ArrayList<Ancestor>();
            for(var ancestor:root.ancestors()){node();charge(128);
                for(var name:ancestor.fieldPath())text(name);
                ancestors.add(new Ancestor(ancestor.fieldPath(),key(ancestor.key())));
            }
            return new Root(root.mappingId(),root.outputId(),key,List.copyOf(ancestors));
        }
        private List<KeyPart> key(ValidationKey key){
            var result=new ArrayList<KeyPart>();
            for(var part:key.parts()){node();charge(64);
                var value=part.value();
                result.add(new KeyPart(part.kind().name(),part.kind()==ValidationKey.Kind.BINARY?binary(part.binarySubtype(),(byte[])value):value(value)));
            }
            return List.copyOf(result);
        }
        private Value value(Object original,int depth,IdentityHashMap<Object,Boolean> active){
            if(depth>64)throw limit();node();charge(256);
            if(original==null)return scalar("null",null);
            if(original instanceof String text){text(text);return scalar("text",text);}
            if(original instanceof Boolean flag)return scalar("boolean",flag);
            if(original instanceof Byte number)return number("int8",number.toString());
            if(original instanceof Short number)return number("int16",number.toString());
            if(original instanceof Integer number)return number("int32",number.toString());
            if(original instanceof Long number)return number("int64",number.toString());
            if(original instanceof BigInteger number)return integer("integer",number,Map.of());
            if(original instanceof BigDecimal number)return integer("decimal",number.unscaledValue(),Map.of("scale",Integer.toString(number.scale())));
            if(original instanceof Float number)return scalar("float32-bits",HexFormat.of().toHexDigits(Float.floatToRawIntBits(number)));
            if(original instanceof Double number)return scalar("float64-bits",HexFormat.of().toHexDigits(Double.doubleToRawLongBits(number)));
            if(original instanceof LocalDate date)return number("date",date.toString());
            if(original instanceof LocalTime time)return number("time",time.toString());
            if(original instanceof LocalDateTime dateTime)return number("local-date-time",dateTime.toString());
            if(original instanceof Instant instant)return number("instant",instant.toString());
            if(original instanceof ValidationValues.Binary binary){chargeBinary(binary.length());return encodedBinary(binary.subtype(),binary.bytes());}
            if(active.put(original,Boolean.TRUE)!=null)throw new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","wire.valueCycle"),null);
            try{
                if(original instanceof ValidationValues.Typed typed){
                    var metadata=new LinkedHashMap<String,String>();metadata.put("logicalType",typed.logicalType());metadata.put("codecBinding",typed.codecBinding());
                    metadata.put("conversionOrigin",typed.conversionOrigin());if(typed.nativeType()!=null)metadata.put("nativeType",typed.nativeType());
                    metadata.forEach((name,content)->{text(name);text(content);});
                    return new Value("typed",value(typed.value(),depth+1,active),Map.copyOf(metadata));
                }
                if(original instanceof Map<?,?> map){
                    if(map.size()>65536-nodes)throw limit();charge(64L+96L*map.size());var members=new ArrayList<Entry>(map.size());
                    for(var entry:map.entrySet()){
                        if(!(entry.getKey() instanceof String name))throw new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","wire.fieldName"),null);
                        text(name);members.add(new Entry(name,value(entry.getValue(),depth+1,active)));
                    }
                    return scalar("object",List.copyOf(members));
                }
                if(original instanceof List<?> list){
                    if(list.size()>65536-nodes)throw limit();charge(64L+16L*list.size());var children=new ArrayList<Value>(list.size());
                    for(var child:list)children.add(value(child,depth+1,active));return scalar("array",List.copyOf(children));
                }
                throw new TapstateException(ValidationError.VALUE_TYPE_UNSUPPORTED,Map.of("type",original.getClass().getName()),null);
            }finally{active.remove(original);}
        }
        private Value integer(String kind,BigInteger value,Map<String,String> metadata){
            long digits=(value.bitLength()*30103L)/100000+2;
            charge(128+8*digits);return new Value(kind,value.toString(),metadata);
        }
        private void node(){if(++nodes>65536)throw limit();}
        private Value number(String kind,String text){text(text);return scalar(kind,text);}
        private Value binary(int subtype,byte[] bytes){
            // Charge the temporary owned copy and encoded output before allocating base64 text.
            chargeBinary(bytes.length);return encodedBinary(subtype,bytes);
        }
        private void chargeBinary(int length){charge(256L+length+8L*((length+2L)/3));}
        private Value encodedBinary(int subtype,byte[] bytes){
            return new Value("binary",Base64.getEncoder().encodeToString(bytes),Map.of("encoding","base64","subtype",Integer.toString(subtype)));
        }
        private Value scalar(String type,Object value){return new Value(type,value,Map.of());}
        private void text(String value){charge(64L+6L*value.length());}
        private void charge(long amount){if(amount<0||amount>MAX_BYTES-bytes)throw limit();bytes+=amount;}
    }
    private static TapstateException limit(){return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation.reportWire"),null);}
}
