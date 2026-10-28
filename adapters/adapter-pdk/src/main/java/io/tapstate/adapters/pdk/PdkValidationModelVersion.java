package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.*;
import io.tapdata.entity.schema.type.*;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.validation.ValidationError;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.*;

/**
 * A bounded, versioned description of the supported independent model, before any business query.
 * Bean properties of exact supported PDK classes participate, with explicit runtime/layout profiles.
 * Unknown classes and opaque
 * defaults are refused rather than serialized as an incomplete shape. The live model is never cloned
 * or replaced: codecs still receive the exact type discovered by their own session. A digest checks
 * the described shape; it neither freezes a plugin's mutable graph nor certifies codec behavior.
 */
final class PdkValidationModelVersion {
    private static final String PROFILE="pdk-validation-model-v1";
    private static final String DOCUMENT_PROFILE="pdk-validation-document-model-v1";
    private static final String CODEC_PROFILE="pdk-validation-codecs-v2";
    private static final Set<String> DOCUMENT_STATISTICS=Set.of("size","storageSize","avgObjSize","indexSize");
    private static final long MAX_BYTES=1024L*1024;
    private static final int MAX_NODES=65536,MAX_DEPTH=32,MAX_ITEMS=8192;
    private static final Set<Class<?>> MODELS=Set.of(TapTable.class,TapField.class,TapIndex.class,TapIndexEx.class,
            TapIndexField.class,TapConstraint.class,TapConstraintMapping.class,TapArray.class,TapBinary.class,
            TapBoolean.class,TapDate.class,TapDateTime.class,TapTime.class,TapMap.class,TapNumber.class,
            TapString.class,TapYear.class,TapMoney.class,
            TapXml.class,TapJson.class,TapRaw.class);
    // Deployment labels and discovery timestamps are not the independently read table schema.
    private static final Set<String> RUNTIME_TABLE_PROPERTIES=Set.of("getLastUpdate","getPdkId","getPdkGroup","getPdkVersion");
    private static final ClassValue<List<Method>> READERS=new ClassValue<>(){
        @Override protected List<Method> computeValue(Class<?> type){
            return Arrays.stream(type.getMethods())
                    .filter(method->!Modifier.isStatic(method.getModifiers())&&method.getParameterCount()==0)
                    .filter(method->!method.getName().equals("getClass"))
                    .filter(method->method.getName().startsWith("get")||method.getName().startsWith("is"))
                    .filter(method->type!=TapTable.class||!RUNTIME_TABLE_PROPERTIES.contains(method.getName()))
                    .sorted(Comparator.comparing(Method::getName)).toList();
        }
    };
    private PdkValidationModelVersion(){}

    static String of(TapTable model){
        var tree=canonicalModel(model);
        return PROFILE+":"+CanonicalHash.ofText(JsonWriter.write(List.of(PROFILE,tree)));
    }

    static String of(TapTable model,ConnectorRef artifact){
        String profile=documentFields(artifact)?DOCUMENT_PROFILE:PROFILE;
        return profile+":"+CanonicalHash.ofText(JsonWriter.write(List.of(profile,canonicalModel(model,artifact))));
    }

    /** Internal bounded representation, also used to verify complete schema-property differences. */
    static Object canonicalModel(TapTable model,ConnectorRef artifact){return canonicalModel(model,documentFields(artifact));}

    private static Object canonicalModel(TapTable model){return canonicalModel(model,false);}
    private static Object canonicalModel(TapTable model,boolean documentFields){
        Objects.requireNonNull(model,"independent model");
        if(model.getNameFieldMap()==null||model.getNameFieldMap().size()>4096)throw invalid("fields");
        for(var entry:model.getNameFieldMap().entrySet())
            if(entry.getKey()==null||entry.getValue()==null||!entry.getKey().equals(entry.getValue().getName()))throw invalid("fieldNames");
        return new Walker(documentFields).value(model,0);
    }
    private static boolean documentFields(ConnectorRef artifact){
        return Objects.requireNonNull(artifact).className().equals("io.tapdata.mongodb.MongodbConnector");
    }

    /** Binding to actual retained artifact/configuration/profile; never an equivalence oracle. */
    static String codecBinding(ConnectorRef artifact,String configurationHash,String modelVersion){
        Objects.requireNonNull(artifact);Objects.requireNonNull(modelVersion);
        if(artifact.contentHash()==null||!artifact.contentHash().matches("[a-f0-9]{64}")
                ||configurationHash==null||!configurationHash.matches("[a-f0-9]{64}"))throw invalid("codecIdentity");
        var specHash=artifact.spec()==null?null:CanonicalHash.ofText(artifact.spec());
        return CODEC_PROFILE+":"+CanonicalHash.ofText(JsonWriter.write(Arrays.asList(
                CODEC_PROFILE,artifact.contentHash(),artifact.className(),artifact.pdkApiVersion(),
                artifact.requiredLevel(),specHash,configurationHash,modelVersion)));
    }

    private static final class Walker {
        private enum Slot { VALUE, NAMED_FIELDS, NAMED_FIELD, TABLE_ATTRIBUTES }
        private long bytes;
        private int nodes;
        private final boolean documentFields;
        private final IdentityHashMap<Object,Boolean> active=new IdentityHashMap<>();
        Walker(boolean documentFields){this.documentFields=documentFields;}
        Object value(Object value,int depth){return value(value,depth,Slot.VALUE);}
        private Object value(Object value,int depth,Slot slot){
            if(depth>MAX_DEPTH||++nodes>MAX_NODES)throw limit();
            charge(64);
            if(value==null)return List.of("null");
            if(value instanceof String text){text(text);return List.of("string",text);}
            if(value instanceof Boolean)return List.of("boolean",value);
            if(value instanceof TapField.TapDefaultFunction function)
                return List.of("pdk-default-function",function.name());
            if(value instanceof Character character)return List.of("character",Integer.toString(character));
            if(value instanceof Byte||value instanceof Short||value instanceof Integer||value instanceof Long)
                return List.of(value.getClass().getSimpleName(),value.toString());
            if(value.getClass()==BigInteger.class){
                var number=(BigInteger)value;if(number.bitLength()>MAX_BYTES)throw limit();
                var text=number.toString();charge(2L*text.length());return List.of("BigInteger",text);
            }
            if(value.getClass()==BigDecimal.class){
                var number=(BigDecimal)value;if(number.unscaledValue().bitLength()>MAX_BYTES)throw limit();
                var text=number.toString();charge(2L*text.length());return List.of("BigDecimal",text);
            }
            if(value instanceof Float number)return List.of("Float",Integer.toHexString(Float.floatToRawIntBits(number)));
            if(value instanceof Double number)return List.of("Double",Long.toHexString(Double.doubleToRawLongBits(number)));
            if(value instanceof Instant instant)return List.of("Instant",Long.toString(instant.getEpochSecond()),Integer.toString(instant.getNano()));
            if(value instanceof byte[] binary){charge(2L*binary.length);return List.of("bytes",HexFormat.of().formatHex(binary));}
            boolean bean=MODELS.contains(value.getClass());
            if(!bean&&!(value instanceof Map<?,?>)&&!(value instanceof List<?>)&&!(value instanceof Set<?>))throw unsupported(value.getClass());
            if(active.put(value,Boolean.TRUE)!=null)throw invalid("cyclicModel");
            try{
                if(value instanceof Map<?,?> map){
                    if(map.size()>MAX_ITEMS)throw limit();var sorted=new TreeMap<String,Object>();
                    for(var entry:map.entrySet()){
                        if(!(entry.getKey() instanceof String key))throw invalid("mapKey");
                        if(documentFields&&slot==Slot.TABLE_ATTRIBUTES&&DOCUMENT_STATISTICS.contains(key)){
                            // These collection counters are not schema, query fields or codec input.
                            // Validate and charge even excluded metadata; opaque values cannot hide here.
                            if(entry.getValue()!=null&&!(entry.getValue() instanceof Number))throw invalid("documentStatistic");
                            value(entry.getValue(),depth+1);continue;
                        }
                        charge(64L);text(key);sorted.put(key,value(entry.getValue(),depth+1,
                                slot==Slot.NAMED_FIELDS?Slot.NAMED_FIELD:Slot.VALUE));
                    }
                    return List.of("map",sorted);
                }
                if(value instanceof Collection<?> collection){
                    if(collection.size()>MAX_ITEMS)throw limit();charge(16L*collection.size());
                    var elements=new ArrayList<Object>(collection.size());
                    for(var element:collection)elements.add(value(element,depth+1));
                    if(value instanceof Set<?>)elements.sort(Comparator.comparing(JsonWriter::write));
                    return List.of(value instanceof Set<?>?"set":"list",elements);
                }
                var description=new TreeMap<String,Object>();
                for(var reader:READERS.get(value.getClass())){
                    // Document inference replaces fields in random sample order. Its layout ordinal
                    // is not a physical column or a named query/codec input. Keep primary-key order,
                    // all types and attributes, and the original live model supplied to the plugin.
                    if(documentFields&&slot==Slot.NAMED_FIELD&&value.getClass()==TapField.class&&reader.getName().equals("getPos"))continue;
                    if(documentFields&&depth==0&&value.getClass()==TapTable.class&&reader.getName().equals("getMaxPos"))continue;
                    charge(64L+2L*reader.getName().length());
                    Slot child=Slot.VALUE;
                    if(depth==0&&value.getClass()==TapTable.class){
                        if(reader.getName().equals("getNameFieldMap"))child=Slot.NAMED_FIELDS;
                        else if(reader.getName().equals("getTableAttr"))child=Slot.TABLE_ATTRIBUTES;
                    }
                    description.put(reader.getName(),value(read(reader,value),depth+1,child));
                }
                return List.of("bean",value.getClass().getName(),description);
            }finally{active.remove(value);}
        }
        void text(String text){
            charge(2L*text.length());
            for(int index=0;index<text.length();index++){
                char next=text.charAt(index);
                if(Character.isHighSurrogate(next)){
                    if(++index==text.length()||!Character.isLowSurrogate(text.charAt(index)))throw invalid("text");
                }else if(Character.isLowSurrogate(next))throw invalid("text");
            }
        }
        void charge(long amount){if(amount<0||amount>MAX_BYTES-bytes)throw limit();bytes+=amount;}
    }
    private static Object read(Method reader,Object model){
        try{return reader.invoke(model);}
        catch(InvocationTargetException failed){
            if(failed.getCause() instanceof Error fatal)throw fatal;
            throw new IllegalStateException("independent PDK model getter failed",failed.getCause());
        }catch(ReflectiveOperationException failed){throw new IllegalStateException("cannot inspect independent PDK model",failed);}
    }
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","readModel."+field),null);}
    private static TapstateException unsupported(Class<?> type){return new TapstateException(ValidationError.VALUE_TYPE_UNSUPPORTED,Map.of("type","pdk-model:"+type.getSimpleName()),null);}
    private static TapstateException limit(){return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation.readModel"),null);}
}
