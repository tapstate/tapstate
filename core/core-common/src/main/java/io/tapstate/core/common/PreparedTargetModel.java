package io.tapstate.core.common;

import java.io.*;
import java.math.BigDecimal;
import java.security.*;
import java.util.*;

/** The original ordered target-preparation hash preimage, independent of SPI and connector classes. */
public record PreparedTargetModel(String name,List<Field> fields,List<Index> indexes) {
    public record Field(String name,String type,boolean primaryKey,TapstateType inferredType,
                        NumericType numericType,StringType stringType) { }
    public record Index(List<String> fields,boolean unique,boolean constrainsEveryRow) {
        public Index { fields=List.copyOf(fields);if(fields.isEmpty() || !unique && constrainsEveryRow)throw new IllegalArgumentException("invalid prepared index"); }
    }
    public PreparedTargetModel {
        if(name==null || name.isBlank())throw new IllegalArgumentException("blank prepared target");
        fields=List.copyOf(fields);indexes=List.copyOf(indexes);
        for(var field:fields)if(field.name()==null || field.name().isBlank())throw new IllegalArgumentException("blank prepared field");
    }
    public byte[] canonical() {
        try {
            var bytes=new ByteArrayOutputStream();
            try(var out=new DataOutputStream(bytes)) {
                out.writeInt(1);text(out,name);out.writeInt(fields.size());
                for(var field:fields) {
                    text(out,field.name());text(out,field.type());out.writeBoolean(field.primaryKey());
                    text(out,field.inferredType()==null?null:field.inferredType().name());
                    var numeric=field.numericType();out.writeBoolean(numeric!=null);
                    if(numeric!=null){scalar(out,numeric.bit());scalar(out,numeric.fixed());scalar(out,numeric.unsigned());scalar(out,numeric.zerofill());scalar(out,numeric.minValue());scalar(out,numeric.maxValue());scalar(out,numeric.precision());scalar(out,numeric.scale());}
                    var string=field.stringType();out.writeBoolean(string!=null);
                    if(string!=null){scalar(out,string.bytes());scalar(out,string.fixed());scalar(out,string.doubleBytes());scalar(out,string.defaultValue());scalar(out,string.byteRatio());}
                }
                out.writeInt(indexes.size());
                for(var index:indexes){out.writeInt(index.fields().size());for(String field:index.fields())text(out,field);out.writeBoolean(index.unique());out.writeBoolean(index.constrainsEveryRow());}
            }
            return bytes.toByteArray();
        } catch(IOException impossible){throw new IllegalStateException(impossible);}
    }
    public String modelHash(){return hash(canonical());}
    public static String hash(byte[] canonical) {
        try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical));}
        catch(NoSuchAlgorithmException impossible){throw new IllegalStateException(impossible);}
    }
    public static PreparedTargetModel decode(byte[] canonical) {
        if(canonical.length>1048576)throw new IllegalArgumentException("prepared model exceeds decode bound");
        try(var in=new DataInputStream(new ByteArrayInputStream(canonical))) {
            if(in.readInt()!=1)throw new IllegalArgumentException("unknown prepared model encoding");
            String name=text(in);var fields=new ArrayList<Field>();
            for(int count=count(in,13);count>0;count--) {
                String field=text(in),type=text(in);boolean key=bool(in);String inferred=text(in);
                NumericType number=bool(in)?new NumericType(integer(text(in)),booleanValue(text(in)),booleanValue(text(in)),booleanValue(text(in)),decimal(text(in)),decimal(text(in)),integer(text(in)),integer(text(in))):null;
                StringType string=bool(in)?new StringType(longValue(text(in)),booleanValue(text(in)),booleanValue(text(in)),longValue(text(in)),integer(text(in))):null;
                fields.add(new Field(field,type,key,inferred==null?null:TapstateType.valueOf(inferred),number,string));
            }
            var indexes=new ArrayList<Index>();
            for(int count=count(in,6);count>0;count--) {
                var names=new ArrayList<String>();for(int size=count(in,4);size>0;size--)names.add(text(in));
                indexes.add(new Index(names,bool(in),bool(in)));
            }
            var model=new PreparedTargetModel(name,fields,indexes);
            if(in.available()!=0 || !Arrays.equals(model.canonical(),canonical))throw new IllegalArgumentException("noncanonical prepared model");
            return model;
        } catch(IOException|NullPointerException invalid){throw new IllegalArgumentException("unreadable prepared model",invalid);}
    }
    private static int count(DataInputStream in,int minimum)throws IOException {
        int n=in.readInt();if(n<0 || n>in.available()/minimum)throw new IllegalArgumentException("invalid prepared count");return n;
    }
    private static boolean bool(DataInputStream in)throws IOException {int v=in.readUnsignedByte();if(v>1)throw new IllegalArgumentException("invalid prepared boolean");return v==1;}
    private static String text(DataInputStream in)throws IOException {
        int n=in.readInt();if(n==-1)return null;if(n<0 || n>in.available()/2)throw new IllegalArgumentException("invalid prepared text");
        var chars=new char[n];for(int i=0;i<n;i++)chars[i]=in.readChar();return new String(chars);
    }
    private static Integer integer(String v){return v==null?null:Integer.valueOf(v);}
    private static Long longValue(String v){return v==null?null:Long.valueOf(v);}
    private static BigDecimal decimal(String v){return v==null?null:new BigDecimal(v);}
    private static Boolean booleanValue(String v){if(v==null)return null;if(!v.equals("true")&&!v.equals("false"))throw new IllegalArgumentException("invalid boolean attribute");return Boolean.valueOf(v);}
    private static void scalar(DataOutputStream out,Object value)throws IOException{text(out,value==null?null:value.toString());}
    private static void text(DataOutputStream out,String value)throws IOException{out.writeInt(value==null?-1:value.length());if(value!=null)out.writeChars(value);}
}
