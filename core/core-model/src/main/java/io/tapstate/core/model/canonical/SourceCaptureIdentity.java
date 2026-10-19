package io.tapstate.core.model.canonical;

import java.time.Instant;
import java.util.*;

/** Canonical source identities shared by capture assembly and storage preparation. */
public final class SourceCaptureIdentity {
    private SourceCaptureIdentity() {}
    public static String physical(String connector,Map<String,Object> settings) {
        StringBuilder input = new StringBuilder(); token(input,Objects.requireNonNull(connector,"connector")); value(input,Objects.requireNonNull(settings,"settings"));
        return "mc-"+CanonicalHash.ofText(input.toString());
    }
    public static String resolve(String connector,Map<String,Object> settings,String key) {
        return key != null && !key.isBlank() ? "mck-"+key : physical(connector,settings);
    }
    public static String directChannel(String connector,Map<String,Object> settings,String key,String pipeline,String source) {
        named(pipeline); named(source); var input = new StringBuilder();
        token(input,resolve(connector,settings,key)); token(input,pipeline); token(input,source);
        return "mcd-"+CanonicalHash.ofText(input.toString());
    }
    public static String directCapture(String connector,Map<String,Object> settings,String key,String pipeline,String source,String start,Collection<String> streams) {
        named(pipeline); named(source); String axis = "tail:direct:"+startAxis(start)+'|'+pipeline.length()+':'+pipeline+'|'+source.length()+':'+source;
        var input = new StringBuilder(resolve(connector,settings,key)).append('|').append(axis);
        var sorted = new ArrayList<>(streams); sorted.sort(String::compareTo); input.append('|').append(sorted.size());
        for (String stream : sorted) input.append('|').append(stream.length()).append(':').append(stream);
        return "capture-"+CanonicalHash.ofText(input.toString());
    }
    public static String startAxis(String start) {
        if (start == null || start.equals("latest")) return "latest";
        if (start.equals("earliest")) return "earliest";
        var instant = Instant.parse(start); instant.toEpochMilli();
        return "at:"+instant;
    }
    private static void named(String name) { if (name == null || name.isBlank()) throw new IllegalArgumentException("a private capture names a pipeline and source"); }
    private static void value(StringBuilder input,Object value) {
        switch (value) {
            case null -> input.append('~');
            case Map<?,?> values -> {
                var sorted = new TreeMap<String,Object>(); values.forEach((key,item) -> sorted.put(String.valueOf(key),item));
                input.append('M').append(sorted.size()).append('{');
                sorted.forEach((key,item) -> { token(input,key); value(input,item); }); input.append('}');
            }
            case List<?> values -> { input.append('L').append(values.size()).append('['); values.forEach(item -> value(input,item)); input.append(']'); }
            case String text -> { input.append('S'); token(input,text); }
            case Boolean flag -> input.append('B').append(flag);
            default -> { input.append('N'); token(input,String.valueOf(value)); }
        }
    }
    private static void token(StringBuilder input,String value) { input.append(value.length()).append(':').append(value); }
}
