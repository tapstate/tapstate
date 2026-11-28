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
    /** Node settings participate independently of the connection settings. */
    public static String resolve(String connector, Map<String, Object> settings, String key,
            Map<String, Object> nodeConfig) {
        if (nodeConfig == null || nodeConfig.isEmpty()) {
            return resolve(connector, settings, key);
        }
        var input = new StringBuilder();
        token(input, key == null || key.isBlank() ? "" : key);
        token(input, physical(connector, settings));
        value(input, nodeConfig);
        return "mcn-" + CanonicalHash.ofText(input.toString());
    }
    /** Preserves the persisted source coordinate of an independent capture. */
    public static String directCoordinate(String connector, Map<String, Object> settings, String key,
            boolean qualified) {
        if (!qualified || key == null || key.isBlank()) return resolve(connector, settings, key);
        var input = new StringBuilder();
        token(input, key);
        token(input, physical(connector, settings));
        return "mcn-" + CanonicalHash.ofText(input.toString());
    }
    public static String sharedCapture(String chainId) {
        named(chainId);return "capture-"+CanonicalHash.ofText(chainId+"|tail:ring");
    }
    public static String versionedSharedChain(String canonical, String artifactHash, String handoff) {
        named(canonical);
        if (artifactHash == null || !artifactHash.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("invalid shared capture artifact hash");
        }
        var input = new StringBuilder();
        token(input, canonical);
        token(input, artifactHash);
        token(input, handoff == null ? "" : handoff);
        return "mcv-" + CanonicalHash.ofText(input.toString());
    }
    public static String snapshotCapture(String chainId,Collection<String> streams) {
        named(chainId);var input=new StringBuilder(chainId).append("|snapshot");
        var sorted=new ArrayList<>(streams);sorted.sort(String::compareTo);input.append('|').append(sorted.size());
        for(String stream:sorted)input.append('|').append(stream.length()).append(':').append(stream);
        return "capture-"+CanonicalHash.ofText(input.toString());
    }
    public static String directChannel(String connector,Map<String,Object> settings,String key,String pipeline,String source) {
        return directChannel(connector,settings,key,pipeline,source,Map.of());
    }
    public static String directChannel(String connector,Map<String,Object> settings,String key,String pipeline,String source,
            Map<String,Object> nodeConfig) {
        return directChannel(resolve(connector,settings,key,nodeConfig),pipeline,source);
    }
    public static String directChannel(String coordinate, String pipeline, String source) {
        named(pipeline); named(source); var input = new StringBuilder();
        token(input,coordinate); token(input,pipeline); token(input,source);
        return "mcd-"+CanonicalHash.ofText(input.toString());
    }
    public static String directCapture(String connector,Map<String,Object> settings,String key,String pipeline,String source,String start,Collection<String> streams) {
        return directCapture(connector,settings,key,pipeline,source,start,streams,Map.of());
    }
    public static String directCapture(String connector,Map<String,Object> settings,String key,String pipeline,String source,String start,Collection<String> streams,
            Map<String,Object> nodeConfig) {
        return directCapture(resolve(connector,settings,key,nodeConfig),pipeline,source,start,streams);
    }
    public static String directCapture(String coordinate, String pipeline, String source, String start, Collection<String> streams) {
        named(pipeline); named(source); String axis = "tail:direct:"+startAxis(start)+'|'+pipeline.length()+':'+pipeline+'|'+source.length()+':'+source;
        var input = new StringBuilder(coordinate).append('|').append(axis);
        var sorted = new ArrayList<>(streams); sorted.sort(String::compareTo); input.append('|').append(sorted.size());
        for (String stream : sorted) input.append('|').append(stream.length()).append(':').append(stream);
        return "capture-"+CanonicalHash.ofText(input.toString());
    }
    // toEpochMilli validates representability while the canonical identity deliberately retains nanoseconds.
    @SuppressWarnings("java:S2201")
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
