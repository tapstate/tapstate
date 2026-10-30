package io.tapstate.runtime.engine;

import io.tapstate.spi.store.OperatorPlacement;
import io.tapstate.spi.store.PipelineIntentScope;
import java.util.List;
import java.util.Objects;

/** Explicit compiled coordinates shared by manifest assembly and the stores that consume it. */
public final class OperatorMapCoordinates {
    public enum NestMap { STATE, PARKING, LOOKUP, REFERENCES }
    private OperatorMapCoordinates() {}

    /** A readable canonical JSON array; root and every authored path retain distinct identities. */
    public static String path(List<String> components) {
        var key = new StringBuilder("["); boolean first = true;
        for (String component : List.copyOf(components)) {
            if (!first) key.append(','); first = false; key.append('"');
            for (int index = 0; index < component.length(); index++) {
                char unit = component.charAt(index);
                if (unit == '"' || unit == '\\') key.append('\\').append(unit);
                else if (unit < 0x20 || Character.isSurrogate(unit)) {
                    key.append('\\').append('u');
                    for (int shift = 12; shift >= 0; shift -= 4) key.append("0123456789abcdef".charAt((unit >>> shift) & 15));
                } else key.append(unit);
            }
            key.append('"');
        }
        return key.append(']').toString();
    }
    public static String nestNamespace(NestMap kind,String alias) {
        Objects.requireNonNull(kind,"kind");
        return switch(kind) {
            case STATE -> "state";
            case PARKING -> "parking";
            case LOOKUP -> "lookup:"+Objects.requireNonNull(alias,"alias");
            case REFERENCES -> "references:"+Objects.requireNonNull(alias,"alias");
        };
    }
    public static String joinNamespace(OperatorPlacement.Resource resource,String alias) {
        return switch(Objects.requireNonNull(resource,"resource")) {
            case JOIN_FACTS -> "facts";
            case JOIN_WRITERS -> "writers";
            case JOIN_DIMENSIONS, JOIN_INDEXES -> Objects.requireNonNull(alias,"alias");
            case NEST_STATE -> throw new IllegalArgumentException("not a Join resource");
        };
    }
    public static OperatorPlacement nest(PipelineIntentScope scope,String database,String step,List<String> path,NestMap kind,String alias) {
        return new OperatorPlacement(scope,database,OperatorPlacement.Resource.NEST_STATE,step,path(path),nestNamespace(kind,alias));
    }
    public static OperatorPlacement join(PipelineIntentScope scope,String database,String step,OperatorPlacement.Resource kind,String alias) {
        return new OperatorPlacement(scope,database,kind,step,"root",joinNamespace(kind,alias));
    }
}
