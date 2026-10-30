package io.tapstate.adapters.pdk;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.sink.WriteMode;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Selects keyed insert/update behavior only from alternatives declared by the exact connector artifact. */
final class PdkWritePolicies {
    private static final Map<String,String> UPSERT=Map.of(
            "dml_insert_policy","update_on_exists","dml_update_policy","insert_on_nonexists");
    private PdkWritePolicies() { }

    static Map<String,String> select(String connector,String spec,WriteMode mode) {
        if(mode!=WriteMode.UPSERT || spec==null) return Map.of();
        if(!(JsonReader.parse(spec) instanceof Map<?,?> root)
                || !(root.get("configOptions") instanceof Map<?,?> options)
                || !(options.get("capabilities") instanceof List<?> capabilities)) return Map.of();
        var selected=new LinkedHashMap<String,String>();
        for(var value:capabilities) {
            if(!(value instanceof Map<?,?> capability) || !(capability.get("id") instanceof String id)) continue;
            var required=UPSERT.get(id);
            if(required==null) continue;
            if(selected.containsKey(id) || !(capability.get("alternatives") instanceof List<?> alternatives)
                    || !alternatives.contains(required)) {
                throw new TapstateException(ConnectorError.WRITE_FAILED,Map.of("connector",connector,
                        "detail","UPSERT requires an unambiguous "+id+" alternative: "+required),null);
            }
            selected.put(id,required);
        }
        return Map.copyOf(selected);
    }
}
