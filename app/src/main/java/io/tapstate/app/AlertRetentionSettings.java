package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import java.time.Duration;
import java.util.*;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Trusted process defaults. Each accepted record freezes its own values; reads never consult this bean. */
@ConfigurationProperties("tapstate.alerting")
public final class AlertRetentionSettings {
    private Map<String,String> retention=new LinkedHashMap<>();
    public Map<String,String> getRetention(){return retention;}
    public void setRetention(Map<String,String> values){retention=values==null?new LinkedHashMap<>():new LinkedHashMap<>(values);}
    AlertRetentionPolicy resolve(AlertChannelPolicy.Deployment deployment) {
        var defaults=AlertRetentionPolicy.defaults(deployment);var values=new EnumMap<AlertRetentionPolicy.Setting,Duration>(AlertRetentionPolicy.Setting.class);
        values.putAll(defaults.values());var seen=EnumSet.noneOf(AlertRetentionPolicy.Setting.class);
        if(retention.size()>AlertRetentionPolicy.Setting.values().length)throw invalid("too-many-settings");
        for(var entry:retention.entrySet()){
            String key=entry.getKey();if(key==null||key.length()>64||!key.matches("[a-zA-Z._-]+"))throw invalid("unknown-setting");
            String normalized=key.replace("-","").replace("_","").replace(".","").toLowerCase(Locale.ROOT);
            var setting=Arrays.stream(AlertRetentionPolicy.Setting.values()).filter(s->s.name().replace("_","").toLowerCase(Locale.ROOT).equals(normalized)).findFirst().orElseThrow(()->invalid("unknown-setting"));
            if(!values.containsKey(setting))throw invalid("not-configurable-for-deployment");
            if(!seen.add(setting))throw invalid("duplicate-setting");String text=entry.getValue();
            if(text==null||text.length()>64)throw invalid("invalid-duration");
            try{values.put(setting,Duration.parse(text));}catch(java.time.format.DateTimeParseException invalid){throw invalid("invalid-duration");}
        }
        return AlertRetentionPolicy.configure(deployment,values);
    }
    private static TapstateException invalid(String reason){return new TapstateException(AlertError.INVALID_CONFIGURATION,Map.of("field","alerting.retention","reason",reason),null);}
}
