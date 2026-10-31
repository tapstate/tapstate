package io.tapstate.app;

import io.tapstate.adapters.alerting.AlarmTypeText;
import io.tapstate.core.alerting.*;
import java.util.Objects;
import java.util.Set;

/** Versioned static host composition; this is not a runtime plugin or user-supplied registry. */
record AlertTypeAssembly(AlarmTypeCatalog catalog,AlarmTypeText descriptions,Set<String> providers) {
    AlertTypeAssembly {Objects.requireNonNull(catalog);Objects.requireNonNull(descriptions);providers=Set.copyOf(providers);
        if(providers.size()>128)throw new IllegalArgumentException("too many fixed providers");}
    static AlertTypeAssembly builtin(){return new AlertTypeAssembly(BuiltinAlarmTypes.catalog(),AlarmTypeText.english(),
            Set.of("pipeline-lifecycle","pipeline-observations","source-operational-health","cluster-member-health","cluster-storage-health"));}
}
