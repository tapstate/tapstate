package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Sink-side table rename (§8, X4): explicit per-table map (highest priority)
 * plus bulk rules; application order = case first, then literal prefix/suffix.
 */
@Doc("Sink-side table rename rules: an explicit per-table name map plus bulk case and prefix/suffix transforms.")
public record RenameSpec(
        @Doc("Explicit per-table rename map from source name to target name; takes highest priority.")
        Map<String, String> map,
        @Doc(value = "Case transform applied to table names before prefix/suffix rules.", key = "case")
        RenameCase caseMode,
        @Doc("Prefix prepended to each target table name.")
        String prefix,
        @Doc("Suffix appended to each target table name.")
        String suffix) {

    public RenameSpec {
        map = map == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(map));
    }
}
