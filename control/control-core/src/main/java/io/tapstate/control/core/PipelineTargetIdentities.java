package io.tapstate.control.core;

import io.tapstate.core.model.PipelineResource;
import java.util.List;

/** Read-only identity assessment of the exact definition and target tables a start would write. */
@FunctionalInterface
public interface PipelineTargetIdentities {
    List<Assessment> assess(PipelineResource definition, boolean fullLoad);

    record Assessment(String element, String connection, String table, String status, String reason,
            String sourceField, String preservedField, List<String> writeKey) {
        public Assessment { writeKey = List.copyOf(writeKey); }
    }
}
