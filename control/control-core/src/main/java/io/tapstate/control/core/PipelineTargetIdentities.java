package io.tapstate.control.core;

import io.tapstate.core.model.PipelineResource;
import java.util.List;

/** Read-only identity assessment of the exact definition and target tables a start would write. */
@FunctionalInterface
public interface PipelineTargetIdentities {
    /** A reader can prove this check irrelevant before target planning asks for metadata. */
    default boolean relevant(PipelineResource definition) { return true; }

    List<Assessment> assess(PipelineResource definition, boolean fullLoad);

    /** One contributing stream retains its identity even when several streams share a sink and table. */
    record Assessment(String element, String stream, String connection, String table, String status, String reason,
            String sourceField, String preservedField, List<String> writeKey) {
        public Assessment {
            java.util.Objects.requireNonNull(stream, "stream");
            writeKey = List.copyOf(writeKey);
        }
    }
}
