package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Runtime diagnostics retain the original outcome and ownership boundaries. */
public final class ActuationErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.ACTUATION);

    public static final ProductErrorDefinition PREVIEW_REFUSED =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition PIPELINE_NOT_FOUND =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition NOT_A_PIPELINE =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("pipeline", "kind"));
    public static final ProductErrorDefinition SOURCE_SCHEMA_NOT_DISCOVERED =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("source"));
    public static final ProductErrorDefinition SOURCE_TABLE_NOT_DISCOVERED =
            DEFINITIONS.define(5, Severity.ERROR, Set.of("source", "table"));
    public static final ProductErrorDefinition SOURCE_TABLE_SELECTION_EMPTY =
            DEFINITIONS.define(6, Severity.ERROR, Set.of("source"));
    public static final ProductErrorDefinition SOURCE_TABLE_REGEX_INVALID =
            DEFINITIONS.define(7, Severity.ERROR, Set.of("source", "regex"));
    public static final ProductErrorDefinition UNWIND_PARENT_KEY_UNRESOLVED =
            DEFINITIONS.define(8, Severity.ERROR, Set.of("step", "stream", "reason"));
    public static final ProductErrorDefinition SOURCE_TABLE_AMBIGUOUS =
            DEFINITIONS.define(9, Severity.ERROR, Set.of("table", "sources"));
    public static final ProductErrorDefinition SOURCE_TABLE_SPEC_UNSUPPORTED =
            DEFINITIONS.define(10, Severity.ERROR, Set.of("source", "table", "fields"));
    public static final ProductErrorDefinition JOIN_SOURCE_KEY_MISSING =
            DEFINITIONS.define(11, Severity.ERROR, Set.of("step", "table"));
    public static final ProductErrorDefinition JOIN_SOURCE_NOT_DECLARED =
            DEFINITIONS.define(12, Severity.ERROR, Set.of("step", "name"));
    public static final ProductErrorDefinition JOIN_INPUT_NOT_A_TABLE =
            DEFINITIONS.define(13, Severity.ERROR, Set.of("step", "alias", "ref"));
    public static final ProductErrorDefinition JOIN_SQL_INVALID =
            DEFINITIONS.define(14, Severity.ERROR, Set.of("step", "detail"));
    public static final ProductErrorDefinition JOIN_OUTPUT_KEY_NOT_PUBLISHED =
            DEFINITIONS.define(15, Severity.ERROR, Set.of("step", "table", "column"));
    public static final ProductErrorDefinition JOIN_OUTPUT_SCHEMA_SOURCE_CHANGED =
            DEFINITIONS.define(16, Severity.ERROR, Set.of("pipeline", "step", "added", "removed", "retyped"));
    public static final ProductErrorDefinition JOIN_OUTPUT_SCHEMA_ENGINE_CHANGED =
            DEFINITIONS.define(17, Severity.ERROR, Set.of("pipeline", "step", "added", "removed", "retyped", "recordedBy", "nowBy"));
    public static final ProductErrorDefinition FROM_REGEX_INVALID =
            DEFINITIONS.define(18, Severity.ERROR, Set.of("regex"));
    public static final ProductErrorDefinition FROM_REGEX_EMPTY =
            DEFINITIONS.define(19, Severity.ERROR, Set.of("regex"));
    public static final ProductErrorDefinition VIEW_KEY_MISSING =
            DEFINITIONS.define(20, Severity.ERROR, Set.of("view"));
    public static final ProductErrorDefinition VIEW_STORAGE_TIER_UNSUPPORTED =
            DEFINITIONS.define(21, Severity.ERROR, Set.of("view", "tier"));
    public static final ProductErrorDefinition VIEW_STORE_NOT_CONFIGURED =
            DEFINITIONS.define(22, Severity.ERROR, Set.of("store"));
    public static final ProductErrorDefinition VIEW_STORE_UNREACHABLE =
            DEFINITIONS.define(23, Severity.ERROR, Set.of("store", "reason"));
    public static final ProductErrorDefinition VIEW_KEY_NOT_FEED_IDENTITY =
            DEFINITIONS.define(24, Severity.ERROR, Set.of("view", "key", "identity"));
    public static final ProductErrorDefinition VIEW_FED_BY_MANY_TABLES =
            DEFINITIONS.define(25, Severity.ERROR, Set.of("view", "tables"));
    public static final ProductErrorDefinition VIEW_STORE_IS_A_CAPTURE_SOURCE =
            DEFINITIONS.define(26, Severity.ERROR, Set.of("store"));
    public static final ProductErrorDefinition SCHEMA_SYNC_WHILE_RUNNING =
            DEFINITIONS.define(27, Severity.ERROR, Set.of("pipeline", "state", "desired"));
    public static final ProductErrorDefinition NO_SAFE_PARALLELISM =
            DEFINITIONS.define(28, Severity.ERROR, Set.of("pipeline", "node", "requested", "members", "candidates"));
    public static final ProductErrorDefinition PARALLELISM_NEEDS_A_KEY =
            DEFINITIONS.define(29, Severity.ERROR, Set.of("pipeline", "node", "requested", "reason"));
    public static final ProductErrorDefinition STREAM_LANDS_TWO_WAYS =
            DEFINITIONS.define(30, Severity.ERROR, Set.of("pipeline", "stream", "nest", "other"));
    public static final ProductErrorDefinition CONNECTOR_UNAVAILABLE_ON_MEMBER =
            DEFINITIONS.define(31, Severity.ERROR, Set.of("pipeline", "member", "connector", "reason"));
    public static final ProductErrorDefinition CONNECTOR_DIFFERS_ACROSS_MEMBERS =
            DEFINITIONS.define(32, Severity.ERROR, Set.of("pipeline", "connector", "artifacts"));
    public static final ProductErrorDefinition OPERATOR_STATE_UNAVAILABLE_ON_MEMBER =
            DEFINITIONS.define(33, Severity.ERROR, Set.of("pipeline","member","reason"));
    public static final ProductErrorDefinition SHARED_RECOVERY_MEMBER_UNAVAILABLE =
            DEFINITIONS.define(34, Severity.ERROR, Set.of("capture","member","reason"));

    public static final ProductErrorDefinition SYNC_OUTPUT_IDENTITY_UNRESOLVED =
            DEFINITIONS.define(35, Severity.ERROR, Set.of("pipeline", "stream"));

    public static final ProductErrorDefinition PREVIEW_ASSOCIATION_KEY_DROPPED =
            DEFINITIONS.define(36, Severity.ERROR, Set.of("relation", "field", "transform", "droppedField"));

    private ActuationErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
