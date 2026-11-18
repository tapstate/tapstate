package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Runtime diagnostics retain the original outcome and ownership boundaries. */
public final class ActuationErrorDefinitions {
    public static final ProductErrorDefinition PREVIEW_REFUSED =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 1, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition PIPELINE_NOT_FOUND =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 2, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition NOT_A_PIPELINE =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 3, Severity.ERROR, Set.of("pipeline", "kind"));
    public static final ProductErrorDefinition SOURCE_SCHEMA_NOT_DISCOVERED =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 4, Severity.ERROR, Set.of("source"));
    public static final ProductErrorDefinition SOURCE_TABLE_NOT_DISCOVERED =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 5, Severity.ERROR, Set.of("source", "table"));
    public static final ProductErrorDefinition SOURCE_TABLE_SELECTION_EMPTY =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 6, Severity.ERROR, Set.of("source"));
    public static final ProductErrorDefinition SOURCE_TABLE_REGEX_INVALID =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 7, Severity.ERROR, Set.of("source", "regex"));
    public static final ProductErrorDefinition UNWIND_PARENT_KEY_UNRESOLVED =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 8, Severity.ERROR, Set.of("step", "stream", "reason"));
    public static final ProductErrorDefinition SOURCE_TABLE_AMBIGUOUS =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 9, Severity.ERROR, Set.of("table", "sources"));
    public static final ProductErrorDefinition SOURCE_TABLE_SPEC_UNSUPPORTED =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 10, Severity.ERROR, Set.of("source", "table", "fields"));
    public static final ProductErrorDefinition JOIN_SOURCE_KEY_MISSING =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 11, Severity.ERROR, Set.of("step", "table"));
    public static final ProductErrorDefinition JOIN_SOURCE_NOT_DECLARED =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 12, Severity.ERROR, Set.of("step", "name"));
    public static final ProductErrorDefinition JOIN_INPUT_NOT_A_TABLE =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 13, Severity.ERROR, Set.of("step", "alias", "ref"));
    public static final ProductErrorDefinition JOIN_SQL_INVALID =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 14, Severity.ERROR, Set.of("step", "detail"));
    public static final ProductErrorDefinition JOIN_OUTPUT_KEY_NOT_PUBLISHED =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 15, Severity.ERROR, Set.of("step", "table", "column"));
    public static final ProductErrorDefinition JOIN_OUTPUT_SCHEMA_SOURCE_CHANGED =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 16, Severity.ERROR, Set.of("pipeline", "step", "added", "removed", "retyped"));
    public static final ProductErrorDefinition JOIN_OUTPUT_SCHEMA_ENGINE_CHANGED =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 17, Severity.ERROR, Set.of("pipeline", "step", "added", "removed", "retyped", "recordedBy", "nowBy"));
    public static final ProductErrorDefinition FROM_REGEX_INVALID =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 18, Severity.ERROR, Set.of("regex"));
    public static final ProductErrorDefinition FROM_REGEX_EMPTY =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 19, Severity.ERROR, Set.of("regex"));
    public static final ProductErrorDefinition VIEW_KEY_MISSING =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 20, Severity.ERROR, Set.of("view"));
    public static final ProductErrorDefinition VIEW_STORAGE_TIER_UNSUPPORTED =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 21, Severity.ERROR, Set.of("view", "tier"));
    public static final ProductErrorDefinition VIEW_STORE_NOT_CONFIGURED =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 22, Severity.ERROR, Set.of("store"));
    public static final ProductErrorDefinition VIEW_STORE_UNREACHABLE =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 23, Severity.ERROR, Set.of("store", "reason"));
    public static final ProductErrorDefinition VIEW_KEY_NOT_FEED_IDENTITY =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 24, Severity.ERROR, Set.of("view", "key", "identity"));
    public static final ProductErrorDefinition VIEW_FED_BY_MANY_TABLES =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 25, Severity.ERROR, Set.of("view", "tables"));
    public static final ProductErrorDefinition VIEW_STORE_IS_A_CAPTURE_SOURCE =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 26, Severity.ERROR, Set.of("store"));
    public static final ProductErrorDefinition SCHEMA_SYNC_WHILE_RUNNING =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 27, Severity.ERROR, Set.of("pipeline", "state", "desired"));
    public static final ProductErrorDefinition NO_SAFE_PARALLELISM =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 28, Severity.ERROR, Set.of("pipeline", "node", "requested", "members", "candidates"));
    public static final ProductErrorDefinition PARALLELISM_NEEDS_A_KEY =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 29, Severity.ERROR, Set.of("pipeline", "node", "requested", "reason"));
    public static final ProductErrorDefinition STREAM_LANDS_TWO_WAYS =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 30, Severity.ERROR, Set.of("pipeline", "stream", "nest", "other"));
    public static final ProductErrorDefinition CONNECTOR_UNAVAILABLE_ON_MEMBER =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 31, Severity.ERROR, Set.of("pipeline", "member", "connector", "reason"));
    public static final ProductErrorDefinition CONNECTOR_DIFFERS_ACROSS_MEMBERS =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 32, Severity.ERROR, Set.of("pipeline", "connector", "artifacts"));
    public static final ProductErrorDefinition OPERATOR_STATE_UNAVAILABLE_ON_MEMBER =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 33, Severity.ERROR, Set.of("pipeline","member","reason"));
    public static final ProductErrorDefinition SHARED_RECOVERY_MEMBER_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.ACTUATION, 34, Severity.ERROR, Set.of("capture","member","reason"));

    private ActuationErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(PREVIEW_REFUSED, PIPELINE_NOT_FOUND, NOT_A_PIPELINE, SOURCE_SCHEMA_NOT_DISCOVERED, SOURCE_TABLE_NOT_DISCOVERED, SOURCE_TABLE_SELECTION_EMPTY, SOURCE_TABLE_REGEX_INVALID, UNWIND_PARENT_KEY_UNRESOLVED, SOURCE_TABLE_AMBIGUOUS, SOURCE_TABLE_SPEC_UNSUPPORTED, JOIN_SOURCE_KEY_MISSING, JOIN_SOURCE_NOT_DECLARED, JOIN_INPUT_NOT_A_TABLE, JOIN_SQL_INVALID, JOIN_OUTPUT_KEY_NOT_PUBLISHED, JOIN_OUTPUT_SCHEMA_SOURCE_CHANGED, JOIN_OUTPUT_SCHEMA_ENGINE_CHANGED, FROM_REGEX_INVALID, FROM_REGEX_EMPTY, VIEW_KEY_MISSING, VIEW_STORAGE_TIER_UNSUPPORTED, VIEW_STORE_NOT_CONFIGURED, VIEW_STORE_UNREACHABLE, VIEW_KEY_NOT_FEED_IDENTITY, VIEW_FED_BY_MANY_TABLES, VIEW_STORE_IS_A_CAPTURE_SOURCE, SCHEMA_SYNC_WHILE_RUNNING, NO_SAFE_PARALLELISM, PARALLELISM_NEEDS_A_KEY, STREAM_LANDS_TWO_WAYS, CONNECTOR_UNAVAILABLE_ON_MEMBER, CONNECTOR_DIFFERS_ACROSS_MEMBERS, OPERATOR_STATE_UNAVAILABLE_ON_MEMBER, SHARED_RECOVERY_MEMBER_UNAVAILABLE);
    }
}
