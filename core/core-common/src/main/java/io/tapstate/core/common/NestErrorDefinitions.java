package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class NestErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.NEST);

    public static final ProductErrorDefinition SIBLING_EMBEDS_TARGET_DIFFERENT_PARENT_KEYS =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("embedPath", "fields"));
    public static final ProductErrorDefinition EMBED_TARGET_NOT_PARENT_KEY =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("embedPath", "fields", "parentKey"));
    public static final ProductErrorDefinition EMBED_PATH_CONFLICT =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("path", "embedPathA", "embedPathB"));
    public static final ProductErrorDefinition FLAT_EMBED_ALIAS_CONFLICT =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("parentPath", "alias"));
    public static final ProductErrorDefinition FLAT_FIELD_CONFLICT =
            DEFINITIONS.define(5, Severity.ERROR, Set.of("embedPath", "fields", "occupiedBy"));
    public static final ProductErrorDefinition ROOT_KEY_REQUIRED =
            DEFINITIONS.define(6, Severity.ERROR, Set.of("rootAlias"));
    public static final ProductErrorDefinition ARRAY_KEY_UNRESOLVABLE =
            DEFINITIONS.define(7, Severity.ERROR, Set.of("embedPath", "table"));
    public static final ProductErrorDefinition KEY_AMBIGUOUS =
            DEFINITIONS.define(8, Severity.ERROR, Set.of("embedPath", "table", "candidates"));
    public static final ProductErrorDefinition REFERENCED_LEVEL_CARRIES_EMBEDS =
            DEFINITIONS.define(9, Severity.ERROR, Set.of("embedPath", "table", "children"));
    public static final ProductErrorDefinition RESOLVER_VERTEX_LIMIT_EXCEEDED =
            DEFINITIONS.define(10, Severity.ERROR, Set.of("vertices", "limit"));
    public static final ProductErrorDefinition CHAIN_AXIS_LIMIT_EXCEEDED =
            DEFINITIONS.define(11, Severity.ERROR, Set.of("chains", "limit"));
    public static final ProductErrorDefinition APPEND_MODE_CONFLICTS_WITH_KEY_TRACKING =
            DEFINITIONS.define(12, Severity.ERROR, Set.of("rootAlias", "embedPath"));
    public static final ProductErrorDefinition SNAPSHOT_PASSTHROUGH_FORBIDDEN =
            DEFINITIONS.define(13, Severity.ERROR, Set.of("rootCollection"));
    public static final ProductErrorDefinition FLAT_CARDINALITY_VIOLATION =
            DEFINITIONS.define(14, Severity.ERROR, Set.of("embedPath", "rows"));
    public static final ProductErrorDefinition MEMORY_BUDGET_BELOW_PARTITION_COUNT =
            DEFINITIONS.define(15, Severity.ERROR, Set.of("entries", "partitions"));
    public static final ProductErrorDefinition MEMORY_BUDGET_CHANGED_WHILE_RUNNING =
            DEFINITIONS.define(16, Severity.ERROR, Set.of("namespace", "configured", "requested"));
    public static final ProductErrorDefinition STATE_DATABASE_CHANGED_WHILE_RUNNING =
            DEFINITIONS.define(17, Severity.ERROR, Set.of("namespace", "configured", "requested"));
    public static final ProductErrorDefinition STATE_PATHS_CHANGED =
            DEFINITIONS.define(18, Severity.ERROR, Set.of("stepId", "recorded", "compiled"));
    public static final ProductErrorDefinition ROOT_FANOUT_LIMIT_EXCEEDED =
            DEFINITIONS.define(19, Severity.ERROR, Set.of("rootKey", "elements", "limit"));
    public static final ProductErrorDefinition REFERENCE_FANOUT_LIMIT_EXCEEDED =
            DEFINITIONS.define(20, Severity.ERROR, Set.of("refPath", "identity", "referrers", "limit"));
    public static final ProductErrorDefinition PENDING_LIMIT_EXCEEDED =
            DEFINITIONS.define(21, Severity.ERROR, Set.of("namespace", "key", "pending", "limit"));
    public static final ProductErrorDefinition TOMBSTONE_LIMIT_EXCEEDED =
            DEFINITIONS.define(22, Severity.ERROR, Set.of("namespace", "key", "tombstones", "limit"));
    public static final ProductErrorDefinition MIGRATION_PARKING_LIMIT_EXCEEDED =
            DEFINITIONS.define(23, Severity.ERROR, Set.of("address", "changes", "limit"));
    public static final ProductErrorDefinition PARENT_ABSENT =
            DEFINITIONS.define(24, Severity.WARNING, Set.of("chain", "bucket", "order", "heldFor"));
    public static final ProductErrorDefinition STATE_SERVED_FROM_COLD_LAYER =
            DEFINITIONS.define(25, Severity.WARNING, Set.of("namespace", "backfills", "accesses", "share", "millis", "entries", "stored", "resident"));
    public static final ProductErrorDefinition KEY_CHANGE_TRACKING_REQUIRES_BEFORE_IMAGE =
            DEFINITIONS.define(26, Severity.ERROR, Set.of("alias", "table", "columns"));
    public static final ProductErrorDefinition REFERENCE_TRACKING_REQUIRES_BEFORE_IMAGE =
            DEFINITIONS.define(27, Severity.ERROR, Set.of("alias", "refPath"));

    public static final ProductErrorDefinition OR_ON_NOT_A_REFERENCE =
            DEFINITIONS.define(28, Severity.ERROR, Set.of("embedPath", "fields", "parentKey"));

    public static final ProductErrorDefinition ROLLUP_ON_REFERENCE_EMBED =
            DEFINITIONS.define(29, Severity.ERROR, Set.of("embedPath"));

    public static final ProductErrorDefinition ROLLUP_FIELD_CONFLICT =
            DEFINITIONS.define(30, Severity.ERROR, Set.of("field", "level", "other"));

    public static final ProductErrorDefinition ROLLUP_EXPRESSION_INVALID =
            DEFINITIONS.define(31, Severity.ERROR, Set.of("field", "part", "reason"));

    public static final ProductErrorDefinition ROLLUP_TYPE_UNKNOWN =
            DEFINITIONS.define(32, Severity.ERROR, Set.of("field", "expression"));

    public static final ProductErrorDefinition ROLLUP_OVERFLOW =
            DEFINITIONS.define(33, Severity.ERROR, Set.of("field", "root", "type"));

    public static final ProductErrorDefinition ROLLUP_VALUE_TYPE_MISMATCH =
            DEFINITIONS.define(34, Severity.ERROR, Set.of("field", "root", "declared", "actual"));

    public static final ProductErrorDefinition ROLLUP_RESULT_NOT_CONVERTIBLE =
            DEFINITIONS.define(35, Severity.ERROR, Set.of("field", "root", "from", "to"));

    public static final ProductErrorDefinition ROLLUP_EVALUATION_FAILED =
            DEFINITIONS.define(36, Severity.ERROR, Set.of("field", "root", "expression", "reason"));

    private NestErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
