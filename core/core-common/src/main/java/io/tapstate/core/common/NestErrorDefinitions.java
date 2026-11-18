package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class NestErrorDefinitions {
    public static final ProductErrorDefinition SIBLING_EMBEDS_TARGET_DIFFERENT_PARENT_KEYS =
            new ProductErrorDefinition(ProductErrorModule.NEST, 1, Severity.ERROR, Set.of("embedPath", "fields"));
    public static final ProductErrorDefinition EMBED_TARGET_NOT_PARENT_KEY =
            new ProductErrorDefinition(ProductErrorModule.NEST, 2, Severity.ERROR, Set.of("embedPath", "fields", "parentKey"));
    public static final ProductErrorDefinition EMBED_PATH_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.NEST, 3, Severity.ERROR, Set.of("path", "embedPathA", "embedPathB"));
    public static final ProductErrorDefinition FLAT_EMBED_ALIAS_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.NEST, 4, Severity.ERROR, Set.of("parentPath", "alias"));
    public static final ProductErrorDefinition FLAT_FIELD_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.NEST, 5, Severity.ERROR, Set.of("embedPath", "fields", "occupiedBy"));
    public static final ProductErrorDefinition ROOT_KEY_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.NEST, 6, Severity.ERROR, Set.of("rootAlias"));
    public static final ProductErrorDefinition ARRAY_KEY_UNRESOLVABLE =
            new ProductErrorDefinition(ProductErrorModule.NEST, 7, Severity.ERROR, Set.of("embedPath", "table"));
    public static final ProductErrorDefinition KEY_AMBIGUOUS =
            new ProductErrorDefinition(ProductErrorModule.NEST, 8, Severity.ERROR, Set.of("embedPath", "table", "candidates"));
    public static final ProductErrorDefinition REFERENCED_LEVEL_CARRIES_EMBEDS =
            new ProductErrorDefinition(ProductErrorModule.NEST, 9, Severity.ERROR, Set.of("embedPath", "table", "children"));
    public static final ProductErrorDefinition RESOLVER_VERTEX_LIMIT_EXCEEDED =
            new ProductErrorDefinition(ProductErrorModule.NEST, 10, Severity.ERROR, Set.of("vertices", "limit"));
    public static final ProductErrorDefinition CHAIN_AXIS_LIMIT_EXCEEDED =
            new ProductErrorDefinition(ProductErrorModule.NEST, 11, Severity.ERROR, Set.of("chains", "limit"));
    public static final ProductErrorDefinition APPEND_MODE_CONFLICTS_WITH_KEY_TRACKING =
            new ProductErrorDefinition(ProductErrorModule.NEST, 12, Severity.ERROR, Set.of("rootAlias", "embedPath"));
    public static final ProductErrorDefinition SNAPSHOT_PASSTHROUGH_FORBIDDEN =
            new ProductErrorDefinition(ProductErrorModule.NEST, 13, Severity.ERROR, Set.of("rootCollection"));
    public static final ProductErrorDefinition FLAT_CARDINALITY_VIOLATION =
            new ProductErrorDefinition(ProductErrorModule.NEST, 14, Severity.ERROR, Set.of("embedPath", "rows"));
    public static final ProductErrorDefinition MEMORY_BUDGET_BELOW_PARTITION_COUNT =
            new ProductErrorDefinition(ProductErrorModule.NEST, 15, Severity.ERROR, Set.of("entries", "partitions"));
    public static final ProductErrorDefinition MEMORY_BUDGET_CHANGED_WHILE_RUNNING =
            new ProductErrorDefinition(ProductErrorModule.NEST, 16, Severity.ERROR, Set.of("namespace", "configured", "requested"));
    public static final ProductErrorDefinition STATE_DATABASE_CHANGED_WHILE_RUNNING =
            new ProductErrorDefinition(ProductErrorModule.NEST, 17, Severity.ERROR, Set.of("namespace", "configured", "requested"));
    public static final ProductErrorDefinition STATE_PATHS_CHANGED =
            new ProductErrorDefinition(ProductErrorModule.NEST, 18, Severity.ERROR, Set.of("stepId", "recorded", "compiled"));
    public static final ProductErrorDefinition ROOT_FANOUT_LIMIT_EXCEEDED =
            new ProductErrorDefinition(ProductErrorModule.NEST, 19, Severity.ERROR, Set.of("rootKey", "elements", "limit"));
    public static final ProductErrorDefinition REFERENCE_FANOUT_LIMIT_EXCEEDED =
            new ProductErrorDefinition(ProductErrorModule.NEST, 20, Severity.ERROR, Set.of("refPath", "identity", "referrers", "limit"));
    public static final ProductErrorDefinition PENDING_LIMIT_EXCEEDED =
            new ProductErrorDefinition(ProductErrorModule.NEST, 21, Severity.ERROR, Set.of("namespace", "key", "pending", "limit"));
    public static final ProductErrorDefinition TOMBSTONE_LIMIT_EXCEEDED =
            new ProductErrorDefinition(ProductErrorModule.NEST, 22, Severity.ERROR, Set.of("namespace", "key", "tombstones", "limit"));
    public static final ProductErrorDefinition MIGRATION_PARKING_LIMIT_EXCEEDED =
            new ProductErrorDefinition(ProductErrorModule.NEST, 23, Severity.ERROR, Set.of("address", "changes", "limit"));
    public static final ProductErrorDefinition PARENT_ABSENT =
            new ProductErrorDefinition(ProductErrorModule.NEST, 24, Severity.WARNING, Set.of("chain", "bucket", "order", "heldFor"));
    public static final ProductErrorDefinition STATE_SERVED_FROM_COLD_LAYER =
            new ProductErrorDefinition(ProductErrorModule.NEST, 25, Severity.WARNING, Set.of("namespace", "backfills", "accesses", "share", "millis", "entries", "stored", "resident"));
    public static final ProductErrorDefinition KEY_CHANGE_TRACKING_REQUIRES_BEFORE_IMAGE =
            new ProductErrorDefinition(ProductErrorModule.NEST, 26, Severity.ERROR, Set.of("alias", "table", "columns"));
    public static final ProductErrorDefinition REFERENCE_TRACKING_REQUIRES_BEFORE_IMAGE =
            new ProductErrorDefinition(ProductErrorModule.NEST, 27, Severity.ERROR, Set.of("alias", "refPath"));

    private NestErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(SIBLING_EMBEDS_TARGET_DIFFERENT_PARENT_KEYS, EMBED_TARGET_NOT_PARENT_KEY, EMBED_PATH_CONFLICT, FLAT_EMBED_ALIAS_CONFLICT, FLAT_FIELD_CONFLICT, ROOT_KEY_REQUIRED, ARRAY_KEY_UNRESOLVABLE, KEY_AMBIGUOUS, REFERENCED_LEVEL_CARRIES_EMBEDS, RESOLVER_VERTEX_LIMIT_EXCEEDED, CHAIN_AXIS_LIMIT_EXCEEDED, APPEND_MODE_CONFLICTS_WITH_KEY_TRACKING, SNAPSHOT_PASSTHROUGH_FORBIDDEN, FLAT_CARDINALITY_VIOLATION, MEMORY_BUDGET_BELOW_PARTITION_COUNT, MEMORY_BUDGET_CHANGED_WHILE_RUNNING, STATE_DATABASE_CHANGED_WHILE_RUNNING, STATE_PATHS_CHANGED, ROOT_FANOUT_LIMIT_EXCEEDED, REFERENCE_FANOUT_LIMIT_EXCEEDED, PENDING_LIMIT_EXCEEDED, TOMBSTONE_LIMIT_EXCEEDED, MIGRATION_PARKING_LIMIT_EXCEEDED, PARENT_ABSENT, STATE_SERVED_FROM_COLD_LAYER, KEY_CHANGE_TRACKING_REQUIRES_BEFORE_IMAGE, REFERENCE_TRACKING_REQUIRES_BEFORE_IMAGE);
    }
}
