package io.tapstate.core.catalog;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The connectors this release officially supports — the one place that set is written down.
 *
 * <p>It lives here, beside the catalog, because two unrelated surfaces have to agree on it: the
 * runtime register path refuses anything outside it, and the authoring surfaces offer only what that
 * path would accept. Holding a second copy on either side is how the two drift, and the drift is
 * invisible until a user is offered a connector that cannot be installed.
 *
 * <p>A list rather than a set, so the order — and therefore the order a refusal message names them
 * in — is fixed. The membership test over a handful of entries costs nothing.
 *
 * <p>Being outside this set is a support boundary, not a judgement about the connector: the set grows
 * as connectors are certified, and a connector absent from it may well be perfectly functional.
 */
public final class OfficialConnectors {

    /**
     * The supported ids grouped by database kind, in refusal-message order.
     *
     * <p>Variants are enumerated rather than matched by prefix: membership is an explicit support
     * decision, and a prefix rule would silently admit future products. Accepting a managed variant
     * rests on it being the same database kind underneath; verification exercises the database kind
     * itself. Both the grouping and each id list are immutable.
     */
    public static final Map<String, List<String>> IDS_BY_DATABASE_KIND = databaseKinds();

    /** The supported ids, in the order a message naming them should read. */
    public static final List<String> IDS = IDS_BY_DATABASE_KIND.values().stream()
            .flatMap(List::stream)
            .toList();

    private static Map<String, List<String>> databaseKinds() {
        Map<String, List<String>> kinds = new LinkedHashMap<>();
        kinds.put("mysql", List.of(
                "mysql", "aliyun-rds-mysql", "aws-rds-mysql", "polar-db-mysql", "mysql-pxc"));
        kinds.put("postgres", List.of(
                "postgres", "aliyun-rds-postgres", "aliyun-adb-postgres", "polar-db-postgres",
                "tencent-db-postgres"));
        kinds.put("mongodb", List.of(
                "mongodb", "mongodb-atlas", "mongodb3", "aliyun-db-mongodb", "tencent-db-mongodb"));
        return Collections.unmodifiableMap(kinds);
    }

    private OfficialConnectors() {
    }

    /** Whether {@code connectorId} is one this release supports. */
    public static boolean isOfficial(String connectorId) {
        return IDS.contains(connectorId);
    }

    /**
     * The supported ids {@code catalog} actually carries, in this list's order — what an authoring
     * surface may offer, and what a refusal names. Intersected rather than returned outright so that
     * everything offered also resolves: a supported id absent from the catalog has no fields to
     * prompt for. Kept in this list's order rather than the catalog's so a menu and the message that
     * follows a refusal read the same way round.
     */
    public static List<String> presentIn(TapstateCatalog catalog) {
        List<String> present = catalog.ids();
        return IDS.stream().filter(present::contains).toList();
    }
}
