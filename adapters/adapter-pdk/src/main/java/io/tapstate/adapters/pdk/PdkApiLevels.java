package io.tapstate.adapters.pdk;

import java.util.Map;
import java.util.OptionalInt;

/**
 * The Tapstate-side registry mapping a PDK API version to its compatibility level, and the level this
 * bridge provides. The level is a monotonic integer: a connector loads when its required level is at
 * or below the provided one. Levels live here, on the Tapstate side, not stamped into the inherited PDK
 * artifacts — a new upstream build adds a row here, the inherited repos stay untouched.
 *
 * <p>Build-number drift is API-equivalent: a version resolves by its {@code major.minor.patch} base,
 * so {@code 2.0.8}, {@code 2.0.8-SNAPSHOT} and a timestamped {@code 2.0.8-20260609.043233-3} are one
 * and the same level. A version with no row is a registry gap, not a compatibility verdict: for a
 * connector's declared version {@link #levelOf} reports the gap so the load path can refuse it with a
 * coded, actionable diagnosis (add the missing row, or use a recognized build); for the bridge's own
 * baseline {@link #level} keeps crashing loudly, because a bridge that cannot place the version it was
 * built against is a build defect, not an operator's connector.
 */
public final class PdkApiLevels {

    /**
     * The version→level table. Holds the frozen baseline the bridge is built against plus the inherited
     * connector build lines that share its API contract; append a row per new upstream API version.
     * Append-only: an existing base version keeps its level (renumbering would silently re-judge every
     * connector). {@code 2.0.5} / {@code 2.0.7} / {@code 2.0.8} are one level because they are API-
     * equivalent under PDK's {@code 2.0.x} backward-compatibility line. The audited
     * {@code 2.0.9} PDK API artifact contains the same class bytes as the baseline.
     */
    private static final Map<String, Integer> LEVELS = Map.of(
            "2.0.5", 1,
            "2.0.7", 1,
            "2.0.8", 1,
            "2.0.9", 1);

    /** The frozen baseline the bridge compiles and runs against. */
    private static final String ENGINE_VERSION = "2.0.8";

    /** The level this bridge provides — the level of the baseline it is built against. */
    public static final int ENGINE_LEVEL = level(ENGINE_VERSION);

    private PdkApiLevels() {
    }

    /**
     * The {@code major.minor.patch} base of a version, dropping any {@code -SNAPSHOT} or timestamped
     * build qualifier. Build-number drift within one base version is treated as API-equivalent.
     */
    public static String baseVersion(String version) {
        int dash = version.indexOf('-');
        return dash < 0 ? version : version.substring(0, dash);
    }

    /**
     * The compatibility level for a PDK API version, resolved by its base, or empty when the base has
     * no registered row. This is the lookup a connector's declared version takes: an empty result is a
     * registry gap the load path turns into a coded refusal, never a bare crash on operator data.
     */
    public static OptionalInt levelOf(String version) {
        Integer level = LEVELS.get(baseVersion(version));
        return level == null ? OptionalInt.empty() : OptionalInt.of(level);
    }

    /**
     * The compatibility level for a PDK API version, resolved by its base. This is the strict variant
     * the bridge takes for its OWN baseline: an unregistered base is a build defect and crashes loudly.
     * A connector's declared version goes through {@link #levelOf} instead, so operator data never
     * bare-crashes here.
     *
     * @throws IllegalStateException if the base version has no registered level — a Tapstate-side build
     *     defect for the baseline, not a condition to launder into a verdict.
     */
    public static int level(String version) {
        return levelOf(version).orElseThrow(() -> new IllegalStateException(
                "no registered PDK API level for version " + version + " (base " + baseVersion(version)
                        + "); add a row to the Tapstate-side level registry"));
    }
}
