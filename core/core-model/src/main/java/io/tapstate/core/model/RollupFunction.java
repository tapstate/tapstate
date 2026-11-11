package io.tapstate.core.model;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * The aggregate functions a rollup field may apply over the live elements of a level. The YAML spelling is
 * the key a rollup field carries its argument under, which is why it is spelled here once rather than by
 * every reader of the tree.
 */
public enum RollupFunction {
    COUNT("count"),
    COUNT_DISTINCT("count_distinct"),
    SUM("sum"),
    AVG("avg"),
    MIN("min"),
    MAX("max"),
    FIRST("first"),
    LAST("last");

    private final String yaml;

    RollupFunction(String yaml) {
        this.yaml = yaml;
    }

    public String yaml() {
        return yaml;
    }

    /** The function a YAML key names, or empty when it names none. */
    public static Optional<RollupFunction> byYaml(String key) {
        return Arrays.stream(values()).filter(f -> f.yaml.equals(key)).findFirst();
    }

    /** Every YAML spelling, in declaration order, for a refusal that has to name what would have worked. */
    public static List<String> spellings() {
        return Arrays.stream(values()).map(RollupFunction::yaml).toList();
    }
}
