package io.tapstate.core.lifecycle;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A nest step's tree as it compiles, with what compiling it inferred: for each embed whether the document
 * owns its rows or points at a shared row, which way its join resolved, and the limits it will run under.
 * Read off the definition and the discovered models, so it is answerable before the pipeline has ever run.
 *
 * @param pipelineId the pipeline
 * @param step the nest step
 * @param rootAlias the alias the root reads
 * @param rootKey the columns that identify one document
 * @param elementLimit how many elements one document may hold before the run is failed
 * @param embeds the embeds directly under the root, in declared order
 */
public record NestTree(String pipelineId, String step, String rootAlias, List<String> rootKey, long elementLimit,
        List<Level> embeds) {

    public NestTree {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(step, "step");
        Objects.requireNonNull(rootAlias, "rootAlias");
        rootKey = List.copyOf(rootKey);
        embeds = List.copyOf(embeds);
    }

    /**
     * One embed.
     *
     * @param path the dotted path from the root to the field the embed fills; a flat embed has its parent's
     * @param alias the alias the embed reads
     * @param as {@code array}, {@code object} or {@code flat}
     * @param owned true when each row belongs to the one parent it names; false when the parent points at a
     *     row that many parents may share
     * @param on the join as declared: this embed's column, then the parent column it matches
     * @param key the columns that identify one of this embed's rows
     * @param referrerLimit for a shared row, how many parents may point at one row before the run is failed;
     *     zero for an owned embed
     * @param children the embeds beneath this one
     */
    public record Level(String path, String alias, String as, boolean owned, Map<String, String> on,
            List<String> key, long referrerLimit, List<Level> children) {
        public Level {
            Objects.requireNonNull(path, "path");
            Objects.requireNonNull(alias, "alias");
            Objects.requireNonNull(as, "as");
            on = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(on));
            key = key == null ? List.of() : List.copyOf(key);
            children = List.copyOf(children);
        }
    }
}
