package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One element of {@code pipeline.transforms} (§5): an inline definition or a
 * {@code use:} reference to a {@code kind: transform} definition body (X19).
 *
 * <p>The model is the post-normalization form: every step carries an id (anonymous inline
 * steps receive a generated {@code <type>_<N>} id at parse time, canonical-form.md §5;
 * a use-reference's id defaults to its {@code use} target) and explicit {@code from}
 * wiring (natural-order sugar is expanded at parse time).
 */
@Doc("One transform in the pipeline: either an inline transform definition or a reference to a named transform.")
public sealed interface Step {

    String id();

    FromClause from();

    /** How this step runs, or null where the author left it to the step type's defaults. */
    ExecutionSpec execution();

    static Inline inline(String id, FromClause from, TransformBody body,
                         Map<String, Object> experimental) {
        return new Inline(id, from, body, null, experimental);
    }

    static Inline inline(String id, FromClause from, TransformBody body, ExecutionSpec execution,
                         Map<String, Object> experimental) {
        return new Inline(id, from, body, execution, experimental);
    }

    static Use use(String id, String use, FromClause from) {
        return new Use(id, use, from, null);
    }

    static Use use(String id, String use, FromClause from, ExecutionSpec execution) {
        return new Use(id, use, from, execution);
    }

    @Doc("A transform defined inline in the pipeline, with its body specified directly.")
    record Inline(
            @Doc(value = "Unique step id within the pipeline; auto-generated for anonymous inline steps.", required = true)
            String id,
            @Doc(value = "The upstream steps or sources this transform reads from.", required = true)
            FromClause from,
            @YamlFlatten TransformBody body,
            @Doc("How this node runs: its target total parallelism across the cluster and its batch.")
            ExecutionSpec execution,
            @Doc("Experimental fields, exempt from the v1 compatibility freeze.")
            Map<String, Object> experimental) implements Step {
        public Inline(String id, FromClause from, TransformBody body, Map<String, Object> experimental) {
            this(id, from, body, null, experimental);
        }

        public Inline {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(body, "body");
            boolean aliased = body instanceof TransformBody.Nest || body instanceof TransformBody.Join;
            if (aliased != (from instanceof FromClause.Aliases)) {
                throw new IllegalArgumentException(
                        "nest/join take an alias-map from:, streaming steps take a list from: (§5)");
            }
            experimental = copy(experimental);
        }
    }

    /**
     * {@code use:} reference. The definition body is taken as it stands: the reference overrides
     * nothing in it. How the step runs is not part of the body - a definition is pure logic, and the
     * same one may run wide in one pipeline and narrow in another - so {@code execution} lives here, on
     * the step that instantiates it, as a typed component rather than the free options map it replaced.
     */
    @Doc("A reference to a named transform definition, used as defined.")
    record Use(
            @Doc("Unique step id within the pipeline; defaults to the referenced transform name.")
            String id,
            @Doc(value = "Name of the transform definition to reuse.", required = true)
            String use,
            @Doc(value = "The upstream steps or sources this transform reads from.", required = true)
            FromClause from,
            @Doc("How this node runs: its target total parallelism across the cluster and its batch.")
            ExecutionSpec execution)
            implements Step {
        public Use(String id, String use, FromClause from) {
            this(id, use, from, null);
        }

        public Use {
            Objects.requireNonNull(use, "use");
            Objects.requireNonNull(from, "from");
            id = id == null ? use : id;
        }
    }

    private static Map<String, Object> copy(Map<String, Object> map) {
        return map == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(map));
    }
}
