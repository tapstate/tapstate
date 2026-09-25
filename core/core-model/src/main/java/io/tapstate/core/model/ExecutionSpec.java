package io.tapstate.core.model;

/**
 * How one pipeline node runs: the total parallelism it asks for across the whole cluster, and the batch it
 * works in. Both parts are optional, and an absent one is not the same thing as its default written out:
 * the runtime reports a value the author left out as the node type's default, so an explanation of how wide
 * a node runs never presents a default as a choice somebody made.
 *
 * <p>The parallelism is a target, not a per-member count. What each member runs is worked out from it and
 * the number of members taking part when a run is built, and it can land on the nearest number the cluster
 * can represent rather than on the target itself; the per-member count is never something an author writes.
 */
@Doc("How one pipeline node runs: its target total parallelism across the cluster and the batch it works in.")
public record ExecutionSpec(
        @Doc("Target total number of processors for this node across the whole cluster, from 1 to 1024. "
                + "Omitted means the node type's default: 1 for sources and transforms, 4 for view and "
                + "serve.sync sinks.")
        Integer parallelism,
        @Doc("The batch this node forms before handing its rows on or writing them out.")
        BatchSpec batch) {

    /** The largest total parallelism an author may ask for. */
    public static final int MAX_PARALLELISM = 1024;

    public ExecutionSpec {
        if (parallelism != null && (parallelism < 1 || parallelism > MAX_PARALLELISM)) {
            throw new IllegalArgumentException(
                    "execution.parallelism must be from 1 to " + MAX_PARALLELISM + ", got " + parallelism);
        }
    }

    /** The batch the author wrote, or one that states nothing, so every field reads as its default. */
    public BatchSpec batchOrDefaults() {
        return batch == null ? BatchSpec.DEFAULTS : batch;
    }
}
