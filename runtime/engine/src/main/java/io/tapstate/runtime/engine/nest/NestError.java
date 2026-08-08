package io.tapstate.runtime.engine.nest;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;

import java.util.Set;

/**
 * The {@code nest} domain's error codes: the diagnosable failures of assembling nested documents.
 * They fall in three groups by when they are raised — when a nest tree is checked, when a deployment
 * is started, and while a pipeline runs — and the group decides what the author can do about it.
 *
 * <p>Engine invariant violations are deliberately absent: a missing order arriving at a stateful node,
 * or state that contradicts itself, crashes bare rather than being dressed as a diagnosable error.
 *
 * <p>{@code placeholders()} is the named-argument contract: every throw site supplies a value for each
 * name, and the build-time placeholder gate checks the catalog templates against it. Codes are an
 * external contract and append-only — renaming or removing one is a breaking change.
 */
public enum NestError implements TapstateErrorCode {

    /**
     * Checking the tree: the embeds under one parent join on different parent fields, so that parent has
     * no single identity for its children to point at.
     */
    SIBLING_EMBEDS_TARGET_DIFFERENT_PARENT_KEYS(
            "nest.sibling-embeds-target-different-parent-keys", Set.of("embedPath", "fields")),

    /** Checking the tree: two embeds under one parent claim the same path, where one would overwrite the other. */
    EMBED_PATH_CONFLICT("nest.embed-path-conflict", Set.of("path", "embedPathA", "embedPathB")),

    /**
     * Checking the tree: the root declares no key, so its documents have no identity for children to be
     * grouped under and nothing to partition the assembled documents by.
     */
    ROOT_KEY_REQUIRED("nest.root-key-required", Set.of("rootAlias")),

    /**
     * Checking the tree: an embed declares no array key and its table offers no primary key to take one
     * from, leaving its elements with no identity — updates would pile up as duplicates.
     */
    ARRAY_KEY_UNRESOLVABLE("nest.array-key-unresolvable", Set.of("embedPath", "table")),

    /** Checking the tree: it compiles to more resolver vertices than the limit allows, each taking a thread. */
    RESOLVER_VERTEX_LIMIT_EXCEEDED("nest.resolver-vertex-limit-exceeded", Set.of("vertices", "limit")),

    /**
     * Building the job: the pipeline draws from more chains than there are axes for their bounds to travel
     * on, so one of them would have no way to report how far the frontier may go. {@code chains} is how
     * many it draws from, {@code limit} how many there are.
     */
    CHAIN_AXIS_LIMIT_EXCEEDED("nest.chain-axis-limit-exceeded", Set.of("chains", "limit")),

    /**
     * Checking the tree: an append-only root cannot also track structural key changes, because moving a
     * subtree has to hold emissions back until it lands, and holding them back is what append-only forbids.
     */
    APPEND_MODE_CONFLICTS_WITH_KEY_TRACKING(
            "nest.append-mode-conflicts-with-key-tracking", Set.of("rootAlias", "embedPath")),

    /** Checking the tree: a nest may not pass its snapshot reads straight to the sink, unassembled. */
    SNAPSHOT_PASSTHROUGH_FORBIDDEN("nest.snapshot-passthrough-forbidden", Set.of("rootCollection")),

    /**
     * Starting up: the memory budget a namespace was given is smaller than the partitions it is spent
     * across, so what is held is the partition count rather than the number that was asked for.
     */
    MEMORY_BUDGET_BELOW_PARTITION_COUNT(
            "nest.memory-budget-below-partition-count", Set.of("entries", "partitions")),

    /**
     * Starting up: this pipeline asks its levels to hold a different number than they were already
     * configured to hold on this process. What a map holds is settled once, the first time a name is
     * configured, and stays settled for as long as the process runs - so the new number cannot take
     * effect here however the pipeline is restarted.
     *
     * <p>Said rather than swallowed because the substrate's own answer is to keep the number it already
     * had. Swallowed, the pipeline would run on the earlier budget while the artifact it was started from
     * reads as the later one, which is the exact shape of "configured, and not in effect".
     */
    MEMORY_BUDGET_CHANGED_WHILE_RUNNING(
            "nest.memory-budget-changed-while-running",
            Set.of("namespace", "configured", "requested")),

    /**
     * Starting up: the tree keeps its state under names it no longer compiles to, because an embed path
     * was renamed or a level inserted. What was stored is left where nothing reads it and the new names
     * answer nothing, so the run would rebuild from empty while reporting that it resumed.
     */
    STATE_PATHS_CHANGED("nest.state-paths-changed", Set.of("stepId", "recorded", "compiled")),

    /**
     * Running: one document has grown past the elements a single document may hold. The one limit on what
     * a level holds that is a limit at all: a document is rendered whole, so however much it has absorbed
     * has to be in memory at once and no eviction reaches inside one. How many documents there are, and how
     * many keys a level below holds, are bounded by what stays in memory rather than by a count.
     */
    ROOT_FANOUT_LIMIT_EXCEEDED("nest.root-fanout-limit-exceeded", Set.of("rootKey", "elements", "limit")),

    /** Running: a subtree being moved to another document has parked more than the limit allows. */
    MIGRATION_PARKING_LIMIT_EXCEEDED(
            "nest.migration-parking-limit-exceeded", Set.of("newRootKey", "bytes", "limit")),

    /**
     * Running: an event held for a parent that never resolved has been released to the dead-letter
     * channel so the frontier can move again. The pipeline keeps running, which is why this is a warning:
     * the verdict says whether the parent was established absent or the wall-clock backstop gave up on it,
     * and those mean very different things to whoever reads it.
     */
    PENDING_PROTECTION_EXPIRED("nest.pending-protection-expired",
            Set.of("chain", "bucket", "order", "verdict", "maxAge"), Severity.WARNING),

    /**
     * Running: a stream tracks structural key changes but its source does not provide a before image, so
     * a key change cannot be told from an ordinary update and the document would silently diverge.
     */
    KEY_CHANGE_TRACKING_REQUIRES_BEFORE_IMAGE(
            "nest.key-change-tracking-requires-before-image", Set.of("alias", "table"));

    private final String code;
    private final Set<String> placeholders;
    private final Severity severity;

    NestError(String code, Set<String> placeholders) {
        this(code, placeholders, Severity.ERROR);
    }

    NestError(String code, Set<String> placeholders, Severity severity) {
        this.code = code;
        this.placeholders = placeholders;
        this.severity = severity;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return severity;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
