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

    /**
     * Checking the tree: an embed joins onto a column that does not identify its parent, so many parent rows
     * share one identity and the rows beneath collapse into whichever of them got there. Sibling embeds catch
     * this by disagreeing with each other; an only child has nobody to disagree with, so the column it names
     * is taken as the level's identity however wrong it is.
     */
    EMBED_TARGET_NOT_PARENT_KEY(
            "nest.embed-target-not-parent-key", Set.of("embedPath", "fields", "parentKey")),

    /** Checking the tree: two embeds under one parent claim the same path, where one would overwrite the other. */
    EMBED_PATH_CONFLICT("nest.embed-path-conflict", Set.of("path", "embedPathA", "embedPathB")),

    /**
     * Checking the tree: the root declares no key, so its documents have no identity for children to be
     * grouped under and nothing to partition the assembled documents by.
     */
    ROOT_KEY_REQUIRED("nest.root-key-required", Set.of("rootAlias")),

    /**
     * Checking the tree: a level declares no key and its stream offers neither a primary key nor a unique
     * index to take one from, leaving its rows with no identity — updates would pile up as duplicates.
     */
    ARRAY_KEY_UNRESOLVABLE("nest.array-key-unresolvable", Set.of("embedPath", "table")),

    /**
     * Checking the tree: a level declares no key and its table offers no primary key but more than one
     * unique index. Each identifies a row equally well, so taking either would settle the identity of the
     * level on catalog order rather than on anything the author said.
     */
    KEY_AMBIGUOUS("nest.key-ambiguous", Set.of("embedPath", "table", "candidates")),

    /**
     * Checking the tree: a level the document points at has embeds hanging beneath it. A row that is pointed
     * at belongs to no one document — the same row can sit under thousands at once — so there is no document
     * for its children to be grouped under and no answer to which of them a change beneath it should reach.
     * Refused rather than assembled: the two shapes are written identically, and left to compile the rows
     * beneath would be gathered into state nothing renders from, with every document still going out.
     */
    REFERENCED_LEVEL_CARRIES_EMBEDS("nest.referenced-level-carries-embeds",
            Set.of("embedPath", "table", "children")),

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
     * Running: one document has grown past the elements a single document may hold. One of the two counts
     * that bound what is inside an entry rather than how many there are: a document is rendered whole, so
     * however much it has absorbed has to be in memory at once and no eviction reaches inside one. How many
     * documents there are, and how many keys a level below holds, are bounded by what stays in memory rather
     * than by a count.
     */
    ROOT_FANOUT_LIMIT_EXCEEDED("nest.root-fanout-limit-exceeded", Set.of("rootKey", "elements", "limit")),

    /**
     * Running: one key is holding more changes for something that has not arrived than it is allowed to.
     * The second limit that bounds memory, and the one no other reaches: what waits lives inside a single
     * entry, so a budget counting entries is met however long one key's queue has grown, and a limit on the
     * width of a document counts elements where this counts every change held against them.
     *
     * <p>Failed rather than let go of. Reaching this says only that much arrived before the parent did,
     * never that the parent is absent, so releasing on it would drop rows that were going to be part of a
     * document - which is what the wait's own three-layer judgement exists to avoid deciding without
     * evidence.
     */
    PENDING_LIMIT_EXCEEDED("nest.pending-limit-exceeded",
            Set.of("namespace", "key", "pending", "limit")),

    /**
     * Running: one document is keeping the record of more deleted elements than it is allowed to, and they
     * cannot be dropped yet. The third limit bounding what lives inside one entry, and the one whose count
     * does not follow the shape of the data: a record of a deletion is kept until a replay can no longer undo
     * it, so what governs how many there are is how far behind the durable frontier is.
     *
     * <p>Reported only once what may be dropped has been, so reaching it says the frontier is not moving
     * rather than that the work is heavy. Failed rather than dropped: a record dropped early is a row the
     * source deleted coming back at the next restart, which no assertion about the document would catch.
     */
    TOMBSTONE_LIMIT_EXCEEDED("nest.tombstone-limit-exceeded",
            Set.of("namespace", "key", "tombstones", "limit")),

    /**
     * Running: rows on their way between two documents have parked more than the limit allows. The fourth
     * count that fails a run rather than being absorbed, and the only one over rows that are in no document
     * at all - so nothing about the documents either side bounds it, and the frontier is held below the
     * change that started the move for as long as they sit there.
     *
     * <p>Counted in changes rather than in bytes: a size would have to be arrived at by serializing the rows
     * on every hand-over, which is the one cost the state layer is built to avoid, and every other limit
     * here is a count an operator can weigh against the width they already allowed.
     */
    MIGRATION_PARKING_LIMIT_EXCEEDED(
            "nest.migration-parking-limit-exceeded", Set.of("address", "changes", "limit")),

    /**
     * Running: an event can never be placed in a document, because the row it hangs from is known to be
     * gone, and has gone to the dead-letter channel instead. The pipeline keeps running, which is why this
     * is a warning: a dangling reference in the source is ordinary, and the event was never going to appear
     * in any document. How long it had been waiting is what says which of the two this is - a reference
     * that has dangled all along, or a parent deleted while its children were in flight.
     */
    PARENT_ABSENT("nest.parent-absent",
            Set.of("chain", "bucket", "order", "heldFor"), Severity.WARNING),

    /**
     * Running: a namespace's state is being served from the storage behind its memory rather than from the
     * memory, so what used to be a lookup is now a round trip on most events. A warning because nothing is
     * wrong in the sense that anything failed — the pipeline runs, the queues are short and the throughput
     * is whatever the storage can sustain — and that is exactly why it is said out loud: per-key state is a
     * buffer backpressure cannot see, so no other statistic moves when this happens.
     */
    STATE_SERVED_FROM_COLD_LAYER("nest.state-served-from-cold-layer",
            Set.of("namespace", "backfills", "accesses", "share", "millis", "entries", "stored", "resident"),
            Severity.WARNING),

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
