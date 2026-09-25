package io.tapstate.runtime.engine;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;

import java.util.Set;

/**
 * The {@code engine} domain's error codes: user-facing, diagnosable failures of running a pipeline —
 * both of a job operation on it (submit / suspend / resume / cancel) and of the data plane the job
 * carries, such as an ordering value the engine's own frontier transport cannot encode. Distinct from
 * the {@code lifecycle} domain, which polices whether a transition is legal before the engine is ever
 * touched. These are converge-side execution failures carried through the error-code system and
 * rendered through the shared message catalog.
 *
 * <p>{@code placeholders()} is the named-argument contract: every throw site supplies a value for
 * each name, and the build-time placeholder gate checks the catalog templates against it.
 */
public enum EngineError implements TapstateErrorCode {

    /**
     * A suspend / resume named a pipeline that has no running job to act on: {@code pipeline} is the
     * id the caller gave.
     */
    NO_SUCH_JOB("engine.no-such-job", Set.of("pipeline")),

    /**
     * A change's ordering value is outside the range the frontier transport encodes it in: {@code chain}
     * is the chain it belongs to, {@code epoch} and {@code seq} the generation and position that did not
     * fit. A capacity line of the transport rather than a defect — the widths are a build-time choice.
     */
    FRONTIER_ORDER_NOT_ENCODABLE("engine.frontier-order-not-encodable",
            Set.of("chain", "epoch", "seq")),

    /**
     * A member was asked to act for a run it can no longer prove is the current one: {@code pipeline} is
     * the pipeline whose run it was carrying. Either the pipeline changed hands, or a newer run of it was
     * submitted, or this member could not reach the coordination store to check within its local window.
     *
     * <p>Not a defect and not the pipeline's fault — it is this member standing down. Refusing is the
     * whole point: the batch that provoked it is not written, so nothing outside the cluster is touched
     * twice by two runs of the same pipeline.
     */
    EXECUTION_NOT_AUTHORIZED("engine.execution-not-authorized", Set.of("pipeline")),

    /**
     * A run could not be started because the previous run of the same pipeline was still ending:
     * {@code pipeline} is the pipeline, {@code seconds} how long the wait for the old job to be over
     * lasted before giving up.
     *
     * <p>Coded rather than silent because the engine's own answer here is silence: asked for a job under
     * a name whose previous job has not finished ending, it hands that dying job back and starts nothing,
     * without throwing. A caller taking that as a start would leave the pipeline reporting a run it does
     * not have.
     */
    JOB_STILL_ENDING("engine.job-still-ending", Set.of("pipeline", "seconds")),

    /**
     * Running: a view selected a discovered alternate identity, but an update or delete reached its
     * sink without that key in the earlier row. {@code view} and {@code key} name the materialization
     * and selected identity, while {@code operation} names the change that cannot be applied safely.
     * Without the old key, a delete cannot remove the materialized row and a key-changing update
     * cannot remove the row under its previous identity, leaving stale data behind.
     */
    VIEW_KEY_MISSING_FROM_BEFORE_IMAGE("engine.view-key-missing-from-before-image",
            Set.of("view", "key", "operation")),

    /**
     * A pipeline's data-plane job died on its own, for a reason the product had not already coded at its
     * throw site: {@code pipeline} is the pipeline whose run died and {@code cause} is what it died of.
     * A fault that does carry its own code keeps that code instead — this is the last resort, so that a
     * dead job always names a reason rather than reporting a bare state change.
     */
    JOB_FAILED("engine.job-failed", Set.of("pipeline", "cause")),

    /**
     * Running: a chain's durable position has stopped moving for long enough to be worth saying so, while
     * nothing else about the pipeline has changed. {@code chain} is the chain, {@code minutes} how long it
     * has been pinned, {@code gap} how far the bound combined for it has run on meanwhile and
     * {@code cause} which of the two pins it is — held back by changes still pending upstream, or starved
     * of positions to advance to. The two are worked on from opposite ends, which is why the reading that
     * tells them apart is carried rather than only the duration.
     *
     * <p>A warning because nothing failed: the pipeline runs, its queues are short and its error count is
     * zero. That is exactly why it is said out loud — what a pinned position consumes is the source's log
     * retention, and when that rotates past it the pipeline has still not failed, it has merely lost the
     * ability to resume, taking every pipeline mining the same chain with it.
     */
    FRONTIER_PINNED("engine.frontier-pinned",
            Set.of("chain", "minutes", "gap", "cause"), Severity.WARNING),

    /**
     * Running: a second row of a joined source arrived under a join key another row was already filed
     * under, so that earlier row has been replaced and is now unreachable. {@code source} is the source
     * the join calls that side by and {@code key} is the key both rows share.
     *
     * <p>A warning because nothing failed and nothing is repaired here. One key holds one row, so every
     * fact row under it joins to whichever arrived last and the target ends up shorter than the query
     * describes - holding both rows instead would move dimension state, output cardinality and row
     * identity together, which is a far larger change than this. What this removes is the third answer a
     * query must not be given: accepted, wrong, and silent. A target quietly short of rows is
     * indistinguishable from a correct one and every row it does hold looks entirely ordinary, so the
     * moment of replacement is the only place anything can observe it.
     */
    JOIN_DIMENSION_ROW_DISPLACED("engine.join-dimension-row-displaced",
            Set.of("source", "key"), Severity.WARNING),

    /**
     * A change reached a node running on several processors without the key it is routed by: a key column
     * is absent from the image the change carries, or holds nothing. {@code node} is the node, {@code stream}
     * the stream the change travelled on and {@code columns} the key it was expected to carry. There is no
     * processor such a change could be sent to that keeps it in order with the other changes of its row.
     */
    ROUTING_KEY_MISSING("engine.routing-key-missing", Set.of("node", "stream", "columns")),

    /**
     * An update moved a row from one key to another at a node running on several processors. {@code node} is
     * the node and {@code stream} the stream. The change belongs to two keys, and the two keys are processed
     * on different processors, so no single place to send it keeps it in order with both - and applying it
     * out of order leaves a row behind under the old key or overwrites a newer value under the new one.
     */
    KEY_CHANGE_ON_PARALLEL_NODE("engine.key-change-on-parallel-node", Set.of("node", "stream")),

    /**
     * A run started on a different number of members than it was planned for: a member joined or left
     * between the plan and the start. {@code pipeline} is the pipeline, {@code planned} the members the widths
     * were worked out for and {@code actual} the members the run started on. Stopped before any processor
     * ran, because every processor count and every set of writers worked out for the plan would be wrong.
     */
    MEMBERSHIP_CHANGED_BEFORE_START("engine.membership-changed-before-start",
            Set.of("pipeline", "planned", "actual"));

    private final String code;
    private final Set<String> placeholders;
    private final Severity severity;

    EngineError(String code, Set<String> placeholders) {
        this(code, placeholders, Severity.ERROR);
    }

    EngineError(String code, Set<String> placeholders, Severity severity) {
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
