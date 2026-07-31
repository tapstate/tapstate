package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;

import java.util.List;
import java.util.Optional;

/**
 * How one tier reaches the product under test. The same specification runs on every binding, so
 * this is the whole fidelity axis: an in-process binding boots the product inside this JVM, a
 * real-process binding drives a shipped artifact. Nothing above this interface knows which.
 *
 * <p>Readings are deliberately taken from outside the product: {@link #count} reads the target
 * endpoint itself rather than any in-process record of what was written, so a specification asserts
 * what a user would see.
 */
public interface TierBinding {

    /** Registers a connector's runtime jar; idempotent by content hash. */
    void registerConnector(String connectorId);

    /**
     * Applies product resource files, by path relative to the specification, as one batch.
     *
     * <p>The batch is deliberate, not a convenience: the product resolves references within the set
     * submitted together, so a pipeline and the source it names by id must arrive in the same apply
     * or the reference points at nothing. Applying them one at a time would fail on the product's own
     * contract, so the seam takes the list the specification wrote.
     */
    void applyResources(List<String> resourceFiles);

    /** Discovers and persists a source model, feeding target-table creation. */
    void discoverSchema(String resourceId);

    /** Lays down initial rows on a table before the run begins. */
    void seed(TableAlias table, long rows);

    /** Records a lifecycle intent. Returns once the intent is recorded, not once it converges. */
    void drive(String pipelineId, LifecycleVerb verb);

    /** Produces changes against a table while the pipeline runs. */
    void cdc(TableAlias table, CdcOp op, long rows);

    /**
     * Re-emits a table's current rows as fresh change events, row keys unchanged.
     *
     * <p>This exists for one seam: a change written to a real source right after its change stream is
     * asked for can land before the stream is positioned, and is then never delivered - nothing the
     * product publishes says when the stream is ready, so no await can be written against readiness
     * itself. Redelivery re-asserts the table's current state row-wise instead; a batch that was lost
     * is re-emitted and one that was merely slow arrives twice under the same keys, which an upserting
     * target absorbs. Deletions already delivered are not compensated: redelivery only re-emits rows
     * that still exist.
     */
    void redeliver(TableAlias table);

    /** Reads the current row count from the endpoint that owns the table. */
    long count(TableAlias table);

    /**
     * Reads the published lifecycle state of a pipeline, or empty when it has published none yet.
     *
     * <p>Empty is a reading and not an error: a pipeline is unobserved until a convergence pass publishes
     * it, so "nothing yet" is the honest answer for that window and a wait is entitled to sit through it.
     */
    Optional<PipelineState> state(String pipelineId);

    /**
     * Reads the published error count of a pipeline from its metrics face, or empty when it has published
     * no observation yet - the same unobserved window {@link #state} sits through, answered the same way.
     */
    Optional<Long> errorCount(String pipelineId);
}
