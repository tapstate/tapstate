package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;

import java.util.List;
import java.util.Map;
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

    /**
     * Learns what the same resource files declare, without applying them.
     *
     * <p>Separate from the apply because two things now need a source's address before the product has
     * been told anything: the harness seeds the table over its own driver, and a discovery has to be
     * asked for before the apply rather than after it. The declaration is the same either way - this
     * reads it, the apply submits it.
     */
    void readResources(List<String> resourceFiles);

    /** Discovers and persists a source model, feeding target-table creation. */
    void discoverSchema(String resourceId);

    /** Lays down initial rows on a table before the run begins, one mapping per row. */
    void seed(TableAlias table, List<Map<String, Object>> rows);

    /**
     * Reads the one document the equality settings locate at the endpoint that owns the table, or
     * empty when none matches. Read from outside the product like {@link #count}, and in the
     * specification's own spelling - identity is {@code id} whatever the store calls it.
     */
    Optional<Map<String, Object>> fetch(TableAlias table, Map<String, Object> where);

    /** Records a lifecycle intent. Returns once the intent is recorded, not once it converges. */
    void drive(String pipelineId, LifecycleVerb verb);

    /**
     * Holds or releases one source's stream, leaving the pipeline and its other streams alone.
     *
     * <p>Not a lifecycle intent and not sent to the product at all: a job is suspended whole, so there
     * is no product verb that could mean this. The harness holds the stream where it owns the ground -
     * between the source and whatever is reading it - which is why the source keeps accepting writes
     * while it is held, and why releasing delivers them in the order the source recorded them rather
     * than the order they were held in. That is the one arrangement a specification cannot reach by
     * waiting: an arrival order that disagrees with the source order.
     *
     * <p>Holding is a state. Holding a held stream is nothing, releasing a running one is nothing, and
     * a stream left held when a run ends is released with the run.
     */
    void driveStream(String sourceId, StreamVerb verb);

    /**
     * Cycles the pipeline the way the terminal's {@code restart} does, and says which of its two
     * forms this is: {@code rereadEverything} is the answer its stop carries, and it is the whole
     * difference between carrying on and reading the source again.
     *
     * <p>The expansion lives in the binding rather than in the executor because it is the product's,
     * and a tier that ever offers the word directly should be free to send it as one call.
     */
    void restart(String pipelineId, boolean rereadEverything);

    /** Produces changes against a table while the pipeline runs. */
    void cdc(TableAlias table, CdcOp op, long rows);

    /**
     * Sets columns on the one row the settings locate, while the pipeline runs.
     *
     * <p>Separate from {@link #cdc} because the two answer different questions. A generated change
     * moves some rows and satisfies a case about a count arriving; this one moves a row the
     * specification chose and writes a value the specification chose, which is what a case has to do
     * before it can read that value back out of an assembled document. Locating is spelled the way
     * {@link #fetch} spells it - identity is {@code id} whatever the store calls it.
     */
    void update(TableAlias table, Map<String, Object> where, Map<String, Object> set);

    /** Removes the one row the settings locate, while the pipeline runs. Located like {@link #fetch}. */
    void delete(TableAlias table, Map<String, Object> where);

    /** Adds the given rows while the pipeline runs, leaving what the table held alone. */
    void insert(TableAlias table, List<Map<String, Object>> rows);

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

    /**
     * Reads the canonical code of the failure a pipeline has published, from its status face, or empty when
     * it has published none - the pipeline is healthy, or no convergence pass has observed it yet. Empty is
     * a reading like the two above, not an error.
     */
    Optional<String> failureCode(String pipelineId);

    /**
     * Reads how many changes a pipeline's nests could never place in a document, added up over its
     * namespaces, from its metrics face; empty when it has published no observation yet, on the same terms
     * as the readings above. A pipeline that discarded nothing answers zero rather than empty - the metric
     * is published only where rows were lost, so no entry is the healthy answer rather than an unmeasured
     * one, and that is the one place this reading's emptiness differs from the others'.
     */
    Optional<Long> deadLettered(String pipelineId);
}
