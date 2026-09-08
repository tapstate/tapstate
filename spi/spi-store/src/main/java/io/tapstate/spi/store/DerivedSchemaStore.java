package io.tapstate.spi.store;

import java.util.Map;
import java.util.Optional;

/**
 * The side record of what a pipeline step works its own columns out to be, keyed by the pipeline and
 * the step within it. A pure interface (rule R2); a store backend persists it.
 *
 * <p><b>Why this is kept at all.</b> A step that derives its output columns rather than being told
 * them — a join works them out from its SQL and the columns of the tables it reads — has an output
 * schema that is a function of two things that both move on their own. Nothing else records what that
 * function last answered, so without this record a start has nothing to compare today's answer against
 * and a schema that has changed under a pipeline nobody edited reaches the target as ordinary writes.
 *
 * <p><b>Beside the artifact, never inside it.</b> A derived value put into the canonical bytes would
 * make every stored artifact re-hash the moment the derivation changed, so a pipeline whose author
 * changed nothing would read as edited. This record is keyed by the same pipeline id and is nobody's
 * input to a hash.
 *
 * <p><b>The history is append-only and versioned by the schema alone.</b> {@link #record} appends a
 * version when the columns differ from the last one and only refreshes the provenance when they do
 * not — so the version count answers "how many times has this step's shape changed", which is the
 * question the record exists for, rather than "how many times has it started". Refreshing provenance
 * on an unchanged schema is not bookkeeping tidiness: it is what keeps the next difference
 * attributable. A source that moves without moving the output leaves the recorded provenance stale,
 * and a later difference would then be read as the sources having changed when it was the derivation.
 *
 * <p><b>The provenance is two fingerprints, not one.</b> What the author wrote and what the derivation
 * read from the world both move, and only one of them is the author's doing. Folded into a single
 * fingerprint, an ordinary edit to a query would be indistinguishable from the sources changing under
 * one nobody touched — and those two want opposite reactions.
 *
 * <p><b>Recording a differing schema is an explicit act.</b> Nothing here refuses one — a store that
 * silently absorbed a changed schema would leave the difference undetectable by the time anyone
 * looked, so the caller compares first and only records what it means to accept.
 */
public interface DerivedSchemaStore {

    /** The latest recorded derivation for a step, or empty where the step has never recorded one. */
    Optional<DerivedSchema> latest(String pipelineId, String stepId);

    /**
     * Records a derivation for a step. Appends a new version when {@code schema} differs from the
     * latest recorded one; when it is the same, updates that version's {@code derivedFrom} and
     * {@code derivedBy} in place and appends nothing.
     *
     * @param schema      the derived columns, name to declared type, in output order
     * @param statement   a fingerprint of what the author wrote
     * @param derivedFrom a fingerprint of what the derivation read from the world
     * @param derivedBy   the version of the derivation itself
     */
    void record(String pipelineId, String stepId, Map<String, String> schema, String statement,
            String derivedFrom, String derivedBy);

    /**
     * Removes every recorded derivation for a pipeline, whichever steps carry one. Idempotent: a
     * pipeline with nothing recorded is not an error. Called when a pipeline is removed — a record
     * left behind would be read as the derivation history of whatever is applied under that id next,
     * and would refuse to start it over a difference against a schema belonging to something else.
     */
    void delete(String pipelineId);
}
