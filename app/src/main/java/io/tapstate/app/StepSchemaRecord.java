package io.tapstate.app;

import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.DerivedSchemaStore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Files what one transform step works its own columns out to be, beside the pipeline that step belongs
 * to.
 *
 * <p><b>A node here is whatever the author wrote that reshapes a row</b> - a transform step's body,
 * or the view block beside them. They are recorded through one class because they are one question,
 * and the record has one shape whichever node answered it.
 *
 * <p><b>Every node, not only the ones that read and the ones that join.</b> A source node's shape is
 * filed by the copy and a join step's by the drift check, which between them cover two of the node
 * kinds; a pipeline built out of the others had a recorded model for what it reads and none for
 * anything it does to it. A reader asking what a pipeline produces at a step then gets the same answer
 * for a step whose model was never derived and for one that could not be derived.
 *
 * <p><b>A step nobody can describe records nothing.</b> Not an empty schema: an empty record is still a
 * record, and the next comparison takes it as the baseline - so the first describable derivation would
 * report every column as newly appeared, an alarm produced here and shaped exactly like a step that
 * moved. The same reading the source copy takes of a table nothing has discovered.
 *
 * <p><b>A node that cannot answer carries the shape that reached it, and says so.</b> A script settles
 * its own columns while it runs, so nothing can work out what it emits - and left at that, the record
 * stops at the script and stops for everything below it, because each step below derives from an
 * unknown and is unknown itself. One script would blank the rest of the pipeline's model, which is a
 * worse answer than an assumption stated out loud. So the shape that reached it is carried on, and the
 * row is written down as carried rather than derived: the columns below a script rest on something the
 * script can break, and a reader has to be able to see the row where that entered. Nothing compares
 * this field, so naming it costs nothing and hiding it would cost the next reader the whole chain.
 */
final class StepSchemaRecord {

    /**
     * What worked the columns out. Informational, like its counterparts on the source and join sides -
     * "did the derivation change" is answered by the columns moving while its inputs did not.
     */
    private static final String DERIVED_BY = "step-derivation-1";

    /**
     * What a row worked out by nobody says instead. Its own word rather than a flag on the derivation's,
     * because the two are different claims: one says these are the columns this node produces, the other
     * says these are the columns that reached it and nothing here knows what it does to them.
     */
    private static final String CARRIED_THROUGH = "carried-through-1";

    private final DerivedSchemaStore records;

    StepSchemaRecord(DerivedSchemaStore records) {
        this.records = Objects.requireNonNull(records, "records");
    }

    /**
     * Records what one step derived, answering whether it recorded anything. An unknown model - a
     * script, or any step downstream of one - records nothing and is not an error: a script settling
     * its own columns while it runs is an ordinary pipeline, and the answer is what keeps a caller from
     * pinning a step with no history to pin into.
     */
    boolean record(String pipelineId, String nodeId, NodeColumns derived,
            Map<String, NodeColumns> inputs, Object authored, boolean carried) {
        if (!derived.known()) {
            return false;
        }
        records.record(pipelineId, nodeId, derived.columns(), fingerprintOf(authored),
                fingerprintOf(inputs), carried ? CARRIED_THROUGH : DERIVED_BY);
        return true;
    }

    /**
     * A fingerprint of what the author wrote: this node's own text, and nothing else of the pipeline.
     * Keyed on the step alone deliberately - hashing the whole artifact instead would make an edit to
     * one step read as an edit to every other, which is the confusion the two provenance fingerprints
     * exist to prevent.
     *
     * <p><b>It is the body's own rendering, which moves if a component of it is ever renamed.</b> Said
     * out loud rather than left to be discovered: the history is versioned by the schema alone, so a
     * rename moves no version and raises nothing - it leaves one recorded statement stale until that
     * step's columns next change. What it would cost is a drift check on these steps, which does not
     * exist yet: such a check reads a moved statement as "the author edited this", so the first
     * assembly after a rename would let one genuine difference through as an ordinary edit.
     */
    private static String fingerprintOf(Object authored) {
        return ContentHash.of(String.valueOf(authored).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * A fingerprint of what the derivation read from the world: every column reaching this step, by the
     * reference it arrived on, by name and by declared type.
     *
     * <p>Sorted, for the reason the source copy sorts: a stream rebuilt with its columns in another
     * order must not move a fingerprint that nothing about the shape moved.
     */
    private static String fingerprintOf(Map<String, NodeColumns> inputs) {
        List<String> lines = new ArrayList<>();
        inputs.forEach((reference, columns) ->
                columns.columns().forEach((name, declared) ->
                        lines.add(reference + ':' + name + ':' + declared)));
        lines.sort(null);
        return ContentHash.of(String.join(";", lines).getBytes(StandardCharsets.UTF_8));
    }
}
