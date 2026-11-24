package io.tapstate.core.dsl;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.NestSizingErrorDefinitions;
import io.tapstate.core.common.TapstateErrorCode;

import java.util.Set;

/**
 * The {@code nest} domain's authoring-time capacity codes: what sizing a nest against the memory it
 * will run on has to say to the author writing it. They are warnings — a batch carrying one is applied,
 * and the pipeline it describes runs.
 *
 * <p>They live apart from the domain's runtime codes because the ring that judges a batch cannot reach
 * the ring that runs one, the same split the {@code connector} domain already carries between loading a
 * connector and cataloguing one. The domain is shared, so the codes are still one namespace and still
 * globally unique.
 *
 * <p>{@code placeholders()} is the named-argument contract: every raise site supplies a value for each
 * name, and the build-time placeholder gate checks the catalog templates against it.
 */
public enum NestSizingError implements TapstateErrorCode {

    /**
     * A level of a nest is estimated to hold so much more than its in-memory budget that nearly every
     * read of its state would come from storage instead. {@code embedPath} is the path the author wrote
     * the level at, {@code estimatedEntries} how many entries its tables would put in it,
     * {@code budget} how many it keeps in memory, and {@code multiple} the ratio that crossed the line.
     */
    STATE_FAR_EXCEEDS_MEMORY_BUDGET(NestSizingErrorDefinitions.STATE_FAR_EXCEEDS_MEMORY_BUDGET),

    /**
     * A level could be sized from only some of the tables that feed it, because the rest report no row
     * count. {@code counted} of {@code total} is how much of the level the estimate could see; what it
     * did see is a floor rather than an answer.
     */
    CAPACITY_ESTIMATE_INCOMPLETE(NestSizingErrorDefinitions.CAPACITY_ESTIMATE_INCOMPLETE),

    /**
     * A level carrying rollup fields is estimated to make its documents heavier than a rollup keeps up with:
     * each change rewrites the document's state and refolds the level, so its cost grows with the level's
     * elements times their width. {@code estimatedElements} is the level's rows over its parent's, an average a
     * skewed level exceeds for some documents; {@code estimatedBytes} the state those elements take; {@code
     * boundary} how many elements of this width a document should hold.
     */
    ROLLUP_LEVEL_WIDE(NestSizingErrorDefinitions.ROLLUP_LEVEL_WIDE);


    private final ProductErrorDefinition definition;

    NestSizingError(ProductErrorDefinition definition) { this.definition = definition; }

    @Override public String code() { return definition.code(); }
    @Override public Severity severity() { return definition.severity(); }
    @Override public Set<String> placeholders() { return definition.parameters(); }
}
