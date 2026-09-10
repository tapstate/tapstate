package io.tapstate.adapters.transform;

import io.tapstate.core.model.TransformBody;
import java.io.Serializable;
import java.util.List;

/**
 * The serializable shape of an expansion: which field holds the list, what each element is told
 * apart by, and what the rows arriving here are already keyed on. Like {@link MapSpec} it is what
 * the app assembly root captures in the Jet supplier, so it holds nothing but self-contained values
 * a member rebuilds the port from.
 *
 * <p><b>{@code parentKey} is handed in rather than worked out here.</b> What a node's rows are keyed
 * on is a property of the whole chain above it - a source's declared key, or whatever an earlier
 * expansion made of it - and only the assembly root has the derivation to answer that. A port that
 * guessed would be a second answer to a question the check and the target's own key are already
 * answering, and two of those disagreeing is not a failure anybody sees: the rows land, the target
 * fills, and the ones that overwrote each other are not counted anywhere.
 *
 * <p>The element's own locator is <em>not</em> in that list, because it is not a column of the row
 * arriving here - it is either a field inside the element or the ordinal this expansion invents, and
 * both are things only the expansion can read. So the whole key of an output row is this list plus
 * whatever {@link #elementKey()} or {@link #includeArrayIndex()} yields for that element.
 */
public final class UnwindSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String path;
    private final String includeArrayIndex;
    private final boolean preserveNullAndEmptyArrays;
    private final String elementKey;
    private final List<String> parentKey;

    private UnwindSpec(String path, String includeArrayIndex, boolean preserveNullAndEmptyArrays,
            String elementKey, List<String> parentKey) {
        this.path = path;
        this.includeArrayIndex = includeArrayIndex;
        this.preserveNullAndEmptyArrays = preserveNullAndEmptyArrays;
        this.elementKey = elementKey;
        this.parentKey = parentKey;
    }

    /**
     * Translates a parsed expansion plus the key of the rows reaching it into the port's shape.
     * {@code elementType} is not carried: it describes the column a target declares, which nothing
     * on this side of the pipeline reads.
     */
    public static UnwindSpec from(TransformBody.Unwind unwind, List<String> parentKey) {
        return new UnwindSpec(unwind.path(), unwind.includeArrayIndex(),
                Boolean.TRUE.equals(unwind.preserveNullAndEmptyArrays()),
                unwind.elementKey(), List.copyOf(parentKey));
    }

    /** The field holding the list. */
    String path() {
        return path;
    }

    /** The column each element's position is carried as, or null where none was asked for. */
    String includeArrayIndex() {
        return includeArrayIndex;
    }

    /** Whether a row whose list is empty, null or absent still produces one row. */
    boolean preserveNullAndEmptyArrays() {
        return preserveNullAndEmptyArrays;
    }

    /** The field inside an element identifying the row it becomes, or null where none was named. */
    String elementKey() {
        return elementKey;
    }

    /** What the rows arriving here are keyed on, in order. */
    List<String> parentKey() {
        return parentKey;
    }
}
