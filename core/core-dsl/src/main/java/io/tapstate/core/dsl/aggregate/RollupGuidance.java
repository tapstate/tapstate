package io.tapstate.core.dsl.aggregate;

/**
 * How wide a rollup level can be before its documents fall behind their changes, said once so the warning given
 * when a pipeline is applied and the one given while it runs agree.
 *
 * <p>Every change to a document rewrites the document's stored state and refolds its rollup levels, so what a
 * change costs grows with how many bytes the document holds - its elements times how wide each one is - not
 * with the element count alone. Measured on a 2-CPU engine against one shared MongoDB, one document kept up with
 * tens of changes a second while it held under about a megabyte of state (a thousand five-column elements kept
 * up with fifty a second; five thousand thirty-column elements with twenty-five but not fifty; ten thousand
 * five-column elements with neither). A level's state per element was measured at about
 * {@value #BYTES_PER_ELEMENT_BASE} bytes plus {@value #BYTES_PER_COLUMN} per column.
 */
public final class RollupGuidance {

    /** The state one document should hold for its rollups to keep up with its changes. */
    public static final long STATE_BYTES_PER_DOCUMENT = 1_000_000L;

    static final long BYTES_PER_ELEMENT_BASE = 100L;
    static final long BYTES_PER_COLUMN = 25L;

    /** The width assumed for a level whose columns nobody discovered: five columns, the narrow end measured. */
    static final int UNKNOWN_WIDTH_COLUMNS = 8;

    private RollupGuidance() {
    }

    /** About how many bytes of state one element with {@code columns} columns takes. */
    public static long bytesPerElement(int columns) {
        int width = columns > 0 ? columns : UNKNOWN_WIDTH_COLUMNS;
        return BYTES_PER_ELEMENT_BASE + BYTES_PER_COLUMN * width;
    }

    /** The widest a rollup level of {@code columns}-column elements should be, in elements per document. */
    public static long elementsPerDocument(int columns) {
        return STATE_BYTES_PER_DOCUMENT / bytesPerElement(columns);
    }
}
