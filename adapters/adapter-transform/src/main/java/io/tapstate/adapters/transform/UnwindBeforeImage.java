package io.tapstate.adapters.transform;

import io.tapstate.core.event.Envelope;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** The shared image check, also applied before projections feeding an expansion. */
final class UnwindBeforeImage {
    private UnwindBeforeImage() {
    }

    /**
     * Refuses an update or a delete whose earlier row is not a whole row.
     *
     * <p>The test is what the row carries, never whether it has the expanded column. A whole
     * document that simply has no such column is legitimate - an optional field in a document store
     * is ordinary, and a read of the same row must accept it and produce nothing - so testing for
     * the column would stop a pipeline on exactly the row a snapshot is required to let through.
     * Half a row is instead recognisable by carrying the columns that identify it and nothing else,
     * which is precisely what a change stream with no pre-image and a relational source under its
     * default replica identity each send.
     */
    static void require(Envelope event, String path, List<String> parentKey) {
        Map<String, Object> was = event.before();
        if (was == null) {
            throw TransformErrors.unwindNeedsACompleteBeforeImage(
                    path, "the event carries no earlier row at all");
        }
        List<String> missing = new ArrayList<>();
        for (String column : parentKey) {
            if (!was.containsKey(column)) {
                missing.add(column);
            }
        }
        if (!missing.isEmpty()) {
            throw TransformErrors.unwindNeedsACompleteBeforeImage(path,
                    "the earlier row does not carry " + String.join(", ", missing));
        }
        if (was.size() <= parentKey.size()) {
            throw TransformErrors.unwindNeedsACompleteBeforeImage(path,
                    "the earlier row carries only the columns identifying it ("
                            + String.join(", ", parentKey) + ")");
        }
    }
}
