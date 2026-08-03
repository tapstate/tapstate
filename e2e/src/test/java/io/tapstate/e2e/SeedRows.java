package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The generated row shape, in one place: ids are the whole numbers 1..N, each carrying a sequence
 * equal to its id. {@code rows: N} in a specification means exactly these rows, and a test seeding a
 * store directly asks here rather than restating the shape - what a published example may depend on
 * (a predicate needs something in a row to filter on) must not fork.
 */
final class SeedRows {

    static final String ID = "id";
    static final String SEQ = "seq";

    private SeedRows() {
    }

    /**
     * The columns a generated row carries, as a row of its own. {@code rows: 0} is a legal seed - the
     * way a specification says a table exists and holds nothing - and it leaves a driver no row to read
     * the shape from, while explicit values can never be empty. So the shape lives here too, and the
     * values in it stand only for their types.
     */
    static Map<String, Object> generatedShape() {
        Map<String, Object> shape = new LinkedHashMap<>();
        shape.put(ID, 0L);
        shape.put(SEQ, 0L);
        return shape;
    }

    /** The columns a generated row carries, in order. */
    static List<String> generatedColumns() {
        return List.of(ID, SEQ);
    }

    /** The rows {@code rows: N} stands for. */
    static List<Map<String, Object>> generated(long rows) {
        List<Map<String, Object>> generated = new ArrayList<>();
        for (long id = 1; id <= rows; id++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put(ID, id);
            row.put(SEQ, id);
            generated.add(row);
        }
        return generated;
    }
}
