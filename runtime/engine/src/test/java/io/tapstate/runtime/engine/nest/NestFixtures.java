package io.tapstate.runtime.engine.nest;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The pieces every nest state test builds its events out of, shared so they read the same everywhere. */
final class NestFixtures {

    private NestFixtures() {
    }

    /** An order in the first epoch, at {@code seq} — enough to order events within one test. */
    static SourceOrder at(long seq) {
        return new SourceOrder(1L, seq);
    }

    /**
     * A change with no chain position of its own, for the tests that are about where an element lands
     * rather than about how far the frontier may go.
     */
    static Map<String, ChainPosition> noPositions() {
        return Map.of();
    }

    /** A row from alternating key/value arguments, in the order given. */
    static Map<String, Object> row(Object... pairs) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            row.put((String) pairs[i], pairs[i + 1]);
        }
        return row;
    }

    /** An element of the embed at {@code pathId}, hung under the parent row that answers to {@code parent}. */
    static ElementRef element(List<String> pathId, Object parent, Object key, Object identity) {
        return new ElementRef(pathId, parent, List.of(key), identity);
    }

    /**
     * The array an embed occupies in a rendered document, reached through the first element of each level
     * along {@code path}.
     */
    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> listAt(Map<String, Object> document, String... path) {
        Object node = document;
        for (int i = 0; i < path.length; i++) {
            node = ((Map<String, Object>) node).get(path[i]);
            if (i < path.length - 1) {
                node = ((List<Map<String, Object>>) node).get(0);
            }
        }
        return (List<Map<String, Object>>) node;
    }
}
