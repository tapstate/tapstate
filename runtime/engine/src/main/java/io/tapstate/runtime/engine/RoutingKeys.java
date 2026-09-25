package io.tapstate.runtime.engine;

import com.hazelcast.function.FunctionEx;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.sql.JoinKey;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What an edge into a node running on several processors routes each item by, so that every change of one
 * row reaches the same processor and is applied there in the order it was read.
 *
 * <p>The key is the row's key as the stream it travels on defines it, read off the image the change carries -
 * the later image, or the earlier one for a removal, which has no other - and encoded the way a join encodes
 * one: each value's bytes behind their length, so the boundary between two columns cannot move, and a
 * converted value by what it holds rather than by its carrier. It is prefixed with the stream, so two tables
 * that happen to hold the same key values are routed independently.
 *
 * <p>Three things are not routed by a row key, and none of them silently. A schema change belongs to the whole
 * stream and goes by the stream alone. Word that a chain got past positions with nothing to deliver belongs
 * to no row and goes to one fixed processor, as a hand-off rather than a copy per processor. And a change whose
 * key cannot be read, or that moves its row from one key to another, ends the run with a code: there is no
 * processor it could be sent to that keeps every change of both keys in order, and sending it to any one of
 * them anyway is how a row's changes come to be applied out of order with nothing reporting it.
 */
final class RoutingKeys {

    /** The one key every word about settled positions is routed on. */
    static final String SETTLED_POSITIONS_LANE = "settled-positions";

    private RoutingKeys() {
    }

    /** The routing function for the edges into {@code node}, whose input streams are keyed as given. */
    static FunctionEx<Object, Object> forNode(String node, Map<String, List<String>> keysByStream) {
        Map<String, List<String>> keys = new LinkedHashMap<>();
        keysByStream.forEach((stream, columns) -> keys.put(stream, List.copyOf(columns)));
        return item -> keyOf(node, keys, item);
    }

    static Object keyOf(String node, Map<String, List<String>> keysByStream, Object item) {
        if (item instanceof SettledPositions) {
            return SETTLED_POSITIONS_LANE;
        }
        Envelope event = (Envelope) item;
        String stream = event.src();
        if (event.op() == Op.DDL) {
            return lane(stream, "");
        }
        List<String> columns = keysByStream.get(stream);
        if (columns == null || columns.isEmpty()) {
            // Every stream that can reach a node running wider than one processor had its key worked out
            // when the graph was drawn, and a stream with none kept the node at one processor. A stream
            // arriving without one is a wiring defect, not something a change can cause.
            throw new IllegalStateException("stream '" + stream + "' reached node '" + node
                    + "' with no key to route it by");
        }
        Map<String, Object> image = event.op() == Op.DELETE ? event.before() : event.after();
        JoinKey key = keyFrom(image, columns);
        if (key == null) {
            throw new TapstateException(EngineError.ROUTING_KEY_MISSING,
                    Map.of("node", node, "stream", stream, "columns", String.join(", ", columns)), null);
        }
        if (event.op() == Op.UPDATE) {
            // Only where the earlier image carries the whole key can a move be told from no move at all: a
            // change whose earlier image is partial or absent says nothing about its old key.
            JoinKey earlier = keyFrom(event.before(), columns);
            if (earlier != null && !earlier.name().equals(key.name())) {
                throw new TapstateException(EngineError.KEY_CHANGE_ON_PARALLEL_NODE,
                        Map.of("node", node, "stream", stream), null);
            }
        }
        return lane(stream, key.name());
    }

    /** The key {@code columns} spell on {@code image}, or null where a column is absent or holds nothing. */
    private static JoinKey keyFrom(Map<String, Object> image, List<String> columns) {
        if (image == null) {
            return null;
        }
        List<Object> values = new ArrayList<>(columns.size());
        for (String column : columns) {
            Object value = ConvertedValue.unwrap(image.get(column));
            if (value == null) {
                return null;
            }
            values.add(value);
        }
        JoinKey key = JoinKey.of(values);
        return key.matchable() ? key : null;
    }

    private static String lane(String stream, String key) {
        return stream.length() + ":" + stream + ":" + key;
    }
}
