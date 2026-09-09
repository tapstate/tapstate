package io.tapstate.adapters.pdk;

import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapstate.core.common.TapstateException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Adapts a MySQL position's reader namespace without moving its binlog coordinate.
 *
 * <p>A shared source position may have been recorded by another pipeline node. The MySQL offset
 * includes that reader's logical name twice, and the connector looks it up using this node's name.
 * Keeping both names unchanged therefore loses the lookup even when both nodes kept their state.
 * Only those lookup names change here: the binlog offset payload remains an opaque, unchanged string.
 * Each reader keeps its own state, including schema history, and its own isolated connector loader.
 */
final class MysqlResumeOffset {
    private static final String TYPE = "io.tapdata.connector.mysql.entity.MysqlStreamOffset";
    private static final String SERVER_NAME = "SERVER_NAME";

    private MysqlResumeOffset() {
    }

    static Object forReader(String connectorId, Object offset, KVMap<Object> state,
            Supplier<JsonParser> parser) {
        // No connector class is imported or scanned. Snapshot seams and every other offset retain
        // their original objects; this compatibility rule is specific to the MySQL offset contract.
        if (offset == null || !TYPE.equals(offset.getClass().getName())) {
            return offset;
        }
        try {
            JsonParser json = parser.get();
            Map<String, Object> document = json.fromJsonObject(json.toJson(offset));
            if (!(document.get("name") instanceof String recorded) || recorded.isBlank()
                    || !(document.get("offset") instanceof Map<?, ?> positions) || positions.size() != 1) {
                throw new IllegalArgumentException("expected a named MySQL offset with exactly one partition");
            }
            Map.Entry<?, ?> position = positions.entrySet().iterator().next();
            if (!(position.getKey() instanceof String key) || !(position.getValue() instanceof String payload)) {
                throw new IllegalArgumentException("expected a MySQL partition and offset encoded as strings");
            }
            Map<String, Object> partition = json.fromJsonObject(key);
            if (!recorded.equals(partition.get("server"))) {
                throw new IllegalArgumentException("the MySQL offset name disagrees with its partition");
            }

            Object identity = state.get(SERVER_NAME);
            if (identity == null) {
                // A new node must not borrow the other reader's state or logical identity. Publishing
                // its own name before streamRead lets the connector and this offset agree on one name.
                String proposed = UUID.randomUUID().toString();
                Object incumbent = state.putIfAbsent(SERVER_NAME, proposed);
                identity = incumbent == null ? proposed : incumbent;
            }
            if (!(identity instanceof String reader) || reader.isBlank()) {
                throw new TapstateException(ConnectorError.STATE_UNREADABLE,
                        Map.of("detail", "the MySQL reader identity is not a non-blank string"), null);
            }
            if (recorded.equals(reader)) {
                return offset;
            }
            Map<String, Object> reboundPartition = new LinkedHashMap<>(partition);
            reboundPartition.put("server", reader);
            Map<String, Object> rebound = new LinkedHashMap<>(document);
            rebound.put("name", reader);
            rebound.put("offset", Map.of(json.toJson(reboundPartition), payload));
            // Rebuild through the frozen PDK JSON service into the same loader's offset class. Parsing
            // the payload would risk changing large numbers, GTIDs or future connector-specific fields.
            return json.fromJson(json.toJson(rebound), offset.getClass());
        } catch (TapstateException coded) {
            throw coded;
        } catch (RuntimeException failure) {
            throw new TapstateException(ConnectorError.POSITION_UNREADABLE,
                    Map.of("connector", connectorId, "detail", "cannot bind MySQL offset to its reader: "
                            + String.valueOf(failure.getMessage())), failure);
        }
    }
}
