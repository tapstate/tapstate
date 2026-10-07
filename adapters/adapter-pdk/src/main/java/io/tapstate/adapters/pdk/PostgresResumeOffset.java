package io.tapstate.adapters.pdk;

import io.tapdata.entity.utils.JsonParser;
import io.tapstate.core.common.TapstateException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/** Distinguishes a post-commit heartbeat boundary from the last change's start LSN on resume. */
final class PostgresResumeOffset {
    private static final String TYPE = "io.tapdata.connector.postgres.cdc.offset.PostgresOffset";

    private PostgresResumeOffset() {
    }

    static Object forReader(String connectorId, Object offset, Supplier<JsonParser> parser) {
        // PostgreSQL derivatives share this native offset class; no connector plugin is imported.
        if (offset == null || !TYPE.equals(offset.getClass().getName())) {
            return offset;
        }
        return prepareReaderCopy(connectorId, offset, parser);
    }

    /** Adjusts the JSON coordinates independently of the connector class that carries them. */
    static Object prepareReaderCopy(String connectorId, Object offset, Supplier<JsonParser> parser) {
        try {
            JsonParser json = parser.get();
            Map<String, Object> document = json.fromJsonObject(json.toJson(offset));
            if (!(document.get("sourceOffset") instanceof String source) || source.isBlank()) {
                return offset;
            }
            Map<String, Object> coordinates = json.fromJsonObject(source);
            Object processed = coordinates.get("lsn_proc");
            Object committed = coordinates.get("lsn_commit");
            if (coordinates.get("txId") != null
                    || !(processed instanceof Long || processed instanceof Integer)
                    || !(committed instanceof Long || committed instanceof Integer)) {
                return offset;
            }
            long lsn = ((Number) processed).longValue();
            if (lsn == 0 || lsn != ((Number) committed).longValue()) {
                return offset;
            }
            // The previous byte cannot begin a WAL record or filter the next transaction's first INSERT.
            // Change only the reader's copy, preserving the commit boundary and the recorded token.
            Map<String, Object> readerCoordinates = new LinkedHashMap<>(coordinates);
            readerCoordinates.put("lsn_proc", lsn - 1);
            Map<String, Object> reader = new LinkedHashMap<>(document);
            reader.put("sourceOffset", json.toJson(readerCoordinates));
            return json.fromJson(json.toJson(reader), offset.getClass());
        } catch (TapstateException coded) {
            throw coded;
        } catch (RuntimeException failure) {
            throw new TapstateException(ConnectorError.POSITION_UNREADABLE,
                    Map.of("connector", connectorId, "detail", "cannot prepare PostgreSQL resume offset: "
                            + failure.getMessage()), failure);
        }
    }
}
