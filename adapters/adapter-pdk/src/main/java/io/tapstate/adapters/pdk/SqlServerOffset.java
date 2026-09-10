package io.tapstate.adapters.pdk;

import io.tapdata.entity.utils.JsonParser;
import io.tapstate.core.common.TapstateException;

import java.util.Map;
import java.util.function.Supplier;

/** Preserves SQL Server's native JSON offset representation across the snapshot seam. */
final class SqlServerOffset {
    private static final String TYPE = "io.tapdata.connector.mssql.cdc.CdcOffset";

    private SqlServerOffset() {
    }

    static Object forStorage(String connectorId, Object offset, Supplier<JsonParser> parser) {
        // The snapshot returns a non-serializable bean, while the same connector's CDC
        // callbacks emit JSON strings and its reader explicitly accepts that string form.
        // Every other offset keeps the original token codec and its refusal behavior.
        if (!"sqlserver".equals(connectorId) || offset == null || !TYPE.equals(offset.getClass().getName())) {
            return offset;
        }
        try {
            JsonParser json = parser.get();
            String nativeOffset = json.toJson(offset);
            if (nativeOffset == null || nativeOffset.isBlank()) {
                throw new IllegalArgumentException("the SQL Server JSON offset is empty");
            }
            Map<?, ?> coordinates = json.fromJson(nativeOffset, Map.class);
            if (coordinates == null || !(coordinates.get("currentStartLSN") instanceof String lsn)
                    || lsn.isBlank()) {
                throw new IllegalArgumentException("SQL Server CDC has not published an initial LSN; "
                        + "verify that SQL Server Agent and the database CDC capture job are running, "
                        + "then retry after CDC has captured a transaction");
            }
            return nativeOffset;
        } catch (RuntimeException failure) {
            throw new TapstateException(ConnectorError.POSITION_UNRENDERABLE,
                    Map.of("connector", connectorId, "detail", "cannot render the SQL Server native offset: "
                            + String.valueOf(failure.getMessage())), failure);
        }
    }
}
