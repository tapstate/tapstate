package io.tapstate.app;

import io.tapstate.spi.capture.TableSchema;
import io.tapstate.spi.store.SourceTable;
import java.util.Map;

/** A selected source table and its secret-bearing connection coordinates, held only during preview. */
record PreviewSourceTable(
        String sourceKey,
        String sourceId,
        String connectorId,
        Map<String, Object> settings,
        TableSchema schema,
        SourceTable model) {
}
