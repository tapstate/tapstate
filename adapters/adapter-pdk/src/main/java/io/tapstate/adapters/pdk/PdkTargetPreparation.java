package io.tapstate.adapters.pdk;

import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.SinkPreparationNamespace;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.store.KeyedStateStore;
import java.util.HashSet;
import java.util.Set;

/** Prepares each table before writing; a durable receipt prevents destructive preparation on recovery. */
final class PdkTargetPreparation {
    private final TapConnectorContext context;
    private final ConnectorFunctions functions;
    private final OnFullLoad onFullLoad;
    private final boolean fullLoad;
    private final String namespace;
    private final KeyedStateStore stateStore;
    private final Set<String> prepared = new HashSet<>();

    PdkTargetPreparation(TapConnectorContext context, ConnectorFunctions functions, OnFullLoad onFullLoad,
            boolean fullLoad, PipelineNode node, KeyedStateStore stateStore) {
        this.context = context;
        this.functions = functions;
        this.onFullLoad = onFullLoad;
        this.fullLoad = fullLoad;
        this.namespace = SinkPreparationNamespace.of(node);
        this.stateStore = stateStore;
    }

    void prepare(TargetTable target, TapTable table) throws Throwable {
        if (target == null || prepared.contains(target.name())) {
            return;
        }
        if (fullLoad && onFullLoad == OnFullLoad.CLEAR && namespace != null && stateStore == null) {
            throw new IllegalStateException("target table " + target.name()
                    + " requires durable preparation state before on_full_load clear");
        }
        byte[] receipt = namespace == null || stateStore == null
                ? null : stateStore.load(namespace, target.name()).orElse(null);
        boolean created = create(table);
        boolean primaryKeyAlreadyCreated = created || (receipt != null && receipt[0] == 2);
        if (receipt == null) {
            if (!created && fullLoad) {
                prepareExisting(table);
            }
            if (namespace != null && stateStore != null) {
                // Before any row can land, and only after a successful clear/check. Rerun teardown owns
                // removing this receipt; a member restart must keep it even when no source ack survived.
                stateStore.save(namespace, target.name(), new byte[] {(byte) (created ? 2 : 1)});
            }
        }
        if (functions.getCreateIndexFunction() != null) {
            var event = TargetTapTable.createIndexEvent(target, primaryKeyAlreadyCreated);
            if (event != null) {
                event.setTableId(table.getId());
                functions.getCreateIndexFunction().createIndex(context, table, event);
            }
        }
        // A failed index call is not success: retry it before the next write without clearing again.
        prepared.add(target.name());
    }

    private boolean create(TapTable table) throws Throwable {
        if (functions.getCreateTableV2Function() == null || table.getNameFieldMap().isEmpty()) {
            return false;
        }
        TapCreateTableEvent event = new TapCreateTableEvent().table(table);
        event.setTableId(table.getId());
        var options = functions.getCreateTableV2Function().createTable(context, event);
        if (options == null || options.getTableExists() == null) {
            throw new IllegalStateException("create-table did not report existence for target table " + table.getId());
        }
        return !options.getTableExists();
    }

    private void prepareExisting(TapTable table) throws Throwable {
        switch (onFullLoad) {
            case APPEND -> { }
            case CLEAR -> {
                if (functions.getClearTableFunction() == null) {
                    throw new IllegalStateException("target table " + table.getId()
                            + " cannot honor on_full_load clear: connector provides no clear-table function");
                }
                TapClearTableEvent event = new TapClearTableEvent();
                event.setTableId(table.getId());
                functions.getClearTableFunction().clearTable(context, event);
            }
            case FAIL -> {
                if (functions.getCountByPartitionFilterFunction() == null) {
                    throw new IllegalStateException("target table " + table.getId()
                            + " cannot honor on_full_load fail: connector provides no row-count function");
                }
                long rows = functions.getCountByPartitionFilterFunction()
                        .countByPartitionFilter(context, table, TapAdvanceFilter.create());
                if (rows < 0) {
                    throw new IllegalStateException("row count is unknown for target table " + table.getId());
                }
                if (rows > 0) {
                    throw new IllegalStateException("target table " + table.getId()
                            + " is not empty; on_full_load is fail");
                }
            }
        }
    }
}
