package io.tapstate.adapters.pdk.fixture;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public final class CdcStartBoundarySource implements TapConnector {
    @Override
    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
        functions.supportTimestampToStreamOffset((context, timestamp) -> timestamp);
        functions.supportStreamRead((context, tables, offset, size, consumer) -> {
            consumer.streamReadStarted();
            consumer.accept(List.of(row(1, 10L, 30L), row(2, null, 17L)), 11L);
            consumer.accept(List.of(row(3, null, 20L), row(4, null, null)), 12L);
            consumer.accept(List.of(row(5, 5L, null)), 13L);
            consumer.streamReadEnded();
        });
    }

    private static TapInsertRecordEvent row(int id, Long referenceTime, Long time) {
        var event = TapInsertRecordEvent.create().table("t1").after(Map.of("id", id));
        event.setReferenceTime(referenceTime);
        event.setTime(time);
        return event;
    }

    @Override
    public void init(TapConnectionContext context) {
    }

    @Override
    public void stop(TapConnectionContext context) {
    }

    @Override
    public void discoverSchema(TapConnectionContext context, List<String> tables, int size,
            Consumer<List<TapTable>> consumer) {
        TapTable table = new TapTable("t1");
        table.add(new TapField("id", "int"));
        consumer.accept(List.of(table));
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext context, Consumer<TestItem> consumer) {
        return new ConnectionOptions();
    }

    @Override
    public int tableCount(TapConnectionContext context) {
        return 1;
    }
}
