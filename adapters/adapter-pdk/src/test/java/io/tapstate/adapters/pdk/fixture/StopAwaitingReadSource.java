package io.tapstate.adapters.pdk.fixture;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** A test connector whose stop waits for its active batch read to leave the host callback. */
public final class StopAwaitingReadSource implements TapConnector {

    public static final String FIFTH_BATCH = "tapstate.test.stop-awaiting-read.fifth-batch";
    public static final String READ_EXITED = "tapstate.test.stop-awaiting-read.read-exited";
    public static final String STOP_SAW_EXIT = "tapstate.test.stop-awaiting-read.stop-saw-exit";

    @Override
    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
        functions.supportBatchRead((context, table, offset, size, consumer) -> {
            try {
                for (int i = 0; i < 5; i++) {
                    if (i == 4) {
                        latch(FIFTH_BATCH).countDown();
                    }
                    TapEvent row = TapInsertRecordEvent.create().table("t1").referenceTime(1L)
                            .after(Map.of("id", i));
                    consumer.accept(List.of(row), null);
                }
            } finally {
                latch(READ_EXITED).countDown();
            }
        });
    }

    @Override
    public void init(TapConnectionContext context) {
    }

    @Override
    public void stop(TapConnectionContext context) {
        try {
            boolean exited = latch(READ_EXITED).await(2, TimeUnit.SECONDS);
            ((AtomicBoolean) System.getProperties().get(STOP_SAW_EXIT)).set(exited);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void discoverSchema(
            TapConnectionContext context, List<String> tables, int size, Consumer<List<TapTable>> consumer) {
        TapTable table = new TapTable("t1");
        table.add(new TapField("id", "int"));
        consumer.accept(List.of(table));
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext context, Consumer<TestItem> consumer) {
        return ConnectionOptions.create();
    }

    @Override
    public int tableCount(TapConnectionContext context) {
        return 1;
    }

    private static CountDownLatch latch(String key) {
        return (CountDownLatch) System.getProperties().get(key);
    }
}
