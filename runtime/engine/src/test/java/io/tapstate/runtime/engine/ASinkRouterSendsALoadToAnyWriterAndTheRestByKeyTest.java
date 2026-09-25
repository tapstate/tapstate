package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.test.TestSupport;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The router in front of a sink running on several processors sends a keyed table's snapshot rows along the
 * edge that hands each row to whichever writer has room, and every other row along the edge that hands it to
 * the writer its key belongs to.
 *
 * <p>Each kind of row that must not be spread is here on its own: a change, a removal, a snapshot row of a
 * table with no key, a schema change, and word that a chain got past positions with nothing to deliver. A
 * router that sent any of them to any writer would apply one row's changes on two writers in whatever order
 * they finished, or write a table with no key on several writers at once; one that sent a keyed table's
 * snapshot rows by key would leave a slow writer with a fixed share of the load.
 */
class ASinkRouterSendsALoadToAnyWriterAndTheRestByKeyTest {

    private static final Map<String, SinkTarget> TARGETS = Map.of(
            "orders", new SinkTarget("orders", List.of("id")),
            "logs", new SinkTarget("logs", List.of()));

    @Test
    void aKeyedTablesSnapshotRowsGoToAnyWriterAndEverythingElseByKey() {
        Envelope loaded = Envelope.read(1, "orders", Map.of("id", 1), null)
                .withOrder(SourceOrder.snapshotRow(1));
        Envelope changed = Envelope.update(2, "orders", Map.of("id", 1), Map.of("id", 1, "v", 2), null)
                .withOrder(new SourceOrder(1, 0));
        Envelope removed = Envelope.delete(3, "orders", Map.of("id", 2), null)
                .withOrder(new SourceOrder(1, 1));
        Envelope loadedWithoutAKey = Envelope.read(4, "logs", Map.of("line", "a"), null)
                .withOrder(SourceOrder.snapshotRow(1));
        Envelope schemaChange = Envelope.ddl(5, "orders", Map.of("id", "INT"));
        SettledPositions settled =
                new SettledPositions(Map.of("orders", new ChainPosition(new SourceOrder(1, 2), "t2")));

        TestSupport.verifyProcessor(() -> new SinkRouter(SinkRouter.spreadStreamsOf(TARGETS), null))
                .input(List.of(loaded, changed, removed, loadedWithoutAKey, schemaChange, settled))
                .expectOutputs(List.of(
                        List.of(loaded),
                        List.of(changed, removed, loadedWithoutAKey, schemaChange, settled)));
    }
}
