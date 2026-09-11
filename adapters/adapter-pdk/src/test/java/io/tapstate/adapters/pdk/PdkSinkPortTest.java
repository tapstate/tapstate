package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.sink.TargetIndex;
import io.tapstate.spi.sink.WriteResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The write-side PDK bridge: {@link PdkSinkPort} encoding tapstate envelopes back to PDK record events
 * and driving a connector's registered write function, with connector-side failures surfaced as coded
 * connector-domain exceptions and the write mode / ddl policy enforced. Synthetic connectors compiled
 * at test time stand in for a real connector jar and the PDK runtime the host does not yet provide.
 */
class PdkSinkPortTest {

    @Test
    void missingDecimalMetadataIsCodedBeforeAnyWrite(@TempDir Path dir) throws Throwable {
        Path jar = Synthetic.countingSink(dir);
        PdkConnector connector = PdkConnector.open("demo", provisioner(jar, "synthetic.CountingSink").resolve("demo"), Map.of());
        TargetTable table = new TargetTable("t1", List.of(new TargetField("amount", "decimal", false,
                io.tapstate.core.common.TapstateType.DECIMAL)));
        AtomicInteger writes = new AtomicInteger();
        try (SinkWriter writer = new PdkSinkWriter(connector,
                (context, events, target, result) -> writes.incrementAndGet(), configWithTarget(table), Map.of("t1", table), null)) {
            assertThatThrownBy(() -> await(writer, List.of(Envelope.insert(1L, "t1", Map.of("amount", 1), null))))
                    .isInstanceOf(ExecutionException.class)
                    .cause().isInstanceOfSatisfying(TapstateException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(ConnectorError.WRITE_FAILED);
                        assertThat(failure.getMessage()).contains("amount", "t1", "rediscover");
                    });
            assertThat(writes).hasValue(0);
        }
    }

    @Test
    void preparationFailuresAreCodedAndNoWriteIsAttempted(@TempDir Path dir) throws Throwable {
        Path jar = Synthetic.countingSink(dir);
        for (io.tapstate.spi.sink.OnFullLoad policy : List.of(
                io.tapstate.spi.sink.OnFullLoad.CLEAR, io.tapstate.spi.sink.OnFullLoad.FAIL)) {
            PdkConnector connector = PdkConnector.open("demo", provisioner(jar, "synthetic.CountingSink")
                    .resolve("demo"), Map.of());
            connector.functions().supportCreateTableV2((context, event) ->
                    io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions.create().tableExists(true));
            connector.functions().supportClearTable((context, event) -> {
                throw new IllegalStateException("cannot clear target " + event.getTableId());
            });
            connector.functions().supportCountByPartitionFilterFunction((context, table, filter) -> 1L);
            AtomicInteger writes = new AtomicInteger();
            SinkConfig config = new SinkConfig("demo", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL,
                    target(), null, policy, true);
            try (SinkWriter writer = new PdkSinkWriter(connector,
                    (context, events, table, result) -> writes.incrementAndGet(), config,
                    Map.of("t1", target()), null)) {
                assertThatThrownBy(() -> await(writer, List.of(Envelope.insert(1L, "t1", Map.of("id", 1), null))))
                        .isInstanceOf(ExecutionException.class)
                        .cause().isInstanceOfSatisfying(TapstateException.class, failure -> {
                            assertThat(failure.code()).isEqualTo(ConnectorError.WRITE_FAILED);
                            assertThat(failure.getMessage()).contains("t1");
                        });
                assertThat(writes).hasValue(0);
            }
        }
    }

    private static ConnectorProvisioner provisioner(Path jar, String className) {
        ConnectorRef ref = new ConnectorRef(List.of(jar), className, "2.0.8", null);
        return connectorId -> ref;
    }

    private static SinkConfig config(WriteMode mode, DdlPolicy ddl) {
        return new SinkConfig("demo", Map.of(), mode, ddl);
    }

    private static SinkConfig configWithTarget(TargetTable target) {
        return new SinkConfig("demo", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL, target);
    }

    private static TargetTable target() {
        return new TargetTable("t1",
                List.of(new TargetField("id", "int", true), new TargetField("v", "int", false)));
    }

    /** A writer assembled the way the port assembles one, with an index function of the test's own. */
    private static PdkSinkWriter writerWith(Path jar, Map<String, TargetTable> targets,
            io.tapdata.pdk.apis.functions.connector.target.CreateIndexFunction createIndex) throws Throwable {
        PdkConnector connector = PdkConnector.open("demo", provisioner(jar, "synthetic.CountingSink").resolve("demo"), Map.of());
        var write = connector.functions().getWriteRecordFunction();
        connector.underLoader(() -> {
            connector.connector().init(connector.context());
            return null;
        });
        return new PdkSinkWriter(connector, write, WriteMode.UPSERT, DdlPolicy.FAIL, targets, createIndex);
    }

    private static TargetTable indexed(String collection) {
        return new TargetTable(collection,
                List.of(new TargetField("id", "int", true)),
                List.of(new TargetIndex(List.of("id"), true)));
    }

    /**
     * The create-index request goes out once per table, not once per stream. A view maps many streams
     * onto one collection - the writer's target map then holds several entries all naming it - and a
     * memo keyed by the delivering stream would send the same request once per stream, never deduping
     * across them.
     */
    @Test
    void theIndexRequestIsIssuedOncePerTableNotOncePerStream(@TempDir Path dir) throws Throwable {
        Path jar = Synthetic.countingSink(dir);
        AtomicInteger requests = new AtomicInteger();
        Map<String, TargetTable> streams =
                Map.of("orders", indexed("order_state"), "order_doc", indexed("order_state"));
        try (SinkWriter writer = writerWith(jar, streams, (context, table, event) -> requests.incrementAndGet())) {
            await(writer, List.of(Envelope.insert(1L, "orders", Map.of("id", 1), null)));
            await(writer, List.of(Envelope.insert(2L, "order_doc", Map.of("id", 2), null)));
        }
        assertThat(requests.get())
                .as("create-index requests for two streams sharing one collection")
                .isEqualTo(1);
    }

    /**
     * A failed index creation is retried on the next batch rather than remembered as done. Memoized
     * before the call, the first failure would be recorded as success and the collection would live on
     * without the indexes it declared - silently, since the write itself proceeds fine afterwards.
     */
    @Test
    void aFailedIndexCreationIsRetriedOnTheNextBatch(@TempDir Path dir) throws Throwable {
        Path jar = Synthetic.countingSink(dir);
        AtomicInteger requests = new AtomicInteger();
        io.tapdata.pdk.apis.functions.connector.target.CreateIndexFunction failsOnce =
                (context, table, event) -> {
                    if (requests.incrementAndGet() == 1) {
                        throw new IllegalStateException("the store was not ready");
                    }
                };
        try (SinkWriter writer = writerWith(jar, Map.of("orders", indexed("order_state")), failsOnce)) {
            assertThatThrownBy(() -> await(writer, List.of(Envelope.insert(1L, "orders", Map.of("id", 1), null))))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TapstateException.class);
            assertThat(await(writer, List.of(Envelope.insert(2L, "orders", Map.of("id", 2), null))).written())
                    .isEqualTo(1);
        }
        assertThat(requests.get())
                .as("the second batch retried the creation instead of trusting the failed first")
                .isEqualTo(2);
    }

    private static WriteResult await(SinkWriter writer, List<Envelope> records) throws Exception {
        return writer.write(records).toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private static Envelope insert(int id) {
        return Envelope.insert(1L, "t1", Map.of("id", id), null);
    }

    @Test
    void writeDeliversRowsAndReportsTheAcceptedCount(@TempDir Path dir) throws Exception {
        Path jar = Synthetic.countingSink(dir);
        PdkSinkPort port = new PdkSinkPort(provisioner(jar, "synthetic.CountingSink"));
        try (SinkWriter writer = port.open(config(WriteMode.UPSERT, DdlPolicy.APPLY))) {
            assertThat(await(writer, List.of(insert(1), insert(2))).written()).isEqualTo(2);
        }
    }

    @Test
    void writeAccumulatesCountsAcrossMultipleConnectorFlushes(@TempDir Path dir) throws Exception {
        // A connector may report a batch in several flushes, one consumer callback each; the writer must
        // sum the accepted counts, not report only the last flush's.
        Path jar = Synthetic.multiFlushSink(dir);
        PdkSinkPort port = new PdkSinkPort(provisioner(jar, "synthetic.MultiFlush"));
        try (SinkWriter writer = port.open(config(WriteMode.UPSERT, DdlPolicy.APPLY))) {
            assertThat(await(writer, List.of(insert(1), insert(2))).written()).isEqualTo(2);
        }
    }

    @Test
    void aConnectorThatThrowsWhileWritingIsACodedWriteFailure(@TempDir Path dir) throws Exception {
        Path jar = Synthetic.throwingWriteSink(dir);
        PdkSinkPort port = new PdkSinkPort(provisioner(jar, "synthetic.ThrowingWrite"));
        try (SinkWriter writer = port.open(config(WriteMode.UPSERT, DdlPolicy.APPLY))) {
            assertThatThrownBy(() -> await(writer, List.of(insert(1))))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TapstateException.class)
                    .satisfies(e -> assertThat(((TapstateException) e.getCause()).code())
                            .isEqualTo(ConnectorError.WRITE_FAILED));
        }
    }

    @Test
    void appendModeReforgesUpdatesAndDeletesToInserts(@TempDir Path dir) throws Exception {
        // The inserts-only sink rejects any non-insert event; that the append-mode write succeeds proves
        // the update and delete were reforged into inserts before reaching the connector.
        Path jar = Synthetic.insertsOnlySink(dir);
        PdkSinkPort port = new PdkSinkPort(provisioner(jar, "synthetic.InsertsOnly"));
        Envelope update = Envelope.update(1L, "t1", Map.of("id", 1), Map.of("id", 1, "v", 2), null);
        Envelope delete = Envelope.delete(1L, "t1", Map.of("id", 1), null);
        try (SinkWriter writer = port.open(config(WriteMode.APPEND, DdlPolicy.IGNORE))) {
            assertThat(await(writer, List.of(update, delete)).written()).isEqualTo(2);
        }
    }

    @Test
    void appendModeRejectsAMultiTableBatchBeforeAnyConnectorWrite(@TempDir Path dir) throws Exception {
        Path jar = Synthetic.throwingWriteSink(dir);
        PdkSinkPort port = new PdkSinkPort(provisioner(jar, "synthetic.ThrowingWrite"));
        try (SinkWriter writer = port.open(config(WriteMode.APPEND, DdlPolicy.IGNORE),
                Map.of("t1", target(), "t2", new TargetTable("t2", List.of())))) {
            assertThatThrownBy(() -> await(writer, List.of(
                    Envelope.insert(1L, "t1", Map.of("id", 1), null),
                    Envelope.insert(2L, "t2", Map.of("id", 2), null))))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TapstateException.class)
                    .satisfies(e -> {
                        TapstateException failure = (TapstateException) e.getCause();
                        assertThat(failure.code()).isEqualTo(ConnectorError.WRITE_FAILED);
                        assertThat(failure).hasMessageContaining("before any connector write");
                    });
        }
    }

    /**
     * When a store refuses a name, the only thing that knows which name it was is the store's own
     * message. Tapstate cannot reconstruct it - a batch names a table, but the rule that rejected it
     * belongs to the target - so carrying that message through verbatim is the whole of what makes
     * the failure diagnosable. A detail that summarised or replaced it would leave the user with
     * "the write failed" and no way to find out which identifier caused it.
     */
    @Test
    void aRefusedIdentifierSurvivesIntoTheFailureDetail(@TempDir Path dir) throws Exception {
        Path jar = Synthetic.identifierRejectingSink(dir);
        PdkSinkPort port = new PdkSinkPort(provisioner(jar, "synthetic.IdentifierRejecting"));
        try (SinkWriter writer = port.open(config(WriteMode.UPSERT, DdlPolicy.IGNORE),
                Map.of("t1", target()))) {
            assertThatThrownBy(() -> await(writer, List.of(Envelope.insert(1L, "t1", Map.of("id", 1), null))))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TapstateException.class)
                    .satisfies(e -> {
                        TapstateException failure = (TapstateException) e.getCause();
                        assertThat(failure.code()).isEqualTo(ConnectorError.WRITE_FAILED);
                        assertThat(failure.args()).extracting("detail").asString()
                                .contains("ord$ers");
                    });
        }
    }

    @Test
    void upsertModePassesRowEventsThroughUnchanged(@TempDir Path dir) throws Exception {
        // In upsert mode an update stays an update; the inserts-only sink rejects it, proving no reforge.
        Path jar = Synthetic.insertsOnlySink(dir);
        PdkSinkPort port = new PdkSinkPort(provisioner(jar, "synthetic.InsertsOnly"));
        Envelope update = Envelope.update(1L, "t1", Map.of("id", 1), Map.of("id", 1, "v", 2), null);
        try (SinkWriter writer = port.open(config(WriteMode.UPSERT, DdlPolicy.IGNORE))) {
            assertThatThrownBy(() -> await(writer, List.of(update)))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TapstateException.class)
                    .satisfies(e -> assertThat(((TapstateException) e.getCause()).code())
                            .isEqualTo(ConnectorError.WRITE_FAILED));
        }
    }

    @Test
    void aSchemaChangeUnderFailPolicyIsACodedWriteFailure(@TempDir Path dir) throws Exception {
        Path jar = Synthetic.countingSink(dir);
        PdkSinkPort port = new PdkSinkPort(provisioner(jar, "synthetic.CountingSink"));
        try (SinkWriter writer = port.open(config(WriteMode.UPSERT, DdlPolicy.FAIL))) {
            assertThatThrownBy(() -> await(writer, List.of(Envelope.ddl(1L, "t1", Map.of()))))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TapstateException.class)
                    .satisfies(e -> assertThat(((TapstateException) e.getCause()).code())
                            .isEqualTo(ConnectorError.WRITE_FAILED));
        }
    }

    @Test
    void aSchemaChangeUnderIgnorePolicyIsSkippedAndRowsStillWrite(@TempDir Path dir) throws Exception {
        Path jar = Synthetic.countingSink(dir);
        PdkSinkPort port = new PdkSinkPort(provisioner(jar, "synthetic.CountingSink"));
        try (SinkWriter writer = port.open(config(WriteMode.UPSERT, DdlPolicy.IGNORE))) {
            // The batch carries one row and one schema change; only the row reaches the connector.
            assertThat(await(writer, List.of(insert(1), Envelope.ddl(1L, "t1", Map.of()))).written()).isEqualTo(1);
        }
    }

    @Test
    void theResolvedTargetPrimaryKeyReachesTheConnector(@TempDir Path dir) throws Exception {
        // The sink reports the primary-key count of the table it is handed; one key column proves the
        // resolved target model's key reached the connector rather than a bare, keyless table.
        Path jar = Synthetic.keyCountingSink(dir);
        PdkSinkPort port = new PdkSinkPort(provisioner(jar, "synthetic.KeyCounting"));
        try (SinkWriter writer = port.open(configWithTarget(target()))) {
            assertThat(await(writer, List.of(insert(1))).written()).isEqualTo(1);
        }
    }

    @Test
    void theResolvedTargetColumnsReachTheConnector(@TempDir Path dir) throws Exception {
        // The sink reports the column count of the table it is handed; both columns prove the resolved
        // target schema reached the connector.
        Path jar = Synthetic.fieldCountingSink(dir);
        PdkSinkPort port = new PdkSinkPort(provisioner(jar, "synthetic.FieldCounting"));
        try (SinkWriter writer = port.open(configWithTarget(target()))) {
            assertThat(await(writer, List.of(insert(1))).written()).isEqualTo(2);
        }
    }

    @Test
    void oneConnectorSessionRoutesEachSourceTableBatchToItsOwnTargetModel(@TempDir Path dir) throws Exception {
        Path jar = Synthetic.keyCountingSink(dir);
        PdkSinkPort port = new PdkSinkPort(provisioner(jar, "synthetic.KeyCounting"));
        TargetTable second = new TargetTable("t2", List.of(
                new TargetField("tenant", "int", true), new TargetField("id", "int", true)));
        try (SinkWriter writer = port.open(config(WriteMode.UPSERT, DdlPolicy.FAIL),
                Map.of("t1", target(), "t2", second))) {
            assertThat(await(writer, List.of(
                    Envelope.insert(1L, "t1", Map.of("id", 1), null),
                    Envelope.insert(2L, "t2", Map.of("tenant", 7, "id", 2), null))).written())
                    .isEqualTo(3);
        }
    }

    @Test
    void withoutAResolvedTargetTheConnectorGetsABareTableWithNoColumns(@TempDir Path dir) throws Exception {
        // No resolved target model: the connector is handed a bare table id with no columns, as before.
        Path jar = Synthetic.fieldCountingSink(dir);
        PdkSinkPort port = new PdkSinkPort(provisioner(jar, "synthetic.FieldCounting"));
        try (SinkWriter writer = port.open(config(WriteMode.UPSERT, DdlPolicy.FAIL))) {
            assertThat(await(writer, List.of(insert(1))).written()).isEqualTo(0);
        }
    }
}
