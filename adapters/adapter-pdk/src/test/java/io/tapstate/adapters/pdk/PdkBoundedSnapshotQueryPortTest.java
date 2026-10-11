package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ConvertedValue;
import io.tapdata.entity.schema.value.DateTime;
import io.tapstate.spi.capture.BoundedSnapshotQueryRequest;
import io.tapstate.spi.capture.BoundedSnapshotQueryRequest.AllRows;
import io.tapstate.spi.capture.BoundedSnapshotQueryRequest.ExactTuples;
import io.tapstate.spi.capture.BoundedSnapshotQueryRequest.Selection;
import io.tapstate.spi.capture.BoundedSnapshotQueryResult;
import io.tapstate.spi.capture.FieldSchema;
import io.tapstate.spi.capture.TableSchema;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.AbstractMap;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.bson.BsonTimestamp;

class PdkBoundedSnapshotQueryPortTest {

    @Test
    void reusesAnInitializedConnectorForRepeatedPreviewReads(@TempDir Path dir) {
        ConnectorRef ref = new ConnectorRef(List.of(Synthetic.exactTupleQuerySource(dir)),
                "synthetic.ExactTupleQuery", "2.0.8", null);
        AtomicInteger resolutions = new AtomicInteger();
        try (PdkBoundedSnapshotQueryPort port = new PdkBoundedSnapshotQueryPort(connectorId -> {
            resolutions.incrementAndGet();
            return ref;
        })) {
            assertThat(port.query(request(new AllRows(), 2)).rows()).hasSize(2);
            assertThat(port.query(request(new AllRows(), 2)).rows()).hasSize(2);
            assertThat(resolutions).hasValue(1);
        }
    }


    @Test
    void exactCompositeTuplesAreQueriedSeparatelyAndNeverBecomeIndependentKeySets(@TempDir Path dir) {
        PdkBoundedSnapshotQueryPort port = port(Synthetic.exactTupleQuerySource(dir));
        Selection exact = new ExactTuples(List.of(
                Map.of("tenant_id", 1, "order_id", 1),
                Map.of("tenant_id", 2, "order_id", 2)));

        BoundedSnapshotQueryResult result = port.query(request(exact, 10));

        assertThat(result.complete()).isTrue();
        assertThat(result.hasMore()).isFalse();
        assertThat(result.queryCount()).isEqualTo(2);
        assertThat(result.rows()).extracting(row -> row.after().get("value"))
                .containsExactly("a", "b");
    }

    @Test
    void requestsOneExtraRowAndMarksAnAnchorResultIncompleteWhenTruncated(@TempDir Path dir) {
        PdkBoundedSnapshotQueryPort port = port(Synthetic.exactTupleQuerySource(dir));

        BoundedSnapshotQueryResult result = port.query(request(new AllRows(), 2));

        assertThat(result.rows()).hasSize(2);
        assertThat(result.complete()).isFalse();
        assertThat(result.hasMore()).isTrue();
        assertThat(result.repeatable()).isTrue();
    }

    @Test
    void refusesAConnectorWithoutExactQueryCapabilityBeforeTableRead(@TempDir Path dir) {
        ConnectorRef ref = new ConnectorRef(List.of(Synthetic.namedStreamSource(dir, "t1", "id")),
                "synthetic.NamedStreamSource", "2.0.8", null);
        PdkBoundedSnapshotQueryPort port = new PdkBoundedSnapshotQueryPort(connectorId -> ref);

        assertThatThrownBy(() -> port.query(request(new AllRows(), 1)))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code())
                .isEqualTo(ConnectorError.CAPABILITY_MISSING);
    }

    @Test
    void rejectsAnExpiredDeadlineWithoutOpeningTheConnector(@TempDir Path dir) {
        PdkBoundedSnapshotQueryPort port = port(Synthetic.exactTupleQuerySource(dir));
        BoundedSnapshotQueryRequest request = new BoundedSnapshotQueryRequest(
                "source-1", "connector-1", Map.of(), schema(), List.of("tenant_id", "order_id"),
                new AllRows(), List.of(), 1, Instant.EPOCH);

        assertThatThrownBy(() -> port.query(request))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code())
                .isEqualTo(ConnectorError.READ_TIMEOUT);
    }

    @Test
    void capsExactTupleReadsAndTreatsAnEmptyTupleSetAsACompleteEmptySample(@TempDir Path dir) {
        PdkBoundedSnapshotQueryPort port = port(Synthetic.exactTupleQuerySource(dir));
        Selection exact = new ExactTuples(List.of(Map.of("tenant_id", 1, "order_id", 1),
                Map.of("tenant_id", 2, "order_id", 2)));

        BoundedSnapshotQueryResult capped = port.query(request(exact, 1));
        assertThat(capped.rows()).hasSize(1);
        assertThat(capped.queryCount()).isEqualTo(1);
        assertThat(capped.complete()).isFalse();
        assertThat(capped.hasMore()).isTrue();

        BoundedSnapshotQueryResult empty = port.query(request(new ExactTuples(List.of()), 1));
        assertThat(empty.rows()).isEmpty();
        assertThat(empty.queryCount()).isZero();
        assertThat(empty.complete()).isTrue();
    }

    @Test
    void rejectsChangedDiscoverySchemaAndSamplesThatExceedTheByteBudget(@TempDir Path dir) {
        PdkBoundedSnapshotQueryPort port = port(Synthetic.exactTupleQuerySource(dir));
        BoundedSnapshotQueryRequest changed = new BoundedSnapshotQueryRequest(
                "source-1", "connector-1", Map.of(),
                new TableSchema("t1", List.of(new FieldSchema("tenant_id", "varchar"))),
                List.of(), new AllRows(), List.of(), 1, Instant.now().plusSeconds(5));
        assertThatThrownBy(() -> port.query(changed))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code())
                .isEqualTo(ConnectorError.READ_FAILED);

        BoundedSnapshotQueryRequest tooSmall = new BoundedSnapshotQueryRequest(
                "source-1", "connector-1", Map.of(), schema(), List.of(), new AllRows(), List.of(), 1, 1,
                Instant.now().plusSeconds(5));
        assertThatThrownBy(() -> port.query(tooSmall))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code())
                .isEqualTo(ConnectorError.READ_FAILED);
    }

    @Test
    void refusesWhenDiscoveryDoesNotResolveTheRequestedTable(@TempDir Path dir) {
        PdkBoundedSnapshotQueryPort port = port(Synthetic.exactTupleQuerySource(dir));
        BoundedSnapshotQueryRequest missing = new BoundedSnapshotQueryRequest(
                "source-1", "connector-1", Map.of(), new TableSchema("missing", List.of()), List.of(),
                new AllRows(), List.of(), 1, Instant.now().plusSeconds(5));
        assertThatThrownBy(() -> port.query(missing))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code())
                .isEqualTo(ConnectorError.DISCOVER_FAILED);
    }

    @Test
    void containsQueryCallbackFailuresAndAcceptsAnEmptyCallback(@TempDir Path dir) {
        for (String mode : List.of("throw", "null-result", "reported-error", "null-row", "null-field")) {
            Path jar = Synthetic.boundedQuerySource(dir.resolve(mode), mode);
            ConnectorRef ref = new ConnectorRef(List.of(jar), "synthetic.BoundedQuery" + mode.replace('-', '_'),
                    "2.0.8", null);
            PdkBoundedSnapshotQueryPort port = new PdkBoundedSnapshotQueryPort(connectorId -> ref);
            assertThatThrownBy(() -> port.query(new BoundedSnapshotQueryRequest(
                    "source-1", "connector-1", Map.of(), new TableSchema("t1", List.of()), List.of(),
                    new AllRows(), List.of(), 1, Instant.now().plusSeconds(5))))
                    .isInstanceOf(TapstateException.class)
                    .extracting(failure -> ((TapstateException) failure).code())
                    .isEqualTo(ConnectorError.READ_FAILED);
        }

        Path jar = Synthetic.boundedQuerySource(dir.resolve("empty-results"), "empty-results");
        ConnectorRef ref = new ConnectorRef(List.of(jar), "synthetic.BoundedQueryempty_results", "2.0.8", null);
        BoundedSnapshotQueryResult empty = new PdkBoundedSnapshotQueryPort(connectorId -> ref).query(
                new BoundedSnapshotQueryRequest("source-1", "connector-1", Map.of(),
                        new TableSchema("t1", List.of()), List.of(), new AllRows(), List.of(), 1,
                        Instant.now().plusSeconds(5)));
        assertThat(empty.rows()).isEmpty();
        assertThat(empty.complete()).isTrue();
        assertThat(empty.queryCount()).isEqualTo(1);
    }

    @Test
    void measuresRegisteredTimestampCodecsWithoutLosingPortableCarriers(@TempDir Path dir) throws Exception {
        String source = """
                package synthetic;
                import io.tapdata.pdk.apis.TapConnector;
                import io.tapdata.pdk.apis.functions.ConnectorFunctions;
                import io.tapdata.entity.codec.TapCodecsRegistry;
                import io.tapdata.entity.schema.value.DateTime;
                import io.tapdata.entity.schema.value.TapDateTimeValue;
                import io.tapdata.pdk.apis.context.TapConnectionContext;
                import io.tapdata.pdk.apis.entity.ConnectionOptions;
                import io.tapdata.pdk.apis.entity.TestItem;
                import io.tapdata.pdk.apis.entity.FilterResults;
                import io.tapdata.entity.schema.TapTable;
                import io.tapdata.entity.schema.TapField;
                import org.bson.BsonTimestamp;
                import java.time.Instant;
                import java.util.List;
                import java.util.Map;
                import java.util.function.Consumer;
                public class TimestampQuery implements TapConnector {
                    public void registerCapabilities(ConnectorFunctions f, TapCodecsRegistry codecs) {
                        codecs.registerToTapValue(BsonTimestamp.class, (value, type) ->
                            new TapDateTimeValue(new DateTime(Instant.ofEpochMilli(((BsonTimestamp) value).getTime()))));
                        f.supportQueryByAdvanceFilter((context, filter, table, consumer) -> {
                            BsonTimestamp timestamp = new BsonTimestamp(1700000000, 7);
                            FilterResults result = new FilterResults();
                            result.setResults(List.of(Map.of("id", 1L, "created_at", timestamp,
                                "nested", Map.of("history", List.of(timestamp)))));
                            consumer.accept(result);
                        });
                    }
                    public void init(TapConnectionContext c) {}
                    public void stop(TapConnectionContext c) {}
                    public void discoverSchema(TapConnectionContext c, List<String> names, int limit,
                            Consumer<List<TapTable>> consumer) {
                        TapTable table = new TapTable("t1");
                        table.add(new TapField("id", "long").isPrimaryKey(true));
                        table.add(new TapField("created_at", "timestamp"));
                        consumer.accept(List.of(table));
                    }
                    public ConnectionOptions connectionTest(TapConnectionContext c, Consumer<TestItem> s) {
                        return ConnectionOptions.create();
                    }
                    public int tableCount(TapConnectionContext c) { return 1; }
                }
                """;
        Path jar = SyntheticJar.compileToJar(dir, "synthetic.TimestampQuery", source);
        Path bson = Path.of(BsonTimestamp.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        ConnectorRef ref = new ConnectorRef(List.of(jar, bson), "synthetic.TimestampQuery", "2.0.8", null);
        PdkBoundedSnapshotQueryPort port = new PdkBoundedSnapshotQueryPort(connectorId -> ref);
        BoundedSnapshotQueryRequest request = new BoundedSnapshotQueryRequest(
                "source-1", "connector-1", Map.of(), new TableSchema("t1", List.of()), List.of("id"),
                new AllRows(), List.of(), 1, Instant.now().plusSeconds(5));

        BoundedSnapshotQueryResult result = port.query(request);

        assertThat(result.complete()).isTrue();
        assertThat(result.rows()).hasSize(1);
        Map<String, Object> row = result.rows().getFirst().after();
        ConvertedValue expected = new ConvertedValue(new DateTime(Instant.ofEpochSecond(1_700_000_000)), "timestamp");
        assertThat(row.get("created_at")).isEqualTo(expected);
        assertThat(((Map<?, ?>) row.get("nested")).get("history")).isEqualTo(List.of(expected));
        BoundedSnapshotQueryRequest tooSmall = new BoundedSnapshotQueryRequest(
                "source-1", "connector-1", Map.of(), request.table(), request.stableOrder(),
                new AllRows(), List.of(), 1, 1, Instant.now().plusSeconds(5));
        assertThatThrownBy(() -> port.query(tooSmall))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code())
                .isEqualTo(ConnectorError.READ_FAILED);
    }

    @Test
    void stopsByteAccountingBeforeTraversingLaterNestedValues() {
        AtomicInteger visits = new AtomicInteger();
        Map<String, Object> later = new AbstractMap<>() {
            @Override
            public Set<Entry<String, Object>> entrySet() {
                visits.incrementAndGet();
                throw new AssertionError("the byte limit must stop traversal before this nested value");
            }
        };
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("payload", "longer than the byte budget");
        row.put("later", later);

        assertThat(PdkPreviewJsonValues.encodedSize(row, 16)).isEqualTo(17);
        assertThat(visits).hasValue(0);
    }

    private static PdkBoundedSnapshotQueryPort port(Path jar) {
        ConnectorRef ref = new ConnectorRef(List.of(jar), "synthetic.ExactTupleQuery", "2.0.8", null);
        return new PdkBoundedSnapshotQueryPort(connectorId -> ref);
    }

    private static BoundedSnapshotQueryRequest request(Selection selection, int limit) {
        return new BoundedSnapshotQueryRequest(
                "source-1", "connector-1", Map.of(), schema(), List.of("tenant_id", "order_id"),
                selection, List.of(), limit, Instant.now().plusSeconds(5));
    }

    private static TableSchema schema() {
        return new TableSchema("t1", List.of(
                new FieldSchema("tenant_id", "int"),
                new FieldSchema("order_id", "int"),
                new FieldSchema("value", "string")));
    }
}
