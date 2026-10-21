package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.type.TapNumber;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationPhysicalIdentity;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.core.validation.ValidationValueComparison;
import io.tapstate.runtime.engine.validation.ValidationOrderedGroups;
import io.tapstate.runtime.engine.validation.ValidationOrderedMerge;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

/** Actual plugin loading and both bounded PDK cursors feed the runtime merge; no database is simulated as acceptance. */
class ValidationReaderMergeTest {
    @Test @Timeout(10)
    void twoBackpressuredSessionsCompleteWithTheirOwnQueryAndDecodeSlots(@TempDir Path directory) {
        var path = SyntheticJar.compileToJar(directory, "synthetic.OrderedValidationRead", """
                package synthetic;
                import io.tapdata.pdk.apis.TapConnector;
                import io.tapdata.pdk.apis.functions.ConnectorFunctions;
                import io.tapdata.entity.codec.TapCodecsRegistry;
                import io.tapdata.pdk.apis.context.TapConnectionContext;
                import io.tapdata.pdk.apis.entity.ConnectionOptions;
                import io.tapdata.pdk.apis.entity.TestItem;
                import io.tapdata.pdk.apis.entity.FilterResults;
                import io.tapdata.entity.schema.TapTable;
                import io.tapdata.entity.schema.TapField;
                import io.tapdata.entity.schema.type.TapNumber;
                import java.util.ArrayList;
                import java.util.List;
                import java.util.Map;
                import java.util.concurrent.atomic.AtomicInteger;
                import java.util.function.Consumer;
                public class OrderedValidationRead implements TapConnector {
                    public void registerCapabilities(ConnectorFunctions f, TapCodecsRegistry codecs) {
                        f.supportQueryByAdvanceFilter((c, filter, table, consumer) -> {
                            List<Long> ids = (List<Long>) c.getConnectionConfig().get("ids");
                            String key = c.getConnectionConfig().getString("key");
                            if (filter.getLimit() != null || filter.getSkip() != null)
                                throw new IllegalStateException("bounded preview is not a full read");
                            for (int start = 0; start < ids.size(); start += 3) {
                                List<Map<String,Object>> batch = new ArrayList<>();
                                for (int i = start; i < Math.min(start + 3, ids.size()); i++) {
                                    long amount = key.equals("target_id") && ids.get(i) == 5L ? 100L : 120L;
                                    batch.add(Map.of(key, ids.get(i), "amount", amount));
                                }
                                FilterResults results = new FilterResults(); results.setResults(batch);
                                consumer.accept(results);
                            }
                        });
                    }
                    public void init(TapConnectionContext c) {}
                    public void stop(TapConnectionContext c) {
                        ((AtomicInteger)c.getConnectionConfig().get("stops")).incrementAndGet();
                    }
                    public void discoverSchema(TapConnectionContext c, List<String> names, int limit,
                            Consumer<List<TapTable>> consumer) {
                        TapTable table = new TapTable(names.get(0));
                        table.add(new TapField(c.getConnectionConfig().getString("key"), "long")
                            .tapType(new TapNumber().fixed(true).bit(64)));
                        table.add(new TapField("amount", "long").tapType(new TapNumber().fixed(true).bit(64)));
                        consumer.accept(List.of(table));
                    }
                    public ConnectionOptions connectionTest(TapConnectionContext c, Consumer<TestItem> s) {
                        return ConnectionOptions.create();
                    }
                    public int tableCount(TapConnectionContext c) { return 1; }
                }
                """);
        var artifact = new ConnectorRef(List.of(path), "synthetic.OrderedValidationRead", "2.0.8", null);
        var limits = new ValidationValues.Limits(4096, 50, 8);
        var sourceStops = new AtomicInteger();
        var targetStops = new AtomicInteger();
        var facts = new ArrayList<String>();
        var valueChecks = new ArrayList<String>();
        // Each live read holds a query slot and a separately admitted decode slot for its duration.
        try (var queryIo = Executors.newFixedThreadPool(2); var decodeIo = Executors.newFixedThreadPool(2);
             var source = PdkValidationSession.scan("fixture", artifact,
                     Map.of("key", "source_id", "ids", List.of(1L, 3L, 3L, 5L, 6L, 7L), "stops", sourceStops),
                     "fixed-artifact", new PdkValidationSession.Scan("orders", List.of("source_id"), 2),
                     (model, codecs) -> {
                         assertThat(model.getNameFieldMap().get("source_id").getTapType()).isInstanceOf(TapNumber.class);
                         return new PdkValidationRows(model, codecs, pathRule -> PdkValidationRows.Rule.AUTO, limits);
                     }, () -> { }, queryIo, decodeIo, 1, limits);
             var target = PdkValidationSession.scan("fixture", artifact,
                     Map.of("key", "target_id", "ids", List.of(2L, 3L, 5L, 6L, 8L), "stops", targetStops),
                     "fixed-artifact", new PdkValidationSession.Scan("orders_v2", List.of("target_id"), 2),
                     (model, codecs) -> new PdkValidationRows(model, codecs, pathRule -> PdkValidationRows.Rule.AUTO, limits), () -> { },
                     queryIo, decodeIo, 1, limits);
             var merge = new ValidationOrderedMerge(
                     new ValidationOrderedGroups("source", source, row -> key(row, "source_id", "orders"), limits.bytes()),
                     new ValidationOrderedGroups("target", target, row -> key(row, "target_id", "orders_v2"), limits.bytes()))) {
            while (true) {
                var next = merge.next();
                if (next.isEmpty()) break;
                try (var pair = next.orElseThrow()) {
                    facts.add(pair.state() + ":" + pair.source().map(group -> group.occurrences()).orElse(0L)
                            + ":" + pair.target().map(group -> group.occurrences()).orElse(0L));
                    pair.source().ifPresent(group -> assertThat(group.key().physical().table()).isEqualTo("orders"));
                    pair.target().ifPresent(group -> assertThat(group.key().physical().table()).isEqualTo("orders_v2"));
                    if (pair.source().isPresent() && pair.target().isPresent()
                            && pair.source().orElseThrow().occurrences() == 1 && pair.target().orElseThrow().occurrences() == 1) {
                        var left = pair.source().orElseThrow().sampleValues().fields();
                        var right = pair.target().orElseThrow().sampleValues().fields();
                        // Physical key mapping was checked separately; this controlled fixture has one
                        // common payload field. Model conformance and general mapping are not inferred.
                        var compared = ValidationValueComparison.compare(
                                ValidationValues.ownRow(Map.of("amount", left.get("amount")), limits),
                                ValidationValues.ownRow(Map.of("amount", right.get("amount")), limits));
                        valueChecks.add(((ValidationValues.Typed)left.get("source_id")).value() + ":" + compared.map(diff -> diff.kind().toString()).orElse("EQUAL"));
                        compared.ifPresent(diff -> assertThat(diff.path()).containsExactly("amount"));
                    }
                }
            }
        }
        assertThat(facts).containsExactly("SOURCE_ONLY:1:0", "TARGET_ONLY:0:1", "MATCHED:2:1", "MATCHED:1:1", "MATCHED:1:1",
                "SOURCE_ONLY:1:0", "TARGET_ONLY:0:1");
        assertThat(valueChecks).containsExactly("5:VALUE", "6:EQUAL");
        assertThat(sourceStops).hasValue(1);
        assertThat(targetStops).hasValue(1);
    }

    private static ValidationOrderedGroups.Key key(ValidationValues.Row row, String field, String table) {
        var key = new ValidationKey(List.of(ValidationKey.Part.number((Long)((ValidationValues.Typed)row.fields().get(field)).value())));
        return new ValidationOrderedGroups.Key(key, new ValidationPhysicalIdentity(field + "-connection",
                "database", null, table, List.of(List.of(field)), key));
    }
}
