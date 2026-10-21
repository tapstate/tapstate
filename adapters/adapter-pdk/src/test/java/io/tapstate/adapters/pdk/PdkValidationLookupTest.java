package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationKeyBinding;
import io.tapstate.core.validation.ValidationValues;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class PdkValidationLookupTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 100, 12);
    @TempDir static Path directory;
    private static ConnectorRef artifact;

    @BeforeAll static void fixture() {
        Path jar = SyntheticJar.compileToJar(directory, "synthetic.EqualityLookup", """
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
                import java.util.List;
                import java.util.Map;
                import java.util.Set;
                import java.util.concurrent.atomic.AtomicInteger;
                import java.util.concurrent.atomic.AtomicReference;
                import java.util.function.Consumer;
                public class EqualityLookup implements TapConnector {
                    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
                        functions.supportQueryByAdvanceFilter((context, filter, table, consumer) -> {
                            ((AtomicInteger)context.getConnectionConfig().get("queries")).incrementAndGet();
                            if (filter.getLimit() != null || filter.getSkip() != null || filter.getBatchSize() != 2
                                    || filter.getMatch() == null || !filter.getMatch().keySet().equals(Set.of("tenant", "parent"))
                                    || filter.getSortOnList() != null && !filter.getSortOnList().isEmpty())
                                throw new IllegalStateException("not a complete conjunction lookup");
                            long tenant = ((Number)filter.getMatch().get("tenant")).longValue();
                            long parent = ((Number)filter.getMatch().get("parent")).longValue();
                            String mode = context.getConnectionConfig().getString("mode");
                            filter.getMatch().put("tenant", -999L);
                            FilterResults rows = new FilterResults();
                            if (mode.equals("empty")) { rows.setResults(List.of()); consumer.accept(rows); return; }
                            Map<String,Object> row = new java.util.LinkedHashMap<>();
                            row.put("id", 11L); row.put("tenant", tenant);
                            if (!mode.equals("missing")) row.put("parent", mode.equals("wrong") || mode.equals("swallow") ? parent + 1 : parent);
                            rows.setResults(List.of(row));
                            if (mode.equals("swallow")) {
                                try { consumer.accept(rows); } catch (RuntimeException ignored) { }
                            } else consumer.accept(rows);
                        });
                    }
                    public void init(TapConnectionContext context) {
                        ((AtomicReference<ClassLoader>)context.getConnectionConfig().get("loader")).set(getClass().getClassLoader());
                    }
                    public void stop(TapConnectionContext context) {
                        ((AtomicInteger)context.getConnectionConfig().get("stops")).incrementAndGet();
                    }
                    public void discoverSchema(TapConnectionContext context, List<String> names, int limit,
                            Consumer<List<TapTable>> consumer) {
                        TapTable table = new TapTable("items");
                        table.add(new TapField("id", "bigint").tapType(new TapNumber().fixed(true).bit(64)));
                        table.add(new TapField("tenant", "bigint").tapType(new TapNumber().fixed(true).bit(64)));
                        if (!context.getConnectionConfig().getString("mode").equals("missing-model"))
                            table.add(new TapField("parent", "bigint").tapType(new TapNumber().fixed(true).bit(64)));
                        consumer.accept(List.of(table));
                    }
                    public ConnectionOptions connectionTest(TapConnectionContext context, Consumer<TestItem> consumer) {
                        return ConnectionOptions.create();
                    }
                    public int tableCount(TapConnectionContext context) { return 1; }
                }
                """);
        artifact = new ConnectorRef(List.of(jar), "synthetic.EqualityLookup", "2.0.8", null);
    }

    @Test @Timeout(10) void conjunctionUsesFreshMutableNativeMapAndChecksActualReturnedFields() {
        var probe = new Probe("valid");
        Map<String, Object> encoded = Map.of("tenant", 7L, "parent", 102L);
        try (var query = Executors.newSingleThreadExecutor(); var decode = Executors.newSingleThreadExecutor();
             var cursor = PdkValidationSession.lookup("fixture", artifact, probe.settings(), "binding", lookup(),
                     (model, codecs) -> row -> ValidationValues.ownRow(row, ROW),
                     (model, codecs, logical) -> {
                         assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(probe.loader.get());
                         assertThat(model.getNameFieldMap()).containsKeys("tenant", "parent");
                         assertThat(logical).isEqualTo(key());
                         return encoded;
                     }, () -> {}, query, decode, 1, ROW)) {
            try (var row = cursor.next().orElseThrow()) {
                assertThat(row.values().fields()).containsEntry("tenant", 7L).containsEntry("parent", 102L).containsEntry("id", 11L);
            }
            assertThat(cursor.next()).isEmpty();
        }
        assertThat(encoded).containsEntry("tenant", 7L);
        assertThat(probe.queries).hasValue(1);
        assertThat(probe.stops).hasValue(1);
    }

    @Test @Timeout(10) void wrongMissingAndSwallowedReturnKeysNeverBecomeMatchingFacts() {
        for (String mode : List.of("wrong", "missing", "swallow")) {
            var probe = new Probe(mode);
            try (var query = Executors.newSingleThreadExecutor(); var decode = Executors.newSingleThreadExecutor();
                 var cursor = PdkValidationSession.lookup("fixture", artifact, probe.settings(), "binding", lookup(),
                         (model, codecs) -> row -> ValidationValues.ownRow(row, ROW),
                         (model, codecs, logical) -> Map.of("tenant", 7L, "parent", 102L), () -> {}, query, decode, 1, ROW)) {
                assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ValidationError.READ_PROTOCOL_VIOLATION));
            }
            assertThat(probe.queries).hasValue(1);
            assertThat(probe.stops).hasValue(1);
        }
    }

    @Test @Timeout(10) void completeEmptyLookupDoesNotFallBackToAScan() {
        var probe = new Probe("empty");
        try (var query = Executors.newSingleThreadExecutor(); var decode = Executors.newSingleThreadExecutor();
             var cursor = PdkValidationSession.lookup("fixture", artifact, probe.settings(), "binding", lookup(),
                     (model, codecs) -> row -> ValidationValues.ownRow(row, ROW),
                     (model, codecs, logical) -> Map.of("tenant", 7L, "parent", 102L), () -> {}, query, decode, 1, ROW)) {
            assertThat(cursor.next()).isEmpty();
        }
        assertThat(probe.queries).hasValue(1);
    }

    @Test @Timeout(10) void missingPredicateModelFailsBeforeAnyEncoderOrBusinessQuery() {
        var probe = new Probe("missing-model");
        var encodings = new AtomicInteger();
        try (var query = Executors.newSingleThreadExecutor(); var decode = Executors.newSingleThreadExecutor();
             var cursor = PdkValidationSession.lookup("fixture", artifact, probe.settings(), "binding", lookup(),
                     (model, codecs) -> row -> ValidationValues.ownRow(row, ROW),
                     (model, codecs, logical) -> { encodings.incrementAndGet(); return Map.of("tenant", 7L, "parent", 102L); },
                     () -> {}, query, decode, 1, ROW)) {
            assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(ValidationError.READ_PROTOCOL_VIOLATION));
        }
        assertThat(encodings).hasValue(0);
        assertThat(probe.queries).hasValue(0);
    }

    @Test @Timeout(10) void encoderCannotWidenTheLookupByOmittingFieldsOrAddingNulls() {
        var nullable = new HashMap<String, Object>(); nullable.put("tenant", 7L); nullable.put("parent", null);
        for (var invalid : List.of(Map.<String, Object>of(), Map.<String, Object>of("tenant", 7L), nullable,
                Map.<String, Object>of("tenant", 7L, "other", 102L))) {
            var probe = new Probe("valid");
            try (var query = Executors.newSingleThreadExecutor(); var decode = Executors.newSingleThreadExecutor();
                 var cursor = PdkValidationSession.lookup("fixture", artifact, probe.settings(), "binding", lookup(),
                         (model, codecs) -> row -> ValidationValues.ownRow(row, ROW),
                         (model, codecs, logical) -> invalid, () -> {}, query, decode, 1, ROW)) {
                assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ValidationError.INVALID_CONTRACT));
            }
            assertThat(probe.queries).hasValue(0);
        }
    }

    @Test void requiredMatchColumnsCannotBeHiddenByProjectionOrImplicitNestedNames() {
        assertThatThrownBy(() -> new PdkValidationSession.Lookup("items", List.of(), List.of("id"), 2, binding(), key()))
                .isInstanceOf(IllegalArgumentException.class);
        var nested = new ValidationKeyBinding("nested", List.of(new ValidationKeyBinding.Component(List.of("parent", "id"),
                ValidationKey.Kind.EXACT_NUMBER, Set.of())), Set.of(), ROW);
        assertThatThrownBy(() -> new PdkValidationSession.Lookup("items", List.of(), List.of(), 2, nested,
                new ValidationKey(List.of(ValidationKey.Part.number(1))))).isInstanceOf(IllegalArgumentException.class);
    }

    private static PdkValidationSession.Lookup lookup() {
        return new PdkValidationSession.Lookup("items", List.of(), List.of("id", "tenant", "parent"), 2, binding(), key());
    }
    private static ValidationKeyBinding binding() {
        return new ValidationKeyBinding("parents", List.of(
                new ValidationKeyBinding.Component(List.of("tenant"), ValidationKey.Kind.EXACT_NUMBER, Set.of()),
                new ValidationKeyBinding.Component(List.of("parent"), ValidationKey.Kind.EXACT_NUMBER, Set.of())), Set.of(), ROW);
    }
    private static ValidationKey key() { return new ValidationKey(List.of(ValidationKey.Part.number(7), ValidationKey.Part.number(102))); }
    private static final class Probe {
        private final String mode;
        private final AtomicInteger queries = new AtomicInteger();
        private final AtomicInteger stops = new AtomicInteger();
        private final AtomicReference<ClassLoader> loader = new AtomicReference<>();
        private Probe(String mode) { this.mode = mode; }
        private Map<String, Object> settings() { return Map.of("mode", mode, "queries", queries, "stops", stops, "loader", loader); }
    }
}
