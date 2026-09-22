package io.tapstate.control.core;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SourceDraftServiceTest {

    @Test
    void rendersAValidatedSourceWithoutPersistingIt() {
        SourceDraftResult result = service().draft(draft(null, null, null, List.of()));

        assertThat(result.yaml()).contains("kind: source", "id: orders", "connector: mysql");
    }

    @Test
    void rendersAllSupportedTableFormsAndRemovesClearedSecrets() {
        SourceDraft draft = draft(null, List.of(
                new SourceTableDraft("literal", "orders", null, null, null, null),
                new SourceTableDraft("regex", null, "order_.*", null, null, null),
                new SourceTableDraft("spec", "order_items", null, "status == 'OPEN'",
                        List.of("id"), null)),
                null, List.of("password"));

        SourceDraftResult result = service().draft(draft);

        assertThat(result.yaml()).contains("orders", "order_.*", "order_items")
                .doesNotContain("password");
    }

    @Test
    void acceptsSnapshotAndCdcModesAndStructuredSrs() {
        assertThatCode(() -> service().draft(draft("snapshot", null, null, List.of()))).doesNotThrowAnyException();
        SourceDraftResult result = service().draft(draft("cdc", null,
                new SourceDraft.SourceSrs("orders", "7d", "track", true, true), List.of()));

        assertThat(result.yaml()).contains("mode: cdc", "schema_evolution: track", "queryable: true");
    }

    @Test
    void rejectsUnknownModesAndSrsEvolutionValues() {
        assertDraftRejected(draft("unknown", null, null, List.of()), "unknown Source mode");
        assertDraftRejected(draft("cdc", null,
                new SourceDraft.SourceSrs("orders", null, "future", null, null), List.of()),
                "unknown srs.schemaEvolution");
    }

    @Test
    void rejectsInvalidTableSelectorsBeforeConnectorValidation() {
        assertDraftRejected(draft(null, Collections.singletonList(null), null, List.of()), "null entries");
        assertDraftRejected(draft(null, List.of(table("other", "orders", null)), null, List.of()),
                "unknown table type");
        assertDraftRejected(draft(null, List.of(table("literal", "orders", "order_.*")), null, List.of()),
                "literal tables accept only name");
        assertDraftRejected(draft(null, List.of(table("regex", "orders", "order_.*")), null, List.of()),
                "regex tables accept only pattern");
        assertDraftRejected(draft(null, List.of(table("spec", "orders", "order_.*")), null, List.of()),
                "spec tables require name");
    }

    @Test
    void rejectsMutableNumberImplementationsAtTheImmutableInputBoundary() {
        AtomicInteger mutable = new AtomicInteger(3306);

        assertInvalidConfig(Map.of("port", mutable), "unsupported JSON value type");
    }

    @Test
    void enforcesTheCompleteJsonValueBoundary() {
        SourceDraft empty = new SourceDraft(
                "orders", null, "mysql", null, null, null, null, null, null, null);
        assertThat(empty.config()).isEmpty();

        Map<String, Object> nullKey = new LinkedHashMap<>();
        nullKey.put(null, "value");
        assertInvalidConfig(nullKey, "keys must not be null");
        assertInvalidConfig(Map.of("value", Double.NaN), "numbers must be finite");
        assertInvalidConfig(Map.of("value", Float.POSITIVE_INFINITY), "numbers must be finite");
        assertInvalidConfig(Map.of("value", new Object()), "unsupported JSON value type");

        Map<Object, Object> invalidNested = new LinkedHashMap<>();
        invalidNested.put(1, "value");
        Map<String, Object> nestedConfig = new LinkedHashMap<>();
        nestedConfig.put("nested", invalidNested);
        assertInvalidConfig(nestedConfig, "keys must be strings");
    }

    private static void assertInvalidConfig(Map<String, Object> config, String message) {
        assertThatThrownBy(() -> new SourceDraft(
                "orders", null, "mysql", config, null, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(message);
    }

    private static void assertDraftRejected(SourceDraft draft, String message) {
        assertThatThrownBy(() -> service().draft(draft))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining(message);
    }

    private static SourceDraftService service() {
        return new SourceDraftService(TapstateCatalog.load());
    }

    private static SourceDraft draft(
            String mode,
            List<SourceTableDraft> tables,
            SourceDraft.SourceSrs srs,
            List<String> clearSecrets) {
        Map<String, Object> config = new java.util.LinkedHashMap<>(Map.of(
                "host", "localhost", "port", 3306, "database", "orders", "username", "root",
                "password", "secret"));
        return new SourceDraft(
                "orders", null, "mysql", config, mode, tables, null, srs, null, clearSecrets);
    }

    private static SourceTableDraft table(String type, String name, String pattern) {
        return new SourceTableDraft(type, name, pattern, null, null, null);
    }
}
