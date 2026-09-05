package io.tapstate.control.core;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Srs;
import io.tapstate.core.model.SrsSchemaEvolution;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SourceRepresentationTest {

    private static final String SECRET = "sentinel-secret-value";

    private final SourceRepresentation representation =
            new SourceRepresentation(TapstateCatalog.load());

    @Test
    void refusesADraftCarryingOptions() {
        // Options are the engine's own configuration and its vocabulary is empty today, so the model
        // has nowhere to put one. Refusing beats dropping: a draft that keeps its options through the
        // API and loses them on the way into the model reads as accepted and configures nothing.
        SourceDraft draft = new SourceDraft(
                "orders", null, "mysql", linkedMap("host", "mysql.internal"), "cdc", null,
                Map.of("readPreference", "secondary"), null, null, List.of());

        assertThatThrownBy(() -> representation.toModel(draft, null))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST);
                    assertThat(error.args()).containsOnlyKeys("reason");
                });
    }

    @Test
    void refusesATableDraftCarryingOptions() {
        SourceDraft draft = new SourceDraft(
                "orders", null, "mysql", linkedMap("host", "mysql.internal"), "cdc",
                List.of(new SourceTableDraft("spec", "customers", null, null, List.of("id"),
                        Map.of("batch", 100))),
                null, null, null, List.of());

        assertThatThrownBy(() -> representation.toModel(draft, null))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
    }

    @Test
    void mapsEverySourceFieldAndNormalizesTableAndEnumSpellings() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("enabled", true);
        nested.put("threshold", new BigDecimal("1.25"));
        List<Object> values = new ArrayList<>(List.of("raw", 7L, false, nested));

        SourceDraft draft = new SourceDraft(
                "orders",
                new Metadata(Map.of("team", "finance"), "Orders CDC source"),
                "mysql",
                linkedMap(
                        "host", "mysql.internal",
                        "port", 3306,
                        "json", values),
                "cdc",
                List.of(
                        new SourceTableDraft("literal", "orders", null, null, null, null),
                        new SourceTableDraft("regex", null, "audit_.*", null, null, null),
                        new SourceTableDraft(
                                "spec",
                                "customers",
                                null,
                                "active == true",
                                List.of("id"),
                                null)),
                null,
                new SourceDraft.SourceSrs(
                        "mysql-primary", "24h", "track", false, true),
                Map.of("preview", List.of(1, 2, 3)),
                List.of());

        SourceResource model = representation.toModel(draft, null);
        SourceView view = representation.toView(
                model, CanonicalHash.of(model));

        assertThat(model.id()).isEqualTo("orders");
        assertThat(model.metadata()).isEqualTo(draft.metadata());
        assertThat(model.connector()).isEqualTo("mysql");
        assertThat(model.mode()).isEqualTo(SourceMode.CDC);
        assertThat(model.tables()).containsExactly(
                TableRef.literal("orders"),
                TableRef.regex("audit_.*"),
                TableRef.spec(
                        "customers",
                        "active == true",
                        List.of("id")));
        assertThat(model.srs()).isEqualTo(
                new Srs("mysql-primary", "24h", SrsSchemaEvolution.TRACK, false, true));
        assertThat(model.experimental()).isEqualTo(draft.experimental());
        assertThat(model.config().get("json")).isEqualTo(values);
        assertThat(((List<?>) model.config().get("json")).get(1)).isInstanceOf(Long.class);
        assertThat(((Map<?, ?>) ((List<?>) model.config().get("json")).get(3)).get("threshold"))
                .isEqualTo(new BigDecimal("1.25"));

        assertThat(view.mode()).isEqualTo("cdc");
        assertThat(view.tables()).extracting(SourceTableView::type)
                .containsExactly("literal", "regex", "spec");
        assertThat(view.srs().schemaEvolution()).isEqualTo("track");
        assertThat(view.options()).isEmpty();
        assertThat(view.experimental()).isEqualTo(draft.experimental());
        assertThat(view.contentHash()).matches("[0-9a-f]{64}");
    }

    @Test
    void defensivelyCopiesJsonValues() {
        List<Object> inner = new ArrayList<>(List.of("original"));
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("nested", inner);
        List<String> pk = new ArrayList<>(List.of("id"));
        List<Object> optionValues = new ArrayList<>(List.of("first"));
        Map<String, Object> tableOptions = new LinkedHashMap<>();
        tableOptions.put("values", optionValues);
        List<SourceTableDraft> tables = new ArrayList<>();
        tables.add(new SourceTableDraft(
                "spec", "orders", null, null, pk, tableOptions));

        SourceDraft draft = new SourceDraft(
                "orders", null, "mysql", config, "snapshot", tables,
                null, null, null, null);
        inner.add("changed");
        config.put("late", true);
        pk.add("late");
        optionValues.add("changed");
        tableOptions.put("late", true);
        tables.clear();

        assertThat(draft.config()).containsOnlyKeys("nested");
        assertThat(draft.config().get("nested")).isEqualTo(List.of("original"));
        assertThat(draft.tables()).hasSize(1);
        assertThat(draft.tables().getFirst().pk()).containsExactly("id");
        assertThat(draft.tables().getFirst().options()).containsOnlyKeys("values");
        assertThat(draft.tables().getFirst().options().get("values"))
                .isEqualTo(List.of("first"));
        assertThatThrownBy(() -> ((List<Object>) draft.config().get("nested")).add("x"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> draft.tables().getFirst().pk().add("x"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> draft.tables().getFirst().options().put("x", true))
                .isInstanceOf(UnsupportedOperationException.class);

        List<String> configuredSecrets = new ArrayList<>(List.of("password"));
        SourceView view = new SourceView(
                "orders", null, "mysql", config, configuredSecrets, "snapshot",
                null, null, null, null, "a".repeat(64));
        configuredSecrets.clear();
        config.put("afterView", true);
        assertThat(view.config()).doesNotContainKey("afterView");
        assertThat(view.configuredSecrets()).containsExactly("password");
        assertThatThrownBy(() -> view.configuredSecrets().add("x"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void redactsSecretsAndReportsConfiguredNamesDeterministically() {
        SourceResource model = source(
                "mysql",
                linkedMap("password", SECRET, "host", "localhost"));

        SourceView view = representation.toView(model, hash(model));

        assertThat(view.config()).containsOnly(Map.entry("host", "localhost"));
        assertThat(view.configuredSecrets()).containsExactly("password");
        assertThat(view.toString()).doesNotContain(SECRET);
    }

    @Test
    void preservesAnOmittedConfiguredSecretOnReplace() {
        SourceResource existing = source(
                "mysql",
                linkedMap("host", "before", "password", SECRET));
        SourceDraft draft = draft("mysql", Map.of("host", "after"), List.of());

        SourceResource model = representation.toModel(draft, existing);

        assertThat(model.config()).containsEntry("host", "after");
        assertThat(model.config()).containsEntry("password", SECRET);
    }

    @Test
    void replacesASuppliedSecret() {
        SourceResource existing = source(
                "mysql",
                linkedMap("host", "before", "password", SECRET));
        SourceDraft draft = draft(
                "mysql", linkedMap("host", "after", "password", "replacement"), List.of());

        SourceResource model = representation.toModel(draft, existing);

        assertThat(model.config()).containsEntry("password", "replacement");
    }

    @Test
    void clearsAnOptionalSecretExplicitly() {
        SourceResource existing = source(
                "mysql",
                linkedMap("host", "before", "password", SECRET));
        SourceDraft draft = draft("mysql", Map.of("host", "after"), List.of("password"));

        SourceResource model = representation.toModel(draft, existing);

        assertThat(model.config()).doesNotContainKey("password");
        assertThat(representation.toView(model, hash(model)).configuredSecrets()).isEmpty();
    }

    @Test
    void rejectsSecretPresentInBothConfigAndClearSecretsWithoutLeakingItsValue() {
        SourceDraft draft = draft(
                "mysql", Map.of("password", SECRET), List.of("password"));

        assertThatThrownBy(() -> representation.toModel(draft, null))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST);
                    assertThat(error).hasMessageNotContaining(SECRET);
                });
        assertThat(draft.toString()).doesNotContain(SECRET);
    }

    @Test
    void rejectsUnknownClearSecretField() {
        SourceDraft draft = draft("mysql", Map.of(), List.of("notASecret"));

        assertMalformed(draft);
    }

    @Test
    void rejectsClearingARequiredSecret() {
        SourceDraft draft = draft("hazelcast", Map.of(), List.of("password"));

        assertMalformed(draft);
    }

    @Test
    void rejectsNullInputListEntriesAsCodedMalformedRequests() {
        List<SourceTableDraft> tables = new ArrayList<>();
        tables.add(null);
        SourceDraft nullTable = new SourceDraft(
                "orders", null, "mysql", Map.of(), "snapshot", tables,
                null, null, null, null);

        List<String> clearSecrets = new ArrayList<>();
        clearSecrets.add(null);
        SourceDraft nullClearSecret = draft("mysql", Map.of(), clearSecrets);

        List<String> pk = new ArrayList<>();
        pk.add(null);
        SourceDraft nullPk = new SourceDraft(
                "orders", null, "mysql", Map.of(), "snapshot",
                List.of(new SourceTableDraft("spec", "orders", null, null, pk, null)),
                null, null, null, null);

        assertMalformed(nullTable);
        assertMalformed(nullClearSecret);
        assertMalformed(nullPk);
    }

    @Test
    void rejectsANullSuppliedSecretInsteadOfTreatingItAsConfigured() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("host", null);
        config.put("password", null);
        SourceDraft draft = draft("mysql", config, List.of());

        assertThatThrownBy(() -> representation.toModel(draft, null))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST);
                    assertThat(error.args()).containsOnlyKeys("reason");
                });
    }

    @Test
    void doesNotPreserveOrReportANullLegacySecretValue() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("host", "before");
        config.put("password", null);
        SourceResource existing = source("mysql", config);

        SourceResource replacement = representation.toModel(
                draft("mysql", Map.of("host", "after"), List.of()), existing);
        SourceView existingView = representation.toView(existing, "a".repeat(64));

        assertThat(replacement.config()).doesNotContainKey("password");
        assertThat(existingView.configuredSecrets()).isEmpty();
        assertThat(existingView.config()).containsEntry("host", "before");
    }

    @Test
    void rejectsUnknownEnumSpellings() {
        SourceDraft badMode = new SourceDraft(
                "orders", null, "mysql", Map.of(), "CDC", null, null, null, null, null);
        SourceDraft badSrs = new SourceDraft(
                "orders", null, "mysql", Map.of(), "cdc", null, null,
                new SourceDraft.SourceSrs(null, null, "TRACK", null, null), null, null);

        assertMalformed(badMode);
        assertMalformed(badSrs);
    }

    @Test
    void rejectsImpossibleTableDiscriminatorAndFieldCombinations() {
        List<SourceTableDraft> invalid = List.of(
                new SourceTableDraft("literal", "orders", "also-pattern", null, null, null),
                new SourceTableDraft("regex", "also-name", "orders.*", null, null, null),
                new SourceTableDraft("spec", "orders", "also-pattern", null, null, null),
                new SourceTableDraft("other", "orders", null, null, null, null));

        for (SourceTableDraft table : invalid) {
            SourceDraft draft = new SourceDraft(
                    "orders", null, "mysql", Map.of(), "snapshot", List.of(table),
                    null, null, null, null);
            assertMalformed(draft);
        }
    }

    private void assertMalformed(SourceDraft draft) {
        assertThatThrownBy(() -> representation.toModel(draft, null))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
    }

    private static SourceDraft draft(
            String connector, Map<String, Object> config, List<String> clearSecrets) {
        return new SourceDraft(
                "orders", null, connector, config, "snapshot", null,
                null, null, null, clearSecrets);
    }

    private static SourceResource source(String connector, Map<String, Object> config) {
        return new SourceResource(
                "orders", null, connector, config, SourceMode.SNAPSHOT,
                null, null, null);
    }

    private static String hash(SourceResource source) {
        return CanonicalHash.of(source);
    }

    private static Map<String, Object> linkedMap(Object... keyValues) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            result.put((String) keyValues[i], keyValues[i + 1]);
        }
        return result;
    }
}
