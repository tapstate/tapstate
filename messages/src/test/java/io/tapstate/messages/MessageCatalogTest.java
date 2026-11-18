package io.tapstate.messages;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.Severity;
import io.tapstate.core.dsl.DslError;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The error-code message catalog: the presentation-layer renderer that turns a coded exception into
 * a user-facing diagnostic via the bundled {@code messages/en.yml} catalog, substituting named
 * placeholders from the exception's arguments and falling back to the bare canonical code.
 */
class MessageCatalogTest {

    private static final MessageCatalog EN = MessageCatalog.bundled();

    @Test
    void requestDefinitionIsReadableByIndependentClientsAndHistoricalTextStaysReadable() {
        String code = io.tapstate.core.common.RequestErrorDefinitions.MALFORMED_REQUEST.code();
        var params = Map.<String, Object>of("reason", "missing id");
        var english = EN.render(code, params);
        assertThat(code).isEqualTo("TS0210001");
        assertThat(english.message()).isEqualTo("The request is malformed: missing id.");
        assertThat(english.solution()).isEqualTo("Correct the request body, then send it again.");
        assertThat(EN.render("control.malformed-request", params)).isEqualTo(english);
        assertThat(EN.render(code, params, Locale.SIMPLIFIED_CHINESE).message())
                .contains("missing id").isNotEqualTo(english.message())
                .isNotEqualTo(EN.render(code, params, Locale.TRADITIONAL_CHINESE).message());
        assertThat(EN.render(code, Map.of()).message()).isEqualTo(code);
        assertThat(EN.render("TS0219999", params).message()).isEqualTo("TS0219999");
    }

    @Test
    void currentConnectionIdentityRetainsTheSourceBoundProjection() {
        var definition = io.tapstate.core.common.ConnectorErrorDefinitions.TEST_FAILED;
        var current = new TapstateErrorCode() {
            @Override public String code() { return definition.code(); }
            @Override public Severity severity() { return definition.severity(); }
            @Override public Set<String> placeholders() { return definition.parameters(); }
        };
        var params = Map.<String, Object>of("connector", "fixture", "detail", "driver detail");
        for (var locale : java.util.List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
            assertThat(ConnectionDiagnosticCatalog.bundled().snapshot(new TapstateException(current, params, null), locale))
                    .containsEntry("code", definition.code()).containsEntry("codeSource", "tapstate-standard")
                    .containsEntry("params", params).containsKeys("message", "solution");
        }
    }

    @Test
    void connectionDefinitionsRenderThreeLanguagesWithTheSameStableIdentity() {
        var testFailed = new TapstateErrorCode() {
            @Override public String code() { return io.tapstate.core.common.ConnectorErrorDefinitions.TEST_FAILED.code(); }
            @Override public Severity severity() { return Severity.ERROR; }
            @Override public Set<String> placeholders() { return Set.of("connector", "detail"); }
        };
        var catalog = ConnectionDiagnosticCatalog.bundled();
        var params = Map.<String, Object>of("connector", "fixture", "detail", "driver detail");
        var error = new TapstateException(testFailed, params, null);
        assertThat(catalog.resolve("connector.test-failed", params, List.of(), Locale.ENGLISH)).isEmpty();
        var english = catalog.snapshot(error, Locale.ENGLISH);
        var simplified = catalog.snapshot(error, Locale.SIMPLIFIED_CHINESE);
        var traditional = catalog.snapshot(error, Locale.TRADITIONAL_CHINESE);
        for (var report : java.util.List.of(english, simplified, traditional)) {
            assertThat(report).containsEntry("code", "TS0060001")
                    .containsEntry("codeSource", "tapstate-standard").containsEntry("params", params);
            assertThat((String) report.get("message")).contains("fixture", "driver detail").doesNotContain("{detail}");
        }
        assertThat(simplified.get("message")).isNotEqualTo(english.get("message")).isNotEqualTo(traditional.get("message"));
        assertThat(catalog.snapshot(new TapstateException(testFailed, Map.of("connector", "fixture"), null), Locale.ENGLISH))
                .isEqualTo(Map.of("code", testFailed.code(), "codeSource", "tapstate-standard"));
        assertThat(catalog.snapshot(new TapstateException(DslError.UNKNOWN_FIELD, params, null), Locale.ENGLISH))
                .isEqualTo(Map.of("code", DslError.UNKNOWN_FIELD.code(), "codeSource", "tapstate-standard"));
    }

    @Test
    void connectionCatalogRejectsIncompleteEntriesAndMismatchedPlaceholders() {
        assertThatThrownBy(() -> ConnectionDiagnosticCatalog.load(path -> "{}"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("incomplete");
        assertThatThrownBy(() -> ConnectionDiagnosticCatalog.load(path ->
                MessageCatalog.read(path).replaceFirst("\"message\": \"[^\"]*\"", "\"message\": \" \"")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("invalid language entry");
        assertThatThrownBy(() -> ConnectionDiagnosticCatalog.load(path ->
                MessageCatalog.read(path).replaceFirst("\\{detail\\}", "{extra}")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("incorrect diagnostic parameters");
    }

    @Test
    void connectionCatalogPreservesExplicitLocaleAndParameterContracts() {
        String code = io.tapstate.core.common.ConnectorErrorDefinitions.TEST_FAILED.code();
        var catalog = ConnectionDiagnosticCatalog.bundled();
        var arguments = Map.<String, Object>of("connector", "orders", "detail", "Refused");
        var traditional = catalog.resolve(code, arguments, List.of(), Locale.TRADITIONAL_CHINESE).orElseThrow();
        assertThat(catalog.resolve(code, arguments, List.of(), Locale.forLanguageTag("zh-Hant")).orElseThrow())
                .isEqualTo(traditional);
        assertThat(catalog.resolve(code, arguments, List.of(), Locale.forLanguageTag("zh-HK")).orElseThrow())
                .isEqualTo(traditional);
        assertThat(catalog.resolve(code, arguments, List.of("position"), Locale.ENGLISH)).isEmpty();
        assertThat(EN.renderConnectorTestText(null, Locale.SIMPLIFIED_CHINESE)).isNull();
    }

    @Test
    void connectionKeysUseExactMembershipAndOneReportLanguage() {
        String key = "pdk.connection.fail";
        String english = EN.renderConnectorTestText(key, Locale.ENGLISH);
        String simplified = EN.renderConnectorTestText(key, Locale.SIMPLIFIED_CHINESE);
        String traditional = EN.renderConnectorTestText(key, Locale.TRADITIONAL_CHINESE);
        assertThat(english).isEqualTo("The connection could not be established.");
        assertThat(simplified).isNotEqualTo(key).isNotEqualTo(english).isNotEqualTo(traditional);
        assertThat(EN.renderConnectorTestText("10.10.0.5", Locale.TRADITIONAL_CHINESE)).isEqualTo("10.10.0.5");
    }

    @Test
    void rendersNamedPlaceholdersFromTheArgs() {
        MessageCatalog.Rendered r = EN.render(DslError.UNKNOWN_FIELD,
                Map.of("field", "srcc", "path", "source"));
        assertThat(r.message()).contains("srcc").contains("source").doesNotContain("{");
    }

    @Test
    void carriesASolutionHint() {
        MessageCatalog.Rendered r = EN.render(DslError.UNKNOWN_FIELD,
                Map.of("field", "srcc", "path", "source"));
        assertThat(r.solution()).isNotNull().isNotEmpty();
    }

    @Test
    void theRefusalAboutADottedNameCarriesTheSpellingThatAddressesIt() {
        // The one refusal that names a field the reader cannot address the obvious way, and so the one
        // whose hint has to do more than point elsewhere: saying it "can still be filtered on" and
        // stopping is a dead end, because the spelling that does it appears nowhere a reader would look.
        // Both forms, because which one they need depends on whether they are typing it or sending it.
        // Named by its literal code: this module cannot see the enum that declares it, and the golden
        // in arch-tests pins the string, so it cannot drift here unnoticed.
        MessageCatalog.Rendered r = EN.render("TS0390003",
                Map.of("field", "price\\.usd"));

        assertThat(r.solution())
                .as("the spelling as it is typed into the shell")
                .contains("price\\.usd");
        assertThat(r.solution())
                .as("and as it survives JSON, where a lone backslash is not an escape the format allows")
                .contains("\"price\\\\.usd\"");
    }

    @Test
    void theRefusalAboutADottedNameAlsoSaysWhatReadingItCostsInstead() {
        // The refusal stops an order, and then offers a filter as the way to reach the field. That offer
        // is the whole reason this hint has to name a cost: ordering is refused, so its cost is obvious,
        // while filtering succeeds and charges quietly. An index is written the same way a path is, so a
        // field whose name holds a dot cannot be indexed at all, and every read that matches on it walks
        // the collection. A hint that sends a reader down that path as though it were the free one is
        // wrong in the direction that never gets reported - the answers are right, only slow.
        MessageCatalog.Rendered r = EN.render("TS0390003",
                Map.of("field", "price\\.usd"));

        assertThat(r.solution())
                .as("that a filter on this field reads the whole collection")
                .contains("every row");
        assertThat(r.solution())
                .as("and why - so the cost reads as a property of the name, not as this read being unlucky")
                .contains("index");
    }

    @Test
    void schemaReadContentionNamesTheConnectionAndExplainsWhenToRetry() {
        MessageCatalog.Rendered rendered = EN.render("TS0480007",
                Map.of("connectionId", "orders-db"));

        assertThat(rendered.message()).contains("orders-db", "changed", "read attempt")
                .doesNotContain("{");
        assertThat(rendered.solution()).contains("re-discovery", "finish", "retry");
    }

    @Test
    void typedErrorsUseOnlyRegisteredDefinitionsWhileHistoricalTextRemainsReadable() {
        TapstateErrorCode absent = new TapstateErrorCode() {
            @Override
            public String code() {
                return "control.malformed-request";
            }

            @Override
            public Severity severity() {
                return Severity.ERROR;
            }

            @Override
            public Set<String> placeholders() {
                return Set.of();
            }
        };
        var args = Map.<String, Object>of("reason", "missing id");
        MessageCatalog.Rendered r = EN.render(absent, args);
        assertThat(r.message()).isEqualTo("control.malformed-request");
        assertThat(r.solution()).isNull();
        assertThat(EN.render(absent.code(), args).message()).contains("missing id");
    }

    @Test
    void everyDslCodeRendersFullyWithNoLeftoverPlaceholders() {
        for (DslError code : DslError.values()) {
            Map<String, Object> args = new HashMap<>();
            for (String placeholder : code.placeholders()) {
                args.put(placeholder, "X");
            }
            MessageCatalog.Rendered r = EN.render(code, args);
            assertThat(r.message())
                    .as("message for %s must be present in the catalog and fully substituted", code.code())
                    .isNotEqualTo(code.code())
                    .doesNotContain("{")
                    .doesNotContain("}");
        }
    }

    @Test
    void readerKeepsColonsInsideQuotedValues() {
        Map<String, MessageCatalog.Entry> parsed = MessageCatalog.parse("x.y:\n  message: \"a: b {z}\"\n");
        assertThat(parsed.get("x.y").message()).isEqualTo("a: b {z}");
        assertThat(parsed.get("x.y").solution()).isNull();
    }
}
