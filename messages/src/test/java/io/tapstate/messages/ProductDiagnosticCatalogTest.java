package io.tapstate.messages;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProductDiagnosticCatalogTest {
    private static final String CODE = "TS0210001";
    private static final ProductDiagnosticCatalog.Definition DEFINITION =
            new ProductDiagnosticCatalog.Definition(CODE, Set.of("reason"));
    private static final String RESOURCE = """
            {"TS0210001":{"message":"Invalid request: {reason}","solution":"Correct the request."}}
            """;

    @Test
    void anOwnerRegistersDefinitionsWithoutAServiceDependencyInTheRenderer() {
        TapstateErrorCode owner = new TapstateErrorCode() {
            @Override public String code() { return CODE; }
            @Override public Severity severity() { return Severity.ERROR; }
            @Override public Set<String> placeholders() { return Set.of("reason"); }
        };
        var catalog = ProductDiagnosticCatalog.load("021", List.of(ProductDiagnosticCatalog.Definition.of(owner)),
                language -> RESOURCE.replace("Invalid request", language + " request"));
        var args = Map.<String, Object>of("reason", "missing id");
        assertThat(catalog.codes()).containsExactly(CODE);
        for (Locale language : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
            var result = catalog.resolve(CODE, args, List.of(), language).orElseThrow();
            assertThat(result.code()).isEqualTo(CODE);
            assertThat(result.message()).isEqualTo(ProductDiagnosticCatalog.language(language) + " request: missing id");
            assertThat(result.solution()).isEqualTo("Correct the request.");
        }
        assertThat(catalog.resolve(CODE, args, List.of(), Locale.forLanguageTag("zh-Hant")))
                .isEqualTo(catalog.resolve(CODE, args, List.of(), Locale.TRADITIONAL_CHINESE));
        assertThat(catalog.resolve(CODE, args, List.of(), Locale.GERMAN))
                .isEqualTo(catalog.resolve(CODE, args, List.of(), Locale.ENGLISH));
    }

    @Test
    void explicitlyTechnicalParametersRemainRequiredWithoutEnteringTheHumanSummary() {
        var definition = new ProductDiagnosticCatalog.Definition(CODE, Set.of("reason", "detail"));
        String resource = "{\"TS0210001\":{\"message\":\"Invalid request: {reason}\",\"solution\":\"Correct the request.\",\"technicalParams\":[\"detail\"]}}";
        var catalog = ProductDiagnosticCatalog.load("021", List.of(definition), language -> resource);
        var params = Map.<String, Object>of("reason", "missing id", "detail", "original-driver-detail");
        var result = catalog.resolve(CODE, params, List.of(), Locale.ENGLISH).orElseThrow();
        assertThat(result.message()).isEqualTo("Invalid request: missing id");
        assertThat(result.message() + result.solution()).doesNotContain("original-driver-detail");
        assertThat(params).containsEntry("detail", "original-driver-detail");
        assertThat(catalog.resolve(CODE, Map.of("reason", "missing id"), List.of(), Locale.ENGLISH)).isEmpty();
        for (String invalid : List.of("[\"unknown\"]", "[\"detail\",\"detail\"]", "\"detail\"", "[null]"))
            assertThatThrownBy(() -> ProductDiagnosticCatalog.load("021", List.of(definition),
                    language -> resource.replace("[\"detail\"]", invalid)))
                    .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void historicalNoParameterTextIsExplicitAndNeverCompletesTheDefinition() {
        String fallback = "\"historicalWithoutParameters\":{\"message\":\"Old failure without recorded facts.\",\"solution\":\"Inspect the original operation.\"}";
        String resource = RESOURCE.replace("\"solution\":\"Correct the request.\"", "\"solution\":\"Correct the request.\"," + fallback);
        var catalog = ProductDiagnosticCatalog.load("021", List.of(DEFINITION), language -> resource);
        assertThat(catalog.renderHistoricalWithoutParameters(CODE, Map.of(), Locale.ENGLISH)
                .orElseThrow().message()).isEqualTo("Old failure without recorded facts.");
        assertThat(catalog.resolve(CODE, Map.of(), List.of(), Locale.ENGLISH)).isEmpty();
        assertThat(catalog.renderHistoricalWithoutParameters(CODE, Map.of("reason", "partial"), Locale.ENGLISH)).isEmpty();
        assertThat(catalog.renderHistoricalWithoutParameters("TS0219999", Map.of(), Locale.ENGLISH)).isEmpty();
        assertThat(catalog.resolve(CODE, Map.of("reason", "original"), List.of(), Locale.ENGLISH)
                .orElseThrow().message()).isEqualTo("Invalid request: original");
        assertThatThrownBy(() -> ProductDiagnosticCatalog.load("021", List.of(DEFINITION),
                language -> resource.replace("Old failure without recorded facts.", "Missing {reason}")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("requires parameters");
        assertThatThrownBy(() -> ProductDiagnosticCatalog.load("021", List.of(DEFINITION),
                language -> language.equals("en") ? resource : RESOURCE))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("translations");
        assertThat(ProductDiagnosticCatalog.load("021", List.of(DEFINITION), language -> RESOURCE)
                .renderHistoricalWithoutParameters(CODE, Map.of(), Locale.ENGLISH)).isEmpty();
    }

    @Test
    void unknownLegacyAndPositionalInputsAreLeftToTheirOwningAdapter() {
        var catalog = ProductDiagnosticCatalog.load("021", List.of(DEFINITION), language -> RESOURCE);
        var args = Map.<String, Object>of("reason", "missing id");
        assertThat(catalog.resolve("control.malformed-request", args, List.of(), Locale.ENGLISH)).isEmpty();
        assertThat(catalog.resolve("TS0219999", args, List.of(), Locale.ENGLISH)).isEmpty();
        assertThat(catalog.resolve(CODE, args, List.of("extra"), Locale.ENGLISH)).isEmpty();
        assertThat(catalog.resolve(CODE, Map.of(), List.of(), Locale.ENGLISH)).isEmpty();
        assertThat(catalog.resolve(CODE, Map.of("reason", "missing", "secret", "private"), List.of(), Locale.ENGLISH)).isEmpty();
    }

    @Test
    void registrationsAndLocaleAssetsFailClosedOnDefinitionDefects() {
        assertThatThrownBy(() -> ProductDiagnosticCatalog.load("021", List.of(DEFINITION, DEFINITION), language -> RESOURCE))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("repeated");
        assertThatThrownBy(() -> ProductDiagnosticCatalog.load("006", List.of(DEFINITION), language -> RESOURCE))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("invalid");
        for (String module : List.of("000", "21", "abc"))
            assertThatThrownBy(() -> ProductDiagnosticCatalog.load(module, List.of(DEFINITION), language -> RESOURCE))
                    .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> ProductDiagnosticCatalog.load("021", List.of(), language -> RESOURCE))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> ProductDiagnosticCatalog.load("999", List.of(
                new ProductDiagnosticCatalog.Definition("TS9990001", Set.of("reason"))),
                language -> RESOURCE.replace(CODE, "TS9990001")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("module registration");
        assertThatThrownBy(() -> ProductDiagnosticCatalog.load("021", List.of(
                new ProductDiagnosticCatalog.Definition("TS0210000", Set.of())), language -> RESOURCE))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> ProductDiagnosticCatalog.load("021", List.of(DEFINITION),
                language -> language.equals("zh-TW") ? "{}" : RESOURCE))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("incomplete");
        assertThatThrownBy(() -> ProductDiagnosticCatalog.load("021", List.of(DEFINITION),
                language -> RESOURCE.replace("{reason}", "{other}")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("incorrect diagnostic parameters");
        assertThatThrownBy(() -> ProductDiagnosticCatalog.load("021", List.of(DEFINITION),
                language -> RESOURCE.replace("{reason}", "{0}")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("named parameters");
        assertThatThrownBy(() -> ProductDiagnosticCatalog.load("021", List.of(DEFINITION),
                language -> RESOURCE.replace("\"solution\":", "\"solution\":\"shadow\",\"solution\":")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("duplicate object member");
    }

    @Test
    void malformedDelimitersCannotPassValidationAndThenRemainUnrendered() {
        for (String malformed : List.of("{{reason}}", "{reason", "reason}", "{reason}}"))
            assertThatThrownBy(() -> ProductDiagnosticCatalog.load("021", List.of(DEFINITION),
                    language -> RESOURCE.replace("{reason}", malformed)))
                    .as(malformed).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void explicitLiteralEscapesDoNotBecomeParametersOrRewriteArgumentText() {
        String resource = RESOURCE.replace("Correct the request.", "Use $\\\\{var:NAME:default\\\\} and \\\\{reason\\\\}.");
        var catalog = ProductDiagnosticCatalog.load("021", List.of(DEFINITION), language -> resource);
        var result = catalog.resolve(CODE, Map.of("reason", "literal \\{reason\\}"), List.of(), Locale.ENGLISH).orElseThrow();
        assertThat(result.solution()).isEqualTo("Use ${var:NAME:default} and {reason}.");
        assertThat(result.message()).isEqualTo("Invalid request: literal \\{reason\\}");
        for (String invalid : List.of("bad \\\\q", "unclosed \\\\{literal}"))
            assertThatThrownBy(() -> ProductDiagnosticCatalog.load("021", List.of(DEFINITION),
                    language -> RESOURCE.replace("Correct the request.", invalid)))
                    .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void callerMutationCannotAlterRegisteredParameterContractsOrRewriteInsertedText() {
        var parameters = new HashSet<>(Set.of("reason"));
        var definition = new ProductDiagnosticCatalog.Definition(CODE, parameters);
        parameters.add("extra");
        var catalog = ProductDiagnosticCatalog.load("021", List.of(definition), language -> RESOURCE);
        assertThat(catalog.resolve(CODE, Map.of("reason", "literal {reason}"), List.of(), Locale.ENGLISH)
                .orElseThrow().message()).isEqualTo("Invalid request: literal {reason}");
        assertThatThrownBy(() -> catalog.codes().clear()).isInstanceOf(UnsupportedOperationException.class);
    }
}
