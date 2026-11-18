package io.tapstate.mcp;

import io.tapstate.control.client.ControlResponse;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class McpResultTest {

    @Test
    void warningEnrichmentPreservesUnknownMalformedServerTextAndRejections() {
        var unknown = Map.of("code", "future.warning", "params", Map.of("name", "x"));
        var missing = Map.of("code", "TS0010032", "params", Map.of("pipeline", "orders"));
        var missingLegacy = Map.of("code", "dsl.setting-has-no-effect", "params", Map.of("pipeline", "orders"));
        var malformed = Map.of("code", "TS0010032", "params", Map.of("pipeline", List.of("orders")));
        var authored = Map.of("code", "TS0010032", "params", Map.of(), "message", "Keep the server language.");
        var warnings = List.of(unknown, missing, missingLegacy, malformed, authored);
        var success = McpResult.success(Map.of("warnings", warnings));
        assertThat(success.withWarningText()).isEqualTo(success);
        var rejected = new McpResult(true, Map.of("warnings", warnings, "report", "keep"));
        assertThat(rejected.withWarningText()).isSameAs(rejected);
        assertThat(success.body().get("warnings")).isSameAs(warnings);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"TS0210016", "control.schema-derivation-incomplete"})
    void appliedArtifactsWithAnIncompleteModelRefreshRetainTheirNestedCauseParameters(String code) {
        var params = Map.of("pipeline", "orders", "causeCode", "runtime.busy", "causeParams", Map.of("state", "RUNNING"));
        var warning = Map.of("code", code, "params", params);
        var result = McpResult.success(Map.of("warnings", List.of(warning), "applied", true)).withWarningText();
        assertThat(result.error()).isFalse();
        assertThat(result.body()).containsEntry("applied", true);
        var rendered = (Map<?, ?>) ((List<?>) result.body().get("warnings")).getFirst();
        assertThat(rendered.get("params")).isEqualTo(params);
        assertThat(rendered.get("message")).as("the real post-commit warning remains readable")
                .isNotNull().asString().contains("artifacts were applied", "orders", "runtime.busy", "RUNNING");
    }

    @Test
    void anExplicitUpstreamOriginCannotBorrowAProductDefinitionWithTheSameCode() {
        for (Object source : List.of("connector", "pdk", "jdbc-sqlstate", "future-origin", 7)) {
            var warning = Map.of("code", "TS0010032", "codeSource", source, "params",
                    Map.of("pipeline", "orders", "setting", "parallelism", "instead", "node.parallelism"));
            var original = McpResult.success(Map.of("warnings", List.of(warning)));
            assertThat(original.withWarningText()).isEqualTo(original);
        }
    }

    @Test
    void malformedOptionalSolutionIsKeptWithoutClientEnrichment() {
        for (Object solution : java.util.Arrays.asList(null, 42, List.of("unexpected"))) {
            Map<String, Object> warning = new java.util.LinkedHashMap<>();
            warning.put("code", "TS0010032");
            warning.put("params", Map.of("pipeline", "orders", "setting", "parallelism", "instead", "node.parallelism"));
            warning.put("solution", solution);
            var original = McpResult.success(Map.of("warnings", List.of(warning)));
            assertThat(original.withWarningText()).isEqualTo(original);
        }
    }

    @Test
    void successfulValuesAreNormalizedToStringKeyedStructuredContent() {
        assertThat(McpResult.from(new ControlResponse.Success(200, Map.of(1, "value"))))
                .isEqualTo(new McpResult(false, Map.of("1", "value")));
        assertThat(McpResult.success(List.of("one", "two")).body())
                .containsEntry("value", List.of("one", "two"));
        assertThat(McpResult.success(null).body()).isEmpty();
    }

    @Test
    void malformedUpstreamRejectionUsesARegisteredMcpError() {
        McpResult result = McpResult.from(
                new ControlResponse.Rejected(502, "", "invalid upstream response", Map.of()));

        assertThat(result.error()).isTrue();
        assertThat(result.body())
                .containsEntry("code", "TS0260004")
                .containsEntry("params", Map.of("status", 502));
    }

    @Test
    void codedRejectionPreservesTheServerContractAndCodedErrorsRenderSolutions() {
        McpResult rejected = McpResult.from(new ControlResponse.Rejected(
                422, "source.invalid", "Invalid source.", Map.of("field", "host")));
        assertThat(rejected.error()).isTrue();
        assertThat(rejected.body()).containsEntry("code", "source.invalid")
                .containsEntry("status", 422)
                .containsEntry("params", Map.of("field", "host"));

        McpResult coded = McpResult.coded(McpError.ENVIRONMENT_MISSING,
                Map.of("variable", "MYSQL_PASSWORD"));
        assertThat(coded.body()).containsEntry("code", "TS0260002")
                .containsKey("solution");
    }
}
