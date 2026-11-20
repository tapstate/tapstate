package io.tapstate.archtests;

import io.tapstate.core.common.McpErrorDefinitions;
import io.tapstate.core.common.Severity;
import io.tapstate.messages.McpDiagnosticCatalog;
import io.tapstate.messages.MessageCatalog;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class McpDiagnosticDefinitionGatesTest {
    @Test
    void localFailuresPreserveHistoricalMeaningAndRejectIncompleteOrForeignParameters() {
        var historical = List.of("mcp.connector-spec-unavailable", "mcp.environment-missing",
                "mcp.invalid-server-response", "mcp.server-rejected");
        var definitions = McpErrorDefinitions.registered();
        var lookup = McpDiagnosticCatalog.bundled();
        for (int i = 0; i < historical.size(); i++) {
            String code = "TS026" + String.format("%04d", i + 1);
            var definition = definitions.stream().filter(value -> value.code().equals(code)).findFirst().orElseThrow();
            Map<String, Object> args = new HashMap<>();
            definition.parameters().forEach(name -> args.put(name, "value-" + name));
            assertThat(definition.severity()).isEqualTo(Severity.ERROR);
            assertThat(MessageCatalog.bundled().render(code, args))
                    .isEqualTo(MessageCatalog.bundled().render(historical.get(i), args));
            assertThat(McpDiagnosticCatalog.bundled().resolve(historical.get(i), args, List.of(), Locale.ENGLISH)).isEmpty();
        }
        for (var definition : definitions) {
            Map<String, Object> args = new HashMap<>();
            definition.parameters().forEach(name -> args.put(name, "value-" + name));
            for (var locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
                var resolved = lookup.resolve(definition.code(), args, List.of(), locale).orElseThrow();
                assertThat(resolved.message()).isNotBlank();
                assertThat(resolved.solution()).isNotBlank();
                definition.parameters().forEach(name -> assertThat(resolved.message() + resolved.solution())
                        .contains("value-" + name));
            }
            for (String parameter : definition.parameters()) {
                var missing = new HashMap<>(args);
                missing.remove(parameter);
                assertThat(lookup.resolve(definition.code(), missing, List.of(), Locale.ENGLISH)).isEmpty();
            }
            args.put("invalid-parameter!", "must-not-leak");
            assertThat(lookup.resolve(definition.code(), args, List.of(), Locale.ENGLISH)).isEmpty();
        }
    }
}
