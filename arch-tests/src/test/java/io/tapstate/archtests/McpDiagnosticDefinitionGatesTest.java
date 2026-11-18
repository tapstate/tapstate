package io.tapstate.archtests;

import io.tapstate.core.common.McpErrorDefinitions;
import io.tapstate.core.common.Severity;
import io.tapstate.messages.McpDiagnosticCatalog;
import io.tapstate.messages.MessageCatalog;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
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
        assertThat(definitions).hasSize(historical.size());
        var lookup = McpDiagnosticCatalog.bundled();
        var catalog = MessageCatalog.bundled();
        for (int i = 0; i < definitions.size(); i++) {
            var definition = definitions.get(i);
            assertThat(definition.code()).isEqualTo("TS026" + String.format("%04d", i + 1));
            assertThat(definition.severity()).isEqualTo(Severity.ERROR);
            Map<String, Object> args = new HashMap<>();
            definition.parameters().forEach(name -> args.put(name, "value-" + name));
            assertThat(catalog.render(definition.code(), args)).isEqualTo(catalog.render(historical.get(i), args));
            var messages = new HashSet<String>();
            for (var locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
                var resolved = lookup.resolve(definition.code(), args, List.of(), locale).orElseThrow();
                messages.add(resolved.message());
                assertThat(resolved.solution()).isNotBlank();
                definition.parameters().forEach(name -> assertThat(resolved.message())
                        .contains("value-" + name).doesNotContain("{" + name + "}"));
            }
            assertThat(messages).hasSize(3);
            assertThat(lookup.resolve(historical.get(i), args, List.of(), Locale.ENGLISH)).isEmpty();
            assertThat(lookup.resolve(definition.code(), Map.of(), List.of(), Locale.ENGLISH)).isEmpty();
            args.put("private", "must-not-leak");
            assertThat(lookup.resolve(definition.code(), args, List.of(), Locale.ENGLISH)).isEmpty();
        }
    }
}
