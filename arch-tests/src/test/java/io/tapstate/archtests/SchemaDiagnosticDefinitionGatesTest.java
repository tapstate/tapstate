package io.tapstate.archtests;

import io.tapstate.core.common.SchemaErrorDefinitions;
import io.tapstate.core.common.Severity;
import io.tapstate.core.dsl.DocumentKeyError;
import io.tapstate.messages.MessageCatalog;
import io.tapstate.messages.SchemaDiagnosticCatalog;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaDiagnosticDefinitionGatesTest {
    @Test
    void advisoryKeepsWarningMeaningAndBothSidesOfTheColumnAttributionInEveryLanguage() {
        var definition = SchemaErrorDefinitions.COLUMN_NAME_READS_AS_A_PATH;
        assertThat(SchemaErrorDefinitions.registered()).contains(definition);
        var owner = DocumentKeyError.COLUMN_NAME_READS_AS_A_PATH;
        assertThat(owner.code()).isEqualTo("TS0270001").isEqualTo(definition.code());
        assertThat(owner.severity()).isEqualTo(Severity.WARNING).isEqualTo(definition.severity());
        assertThat(owner.placeholders()).isEqualTo(definition.parameters());
        Map<String, Object> args = Map.of("pipeline", "orders", "source", "input", "table", "sales",
                "column", "price.usd", "target", "output");
        var catalog = MessageCatalog.bundled();
        assertThat(catalog.render(owner.code(), args))
                .isEqualTo(catalog.render("schema.column-name-reads-as-a-path", args));
        var messages = new HashSet<String>();
        for (var locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
            var resolved = SchemaDiagnosticCatalog.bundled().resolve(owner.code(), args, List.of(), locale).orElseThrow();
            assertThat(resolved.message()).contains("orders", "sales", "price.usd", "output");
            assertThat(resolved.solution()).contains("input", "price.usd", "output");
            messages.add(resolved.message());
        }
        assertThat(messages).hasSize(3);
        assertThat(SchemaDiagnosticCatalog.bundled().resolve(owner.code(), Map.of(), List.of(), Locale.ENGLISH)).isEmpty();
        assertThat(SchemaDiagnosticCatalog.bundled().resolve("schema.column-name-reads-as-a-path", args,
                List.of(), Locale.ENGLISH)).isEmpty();
    }
}
