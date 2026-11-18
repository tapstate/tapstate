package io.tapstate.archtests;

import io.tapstate.control.core.SourceError;
import io.tapstate.core.common.ProductErrorModule;
import io.tapstate.core.common.Severity;
import io.tapstate.core.common.SourceErrorDefinitions;
import io.tapstate.messages.MessageCatalog;
import io.tapstate.messages.SourceDiagnosticCatalog;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SourceDiagnosticDefinitionGatesTest {
    @Test
    void everySourceOwnerRetainsItsParameterMeaningAndReadableTranslations() {
        assertThat(ProductErrorModule.SOURCE.number()).isEqualTo("022");
        var definitions = SourceErrorDefinitions.registered();
        assertThat(definitions).hasSize(7);
        assertThat(Arrays.stream(SourceError.values()).map(SourceError::code).toList())
                .doesNotHaveDuplicates().containsExactlyInAnyOrderElementsOf(
                        definitions.stream().map(definition -> definition.code()).toList());
        var historical = List.of("source.not-found", "source.already-exists", "source.id-mismatch",
                "source.precondition-required", "source.version-conflict", "source.in-use", "source.srs-change-while-running");
        for (int i = 0; i < definitions.size(); i++) {
            var definition = definitions.get(i);
            var owner = Arrays.stream(SourceError.values()).filter(code -> code.code().equals(definition.code()))
                    .findFirst().orElseThrow();
            assertThat(owner.severity()).isEqualTo(Severity.ERROR);
            assertThat(owner.placeholders()).isEqualTo(definition.parameters());
            Map<String, Object> args = new HashMap<>();
            definition.parameters().forEach(name -> args.put(name, "value-" + name));
            var catalog = MessageCatalog.bundled();
            assertThat(catalog.render(definition.code(), args)).isEqualTo(catalog.render(historical.get(i), args));
            var messages = new java.util.HashSet<String>();
            for (var locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
                var resolved = SourceDiagnosticCatalog.bundled().resolve(definition.code(), args, List.of(), locale).orElseThrow();
                messages.add(resolved.message());
                assertThat(resolved.solution()).isNotBlank();
                for (var name : definition.parameters())
                    assertThat(resolved.message()).contains("value-" + name).doesNotContain("{" + name + "}");
            }
            assertThat(messages).hasSize(3);
            assertThat(SourceDiagnosticCatalog.bundled().resolve(historical.get(i), args, List.of(), Locale.ENGLISH)).isEmpty();
            var unexpected = new HashMap<>(args);
            unexpected.put("unexpected", "value");
            assertThat(SourceDiagnosticCatalog.bundled().resolve(definition.code(), unexpected, List.of(), Locale.ENGLISH)).isEmpty();
            var missing = new HashMap<>(args);
            missing.remove("id");
            missing.remove("pathId");
            assertThat(SourceDiagnosticCatalog.bundled().resolve(definition.code(), missing, List.of(), Locale.ENGLISH)).isEmpty();
        }
    }
}
