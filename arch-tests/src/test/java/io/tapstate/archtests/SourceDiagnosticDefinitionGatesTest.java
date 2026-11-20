package io.tapstate.archtests;

import io.tapstate.control.core.SourceError;
import io.tapstate.core.common.ProductErrorModule;
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
        assertThat(Arrays.stream(SourceError.values()).map(SourceError::code).toList())
                .doesNotHaveDuplicates().containsExactlyInAnyOrderElementsOf(
                        definitions.stream().map(definition -> definition.code()).toList());
        var historical = List.of("source.not-found", "source.already-exists", "source.id-mismatch",
                "source.precondition-required", "source.version-conflict", "source.in-use", "source.srs-change-while-running");
        for (int i = 0; i < historical.size(); i++) {
            String code = "TS022" + String.format("%04d", i + 1);
            var definition = definitions.stream().filter(value -> value.code().equals(code)).findFirst().orElseThrow();
            Map<String, Object> args = new HashMap<>();
            definition.parameters().forEach(name -> args.put(name, "value-" + name));
            assertThat(MessageCatalog.bundled().render(code, args))
                    .isEqualTo(MessageCatalog.bundled().render(historical.get(i), args));
            assertThat(SourceDiagnosticCatalog.bundled().resolve(historical.get(i), args, List.of(), Locale.ENGLISH)).isEmpty();
        }
        for (var definition : definitions) {
            var owner = Arrays.stream(SourceError.values()).filter(code -> code.code().equals(definition.code()))
                    .findFirst().orElseThrow();
            assertThat(owner.severity()).isEqualTo(definition.severity());
            assertThat(owner.placeholders()).isEqualTo(definition.parameters());
            Map<String, Object> args = new HashMap<>();
            definition.parameters().forEach(name -> args.put(name, "value-" + name));
            for (var locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
                var resolved = SourceDiagnosticCatalog.bundled().resolve(definition.code(), args, List.of(), locale).orElseThrow();
                assertThat(resolved.message()).isNotBlank();
                assertThat(resolved.solution()).isNotBlank();
                for (var name : definition.parameters())
                    assertThat(resolved.message() + resolved.solution()).contains("value-" + name);
            }
            var unexpected = new HashMap<>(args);
            unexpected.put("invalid-parameter!", "value");
            assertThat(SourceDiagnosticCatalog.bundled().resolve(definition.code(), unexpected, List.of(), Locale.ENGLISH)).isEmpty();
            for (String parameter : definition.parameters()) {
                var missing = new HashMap<>(args);
                missing.remove(parameter);
                assertThat(SourceDiagnosticCatalog.bundled().resolve(definition.code(), missing, List.of(), Locale.ENGLISH)).isEmpty();
            }
        }
    }
}
