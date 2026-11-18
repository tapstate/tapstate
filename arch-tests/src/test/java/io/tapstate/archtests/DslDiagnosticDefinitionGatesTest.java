package io.tapstate.archtests;

import io.tapstate.core.common.DslErrorDefinitions;
import io.tapstate.core.common.ProductErrorModule;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.dsl.DslError;
import io.tapstate.core.dsl.ExecutionAdvisoryError;
import io.tapstate.messages.DslDiagnosticCatalog;
import io.tapstate.messages.MessageCatalog;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class DslDiagnosticDefinitionGatesTest {
    @Test
    void allTypedOwnersUseExactlyTheRegisteredDefinitionsAndAllThreeLanguages() {
        assertThat(ProductErrorModule.DSL.number()).isEqualTo("001");
        List<TapstateErrorCode> owners = Stream.concat(Arrays.stream(DslError.values()),
                Arrays.stream(ExecutionAdvisoryError.values())).map(code -> (TapstateErrorCode) code).toList();
        assertThat(owners).hasSize(33);
        assertThat(owners).extracting(TapstateErrorCode::code).doesNotHaveDuplicates()
                .containsExactlyInAnyOrderElementsOf(DslErrorDefinitions.registered().stream().map(d -> d.code()).toList());
        for (var definition : DslErrorDefinitions.registered()) {
            var owner = owners.stream().filter(code -> code.code().equals(definition.code())).findFirst().orElseThrow();
            assertThat(owner.placeholders()).isEqualTo(definition.parameters());
            assertThat(owner.severity()).isEqualTo(definition.severity());
            Map<String, Object> args = new HashMap<>();
            definition.parameters().forEach(name -> args.put(name, "value-" + name));
            var rendered = List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE).stream()
                    .map(locale -> DslDiagnosticCatalog.bundled().resolve(definition.code(), args, List.of(), locale).orElseThrow())
                    .toList();
            assertThat(rendered).extracting(d -> d.message()).doesNotHaveDuplicates();
            for (var text : rendered) {
                for (String parameter : definition.parameters()) {
                    assertThat(text.message() + text.solution()).contains("value-" + parameter)
                            .doesNotContain("{" + parameter + "}");
                }
            }
            assertThat(MessageCatalog.bundled().render(definition.code(), args).message())
                    .isEqualTo(rendered.getFirst().message());
        }
    }

    @Test
    void grammarExamplesRemainLiteralAndOldRecordsRemainReadableWithoutAnIdentityAlias() {
        var args = Map.<String, Object>of("name", "MISSING");
        var catalog = MessageCatalog.bundled();
        var old = catalog.render("dsl.undefined-variable", args);
        var current = catalog.render(DslError.UNDEFINED_VARIABLE.code(), args);
        assertThat(current).isEqualTo(old);
        assertThat(current.solution()).contains("${var:NAME:default}");
        assertThat(DslDiagnosticCatalog.bundled().resolve("dsl.undefined-variable", args, List.of(), Locale.ENGLISH)).isEmpty();
    }
}
