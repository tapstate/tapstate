package io.tapstate.archtests;

import io.tapstate.control.core.ArtifactError;
import io.tapstate.control.core.PipelineDraftError;
import io.tapstate.control.core.PipelineError;
import io.tapstate.core.common.ArtifactErrorDefinitions;
import io.tapstate.core.common.PipelineDraftErrorDefinitions;
import io.tapstate.core.common.PipelineErrorDefinitions;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.messages.MessageCatalog;
import io.tapstate.messages.MutationDiagnosticCatalog;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MutationDiagnosticDefinitionGatesTest {
    @Test void mutationsPreserveOwnersParametersAndHistoricalMeaningInEveryLanguage() {
        verify("TS023", PipelineErrorDefinitions.registered(), List.of(PipelineError.values()), List.of(
                "pipeline.already-exists", "pipeline.not-found", "pipeline.id-mismatch", "pipeline.precondition-required",
                "pipeline.version-conflict", "pipeline.start-change-not-applied", "pipeline.definition-change-unavailable"));
        verify("TS024", ArtifactErrorDefinitions.registered(), List.of(ArtifactError.values()), List.of(
                "artifact.not-found", "artifact.precondition-required", "artifact.version-conflict", "artifact.in-use",
                "artifact.pipeline-not-stopped", "artifact.reclaim-incomplete"));
        verify("TS025", PipelineDraftErrorDefinitions.registered(), List.of(PipelineDraftError.values()), List.of(
                "pipeline-draft.not-found", "pipeline-draft.already-exists", "pipeline-draft.precondition-required",
                "pipeline-draft.revision-conflict", "pipeline-draft.invalid", "pipeline-draft.mode-conflict",
                "pipeline-draft.artifact-conflict"));
    }

    private static void verify(String prefix, List<ProductErrorDefinition> definitions, List<? extends TapstateErrorCode> owners,
            List<String> historical) {
        assertThat(owners).extracting(TapstateErrorCode::code).doesNotHaveDuplicates()
                .containsExactlyInAnyOrderElementsOf(definitions.stream().map(ProductErrorDefinition::code).toList());
        var lookup = MutationDiagnosticCatalog.bundled();
        var catalog = MessageCatalog.bundled();
        for (int i = 0; i < historical.size(); i++) {
            String code = prefix + String.format("%04d", i + 1);
            var definition = definitions.stream().filter(value -> value.code().equals(code)).findFirst().orElseThrow();
            Map<String, Object> args = new HashMap<>();
            definition.parameters().forEach(name -> args.put(name, "value-" + name));
            assertThat(catalog.render(code, args)).isEqualTo(catalog.render(historical.get(i), args));
            assertThat(lookup.resolve(historical.get(i), args, List.of(), Locale.ENGLISH)).isEmpty();
        }
        for (var definition : definitions) {
            var owner = owners.stream().filter(value -> value.code().equals(definition.code())).findFirst().orElseThrow();
            assertThat(owner.code()).isEqualTo(definition.code());
            assertThat(owner.placeholders()).isEqualTo(definition.parameters());
            assertThat(owner.severity()).isEqualTo(definition.severity());
            Map<String, Object> args = new HashMap<>();
            definition.parameters().forEach(name -> args.put(name, "value-" + name));
            for (var locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
                var text = lookup.resolve(definition.code(), args, List.of(), locale).orElseThrow();
                assertThat(text.message()).isNotBlank();
                assertThat(text.solution()).isNotBlank();
                definition.parameters().forEach(name -> assertThat(text.message() + text.solution()).contains("value-" + name));
            }
            for (String parameter : definition.parameters()) {
                var missing = new HashMap<>(args);
                missing.remove(parameter);
                assertThat(lookup.resolve(definition.code(), missing, List.of(), Locale.ENGLISH)).isEmpty();
            }
            var extra = new HashMap<>(args);
            extra.put("invalid-parameter!", "value");
            assertThat(lookup.resolve(definition.code(), extra, List.of(), Locale.ENGLISH)).isEmpty();
        }
    }
}
