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
        verify(PipelineErrorDefinitions.registered(), List.of(PipelineError.values()), List.of(
                "pipeline.already-exists", "pipeline.not-found", "pipeline.id-mismatch", "pipeline.precondition-required",
                "pipeline.version-conflict", "pipeline.start-change-not-applied", "pipeline.definition-change-unavailable"));
        verify(ArtifactErrorDefinitions.registered(), List.of(ArtifactError.values()), List.of(
                "artifact.not-found", "artifact.precondition-required", "artifact.version-conflict", "artifact.in-use",
                "artifact.pipeline-not-stopped", "artifact.reclaim-incomplete"));
        verify(PipelineDraftErrorDefinitions.registered(), List.of(PipelineDraftError.values()), List.of(
                "pipeline-draft.not-found", "pipeline-draft.already-exists", "pipeline-draft.precondition-required",
                "pipeline-draft.revision-conflict", "pipeline-draft.invalid", "pipeline-draft.mode-conflict",
                "pipeline-draft.artifact-conflict"));
    }

    private static void verify(List<ProductErrorDefinition> definitions, List<? extends TapstateErrorCode> owners,
            List<String> historical) {
        assertThat(definitions).hasSameSizeAs(historical);
        assertThat(owners).hasSameSizeAs(definitions);
        var lookup = MutationDiagnosticCatalog.bundled();
        var catalog = MessageCatalog.bundled();
        for (int i = 0; i < definitions.size(); i++) {
            var definition = definitions.get(i);
            var owner = owners.get(i);
            assertThat(owner.code()).isEqualTo(definition.code());
            assertThat(owner.placeholders()).isEqualTo(definition.parameters());
            assertThat(owner.severity()).isEqualTo(definition.severity());
            Map<String, Object> args = new HashMap<>();
            definition.parameters().forEach(name -> args.put(name, "value-" + name));
            assertThat(catalog.render(definition.code(), args)).isEqualTo(catalog.render(historical.get(i), args));
            var messages = new java.util.HashSet<String>();
            for (var locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
                var text = lookup.resolve(definition.code(), args, List.of(), locale).orElseThrow();
                messages.add(text.message());
                assertThat(text.solution()).isNotBlank();
                definition.parameters().forEach(name -> assertThat(text.message()).contains("value-" + name)
                        .doesNotContain("{" + name + "}"));
            }
            assertThat(messages).hasSize(3);
            assertThat(lookup.resolve(historical.get(i), args, List.of(), Locale.ENGLISH)).isEmpty();
            var missing = new HashMap<>(args);
            missing.remove(definition.parameters().iterator().next());
            assertThat(lookup.resolve(definition.code(), missing, List.of(), Locale.ENGLISH)).isEmpty();
            var extra = new HashMap<>(args);
            extra.put("unexpected", "value");
            assertThat(lookup.resolve(definition.code(), extra, List.of(), Locale.ENGLISH)).isEmpty();
        }
    }
}
