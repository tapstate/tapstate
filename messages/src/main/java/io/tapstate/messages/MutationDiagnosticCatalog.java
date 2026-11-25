package io.tapstate.messages;

import io.tapstate.core.common.ArtifactErrorDefinitions;
import io.tapstate.core.common.DiagnosticDefinitionLookup;
import io.tapstate.core.common.PipelineDraftErrorDefinitions;
import io.tapstate.core.common.PipelineErrorDefinitions;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.ProductErrorModule;
import java.util.List;

/** Explicit catalogs for the mutually dependent Pipeline, draft and artifact mutation surfaces. */
public final class MutationDiagnosticCatalog {
    private MutationDiagnosticCatalog() {}

    private static ProductDiagnosticCatalog load(ProductErrorModule module, List<ProductErrorDefinition> definitions,
            String resource) {
        return ProductDiagnosticCatalog.load(module.number(), definitions.stream()
                .map(definition -> new ProductDiagnosticCatalog.Definition(definition.code(), definition.parameters()))
                .toList(), language -> MessageCatalog.read("/messages/" + resource + "-" + language + ".json"));
    }

    private static final class Bundled {
        static final ProductDiagnosticCatalog PIPELINE = load(ProductErrorModule.PIPELINE,
                PipelineErrorDefinitions.registered(), "pipeline-diagnostic");
        static final ProductDiagnosticCatalog ARTIFACT = load(ProductErrorModule.ARTIFACT,
                ArtifactErrorDefinitions.registered(), "artifact-diagnostic");
        static final ProductDiagnosticCatalog DRAFT = load(ProductErrorModule.PIPELINE_DRAFT,
                PipelineDraftErrorDefinitions.registered(), "pipeline-draft-diagnostic");
        static final DiagnosticDefinitionLookup INSTANCE = (code, parameters, sources, locale) ->
                PIPELINE.resolve(code, parameters, sources, locale)
                        .or(() -> ARTIFACT.resolve(code, parameters, sources, locale))
                        .or(() -> DRAFT.resolve(code, parameters, sources, locale));
    }

    public static DiagnosticDefinitionLookup bundled() { return Bundled.INSTANCE; }
}
