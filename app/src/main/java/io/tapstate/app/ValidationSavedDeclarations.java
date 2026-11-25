package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.ValidationDeclarations;
import io.tapstate.core.model.*;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationTaskSpec;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ValidationTaskStore;
import java.util.*;

/**
 * Selected declarations from the saved, bounded native CONFIG reading. This is not read admission:
 * dynamic table resolution, independent models/codecs, physical SYNC names and all native I/O/commit
 * capabilities remain obligations of the execution planner. No production state or row is read.
 */
final class ValidationSavedDeclarations {
    private static final long MAX_DECLARATION_BYTES = 8L * 1024 * 1024;
    private final ValidationTaskStore.Inputs original;
    private final ReadOnlyArtifactSnapshot artifacts;
    private final Map<String, ValidationDeclarations.Inspection> mappings;

    private ValidationSavedDeclarations(ValidationTaskStore.Inputs original, ReadOnlyArtifactSnapshot artifacts,
            Map<String, ValidationDeclarations.Inspection> mappings) {
        this.original = original; this.artifacts = artifacts; this.mappings = Map.copyOf(mappings);
    }

    static ValidationSavedDeclarations inspect(ValidationTaskStore.Inputs original, String actualDefaultSource) {
        Objects.requireNonNull(original);
        var artifacts = ReadOnlyArtifactSnapshot.validation(original, actualDefaultSource);
        var saved = original.task(); var pipeline = StoredArtifacts.requirePipeline(artifacts, saved.task().pipelineId());
        var resources = new LinkedHashMap<String, Resource>(); artifacts.list().forEach(resource -> resources.put(resource.id(), resource));
        var outputs = new HashMap<ValidationDeclarations.Output, ValidationDeclarations.Inspection>();
        var mappings = new LinkedHashMap<String, ValidationDeclarations.Inspection>();
        var required = new HashSet<String>(); required.add(pipeline.id()); long bytes = 0;
        var defaults = ValidationDeclarations.Limits.defaults();
        for (var mapping : saved.comparison().mappings()) {
            var output = new ValidationDeclarations.Output(mapping.outputKind() == ValidationTaskSpec.OutputKind.VIEW
                    ? ValidationDeclarations.OutputKind.VIEW : ValidationDeclarations.OutputKind.SYNC, mapping.outputId());
            var inspection = outputs.get(output);
            if (inspection == null) {
                long remaining = MAX_DECLARATION_BYTES - bytes; if (remaining <= 0) throw limit();
                inspection = ValidationDeclarations.inspect(pipeline, resources, output, new ValidationDeclarations.Limits(defaults.nodes(), defaults.references(),
                        defaults.routes(), defaults.depth(), Math.min(defaults.metadataBytes(), remaining)));
                bytes += inspection.accountedMetadataBytes(); outputs.put(output, inspection); required.addAll(inspection.requiredResourceIds());
            }
            if (!(resources.get(mapping.source().sourceId()) instanceof SourceResource)
                    || !inspection.upstreamSourceIds().contains(mapping.source().sourceId())) throw invalid("configuration.sourceScope");
            if (mapping.outputKind() == ValidationTaskSpec.OutputKind.SYNC) {
                if (!mapping.target().sourceId().equals(inspection.sync().source())) throw invalid("configuration.targetScope");
            } else {
                if (actualDefaultSource == null || !(resources.get(actualDefaultSource) instanceof SourceResource)) throw invalid("configuration.viewStore");
                var target = ViewTargetResolver.resolve(inspection.view(), actualDefaultSource); required.add(target.sourceId());
                if (!mapping.target().sourceId().equals(target.sourceId()) || !mapping.target().table().equals(target.collection())) throw invalid("configuration.targetScope");
            }
            mappings.put(mapping.mappingId(), inspection);
        }
        if (!required.equals(saved.comparison().dependencyHashes().keySet())) throw invalid("configuration.dependencyClosure");
        return new ValidationSavedDeclarations(original, artifacts, mappings);
    }

    ValidationTaskStore.Inputs original() { return original; }
    ArtifactStore artifacts() { return artifacts; }
    PipelineResource pipeline() { return StoredArtifacts.requirePipeline(artifacts, original.task().task().pipelineId()); }
    ValidationDeclarations.Inspection mapping(String id) {
        var found = mappings.get(id); if (found == null) throw invalid("configuration.mapping"); return found;
    }
    @Override public String toString() { return "ValidationSavedDeclarations[mappings=" + mappings.size() + "]"; }
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null); }
    private static TapstateException limit() { return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "validation.declarations"), null); }
}
