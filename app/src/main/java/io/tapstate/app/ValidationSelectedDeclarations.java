package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.ValidationDeclarations;
import io.tapstate.core.model.*;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationTaskSpec;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ValidationTaskStore;
import java.util.*;

/** Server-derived direct physical choices before task creation; no model, key, owner or run is invented. */
final class ValidationSelectedDeclarations {
    record Choice(String mappingId,String outputId,String sourceId,String table){
        Choice{identifier(mappingId);identifier(outputId);identifier(sourceId);identifier(table);}
        @Override public String toString(){return "ValidationTableChoice[mapping="+mappingId+"]";}
    }
    private final ValidationTaskStore.DeclarationInputs original;
    private final ReadOnlyArtifactSnapshot artifacts;
    private final List<ValidationDirectMappings.Declared> mappings;
    private ValidationSelectedDeclarations(ValidationTaskStore.DeclarationInputs original,ReadOnlyArtifactSnapshot artifacts,
            List<ValidationDirectMappings.Declared> mappings){this.original=original;this.artifacts=artifacts;this.mappings=mappings;}

    /** The caller owns the admitted metadata frame until all returned declarations and models are released. */
    static ValidationSelectedDeclarations inspect(ValidationTaskStore.DeclarationInputs original,List<Choice> choices){
        Objects.requireNonNull(original);
        if(choices==null||choices.isEmpty()||choices.size()>ValidationTaskSpec.MAX_MAPPINGS)throw invalid("choices");
        choices=List.copyOf(choices);
        var selected=new HashSet<String>();var identities=new HashSet<String>();
        for(var choice:choices){if(!identities.add(choice.mappingId()))throw invalid("duplicateMapping");selected.add(choice.outputId());}
        if(!selected.equals(new HashSet<>(original.selection().outputIds())))throw invalid("outputSelection");
        var artifacts=ReadOnlyArtifactSnapshot.validation(original,null);
        var pipeline=StoredArtifacts.requirePipeline(artifacts,original.selection().pipelineId());
        var resources=new LinkedHashMap<String,Resource>();artifacts.list().forEach(value->resources.put(value.id(),value));
        var inspections=new LinkedHashMap<String,ValidationDeclarations.Inspection>();var required=new HashSet<String>();required.add(pipeline.id());
        long traversal=0;var defaults=ValidationDeclarations.Limits.defaults();
        for(var output:original.selection().outputIds()){
            long remaining=8L*1024*1024-traversal;if(remaining<=0)throw limit();
            var inspection=ValidationDeclarations.inspect(pipeline,resources,new ValidationDeclarations.Output(ValidationDeclarations.OutputKind.SYNC,output),
                    new ValidationDeclarations.Limits(defaults.nodes(),defaults.references(),defaults.routes(),defaults.depth(),Math.min(defaults.metadataBytes(),remaining)));
            inspections.put(output,inspection);required.addAll(inspection.requiredResourceIds());traversal+=inspection.accountedMetadataBytes();
        }
        if(!required.equals(original.dependencyHashes().keySet()))throw invalid("dependencyClosure");
        var draft=new ArrayList<ValidationTaskSpec.Mapping>();
        for(var choice:choices){
            var inspection=inspections.get(choice.outputId());
            if(!inspection.upstreamSourceIds().contains(choice.sourceId()))throw invalid("sourceScope");
            var source=StoredArtifacts.requireSource(artifacts,choice.sourceId());
            var target=StoredArtifacts.requireSource(artifacts,inspection.sync().source());
            draft.add(new ValidationTaskSpec.Mapping(choice.mappingId(),ValidationTaskSpec.OutputKind.SYNC,choice.outputId(),
                    table(source.id(),source,choice.table()),table(choice.outputId(),target,TableRename.apply(choice.table(),inspection.sync().rename())),null));
        }
        var mappings=ValidationDirectMappings.declare(artifacts,draft,mapping->inspections.get(mapping.outputId()));
        for(var mapping:mappings)if(!PipelineTableSelection.selectsName(mapping.from(),mapping.source(),mapping.mapping().source().table()))
            throw invalid("sourceTableScope");
        return new ValidationSelectedDeclarations(original,artifacts,mappings);
    }
    private static ValidationTaskSpec.Table table(String node,SourceResource resource,String table){
        return new ValidationTaskSpec.Table(node,resource.id(),namespace(resource,"database"),namespace(resource,"schema"),table,null,null);
    }
    private static String namespace(SourceResource source,String field){
        var value=source.config().get(field);if(value==null)return null;
        if(!(value instanceof String text)||text.isBlank())throw invalid(field);ValidationError.fieldName(text,"selection."+field);return text;
    }
    ValidationTaskStore.DeclarationInputs original(){return original;}
    ArtifactStore artifacts(){return artifacts;}
    List<ValidationDirectMappings.Declared> mappings(){return mappings;}
    @Override public String toString(){return "ValidationSelectedDeclarations[mappings="+mappings.size()+"]";}
    private static void identifier(String value){if(value==null||value.isBlank())throw invalid("identity");ValidationError.fieldName(value,"selection.identity");}
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","selection."+field),null);}
    private static TapstateException limit(){return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation.declarations"),null);}
}
