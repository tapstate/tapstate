package io.tapstate.app;

import io.tapstate.core.validation.*;
import io.tapstate.runtime.probe.*;
import java.util.*;

/** Independent server-derived evaluation. The outer metadata frame also owns its response projection. */
final class NativeValidationEvaluation implements ValidationEvaluationProbe {
    private final NativeValidationMetadata metadata;
    NativeValidationEvaluation(NativeValidationMetadata metadata){this.metadata=Objects.requireNonNull(metadata);}
    @Override public ValidationEvaluation evaluate(ValidationEvaluation.Request request){
        var confirmations=new LinkedHashMap<String,ValidationTaskCompiler.Confirmation>();
        request.confirmations().forEach((id,value)->confirmations.put(id,new ValidationTaskCompiler.Confirmation(value.id(),
                value.fields().stream().map(pair->new ValidationTaskCompiler.Pair(pair.source(),pair.target())).toList())));
        var projections=new LinkedHashMap<String,NativeValidationMetadata.Projection>();
        if(request.method()==ValidationMethod.UNIQUE_KEY)confirmations.forEach((id,value)->projections.put(id,new NativeValidationMetadata.Projection(
                value.fields().stream().map(ValidationTaskCompiler.Pair::source).toList(),value.fields().stream().map(ValidationTaskCompiler.Pair::target).toList())));
        var scope=metadata.inspect(request.selection(),request.choices().stream().map(choice->new ValidationSelectedDeclarations.Choice(
                choice.mappingId(),choice.outputId(),choice.sourceId(),choice.table())).toList(),request.method(),projections,request.deadline());
        try{
            ValidationTaskCompiler.Compiled compiled=request.method()==ValidationMethod.COUNT||!confirmations.isEmpty()
                    ?ValidationTaskCompiler.compile(scope,confirmations,request.previous()):null;
            var mappings=new ArrayList<ValidationEvaluation.Mapping>();long projected=0;
            var actual=new HashMap<String,NativeValidationMetadata.Mapping>();scope.mappings().forEach(value->actual.put(value.id(),value));
            for(var declared:scope.declarations().mappings()){
                var observed=actual.get(declared.mapping().mappingId());
                var source=side(scope,declared.mapping().source(),observed.source(),true);
                var target=side(scope,declared.mapping().target(),observed.target(),false);
                projected+=projectionBytes(source)+projectionBytes(target);
                if(projected>8L*1024*1024)throw new io.tapstate.core.common.TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation.evaluationProjection"),null);
                mappings.add(new ValidationEvaluation.Mapping(declared.mapping().mappingId(),source,target));
            }
            var view=new ValidationEvaluation.View(request.method().name(),Long.toString(scope.declarations().original().configurationRevision()),
                    compiled==null?null:Long.toString(compiled.comparison().mappingRevision()),compiled==null?null:compiled.fingerprint(),mappings,
                    "INITIAL_SYNC_FINISHED_AND_BOTH_SIDES_STABLE","READ_OBSERVATIONS_NO_COMMON_TIME_SNAPSHOT");
            return new Evaluated(scope,view,compiled==null?null:compiled.comparison());
        }catch(RuntimeException|Error failed){scope.close();throw failed;}
    }
    private static final class Evaluated extends ValidationEvaluation {
        private final NativeValidationMetadata.Scope scope;private View retained;private ValidationTaskSpec comparison;
        private Evaluated(NativeValidationMetadata.Scope scope,View view,ValidationTaskSpec comparison){this.scope=scope;this.retained=view;this.comparison=comparison;}
        @Override public View view(){requireCurrent();return retained;}
        @Override public Optional<ValidationTaskSpec> comparison(){requireCurrent();return Optional.ofNullable(comparison);}
        @Override public void requireCurrent(){scope.require();}
        @Override public void close(){retained=null;comparison=null;scope.close();}
    }
    private static ValidationEvaluation.Side side(NativeValidationMetadata.Scope scope,ValidationTaskSpec.Table table,NativeValidationMetadata.Side observed,boolean source){
        if(observed.model()==null)return new ValidationEvaluation.Side(table.nodeId(),table.sourceId(),table.database(),table.schema(),table.table(),null,null,
                observed.countOperation().name(),List.of(),List.of(),!source);
        var model=observed.model().model();var rules=ValidationKeyMetadata.describe(model,scope.nativeBson(table.sourceId()));
        var fields=model.fields().stream().map(field->new ValidationEvaluation.Field(field.name(),field.type().name(),
                rules.fields().containsKey(field.name())?rules.fields().get(field.name()).domain().name():null,rules.unavailable().get(field.name()))).toList();
        var domains=new HashMap<String,ValidationKey.Kind>();rules.fields().forEach((name,rule)->domains.put(name,rule.domain()));
        // Current independent metadata cannot establish the original production write-model binding.
        var recommended=ValidationKeyRecommendations.recommend("mapping",model,domains,model,domains,Optional.empty(),ValidationKeyRecommendations.Limits.defaults());
        var candidates=recommended.sourceCandidates().stream().map(candidate->new ValidationEvaluation.Candidate(candidate.origin().name(),candidate.fields(),
                candidate.issues().stream().map(Enum::name).sorted().toList())).toList();
        return new ValidationEvaluation.Side(table.nodeId(),table.sourceId(),table.database(),table.schema(),table.table(),observed.model().binding().modelVersion(),
                observed.model().binding().codecBinding(),null,fields,candidates,!source);
    }
    private static long projectionBytes(ValidationEvaluation.Side side){
        long bytes=1024L;for(var field:side.fields())bytes+=512L+2L*field.name().length();
        for(var candidate:side.candidates()){bytes+=512L;for(var field:candidate.fields())bytes+=128L+2L*field.length();}return bytes;
    }
}
