package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/** Builds the saved comparison exclusively from server-held declarations, models and explicit field choices. */
final class ValidationTaskCompiler {
    static final String RULES_VERSION="direct-full-v1";
    record Pair(String source,String target){Pair{identifier(source);identifier(target);}}
    record Confirmation(UUID id,List<Pair> fields){
        Confirmation{
            Objects.requireNonNull(id);if(fields==null||fields.isEmpty()||fields.size()>ValidationKey.MAX_COMPONENTS)throw invalid("confirmation");
            fields=List.copyOf(fields);var source=new HashSet<String>();var target=new HashSet<String>();
            for(var field:fields)if(!source.add(field.source())||!target.add(field.target()))throw invalid("duplicateKeyField");
        }
    }
    record Compiled(ValidationTaskSpec comparison,String fingerprint,ValidationMethod method){}
    static Compiled compile(NativeValidationMetadata.Scope metadata,Map<String,Confirmation> confirmations,ValidationTaskSpec previous){
        metadata.require();Objects.requireNonNull(confirmations);if(confirmations.size()>ValidationTaskSpec.MAX_MAPPINGS)throw invalid("confirmations");
        var selected=metadata.declarations();var method=metadata.method();var mappings=new ArrayList<ValidationTaskSpec.Mapping>();
        var observed=new LinkedHashMap<String,NativeValidationMetadata.Mapping>();for(var mapping:metadata.mappings())observed.put(mapping.id(),mapping);
        if(method==ValidationMethod.COUNT?!confirmations.isEmpty():!confirmations.keySet().equals(observed.keySet()))throw invalid("confirmationScope");
        for(var declared:selected.mappings()){
            var draft=declared.mapping();var actual=observed.get(draft.mappingId());if(actual==null)throw invalid("observedMapping");
            if(method==ValidationMethod.COUNT){
                if(actual.source().countOperation()==null||actual.target().countOperation()==null)throw invalid("countCapability");
                mappings.add(draft);continue;
            }
            var confirmed=Objects.requireNonNull(confirmations.get(draft.mappingId()));
            if(method==ValidationMethod.UNIQUE_KEY){
                var projected=metadata.projection(draft.mappingId());
                if(projected==null||!projected.source().equals(confirmed.fields().stream().map(Pair::source).toList())
                        ||!projected.target().equals(confirmed.fields().stream().map(Pair::target).toList()))
                    throw unsupported(draft.mappingId(),"keys","key-confirmation-preflight-required");
            }
            var sourceRules=ValidationKeyMetadata.describe(actual.source().model().model());
            var targetRules=ValidationKeyMetadata.describe(actual.target().model().model());
            var source=new ArrayList<ValidationKeyBinding.Component>();var target=new ArrayList<ValidationKeyBinding.Component>();
            for(var pair:confirmed.fields()){
                var from=component(sourceRules,pair.source(),draft.mappingId());var to=component(targetRules,pair.target(),draft.mappingId());
                if(from.domain()!=to.domain()||from.numericPolicy()!=to.numericPolicy())throw unsupported(draft.mappingId(),pair.source(),"key-domain-mismatch");
                if((from.logicalTypes().contains("object-id")||to.logicalTypes().contains("object-id"))&&!from.logicalTypes().equals(to.logicalTypes()))
                    throw unsupported(draft.mappingId(),pair.source(),"native-object-id-domain-mismatch");
                source.add(from);target.add(to);
            }
            mappings.add(new ValidationTaskSpec.Mapping(draft.mappingId(),draft.outputKind(),draft.outputId(),
                    bind(draft.source(),actual.source()),bind(draft.target(),actual.target()),new ValidationTaskSpec.Keys(confirmed.id().toString(),source,target)));
        }
        if(previous!=null&&!previous.pipelineId().equals(selected.original().selection().pipelineId()))throw invalid("previousPipeline");
        long revision=previous==null?1:previous.mappingRevision();
        if(previous!=null&&!previous.mappings().equals(mappings)){if(revision>=Long.MAX_VALUE-1)throw invalid("mappingRevision");revision++;}
        var comparison=new ValidationTaskSpec(selected.original().selection().pipelineId(),selected.original().dependencyHashes(),revision,RULES_VERSION,mappings);
        comparison.requireFor(method,ValidationMode.FULL);metadata.require();return new Compiled(comparison,fingerprint(comparison,method,metadata.mappings(),selected.original().parentGeneration()),method);
    }
    static Compiled compileDeclaration(NativeValidationMetadata.Scope metadata, Map<String,Confirmation> confirmations,
            ValidationTaskSpec previous, ValidationMethod previousMethod) {
        var selected = metadata.declarations();
        var method = metadata.method();
        var previousMappings = new HashMap<String,ValidationTaskSpec.Mapping>();
        if (previous != null) previous.mappings().forEach(mapping -> previousMappings.put(mapping.mappingId(), mapping));
        var mappings = new ArrayList<ValidationTaskSpec.Mapping>();
        for (var declared : selected.mappings()) {
            var draft = declared.mapping();
            if (method == ValidationMethod.COUNT) {
                mappings.add(draft);
                continue;
            }
            var confirmed = Objects.requireNonNull(confirmations.get(draft.mappingId()));
            var keys = ValidationTaskSpec.Keys.deferred(confirmed.id().toString(),
                    confirmed.fields().stream().map(pair -> List.of(pair.source())).toList(),
                    confirmed.fields().stream().map(pair -> List.of(pair.target())).toList());
            var old = previousMappings.get(draft.mappingId());
            var dependencies = selected.original().dependencyHashes();
            if (method == previousMethod && old != null && old.keys() != null && !old.keys().deferred()
                    && old.keys().confirmationId().equals(keys.confirmationId())
                    && old.keys().sourcePaths().equals(keys.sourcePaths()) && old.keys().targetPaths().equals(keys.targetPaths())
                    && draft.outputId().equals(old.outputId()) && draft.outputKind() == old.outputKind()
                    && draft.source().equals(unbound(old.source())) && draft.target().equals(unbound(old.target()))
                    && sameDependency(previous, dependencies, draft.source().sourceId())
                    && sameDependency(previous, dependencies, draft.target().sourceId())) {
                mappings.add(old);
            } else {
                mappings.add(new ValidationTaskSpec.Mapping(draft.mappingId(), draft.outputKind(), draft.outputId(),
                        draft.source(), draft.target(), keys));
            }
        }
        long revision = previous == null ? 1 : previous.mappingRevision();
        if (previous != null && !previous.mappings().equals(mappings)) {
            if (revision >= Long.MAX_VALUE - 1) throw invalid("mappingRevision");
            revision++;
        }
        var comparison = new ValidationTaskSpec(selected.original().selection().pipelineId(),
                selected.original().dependencyHashes(), revision, RULES_VERSION, mappings);
        comparison.requireFor(method, ValidationMode.FULL);
        return new Compiled(comparison, fingerprint(comparison, method, List.of(), selected.original().parentGeneration()), method);
    }

    private static boolean sameDependency(ValidationTaskSpec previous, Map<String,String> dependencies, String id) {
        return Objects.equals(previous.dependencyHashes().get(id), dependencies.get(id));
    }

    private static ValidationTaskSpec.Table unbound(ValidationTaskSpec.Table table) {
        return new ValidationTaskSpec.Table(table.nodeId(), table.sourceId(), table.database(), table.schema(), table.table(), null, null);
    }

    private static ValidationTaskSpec.Table bind(ValidationTaskSpec.Table table,NativeValidationMetadata.Side observed){
        var metadata=Objects.requireNonNull(observed.model());if(!table.table().equals(metadata.model().name()))throw invalid("modelTable");
        return new ValidationTaskSpec.Table(table.nodeId(),table.sourceId(),table.database(),table.schema(),table.table(),metadata.binding().modelVersion(),metadata.binding().codecBinding());
    }
    private static ValidationKeyBinding.Component component(ValidationKeyMetadata.Rules rules,String field,String mapping){
        var selected=rules.fields().get(field);if(selected==null)throw unsupported(mapping,field,rules.unavailable().getOrDefault(field,"key-field-missing"));return selected;
    }
    static String fingerprint(ValidationTaskSpec comparison,ValidationMethod method,List<NativeValidationMetadata.Mapping> observed){return fingerprint(comparison,method,observed,null);}
    private static String fingerprint(ValidationTaskSpec comparison,ValidationMethod method,List<NativeValidationMetadata.Mapping> observed,UUID parent){
        try{
            var hash=MessageDigest.getInstance("SHA-256");
            try(var data=new DataOutputStream(new DigestOutputStream(OutputStream.nullOutputStream(),hash))){
                text(data,"validation-evaluation-v1");text(data,parent==null?"unbound-parent":parent.toString());text(data,comparison.fingerprint());text(data,method.name());
                for(var mapping:observed){text(data,mapping.id());
                    for(var side:List.of(mapping.source(),mapping.target())){text(data,side.artifactHash());text(data,side.countOperation()==null?"ROWS":side.countOperation().name());}
                }
            }
            return HexFormat.of().formatHex(hash.digest());
        }catch(IOException|NoSuchAlgorithmException invariant){throw new IllegalStateException("evaluation fingerprint unavailable",invariant);}
    }
    private static void text(DataOutputStream out,String value)throws IOException{var bytes=value.getBytes(StandardCharsets.UTF_8);out.writeInt(bytes.length);out.write(bytes);}
    private static void identifier(String value){if(value==null||value.isBlank())throw invalid("field");ValidationError.fieldName(value,"confirmation.field");}
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","compile."+field),null);}
    private static TapstateException unsupported(String node,String field,String rule){return new TapstateException(ValidationError.RULE_UNSUPPORTED,Map.of("node",node,"field",field,"rule",rule),null);}
}
