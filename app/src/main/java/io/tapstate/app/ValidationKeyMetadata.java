package io.tapstate.app;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.SourceTable;
import java.util.*;

/** Fixed expected scalar rules from independent models; selected PDK codecs still run before row admission. */
final class ValidationKeyMetadata {
    record Rules(Map<String,ValidationKeyBinding.Component> fields,Map<String,String> unavailable){
        Rules{fields=Map.copyOf(fields);unavailable=Map.copyOf(unavailable);}
        Map<String,ValidationKey.Kind> domains(){var result=new HashMap<String,ValidationKey.Kind>();fields.forEach((field,rule)->result.put(field,rule.domain()));return Map.copyOf(result);}
    }
    static Rules describe(SourceTable model,boolean nativeBson){
        Objects.requireNonNull(model);if(model.fields().size()>4096)throw invalid("keyFields");
        var rules=new LinkedHashMap<String,ValidationKeyBinding.Component>();var unavailable=new LinkedHashMap<String,String>();var names=new HashSet<String>();
        for(var field:model.fields()){
            var name=field.name();if(!names.add(name))throw invalid("duplicateField");
            if(!name.matches("[A-Za-z_][A-Za-z0-9_]*")||name.length()>512){unavailable.put(name,"native-field-syntax");continue;}
            ValidationKey.Kind domain;Set<String> logical;var numeric=ValidationKeyBinding.NumericPolicy.EXACT_ONLY;
            switch(field.type()){
                case INT64,DECIMAL -> {domain=ValidationKey.Kind.EXACT_NUMBER;logical=Set.of("exact-number");}
                case DOUBLE -> {
                    domain=ValidationKey.Kind.EXACT_NUMBER;logical=Set.of("float32","float64","exact-number");
                    // A custom codec may retain an exact numeric result; never force it through Double.
                    numeric=ValidationKeyBinding.NumericPolicy.EXACT_BINARY_FLOAT;
                }
                case STRING -> {
                    domain=ValidationKey.Kind.TEXT;
                    boolean identifier=nativeBson&&field.dataType()!=null&&field.dataType().replace("_","").equalsIgnoreCase("OBJECTID");
                    logical=identifier?Set.of("object-id"):Set.of("text");
                }
                case BOOLEAN -> {domain=ValidationKey.Kind.BOOLEAN;logical=Set.of("boolean");}
                case BINARY -> {unavailable.put(name,"binary-key-subtype-unavailable");continue;}
                case DATE,TIME,DATETIME,YEAR -> {unavailable.put(name,"temporal-key-domain-unavailable");continue;}
                case ARRAY,MAP,JSON -> {unavailable.put(name,"key-scalar-domain-required");continue;}
                case UNKNOWN -> {unavailable.put(name,"key-model-unknown");continue;}
                default -> throw new IllegalStateException("unhandled model type "+field.type());
            }
            rules.put(name,new ValidationKeyBinding.Component(List.of(name),domain,logical,numeric));
        }
        return new Rules(rules,unavailable);
    }
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","metadata."+field),null);}
}
