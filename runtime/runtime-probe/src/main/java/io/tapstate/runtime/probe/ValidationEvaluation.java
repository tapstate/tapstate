package io.tapstate.runtime.probe;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.ValidationTaskStore;
import java.time.Instant;
import java.util.*;

/** Admitted metadata lease: consumers must finish projection or saving before closing it. */
public abstract class ValidationEvaluation implements AutoCloseable {
    public record Choice(String mappingId,String outputId,String sourceId,String table) {
        public Choice { name(mappingId);name(outputId);name(sourceId);name(table); }
    }
    public record Pair(String source,String target) { public Pair { name(source);name(target); } }
    public record Confirmation(UUID id,List<Pair> fields) {
        public Confirmation {
            if(id==null||fields==null||fields.isEmpty()||fields.size()>ValidationKey.MAX_COMPONENTS)throw invalid("confirmation");
            fields=List.copyOf(fields);var source=new HashSet<String>();var target=new HashSet<String>();
            for(var pair:fields)if(!source.add(pair.source())||!target.add(pair.target()))throw invalid("duplicateKey");
        }
    }
    /** Previous spec is server-loaded, never accepted as a trusted client model. */
    public record Request(ValidationTaskStore.Selection selection,List<Choice> choices,ValidationMethod method,
            Map<String,Confirmation> confirmations,ValidationTaskSpec previous,Instant deadline) {
        public Request {
            if(selection==null||method==null||deadline==null||choices==null||choices.isEmpty()||choices.size()>ValidationTaskSpec.MAX_MAPPINGS
                    ||confirmations==null||confirmations.size()>ValidationTaskSpec.MAX_MAPPINGS)throw invalid("request");
            choices=List.copyOf(choices);confirmations=Map.copyOf(confirmations);
            var mappings=new HashSet<String>();var outputs=new HashSet<String>();
            for(var choice:choices){if(!mappings.add(choice.mappingId()))throw invalid("duplicateMapping");outputs.add(choice.outputId());}
            if(!outputs.equals(new HashSet<>(selection.outputIds()))||!mappings.containsAll(confirmations.keySet())
                    ||method==ValidationMethod.COUNT&&!confirmations.isEmpty()
                    ||!confirmations.isEmpty()&&!mappings.equals(confirmations.keySet())
                    ||previous!=null&&!previous.pipelineId().equals(selection.pipelineId()))throw invalid("selection");
        }
    }
    /** Safe metadata projection, with no connector configuration, body values or driver objects. */
    public record Field(String name,String type,String keyDomain,String unavailableReason) {}
    public record Candidate(String origin,List<String> fields,List<String> issues) {
        public Candidate { fields=List.copyOf(fields);issues=List.copyOf(issues); }
    }
    public record Side(String nodeId,String sourceId,String database,String schema,String table,String modelVersion,
            String codecBinding,String countOperation,List<Field> fields,List<Candidate> candidates,boolean pipelineWriteKeyUnknown) {
        public Side { fields=List.copyOf(fields);candidates=List.copyOf(candidates); }
    }
    public record Mapping(String id,Side source,Side target) {}
    public record View(String method,String configurationRevision,String mappingRevision,String fingerprint,List<Mapping> mappings,
            String premise,String conclusionScope) {
        public View { mappings=List.copyOf(mappings); }
    }
    public abstract View view();
    /** Available only after every non-count mapping has explicit, independently checked key confirmation. */
    public abstract Optional<ValidationTaskSpec> comparison();
    public abstract void requireCurrent();
    @Override public abstract void close();
    private static void name(String value){if(value==null||value.isBlank())throw invalid("identity");ValidationError.fieldName(value,"evaluation.identity");}
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","evaluation."+field),null);}
}
