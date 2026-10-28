package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRename;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationTaskSpec;
import io.tapstate.spi.store.SourceTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Resolves fixed direct mappings against independently obtained table metadata. This compiler does
 * not certify native ordering, codec/model versions, driver budgets or run authority: the actual
 * reader factory must establish those before opening each query. COUNT may supply name-only table
 * metadata from table enumeration; no business value model or production state is required here.
 */
final class ValidationDirectMappings {
    private ValidationDirectMappings() {}

    record Mapping(ValidationTaskSpec.Mapping original, SourceResource source, SourceResource target,
                   SourceTable sourceModel, SourceTable targetModel) {
        @Override public String toString() { return "DirectValidationMapping[id=" + original.mappingId() + "]"; }
    }

    private record Declared(ValidationTaskSpec.Mapping mapping, SourceResource source, SourceResource target,
            io.tapstate.core.model.FromClause from) {}

    static List<Mapping> compile(ValidationSavedDeclarations saved,
            Function<ValidationTaskSpec.Table, SourceTable> independentMetadata) {
        Objects.requireNonNull(saved); Objects.requireNonNull(independentMetadata);
        var declarations = new ArrayList<Declared>();
        for (var mapping : saved.original().task().comparison().mappings()) {
            var selected = saved.mapping(mapping.mappingId());
            if (mapping.outputKind() != ValidationTaskSpec.OutputKind.SYNC || selected.sync() == null
                    || selected.view() != null) throw unsupported(mapping.outputId(), "from", "direct.source-to-destination-required");
            if (!selected.steps().isEmpty()) {
                var step = selected.steps().values().stream().sorted(java.util.Comparator.comparing(value -> value.id())).findFirst().orElseThrow();
                throw unsupported(step.id(), "type", "direct.transform-" + step.body().type());
            }
            // A selected pending reference with downstream operators must not bypass this gate.
            for (var pending : selected.pending()) if (!pending.downstreamSteps().isEmpty())
                throw unsupported(pending.downstreamSteps().getFirst().id(), "type", "direct.transform-required");
            var source = StoredArtifacts.requireSource(saved.artifacts(), mapping.source().sourceId());
            var target = StoredArtifacts.requireSource(saved.artifacts(), mapping.target().sourceId());
            if (!mapping.source().nodeId().equals(source.id()) || !mapping.target().nodeId().equals(mapping.outputId()))
                throw invalid("nodeBinding");
            if (!TableRename.apply(mapping.source().table(), selected.sync().rename()).equals(mapping.target().table()))
                throw invalid("targetTableScope");
            declarations.add(new Declared(mapping, source, target, selected.from()));
        }
        var compiled = new ArrayList<Mapping>();
        for (var declaration : declarations) {
            var mapping = declaration.mapping();
            var sourceModel = model(mapping.source(), independentMetadata);
            var targetModel = model(mapping.target(), independentMetadata);
            if (!PipelineTableSelection.selects(declaration.from(), declaration.source(), sourceModel)) throw invalid("sourceTableScope");
            compiled.add(new Mapping(mapping, declaration.source(), declaration.target(), sourceModel, targetModel));
        }
        return List.copyOf(compiled);
    }

    private static SourceTable model(ValidationTaskSpec.Table table, Function<ValidationTaskSpec.Table, SourceTable> metadata) {
        var model = Objects.requireNonNull(metadata.apply(table), "independent table metadata");
        if (!model.name().equals(table.table())) throw invalid("modelTableScope");
        return model;
    }
    private static TapstateException unsupported(String node, String field, String rule) {
        return new TapstateException(ValidationError.RULE_UNSUPPORTED, Map.of("node", node, "field", field, "rule", rule), null);
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "direct." + field), null);
    }
}
